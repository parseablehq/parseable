/*
 * Parseable Server (C) 2022 - 2024 Parseable, Inc.
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 *
 */

use std::fs::File;

use tokio::{fs::OpenOptions, io::AsyncWriteExt, select, sync::mpsc::channel, time::interval};
use tracing::{error, warn};
use url::Url;

use crate::{option::CONFIG, HTTP_CLIENT};

use super::{AuditLog, AUDIT_LOG_TX};

// AuditLogger handles sending audit logs to a remote logging system
pub struct AuditLogger {
    log_endpoint: Option<Url>,
    batch: Vec<AuditLog>,

    // NOTE: good until usize overflows
    next_log_file_id: usize,
    oldest_log_file_id: usize,
}

impl Default for AuditLogger {
    /// Create an audit logger that can be used to capture and push
    /// audit logs to the appropriate logging system over HTTP
    fn default() -> Self {
        // Try to construct the log endpoint URL by joining the base URL
        // with the ingest path, This can fail if the URL is not valid,
        // when the base URL is not set or the ingest path is not valid
        let log_endpoint = CONFIG.parseable.audit_logger.as_ref().and_then(|endpoint| {
            endpoint
                .join("/api/v1/ingest")
                .inspect_err(|err| eprintln!("Couldn't setup audit logger: {err}"))
                .ok()
        });

        // Created directory for audit logs if it doesn't exist
        std::fs::create_dir_all(&CONFIG.parseable.audit_log_dir)
            .expect("Failed to create audit log directory");

        // Figure out the latest and oldest log file in directory
        let files = std::fs::read_dir(&CONFIG.parseable.audit_log_dir)
            .expect("Failed to read audit log directory");
        let (mut oldest_log_file_id, next_log_file_id) =
            files.fold((usize::MAX, 0), |(oldest, next), r| {
                let file_name = r.unwrap().file_name();
                let Ok(file_id) = file_name
                    .to_str()
                    .expect("File name is not utf8")
                    .split('.')
                    .next()
                    .expect("File name is not valid")
                    .parse::<usize>()
                    .inspect_err(|e| warn!("Unexpected file in logs directory: {e}"))
                else {
                    return (oldest, next);
                };
                (oldest.min(file_id), next.max(file_id))
            });

        if oldest_log_file_id == usize::MAX {
            oldest_log_file_id = 0;
        }

        AuditLogger {
            log_endpoint,
            batch: Vec::with_capacity(CONFIG.parseable.audit_batch_size),
            next_log_file_id,
            oldest_log_file_id,
        }
    }
}

impl AuditLogger {
    /// Flushes audit logs to the remote logging system
    async fn flush(&mut self) {
        if self.batch.is_empty() {
            return;
        }

        // swap the old batch with a new empty one
        let mut logs_to_send = Vec::with_capacity(CONFIG.parseable.audit_batch_size);
        std::mem::swap(&mut self.batch, &mut logs_to_send);

        // send the logs to the remote logging system, if no backlog, else write to disk
        if self.oldest_log_file_id >= self.next_log_file_id
            && self.send_logs_to_remote(&logs_to_send).await.is_ok()
        {
            return;
        }

        // write the logs to the next log file
        let log_file_path = CONFIG
            .parseable
            .audit_log_dir
            .join(format!("{}.json", self.next_log_file_id));
        let mut log_file = OpenOptions::new()
            .create(true)
            .truncate(true)
            .open(log_file_path)
            .await
            .expect("Failed to open audit log file");
        let buf = serde_json::to_vec(&logs_to_send).expect("Failed to serialize audit logs");
        log_file.write_all(&buf).await.unwrap();

        // increment the next log file id
        self.next_log_file_id += 1;
    }

    /// Inserts an audit log into the batch, and flushes the batch if it exceeds the configured batch size
    async fn insert(&mut self, log: AuditLog) {
        self.batch.push(log);

        // Flush if batch size exceeds threshold
        if self.batch.len() >= CONFIG.parseable.audit_batch_size {
            self.flush().await
        }
    }

    /// Reads the oldest log file and sends it to the audit logging backend
    async fn send_logs(&self) -> Result<(), anyhow::Error> {
        // if there are no logs to send, do nothing
        if self.oldest_log_file_id >= self.next_log_file_id {
            return Ok(());
        }

        // read the oldest log file
        let oldest_file_path = CONFIG
            .parseable
            .audit_log_dir
            .join(format!("{}.json", self.oldest_log_file_id));
        let mut oldest_file = File::open(&oldest_file_path)?;
        let logs_to_send: Vec<AuditLog> = serde_json::from_reader(&mut oldest_file)?;
        self.send_logs_to_remote(&logs_to_send).await?;

        // Delete the oldest log file
        std::fs::remove_file(oldest_file_path)?;

        Ok(())
    }

    async fn send_logs_to_remote(&self, logs: &Vec<AuditLog>) -> Result<(), anyhow::Error> {
        // send the logs to the audit logging backend
        let log_endpoint = self
            .log_endpoint
            .as_ref()
            .expect("Audit logger was initialized!");
        let mut req = HTTP_CLIENT
            .post(log_endpoint.as_str())
            .json(&logs)
            .header("x-p-stream", "audit_log");

        // Use basic auth if credentials are configured
        if let Some(username) = CONFIG.parseable.audit_username.as_ref() {
            req = req.basic_auth(username, CONFIG.parseable.audit_password.as_ref())
        }

        // Send batched logs to the audit logging backend
        req.send().await?.error_for_status()?;

        Ok(())
    }

    /// Spawns a background task for periodic flushing of audit logs, if configured
    pub async fn spawn_batcher(mut self) {
        if self.log_endpoint.is_none() {
            return;
        }

        // setup the audit log channel
        let (audit_log_tx, mut audit_log_rx) = channel(0);
        AUDIT_LOG_TX
            .set(audit_log_tx)
            .expect("Failed to set audit logger tx");

        // spawn the batcher
        tokio::spawn(async move {
            let mut interval = interval(CONFIG.parseable.audit_flush_interval);
            loop {
                select! {
                    _ = interval.tick() => {
                        self.flush().await;
                    }

                    Some(log) = audit_log_rx.recv() => {
                        self.insert(log).await;
                    }

                    r = self.send_logs() => {
                        if let Err(e) = r {
                            error!("Failed to send logs: {e}");
                            continue;
                        }
                        self.oldest_log_file_id += 1;
                    },
                }
            }
        });
    }
}
