/*
 * Parseable Server (C) 2022 Parseable, Inc.
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

use actix_cors::Cors;
use actix_web::dev::ServiceRequest;
use actix_web::{middleware, web, App, HttpServer};
use actix_web_httpauth::extractors::basic::BasicAuth;
use actix_web_httpauth::middleware::HttpAuthentication;
use actix_web_static_files::ResourceFiles;
use chrono::{DateTime, Timelike, Utc};
use clokwerk::{AsyncScheduler, Scheduler, TimeUnits};
use log::warn;
use openssl::ssl::{SslAcceptor, SslFiletype, SslMethod};

include!(concat!(env!("OUT_DIR"), "/generated.rs"));

use std::fs;
use std::panic::{catch_unwind, AssertUnwindSafe};
use std::path::Path;
use std::thread::{self, JoinHandle};
use std::time::Duration;
use tokio::sync::oneshot;
use tokio::sync::oneshot::error::TryRecvError;

mod alerts;
mod banner;
mod error;
mod event;
mod handlers;
mod metadata;
mod option;
mod query;
mod response;
mod s3;
mod storage;
mod utils;
mod validator;

use error::Error;
use option::CONFIG;
use s3::S3;
use storage::{ObjectStorage, StorageDir};

// Global configurations
const MAX_EVENT_PAYLOAD_SIZE: usize = 102400;
const API_BASE_PATH: &str = "/api";
const API_VERSION: &str = "v1";

#[actix_web::main]
async fn main() -> anyhow::Result<()> {
    env_logger::init();
    CONFIG.print();
    CONFIG.validate();
    let storage = S3::new();
    CONFIG.validate_storage(&storage).await;
    if let Err(e) = metadata::STREAM_INFO.load(&storage).await {
        warn!("could not populate local metadata. {:?}", e);
    }

    // Move all exiting data.parquet file to their respective tmp directory
    // they will be synced to object store on next s3 sync cycle
    startup_sync();

    let (localsync_handler, mut localsync_outbox, localsync_inbox) = run_local_sync();
    let (mut s3sync_handler, mut s3sync_outbox, mut s3sync_inbox) = s3_sync();

    let app = run_http();
    tokio::pin!(app);
    loop {
        tokio::select! {
            e = &mut app => {
                // actix server finished .. stop other threads and stop the server
                s3sync_inbox.send(()).unwrap_or(());
                localsync_inbox.send(()).unwrap_or(());
                localsync_handler.join().unwrap_or(());
                s3sync_handler.join().unwrap_or(());
                return e
            },
            _ = &mut localsync_outbox => {
                // crash the server if localsync fails for any reason
                // panic!("Local Sync thread died. Server will fail now!")
                return Err(anyhow::Error::msg("Failed to sync local data to disc. This can happen due to critical error in disc or environment. Please restart the Parseable server.\n\nJoin us on Parseable Slack if the issue persists after restart : https://launchpass.com/parseable"))
            },
            _ = &mut s3sync_outbox => {
                // s3sync failed, this is recoverable by just starting s3sync thread again
                s3sync_handler.join().unwrap_or(());
                (s3sync_handler, s3sync_outbox, s3sync_inbox) = s3_sync();
            }
        };
    }
}

fn startup_sync() {
    if !Path::new(&CONFIG.parseable.local_disk_path).exists() {
        return;
    }

    for stream in metadata::STREAM_INFO.list_streams() {
        let dir = StorageDir::new(&stream);
        // if data.parquet file is not present then skip this stream
        if !dir.parquet_path_exists() {
            continue;
        }
        if let Err(e) = dir.create_temp_dir() {
            log::error!(
                "Error creating tmp directory for {} due to error [{}]",
                &stream,
                e
            );
            continue;
        }
        // create prefix for this file from its last modified time
        let path = dir.data_path.join("data.parquet");

        // metadata.modified gives us system time
        // This may not work on all platfomns
        let last_modified: DateTime<Utc> = match fs::metadata(&path)
            .and_then(|meta| meta.modified())
            .map(DateTime::from)
        {
            Ok(time) => time,
            Err(err) => {
                log::warn!(
                    "Failed to get last modified time for {} due to {:?}. Skipping!",
                    path.display(),
                    err
                );
                continue;
            }
        };

        let uri = utils::date_to_prefix(last_modified.date())
            + &utils::hour_to_prefix(last_modified.hour())
            + &utils::minute_to_prefix(
                last_modified.minute(),
                storage::OBJECT_STORE_DATA_GRANULARITY,
            )
            .unwrap();
        let local_uri = str::replace(&uri, "/", ".");
        let hostname = utils::hostname_unchecked();
        let parquet_file_local = format!("{}{}.data.parquet", local_uri, hostname);
        if let Err(err) = dir.move_parquet_to_temp(parquet_file_local) {
            log::warn!(
                "Failed to move parquet file at {} to tmp directory due to error {}",
                path.display(),
                err
            )
        }
    }
}

fn s3_sync() -> (JoinHandle<()>, oneshot::Receiver<()>, oneshot::Sender<()>) {
    let (outbox_tx, outbox_rx) = oneshot::channel::<()>();
    let (inbox_tx, inbox_rx) = oneshot::channel::<()>();
    let mut inbox_rx = AssertUnwindSafe(inbox_rx);
    let handle = thread::spawn(move || {
        let res = catch_unwind(move || {
            let rt = actix_web::rt::System::new();
            rt.block_on(async {
                let mut scheduler = AsyncScheduler::new();
                scheduler
                    .every((CONFIG.parseable.upload_interval as u32).seconds())
                    .run(|| async {
                        if let Err(e) = S3::new().s3_sync().await {
                            warn!("failed to sync local data with object store. {:?}", e);
                        }
                    });

                loop {
                    scheduler.run_pending().await;
                    match AssertUnwindSafe(|| inbox_rx.try_recv())() {
                        Ok(_) => break,
                        Err(TryRecvError::Empty) => continue,
                        Err(TryRecvError::Closed) => {
                            // should be unreachable but breaking anyways
                            break;
                        }
                    }
                }
            })
        });

        if res.is_err() {
            outbox_tx.send(()).unwrap();
        }
    });

    (handle, outbox_rx, inbox_tx)
}

fn run_local_sync() -> (JoinHandle<()>, oneshot::Receiver<()>, oneshot::Sender<()>) {
    let (outbox_tx, outbox_rx) = oneshot::channel::<()>();
    let (inbox_tx, inbox_rx) = oneshot::channel::<()>();
    let mut inbox_rx = AssertUnwindSafe(inbox_rx);
    let handle = thread::spawn(move || {
        let res = catch_unwind(move || {
            let mut scheduler = Scheduler::new();
            scheduler
                .every((storage::LOCAL_SYNC_INTERVAL as u32).seconds())
                .run(move || {
                    if let Err(e) = S3::new().local_sync() {
                        warn!("failed to sync local data. {:?}", e);
                    }
                });

            loop {
                thread::sleep(Duration::from_millis(50));
                scheduler.run_pending();
                match AssertUnwindSafe(|| inbox_rx.try_recv())() {
                    Ok(_) => break,
                    Err(TryRecvError::Empty) => continue,
                    Err(TryRecvError::Closed) => {
                        // should be unreachable but breaking anyways
                        break;
                    }
                }
            }
        });

        if res.is_err() {
            outbox_tx.send(()).unwrap();
        }
    });

    (handle, outbox_rx, inbox_tx)
}

async fn validator(
    req: ServiceRequest,
    credentials: BasicAuth,
) -> Result<ServiceRequest, actix_web::Error> {
    if credentials.user_id().trim() == CONFIG.parseable.username
        && credentials.password().unwrap().trim() == CONFIG.parseable.password
    {
        return Ok(req);
    }

    Err(actix_web::error::ErrorUnauthorized("Unauthorized"))
}

async fn run_http() -> anyhow::Result<()> {
    let ssl_acceptor = match (
        &CONFIG.parseable.tls_cert_path,
        &CONFIG.parseable.tls_key_path,
    ) {
        (Some(cert), Some(key)) => {
            let mut builder = SslAcceptor::mozilla_intermediate(SslMethod::tls())?;
            builder.set_private_key_file(key, SslFiletype::PEM)?;
            builder.set_certificate_chain_file(cert)?;
            Some(builder)
        }
        (_, _) => None,
    };

    let http_server = HttpServer::new(move || create_app!());
    if let Some(builder) = ssl_acceptor {
        http_server
            .bind_openssl(&CONFIG.parseable.address, builder)?
            .run()
            .await?;
    } else {
        http_server.bind(&CONFIG.parseable.address)?.run().await?;
    }

    Ok(())
}

pub fn configure_routes(cfg: &mut web::ServiceConfig) {
    let generated = generate();

    cfg.service(
        // Base path "{url}/api/v1"
        web::scope(&base_path())
            // POST "/query" ==> Get results of the SQL query passed in request body
            .service(web::resource(query_path()).route(web::post().to(handlers::event::query)))
            .service(
                // logstream API
                web::resource(logstream_path("{logstream}"))
                    // PUT "/logstream/{logstream}" ==> Create log stream
                    .route(web::put().to(handlers::logstream::put))
                    // POST "/logstream/{logstream}" ==> Post logs to given log stream
                    .route(web::post().to(handlers::event::post_event))
                    // DELETE "/logstream/{logstream}" ==> Delete log stream
                    .route(web::delete().to(handlers::logstream::delete))
                    .app_data(web::JsonConfig::default().limit(MAX_EVENT_PAYLOAD_SIZE)),
            )
            .service(
                web::resource(alert_path("{logstream}"))
                    // PUT "/logstream/{logstream}/alert" ==> Set alert for given log stream
                    .route(web::put().to(handlers::logstream::put_alert))
                    // GET "/logstream/{logstream}/alert" ==> Get alert for given log stream
                    .route(web::get().to(handlers::logstream::get_alert)),
            )
            // GET "/logstream" ==> Get list of all Log Streams on the server
            .service(
                web::resource(logstream_path("")).route(web::get().to(handlers::logstream::list)),
            )
            .service(
                // GET "/logstream/{logstream}/schema" ==> Get schema for given log stream
                web::resource(schema_path("{logstream}"))
                    .route(web::get().to(handlers::logstream::schema)),
            )
            // GET "/liveness" ==> Livenss check as per https://kubernetes.io/docs/tasks/configure-pod-container/configure-liveness-readiness-startup-probes/#define-a-liveness-command
            .service(web::resource(liveness_path()).route(web::get().to(handlers::liveness)))
            // GET "/readiness" ==> Readiness check as per https://kubernetes.io/docs/tasks/configure-pod-container/configure-liveness-readiness-startup-probes/#define-readiness-probes
            .service(web::resource(readiness_path()).route(web::get().to(handlers::readiness)))
            .wrap(HttpAuthentication::basic(validator)),
    )
    // GET "/" ==> Serve the static frontend directory
    .service(ResourceFiles::new("/", generated));
}

#[macro_export]
macro_rules! create_app {
    () => {
        App::new()
            .configure(|cfg| configure_routes(cfg))
            .wrap(middleware::Logger::default())
            .wrap(middleware::Compress::default())
            .wrap(
                Cors::default()
                    .allow_any_header()
                    .allow_any_method()
                    .allow_any_origin(),
            )
    };
}

fn base_path() -> String {
    format!("{}/{}", API_BASE_PATH, API_VERSION)
}

fn logstream_path(stream_name: &str) -> String {
    if stream_name.is_empty() {
        "/logstream".to_string()
    } else {
        format!("/logstream/{}", stream_name)
    }
}

fn readiness_path() -> String {
    "/readiness".to_string()
}

fn liveness_path() -> String {
    "/liveness".to_string()
}

fn query_path() -> String {
    "/query".to_string()
}

fn alert_path(stream_name: &str) -> String {
    format!("{}/alert", logstream_path(stream_name))
}

fn schema_path(stream_name: &str) -> String {
    format!("{}/schema", logstream_path(stream_name))
}
