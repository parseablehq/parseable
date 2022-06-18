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

use crate::error::Error;
use crate::option::CONFIG;
use crate::utils;

use async_trait::async_trait;
use bytes::Bytes;
use chrono::{Timelike, Utc};
use serde::Serialize;

use std::fmt::{Debug, Display};
use std::fs;
use std::io;
use std::iter::Iterator;
use std::path::Path;
use std::time::Duration;

pub trait ObjectStorageError: Display + Debug {}

#[async_trait]
pub trait ObjectStorage: Sync + 'static {
    fn new() -> Self;
    async fn is_available(&self) -> bool;
    async fn put_schema(&self, stream_name: String, body: String) -> Result<(), Error>;
    async fn create_stream(&self, stream_name: &str) -> Result<(), Error>;
    async fn delete_stream(&self, stream_name: &str) -> Result<(), Error>;
    async fn create_alert(&self, stream_name: &str, body: String) -> Result<(), Error>;
    async fn stream_exists(&self, stream_name: &str) -> Result<Bytes, Error>;
    async fn alert_exists(&self, stream_name: &str) -> Result<Bytes, Error>;
    async fn list_streams(&self) -> Result<Vec<LogStream>, Error>;
    async fn put_parquet(&self, key: &str, path: &str) -> Result<(), Error>;
    async fn sync(&self) -> Result<(), Error> {
        if !Path::new(&CONFIG.parseable.local_disk_path).exists() {
            return Ok(());
        }

        let entries = fs::read_dir(&CONFIG.parseable.local_disk_path)?
            .map(|res| res.map(|e| e.path()))
            .collect::<Result<Vec<_>, io::Error>>()?;
        let sync_duration = Duration::from_secs(CONFIG.parseable.sync_duration);

        for entry in entries {
            let path = entry.into_os_string().into_string().unwrap();
            let init_sync = StorageSync {
                path,
                time: Utc::now(),
            };

            let dir = init_sync.get_dir_name();
            if !init_sync.parquet_path_exists() {
                continue;
            }

            let metadata = fs::metadata(&dir.parquet_path)?;
            let time = match metadata.created() {
                Ok(time) => time,
                _ => continue,
            };

            if time.elapsed().unwrap() <= sync_duration {
                continue;
            }

            if let Err(e) = dir.create_dir_name_tmp() {
                log::error!(
                    "Error copying parquet file {} due to error [{}]",
                    dir.parquet_path,
                    e
                );
                continue;
            }

            if let Err(e) = dir.copy_parquet_to_tmp() {
                log::error!(
                    "Error copying parquet from stream dir to tmp in path {} due to error [{}]",
                    dir.dir_name_local,
                    e
                );
                continue;
            }
            // TODO: retries to storage
            let _put_parquet_file = self
                .put_parquet(
                    &format!("{}/{}", dir.storage_dir_name, dir.parquet_file),
                    &format!("{}/{}", dir.dir_name_tmp, dir.parquet_file),
                )
                .await;

            if let Err(e) = dir.delete_parquet_file() {
                log::error!(
                    "Error deleting parquet file in path {} due to error [{}]",
                    dir.parquet_path,
                    e
                );
                continue;
            }
        }

        Ok(())
    }
}

#[derive(Serialize)]
pub struct LogStream {
    pub name: String,
}

#[derive(Debug)]
struct DirName {
    storage_dir_name: String,
    dir_name_tmp: String,
    dir_name_local: String,
    parquet_path: String,
    parquet_file: String,
}

impl DirName {
    fn copy_parquet_to_tmp(&self) -> io::Result<()> {
        fs::copy(
            &self.parquet_path,
            format!("{}/{}", self.dir_name_tmp, self.parquet_file),
        )?;

        Ok(())
    }

    fn create_dir_name_tmp(&self) -> io::Result<()> {
        fs::create_dir_all(&self.dir_name_tmp)
    }

    fn delete_parquet_file(&self) -> io::Result<()> {
        fs::remove_file(format!("{}/data.parquet", self.dir_name_local))
    }
}

struct StorageSync {
    path: String,
    time: chrono::DateTime<Utc>,
}

impl StorageSync {
    fn parquet_path_exists(&self) -> bool {
        let new_parquet_path = format!("{}/data.parquet", &self.path);

        Path::new(&new_parquet_path).exists()
    }

    fn get_dir_name(&self) -> DirName {
        let local_path = format!("{}/", CONFIG.parseable.local_disk_path);
        let _storage_path = format!("{}/", CONFIG.storage.bucket_name());
        let stream_names = self.path.replace(&local_path, "");
        let new_parquet_path = format!("{}/data.parquet", self.path);

        let dir_name_tmp = format!(
            "{}{}/tmp/date={}/hour={:02}/minute={}",
            local_path,
            stream_names,
            chrono::offset::Utc::now().date(),
            self.time.hour(),
            time_slot(),
        );

        let storage_dir_name = format!(
            "{}/date={}/hour={:02}/minute={}",
            stream_names,
            chrono::offset::Utc::now().date(),
            self.time.hour(),
            time_slot()
        );

        let parquet = format!("{}.parquet", utils::random_string());
        let dir_name_local = local_path + &stream_names;

        DirName {
            storage_dir_name,
            dir_name_tmp,
            dir_name_local,
            parquet_path: new_parquet_path,
            parquet_file: parquet,
        }
    }
}

pub fn time_slot() -> &'static str {
    match chrono::offset::Utc::now().minute() {
        0..=9 => "00-09",
        10..=19 => "10-19",
        20..=29 => "20-29",
        30..=39 => "30-39",
        40..=49 => "40-49",
        50..=59 => "49-59",
        _ => "",
    }
}
