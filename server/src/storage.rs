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
use crate::query::Query;
use crate::utils;

use async_trait::async_trait;
use bytes::Bytes;
use chrono::{Timelike, Utc};
use datafusion::arrow::record_batch::RecordBatch;
use serde::Serialize;

use std::fmt::{Debug, Display};
use std::fs;
use std::io;
use std::iter::Iterator;
use std::path::Path;
use std::time::Duration;

pub trait ObjectStorageError: Display + Debug {}

/// duration used to configure prefix in s3 and local disk structure
/// used for storage. Defaults to 1 min.
pub const BLOCK_DURATION: u32 = 1;

#[async_trait]
pub trait ObjectStorage: Sync + 'static {
    async fn is_available(&self) -> bool;
    async fn put_schema(&self, stream_name: String, body: String) -> Result<(), Error>;
    async fn create_stream(&self, stream_name: &str) -> Result<(), Error>;
    async fn delete_stream(&self, stream_name: &str) -> Result<(), Error>;
    async fn create_alert(&self, stream_name: &str, body: String) -> Result<(), Error>;
    async fn get_schema(&self, stream_name: &str) -> Result<Bytes, Error>;
    async fn get_alert(&self, stream_name: &str) -> Result<Bytes, Error>;
    async fn list_streams(&self) -> Result<Vec<LogStream>, Error>;
    async fn put_parquet(&self, key: &str, path: &str) -> Result<(), Error>;
    async fn query(&self, query: &Query, results: &mut Vec<RecordBatch>) -> Result<(), Error>;
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
            let init_sync = StorageSync::new(path);

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

            if let Err(e) = dir.move_parquet_to_tmp() {
                log::error!(
                    "Error copying parquet from stream dir to tmp in path {} due to error [{}]",
                    dir.dir_name_local,
                    e
                );
                continue;
            }

            let _put_parquet_file = self
                .put_parquet(
                    &dir.parquet_file_s3,
                    &format!("{}/tmp/{}", &dir.dir_name_local, &dir.parquet_file_local),
                )
                .await?;

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
    dir_name_tmp_local: String,
    dir_name_local: String,
    parquet_path: String,
    parquet_file_local: String,
    parquet_file_s3: String,
}

impl DirName {
    fn move_parquet_to_tmp(&self) -> io::Result<()> {
        fs::rename(
            &self.parquet_path,
            format!("{}/{}", self.dir_name_tmp_local, self.parquet_file_local),
        )
    }

    fn create_dir_name_tmp(&self) -> io::Result<()> {
        fs::create_dir_all(&self.dir_name_tmp_local)
    }

    fn delete_parquet_file(&self) -> io::Result<()> {
        fs::remove_file(format!(
            "{}/tmp/{}",
            self.dir_name_local, self.parquet_file_local
        ))
    }
}

struct StorageSync {
    path: String,
    time: chrono::DateTime<Utc>,
}

impl StorageSync {
    fn new(path: String) -> Self {
        Self {
            path,
            time: Utc::now(),
        }
    }

    fn parquet_path_exists(&self) -> bool {
        let new_parquet_path = format!("{}/data.parquet", &self.path);

        Path::new(&new_parquet_path).exists()
    }

    fn get_dir_name(&self) -> DirName {
        let local_path = format!("{}/", CONFIG.parseable.local_disk_path);
        let _storage_path = format!("{}/", CONFIG.storage.bucket_name());
        let stream_names = self.path.replace(&local_path, "");
        let parquet_path = format!("{}/data.parquet", self.path);
        let uri = utils::date_to_prefix(self.time.date())
            + &utils::hour_to_prefix(self.time.hour())
            + &utils::minute_to_prefix(self.time.minute(), BLOCK_DURATION).unwrap();

        let local_uri = str::replace(&uri, "/", ".");

        let dir_name_tmp_local = format!("{}{}/tmp", local_path, stream_names);

        let storage_dir_name = format!("{}/{}", stream_names, uri);

        let random_string = utils::random_string();

        let parquet_file_local = format!("{}{}.parquet", local_uri, random_string);

        let parquet_file_s3 = format!("{}{}.parquet", storage_dir_name, random_string);

        let dir_name_local = local_path + &stream_names;

        DirName {
            dir_name_tmp_local,
            dir_name_local,
            parquet_path,
            parquet_file_local,
            parquet_file_s3,
        }
    }
}
