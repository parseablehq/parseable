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
use crate::metadata::Stats;
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

extern crate walkdir;
use walkdir::WalkDir;
pub trait ObjectStorageError: Display + Debug {}

/// local sync interval to move data.parquet to /tmp dir of that stream.
/// 60 sec is a reasonable value.
pub const LOCAL_SYNC_INTERVAL: u64 = 60;

/// duration used to configure prefix in s3 and local disk structure
/// used for storage. Defaults to 1 min.
pub const OBJECT_STORE_DATA_GRANULARITY: u32 = (LOCAL_SYNC_INTERVAL as u32) / 60;

#[async_trait]
pub trait ObjectStorage: Sync + 'static {
    async fn is_available(&self) -> bool;
    async fn put_schema(&self, stream_name: String, body: String) -> Result<(), Error>;
    async fn create_stream(&self, stream_name: &str) -> Result<(), Error>;
    async fn delete_stream(&self, stream_name: &str) -> Result<(), Error>;
    async fn create_alert(&self, stream_name: &str, body: String) -> Result<(), Error>;
    async fn get_schema(&self, stream_name: &str) -> Result<Bytes, Error>;
    async fn get_alert(&self, stream_name: &str) -> Result<Bytes, Error>;
    async fn get_stats(&self, stream_name: &str) -> Result<Stats, Error>;
    async fn list_streams(&self) -> Result<Vec<LogStream>, Error>;
    async fn upload_file(&self, key: &str, path: &str) -> Result<(), Error>;
    async fn query(&self, query: &Query, results: &mut Vec<RecordBatch>) -> Result<(), Error>;
    async fn local_sync(&self) -> Result<(), Error> {
        // If the local data path doesn't exist yet, return early.
        // This method will be called again after next ticker interval
        if !Path::new(&CONFIG.parseable.local_disk_path).exists() {
            return Ok(());
        }

        let entries = fs::read_dir(&CONFIG.parseable.local_disk_path)?
            .map(|res| res.map(|e| e.path()))
            .collect::<Result<Vec<_>, io::Error>>()?;

        // entries here means all the streams present on local disk
        for entry in entries {
            let path = entry.into_os_string().into_string().unwrap();
            let init_sync = StorageSync::new(path);

            // if data.parquet file not present, skip this stream
            if !init_sync.parquet_path_exists() {
                continue;
            }

            let dir = init_sync.get_dir_name();
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
        }

        Ok(())
    }

    async fn s3_sync(&self) -> Result<(), Error> {
        if !Path::new(&CONFIG.parseable.local_disk_path).exists() {
            return Ok(());
        }

        let entries = fs::read_dir(&CONFIG.parseable.local_disk_path)?
            .map(|res| res.map(|e| e.path()))
            .collect::<Result<Vec<_>, io::Error>>()?;

        for entry in entries {
            let path = entry.into_os_string().into_string().unwrap();
            let init_sync = StorageSync::new(path);

            let dir = init_sync.get_dir_name();

            for file in WalkDir::new(&format!("{}/tmp", &dir.dir_name_local))
                .into_iter()
                .filter_map(|file| file.ok())
            {
                if file.metadata().unwrap().is_file() {
                    let file_local = format!("{}", file.path().display());
                    let file_s3 = file_local.replace("/tmp", "");
                    let final_s3_path =
                        file_s3.replace(&format!("{}/", CONFIG.parseable.local_disk_path), "");
                    let f_path = str::replace(&final_s3_path, ".", "/");
                    let f_new_path = f_path.replace("/parquet", ".parquet");
                    let _put_parquet_file = self.upload_file(&f_new_path, &file_local).await?;
                    if let Err(e) = dir.delete_parquet_file(file_local.clone()) {
                        log::error!(
                            "Error deleting parquet file in path {} due to error [{}]",
                            file_local,
                            e
                        );
                    }
                }
            }

            if let Err(e) = dir.flush_stream_stats(self).await {
                log::error!(
                    "Error flushing stats for stream {} due to error [{}]",
                    dir.stream_name,
                    e
                );
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
    stream_name: String,
    dir_name_tmp_local: String,
    dir_name_local: String,
    parquet_path: String,
    parquet_file_local: String,
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

    fn delete_parquet_file(&self, path: String) -> io::Result<()> {
        fs::remove_file(path)
    }

    async fn flush_stream_stats(
        &self,
        obj_store: &(impl ObjectStorage + ?Sized),
    ) -> Result<(), Error> {
        let stat_path = format!(
            "{}/{}/.stats.json",
            CONFIG.parseable.local_disk_path, self.stream_name
        );
        let stats = crate::metadata::STREAM_INFO.flush_stream_stats(&self.stream_name)?;
        fs::write(&stat_path, serde_json::to_vec(&stats)?)?;

        let stat_key = format!("{}/.stats.json", self.stream_name);
        obj_store.upload_file(&stat_key, &stat_path).await?;

        Ok(())
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
        let stream_name = self.path.replace(&local_path, "");
        let parquet_path = format!("{}/data.parquet", self.path);
        let uri = utils::date_to_prefix(self.time.date())
            + &utils::hour_to_prefix(self.time.hour())
            // subtract OBJECT_STORE_DATA_GRANULARITY from current time here,
            // this is because, when we're creating this file 
            // the data in the file is from OBJECT_STORE_DATA_GRANULARITY time ago.
            + &utils::minute_to_prefix(self.time.minute()-OBJECT_STORE_DATA_GRANULARITY, OBJECT_STORE_DATA_GRANULARITY).unwrap();

        let local_uri = str::replace(&uri, "/", ".");

        let dir_name_tmp_local = format!("{}{}/tmp", local_path, stream_name);

        let storage_dir_name_s3 = format!("{}/{}", stream_name, uri);

        let random_string = utils::random_string();

        let parquet_file_local = format!("{}{}.parquet", local_uri, random_string);

        let _parquet_file_s3 = format!("{}{}.parquet", storage_dir_name_s3, random_string);

        let dir_name_local = local_path + &stream_name;

        DirName {
            stream_name,
            dir_name_tmp_local,
            dir_name_local,
            parquet_path,
            parquet_file_local,
        }
    }
}
