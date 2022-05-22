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
use crate::mem_store;
use crate::option;
use crate::utils;
use chrono::{Timelike, Utc};
use derive_more::Display;
use fslock::LockFile;
use std::env;
use std::fs;
use std::io;
use std::path::Path;
use std::time::Duration;
extern crate fs_extra;
use crate::storage;
use aws_smithy_http::byte_stream::ByteStream;
use fs_extra::dir;

pub fn syncer(opt: option::Opt) -> Result<(), Error> {
    if !Path::new(&opt.local_disk_path).exists() {
        return Ok(());
    }

    let entries = fs::read_dir(&opt.local_disk_path)?
        .map(|res| res.map(|e| e.path()))
        .collect::<Result<Vec<_>, io::Error>>()?;
    for entry in entries {
        let path = format!("{:?}", entry);
        let init_s3_sync = S3Sync {
            opt: opt.clone(),
            path,
            time: Utc::now(),
        };
        let dir = init_s3_sync.get_dir_name();
        if init_s3_sync.parquet_path_exists() {
            let metadata = fs::metadata(&dir.parquet_path)?;
            if let Ok(time) = metadata.created() {
                let sync_duration = Duration::from_secs(opt.sync_duration);
                if time.elapsed().unwrap() > sync_duration {
                    let local_ops = dir.local_ops();
                    if let Some(x) = local_ops {
                        x.log_syncer_error()
                    }
                    new_rb(dir.stream_name)?;
                }
            }
        }
    }

    Ok(())
}

struct S3Sync {
    opt: option::Opt,
    path: String,
    time: chrono::DateTime<Utc>,
}

#[derive(Debug)]
struct DirName {
    dir_name_s3: String,
    dir_name_cache: String,
    dir_name_cache_tmp: String,
    parquet_path: String,
    parquet_file: String,
    stream_name: String,
}

#[derive(Debug, Display, derive_more::Error, PartialEq)]
pub struct SyncerError {
    pub msg: String,
}

impl SyncerError {
    fn log_syncer_error(&self) {
        log::error!("{}", self.msg);
    }
}

impl DirName {
    fn local_ops(&self) -> Option<SyncerError> {
        if let Err(error) = self.create_dir_name_cache() {
            return Some(error);
        }

        if let Err(error) = self.create_dir_name_cache_tmp() {
            return Some(error);
        }

        let _copy_parquet_to_cache = self.file_lock();
        // if let Err(error) = copy_parquet_to_cache {
        //     return Some(error)
        // }

        if let Err(error) = self.copy_parquet_to_cache() {
            return Some(error);
        }

        if let Err(error) = self.delete_parquet_file() {
            return Some(error);
        }

        None
    }

    fn copy_parquet_to_cache(&self) -> Result<(), SyncerError> {
        // println!("{}", format!("{}/{}", &self.dir_name_cache_tmp, &self.parquet_file));
        // println!("{}",format!("{}/{}", &self.dir_name_cache, &self.parquet_file));

        fs::copy(
            format!("{}/{}", &self.dir_name_cache_tmp, "data.parquet"),
            format!("{}/{}", &self.dir_name_cache, &self.parquet_file),
        )
        .map_err(|e| SyncerError {
            msg: format! {
                "Error copy parquet file {} due to error [{}]", self.parquet_path, e
            },
        })?;

        Ok(())
    }

    fn create_dir_name_cache(&self) -> Result<(), SyncerError> {
        fs::create_dir_all(&self.dir_name_cache).map_err(|e| SyncerError {
            msg: format! {
                "Error creating dir cache in path {} due to error [{}]", self.dir_name_cache, e
            },
        })?;

        Ok(())
    }

    fn create_dir_name_cache_tmp(&self) -> Result<(), SyncerError> {
        fs::create_dir_all(&self.dir_name_cache_tmp).map_err(|e| SyncerError {
            msg: format! {
                "Error creating dir cache in path {} due to error [{}]", self.dir_name_cache_tmp, e
            },
        })?;

        Ok(())
    }

    fn delete_parquet_file(&self) -> Result<(), SyncerError> {
        fs::remove_file(format!("{}/data.parquet", &self.dir_name_cache_tmp)).map_err(|e| {
            SyncerError {
                msg: format! {
                    "Error deleting parquet file in path {} due to error [{}]", self.parquet_path, e
                },
            }
        })?;

        Ok(())
    }

    fn file_lock(&self) -> Result<(), std::io::Error> {
        let mut file = LockFile::open(&self.parquet_path)?;
        file.lock()?;
        let options = dir::CopyOptions::new(); //Initialize default values for CopyOptions

        // move dir1 and file1.txt to target/dir1 and target/file1.txt
        let from_paths = vec![&self.parquet_path];
        let _result = fs_extra::move_items(&from_paths, &self.dir_name_cache_tmp, &options);
        let _put_parquet_file = put_parquet(
            format!("{}/{}", &self.dir_name_cache_tmp, "data.parquet"),
            format!("{}/{}", &self.dir_name_s3, &self.parquet_file),
        );
        file.unlock()?;

        Ok(())
    }
}

impl S3Sync {
    fn parquet_path_exists(&self) -> bool {
        let path = (&self.path).to_string();
        let new_path = utils::rem_first_and_last(&path);
        let new_parquet_path = format!("{}/{}", &new_path, "data.parquet");

        Path::new(&new_parquet_path).exists()
    }

    fn get_dir_name(&self) -> DirName {
        let new_path = utils::rem_first_and_last(&self.path);
        let cache_path = format!("{}/", &self.opt.local_disk_path);
        let _s3_path = format!("{}/", &self.opt.s3_bucket_name);
        let stream_names = str::replace(new_path, &cache_path, "");
        let new_parquet_path = format!("{}/{}", &new_path, "data.parquet");
        let dir_name_cache = format!(
            "{}{}/cache/date={}/hour={:02}",
            cache_path,
            stream_names,
            chrono::offset::Utc::now().date(),
            self.time.hour()
        );
        let dir_name_s3 = format!(
            "{}/date={}/hour={:02}",
            stream_names,
            chrono::offset::Utc::now().date(),
            self.time.hour(),
        );
        let parquet = format!(
            "data.{:02}.{:02}.parquet",
            self.time.hour(),
            self.time.minute()
        );
        let dir_name_cache_tmp = format!(
            "{}{}/cache/date={}/hour={:02}/tmp",
            cache_path,
            stream_names,
            chrono::offset::Utc::now().date(),
            self.time.hour()
        );

        DirName {
            dir_name_s3,
            dir_name_cache,
            dir_name_cache_tmp,
            parquet_path: new_parquet_path,
            parquet_file: parquet,
            stream_name: stream_names,
        }
    }
}

fn new_rb(stream_name: String) -> Result<(), crate::error::Error> {
    let rb = mem_store::MEM_STREAMS::get_rb(stream_name.clone())?;
    let sc = rb.schema();
    let new_rb = arrow::record_batch::RecordBatch::new_empty(sc);
    mem_store::MEM_STREAMS::put(
        stream_name.clone(),
        mem_store::LogStream {
            schema: Some(mem_store::MEM_STREAMS::get_schema(stream_name)),
            rb: Some(new_rb),
        },
    );

    Ok(())
}

#[tokio::main]
pub async fn put_parquet(key: String, path: String) -> Result<(), Error> {
    let opt = option::get_opts();
    let client = storage::setup_storage(&opt).client;
    let body = ByteStream::from_path(key).await;
    let _resp = client
        .put_object()
        .bucket(env::var("AWS_BUCKET_NAME").unwrap().to_string())
        .key(&path)
        .body(body.unwrap())
        .send()
        .await;
    println!("{:?}", _resp);

    Ok(())
}
