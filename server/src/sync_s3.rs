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
use crate::option;
use crate::utils;
use chrono::{Timelike, Utc};
use derive_more::Display;
use std::env;
use std::fs;
use std::io;
use std::path::Path;
use std::time::Duration;
extern crate fs_extra;
use crate::storage;
use aws_smithy_http::byte_stream::ByteStream;

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
    dir_name_tmp: String,
    dir_name_local: String,
    parquet_path: String,
    parquet_file: String,
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
        if let Err(error) = self.create_dir_name_tmp() {
            return Some(error);
        }

        if let Err(error) = self.copy_parquet_to_tmp() {
            return Some(error);
        }

        if let Err(error) = self.delete_parquet_file() {
            return Some(error);
        }

        None
    }

    fn copy_parquet_to_tmp(&self) -> Result<(), SyncerError> {
        // TODO: retries to s3
        let _put_parquet_file = put_parquet(
            format!("{}/{}", &self.dir_name_local, "data.parquet"),
            format!("{}/{}", &self.dir_name_s3, &self.parquet_file),
        );

        fs::copy(
            format!("{}/{}", &self.dir_name_local, "data.parquet"),
            format!("{}/{}", &self.dir_name_tmp, &self.parquet_file),
        )
        .map_err(|e| SyncerError {
            msg: format! {
                "Error copy parquet file {} due to error [{}]", self.parquet_path, e
            },
        })?;

        Ok(())
    }

    fn create_dir_name_tmp(&self) -> Result<(), SyncerError> {
        fs::create_dir_all(&self.dir_name_tmp).map_err(|e| SyncerError {
            msg: format! {
                "Error creating dir tmp in path {} due to error [{}]", self.dir_name_local, e
            },
        })?;

        Ok(())
    }

    fn delete_parquet_file(&self) -> Result<(), SyncerError> {
        fs::remove_file(format!("{}/data.parquet", &self.dir_name_local)).map_err(|e| {
            SyncerError {
                msg: format! {
                    "Error deleting parquet file in path {} due to error [{}]", self.parquet_path, e
                },
            }
        })?;

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
        let local_path = format!("{}/", &self.opt.local_disk_path);
        let _s3_path = format!("{}/", &self.opt.s3_bucket_name);
        let stream_names = str::replace(new_path, &local_path, "");
        let new_parquet_path = format!("{}/{}", &new_path, "data.parquet");

        let dir_name_tmp = format!(
            "{}{}/tmp/date={}/hour={:02}/minute={}",
            local_path,
            stream_names,
            chrono::offset::Utc::now().date(),
            self.time.hour(),
            time_slot(),
        );

        let dir_name_s3 = format!(
            "{}/date={}/hour={:02}/minute={}",
            stream_names,
            chrono::offset::Utc::now().date(),
            self.time.hour(),
            time_slot()
        );

        let parquet = format!("{}.parquet", utils::random_string());
        let dir_name_local = format!("{}{}", local_path, stream_names,);

        DirName {
            dir_name_s3,
            dir_name_tmp,
            dir_name_local,
            parquet_path: new_parquet_path,
            parquet_file: parquet,
        }
    }
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

pub fn time_slot() -> String {
    let t = chrono::offset::Utc::now().minute();
    if t > 00 && t < 10 {
        return "00-09".to_string();
    } else if t > 9 && t < 19 {
        return "10-19".to_string();
    } else if t > 19 && t < 29 {
        return "20-29".to_string();
    } else if t > 29 && t < 39 {
        return "30-39".to_string();
    } else if t > 39 && t < 49 {
        return "40-49".to_string();
    } else if t > 49 && t < 59 {
        return "49-59".to_string();
    };
    "".to_string()
}
