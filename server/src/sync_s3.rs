use crate::mem_store;
use crate::option;
use crate::utils;
use actix_web::Error;
use chrono::{Timelike, Utc};
use derive_more::{Display, Error};
use fslock::LockFile;
use std::env;
use std::fs;
use std::io;
use std::path::Path;
use std::time::Duration;
extern crate fs_extra;
use crate::storage;
use fs_extra::dir;

pub fn syncer(opt: option::Opt) -> Result<bool, Error> {
    if Path::new(&opt.local_disk_path).exists() {
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
                    let ten_min = Duration::new(600, 0);
                    if time.elapsed().unwrap() > ten_min {
                        let local_ops = dir.local_ops();
                        if let Some(x) = local_ops {
                            x.log_syncer_error()
                        }
                        new_rb(dir.stream_name);
                    }
                }
            }
        }
    }
    Ok(true)
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

#[derive(Debug, Display, Error, PartialEq)]
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
        let create_dir_name_cache = self.create_dir_name_cache();
        let _ = match create_dir_name_cache {
            Ok(_) => {}
            Err(error) => return Some(error),
        };

        let _create_dir_name_cache_tmp = self.create_dir_name_cache_tmp();
        let _ = match create_dir_name_cache {
            Ok(_) => {}
            Err(error) => return Some(error),
        };

        let _copy_parquet_to_cache = self.file_lock();
        // let _ = match copy_parquet_to_cache {
        //     Ok(_) => {}
        //     Err(error) => return Some(error),
        // };

        let copy_parquet_to_cache = self.copy_parquet_to_cache();
        let _ = match copy_parquet_to_cache {
            Ok(_) => {}
            Err(error) => return Some(error),
        };

        let delete_parquet_file = self.delete_parquet_file();
        let _ = match delete_parquet_file {
            Ok(_) => {}
            Err(error) => return Some(error),
        };
        None
    }

    fn copy_parquet_to_cache(&self) -> Result<bool, SyncerError> {
        let copy_file = fs::copy(
            format!("{}/{}", &self.dir_name_cache_tmp, "data.parquet"),
            format!("{}/{}", &self.dir_name_cache, &self.parquet_file),
        );
        // println!("{}", format!("{}/{}", &self.dir_name_cache_tmp, &self.parquet_file));
        // println!("{}",format!("{}/{}", &self.dir_name_cache, &self.parquet_file));
        let _ = match copy_file {
            Ok(_) => return Ok(true),
            Err(error) => {
                return Err(SyncerError {
                    msg: format! {
                        "Error copy parquet file {} due to error [{}]", self.parquet_path, error
                    },
                })
            }
        };
    }

    fn create_dir_name_cache(&self) -> Result<bool, SyncerError> {
        let dir_name_cache = fs::create_dir_all(&self.dir_name_cache);
        let _ = match dir_name_cache {
            Ok(_) => return Ok(true),
            Err(error) => {
                return Err(SyncerError {
                    msg: format! {
                        "Error creating dir cache in path {} due to error [{}]", self.dir_name_cache, error
                    },
                })
            }
        };
    }

    fn create_dir_name_cache_tmp(&self) -> Result<bool, SyncerError> {
        let dir_name_cache = fs::create_dir_all(&self.dir_name_cache_tmp);
        let _ = match dir_name_cache {
            Ok(_) => return Ok(true),
            Err(error) => {
                return Err(SyncerError {
                    msg: format! {
                        "Error creating dir cache in path {} due to error [{}]", self.dir_name_cache_tmp, error
                    },
                })
            }
        };
    }

    fn delete_parquet_file(&self) -> Result<bool, SyncerError> {
        let delete_file = fs::remove_file(format!("{}/data.parquet", &self.dir_name_cache_tmp));
        let _ = match delete_file {
            Ok(_) => return Ok(true),
            Err(error) => {
                return Err(SyncerError {
                    msg: format! {
                        "Error deleting parquet file in path {} due to error [{}]", self.parquet_path, error
                    },
                })
            }
        };
    }

    fn file_lock(&self) -> Result<(), std::io::Error> {
        let mut file = LockFile::open(&self.parquet_path)?;
        file.lock()?;
        let options = dir::CopyOptions::new(); //Initialize default values for CopyOptions

        // move dir1 and file1.txt to target/dir1 and target/file1.txt
        let from_paths = vec![&self.parquet_path];
        let _result = fs_extra::move_items(&from_paths, &self.dir_name_cache_tmp, &options);
        let _put_parquet_file = put_parquet(
            format!("{}/{}", &self.dir_name_s3, "data.parquet"),
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
        return Path::new(&new_parquet_path).exists();
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

fn new_rb(stream_name: String) {
    let rb = mem_store::MEM_STREAMS::get_rb(stream_name.clone());
    let sc = rb.schema();
    let new_rb = arrow::record_batch::RecordBatch::new_empty(sc);
    mem_store::MEM_STREAMS::put(
        stream_name.clone(),
        mem_store::Stream {
            schema: Some(mem_store::MEM_STREAMS::get_schema(stream_name)),
            rb: Some(new_rb),
        },
    );
}

#[tokio::main]
pub async fn put_parquet(key: String, path: String) -> Result<(), Error> {
    let opt = option::get_opts();
    let client = storage::setup_storage(&opt).client;
    let _resp = client
        .put_object()
        .bucket(env::var("AWS_BUCKET_NAME").unwrap().to_string())
        .key(&path)
        .body(key.into_bytes().into())
        .send()
        .await;
    println!("{:?}", _resp);
    Ok(())
}
