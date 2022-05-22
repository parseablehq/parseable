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

use crate::mem_store;
use crate::option;
use crate::utils;
use crate::Error;
use arrow::record_batch::RecordBatch;
use std::path::Path;
use std::path::PathBuf;
use std::{fs, io};
use walkdir::WalkDir;

pub type DataDirPaths = Vec<PathBuf>;

pub fn load_memstore(opt: option::Opt) -> Result<(), Error> {
    // Check local data path and load streams and corresponding schema to
    // internal in-memory store
    if Path::new(&opt.local_disk_path).exists() {
        let entries = new_data_dir_paths(opt.clone())?;
        for entry in entries {
            let paths = new_dir_paths(entry, opt.clone()).ok_or(Error::MissingPath)?;
            let schema = fs::read_to_string(&paths.schema_path)?;
            if Path::new(&paths.parquet_path).exists() {
                let parquet_file = fs::File::open(&paths.parquet_path)?;
                let rb_reader = utils::convert_parquet_rb_reader(parquet_file)?;
                for rb in rb_reader {
                    mem_store::MEM_STREAMS::put(
                        paths.stream_name.clone(),
                        mem_store::LogStream {
                            schema: Some(schema.clone()),
                            rb: Some(rb?),
                        },
                    );
                }
            } else {
                for a in WalkDir::new(paths.path_cache_dir)
                    .follow_links(true)
                    .into_iter()
                    .filter_map(|e| e.ok())
                {
                    let f_name = a.file_name().to_string_lossy();

                    if f_name.ends_with(".parquet") {
                        let parquet_file = fs::File::open(a.path()).unwrap();
                        let rb_reader = utils::convert_parquet_rb_reader(parquet_file)?;

                        for rb in rb_reader {
                            mem_store::MEM_STREAMS::put(
                                paths.stream_name.clone(),
                                mem_store::LogStream {
                                    schema: Some(schema.clone()),
                                    rb: Some(RecordBatch::new_empty(rb?.schema())),
                                },
                            );
                        }
                    }
                }
            }
        }
    }

    Ok(())
}

pub fn new_data_dir_paths(opt: option::Opt) -> Result<DataDirPaths, io::Error> {
    let entries = fs::read_dir(&opt.local_disk_path)?
        .map(|res| res.map(|e| e.path()))
        .collect::<Result<Vec<_>, io::Error>>()?;

    Ok(entries)
}

#[derive(Debug)]
pub struct DirPaths {
    pub parquet_path: String,
    pub stream_name: String,
    pub schema_path: String,
    pub path_cache_dir: String,
}

pub fn new_dir_paths(path: std::path::PathBuf, opt: option::Opt) -> Option<DirPaths> {
    let new_path = utils::rem_first_and_last(path.to_str()?);
    let stream_vec: Vec<&str> = new_path.split('/').collect();

    Some(DirPaths {
        parquet_path: format!("{}/data.parquet", &new_path),
        stream_name: stream_vec[2].to_string(),
        schema_path: format!("{}/.schema", &new_path),
        path_cache_dir: format!("{}/{}", opt.local_disk_path, stream_vec[2]),
    })
}
