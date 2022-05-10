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
use actix_web::Error;
use arrow::record_batch::RecordBatch;
use std::path::Path;
use std::{fs, io};
use walkdir::WalkDir;

pub fn load_memstore(opt: option::Opt) -> anyhow::Result<()> {
    // Check local data path and load streams and corresponding schema to
    // internal in-memory store
    if Path::new(&opt.local_disk_path).exists() {
        let entries = new_data_dir_paths(opt.clone());
        for entry in entries.unwrap().entries {
            let paths = new_dir_paths(entry, opt.clone());
            if Path::new(&paths.parquet_path).exists() {
                let parquet_file = fs::File::open(&paths.parquet_path).unwrap();
                let rb_reader = utils::convert_parquet_rb_reader(parquet_file);
                for rb in rb_reader {
                    mem_store::MEM_STREAMS::put(
                        paths.stream_name.clone(),
                        mem_store::Stream {
                            schema: Some(fs::read_to_string(&paths.schema_path)?.parse()?),
                            rb: Some(rb.unwrap()),
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
                        let rb_reader = utils::convert_parquet_rb_reader(parquet_file);

                        for rb in rb_reader {
                            let sc = rb.unwrap();
                            mem_store::MEM_STREAMS::put(
                                paths.stream_name.clone(),
                                mem_store::Stream {
                                    schema: Some(
                                        fs::read_to_string(&paths.schema_path.clone())?.parse()?,
                                    ),
                                    rb: Some(RecordBatch::new_empty(sc.schema())),
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

#[derive(Debug)]
pub struct DataDirPaths {
    pub entries: std::vec::Vec<std::path::PathBuf>,
}

pub fn new_data_dir_paths(opt: option::Opt) -> Result<DataDirPaths, Error> {
    let entries = fs::read_dir(&opt.local_disk_path)?
        .map(|res| res.map(|e| e.path()))
        .collect::<Result<Vec<_>, io::Error>>()?;
    Ok(DataDirPaths { entries })
}

#[derive(Debug)]
pub struct DirPaths {
    pub parquet_path: String,
    pub stream_name: String,
    pub schema_path: String,
    pub path_cache_dir: String,
}

pub fn new_dir_paths(paths: std::path::PathBuf, opt: option::Opt) -> DirPaths {
    let path = format!("{:?}", paths);
    let new_path = utils::rem_first_and_last(&path);
    let stream_vec: Vec<&str> = new_path.split('/').collect();
    return DirPaths {
        parquet_path: format!("{}/{}", &new_path, "data.parquet"),
        stream_name: stream_vec[2].to_string(),
        schema_path: format!("{}/{}", &new_path, ".schema"),
        path_cache_dir: format!("{}/{}", opt.local_disk_path, stream_vec[2]),
    };
}
