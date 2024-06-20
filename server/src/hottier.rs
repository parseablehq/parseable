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

use std::{io, path::PathBuf};

use crate::{metadata::error::stream_info::MetadataError, option::CONFIG};
use fs_extra::file::CopyOptions;
use futures::TryStreamExt;
use futures_util::TryFutureExt;
use hashlru::Cache;
use human_size::{Byte, Gigibyte, SpecificSize};
use itertools::{Either, Itertools};
use object_store::{local::LocalFileSystem, ObjectStore};
use once_cell::sync::OnceCell;
use parquet::errors::ParquetError;
use tokio::{fs, sync::Mutex};
use arrow_array::RecordBatch;
use parquet::arrow::ParquetRecordBatchStreamBuilder;

pub const STREAM_HOT_TIER_FILENAME: &str = ".hot_tier.json";
pub const HOT_TIER_META_FILENAME: &str = ".hot_tier_meta.json";
pub const CURRENT_HOT_TIER_VERSION: &str = "v1";

#[derive(Debug, serde::Deserialize, serde::Serialize)]
pub struct LocalHotTier {
    version: String,
    current_size: u64,
    /// Mapping between storage path and cache path.
    files: Cache<String, PathBuf>,
}

impl LocalHotTier {
    fn new() -> Self {
        Self {
            version: CURRENT_HOT_TIER_VERSION.to_string(),
            current_size: 0,
            files: Cache::new(100),
        }
    }
}

#[derive(Debug, serde::Deserialize, serde::Serialize)]
pub struct HotTierMeta {
    version: String,
    size_capacity: u64,
}

impl HotTierMeta {
    fn new() -> Self {
        Self {
            version: CURRENT_HOT_TIER_VERSION.to_string(),
            size_capacity: 0,
        }
    }
}

pub struct LocalHotTierManager {
    filesystem: LocalFileSystem,
    hot_tier_path: PathBuf,
    hot_tier_size: u64,
    copy_options: CopyOptions,
    semaphore: Mutex<()>,
}

impl LocalHotTierManager {
    pub fn global() -> Option<&'static LocalHotTierManager> {
        static INSTANCE: OnceCell<LocalHotTierManager> = OnceCell::new();

        let hot_tier_path = CONFIG.parseable.hot_tier_storage_path.as_ref()?;

        Some(INSTANCE.get_or_init(|| {
            let hot_tier_path = hot_tier_path.clone();
            std::fs::create_dir_all(&hot_tier_path).unwrap();
            LocalHotTierManager {
                filesystem: LocalFileSystem::new(),
                hot_tier_path,
                hot_tier_size: CONFIG.parseable.hot_tier_size,
                copy_options: CopyOptions {
                    overwrite: true,
                    skip_exist: false,
                    ..CopyOptions::new()
                },
                semaphore: Mutex::new(()),
            }
        }))
    }

    pub async fn validate(&self, config_capacity: u64) -> Result<(), HotTierError> {
        fs::create_dir_all(&self.hot_tier_path).await?;
        let path = hot_tier_meta_path(&self.hot_tier_path)
            .map_err(|err| HotTierError::ObjectStoreError(err.into()))?;
        let resp = self
            .filesystem
            .get(&path)
            .and_then(|resp| resp.bytes())
            .await;

        let updated_hot_tier = match resp {
            Ok(bytes) => {
                let mut meta: HotTierMeta = serde_json::from_slice(&bytes)?;
                if meta.size_capacity != config_capacity {
                    // log the change in hot tier size
                    let configured_size_human: SpecificSize<Gigibyte> =
                        SpecificSize::new(config_capacity as f64, Byte)
                            .unwrap()
                            .into();
                    let current_size_human: SpecificSize<Gigibyte> =
                        SpecificSize::new(meta.size_capacity as f64, Byte)
                            .unwrap()
                            .into();
                    log::warn!(
                        "Hot tier size is updated from {} to {}",
                        current_size_human,
                        configured_size_human
                    );
                    meta.size_capacity = config_capacity;
                    Some(meta)
                } else {
                    None
                }
            }
            Err(object_store::Error::NotFound { .. }) => {
                let mut meta = HotTierMeta::new();
                meta.size_capacity = config_capacity;
                Some(meta)
            }
            Err(err) => return Err(err.into()),
        };

        if let Some(updated_hot_tier) = updated_hot_tier {
            let result = self
                .filesystem
                .put(&path, serde_json::to_vec(&updated_hot_tier)?.into())
                .await?;
            log::info!("HotTier meta file updated: {:?}", result);
        }

        Ok(())
    }

    pub async fn get_hot_tier(&self, stream: &str) -> Result<LocalHotTier, HotTierError> {
        let path = hot_tier_file_path(&self.hot_tier_path, stream).unwrap();
        let res = self
            .filesystem
            .get(&path)
            .and_then(|resp| resp.bytes())
            .await;
        let hot_tier = match res {
            Ok(bytes) => serde_json::from_slice(&bytes)?,
            Err(object_store::Error::NotFound { .. }) => LocalHotTier::new(),
            Err(err) => return Err(err.into()),
        };
        Ok(hot_tier)
    }

    pub async fn put_hot_tier(
        &self,
        stream: &str,
        hot_tier: &LocalHotTier,
    ) -> Result<(), HotTierError> {
        let path = hot_tier_file_path(&self.hot_tier_path, stream).unwrap();
        let bytes = serde_json::to_vec(hot_tier)?.into();
        let result = self.filesystem.put(&path, bytes).await?;
        log::info!("Hot tier file updated: {:?}", result);
        Ok(())
    }

    pub async fn move_to_hot_tier(
        &self,
        stream: &str,
        key: String,
        staging_path: PathBuf,
    ) -> Result<(), HotTierError> {
        let lock = self.semaphore.lock().await;
        let mut hot_tier_path = self.hot_tier_path.join(stream);
        fs::create_dir_all(&hot_tier_path).await?;
        hot_tier_path.push(staging_path.file_name().unwrap());
        fs_extra::file::move_file(staging_path, &hot_tier_path, &self.copy_options)?;
        let file_size = std::fs::metadata(&hot_tier_path)?.len();
        let mut hot_tier = self.get_hot_tier(stream).await?;

        while hot_tier.current_size + file_size > self.hot_tier_size {
            if let Some((_, file_for_removal)) = hot_tier.files.pop_lru() {
                let lru_file_size = std::fs::metadata(&file_for_removal)?.len();
                hot_tier.current_size = hot_tier.current_size.saturating_sub(lru_file_size);
                log::info!("removing hot tier entry");
                tokio::spawn(fs::remove_file(file_for_removal));
            } else {
                log::error!("hot tier size too small");
                break;
            }
        }

        if hot_tier.files.is_full() {
            hot_tier.files.resize(hot_tier.files.capacity() * 2);
        }
        hot_tier.files.push(key, hot_tier_path);
        hot_tier.current_size += file_size;
        self.put_hot_tier(stream, &hot_tier).await?;
        drop(lock);
        Ok(())
    }

    pub async fn delete_from_hot_tier(
        &self,
        stream: &str,
        hot_tier_path: PathBuf,
    ) -> Result<(), HotTierError> {
        let file_size = std::fs::metadata(&hot_tier_path)?.len();
        let mut hot_tier = self.get_hot_tier(stream).await?;
        hot_tier.current_size = hot_tier.current_size.saturating_sub(file_size);
        if let Err(err) = std::fs::remove_file(&hot_tier_path) {
            log::error!("Failed to remove file: {:?}", err);
        }
        // hot_tier.files.
        let hot_tier_files = &mut hot_tier.files;
        let hot_tier_file = hot_tier_files.iter().find(|(_, v)| **v == hot_tier_path);
        if hot_tier_file.is_some() {
            let key = hot_tier_file.unwrap().0.clone();
            hot_tier_files.delete(&key);
        }
        self.put_hot_tier(stream, &hot_tier).await?;
        Ok(())
    }

    // read the parquet
    // return the recordbatches
    pub async fn get_hottier_records(
        &self,
        path: &PathBuf,
    ) -> Result<(Vec<RecordBatch>, Vec<String>), HotTierError> {
        let mut fetched_records: Vec<RecordBatch> = vec![];
        let mut fields: Vec<String> = vec![];
        let files = std::fs::read_dir(path).unwrap().collect::<Vec<_>>();
        println!("Files: {:?}", files);
        for file in files.iter(){
            match file{
                Err(err) => {
                    log::error!("Failed to read file: {:?}", err);
                    continue;
                }
                Ok(file)=>{
                    if file.path().extension().expect("should have an extension") == "parquet" {
                        let file_name = file
                            .file_name()
                            .to_str()
                            .expect("should be valid str")
                            .to_owned();
                        println!("File Name: {:?}", file_name);
                        let parquet_file = tokio::fs::File::open(file_name).await?;
                        let builder = ParquetRecordBatchStreamBuilder::new(parquet_file).await?;
                        // Build a async parquet reader.
                        let stream = builder.build()?;
                        let records = stream.try_collect::<Vec<_>>().await?;
                        fetched_records.extend(records);
                        fields = fetched_records.first().map_or_else(Vec::new, |record| {
                            record
                                .schema()
                                .fields()
                                .iter()
                                .map(|field| field.name())
                                .cloned()
                                .collect_vec()
                        });
                    }
                }
            }
        }
        

        Ok((fetched_records, fields))
    }

    pub async fn partition_on_cached<T>(
        &self,
        stream: &str,
        collection: Vec<T>,
        key: fn(&T) -> &String,
    ) -> Result<(Vec<(T, PathBuf)>, Vec<T>), HotTierError> {
        let lock = self.semaphore.lock().await;
        let mut hot_tier = self.get_hot_tier(stream).await?;
        let (cached, remainder): (Vec<_>, Vec<_>) = collection.into_iter().partition_map(|item| {
            let key = key(&item);
            match hot_tier.files.get(key).cloned() {
                Some(path) => Either::Left((item, path)),
                None => Either::Right(item),
            }
        });
        self.put_hot_tier(stream, &hot_tier).await?;
        drop(lock);
        Ok((cached, remainder))
    }
}

pub fn hot_tier_file_path(
    root: impl AsRef<std::path::Path>,
    stream: &str,
) -> Result<object_store::path::Path, object_store::path::Error> {
    let mut path = root.as_ref().join(stream);
    path.push(STREAM_HOT_TIER_FILENAME);
    object_store::path::Path::from_absolute_path(path)
}

fn hot_tier_meta_path(
    root: impl AsRef<std::path::Path>,
) -> Result<object_store::path::Path, object_store::path::Error> {
    let path = root.as_ref().join(HOT_TIER_META_FILENAME);
    object_store::path::Path::from_absolute_path(path)
}

#[derive(Debug, thiserror::Error)]
pub enum HotTierError {
    #[error("{0}")]
    Serde(#[from] serde_json::Error),
    #[error("{0}")]
    IOError(#[from] io::Error),
    #[error("{0}")]
    MoveError(#[from] fs_extra::error::Error),
    #[error("{0}")]
    ObjectStoreError(#[from] object_store::Error),
    #[error("{0}")]
    ParquetError(#[from] ParquetError),
    #[error("{0}")]
    MetadataError(#[from] MetadataError),
}
