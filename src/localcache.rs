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

use fs_extra::file::CopyOptions;
use futures_util::TryFutureExt;
use hashlru::Cache;
use human_size::{Byte, Gigibyte, SpecificSize};
use itertools::{Either, Itertools};
use object_store::{local::LocalFileSystem, ObjectStore};
use once_cell::sync::OnceCell;
use parquet::errors::ParquetError;
use tokio::{fs, sync::Mutex};
use tracing::{error, info, warn};

use crate::{metadata::error::stream_info::MetadataError, option::CONFIG};

pub const STREAM_CACHE_FILENAME: &str = ".cache.json";
pub const CACHE_META_FILENAME: &str = ".cache_meta.json";
pub const CURRENT_CACHE_VERSION: &str = "v1";

#[derive(Debug, serde::Deserialize, serde::Serialize)]
pub struct LocalCache {
    version: String,
    current_size: u64,
    /// Mapping between storage path and cache path.
    files: Cache<String, PathBuf>,
}

impl LocalCache {
    fn new() -> Self {
        Self {
            version: CURRENT_CACHE_VERSION.to_string(),
            current_size: 0,
            files: Cache::new(100),
        }
    }
}

#[derive(Debug, serde::Deserialize, serde::Serialize)]
pub struct CacheMeta {
    version: String,
    size_capacity: u64,
}

impl CacheMeta {
    fn new() -> Self {
        Self {
            version: CURRENT_CACHE_VERSION.to_string(),
            size_capacity: 0,
        }
    }
}

pub struct LocalCacheManager {
    filesystem: LocalFileSystem,
    cache_path: PathBuf,
    cache_capacity: u64,
    copy_options: CopyOptions,
    semaphore: Mutex<()>,
}

impl LocalCacheManager {
    pub fn global() -> Option<&'static LocalCacheManager> {
        static INSTANCE: OnceCell<LocalCacheManager> = OnceCell::new();

        let cache_path = CONFIG.parseable.local_cache_path.as_ref()?;

        Some(INSTANCE.get_or_init(|| {
            let cache_path = cache_path.clone();
            std::fs::create_dir_all(&cache_path).unwrap();
            LocalCacheManager {
                filesystem: LocalFileSystem::new(),
                cache_path,
                cache_capacity: CONFIG.parseable.local_cache_size,
                copy_options: CopyOptions {
                    overwrite: true,
                    skip_exist: false,
                    ..CopyOptions::new()
                },
                semaphore: Mutex::new(()),
            }
        }))
    }

    pub async fn validate(&self, config_capacity: u64) -> Result<(), CacheError> {
        fs::create_dir_all(&self.cache_path).await?;
        let path = cache_meta_path(&self.cache_path)
            .map_err(|err| CacheError::ObjectStoreError(err.into()))?;
        let resp = self
            .filesystem
            .get(&path)
            .and_then(|resp| resp.bytes())
            .await;

        let updated_cache = match resp {
            Ok(bytes) => {
                let mut meta: CacheMeta = serde_json::from_slice(&bytes)?;
                if meta.size_capacity != config_capacity {
                    // log the change in cache size
                    let configured_size_human: SpecificSize<Gigibyte> =
                        SpecificSize::new(config_capacity as f64, Byte)
                            .unwrap()
                            .into();
                    let current_size_human: SpecificSize<Gigibyte> =
                        SpecificSize::new(meta.size_capacity as f64, Byte)
                            .unwrap()
                            .into();
                    warn!(
                        "Cache size is updated from {} to {}",
                        current_size_human, configured_size_human
                    );
                    meta.size_capacity = config_capacity;
                    Some(meta)
                } else {
                    None
                }
            }
            Err(object_store::Error::NotFound { .. }) => {
                let mut meta = CacheMeta::new();
                meta.size_capacity = config_capacity;
                Some(meta)
            }
            Err(err) => return Err(err.into()),
        };

        if let Some(updated_cache) = updated_cache {
            let result = self
                .filesystem
                .put(&path, serde_json::to_vec(&updated_cache)?.into())
                .await?;
            info!("Cache meta file updated: {:?}", result);
        }

        Ok(())
    }

    pub async fn get_cache(&self, stream: &str) -> Result<LocalCache, CacheError> {
        let path = cache_file_path(&self.cache_path, stream).unwrap();
        let res = self
            .filesystem
            .get(&path)
            .and_then(|resp| resp.bytes())
            .await;
        let cache = match res {
            Ok(bytes) => serde_json::from_slice(&bytes)?,
            Err(object_store::Error::NotFound { .. }) => LocalCache::new(),
            Err(err) => return Err(err.into()),
        };
        Ok(cache)
    }

    pub async fn put_cache(&self, stream: &str, cache: &LocalCache) -> Result<(), CacheError> {
        let path = cache_file_path(&self.cache_path, stream).unwrap();
        let bytes = serde_json::to_vec(cache)?.into();
        let result = self.filesystem.put(&path, bytes).await?;
        info!("Cache file updated: {:?}", result);
        Ok(())
    }

    pub async fn move_to_cache(
        &self,
        stream: &str,
        key: String,
        staging_path: PathBuf,
    ) -> Result<(), CacheError> {
        let lock = self.semaphore.lock().await;
        let mut cache_path = self.cache_path.join(stream);
        fs::create_dir_all(&cache_path).await?;
        cache_path.push(staging_path.file_name().unwrap());
        fs_extra::file::move_file(staging_path, &cache_path, &self.copy_options)?;
        let file_size = std::fs::metadata(&cache_path)?.len();
        let mut cache = self.get_cache(stream).await?;

        while cache.current_size + file_size > self.cache_capacity {
            if let Some((_, file_for_removal)) = cache.files.pop_lru() {
                let lru_file_size = std::fs::metadata(&file_for_removal)?.len();
                cache.current_size = cache.current_size.saturating_sub(lru_file_size);
                info!("removing cache entry");
                tokio::spawn(fs::remove_file(file_for_removal));
            } else {
                error!("Cache size too small");
                break;
            }
        }

        if cache.files.is_full() {
            cache.files.resize(cache.files.capacity() * 2);
        }
        cache.files.push(key, cache_path);
        cache.current_size += file_size;
        self.put_cache(stream, &cache).await?;
        drop(lock);
        Ok(())
    }

    pub async fn partition_on_cached<T>(
        &self,
        stream: &str,
        collection: Vec<T>,
        key: fn(&T) -> &String,
    ) -> Result<(Vec<(T, PathBuf)>, Vec<T>), CacheError> {
        let lock = self.semaphore.lock().await;
        let mut cache = self.get_cache(stream).await?;
        let (cached, remainder): (Vec<_>, Vec<_>) = collection.into_iter().partition_map(|item| {
            let key = key(&item);
            match cache.files.get(key).cloned() {
                Some(path) => Either::Left((item, path)),
                None => Either::Right(item),
            }
        });
        self.put_cache(stream, &cache).await?;
        drop(lock);
        Ok((cached, remainder))
    }
}

fn cache_file_path(
    root: impl AsRef<std::path::Path>,
    stream: &str,
) -> Result<object_store::path::Path, object_store::path::Error> {
    let mut path = root.as_ref().join(stream);
    path.push(STREAM_CACHE_FILENAME);
    object_store::path::Path::from_absolute_path(path)
}

fn cache_meta_path(
    root: impl AsRef<std::path::Path>,
) -> Result<object_store::path::Path, object_store::path::Error> {
    let path = root.as_ref().join(CACHE_META_FILENAME);
    object_store::path::Path::from_absolute_path(path)
}

#[derive(Debug, thiserror::Error)]
pub enum CacheError {
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
    #[error("Error: Cache File Does Not Exist")]
    DoesNotExist,
    #[error("Error: {0}")]
    Other(&'static str),
}
