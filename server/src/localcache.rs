/*
 * Parseable Server (C) 2022 - 2023 Parseable, Inc.
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

use std::{io, path::PathBuf, sync::Arc};

use fs_extra::file::CopyOptions;
use futures_util::TryFutureExt;
use hashlru::Cache;
use itertools::{Either, Itertools};
use object_store::ObjectStore;
use once_cell::sync::OnceCell;
use tokio::{fs, sync::Mutex};

use crate::option::CONFIG;

pub const STREAM_CACHE_FILENAME: &str = ".cache.json";

#[derive(Debug, serde::Deserialize, serde::Serialize)]
pub struct LocalCache {
    version: String,
    current_size: u64,
    capacity: u64,
    /// Mapping between storage path and cache path.
    files: Cache<String, PathBuf>,
}

impl LocalCache {
    fn new_with_size(capacity: u64) -> Self {
        Self {
            version: "v1".to_string(),
            current_size: 0,
            capacity,
            files: Cache::new(100),
        }
    }

    fn can_push(&self, size: u64) -> bool {
        self.capacity >= self.current_size + size
    }
}

pub struct LocalCacheManager {
    object_store: Arc<dyn ObjectStore>,
    cache_path: PathBuf,
    cache_capacity: u64,
    copy_options: CopyOptions,
    semaphore: Mutex<()>,
}

impl LocalCacheManager {
    pub fn global() -> Option<&'static LocalCacheManager> {
        static INSTANCE: OnceCell<LocalCacheManager> = OnceCell::new();

        let Some(cache_path) = &CONFIG.parseable.local_cache_path else {
            return None;
        };

        Some(INSTANCE.get_or_init(|| {
            let cache_path = cache_path.clone();
            std::fs::create_dir_all(&cache_path).unwrap();
            let object_store = Arc::new(object_store::local::LocalFileSystem::new());
            LocalCacheManager {
                object_store,
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

    pub async fn get_cache(&self, stream: &str) -> Result<LocalCache, CacheError> {
        let path = cache_file_path(&self.cache_path, stream).unwrap();
        let res = self
            .object_store
            .get(&path)
            .and_then(|resp| resp.bytes())
            .await;
        let cache = match res {
            Ok(bytes) => serde_json::from_slice(&bytes)?,
            Err(object_store::Error::NotFound { .. }) => {
                LocalCache::new_with_size(self.cache_capacity)
            }
            Err(err) => return Err(err.into()),
        };
        Ok(cache)
    }

    pub async fn put_cache(&self, stream: &str, cache: &LocalCache) -> Result<(), CacheError> {
        let path = cache_file_path(&self.cache_path, stream).unwrap();
        let bytes = serde_json::to_vec(cache)?.into();
        Ok(self.object_store.put(&path, bytes).await?)
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

        while !cache.can_push(file_size) {
            if let Some((_, file_for_removal)) = cache.files.pop_lru() {
                let lru_file_size = std::fs::metadata(&file_for_removal)?.len();
                cache.current_size = cache.current_size.saturating_sub(lru_file_size);
                log::info!("removing cache entry");
                tokio::spawn(fs::remove_file(file_for_removal));
            } else {
                log::error!("Cache size too small");
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
    path.set_file_name(STREAM_CACHE_FILENAME);
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
}
