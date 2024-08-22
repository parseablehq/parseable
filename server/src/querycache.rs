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

use arrow_array::RecordBatch;
use futures::TryStreamExt;
use futures_util::TryFutureExt;
use hashlru::Cache;
use human_size::{Byte, Gigibyte, SpecificSize};
use itertools::Itertools;
use object_store::{local::LocalFileSystem, ObjectStore};
use once_cell::sync::OnceCell;
use parquet::arrow::{AsyncArrowWriter, ParquetRecordBatchStreamBuilder};
use std::collections::{hash_map::DefaultHasher, HashMap};
use std::hash::{Hash, Hasher};
use std::path::{Path, PathBuf};
use tokio::fs as AsyncFs;
use tokio::{fs, sync::Mutex};

use crate::handlers::http::users::USERS_ROOT_DIR;
use crate::metadata::STREAM_INFO;
use crate::storage::staging::parquet_writer_props;
use crate::{localcache::CacheError, option::CONFIG};

pub const QUERY_CACHE_FILENAME: &str = ".cache.json";
pub const QUERY_CACHE_META_FILENAME: &str = ".cache_meta.json";
pub const CURRENT_QUERY_CACHE_VERSION: &str = "v1";

#[derive(Default, Clone, serde::Deserialize, serde::Serialize, Debug, Hash, Eq, PartialEq)]
pub struct CacheMetadata {
    pub query: String,
    pub start_time: String,
    pub end_time: String,
}

impl CacheMetadata {
    pub const fn new(query: String, start_time: String, end_time: String) -> Self {
        Self {
            query,
            start_time,
            end_time,
        }
    }
}

#[derive(Debug, serde::Deserialize, serde::Serialize)]
pub struct QueryCache {
    version: String,
    current_size: u64,

    /// Mapping between storage path and cache path.
    files: Cache<CacheMetadata, PathBuf>,
}

impl QueryCache {
    fn new() -> Self {
        Self {
            version: CURRENT_QUERY_CACHE_VERSION.to_string(),
            current_size: 0,
            files: Cache::new(100),
        }
    }

    pub fn get_file(&mut self, key: &CacheMetadata) -> Option<PathBuf> {
        self.files.get(key).cloned()
    }

    pub fn used_cache_size(&self) -> u64 {
        self.current_size
    }

    pub fn remove(&mut self, key: &CacheMetadata) -> Option<PathBuf> {
        self.files.remove(key)
    }

    pub async fn delete(&mut self, key: &CacheMetadata, path: PathBuf) -> Result<(), CacheError> {
        self.files.delete(key);
        AsyncFs::remove_file(path).await?;

        Ok(())
    }

    pub fn queries(&self) -> Vec<&CacheMetadata> {
        self.files.keys().collect_vec()
    }

    // read the parquet
    // return the recordbatches
    pub async fn get_cached_records(
        &self,
        path: &PathBuf,
    ) -> Result<(Vec<RecordBatch>, Vec<String>), CacheError> {
        let file = AsyncFs::File::open(path).await?;
        let builder = ParquetRecordBatchStreamBuilder::new(file).await?;
        // Build a async parquet reader.
        let stream = builder.build()?;

        let records = stream.try_collect::<Vec<_>>().await?;
        let fields = records.first().map_or_else(Vec::new, |record| {
            record
                .schema()
                .fields()
                .iter()
                .map(|field| field.name())
                .cloned()
                .collect_vec()
        });

        Ok((records, fields))
    }
}

// .cache_meta.json
#[derive(Debug, serde::Deserialize, serde::Serialize)]
pub struct QueryCacheMeta {
    version: String,
    size_capacity: u64,
}

impl QueryCacheMeta {
    fn new() -> Self {
        Self {
            version: CURRENT_QUERY_CACHE_VERSION.to_string(),
            size_capacity: 0,
        }
    }
}

pub struct QueryCacheManager {
    filesystem: LocalFileSystem,
    cache_path: PathBuf, // refers to the path passed in the env var
    total_cache_capacity: u64,
    semaphore: Mutex<()>,
}

impl QueryCacheManager {
    pub fn gen_file_path(query_staging_path: &str, start: &str, end: &str, query: &str) -> PathBuf {
        PathBuf::from_iter([
            query_staging_path,
            USERS_ROOT_DIR,
            &format!("{}.parquet", generate_hash(start, end, query)),
        ])
    }
    pub async fn global(config_capacity: u64) -> Result<&'static Self, CacheError> {
        static INSTANCE: OnceCell<QueryCacheManager> = OnceCell::new();

        let cache_manager = INSTANCE.get_or_init(|| {
            let cache_path = String::from("/query-cache");
            std::fs::create_dir_all(&cache_path).unwrap();
            Self {
                filesystem: LocalFileSystem::new(),
                cache_path: cache_path.into(),
                total_cache_capacity: CONFIG.parseable.query_cache_size,
                semaphore: Mutex::new(()),
            }
        });

        cache_manager.validate(config_capacity).await?;
        Ok(cache_manager)
    }

    async fn validate(&self, config_capacity: u64) -> Result<(), CacheError> {
        fs::create_dir_all(&self.cache_path).await?;
        let path = query_cache_meta_path(&self.cache_path)
            .map_err(|err| CacheError::ObjectStoreError(err.into()))?;
        let resp = self
            .filesystem
            .get(&path)
            .and_then(|resp| resp.bytes())
            .await;

        let updated_cache = match resp {
            Ok(bytes) => {
                let mut meta: QueryCacheMeta = serde_json::from_slice(&bytes)?;
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
                    log::warn!(
                        "Cache size is updated from {} to {}",
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
                let mut meta = QueryCacheMeta::new();
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
            log::info!("Cache meta file updated: {:?}", result);
        }

        Ok(())
    }

    pub async fn get_cache(
        &self,
        start: &str,
        end: &str,
        query: &str,
    ) -> Result<QueryCache, CacheError> {
        let path = query_cache_file_path(&self.cache_path, start, end, query).unwrap();
        let res = self
            .filesystem
            .get(&path)
            .and_then(|resp| resp.bytes())
            .await;
        let cache = match res {
            Ok(bytes) => serde_json::from_slice(&bytes)?,
            Err(object_store::Error::NotFound { .. }) => QueryCache::new(),
            Err(err) => return Err(err.into()),
        };
        Ok(cache)
    }

    pub async fn remove_from_cache(
        &self,
        key: CacheMetadata,
        start: &str,
        end: &str,
        query: &str,
    ) -> Result<(), CacheError> {
        let mut cache = self.get_cache(start, end, query).await?;
    
        if let Some(file_for_removal) = cache.remove(&key) {
            self.put_cache(start, end, query, &cache).await?;
            tokio::spawn(fs::remove_file(file_for_removal));            
            Ok(())
        } else {
            Err(CacheError::DoesNotExist)
        }
    }
    

    pub async fn put_cache(
        &self,
        start: &str,
        end: &str,
        query: &str,
        cache: &QueryCache,
    ) -> Result<(), CacheError> {
        let path = query_cache_file_path(&self.cache_path, start, end, query).unwrap();

        let bytes = serde_json::to_vec(cache)?.into();
        let result = self.filesystem.put(&path, bytes).await?;
        log::info!("Cache file updated: {:?}", result);
        Ok(())
    }

    pub async fn move_to_cache(
        &self,
        key: CacheMetadata,
        file_path: &Path,
        start: &str,
        end: &str,
        query: &str,
    ) -> Result<(), CacheError> {
        let lock = self.semaphore.lock().await;
        let file_size = std::fs::metadata(file_path)?.len();
        let mut cache = self.get_cache(start, end, query).await?;

        while cache.current_size + file_size > self.total_cache_capacity {
            if let Some((_, file_for_removal)) = cache.files.pop_lru() {
                let lru_file_size = fs::metadata(&file_for_removal).await?.len();
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
        cache.files.push(key, file_path.to_path_buf());
        cache.current_size += file_size;
        self.put_cache(start, end, query, &cache).await?;
        drop(lock);
        Ok(())
    }

    pub async fn create_parquet_cache(
        &self,
        table_name: &str,
        records: &[RecordBatch],
        start: &str,
        end: &str,
        query: &str,
    ) -> Result<(), CacheError> {
        let parquet_path = Self::gen_file_path(
            self.cache_path.to_str().expect("utf-8 compat path"),
            &start,
            &end,
            &query,
        );
        AsyncFs::create_dir_all(parquet_path.parent().expect("parent path exists")).await?;
        let parquet_file = AsyncFs::File::create(&parquet_path).await?;
        let time_partition = STREAM_INFO.get_time_partition(table_name)?;
        let props = parquet_writer_props(time_partition.clone(), 0, HashMap::new()).build();

        let sch = if let Some(record) = records.first() {
            record.schema()
        } else {
            // the record batch is empty, do not cache and return early
            return Ok(());
        };

        let mut arrow_writer = AsyncArrowWriter::try_new(parquet_file, sch, Some(props))?;

        for record in records {
            if let Err(e) = arrow_writer.write(record).await {
                log::error!("Error While Writing to Query Cache: {}", e);
            }
        }

        arrow_writer.close().await?;
        self.move_to_cache(
            CacheMetadata::new(query.to_string(), start.to_string(), end.to_string()),
            &parquet_path,
            &start,
            &end,
            &query,
        )
        .await
    }

    pub async fn clear_cache(&self, start: &str, end: &str, query: &str) -> Result<(), CacheError> {
        // Generate the hash using start, end, and query
        let hash = generate_hash(start, end, query);

        // Construct the path to the cache directory
        let p_path = self.cache_path.join(format!("{}", hash));

        // Get the list of files in the cache directory
        let mut paths = fs::read_dir(&p_path).await?;

        while let Some(entry) = paths.next_entry().await? {
            let path = entry.path();

            // Check if the file should be deleted
            if path.is_file()
                && !path
                    .file_name()
                    .expect("File Name is Proper")
                    .to_str()
                    .expect("Path is Proper utf-8")
                    .ends_with(".json")
            {
                // Remove the file asynchronously
                fs::remove_file(path).await?;
            }
        }

        Ok(())
    }
}

pub fn generate_hash(start: &str, end: &str, query: &str) -> u64 {
    let mut hasher = DefaultHasher::new();
    start.hash(&mut hasher);
    end.hash(&mut hasher);
    query.hash(&mut hasher);
    hasher.finish()
}

fn query_cache_file_path(
    root: impl AsRef<std::path::Path>,
    start: &str,
    end: &str,
    query: &str,
) -> Result<object_store::path::Path, object_store::path::Error> {
    let mut path = root.as_ref().to_path_buf();
    path.push(format!("{}.parquet", generate_hash(start, end, query)));
    object_store::path::Path::from_absolute_path(path)
}

fn query_cache_meta_path(
    root: impl AsRef<std::path::Path>,
) -> Result<object_store::path::Path, object_store::path::Error> {
    let path = root.as_ref().join(QUERY_CACHE_META_FILENAME);
    object_store::path::Path::from_absolute_path(path)
}
