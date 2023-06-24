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

use std::{
    path::{Path, PathBuf},
    sync::Arc,
    time::Instant,
};

use async_trait::async_trait;
use bytes::Bytes;
use datafusion::{arrow::datatypes::Schema, prelude::col};
use datafusion::{
    datasource::{
        file_format::parquet::ParquetFormat,
        listing::{ListingOptions, ListingTable, ListingTableConfig, ListingTableUrl},
    },
    error::DataFusionError,
    execution::runtime_env::RuntimeConfig,
};
use fs_extra::file::{move_file, CopyOptions};
use futures::{stream::FuturesUnordered, TryStreamExt};
use relative_path::RelativePath;
use tokio::fs::{self, DirEntry};
use tokio_stream::wrappers::ReadDirStream;

use crate::{
    event::DEFAULT_TIMESTAMP_KEY,
    metrics::storage::{localfs::REQUEST_RESPONSE_TIME, StorageMetrics},
};
use crate::{option::validation, utils::validate_path_is_writeable};

use super::{object_storage, LogStream, ObjectStorage, ObjectStorageError, ObjectStorageProvider};

#[derive(Debug, Clone, clap::Args)]
#[command(
    name = "Local filesystem config",
    about = "Start Parseable with a drive as storage",
    help_template = "\
{about-section}
{all-args}
"
)]
pub struct FSConfig {
    #[arg(
        env = "P_FS_DIR",
        value_name = "filesystem path",
        default_value = "./data",
        value_parser = validation::canonicalize_path
    )]
    pub root: PathBuf,
}

impl ObjectStorageProvider for FSConfig {
    fn get_datafusion_runtime(&self) -> RuntimeConfig {
        RuntimeConfig::new()
    }

    fn get_object_store(&self) -> Arc<dyn ObjectStorage + Send> {
        Arc::new(LocalFS::new(self.root.clone()))
    }

    fn get_endpoint(&self) -> String {
        self.root.to_str().unwrap().to_string()
    }

    fn register_store_metrics(&self, handler: &actix_web_prometheus::PrometheusMetrics) {
        self.register_metrics(handler);
    }
}

pub struct LocalFS {
    root: PathBuf,
}

impl LocalFS {
    pub fn new(root: PathBuf) -> Self {
        Self { root }
    }

    pub fn path_in_root(&self, path: &RelativePath) -> PathBuf {
        path.to_path(&self.root)
    }
}

#[async_trait]
impl ObjectStorage for LocalFS {
    async fn get_object(&self, path: &RelativePath) -> Result<Bytes, ObjectStorageError> {
        let time = Instant::now();
        let file_path = self.path_in_root(path);
        let res: Result<Bytes, ObjectStorageError> = match fs::read(file_path).await {
            Ok(x) => Ok(x.into()),
            Err(e) => match e.kind() {
                std::io::ErrorKind::NotFound => {
                    Err(ObjectStorageError::NoSuchKey(path.to_string()))
                }
                _ => Err(ObjectStorageError::UnhandledError(Box::new(e))),
            },
        };

        let status = if res.is_ok() { "200" } else { "400" };
        let time = time.elapsed().as_secs_f64();
        REQUEST_RESPONSE_TIME
            .with_label_values(&["GET", status])
            .observe(time);
        res
    }

    async fn put_object(
        &self,
        path: &RelativePath,
        resource: Bytes,
    ) -> Result<(), ObjectStorageError> {
        let time = Instant::now();

        let path = self.path_in_root(path);
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent).await?;
        }
        let res = fs::write(path, resource).await;

        let status = if res.is_ok() { "200" } else { "400" };
        let time = time.elapsed().as_secs_f64();
        REQUEST_RESPONSE_TIME
            .with_label_values(&["PUT", status])
            .observe(time);

        res.map_err(Into::into)
    }

    async fn delete_prefix(&self, path: &RelativePath) -> Result<(), ObjectStorageError> {
        let path = self.path_in_root(path);
        tokio::fs::remove_dir_all(path).await?;
        Ok(())
    }

    async fn check(&self) -> Result<(), ObjectStorageError> {
        fs::create_dir_all(&self.root).await?;
        validate_path_is_writeable(&self.root)
            .map_err(|e| ObjectStorageError::UnhandledError(e.into()))
    }

    async fn delete_stream(&self, stream_name: &str) -> Result<(), ObjectStorageError> {
        let path = self.root.join(stream_name);
        Ok(fs::remove_dir_all(path).await?)
    }

    async fn list_streams(&self) -> Result<Vec<LogStream>, ObjectStorageError> {
        let ignore_dir = &["lost+found"];
        let directories = ReadDirStream::new(fs::read_dir(&self.root).await?);
        let entries: Vec<DirEntry> = directories.try_collect().await?;
        let entries = entries
            .into_iter()
            .map(|entry| dir_with_stream(entry, ignore_dir));

        let logstream_dirs: Vec<Option<String>> =
            FuturesUnordered::from_iter(entries).try_collect().await?;

        let logstreams = logstream_dirs
            .into_iter()
            .flatten()
            .map(|name| LogStream { name })
            .collect();

        Ok(logstreams)
    }

    async fn list_dates(&self, stream_name: &str) -> Result<Vec<String>, ObjectStorageError> {
        let path = self.root.join(stream_name);
        let directories = ReadDirStream::new(fs::read_dir(&path).await?);
        let entries: Vec<DirEntry> = directories.try_collect().await?;
        let entries = entries.into_iter().map(dir_name);
        let dates: Vec<_> = FuturesUnordered::from_iter(entries).try_collect().await?;

        Ok(dates.into_iter().flatten().collect())
    }

    async fn upload_file(&self, key: &str, path: &Path) -> Result<(), ObjectStorageError> {
        let op = CopyOptions {
            overwrite: true,
            skip_exist: true,
            ..CopyOptions::default()
        };
        let to_path = self.root.join(key);
        if let Some(path) = to_path.parent() {
            fs::create_dir_all(path).await?
        }
        let _ = move_file(path, to_path, &op)?;

        Ok(())
    }

    fn query_table(
        &self,
        prefixes: Vec<String>,
        schema: Arc<Schema>,
    ) -> Result<Option<ListingTable>, DataFusionError> {
        let prefixes: Vec<ListingTableUrl> = prefixes
            .into_iter()
            .filter_map(|prefix| {
                let path = self.root.join(prefix);
                ListingTableUrl::parse(path.to_str().unwrap()).ok()
            })
            .collect();

        if prefixes.is_empty() {
            return Ok(None);
        }

        let file_format = ParquetFormat::default().with_enable_pruning(Some(true));
        let listing_options = ListingOptions {
            file_extension: ".parquet".to_string(),
            file_sort_order: vec![vec![col(DEFAULT_TIMESTAMP_KEY).sort(true, false)]],
            infinite_source: false,
            format: Arc::new(file_format),
            table_partition_cols: vec![],
            collect_stat: true,
            target_partitions: 32,
        };

        let config = ListingTableConfig::new_with_multi_paths(prefixes)
            .with_listing_options(listing_options)
            .with_schema(schema);

        Ok(Some(ListingTable::try_new(config)?))
    }
}

async fn dir_with_stream(
    entry: DirEntry,
    ignore_dirs: &[&str],
) -> Result<Option<String>, ObjectStorageError> {
    let dir_name = entry
        .path()
        .file_name()
        .expect("valid path")
        .to_str()
        .expect("valid unicode")
        .to_owned();

    if ignore_dirs.contains(&dir_name.as_str()) {
        return Ok(None);
    }

    if entry.file_type().await?.is_dir() {
        let path = entry.path();
        let stream_json_path = path.join(object_storage::STREAM_METADATA_FILE_NAME);
        if stream_json_path.exists() {
            Ok(Some(dir_name))
        } else {
            let err: Box<dyn std::error::Error + Send + Sync + 'static> =
                format!("found {}", entry.path().display()).into();
            Err(ObjectStorageError::UnhandledError(err))
        }
    } else {
        Ok(None)
    }
}

async fn dir_name(entry: DirEntry) -> Result<Option<String>, ObjectStorageError> {
    if entry.file_type().await?.is_dir() {
        let dir_name = entry
            .path()
            .file_name()
            .expect("valid path")
            .to_str()
            .expect("valid unicode")
            .to_owned();
        Ok(Some(dir_name))
    } else {
        Ok(None)
    }
}

impl From<fs_extra::error::Error> for ObjectStorageError {
    fn from(e: fs_extra::error::Error) -> Self {
        ObjectStorageError::UnhandledError(Box::new(e))
    }
}
