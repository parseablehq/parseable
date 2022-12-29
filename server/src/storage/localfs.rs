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
};

use async_trait::async_trait;
use bytes::Bytes;
use datafusion::{
    datasource::{
        file_format::parquet::ParquetFormat,
        listing::{ListingOptions, ListingTable, ListingTableConfig, ListingTableUrl},
    },
    execution::runtime_env::{RuntimeConfig, RuntimeEnv},
};
use fs_extra::file::{move_file, CopyOptions};
use futures::StreamExt;
use relative_path::RelativePath;
use tokio::fs;
use tokio_stream::wrappers::ReadDirStream;

use crate::query::Query;

use super::{LogStream, ObjectStorage, ObjectStorageError, ObjectStorageProvider};

#[derive(Debug, Clone, clap::Args)]
#[command(
    name = "Local filesystem config",
    about = "Start Parseable with local filesystem as storage backend (non production use only)",
    help_template = "\
{about-section}
{all-args}
"
)]
pub struct FSConfig {
    #[arg(env = "P_FS_PATH", value_name = "filesystem path", required = true)]
    root: PathBuf,
}

impl ObjectStorageProvider for FSConfig {
    fn get_datafusion_runtime(&self) -> Arc<RuntimeEnv> {
        let config = RuntimeConfig::new();
        let runtime = RuntimeEnv::new(config).unwrap();
        Arc::new(runtime)
    }

    fn get_object_store(&self) -> Arc<dyn ObjectStorage + Send> {
        Arc::new(LocalFS::new(self.root.clone()))
    }

    fn get_endpoint(&self) -> String {
        self.root.to_str().unwrap().to_string()
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
        let file_path = self.path_in_root(path);
        match fs::read(file_path).await {
            Ok(x) => Ok(x.into()),
            Err(e) => match e.kind() {
                std::io::ErrorKind::NotFound => {
                    Err(ObjectStorageError::NoSuchKey(path.to_string()))
                }
                _ => Err(ObjectStorageError::UnhandledError(Box::new(e))),
            },
        }
    }

    async fn put_object(
        &self,
        path: &RelativePath,
        resource: Bytes,
    ) -> Result<(), ObjectStorageError> {
        let path = self.path_in_root(path);
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent).await?;
        }
        Ok(fs::write(path, resource).await?)
    }

    async fn check(&self) -> Result<(), ObjectStorageError> {
        Ok(fs::create_dir_all(&self.root).await?)
    }

    async fn delete_stream(&self, stream_name: &str) -> Result<(), ObjectStorageError> {
        let path = self.root.join(stream_name);
        Ok(fs::remove_dir_all(path).await?)
    }
    async fn list_streams(&self) -> Result<Vec<LogStream>, ObjectStorageError> {
        let directories = ReadDirStream::new(fs::read_dir(&self.root).await?);
        let directories = directories
            .filter_map(|res| async {
                let entry = res.ok()?;
                if entry.file_type().await.ok()?.is_dir() {
                    Some(LogStream {
                        name: entry
                            .path()
                            .file_name()
                            .expect("valid path")
                            .to_str()
                            .expect("valid unicode")
                            .to_string(),
                    })
                } else {
                    None
                }
            })
            .collect::<Vec<LogStream>>()
            .await;

        Ok(directories)
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

    fn query_table(&self, query: &Query) -> Result<Option<ListingTable>, ObjectStorageError> {
        let prefixes: Vec<ListingTableUrl> = query
            .get_prefixes()
            .into_iter()
            .filter_map(|prefix| {
                let path = self.root.join(prefix);
                ListingTableUrl::parse(path.to_str().unwrap()).ok()
            })
            .collect();

        if prefixes.is_empty() {
            return Ok(None);
        }

        let file_format = ParquetFormat::default().with_enable_pruning(true);
        let listing_options = ListingOptions {
            file_extension: ".parquet".to_string(),
            format: Arc::new(file_format),
            table_partition_cols: vec![],
            collect_stat: true,
            target_partitions: 1,
        };

        let config = ListingTableConfig::new_with_multi_paths(prefixes)
            .with_listing_options(listing_options)
            .with_schema(Arc::clone(&query.schema));

        Ok(Some(ListingTable::try_new(config)?))
    }
}

impl From<fs_extra::error::Error> for ObjectStorageError {
    fn from(e: fs_extra::error::Error) -> Self {
        ObjectStorageError::UnhandledError(Box::new(e))
    }
}
