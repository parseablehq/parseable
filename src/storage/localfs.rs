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

use std::{
    collections::{BTreeMap, HashSet},
    path::{Path, PathBuf},
    sync::Arc,
    time::Instant,
};

use async_trait::async_trait;
use bytes::Bytes;
use datafusion::{datasource::listing::ListingTableUrl, execution::runtime_env::RuntimeEnvBuilder};
use fs_extra::file::CopyOptions;
use futures::{TryStreamExt, stream::FuturesUnordered};
use object_store::{ObjectMeta, buffered::BufReader};
use relative_path::{RelativePath, RelativePathBuf};
use tokio::{
    fs::{self, DirEntry, OpenOptions},
    io::AsyncReadExt,
};
use tokio_stream::wrappers::ReadDirStream;

use crate::{
    handlers::http::users::USERS_ROOT_DIR,
    metrics::storage::{StorageMetrics, azureblob::REQUEST_RESPONSE_TIME},
    option::validation,
    parseable::LogStream,
    storage::SETTINGS_ROOT_DIRECTORY,
};

use super::{
    ALERTS_ROOT_DIRECTORY, ObjectStorage, ObjectStorageError, ObjectStorageProvider,
    PARSEABLE_ROOT_DIRECTORY, SCHEMA_FILE_NAME, STREAM_METADATA_FILE_NAME, STREAM_ROOT_DIRECTORY,
};

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
    fn name(&self) -> &'static str {
        "drive"
    }

    fn get_datafusion_runtime(&self) -> RuntimeEnvBuilder {
        RuntimeEnvBuilder::new()
    }

    fn construct_client(&self) -> Arc<dyn ObjectStorage> {
        Arc::new(LocalFS::new(self.root.clone()))
    }

    fn get_endpoint(&self) -> String {
        self.root.to_str().unwrap().to_string()
    }

    fn register_store_metrics(&self, handler: &actix_web_prometheus::PrometheusMetrics) {
        self.register_metrics(handler);
    }
}

#[derive(Debug)]
pub struct LocalFS {
    // absolute path of the data directory
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
    async fn upload_multipart(
        &self,
        key: &RelativePath,
        path: &Path,
    ) -> Result<(), ObjectStorageError> {
        let mut file = OpenOptions::new().read(true).open(path).await?;
        let mut data = Vec::new();
        file.read_to_end(&mut data).await?;
        self.put_object(key, data.into()).await
    }
    async fn get_buffered_reader(
        &self,
        _path: &RelativePath,
    ) -> Result<BufReader, ObjectStorageError> {
        Err(ObjectStorageError::UnhandledError(Box::new(
            std::io::Error::new(
                std::io::ErrorKind::Unsupported,
                "Buffered reader not implemented for LocalFS yet",
            ),
        )))
    }
    async fn head(&self, _path: &RelativePath) -> Result<ObjectMeta, ObjectStorageError> {
        Err(ObjectStorageError::UnhandledError(Box::new(
            std::io::Error::new(
                std::io::ErrorKind::Unsupported,
                "Head operation not implemented for LocalFS yet",
            ),
        )))
    }
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

    async fn get_ingestor_meta_file_paths(
        &self,
    ) -> Result<Vec<RelativePathBuf>, ObjectStorageError> {
        let time = Instant::now();

        let mut path_arr = vec![];
        let mut entries = fs::read_dir(&self.root).await?;

        while let Some(entry) = entries.next_entry().await? {
            let flag = entry
                .path()
                .file_name()
                .unwrap_or_default()
                .to_str()
                .unwrap_or_default()
                .contains("ingestor");

            if flag {
                path_arr.push(
                    RelativePathBuf::from_path(entry.path().file_name().unwrap())
                        .map_err(ObjectStorageError::PathError)?,
                );
            }
        }

        let time = time.elapsed().as_secs_f64();
        REQUEST_RESPONSE_TIME
            .with_label_values(&["GET", "200"]) // this might not be the right status code
            .observe(time);

        Ok(path_arr)
    }

    async fn get_stream_file_paths(
        &self,
        stream_name: &str,
    ) -> Result<Vec<RelativePathBuf>, ObjectStorageError> {
        let time = Instant::now();
        let mut path_arr = vec![];

        // = data/stream_name
        let stream_dir_path = self.path_in_root(&RelativePathBuf::from(stream_name));
        let mut entries = fs::read_dir(&stream_dir_path).await?;

        while let Some(entry) = entries.next_entry().await? {
            let flag = entry
                .path()
                .file_name()
                .ok_or(ObjectStorageError::NoSuchKey(
                    "Dir Entry Suggests no file present".to_string(),
                ))?
                .to_str()
                .expect("file name is parseable to str")
                .contains("ingestor");

            if flag {
                path_arr.push(RelativePathBuf::from_iter([
                    stream_name,
                    entry.path().file_name().unwrap().to_str().unwrap(), // checking the error before hand
                ]));
            }
        }

        path_arr.push(RelativePathBuf::from_iter([
            stream_name,
            STREAM_METADATA_FILE_NAME,
        ]));
        path_arr.push(RelativePathBuf::from_iter([stream_name, SCHEMA_FILE_NAME]));

        let time = time.elapsed().as_secs_f64();
        REQUEST_RESPONSE_TIME
            .with_label_values(&["GET", "200"]) // this might not be the right status code
            .observe(time);

        Ok(path_arr)
    }

    /// currently it is not using the starts_with_pattern
    async fn get_objects(
        &self,
        base_path: Option<&RelativePath>,
        filter_func: Box<(dyn Fn(String) -> bool + std::marker::Send + 'static)>,
    ) -> Result<Vec<Bytes>, ObjectStorageError> {
        let time = Instant::now();

        let prefix = if let Some(path) = base_path {
            path.to_path(&self.root)
        } else {
            self.root.clone()
        };

        let mut entries = fs::read_dir(&prefix).await?;
        let mut res = Vec::new();
        while let Some(entry) = entries.next_entry().await? {
            let path = entry
                .path()
                .file_name()
                .ok_or(ObjectStorageError::NoSuchKey(
                    "Dir Entry suggests no file present".to_string(),
                ))?
                .to_str()
                .expect("file name is parseable to str")
                .to_owned();
            let ingestor_file = filter_func(path);

            if !ingestor_file {
                continue;
            }

            let file = fs::read(entry.path()).await?;
            res.push(file.into());
        }

        // maybe change the return code
        let status = if res.is_empty() { "200" } else { "400" };
        let time = time.elapsed().as_secs_f64();
        REQUEST_RESPONSE_TIME
            .with_label_values(&["GET", status])
            .observe(time);

        Ok(res)
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

    async fn delete_object(&self, path: &RelativePath) -> Result<(), ObjectStorageError> {
        let path = self.path_in_root(path);
        tokio::fs::remove_file(path).await?;
        Ok(())
    }

    async fn check(&self) -> Result<(), ObjectStorageError> {
        fs::create_dir_all(&self.root)
            .await
            .map_err(|e| ObjectStorageError::UnhandledError(e.into()))
    }

    async fn delete_stream(&self, stream_name: &str) -> Result<(), ObjectStorageError> {
        let path = self.root.join(stream_name);
        Ok(fs::remove_dir_all(path).await?)
    }

    async fn try_delete_node_meta(&self, node_filename: String) -> Result<(), ObjectStorageError> {
        let path = self.root.join(node_filename);
        Ok(fs::remove_file(path).await?)
    }

    async fn list_streams(&self) -> Result<HashSet<LogStream>, ObjectStorageError> {
        let ignore_dir = &[
            "lost+found",
            PARSEABLE_ROOT_DIRECTORY,
            USERS_ROOT_DIR,
            ALERTS_ROOT_DIRECTORY,
            SETTINGS_ROOT_DIRECTORY,
        ];
        let directories = ReadDirStream::new(fs::read_dir(&self.root).await?);
        let entries: Vec<DirEntry> = directories.try_collect().await?;
        let entries = entries
            .into_iter()
            .map(|entry| dir_with_stream(entry, ignore_dir));

        let logstream_dirs: Vec<Option<String>> =
            FuturesUnordered::from_iter(entries).try_collect().await?;

        let logstreams = logstream_dirs.into_iter().flatten().collect();

        Ok(logstreams)
    }

    async fn list_old_streams(&self) -> Result<HashSet<LogStream>, ObjectStorageError> {
        let ignore_dir = &[
            "lost+found",
            PARSEABLE_ROOT_DIRECTORY,
            ALERTS_ROOT_DIRECTORY,
            SETTINGS_ROOT_DIRECTORY,
        ];
        let directories = ReadDirStream::new(fs::read_dir(&self.root).await?);
        let entries: Vec<DirEntry> = directories.try_collect().await?;
        let entries = entries
            .into_iter()
            .map(|entry| dir_with_old_stream(entry, ignore_dir));

        let logstream_dirs: Vec<Option<String>> =
            FuturesUnordered::from_iter(entries).try_collect().await?;

        let logstreams = logstream_dirs.into_iter().flatten().collect();

        Ok(logstreams)
    }

    async fn list_dirs(&self) -> Result<Vec<String>, ObjectStorageError> {
        let dirs = ReadDirStream::new(fs::read_dir(&self.root).await?)
            .try_collect::<Vec<DirEntry>>()
            .await?
            .into_iter()
            .map(dir_name);

        let dirs = FuturesUnordered::from_iter(dirs)
            .try_collect::<Vec<_>>()
            .await?
            .into_iter()
            .flatten()
            .collect::<Vec<_>>();

        Ok(dirs)
    }

    async fn list_dirs_relative(
        &self,
        relative_path: &RelativePath,
    ) -> Result<Vec<String>, ObjectStorageError> {
        let root = self.root.join(relative_path.as_str());
        let dirs = ReadDirStream::new(fs::read_dir(root).await?)
            .try_collect::<Vec<DirEntry>>()
            .await?
            .into_iter()
            .map(dir_name);

        let dirs = FuturesUnordered::from_iter(dirs)
            .try_collect::<Vec<_>>()
            .await?
            .into_iter()
            .flatten()
            .collect::<Vec<_>>();

        Ok(dirs)
    }

    async fn list_dates(&self, stream_name: &str) -> Result<Vec<String>, ObjectStorageError> {
        let path = self.root.join(stream_name);
        let directories = ReadDirStream::new(fs::read_dir(&path).await?);
        let entries: Vec<DirEntry> = directories.try_collect().await?;
        let entries = entries.into_iter().map(dir_name);
        let dates: Vec<_> = FuturesUnordered::from_iter(entries).try_collect().await?;

        Ok(dates.into_iter().flatten().collect())
    }

    async fn list_manifest_files(
        &self,
        _stream_name: &str,
    ) -> Result<BTreeMap<String, Vec<String>>, ObjectStorageError> {
        //unimplemented
        Ok(BTreeMap::new())
    }

    async fn upload_file(&self, key: &str, path: &Path) -> Result<(), ObjectStorageError> {
        let op = CopyOptions {
            overwrite: true,
            skip_exist: true,
            ..CopyOptions::default()
        };
        let to_path = self.root.join(key);
        if let Some(path) = to_path.parent() {
            fs::create_dir_all(path).await?;
        }
        let _ = fs_extra::file::copy(path, to_path, &op)?;
        Ok(())
    }

    fn absolute_url(&self, prefix: &RelativePath) -> object_store::path::Path {
        object_store::path::Path::parse(
            format!("{}", self.root.join(prefix.as_str()).display())
                .trim_start_matches(std::path::MAIN_SEPARATOR),
        )
        .unwrap()
    }

    fn query_prefixes(&self, prefixes: Vec<String>) -> Vec<ListingTableUrl> {
        prefixes
            .into_iter()
            .filter_map(|prefix| ListingTableUrl::parse(format!("/{prefix}")).ok())
            .collect()
    }

    fn store_url(&self) -> url::Url {
        url::Url::parse("file:///").unwrap()
    }

    fn get_bucket_name(&self) -> String {
        self.root
            .iter()
            .next_back()
            .expect("can be unwrapped without checking as the path is absolute")
            .to_str()
            .expect("valid unicode")
            .to_string()
    }
}

async fn dir_with_old_stream(
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

        // even in ingest mode, we should only look for the global stream metadata file
        let stream_json_path = path.join(STREAM_METADATA_FILE_NAME);

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

        // even in ingest mode, we should only look for the global stream metadata file
        let stream_json_path = path
            .join(STREAM_ROOT_DIRECTORY)
            .join(STREAM_METADATA_FILE_NAME);

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
