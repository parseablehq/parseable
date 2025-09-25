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
    collections::HashSet,
    path::{Path, PathBuf},
    sync::Arc,
};

use async_trait::async_trait;
use bytes::Bytes;
use chrono::Utc;
use datafusion::{datasource::listing::ListingTableUrl, execution::runtime_env::RuntimeEnvBuilder};
use fs_extra::file::CopyOptions;
use futures::{TryStreamExt, stream::FuturesUnordered};
use object_store::{ListResult, ObjectMeta, buffered::BufReader};
use relative_path::{RelativePath, RelativePathBuf};
use tokio::{
    fs::{self, DirEntry, OpenOptions},
    io::AsyncReadExt,
};
use tokio_stream::wrappers::ReadDirStream;

use crate::{
    handlers::http::users::USERS_ROOT_DIR,
    metrics::{
        increment_files_scanned_in_object_store_calls_by_date, increment_object_store_calls_by_date,
    },
    option::validation,
    parseable::LogStream,
    storage::SETTINGS_ROOT_DIRECTORY,
};

use super::{
    ALERTS_ROOT_DIRECTORY, ObjectStorage, ObjectStorageError, ObjectStorageProvider,
    PARSEABLE_ROOT_DIRECTORY, STREAM_METADATA_FILE_NAME, STREAM_ROOT_DIRECTORY,
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
        let file_path;

        // this is for the `get_manifest()` function because inside a snapshot, we store the absolute path (without `/`) on linux based OS
        // `home/user/.../manifest.json`
        // on windows, the path is stored with the drive letter
        // `D:\\parseable\\data..\\manifest.json`
        // thus, we need to check if the root of localfs is already present in the path
        #[cfg(windows)]
        {
            // in windows the absolute path (self.root) doesn't matter because we store the complete path
            file_path = path.to_path("");
        }
        #[cfg(not(windows))]
        {
            // absolute path (self.root) will always start with `/`
            let root_str = self.root.to_str().unwrap();
            file_path = if path.to_string().contains(&root_str[1..]) && root_str.len() > 1 {
                path.to_path("/")
            } else {
                self.path_in_root(path)
            };
        }

        let file_result = fs::read(file_path).await;
        let res: Result<Bytes, ObjectStorageError> = match file_result {
            Ok(x) => {
                // Record single file accessed successfully
                increment_files_scanned_in_object_store_calls_by_date(
                    "GET",
                    1,
                    &Utc::now().date_naive().to_string(),
                );
                increment_object_store_calls_by_date("GET", &Utc::now().date_naive().to_string());
                Ok(x.into())
            }
            Err(e) => {
                if e.kind() == std::io::ErrorKind::NotFound {
                    Err(ObjectStorageError::NoSuchKey(path.to_string()))
                } else {
                    Err(ObjectStorageError::UnhandledError(Box::new(e)))
                }
            }
        };

        res
    }

    async fn get_ingestor_meta_file_paths(
        &self,
    ) -> Result<Vec<RelativePathBuf>, ObjectStorageError> {
        let mut path_arr = vec![];
        let mut files_scanned = 0u64;

        let entries_result = fs::read_dir(&self.root).await;
        let mut entries = match entries_result {
            Ok(entries) => entries,
            Err(err) => {
                return Err(err.into());
            }
        };

        while let Some(entry) = entries.next_entry().await? {
            files_scanned += 1;
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

        // Record total files scanned
        increment_files_scanned_in_object_store_calls_by_date(
            "LIST",
            files_scanned,
            &Utc::now().date_naive().to_string(),
        );
        increment_object_store_calls_by_date("LIST", &Utc::now().date_naive().to_string());
        Ok(path_arr)
    }

    /// currently it is not using the starts_with_pattern
    async fn get_objects(
        &self,
        base_path: Option<&RelativePath>,
        filter_func: Box<dyn Fn(String) -> bool + std::marker::Send + 'static>,
    ) -> Result<Vec<Bytes>, ObjectStorageError> {
        let prefix = if let Some(path) = base_path {
            path.to_path(&self.root)
        } else {
            self.root.clone()
        };

        let entries_result = fs::read_dir(&prefix).await;
        let mut entries = match entries_result {
            Ok(entries) => entries,
            Err(err) => {
                return Err(err.into());
            }
        };

        let mut res = Vec::new();
        let mut files_scanned = 0;
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

            files_scanned += 1;
            let ingestor_file = filter_func(path);

            if !ingestor_file {
                continue;
            }

            let file_result = fs::read(entry.path()).await;
            match file_result {
                Ok(file) => {
                    // Record total files scanned
                    increment_files_scanned_in_object_store_calls_by_date(
                        "GET",
                        1,
                        &Utc::now().date_naive().to_string(),
                    );
                    increment_object_store_calls_by_date(
                        "GET",
                        &Utc::now().date_naive().to_string(),
                    );
                    res.push(file.into());
                }
                Err(err) => {
                    return Err(err.into());
                }
            }
        }

        increment_files_scanned_in_object_store_calls_by_date(
            "LIST",
            files_scanned as u64,
            &Utc::now().date_naive().to_string(),
        );
        increment_object_store_calls_by_date("LIST", &Utc::now().date_naive().to_string());

        Ok(res)
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

        let res = fs::write(path, resource).await;
        if res.is_ok() {
            // Record single file written successfully
            increment_files_scanned_in_object_store_calls_by_date(
                "PUT",
                1,
                &Utc::now().date_naive().to_string(),
            );
            increment_object_store_calls_by_date("PUT", &Utc::now().date_naive().to_string());
        }

        res.map_err(Into::into)
    }

    async fn delete_prefix(&self, path: &RelativePath) -> Result<(), ObjectStorageError> {
        let path = self.path_in_root(path);

        let result = tokio::fs::remove_dir_all(path).await;
        if result.is_ok() {
            increment_object_store_calls_by_date("DELETE", &Utc::now().date_naive().to_string());
        }
        result?;
        Ok(())
    }

    async fn delete_object(&self, path: &RelativePath) -> Result<(), ObjectStorageError> {
        let path = self.path_in_root(path);

        let result = tokio::fs::remove_file(path).await;
        if result.is_ok() {
            // Record single file deleted successfully
            increment_files_scanned_in_object_store_calls_by_date(
                "DELETE",
                1,
                &Utc::now().date_naive().to_string(),
            );
            increment_object_store_calls_by_date("DELETE", &Utc::now().date_naive().to_string());
        }

        result?;
        Ok(())
    }

    async fn check(&self) -> Result<(), ObjectStorageError> {
        let result = fs::create_dir_all(&self.root).await;
        if result.is_ok() {
            increment_object_store_calls_by_date("HEAD", &Utc::now().date_naive().to_string());
        }

        result.map_err(|e| ObjectStorageError::UnhandledError(e.into()))
    }

    async fn delete_stream(&self, stream_name: &str) -> Result<(), ObjectStorageError> {
        let path = self.root.join(stream_name);

        let result = fs::remove_dir_all(path).await;
        if result.is_ok() {
            increment_object_store_calls_by_date("DELETE", &Utc::now().date_naive().to_string());
        }

        Ok(result?)
    }

    async fn try_delete_node_meta(&self, node_filename: String) -> Result<(), ObjectStorageError> {
        let path = self.root.join(node_filename);

        let result = fs::remove_file(path).await;
        if result.is_ok() {
            increment_object_store_calls_by_date("DELETE", &Utc::now().date_naive().to_string());
        }

        Ok(result?)
    }

    async fn list_streams(&self) -> Result<HashSet<LogStream>, ObjectStorageError> {
        let ignore_dir = &[
            "lost+found",
            PARSEABLE_ROOT_DIRECTORY,
            USERS_ROOT_DIR,
            ALERTS_ROOT_DIRECTORY,
            SETTINGS_ROOT_DIRECTORY,
        ];

        let result = fs::read_dir(&self.root).await;
        let directories = match result {
            Ok(read_dir) => {
                increment_object_store_calls_by_date("LIST", &Utc::now().date_naive().to_string());
                ReadDirStream::new(read_dir)
            }
            Err(err) => {
                return Err(err.into());
            }
        };

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

        let result = fs::read_dir(&self.root).await;
        let directories = match result {
            Ok(read_dir) => {
                increment_object_store_calls_by_date("LIST", &Utc::now().date_naive().to_string());
                ReadDirStream::new(read_dir)
            }
            Err(err) => {
                return Err(err.into());
            }
        };

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
        let result = fs::read_dir(&self.root).await;
        let read_dir = match result {
            Ok(read_dir) => {
                increment_object_store_calls_by_date("LIST", &Utc::now().date_naive().to_string());
                read_dir
            }
            Err(err) => {
                return Err(err.into());
            }
        };

        let dirs = ReadDirStream::new(read_dir)
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

        let result = fs::read_dir(root).await;
        let read_dir = match result {
            Ok(read_dir) => read_dir,
            Err(err) => {
                return Err(err.into());
            }
        };

        let dirs = ReadDirStream::new(read_dir)
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

        let result = fs::read_dir(&path).await;
        let read_dir = match result {
            Ok(read_dir) => {
                increment_object_store_calls_by_date("LIST", &Utc::now().date_naive().to_string());
                read_dir
            }
            Err(err) => {
                return Err(err.into());
            }
        };

        let directories = ReadDirStream::new(read_dir);
        let entries: Vec<DirEntry> = directories.try_collect().await?;
        let entries = entries.into_iter().map(dir_name);
        let dates: Vec<_> = FuturesUnordered::from_iter(entries).try_collect().await?;

        Ok(dates.into_iter().flatten().collect())
    }

    async fn list_hours(
        &self,
        stream_name: &str,
        date: &str,
    ) -> Result<Vec<String>, ObjectStorageError> {
        let path = self.root.join(stream_name).join(date);
        let directories = ReadDirStream::new(fs::read_dir(&path).await?);
        let entries: Vec<DirEntry> = directories.try_collect().await?;
        let entries = entries.into_iter().map(dir_name);
        let hours: Vec<_> = FuturesUnordered::from_iter(entries).try_collect().await?;
        Ok(hours
            .into_iter()
            .flatten()
            .filter(|dir| dir.starts_with("hour="))
            .collect())
    }

    async fn list_minutes(
        &self,
        stream_name: &str,
        date: &str,
        hour: &str,
    ) -> Result<Vec<String>, ObjectStorageError> {
        let path = self.root.join(stream_name).join(date).join(hour);
        // Propagate any read_dir errors instead of swallowing them
        let directories = ReadDirStream::new(fs::read_dir(&path).await?);
        let entries: Vec<DirEntry> = directories.try_collect().await?;
        let entries = entries.into_iter().map(dir_name);
        let minutes: Vec<_> = FuturesUnordered::from_iter(entries).try_collect().await?;
        // Filter down to only the "minute=" prefixed directories
        Ok(minutes
            .into_iter()
            .flatten()
            .filter(|dir| dir.starts_with("minute="))
            .collect())
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

        let result = fs_extra::file::copy(path, to_path, &op);
        match result {
            Ok(_) => {
                increment_object_store_calls_by_date("PUT", &Utc::now().date_naive().to_string());
                Ok(())
            }
            Err(err) => Err(err.into()),
        }
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

    async fn list_with_delimiter(
        &self,
        _prefix: Option<object_store::path::Path>,
    ) -> Result<ListResult, ObjectStorageError> {
        Err(ObjectStorageError::UnhandledError(Box::new(
            std::io::Error::new(
                std::io::ErrorKind::Unsupported,
                "list_with_delimiter is not implemented for LocalFS",
            ),
        )))
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
