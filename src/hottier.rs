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
    collections::BTreeMap,
    io,
    path::{Path, PathBuf},
    sync::Arc,
};

use crate::{
    catalog::manifest::{File, Manifest},
    handlers::http::cluster::INTERNAL_STREAM_NAME,
    parseable::PARSEABLE,
    storage::{ObjectStorage, ObjectStorageError},
    utils::{extract_datetime, human_size::bytes_to_human_size},
    validator::error::HotTierValidationError,
};
use chrono::NaiveDate;
use clokwerk::{AsyncScheduler, Interval, Job};
use futures::{StreamExt, TryStreamExt, stream::FuturesUnordered};
use futures_util::TryFutureExt;
use object_store::{ObjectStore, local::LocalFileSystem};
use once_cell::sync::OnceCell;
use parquet::errors::ParquetError;
use relative_path::RelativePathBuf;
use std::time::Duration;
use sysinfo::Disks;
use tokio::fs::{self, DirEntry};
use tokio::io::AsyncWriteExt;
use tokio_stream::wrappers::ReadDirStream;
use tracing::{error, warn};

pub const STREAM_HOT_TIER_FILENAME: &str = ".hot_tier.json";
pub const MIN_STREAM_HOT_TIER_SIZE_BYTES: u64 = 10737418240; // 10 GiB
const HOT_TIER_SYNC_DURATION: Interval = clokwerk::Interval::Minutes(1);
pub const INTERNAL_STREAM_HOT_TIER_SIZE_BYTES: u64 = 10485760; //10 MiB
pub const CURRENT_HOT_TIER_VERSION: &str = "v2";

#[derive(Debug, serde::Deserialize, serde::Serialize)]
pub struct StreamHotTier {
    pub version: Option<String>,
    #[serde(with = "crate::utils::human_size")]
    pub size: u64,
    #[serde(default, with = "crate::utils::human_size")]
    pub used_size: u64,
    #[serde(default, with = "crate::utils::human_size")]
    pub available_size: u64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub oldest_date_time_entry: Option<String>,
}

pub struct HotTierManager {
    filesystem: LocalFileSystem,
    hot_tier_path: &'static Path,
}

impl HotTierManager {
    pub fn new(hot_tier_path: &'static Path) -> Self {
        std::fs::create_dir_all(hot_tier_path).unwrap();
        HotTierManager {
            filesystem: LocalFileSystem::new(),
            hot_tier_path,
        }
    }

    /// Get a global
    pub fn global() -> Option<&'static HotTierManager> {
        static INSTANCE: OnceCell<HotTierManager> = OnceCell::new();

        PARSEABLE
            .options
            .hot_tier_storage_path
            .as_ref()
            .map(|hot_tier_path| INSTANCE.get_or_init(|| HotTierManager::new(hot_tier_path)))
    }

    ///get the total hot tier size for all streams
    pub async fn get_hot_tiers_size(
        &self,
        current_stream: &str,
    ) -> Result<(u64, u64), HotTierError> {
        let mut total_hot_tier_size = 0;
        let mut total_hot_tier_used_size = 0;
        for stream in PARSEABLE.streams.list() {
            if self.check_stream_hot_tier_exists(&stream) && stream != current_stream {
                let stream_hot_tier = self.get_hot_tier(&stream).await?;
                total_hot_tier_size += &stream_hot_tier.size;
                total_hot_tier_used_size += stream_hot_tier.used_size;
            }
        }
        Ok((total_hot_tier_size, total_hot_tier_used_size))
    }

    /// validate if hot tier size can be fit in the disk
    /// check disk usage and hot tier size of all other streams
    /// check if total hot tier size of all streams is less than max disk usage
    /// delete all the files from hot tier once validation is successful and hot tier is ready to be updated
    pub async fn validate_hot_tier_size(
        &self,
        stream: &str,
        stream_hot_tier_size: u64,
    ) -> Result<u64, HotTierError> {
        let mut existing_hot_tier_used_size = 0;
        if self.check_stream_hot_tier_exists(stream) {
            //delete existing hot tier if its size is less than the updated hot tier size else return error
            let existing_hot_tier = self.get_hot_tier(stream).await?;
            existing_hot_tier_used_size = existing_hot_tier.used_size;

            if stream_hot_tier_size < existing_hot_tier_used_size {
                return Err(HotTierError::ObjectStorageError(
                    ObjectStorageError::Custom(format!(
                        "Reducing hot tier size is not supported, failed to reduce the hot tier size from {} to {}",
                        bytes_to_human_size(existing_hot_tier_used_size),
                        bytes_to_human_size(stream_hot_tier_size)
                    )),
                ));
            }
        }

        let DiskUtil {
            total_space,
            used_space,
            ..
        } = self
            .get_disk_usage()
            .expect("Codepath should only be hit if hottier is enabled");

        let (total_hot_tier_size, total_hot_tier_used_size) =
            self.get_hot_tiers_size(stream).await?;
        let disk_threshold = (PARSEABLE.options.max_disk_usage * total_space as f64) / 100.0;
        let max_allowed_hot_tier_size = disk_threshold
            - total_hot_tier_size as f64
            - (used_space as f64
                - total_hot_tier_used_size as f64
                - existing_hot_tier_used_size as f64);

        if stream_hot_tier_size as f64 > max_allowed_hot_tier_size {
            error!(
                "disk_threshold: {}, used_disk_space: {}, total_hot_tier_used_size: {}, existing_hot_tier_used_size: {}, total_hot_tier_size: {}",
                bytes_to_human_size(disk_threshold as u64),
                bytes_to_human_size(used_space),
                bytes_to_human_size(total_hot_tier_used_size),
                bytes_to_human_size(existing_hot_tier_used_size),
                bytes_to_human_size(total_hot_tier_size)
            );

            return Err(HotTierError::ObjectStorageError(
                ObjectStorageError::Custom(format!(
                    "{} is the total usable disk space for hot tier, cannot set a bigger value.",
                    bytes_to_human_size(max_allowed_hot_tier_size as u64)
                )),
            ));
        }

        Ok(existing_hot_tier_used_size)
    }

    ///get the hot tier metadata file for the stream
    pub async fn get_hot_tier(&self, stream: &str) -> Result<StreamHotTier, HotTierError> {
        if !self.check_stream_hot_tier_exists(stream) {
            return Err(HotTierValidationError::NotFound(stream.to_owned()).into());
        }
        let path = self.hot_tier_file_path(stream)?;
        let bytes = self
            .filesystem
            .get(&path)
            .and_then(|resp| resp.bytes())
            .await?;

        let mut stream_hot_tier: StreamHotTier = serde_json::from_slice(&bytes)?;
        stream_hot_tier.oldest_date_time_entry = self.get_oldest_date_time_entry(stream).await?;

        Ok(stream_hot_tier)
    }

    pub async fn delete_hot_tier(&self, stream: &str) -> Result<(), HotTierError> {
        if !self.check_stream_hot_tier_exists(stream) {
            return Err(HotTierValidationError::NotFound(stream.to_owned()).into());
        }
        let path = self.hot_tier_path.join(stream);
        fs::remove_dir_all(path).await?;

        Ok(())
    }

    ///put the hot tier metadata file for the stream
    /// set the updated_date_range in the hot tier metadata file
    pub async fn put_hot_tier(
        &self,
        stream: &str,
        hot_tier: &mut StreamHotTier,
    ) -> Result<(), HotTierError> {
        let path = self.hot_tier_file_path(stream)?;
        let bytes = serde_json::to_vec(&hot_tier)?.into();
        self.filesystem.put(&path, bytes).await?;
        Ok(())
    }

    /// get the hot tier file path for the stream
    pub fn hot_tier_file_path(
        &self,
        stream: &str,
    ) -> Result<object_store::path::Path, HotTierError> {
        let path = self
            .hot_tier_path
            .join(stream)
            .join(STREAM_HOT_TIER_FILENAME);
        let path = object_store::path::Path::from_absolute_path(path)?;

        Ok(path)
    }

    ///schedule the download of the hot tier files from S3 every minute
    pub fn download_from_s3<'a>(&'a self) -> Result<(), HotTierError>
    where
        'a: 'static,
    {
        let mut scheduler = AsyncScheduler::new();
        scheduler
            .every(HOT_TIER_SYNC_DURATION)
            .plus(Interval::Seconds(5))
            .run(move || async {
                if let Err(err) = self.sync_hot_tier().await {
                    error!("Error in hot tier scheduler: {:?}", err);
                }
            });

        tokio::spawn(async move {
            loop {
                scheduler.run_pending().await;
                tokio::time::sleep(Duration::from_secs(10)).await;
            }
        });
        Ok(())
    }

    ///sync the hot tier files from S3 to the hot tier directory for all streams
    async fn sync_hot_tier(&self) -> Result<(), HotTierError> {
        let mut sync_hot_tier_tasks = FuturesUnordered::new();
        for stream in PARSEABLE.streams.list() {
            if self.check_stream_hot_tier_exists(&stream) {
                sync_hot_tier_tasks.push(self.process_stream(stream));
            }
        }

        while let Some(res) = sync_hot_tier_tasks.next().await {
            if let Err(err) = res {
                error!("Failed to run hot tier sync task {err:?}");
                return Err(err);
            }
        }
        Ok(())
    }

    ///process the hot tier files for the stream
    /// delete the files from the hot tier directory if the available date range is outside the hot tier range
    async fn process_stream(&self, stream: String) -> Result<(), HotTierError> {
        let stream_hot_tier = self.get_hot_tier(&stream).await?;
        let mut parquet_file_size = stream_hot_tier.used_size;

        let object_store = PARSEABLE.storage.get_object_store();
        let mut s3_manifest_file_list = object_store.list_manifest_files(&stream).await?;
        self.process_manifest(
            &stream,
            &mut s3_manifest_file_list,
            &mut parquet_file_size,
            object_store.clone(),
        )
        .await?;

        Ok(())
    }

    ///process the hot tier files for the date for the stream
    /// collect all manifests from S3 for the date, sort the parquet file list
    /// in order to download the latest files first
    /// download the parquet files if not present in hot tier directory
    async fn process_manifest(
        &self,
        stream: &str,
        manifest_files_to_download: &mut BTreeMap<String, Vec<String>>,
        parquet_file_size: &mut u64,
        object_store: Arc<dyn ObjectStorage>,
    ) -> Result<(), HotTierError> {
        if manifest_files_to_download.is_empty() {
            return Ok(());
        }
        for (str_date, manifest_files) in manifest_files_to_download.iter().rev() {
            let mut storage_combined_manifest = Manifest::default();

            for manifest_file in manifest_files {
                let manifest_path: RelativePathBuf = RelativePathBuf::from(manifest_file.clone());
                let storage_manifest_bytes = object_store.get_object(&manifest_path).await?;
                let storage_manifest: Manifest = serde_json::from_slice(&storage_manifest_bytes)?;
                storage_combined_manifest
                    .files
                    .extend(storage_manifest.files);
            }

            storage_combined_manifest
                .files
                .sort_by_key(|file| file.file_path.clone());

            while let Some(parquet_file) = storage_combined_manifest.files.pop() {
                let parquet_file_path = &parquet_file.file_path;
                let parquet_path = self.hot_tier_path.join(parquet_file_path);

                if !parquet_path.exists() {
                    if let Ok(date) =
                        NaiveDate::parse_from_str(str_date.trim_start_matches("date="), "%Y-%m-%d")
                    {
                        if !self
                            .process_parquet_file(
                                stream,
                                &parquet_file,
                                parquet_file_size,
                                parquet_path,
                                date,
                            )
                            .await?
                        {
                            break;
                        }
                    } else {
                        warn!("Invalid date format: {}", str_date);
                    }
                }
            }
        }

        Ok(())
    }

    ///process the parquet file for the stream
    /// check if the disk is available to download the parquet file
    /// if not available, delete the oldest entry from the hot tier directory
    /// download the parquet file from S3 to the hot tier directory
    /// update the used and available size in the hot tier metadata
    /// return true if the parquet file is processed successfully
    async fn process_parquet_file(
        &self,
        stream: &str,
        parquet_file: &File,
        parquet_file_size: &mut u64,
        parquet_path: PathBuf,
        date: NaiveDate,
    ) -> Result<bool, HotTierError> {
        let mut file_processed = false;
        let mut stream_hot_tier = self.get_hot_tier(stream).await?;
        if !self.is_disk_available(parquet_file.file_size).await?
            || stream_hot_tier.available_size <= parquet_file.file_size
        {
            if !self
                .cleanup_hot_tier_old_data(
                    stream,
                    &mut stream_hot_tier,
                    &parquet_path,
                    parquet_file.file_size,
                )
                .await?
            {
                return Ok(file_processed);
            }
            *parquet_file_size = stream_hot_tier.used_size;
        }
        let parquet_file_path = RelativePathBuf::from(parquet_file.file_path.clone());
        fs::create_dir_all(parquet_path.parent().unwrap()).await?;
        let mut file = fs::File::create(parquet_path.clone()).await?;
        let parquet_data = PARSEABLE
            .storage
            .get_object_store()
            .get_object(&parquet_file_path)
            .await?;
        file.write_all(&parquet_data).await?;
        *parquet_file_size += parquet_file.file_size;
        stream_hot_tier.used_size = *parquet_file_size;

        stream_hot_tier.available_size -= parquet_file.file_size;
        self.put_hot_tier(stream, &mut stream_hot_tier).await?;
        file_processed = true;
        let path = self.get_stream_path_for_date(stream, &date);
        let mut hot_tier_manifest = HotTierManager::get_hot_tier_manifest_from_path(path).await?;
        hot_tier_manifest.files.push(parquet_file.clone());
        hot_tier_manifest
            .files
            .sort_by_key(|file| file.file_path.clone());
        // write the manifest file to the hot tier directory
        let manifest_path = self
            .get_stream_path_for_date(stream, &date)
            .join("hottier.manifest.json");
        fs::create_dir_all(manifest_path.parent().unwrap()).await?;
        fs::write(manifest_path, serde_json::to_vec(&hot_tier_manifest)?).await?;

        Ok(file_processed)
    }

    ///fetch the list of dates available in the hot tier directory for the stream and sort them
    pub async fn fetch_hot_tier_dates(&self, stream: &str) -> Result<Vec<NaiveDate>, HotTierError> {
        let mut date_list = Vec::new();
        let path = self.hot_tier_path.join(stream);
        if !path.exists() {
            return Ok(date_list);
        }

        let directories = fs::read_dir(&path).await?;
        let mut dates = ReadDirStream::new(directories);
        while let Some(date) = dates.next().await {
            let date = date?;
            if !date.path().is_dir() {
                continue;
            }
            let date = NaiveDate::parse_from_str(
                date.file_name()
                    .to_string_lossy()
                    .trim_start_matches("date="),
                "%Y-%m-%d",
            )
            .unwrap();
            date_list.push(date);
        }
        date_list.sort();

        Ok(date_list)
    }

    ///get hot tier manifest on path
    pub async fn get_hot_tier_manifest_from_path(path: PathBuf) -> Result<Manifest, HotTierError> {
        if !path.exists() {
            return Ok(Manifest::default());
        }

        // List the directories and prepare the hot tier manifest
        let mut date_dirs = fs::read_dir(&path).await?;
        let mut hot_tier_manifest = Manifest::default();

        // Avoid unnecessary checks and keep only valid manifest files
        while let Some(manifest) = date_dirs.next_entry().await? {
            if !manifest
                .file_name()
                .to_string_lossy()
                .ends_with(".manifest.json")
            {
                continue;
            }
            // Deserialize each manifest file and extend the hot tier manifest with its files
            let file = fs::read(manifest.path()).await?;
            let manifest: Manifest = serde_json::from_slice(&file)?;
            hot_tier_manifest.files.extend(manifest.files);
        }

        Ok(hot_tier_manifest)
    }

    /// get hot tier path for the stream and date
    pub fn get_stream_path_for_date(&self, stream: &str, date: &NaiveDate) -> PathBuf {
        self.hot_tier_path.join(stream).join(format!("date={date}"))
    }

    /// Returns the list of manifest files present in hot tier directory for the stream
    pub async fn get_hot_tier_manifest_files(
        &self,
        stream: &str,
        manifest_files: &mut Vec<File>,
    ) -> Result<Vec<File>, HotTierError> {
        // Fetch the list of hot tier parquet files for the given stream.
        let mut hot_tier_files = self.get_hot_tier_parquet_files(stream).await?;

        // Retain only the files in `hot_tier_files` that also exist in `manifest_files`.
        hot_tier_files.retain(|file| {
            manifest_files
                .iter()
                .any(|manifest_file| manifest_file.file_path.eq(&file.file_path))
        });

        // Sort `hot_tier_files` in descending order by file path.
        hot_tier_files.sort_unstable_by(|a, b| b.file_path.cmp(&a.file_path));

        // Update `manifest_files` to exclude files that are present in the filtered `hot_tier_files`.
        manifest_files.retain(|manifest_file| {
            hot_tier_files
                .iter()
                .all(|file| !file.file_path.eq(&manifest_file.file_path))
        });

        // Sort `manifest_files` in descending order by file path.
        manifest_files.sort_unstable_by(|a, b| b.file_path.cmp(&a.file_path));

        Ok(hot_tier_files)
    }

    ///get the list of parquet files from the hot tier directory for the stream
    pub async fn get_hot_tier_parquet_files(
        &self,
        stream: &str,
    ) -> Result<Vec<File>, HotTierError> {
        // Fetch list of dates for the given stream
        let date_list = self.fetch_hot_tier_dates(stream).await?;

        // Create an unordered iter of futures to async collect files
        let mut tasks = FuturesUnordered::new();

        // For each date, fetch the manifest and extract parquet files
        for date in date_list {
            let path = self.get_stream_path_for_date(stream, &date);
            tasks.push(async move {
                HotTierManager::get_hot_tier_manifest_from_path(path)
                    .await
                    .map(|manifest| manifest.files.clone())
                    .unwrap_or_default() // If fetching manifest fails, return an empty vector
            });
        }

        // Collect parquet files for all dates
        let mut hot_tier_parquet_files: Vec<File> = vec![];
        while let Some(files) = tasks.next().await {
            hot_tier_parquet_files.extend(files);
        }

        Ok(hot_tier_parquet_files)
    }

    ///check if the hot tier metadata file exists for the stream
    pub fn check_stream_hot_tier_exists(&self, stream: &str) -> bool {
        let path = self
            .hot_tier_path
            .join(stream)
            .join(STREAM_HOT_TIER_FILENAME);
        path.exists()
    }

    ///delete the parquet file from the hot tier directory for the stream
    /// loop through all manifests in the hot tier directory for the stream
    /// loop through all parquet files in the manifest
    /// check for the oldest entry to delete if the path exists in hot tier
    /// update the used and available size in the hot tier metadata
    /// loop if available size is still less than the parquet file size
    pub async fn cleanup_hot_tier_old_data(
        &self,
        stream: &str,
        stream_hot_tier: &mut StreamHotTier,
        download_file_path: &Path,
        parquet_file_size: u64,
    ) -> Result<bool, HotTierError> {
        let mut delete_successful = false;
        let dates = self.fetch_hot_tier_dates(stream).await?;
        'loop_dates: for date in dates {
            let path = self.get_stream_path_for_date(stream, &date);
            if !path.exists() {
                continue;
            }

            let date_dirs = ReadDirStream::new(fs::read_dir(&path).await?);
            let mut manifest_files: Vec<DirEntry> = date_dirs.try_collect().await?;
            manifest_files.retain(|manifest| {
                manifest
                    .file_name()
                    .to_string_lossy()
                    .ends_with(".manifest.json")
            });
            for manifest_file in manifest_files {
                let file = fs::read(manifest_file.path()).await?;
                let mut manifest: Manifest = serde_json::from_slice(&file)?;

                manifest.files.sort_by_key(|file| file.file_path.clone());
                manifest.files.reverse();

                'loop_files: while let Some(file_to_delete) = manifest.files.pop() {
                    let file_size = file_to_delete.file_size;
                    let path_to_delete = self.hot_tier_path.join(&file_to_delete.file_path);

                    if path_to_delete.exists() {
                        if let (Some(download_date_time), Some(delete_date_time)) = (
                            extract_datetime(download_file_path.to_str().unwrap()),
                            extract_datetime(path_to_delete.to_str().unwrap()),
                        ) {
                            if download_date_time <= delete_date_time {
                                delete_successful = false;
                                break 'loop_files;
                            }
                        }

                        fs::write(manifest_file.path(), serde_json::to_vec(&manifest)?).await?;

                        fs::remove_dir_all(path_to_delete.parent().unwrap()).await?;
                        delete_empty_directory_hot_tier(
                            path_to_delete.parent().unwrap().to_path_buf(),
                        )
                        .await?;

                        stream_hot_tier.used_size -= file_size;
                        stream_hot_tier.available_size += file_size;
                        self.put_hot_tier(stream, stream_hot_tier).await?;
                        delete_successful = true;

                        if stream_hot_tier.available_size <= parquet_file_size {
                            continue 'loop_files;
                        } else {
                            break 'loop_dates;
                        }
                    } else {
                        fs::write(manifest_file.path(), serde_json::to_vec(&manifest)?).await?;
                    }
                }
            }
        }

        Ok(delete_successful)
    }

    ///check if the disk is available to download the parquet file
    /// check if the disk usage is above the threshold
    pub async fn is_disk_available(&self, size_to_download: u64) -> Result<bool, HotTierError> {
        if let Some(DiskUtil {
            total_space,
            available_space,
            used_space,
        }) = self.get_disk_usage()
        {
            if available_space < size_to_download {
                return Ok(false);
            }

            if ((used_space + size_to_download) as f64 * 100.0 / total_space as f64)
                > PARSEABLE.options.max_disk_usage
            {
                return Ok(false);
            }
        }

        Ok(true)
    }

    pub async fn get_oldest_date_time_entry(
        &self,
        stream: &str,
    ) -> Result<Option<String>, HotTierError> {
        let date_list = self.fetch_hot_tier_dates(stream).await?;
        if date_list.is_empty() {
            return Ok(None);
        }

        for date in date_list {
            let path = self.get_stream_path_for_date(stream, &date);
            let hours_dir = ReadDirStream::new(fs::read_dir(&path).await?);
            let mut hours: Vec<DirEntry> = hours_dir.try_collect().await?;
            hours.retain(|entry| {
                entry.path().is_dir() && entry.file_name().to_string_lossy().starts_with("hour=")
            });
            hours.sort_by_key(|entry| entry.file_name().to_string_lossy().to_string());

            for hour in hours {
                let hour_str = hour
                    .file_name()
                    .to_string_lossy()
                    .trim_start_matches("hour=")
                    .to_string();

                let minutes_dir = ReadDirStream::new(fs::read_dir(hour.path()).await?);
                let mut minutes: Vec<DirEntry> = minutes_dir.try_collect().await?;
                minutes.retain(|entry| {
                    entry.path().is_dir()
                        && entry.file_name().to_string_lossy().starts_with("minute=")
                });
                minutes.sort_by_key(|entry| entry.file_name().to_string_lossy().to_string());

                if let Some(minute) = minutes.first() {
                    let minute_str = minute
                        .file_name()
                        .to_string_lossy()
                        .trim_start_matches("minute=")
                        .to_string();
                    let oldest_date_time = format!("{date}T{hour_str}:{minute_str}:00.000Z");
                    return Ok(Some(oldest_date_time));
                }
            }
        }

        Ok(None)
    }

    pub async fn put_internal_stream_hot_tier(&self) -> Result<(), HotTierError> {
        if !self.check_stream_hot_tier_exists(INTERNAL_STREAM_NAME) {
            let mut stream_hot_tier = StreamHotTier {
                version: Some(CURRENT_HOT_TIER_VERSION.to_string()),
                size: INTERNAL_STREAM_HOT_TIER_SIZE_BYTES,
                used_size: 0,
                available_size: INTERNAL_STREAM_HOT_TIER_SIZE_BYTES,
                oldest_date_time_entry: None,
            };
            self.put_hot_tier(INTERNAL_STREAM_NAME, &mut stream_hot_tier)
                .await?;
        }
        Ok(())
    }

    /// Get the disk usage for the hot tier storage path. If we have a three disk paritions
    /// mounted as follows:
    /// 1. /
    /// 2. /home/parseable
    /// 3. /home/example/ignore
    ///
    /// And parseable is running with `P_HOT_TIER_DIR` pointing to a directory in
    /// `/home/parseable`, we should return the usage stats of the disk mounted there.
    fn get_disk_usage(&self) -> Option<DiskUtil> {
        let mut disks = Disks::new_with_refreshed_list();
        // Order the disk partitions by decreasing length of mount path
        disks.sort_by_key(|disk| disk.mount_point().to_str().unwrap().len());
        disks.reverse();

        for disk in disks.iter() {
            // Returns disk utilisation of first matching mount point
            if self.hot_tier_path.starts_with(disk.mount_point()) {
                return Some(DiskUtil {
                    total_space: disk.total_space(),
                    available_space: disk.available_space(),
                    used_space: disk.total_space() - disk.available_space(),
                });
            }
        }

        None
    }
}

struct DiskUtil {
    total_space: u64,
    available_space: u64,
    used_space: u64,
}

async fn delete_empty_directory_hot_tier(path: PathBuf) -> io::Result<()> {
    if !path.is_dir() {
        return Ok(());
    }
    let mut read_dir = fs::read_dir(&path).await?;

    let mut tasks = vec![];
    while let Some(entry) = read_dir.next_entry().await? {
        let entry_path = entry.path();
        if entry_path.is_dir() {
            tasks.push(delete_empty_directory_hot_tier(entry_path));
        }
    }

    futures::stream::iter(tasks)
        .buffer_unordered(10)
        .try_collect::<Vec<_>>()
        .await?;

    // Re-check the directory after deleting its subdirectories
    let mut read_dir = fs::read_dir(&path).await?;
    if read_dir.next_entry().await?.is_none() {
        fs::remove_dir(&path).await?;
    }

    Ok(())
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
    ObjectStorePathError(#[from] object_store::path::Error),
    #[error("{0}")]
    ObjectStorageError(#[from] ObjectStorageError),
    #[error("{0}")]
    ParquetError(#[from] ParquetError),
    #[error("{0}")]
    HotTierValidationError(#[from] HotTierValidationError),
    #[error("{0}")]
    Anyhow(#[from] anyhow::Error),
}
