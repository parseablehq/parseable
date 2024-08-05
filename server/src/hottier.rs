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
    metadata::{error::stream_info::MetadataError, STREAM_INFO},
    option::{
        validation::{bytes_to_human_size, human_size_to_bytes},
        CONFIG,
    },
    storage::{ObjectStorage, ObjectStorageError},
    utils::extract_datetime,
    validator::error::HotTierValidationError,
};
use chrono::NaiveDate;
use clokwerk::{AsyncScheduler, Interval, Job};
use futures::{stream::FuturesUnordered, StreamExt, TryStreamExt};
use futures_util::TryFutureExt;
use object_store::{local::LocalFileSystem, ObjectStore};
use once_cell::sync::OnceCell;
use parquet::errors::ParquetError;
use relative_path::RelativePathBuf;
use std::time::Duration;
use sysinfo::{Disks, System};
use tokio::fs::{self, DirEntry};
use tokio::io::AsyncWriteExt;
use tokio_stream::wrappers::ReadDirStream;

pub const STREAM_HOT_TIER_FILENAME: &str = ".hot_tier.json";
pub const MIN_STREAM_HOT_TIER_SIZE_BYTES: u64 = 10737418240; // 10 GiB
const HOT_TIER_SYNC_DURATION: Interval = clokwerk::Interval::Minutes(1);

#[derive(Debug, serde::Deserialize, serde::Serialize)]
pub struct StreamHotTier {
    #[serde(rename = "size")]
    pub size: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub used_size: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub available_size: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub oldest_date_time_entry: Option<String>,
}

pub struct HotTierManager {
    filesystem: LocalFileSystem,
    hot_tier_path: PathBuf,
}

impl HotTierManager {
    pub fn global() -> Option<&'static HotTierManager> {
        static INSTANCE: OnceCell<HotTierManager> = OnceCell::new();

        let hot_tier_path = CONFIG.parseable.hot_tier_storage_path.as_ref()?;

        Some(INSTANCE.get_or_init(|| {
            let hot_tier_path = hot_tier_path.clone();
            std::fs::create_dir_all(&hot_tier_path).unwrap();
            HotTierManager {
                filesystem: LocalFileSystem::new(),
                hot_tier_path,
            }
        }))
    }

    ///get the total hot tier size for all streams
    pub async fn get_hot_tiers_size(
        &self,
        current_stream: &str,
    ) -> Result<(u64, u64), HotTierError> {
        let mut total_hot_tier_size = 0;
        let mut total_hot_tier_used_size = 0;
        for stream in STREAM_INFO.list_streams() {
            if self.check_stream_hot_tier_exists(&stream) && stream != current_stream {
                let stream_hot_tier = self.get_hot_tier(&stream).await?;
                total_hot_tier_size += human_size_to_bytes(&stream_hot_tier.size).unwrap();
                total_hot_tier_used_size +=
                    human_size_to_bytes(&stream_hot_tier.used_size.unwrap()).unwrap();
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
        size: &str,
    ) -> Result<(), HotTierError> {
        let mut existing_hot_tier_used_size = 0;
        if self.check_stream_hot_tier_exists(stream) {
            //delete existing hot tier if its size is less than the updated hot tier size else return error
            let existing_hot_tier = self.get_hot_tier(stream).await?;
            existing_hot_tier_used_size =
                human_size_to_bytes(&existing_hot_tier.used_size.unwrap()).unwrap();
            if human_size_to_bytes(size) < human_size_to_bytes(&existing_hot_tier.size) {
                return Err(HotTierError::ObjectStorageError(ObjectStorageError::Custom(format!(
                    "The hot tier size for the stream is already set to {} which is greater than the updated hot tier size of {}, reducing the hot tier size is not allowed",
                    existing_hot_tier.size,
                    size
                ))));
            }
        }

        let (total_disk_space, available_disk_space, used_disk_space) = get_disk_usage();

        if let (Some(total_disk_space), Some(available_disk_space), Some(used_disk_space)) =
            (total_disk_space, available_disk_space, used_disk_space)
        {
            let stream_hot_tier_size = human_size_to_bytes(size).unwrap();
            let (total_hot_tier_size, total_hot_tier_used_size) =
                self.get_hot_tiers_size(stream).await?;
            let projected_disk_usage = total_hot_tier_size + stream_hot_tier_size + used_disk_space
                - existing_hot_tier_used_size
                - total_hot_tier_used_size;
            let usage_percentage =
                ((projected_disk_usage as f64 / total_disk_space as f64) * 100.0).round();
            if usage_percentage > CONFIG.parseable.max_disk_usage {
                return Err(HotTierError::ObjectStorageError(ObjectStorageError::Custom(format!(
                    "Including the hot tier size of all the streams, the projected disk usage will be {}% which is above the set threshold of {}%, hence unable to set the hot tier for the stream. Total Disk Size: {}, Available Disk Size: {}, Used Disk Size: {}, Total Hot Tier Size (all other streams): {}",
                    usage_percentage,
                    CONFIG.parseable.max_disk_usage,
                    bytes_to_human_size(total_disk_space),
                    bytes_to_human_size(available_disk_space),
                    bytes_to_human_size(used_disk_space),
                    bytes_to_human_size(total_hot_tier_size)
                ))));
            }
        }

        Ok(())
    }

    ///get the hot tier metadata file for the stream
    pub async fn get_hot_tier(&self, stream: &str) -> Result<StreamHotTier, HotTierError> {
        if !self.check_stream_hot_tier_exists(stream) {
            return Err(HotTierError::HotTierValidationError(
                HotTierValidationError::NotFound(stream.to_owned()),
            ));
        }
        let path = hot_tier_file_path(&self.hot_tier_path, stream)?;
        let res = self
            .filesystem
            .get(&path)
            .and_then(|resp| resp.bytes())
            .await;
        match res {
            Ok(bytes) => {
                let mut stream_hot_tier: StreamHotTier = serde_json::from_slice(&bytes)?;
                let oldest_date_time_entry = self.get_oldest_date_time_entry(stream).await?;
                stream_hot_tier.oldest_date_time_entry = oldest_date_time_entry;
                Ok(stream_hot_tier)
            }
            Err(err) => Err(err.into()),
        }
    }

    pub async fn delete_hot_tier(&self, stream: &str) -> Result<(), HotTierError> {
        if self.check_stream_hot_tier_exists(stream) {
            let path = self.hot_tier_path.join(stream);
            fs::remove_dir_all(path).await?;
            Ok(())
        } else {
            Err(HotTierError::HotTierValidationError(
                HotTierValidationError::NotFound(stream.to_owned()),
            ))
        }
    }

    ///put the hot tier metadata file for the stream
    /// set the updated_date_range in the hot tier metadata file
    pub async fn put_hot_tier(
        &self,
        stream: &str,
        hot_tier: &mut StreamHotTier,
    ) -> Result<(), HotTierError> {
        let path = hot_tier_file_path(&self.hot_tier_path, stream)?;
        let bytes = serde_json::to_vec(&hot_tier)?.into();
        self.filesystem.put(&path, bytes).await?;
        Ok(())
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
                    log::error!("Error in hot tier scheduler: {:?}", err);
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
        let streams = STREAM_INFO.list_streams();
        let sync_hot_tier_tasks = FuturesUnordered::new();
        for stream in streams {
            if self.check_stream_hot_tier_exists(&stream) {
                sync_hot_tier_tasks.push(async move { self.process_stream(stream).await });
                //self.process_stream(stream).await?;
            }
        }

        let res: Vec<_> = sync_hot_tier_tasks.collect().await;
        for res in res {
            if let Err(err) = res {
                log::error!("Failed to run hot tier sync task {err:?}");
                return Err(err);
            }
        }
        Ok(())
    }

    ///process the hot tier files for the stream
    /// delete the files from the hot tier directory if the available date range is outside the hot tier range
    async fn process_stream(&self, stream: String) -> Result<(), HotTierError> {
        let stream_hot_tier = self.get_hot_tier(&stream).await?;
        let mut parquet_file_size =
            human_size_to_bytes(stream_hot_tier.used_size.as_ref().unwrap()).unwrap();

        let object_store = CONFIG.storage().get_object_store();
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
        object_store: Arc<dyn ObjectStorage + Send>,
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
                        log::warn!("Invalid date format: {}", str_date);
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
            || human_size_to_bytes(&stream_hot_tier.available_size.clone().unwrap()).unwrap()
                <= parquet_file.file_size
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
            *parquet_file_size =
                human_size_to_bytes(&stream_hot_tier.used_size.clone().unwrap()).unwrap();
        }
        let parquet_file_path = RelativePathBuf::from(parquet_file.file_path.clone());
        fs::create_dir_all(parquet_path.parent().unwrap()).await?;
        let mut file = fs::File::create(parquet_path.clone()).await?;
        let parquet_data = CONFIG
            .storage()
            .get_object_store()
            .get_object(&parquet_file_path)
            .await?;
        file.write_all(&parquet_data).await?;
        *parquet_file_size += parquet_file.file_size;
        stream_hot_tier.used_size = Some(bytes_to_human_size(*parquet_file_size));

        stream_hot_tier.available_size = Some(bytes_to_human_size(
            human_size_to_bytes(&stream_hot_tier.available_size.clone().unwrap()).unwrap()
                - parquet_file.file_size,
        ));
        self.put_hot_tier(stream, &mut stream_hot_tier).await?;
        file_processed = true;
        let mut hot_tier_manifest = self
            .get_stream_hot_tier_manifest_for_date(stream, &date)
            .await?;
        hot_tier_manifest.files.push(parquet_file.clone());
        // write the manifest file to the hot tier directory
        let manifest_path = self
            .hot_tier_path
            .join(stream)
            .join(format!("date={}/hottier.manifest.json", date));
        fs::create_dir_all(manifest_path.parent().unwrap()).await?;
        fs::write(manifest_path, serde_json::to_vec(&hot_tier_manifest)?).await?;
        Ok(file_processed)
    }

    #[allow(dead_code)]
    ///delete the files for the date range given from the hot tier directory for the stream
    /// update the used and available size in the hot tier metadata
    pub async fn delete_files_from_hot_tier(
        &self,
        stream: &str,
        dates: &[NaiveDate],
    ) -> Result<(), HotTierError> {
        for date in dates.iter() {
            let path = self.hot_tier_path.join(format!("{}/date={}", stream, date));
            if path.exists() {
                fs::remove_dir_all(path.clone()).await?;
            }
        }

        Ok(())
    }

    ///fetch the list of dates available in the hot tier directory for the stream and sort them
    pub async fn fetch_hot_tier_dates(&self, stream: &str) -> Result<Vec<NaiveDate>, HotTierError> {
        let mut date_list = Vec::new();
        let path = self.hot_tier_path.join(stream);
        if path.exists() {
            let directories = ReadDirStream::new(fs::read_dir(&path).await?);
            let dates: Vec<DirEntry> = directories.try_collect().await?;
            for date in dates {
                if !date.path().is_dir() {
                    continue;
                }
                let date = date.file_name().into_string().unwrap();
                date_list.push(
                    NaiveDate::parse_from_str(date.trim_start_matches("date="), "%Y-%m-%d")
                        .unwrap(),
                );
            }
        }
        date_list.sort();
        Ok(date_list)
    }

    ///get hot tier manifest for the stream and date
    pub async fn get_stream_hot_tier_manifest_for_date(
        &self,
        stream: &str,
        date: &NaiveDate,
    ) -> Result<Manifest, HotTierError> {
        let mut hot_tier_manifest = Manifest::default();
        let path = self
            .hot_tier_path
            .join(stream)
            .join(format!("date={}", date));
        if path.exists() {
            let date_dirs = ReadDirStream::new(fs::read_dir(&path).await?);
            let manifest_files: Vec<DirEntry> = date_dirs.try_collect().await?;
            for manifest in manifest_files {
                if !manifest
                    .file_name()
                    .to_string_lossy()
                    .ends_with(".manifest.json")
                {
                    continue;
                }
                let file = fs::read(manifest.path()).await?;
                let manifest: Manifest = serde_json::from_slice(&file)?;
                hot_tier_manifest.files.extend(manifest.files);
            }
        }
        Ok(hot_tier_manifest)
    }

    ///get the list of files from all the manifests present in hot tier directory for the stream
    pub async fn get_hot_tier_manifest_files(
        &self,
        stream: &str,
        manifest_files: Vec<File>,
    ) -> Result<(Vec<File>, Vec<File>), HotTierError> {
        let mut hot_tier_files = self.get_hot_tier_parquet_files(stream).await?;
        hot_tier_files.retain(|file| {
            manifest_files
                .iter()
                .any(|manifest_file| manifest_file.file_path.eq(&file.file_path))
        });
        let remaining_files: Vec<File> = manifest_files
            .into_iter()
            .filter(|manifest_file| {
                hot_tier_files
                    .iter()
                    .all(|file| !file.file_path.eq(&manifest_file.file_path))
            })
            .collect();
        Ok((hot_tier_files, remaining_files))
    }

    ///get the list of parquet files from the hot tier directory for the stream
    pub async fn get_hot_tier_parquet_files(
        &self,
        stream: &str,
    ) -> Result<Vec<File>, HotTierError> {
        let mut hot_tier_parquet_files: Vec<File> = Vec::new();
        let date_list = self.fetch_hot_tier_dates(stream).await?;
        for date in date_list {
            let manifest = self
                .get_stream_hot_tier_manifest_for_date(stream, &date)
                .await?;

            for parquet_file in manifest.files {
                hot_tier_parquet_files.push(parquet_file.clone());
            }
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
            let date_str = date.to_string();
            let path = &self
                .hot_tier_path
                .join(stream)
                .join(format!("date={}", date_str));
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
                    let path_to_delete = CONFIG
                        .parseable
                        .hot_tier_storage_path
                        .as_ref()
                        .unwrap()
                        .join(&file_to_delete.file_path);

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
                        delete_empty_directory_hot_tier(path_to_delete.parent().unwrap()).await?;

                        stream_hot_tier.used_size = Some(bytes_to_human_size(
                            human_size_to_bytes(&stream_hot_tier.used_size.clone().unwrap())
                                .unwrap()
                                - file_size,
                        ));
                        stream_hot_tier.available_size = Some(bytes_to_human_size(
                            human_size_to_bytes(&stream_hot_tier.available_size.clone().unwrap())
                                .unwrap()
                                + file_size,
                        ));
                        self.put_hot_tier(stream, stream_hot_tier).await?;
                        delete_successful = true;

                        if human_size_to_bytes(&stream_hot_tier.available_size.clone().unwrap())
                            .unwrap()
                            <= parquet_file_size
                        {
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
        let (total_disk_space, available_disk_space, used_disk_space) = get_disk_usage();

        if let (Some(total_disk_space), Some(available_disk_space), Some(used_disk_space)) =
            (total_disk_space, available_disk_space, used_disk_space)
        {
            if available_disk_space < size_to_download {
                return Ok(false);
            }

            if ((used_disk_space + size_to_download) as f64 * 100.0 / total_disk_space as f64)
                > CONFIG.parseable.max_disk_usage
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
            let path = self
                .hot_tier_path
                .join(stream)
                .join(format!("date={}", date));

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
                    let oldest_date_time = format!("{} {}:{}:00", date, hour_str, minute_str);
                    return Ok(Some(oldest_date_time));
                }
            }
        }

        Ok(None)
    }
}

/// get the hot tier file path for the stream
pub fn hot_tier_file_path(
    root: impl AsRef<std::path::Path>,
    stream: &str,
) -> Result<object_store::path::Path, object_store::path::Error> {
    let path = root.as_ref().join(stream).join(STREAM_HOT_TIER_FILENAME);
    object_store::path::Path::from_absolute_path(path)
}

///get the disk usage for the hot tier storage path
pub fn get_disk_usage() -> (Option<u64>, Option<u64>, Option<u64>) {
    let mut sys = System::new_all();
    sys.refresh_all();
    let path = CONFIG.parseable.hot_tier_storage_path.as_ref().unwrap();

    let mut disks = Disks::new_with_refreshed_list();
    disks.sort_by_key(|disk| disk.mount_point().to_str().unwrap().len());
    disks.reverse();

    for disk in disks.iter() {
        if path.starts_with(disk.mount_point().to_str().unwrap()) {
            let total_disk_space = disk.total_space();
            let available_disk_space = disk.available_space();
            let used_disk_space = total_disk_space - available_disk_space;
            return (
                Some(total_disk_space),
                Some(available_disk_space),
                Some(used_disk_space),
            );
        }
    }

    (None, None, None)
}

async fn delete_empty_directory_hot_tier(path: &Path) -> io::Result<()> {
    async fn delete_helper(path: &Path) -> io::Result<()> {
        if path.is_dir() {
            let mut read_dir = fs::read_dir(path).await?;
            let mut subdirs = vec![];

            while let Some(entry) = read_dir.next_entry().await? {
                let entry_path = entry.path();
                if entry_path.is_dir() {
                    subdirs.push(entry_path);
                }
            }
            let mut tasks = vec![];
            for subdir in &subdirs {
                tasks.push(delete_empty_directory_hot_tier(subdir));
            }
            futures::stream::iter(tasks)
                .buffer_unordered(10)
                .try_collect::<Vec<_>>()
                .await?;

            // Re-check the directory after deleting its subdirectories
            let mut read_dir = fs::read_dir(path).await?;
            if read_dir.next_entry().await?.is_none() {
                fs::remove_dir(path).await?;
            }
        }
        Ok(())
    }
    delete_helper(path).await
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
    MetadataError(#[from] MetadataError),
    #[error("{0}")]
    HotTierValidationError(#[from] HotTierValidationError),
    #[error("{0}")]
    Anyhow(#[from] anyhow::Error),
}
