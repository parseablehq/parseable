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
    utils::{extract_datetime, get_dir_size},
    validator::{error::HotTierValidationError, parse_human_date},
};
use chrono::{NaiveDate, Utc};
use clokwerk::{AsyncScheduler, Interval, Job};
use futures::TryStreamExt;
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

const HOT_TIER_SYNC_DURATION: Interval = clokwerk::Interval::Minutes(1);

#[derive(Debug, serde::Deserialize, serde::Serialize)]
pub struct StreamHotTier {
    #[serde(rename = "size")]
    pub size: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub used_size: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub available_size: Option<String>,
    #[serde(rename = "start_date")]
    pub start_date: String,
    #[serde(rename = "end_date")]
    pub end_date: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub updated_date_range: Option<Vec<String>>,
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

    ///validate the hot tier configuration, check the disk availability
    /// update the hot tier range if the disk is not available to download the entire set
    /// delete the existing hot tier files once validation is successful
    pub async fn validate(
        &self,
        stream: &str,
        stream_hot_tier: &mut StreamHotTier,
    ) -> Result<(), HotTierError> {
        async fn validate_helper(
            this: &HotTierManager,
            stream: &str,
            stream_hot_tier: &mut StreamHotTier,
        ) -> Result<(), HotTierError> {
            let date_list = this.get_date_list(
                &stream_hot_tier.start_date,
                &stream_hot_tier.end_date,
                &stream_hot_tier.updated_date_range,
            )?;
            let object_store = CONFIG.storage().get_object_store();
            let s3_file_list = object_store.list_files(stream).await?;
            let mut manifest_list = Vec::new();
            let mut total_size_to_download = 0;

            for date in &date_list {
                let date_str = date.to_string();
                let manifest_files_to_download = s3_file_list
                    .iter()
                    .filter(|file| file.starts_with(&format!("{}/date={}", stream, date_str)))
                    .collect::<Vec<_>>();

                for file in manifest_files_to_download {
                    let manifest_path: RelativePathBuf = RelativePathBuf::from(file);
                    let manifest_file = object_store.get_object(&manifest_path).await?;
                    let manifest: Manifest = serde_json::from_slice(&manifest_file)?;
                    manifest_list.push(manifest.clone());
                }
            }

            for manifest in &manifest_list {
                total_size_to_download += manifest
                    .files
                    .iter()
                    .map(|file| file.file_size)
                    .sum::<u64>();
            }

            if (!this
                .is_disk_available(total_size_to_download, &stream_hot_tier.size)
                .await?
                || human_size_to_bytes(&stream_hot_tier.size).unwrap() < total_size_to_download)
                && date_list.len() > 1
            {
                stream_hot_tier.updated_date_range = Some(
                    date_list
                        .iter()
                        .skip(1)
                        .map(|date| date.to_string())
                        .collect(),
                );
                return Box::pin(validate_helper(this, stream, stream_hot_tier)).await;
            }

            if let Ok(mut existing_hot_tier) = this.get_hot_tier(stream).await {
                let available_date_list = this.fetch_hot_tier_dates(stream).await?;
                this.delete_files_from_hot_tier(
                    &mut existing_hot_tier,
                    stream,
                    &available_date_list,
                    true,
                )
                .await?;
            }

            Ok(())
        }

        validate_helper(self, stream, stream_hot_tier).await
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
            Ok(bytes) => serde_json::from_slice(&bytes).map_err(Into::into),
            Err(err) => Err(err.into()),
        }
    }

    ///put the hot tier metadata file for the stream
    /// set the updated_date_range in the hot tier metadata file
    pub async fn put_hot_tier(
        &self,
        stream: &str,
        hot_tier: &mut StreamHotTier,
    ) -> Result<(), HotTierError> {
        let date_list = if let Some(updated_date_range) = &hot_tier.updated_date_range {
            updated_date_range
                .iter()
                .map(|date| NaiveDate::parse_from_str(date, "%Y-%m-%d").unwrap())
                .collect()
        } else {
            self.get_date_list(&hot_tier.start_date, &hot_tier.end_date, &None)?
        };
        hot_tier.updated_date_range = date_list
            .iter()
            .map(|date| date.to_string())
            .collect::<Vec<String>>()
            .into();
        let path = hot_tier_file_path(&self.hot_tier_path, stream)?;
        let bytes = serde_json::to_vec(hot_tier)?.into();
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
        for stream in streams {
            if self.check_stream_hot_tier_exists(&stream) {
                self.process_stream(stream).await?;
            }
        }
        Ok(())
    }

    ///process the hot tier files for the stream
    /// delete the files from the hot tier directory if the available date range is outside the hot tier range
    async fn process_stream(&self, stream: String) -> Result<(), HotTierError> {
        let mut stream_hot_tier = self.get_hot_tier(&stream).await?;
        let mut parquet_file_size =
            human_size_to_bytes(stream_hot_tier.used_size.as_ref().unwrap()).unwrap();
        let date_list = self.get_hot_tier_time_range(&stream_hot_tier).await?;
        let available_date_list = self.fetch_hot_tier_dates(&stream).await?;
        let dates_to_delete: Vec<NaiveDate> = available_date_list
            .into_iter()
            .filter(|available_date| !date_list.contains(available_date))
            .collect();
        if !dates_to_delete.is_empty() {
            self.delete_files_from_hot_tier(&mut stream_hot_tier, &stream, &dates_to_delete, false)
                .await?;
        }
        let object_store = CONFIG.storage().get_object_store();
        let s3_file_list = object_store.list_files(&stream).await?;

        for date in date_list {
            self.process_date(
                &stream,
                &mut stream_hot_tier,
                &s3_file_list,
                date,
                &mut parquet_file_size,
                object_store.clone(),
            )
            .await?;
            self.put_hot_tier(&stream, &mut stream_hot_tier).await?;
        }

        Ok(())
    }

    ///get the list of date range from hot tier metadata
    /// if updated_date_range is available, get the list of dates from the updated_date_range
    /// else get the list of dates from the start and end date
    fn get_date_list(
        &self,
        start_date: &str,
        end_date: &str,
        updated_date_range: &Option<Vec<String>>,
    ) -> Result<Vec<NaiveDate>, HotTierError> {
        let (dt_start_date, dt_end_date) = if let Some(updated_date_range) = updated_date_range {
            parse_human_date(
                updated_date_range.first().unwrap(),
                updated_date_range.last().unwrap(),
            )?
        } else {
            parse_human_date(start_date, end_date)?
        };

        let date_list: Vec<NaiveDate> = (0..)
            .map(|i| dt_start_date + chrono::Duration::days(i))
            .take_while(|&date| date <= dt_end_date)
            .collect();

        Ok(date_list)
    }

    ///process the hot tier files for the date for the stream
    /// collect all manifests from S3 for the date, sort the parquet file list
    /// in order to download the latest files first
    /// download the parquet files if not present in hot tier directory
    async fn process_date(
        &self,
        stream: &str,
        stream_hot_tier: &mut StreamHotTier,
        s3_file_list: &[String],
        date: NaiveDate,
        parquet_file_size: &mut u64,
        object_store: Arc<dyn ObjectStorage + Send>,
    ) -> Result<(), HotTierError> {
        let date_str = date.to_string();
        let available_date_list = self.fetch_hot_tier_dates(stream).await?;
        if available_date_list.contains(&date) && !date.eq(&Utc::now().date_naive()) {
            return Ok(());
        }
        let manifest_files_to_download = s3_file_list
            .iter()
            .filter(|file| file.starts_with(&format!("{}/date={}", stream, date_str)))
            .collect::<Vec<_>>();
        if manifest_files_to_download.is_empty() {
            self.update_hot_tier_time_range(stream, stream_hot_tier, &date_str)
                .await?;
        }
        let mut manifest_list = Vec::new();
        for file in manifest_files_to_download {
            let path = self.hot_tier_path.join(file);
            fs::create_dir_all(path.parent().unwrap()).await?;
            let manifest_path: RelativePathBuf = RelativePathBuf::from(file);
            let manifest_file = object_store.get_object(&manifest_path).await?;

            let mut file = fs::File::create(path.clone()).await?;
            file.write_all(&manifest_file).await?;
            let manifest: Manifest = serde_json::from_slice(&manifest_file)?;
            manifest_list.push(manifest.clone());
        }
        let mut parquet_file_list = Vec::new();
        for manifest in manifest_list {
            parquet_file_list.extend(manifest.files);
        }
        parquet_file_list.sort_by_key(|file| file.file_path.clone());
        parquet_file_list.reverse();
        for parquet_file in parquet_file_list {
            let parquet_file_path = &parquet_file.file_path;
            let parquet_path = self.hot_tier_path.join(parquet_file_path);
            if !parquet_path.exists() {
                if !self
                    .process_parquet_file(
                        stream,
                        stream_hot_tier,
                        &parquet_file,
                        parquet_file_size,
                        parquet_path,
                    )
                    .await?
                {
                    break;
                }
            } else {
                continue;
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
        stream_hot_tier: &mut StreamHotTier,
        parquet_file: &File,
        parquet_file_size: &mut u64,
        parquet_path: PathBuf,
    ) -> Result<bool, HotTierError> {
        let mut file_processed = false;
        if !self
            .is_disk_available(parquet_file.file_size, &stream_hot_tier.size)
            .await?
            || human_size_to_bytes(&stream_hot_tier.available_size.clone().unwrap()).unwrap()
                <= parquet_file.file_size
        {
            if !self
                .delete_1minute_data(
                    stream,
                    stream_hot_tier,
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
        file_processed = true;
        Ok(file_processed)
    }

    ///delete the files for the date range given from the hot tier directory for the stream
    /// update the used and available size in the hot tier metadata
    pub async fn delete_files_from_hot_tier(
        &self,
        stream_hot_tier: &mut StreamHotTier,
        stream: &str,
        dates: &[NaiveDate],
        validate: bool,
    ) -> Result<(), HotTierError> {
        for date in dates.iter() {
            let path = self.hot_tier_path.join(format!("{}/date={}", stream, date));
            if path.exists() {
                if !validate {
                    let size = get_dir_size(path.clone())?;
                    if self
                        .check_current_date_for_deletion(stream_hot_tier, stream)
                        .await?
                    {
                        return Err(HotTierError::ObjectStorageError(ObjectStorageError::Custom(
                            format!(
                                "Hot tier capacity for stream {} is exhausted (Total: {}, Available - {}). Today's data cannot be deleted. Please increase the hot tier size. Download will resume tomorrow"
                            , stream, stream_hot_tier.size, stream_hot_tier.available_size.clone().unwrap()) )));
                    }
                    stream_hot_tier.used_size = Some(bytes_to_human_size(
                        human_size_to_bytes(&stream_hot_tier.used_size.clone().unwrap()).unwrap()
                            - size,
                    ));
                    stream_hot_tier.available_size = Some(bytes_to_human_size(
                        human_size_to_bytes(&stream_hot_tier.available_size.clone().unwrap())
                            .unwrap()
                            + size,
                    ));
                    self.put_hot_tier(stream, stream_hot_tier).await?;
                }
                fs::remove_dir_all(path.clone()).await?;
            }
            self.update_hot_tier_time_range(stream, stream_hot_tier, &date.to_string())
                .await?;
        }

        Ok(())
    }

    ///check if the current date is the last date available in the hot tier directory
    async fn check_current_date_for_deletion(
        &self,
        stream_hot_tier: &StreamHotTier,
        stream: &str,
    ) -> Result<bool, HotTierError> {
        let current_date = Utc::now().date_naive();
        let (_, end_date) =
            parse_human_date(&stream_hot_tier.start_date, &stream_hot_tier.end_date)?;
        let is_end_date_today = end_date == current_date;
        let available_date_list = self.fetch_hot_tier_dates(stream).await?;
        let is_current_date_available = available_date_list.contains(&current_date);
        Ok(available_date_list.len() == 1 && is_current_date_available && is_end_date_today)
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

    ///update the hot tier time range for the stream
    /// remove the date given from the updated_date_range
    /// put the hot tier metadata file for the stream
    pub async fn update_hot_tier_time_range(
        &self,
        stream: &str,
        stream_hot_tier: &mut StreamHotTier,
        date: &str,
    ) -> Result<(), HotTierError> {
        if stream_hot_tier.updated_date_range.as_ref().unwrap().len() == 1
            && stream_hot_tier
                .updated_date_range
                .as_ref()
                .unwrap()
                .contains(&Utc::now().date_naive().to_string())
        {
            return Ok(());
        }
        let mut existing_date_range = stream_hot_tier.updated_date_range.as_ref().unwrap().clone();
        existing_date_range.retain(|d| d != date);
        stream_hot_tier.updated_date_range = Some(existing_date_range);

        self.put_hot_tier(stream, stream_hot_tier).await?;
        Ok(())
    }

    ///get the updated hot tier time range for the stream
    /// get the existing dates from the updated_date_range
    /// update the updated_date_range with the existing dates and the new dates
    pub async fn get_hot_tier_time_range(
        &self,
        stream_hot_tier: &StreamHotTier,
    ) -> Result<Vec<NaiveDate>, HotTierError> {
        let (start_date, end_date) =
            parse_human_date(&stream_hot_tier.start_date, &stream_hot_tier.end_date)?;
        let date_list: Vec<NaiveDate> = (0..)
            .map(|i| start_date + chrono::Duration::days(i))
            .take_while(|&date| date <= end_date)
            .collect();
        let mut existing_date_range: Vec<NaiveDate> = stream_hot_tier
            .updated_date_range
            .as_ref()
            .unwrap()
            .iter()
            .map(|date| NaiveDate::parse_from_str(date, "%Y-%m-%d").unwrap())
            .collect();
        existing_date_range.sort();
        let mut updated_date_range = vec![*date_list.last().unwrap()];
        updated_date_range.extend(
            date_list
                .into_iter()
                .filter(|date| existing_date_range.contains(date)),
        );
        updated_date_range.sort();
        updated_date_range.dedup();
        Ok(updated_date_range)
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
            let date_str = date.to_string();
            let path = &self
                .hot_tier_path
                .join(stream)
                .join(format!("date={}", date_str));
            if !path.exists() {
                continue;
            }

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

                for parquet_file in manifest.files {
                    hot_tier_parquet_files.push(parquet_file.clone());
                }
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
    pub async fn delete_1minute_data(
        &self,
        stream: &str,
        stream_hot_tier: &mut StreamHotTier,
        download_file_path: &Path,
        parquet_file_size: u64,
    ) -> Result<bool, HotTierError> {
        let mut delete_successful = false;
        let date_list = self.get_date_list(
            &stream_hot_tier.start_date,
            &stream_hot_tier.end_date,
            &stream_hot_tier.updated_date_range,
        )?;

        if let Some(date_to_delete) = date_list.first() {
            let date_str = date_to_delete.to_string();
            let path = self
                .hot_tier_path
                .join(stream)
                .join(format!("date={}", date_str));

            if !path.exists() {
                return Ok(delete_successful);
            }

            let date_dirs = ReadDirStream::new(fs::read_dir(&path).await?);
            let mut manifest_files: Vec<DirEntry> = date_dirs.try_collect().await?;
            manifest_files.retain(|file| {
                file.file_name()
                    .to_string_lossy()
                    .ends_with("manifest.json")
            });
            'loop_manifests: for manifest_file in manifest_files {
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
                        fs::remove_dir_all(&path_to_delete.parent().unwrap()).await?;

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
                            break 'loop_manifests;
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
    pub async fn is_disk_available(
        &self,
        size_to_download: u64,
        hot_tier_size: &str,
    ) -> Result<bool, HotTierError> {
        let (total_disk_space, available_disk_space, used_disk_space) = get_disk_usage();

        if let Some(available_space) = available_disk_space {
            let hot_tier_size = human_size_to_bytes(hot_tier_size).unwrap();
            if available_space < hot_tier_size {
                return Err(HotTierError::ObjectStorageError(ObjectStorageError::Custom(format!(
                    "Not enough space left in the disk for hot tier. Disk available Size: {}, Hot Tier Size: {}",
                    bytes_to_human_size(available_space),
                    bytes_to_human_size(hot_tier_size)
                ))));
            }

            if used_disk_space.unwrap() as f64 * 100.0 / total_disk_space.unwrap() as f64
                > CONFIG.parseable.max_disk_usage
            {
                return Err(HotTierError::ObjectStorageError(ObjectStorageError::Custom(format!(
                    "Disk usage is above the threshold. Disk Used Size: {}, Disk Total Size: {}, Disk Usage {}%",
                    bytes_to_human_size(used_disk_space.unwrap()),
                    bytes_to_human_size(total_disk_space.unwrap()),
                    used_disk_space.unwrap() as f64 * 100.0 / total_disk_space.unwrap() as f64
                ))));
            }
            if available_space < size_to_download {
                return Ok(false);
            }

            if ((used_disk_space.unwrap() + size_to_download) as f64 * 100.0
                / total_disk_space.unwrap() as f64)
                > CONFIG.parseable.max_disk_usage
            {
                return Ok(false);
            }
        }

        Ok(true)
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
