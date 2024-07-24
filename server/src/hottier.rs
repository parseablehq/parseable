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

use std::{io, path::PathBuf, sync::Arc};

use crate::{
    catalog::manifest::{File, Manifest},
    metadata::{error::stream_info::MetadataError, STREAM_INFO},
    option::{
        validation::{bytes_to_human_size, human_size_to_bytes},
        CONFIG,
    },
    storage::{ObjectStorage, ObjectStorageError},
    utils::get_dir_size,
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

    pub async fn validate(
        &self,
        stream: &str,
        stream_hot_tier: &StreamHotTier,
    ) -> Result<(), HotTierError> {
        let date_list =
            self.get_date_list(&stream_hot_tier.start_date, &stream_hot_tier.end_date)?;
        let object_store = CONFIG.storage().get_object_store();
        let s3_file_list = object_store.list_files(stream).await?;
        let mut manifest_list = Vec::new();
        let mut total_size_to_download = 0;
        for date in date_list {
            let date_str = date.to_string();
            let manifest_files_to_download = s3_file_list
                .iter()
                .filter(|file| file.starts_with(&format!("{}/date={}", stream, date_str)))
                .collect::<Vec<_>>();

            for file in manifest_files_to_download {
                let path = self.hot_tier_path.join(file);
                fs::create_dir_all(path.parent().unwrap()).await?;
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
        if human_size_to_bytes(&stream_hot_tier.size).unwrap() < total_size_to_download {
            return Err(HotTierError::ObjectStorageError(ObjectStorageError::Custom(
                format!(
                    "Total size required to download the files: {}. Provided hot tier size: {}. Not enough space in the hot tier. Please increase the hot tier size.",
                    bytes_to_human_size(total_size_to_download),
                    &stream_hot_tier.size
                ),
            )));
        }
        if let Ok(mut existing_hot_tier) = self.get_hot_tier(stream).await {
            let available_date_list = self.get_hot_tier_date_list(stream).await?;
            self.delete_from_hot_tier(&mut existing_hot_tier, stream, &available_date_list, true)
                .await?;
        }

        Ok(())
    }

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

    pub async fn put_hot_tier(
        &self,
        stream: &str,
        hot_tier: &StreamHotTier,
    ) -> Result<(), HotTierError> {
        let path = hot_tier_file_path(&self.hot_tier_path, stream)?;
        let bytes = serde_json::to_vec(hot_tier)?.into();
        self.filesystem.put(&path, bytes).await?;
        Ok(())
    }

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

    async fn sync_hot_tier(&self) -> Result<(), HotTierError> {
        let streams = STREAM_INFO.list_streams();
        for stream in streams {
            if self.check_stream_hot_tier_exists(&stream) {
                self.process_stream(stream).await?;
            }
        }
        Ok(())
    }

    async fn process_stream(&self, stream: String) -> Result<(), HotTierError> {
        let mut stream_hot_tier = self.get_hot_tier(&stream).await?;
        let mut parquet_file_size =
            human_size_to_bytes(stream_hot_tier.used_size.as_ref().unwrap()).unwrap();
        let date_list =
            self.get_date_list(&stream_hot_tier.start_date, &stream_hot_tier.end_date)?;
        let available_date_list = self.get_hot_tier_date_list(&stream).await?;
        let dates_to_delete: Vec<NaiveDate> = available_date_list
            .into_iter()
            .filter(|available_date| !date_list.contains(available_date))
            .collect();

        if !dates_to_delete.is_empty() {
            self.delete_from_hot_tier(&mut stream_hot_tier, &stream, &dates_to_delete, false)
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
            self.put_hot_tier(&stream, &stream_hot_tier).await?;
        }

        Ok(())
    }

    fn get_date_list(
        &self,
        start_date: &str,
        end_date: &str,
    ) -> Result<Vec<NaiveDate>, HotTierError> {
        let (start_date, end_date) = parse_human_date(start_date, end_date)?;
        let mut date_list = Vec::new();
        let mut current_date = start_date;

        while current_date <= end_date {
            date_list.push(current_date);
            current_date += chrono::Duration::days(1);
        }

        Ok(date_list)
    }

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
        let available_date_list = self.get_hot_tier_date_list(stream).await?;
        if available_date_list.contains(&date) && !date.eq(&Utc::now().date_naive()) {
            return Ok(());
        }
        let manifest_files_to_download = s3_file_list
            .iter()
            .filter(|file| file.starts_with(&format!("{}/date={}", stream, date_str)))
            .collect::<Vec<_>>();
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

        for manifest in manifest_list {
            for parquet_file in manifest.files {
                self.process_parquet_file(
                    stream,
                    stream_hot_tier,
                    &parquet_file,
                    parquet_file_size,
                )
                .await?;
            }
        }
        Ok(())
    }

    async fn process_parquet_file(
        &self,
        stream: &str,
        stream_hot_tier: &mut StreamHotTier,
        parquet_file: &File,
        parquet_file_size: &mut u64,
    ) -> Result<(), HotTierError> {
        let parquet_file_path = &parquet_file.file_path;
        let parquet_path = self.hot_tier_path.join(parquet_file_path);

        if !parquet_path.exists() {
            let parquet_file_path = RelativePathBuf::from(parquet_file_path);
            if human_size_to_bytes(&stream_hot_tier.available_size.clone().unwrap()).unwrap()
                <= parquet_file.file_size
            {
                let date_list = self.get_hot_tier_date_list(stream).await?;
                let date_to_delete = vec![*date_list.first().unwrap()];
                self.delete_from_hot_tier(stream_hot_tier, stream, &date_to_delete, false)
                    .await?;
                self.update_hot_tier(stream_hot_tier).await?;
                *parquet_file_size =
                    human_size_to_bytes(&stream_hot_tier.used_size.clone().unwrap()).unwrap();
            }

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
        }
        Ok(())
    }

    pub async fn delete_from_hot_tier(
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
                                "Hot tier capacity for stream {} is exhausted (Total: {}, Available - {})Today's data cannot be deleted. Please increase the hot tier size. Download will resume tomorrow"
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
                }
                fs::remove_dir_all(path.clone()).await?;
            }
        }

        Ok(())
    }

    async fn check_current_date_for_deletion(
        &self,
        stream_hot_tier: &StreamHotTier,
        stream: &str,
    ) -> Result<bool, HotTierError> {
        let current_date = Utc::now().date_naive();
        let (_, end_date) =
            parse_human_date(&stream_hot_tier.start_date, &stream_hot_tier.end_date)?;
        let is_end_date_today = end_date == current_date;
        let available_date_list = self.get_hot_tier_date_list(stream).await?;
        let is_current_date_available = available_date_list.contains(&current_date);
        Ok(available_date_list.len() == 1 && is_current_date_available && is_end_date_today)
    }

    pub async fn get_hot_tier_date_list(
        &self,
        stream: &str,
    ) -> Result<Vec<NaiveDate>, HotTierError> {
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
        Ok(date_list)
    }

    pub async fn update_hot_tier(
        &self,
        stream_hot_tier: &mut StreamHotTier,
    ) -> Result<(), HotTierError> {
        let start_date = &stream_hot_tier.start_date;
        let end_date = &stream_hot_tier.end_date;
        let mut date_list = self.get_date_list(start_date, end_date)?;

        date_list.retain(|date: &NaiveDate| *date.to_string() != *start_date);
        stream_hot_tier.start_date = date_list.first().unwrap().to_string();
        Ok(())
    }

    pub async fn get_hot_tier_manifests(
        &self,
        stream: &str,
        manifest_files: Vec<File>,
    ) -> Result<(Vec<File>, Vec<File>), HotTierError> {
        let hot_tier_files = self.get_hot_tier_parquet_files(stream).await?;
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

    pub async fn get_hot_tier_parquet_files(
        &self,
        stream: &str,
    ) -> Result<Vec<File>, HotTierError> {
        let mut hot_tier_parquet_files: Vec<File> = Vec::new();
        let stream_hot_tier = self.get_hot_tier(stream).await?;
        let date_list =
            self.get_date_list(&stream_hot_tier.start_date, &stream_hot_tier.end_date)?;
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
    pub fn check_stream_hot_tier_exists(&self, stream: &str) -> bool {
        let path = self
            .hot_tier_path
            .join(stream)
            .join(STREAM_HOT_TIER_FILENAME);
        path.exists()
    }
}

pub fn hot_tier_file_path(
    root: impl AsRef<std::path::Path>,
    stream: &str,
) -> Result<object_store::path::Path, object_store::path::Error> {
    let path = root.as_ref().join(stream).join(STREAM_HOT_TIER_FILENAME);
    object_store::path::Path::from_absolute_path(path)
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
