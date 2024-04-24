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

use std::{io::ErrorKind, sync::Arc};

use self::{column::Column, snapshot::ManifestItem};
use crate::handlers::http::base_path_without_preceding_slash;
use crate::option::CONFIG;
use crate::{
    catalog::manifest::Manifest,
    query::PartialTimeFilter,
    storage::{object_storage::manifest_path, ObjectStorage, ObjectStorageError},
};
use crate::{handlers, Mode};
use bytes::Bytes;
use chrono::{DateTime, Local, NaiveTime, Utc};
use relative_path::RelativePathBuf;
use std::io::Error as IOError;
pub mod column;
pub mod manifest;
pub mod snapshot;

pub use manifest::create_from_parquet_file;

pub trait Snapshot {
    fn manifests(&self, time_predicates: &[PartialTimeFilter]) -> Vec<ManifestItem>;
}

pub trait ManifestFile {
    fn file_name(&self) -> &str;
    fn ingestion_size(&self) -> u64;
    fn file_size(&self) -> u64;
    fn num_rows(&self) -> u64;
    fn columns(&self) -> &[Column];
}

impl ManifestFile for manifest::File {
    fn file_name(&self) -> &str {
        &self.file_path
    }

    fn ingestion_size(&self) -> u64 {
        self.ingestion_size
    }

    fn file_size(&self) -> u64 {
        self.file_size
    }

    fn num_rows(&self) -> u64 {
        self.num_rows
    }

    fn columns(&self) -> &[Column] {
        self.columns.as_slice()
    }
}

fn get_file_bounds(file: &manifest::File) -> (DateTime<Utc>, DateTime<Utc>) {
    match file
        .columns()
        .iter()
        .find(|col| col.name == "p_timestamp")
        .unwrap()
        .stats
        .clone()
        .unwrap()
    {
        column::TypedStatistics::Int(stats) => (
            DateTime::from_timestamp_millis(stats.min).unwrap(),
            DateTime::from_timestamp_millis(stats.max).unwrap(),
        ),
        _ => unreachable!(),
    }
}

pub async fn update_snapshot(
    storage: Arc<dyn ObjectStorage + Send>,
    stream_name: &str,
    change: manifest::File,
) -> Result<(), ObjectStorageError> {
    // get current snapshot
    let mut meta = storage.get_object_store_format(stream_name).await?;
    let manifests = &mut meta.snapshot.manifest_list;

    let (lower_bound, _) = get_file_bounds(&change);
    let pos = manifests.iter().position(|item| {
        item.time_lower_bound <= lower_bound && lower_bound < item.time_upper_bound
    });

    // if the mode in I.S. manifest needs to be created but it is not getting created because
    // there is already a pos, to index into stream.json

    // We update the manifest referenced by this position
    // This updates an existing file so there is no need to create a snapshot entry.
    if let Some(pos) = pos {
        let info = &mut manifests[pos];
        let path = partition_path(stream_name, info.time_lower_bound, info.time_upper_bound);

        let mut ch = false;
        for m in manifests.iter() {
            let p = manifest_path("").to_string();
            if m.manifest_path.contains(&p) {
                ch = true;
            }
        }
        if ch {
            let Some(mut manifest) = storage.get_manifest(&path).await? else {
                return Err(ObjectStorageError::UnhandledError(
                    "Manifest found in snapshot but not in object-storage"
                        .to_string()
                        .into(),
                ));
            };
            manifest.apply_change(change);
            storage.put_manifest(&path, manifest).await?;
        } else {
            let lower_bound = lower_bound.date_naive().and_time(NaiveTime::MIN).and_utc();
            let upper_bound = lower_bound
                .date_naive()
                .and_time(
                    NaiveTime::from_num_seconds_from_midnight_opt(
                        23 * 3600 + 59 * 60 + 59,
                        999_999_999,
                    )
                    .ok_or(IOError::new(
                        ErrorKind::Other,
                        "Failed to create upper bound for manifest",
                    ))
                    .map_err(ObjectStorageError::IoError)?,
                )
                .and_utc();

            let manifest = Manifest {
                files: vec![change],
                ..Manifest::default()
            };

            let mainfest_file_name = manifest_path("").to_string();
            let path =
                partition_path(stream_name, lower_bound, upper_bound).join(&mainfest_file_name);
            storage
                .put_object(&path, serde_json::to_vec(&manifest)?.into())
                .await?;
            let path = storage.absolute_url(&path);
            let new_snapshot_entriy = snapshot::ManifestItem {
                manifest_path: path.to_string(),
                time_lower_bound: lower_bound,
                time_upper_bound: upper_bound,
            };
            manifests.push(new_snapshot_entriy);
            storage.put_snapshot(stream_name, meta.snapshot).await?;
        }
    } else {
        let lower_bound = lower_bound.date_naive().and_time(NaiveTime::MIN).and_utc();
        let upper_bound = lower_bound
            .date_naive()
            .and_time(
                NaiveTime::from_num_seconds_from_midnight_opt(
                    23 * 3600 + 59 * 60 + 59,
                    999_999_999,
                )
                .unwrap(),
            )
            .and_utc();

        let manifest = Manifest {
            files: vec![change],
            ..Manifest::default()
        };

        let mainfest_file_name = manifest_path("").to_string();
        let path = partition_path(stream_name, lower_bound, upper_bound).join(&mainfest_file_name);
        storage
            .put_object(&path, serde_json::to_vec(&manifest).unwrap().into())
            .await?;
        let path = storage.absolute_url(&path);
        let new_snapshot_entriy = snapshot::ManifestItem {
            manifest_path: path.to_string(),
            time_lower_bound: lower_bound,
            time_upper_bound: upper_bound,
        };
        manifests.push(new_snapshot_entriy);
        storage.put_snapshot(stream_name, meta.snapshot).await?;
    }

    Ok(())
}

pub async fn remove_manifest_from_snapshot(
    storage: Arc<dyn ObjectStorage + Send>,
    stream_name: &str,
    dates: Vec<String>,
) -> Result<Option<String>, ObjectStorageError> {
    match CONFIG.parseable.mode {
        Mode::All | Mode::Ingest => {
            if !dates.is_empty() {
                // get current snapshot
                let mut meta = storage.get_object_store_format(stream_name).await?;
                let manifests = &mut meta.snapshot.manifest_list;
                // Filter out items whose manifest_path contains any of the dates_to_delete
                manifests
                    .retain(|item| !dates.iter().any(|date| item.manifest_path.contains(date)));
                storage.put_snapshot(stream_name, meta.snapshot).await?;
            }

            let first_event_at = get_first_event(storage.clone(), stream_name, Vec::new()).await?;

            Ok(first_event_at)
        }
        Mode::Query => Ok(get_first_event(storage, stream_name, dates).await?),
    }
}

pub async fn get_first_event(
    storage: Arc<dyn ObjectStorage + Send>,
    stream_name: &str,
    dates: Vec<String>,
) -> Result<Option<String>, ObjectStorageError> {
    let mut first_event_at: String = String::default();
    match CONFIG.parseable.mode {
        Mode::All | Mode::Ingest => {
            // get current snapshot
            let mut meta = storage.get_object_store_format(stream_name).await?;
            let manifests = &mut meta.snapshot.manifest_list;
            if manifests.is_empty() {
                log::info!("No manifest found for stream {stream_name}");
                return Err(ObjectStorageError::Custom("No manifest found".to_string()));
            }
            let manifest = &manifests[0];
            let path = partition_path(
                stream_name,
                manifest.time_lower_bound,
                manifest.time_upper_bound,
            );
            let Some(manifest) = storage.get_manifest(&path).await? else {
                return Err(ObjectStorageError::UnhandledError(
                    "Manifest found in snapshot but not in object-storage"
                        .to_string()
                        .into(),
                ));
            };
            if let Some(first_event) = manifest.files.first() {
                let (lower_bound, _) = get_file_bounds(first_event);
                first_event_at = lower_bound.with_timezone(&Local).to_rfc3339();
            }
        }
        Mode::Query => {
            let ingestor_metadata =
                handlers::http::cluster::get_ingestor_info()
                    .await
                    .map_err(|err| {
                        log::error!("Fatal: failed to get ingestor info: {:?}", err);
                        ObjectStorageError::from(err)
                    })?;
            let mut ingestors_first_event_at: Vec<String> = Vec::new();
            for ingestor in ingestor_metadata {
                let url = format!(
                    "{}{}/logstream/{}/retention/cleanup",
                    ingestor.domain_name,
                    base_path_without_preceding_slash(),
                    stream_name
                );
                // Convert dates vector to Bytes object
                let dates_bytes = Bytes::from(serde_json::to_vec(&dates).unwrap());
                // delete the stream

                let ingestor_first_event_at =
                    handlers::http::cluster::send_retention_cleanup_request(
                        &url,
                        ingestor.clone(),
                        dates_bytes,
                    )
                    .await?;
                if !ingestor_first_event_at.is_empty() {
                    ingestors_first_event_at.push(ingestor_first_event_at);
                }
            }
            if ingestors_first_event_at.is_empty() {
                return Ok(None);
            }
            first_event_at = ingestors_first_event_at.iter().min().unwrap().to_string();
        }
    }

    Ok(Some(first_event_at))
}

/// Partition the path to which this manifest belongs.
/// Useful when uploading the manifest file.
fn partition_path(
    stream: &str,
    lower_bound: DateTime<Utc>,
    upper_bound: DateTime<Utc>,
) -> RelativePathBuf {
    let lower = lower_bound.date_naive().format("%Y-%m-%d").to_string();
    let upper = upper_bound.date_naive().format("%Y-%m-%d").to_string();
    if lower == upper {
        RelativePathBuf::from_iter([stream, &format!("date={}", lower)])
    } else {
        RelativePathBuf::from_iter([stream, &format!("date={}:{}", lower, upper)])
    }
}
