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

use std::sync::Arc;

use chrono::{DateTime, Local, NaiveTime, Utc};
use column::Column;
use manifest::Manifest;
use relative_path::RelativePathBuf;
use snapshot::ManifestItem;
use std::io::Error as IOError;
use tracing::{error, info};

use crate::{
    event::DEFAULT_TIMESTAMP_KEY,
    handlers::{
        self,
        http::{
            base_path_without_preceding_slash,
            modal::{NodeMetadata, NodeType},
        },
    },
    metrics::{EVENTS_INGESTED_DATE, EVENTS_INGESTED_SIZE_DATE, EVENTS_STORAGE_SIZE_DATE},
    option::Mode,
    parseable::PARSEABLE,
    query::PartialTimeFilter,
    stats::{event_labels_date, get_current_stats, storage_size_labels_date, update_deleted_stats},
    storage::{
        ObjectStorage, ObjectStorageError, ObjectStoreFormat, object_storage::manifest_path,
    },
};
pub use manifest::create_from_parquet_file;

pub mod column;
pub mod manifest;
pub mod snapshot;
pub trait Snapshot {
    fn manifests(&self, time_predicates: &[PartialTimeFilter]) -> Vec<ManifestItem>;
}

pub trait ManifestFile {
    #[allow(unused)]
    fn file_name(&self) -> &str;
    #[allow(unused)]
    fn ingestion_size(&self) -> u64;
    #[allow(unused)]
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

fn get_file_bounds(
    file: &manifest::File,
    partition_column: String,
) -> (DateTime<Utc>, DateTime<Utc>) {
    match file
        .columns()
        .iter()
        .find(|col| col.name == partition_column)
        .unwrap()
        .stats
        .as_ref()
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
    storage: Arc<dyn ObjectStorage>,
    stream_name: &str,
    change: manifest::File,
) -> Result<(), ObjectStorageError> {
    let mut meta = storage.get_object_store_format(stream_name).await?;
    let manifests = &mut meta.snapshot.manifest_list;
    let time_partition = &meta.time_partition;
    let lower_bound = match time_partition {
        Some(time_partition) => {
            let (lower_bound, _) = get_file_bounds(&change, time_partition.to_string());
            lower_bound
        }
        None => {
            let (lower_bound, _) = get_file_bounds(&change, DEFAULT_TIMESTAMP_KEY.to_string());
            lower_bound
        }
    };
    let date = lower_bound.date_naive().format("%Y-%m-%d").to_string();
    let event_labels = event_labels_date(stream_name, "json", &date);
    let storage_size_labels = storage_size_labels_date(stream_name, &date);
    let events_ingested = EVENTS_INGESTED_DATE
        .get_metric_with_label_values(&event_labels)
        .unwrap()
        .get() as u64;
    let ingestion_size = EVENTS_INGESTED_SIZE_DATE
        .get_metric_with_label_values(&event_labels)
        .unwrap()
        .get() as u64;
    let storage_size = EVENTS_STORAGE_SIZE_DATE
        .get_metric_with_label_values(&storage_size_labels)
        .unwrap()
        .get() as u64;
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
        for m in manifests.iter_mut() {
            let p = manifest_path("").to_string();
            if m.manifest_path.contains(&p) {
                let date = m
                    .time_lower_bound
                    .date_naive()
                    .format("%Y-%m-%d")
                    .to_string();
                let event_labels = event_labels_date(stream_name, "json", &date);
                let storage_size_labels = storage_size_labels_date(stream_name, &date);
                let events_ingested = EVENTS_INGESTED_DATE
                    .get_metric_with_label_values(&event_labels)
                    .unwrap()
                    .get() as u64;
                let ingestion_size = EVENTS_INGESTED_SIZE_DATE
                    .get_metric_with_label_values(&event_labels)
                    .unwrap()
                    .get() as u64;
                let storage_size = EVENTS_STORAGE_SIZE_DATE
                    .get_metric_with_label_values(&storage_size_labels)
                    .unwrap()
                    .get() as u64;
                ch = true;
                m.events_ingested = events_ingested;
                m.ingestion_size = ingestion_size;
                m.storage_size = storage_size;
            }
        }

        if ch {
            if let Some(mut manifest) = storage.get_manifest(&path).await? {
                manifest.apply_change(change);
                storage.put_manifest(&path, manifest).await?;
                let stats = get_current_stats(stream_name, "json");
                if let Some(stats) = stats {
                    meta.stats = stats;
                }
                meta.snapshot.manifest_list = manifests.to_vec();

                storage.put_stream_manifest(stream_name, &meta).await?;
            } else {
                //instead of returning an error, create a new manifest (otherwise local to storage sync fails)
                //but don't update the snapshot
                create_manifest(
                    lower_bound,
                    change,
                    storage.clone(),
                    stream_name,
                    false,
                    meta,
                    events_ingested,
                    ingestion_size,
                    storage_size,
                )
                .await?;
            }
        } else {
            create_manifest(
                lower_bound,
                change,
                storage.clone(),
                stream_name,
                true,
                meta,
                events_ingested,
                ingestion_size,
                storage_size,
            )
            .await?;
        }
    } else {
        create_manifest(
            lower_bound,
            change,
            storage.clone(),
            stream_name,
            true,
            meta,
            events_ingested,
            ingestion_size,
            storage_size,
        )
        .await?;
    }

    Ok(())
}

#[allow(clippy::too_many_arguments)]
async fn create_manifest(
    lower_bound: DateTime<Utc>,
    change: manifest::File,
    storage: Arc<dyn ObjectStorage>,
    stream_name: &str,
    update_snapshot: bool,
    mut meta: ObjectStoreFormat,
    events_ingested: u64,
    ingestion_size: u64,
    storage_size: u64,
) -> Result<(), ObjectStorageError> {
    let lower_bound = lower_bound.date_naive().and_time(NaiveTime::MIN).and_utc();
    let upper_bound = lower_bound
        .date_naive()
        .and_time(
            NaiveTime::from_num_seconds_from_midnight_opt(23 * 3600 + 59 * 60 + 59, 999_999_999)
                .ok_or(IOError::other("Failed to create upper bound for manifest"))?,
        )
        .and_utc();

    let manifest = Manifest {
        files: vec![change],
        ..Manifest::default()
    };
    let mut first_event_at = PARSEABLE.get_stream(stream_name)?.get_first_event();
    if first_event_at.is_none() {
        if let Some(first_event) = manifest.files.first() {
            let time_partition = &meta.time_partition;
            let lower_bound = match time_partition {
                Some(time_partition) => {
                    let (lower_bound, _) = get_file_bounds(first_event, time_partition.to_string());
                    lower_bound
                }
                None => {
                    let (lower_bound, _) =
                        get_file_bounds(first_event, DEFAULT_TIMESTAMP_KEY.to_string());
                    lower_bound
                }
            };
            first_event_at = Some(lower_bound.with_timezone(&Local).to_rfc3339());
            match PARSEABLE.get_stream(stream_name) {
                Ok(stream) => stream.set_first_event_at(first_event_at.as_ref().unwrap()),
                Err(err) => error!(
                    "Failed to update first_event_at in streaminfo for stream {stream_name:?}, error = {err:?}"
                ),
            }
        }
    }

    let mainfest_file_name = manifest_path("").to_string();
    let path = partition_path(stream_name, lower_bound, upper_bound).join(&mainfest_file_name);
    storage
        .put_object(&path, serde_json::to_vec(&manifest)?.into())
        .await?;
    if update_snapshot {
        let mut manifests = meta.snapshot.manifest_list;
        let path = storage.absolute_url(&path);
        let new_snapshot_entry = snapshot::ManifestItem {
            manifest_path: path.to_string(),
            time_lower_bound: lower_bound,
            time_upper_bound: upper_bound,
            events_ingested,
            ingestion_size,
            storage_size,
        };
        manifests.push(new_snapshot_entry);
        meta.snapshot.manifest_list = manifests;
        let stats = get_current_stats(stream_name, "json");
        if let Some(stats) = stats {
            meta.stats = stats;
        }
        meta.first_event_at = first_event_at;
        storage.put_stream_manifest(stream_name, &meta).await?;
    }

    Ok(())
}

pub async fn remove_manifest_from_snapshot(
    storage: Arc<dyn ObjectStorage>,
    stream_name: &str,
    dates: Vec<String>,
) -> Result<Option<String>, ObjectStorageError> {
    if !dates.is_empty() {
        // get current snapshot
        let mut meta = storage.get_object_store_format(stream_name).await?;
        let meta_for_stats = meta.clone();
        update_deleted_stats(storage.clone(), stream_name, meta_for_stats, dates.clone()).await?;
        let manifests = &mut meta.snapshot.manifest_list;
        // Filter out items whose manifest_path contains any of the dates_to_delete
        manifests.retain(|item| !dates.iter().any(|date| item.manifest_path.contains(date)));
        PARSEABLE.get_stream(stream_name)?.reset_first_event_at();
        meta.first_event_at = None;
        storage.put_snapshot(stream_name, meta.snapshot).await?;
    }

    // retention is initiated from the querier
    // request is forwarded to all ingestors to clean up their manifests
    // no action required for the Index or Prism nodes
    match PARSEABLE.options.mode {
        Mode::All | Mode::Ingest => {
            Ok(get_first_event(storage.clone(), stream_name, Vec::new()).await?)
        }
        Mode::Query => Ok(get_first_event(storage, stream_name, dates).await?),
        Mode::Index | Mode::Prism => Err(ObjectStorageError::UnhandledError(Box::new(
            std::io::Error::new(
                std::io::ErrorKind::Unsupported,
                "Can't remove manifest from within Index or Prism server",
            ),
        ))),
    }
}

pub async fn get_first_event(
    storage: Arc<dyn ObjectStorage>,
    stream_name: &str,
    dates: Vec<String>,
) -> Result<Option<String>, ObjectStorageError> {
    let mut first_event_at: String = String::default();
    match PARSEABLE.options.mode {
        Mode::All | Mode::Ingest => {
            // get current snapshot
            let stream_first_event = PARSEABLE.get_stream(stream_name)?.get_first_event();
            if stream_first_event.is_some() {
                first_event_at = stream_first_event.unwrap();
            } else {
                let mut meta = storage.get_object_store_format(stream_name).await?;
                let meta_clone = meta.clone();
                let manifests = meta_clone.snapshot.manifest_list;
                let time_partition = meta_clone.time_partition;
                if manifests.is_empty() {
                    info!("No manifest found for stream {stream_name}");
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
                    let lower_bound = match time_partition {
                        Some(time_partition) => {
                            let (lower_bound, _) = get_file_bounds(first_event, time_partition);
                            lower_bound
                        }
                        None => {
                            let (lower_bound, _) =
                                get_file_bounds(first_event, DEFAULT_TIMESTAMP_KEY.to_string());
                            lower_bound
                        }
                    };
                    first_event_at = lower_bound.with_timezone(&Local).to_rfc3339();
                    meta.first_event_at = Some(first_event_at.clone());
                    storage.put_stream_manifest(stream_name, &meta).await?;
                    PARSEABLE
                        .get_stream(stream_name)?
                        .set_first_event_at(&first_event_at);
                }
            }
        }
        Mode::Query => {
            let ingestor_metadata: Vec<NodeMetadata> =
                handlers::http::cluster::get_node_info(NodeType::Ingestor)
                    .await
                    .map_err(|err| {
                        error!("Fatal: failed to get ingestor info: {:?}", err);
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
                let ingestor_first_event_at =
                    handlers::http::cluster::send_retention_cleanup_request(
                        &url,
                        ingestor.clone(),
                        &dates,
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
        _ => {}
    }

    Ok(Some(first_event_at))
}

/// Partition the path to which this manifest belongs.
/// Useful when uploading the manifest file.
pub fn partition_path(
    stream: &str,
    lower_bound: DateTime<Utc>,
    upper_bound: DateTime<Utc>,
) -> RelativePathBuf {
    let lower = lower_bound.date_naive().format("%Y-%m-%d").to_string();
    let upper = upper_bound.date_naive().format("%Y-%m-%d").to_string();
    if lower == upper {
        RelativePathBuf::from_iter([stream, &format!("date={lower}")])
    } else {
        RelativePathBuf::from_iter([stream, &format!("date={lower}:{upper}")])
    }
}
