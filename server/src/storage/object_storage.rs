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

use super::{
    file_link::CacheState, retention::Retention, LogStream, MoveDataError, ObjectStorageError,
    ObjectStoreFormat, Permisssion, StorageDir, StorageMetadata, CACHED_FILES,
};
use crate::{
    alerts::Alerts,
    metadata::STREAM_INFO,
    metrics::{storage::StorageMetrics, STAGING_FILES, STORAGE_SIZE},
    option::CONFIG,
    stats::Stats,
    utils::batch_adapter::adapt_batch,
};

use actix_web_prometheus::PrometheusMetrics;
use arrow_array::{RecordBatch, TimestampMillisecondArray};
use arrow_ipc::reader::StreamReader;
use arrow_schema::Schema;
use async_trait::async_trait;
use bytes::Bytes;
use datafusion::{
    datasource::listing::ListingTable, error::DataFusionError, execution::runtime_env::RuntimeEnv,
};
use itertools::kmerge_by;
use parquet::{
    arrow::ArrowWriter, basic::Encoding, file::properties::WriterProperties,
    schema::types::ColumnPath,
};
use relative_path::RelativePath;
use relative_path::RelativePathBuf;
use serde_json::Value;

use std::{
    collections::HashMap,
    fs::{self, File},
    path::{Path, PathBuf},
    process,
    sync::Arc,
};

// metadata file names in a Stream prefix
pub(super) const STREAM_METADATA_FILE_NAME: &str = ".stream.json";
pub(super) const PARSEABLE_METADATA_FILE_NAME: &str = ".parseable.json";
const SCHEMA_FILE_NAME: &str = ".schema";
const ALERT_FILE_NAME: &str = ".alert.json";

pub trait ObjectStorageProvider: StorageMetrics {
    fn get_datafusion_runtime(&self) -> Arc<RuntimeEnv>;
    fn get_object_store(&self) -> Arc<dyn ObjectStorage + Send>;
    fn get_endpoint(&self) -> String;
    fn register_store_metrics(&self, handler: &PrometheusMetrics);
}

#[async_trait]
pub trait ObjectStorage: Sync + 'static {
    async fn get_object(&self, path: &RelativePath) -> Result<Bytes, ObjectStorageError>;
    async fn put_object(
        &self,
        path: &RelativePath,
        resource: Bytes,
    ) -> Result<(), ObjectStorageError>;
    async fn delete_prefix(&self, path: &RelativePath) -> Result<(), ObjectStorageError>;
    async fn check(&self) -> Result<(), ObjectStorageError>;
    async fn delete_stream(&self, stream_name: &str) -> Result<(), ObjectStorageError>;
    async fn list_streams(&self) -> Result<Vec<LogStream>, ObjectStorageError>;
    async fn list_dates(&self, stream_name: &str) -> Result<Vec<String>, ObjectStorageError>;
    async fn upload_file(&self, key: &str, path: &Path) -> Result<(), ObjectStorageError>;
    fn query_table(
        &self,
        prefixes: Vec<String>,
        schema: Arc<Schema>,
    ) -> Result<Option<ListingTable>, DataFusionError>;

    async fn put_schema(
        &self,
        stream_name: &str,
        schema: &Schema,
    ) -> Result<(), ObjectStorageError> {
        self.put_object(&schema_path(stream_name), to_bytes(schema))
            .await?;

        Ok(())
    }

    async fn create_stream(&self, stream_name: &str) -> Result<(), ObjectStorageError> {
        let mut format = ObjectStoreFormat::default();
        format.set_id(CONFIG.parseable.username.clone());
        let permission = Permisssion::new(CONFIG.parseable.username.clone());
        format.permissions = vec![permission];

        let format_json = to_bytes(&format);

        self.put_object(&schema_path(stream_name), to_bytes(&Schema::empty()))
            .await?;

        self.put_object(&stream_json_path(stream_name), format_json)
            .await?;

        Ok(())
    }

    async fn put_alerts(
        &self,
        stream_name: &str,
        alerts: &Alerts,
    ) -> Result<(), ObjectStorageError> {
        self.put_object(&alert_json_path(stream_name), to_bytes(alerts))
            .await
    }

    async fn put_stats(&self, stream_name: &str, stats: &Stats) -> Result<(), ObjectStorageError> {
        let path = stream_json_path(stream_name);
        let stream_metadata = self.get_object(&path).await?;
        let stats = serde_json::to_value(stats).expect("stats are perfectly serializable");
        let mut stream_metadata: serde_json::Value =
            serde_json::from_slice(&stream_metadata).expect("parseable config is valid json");

        stream_metadata["stats"] = stats;

        self.put_object(&path, to_bytes(&stream_metadata)).await
    }

    async fn put_retention(
        &self,
        stream_name: &str,
        retention: &Retention,
    ) -> Result<(), ObjectStorageError> {
        let path = stream_json_path(stream_name);
        let stream_metadata = self.get_object(&path).await?;
        let stats =
            serde_json::to_value(retention).expect("rentention tasks are perfectly serializable");
        let mut stream_metadata: serde_json::Value =
            serde_json::from_slice(&stream_metadata).expect("parseable config is valid json");

        stream_metadata["retention"] = stats;

        self.put_object(&path, to_bytes(&stream_metadata)).await
    }

    async fn put_metadata(
        &self,
        parseable_metadata: &StorageMetadata,
    ) -> Result<(), ObjectStorageError> {
        self.put_object(&parseable_json_path(), to_bytes(parseable_metadata))
            .await
    }

    async fn get_schema(&self, stream_name: &str) -> Result<Schema, ObjectStorageError> {
        let schema_map = self.get_object(&schema_path(stream_name)).await?;
        Ok(serde_json::from_slice(&schema_map)?)
    }

    async fn get_alerts(&self, stream_name: &str) -> Result<Alerts, ObjectStorageError> {
        match self.get_object(&alert_json_path(stream_name)).await {
            Ok(alerts) => {
                if let Ok(alerts) = serde_json::from_slice(&alerts) {
                    Ok(alerts)
                } else {
                    log::error!("Incompatible alerts found for stream - {stream_name}. Refer https://www.parseable.io/docs/alerts for correct alert config.");
                    Ok(Alerts::default())
                }
            }
            Err(e) => match e {
                ObjectStorageError::NoSuchKey(_) => Ok(Alerts::default()),
                e => Err(e),
            },
        }
    }

    async fn get_stream_metadata(
        &self,
        stream_name: &str,
    ) -> Result<ObjectStoreFormat, ObjectStorageError> {
        let stream_metadata = self.get_object(&stream_json_path(stream_name)).await?;
        Ok(serde_json::from_slice(&stream_metadata).expect("parseable config is valid json"))
    }

    async fn get_stats(&self, stream_name: &str) -> Result<Stats, ObjectStorageError> {
        let stream_metadata = self.get_object(&stream_json_path(stream_name)).await?;
        let stream_metadata: Value =
            serde_json::from_slice(&stream_metadata).expect("parseable config is valid json");

        let stats = &stream_metadata["stats"];

        let stats = serde_json::from_value(stats.clone()).unwrap_or_default();

        Ok(stats)
    }

    async fn get_retention(&self, stream_name: &str) -> Result<Retention, ObjectStorageError> {
        let stream_metadata = self.get_object(&stream_json_path(stream_name)).await?;
        let stream_metadata: Value =
            serde_json::from_slice(&stream_metadata).expect("parseable config is valid json");

        let retention = stream_metadata
            .as_object()
            .expect("is object")
            .get("retention")
            .cloned();

        if let Some(retention) = retention {
            Ok(serde_json::from_value(retention).unwrap())
        } else {
            Ok(Retention::default())
        }
    }

    async fn get_metadata(&self) -> Result<Option<StorageMetadata>, ObjectStorageError> {
        let parseable_metadata: Option<StorageMetadata> =
            match self.get_object(&parseable_json_path()).await {
                Ok(bytes) => {
                    Some(serde_json::from_slice(&bytes).expect("parseable config is valid json"))
                }
                Err(err) => {
                    if matches!(err, ObjectStorageError::NoSuchKey(_)) {
                        None
                    } else {
                        return Err(err);
                    }
                }
            };

        Ok(parseable_metadata)
    }

    async fn stream_exists(&self, stream_name: &str) -> Result<bool, ObjectStorageError> {
        let res = self.get_object(&stream_json_path(stream_name)).await;

        match res {
            Ok(_) => Ok(true),
            Err(ObjectStorageError::NoSuchKey(_)) => Ok(false),
            Err(e) => Err(e),
        }
    }

    async fn sync(&self) -> Result<(), MoveDataError> {
        if !Path::new(&CONFIG.staging_dir()).exists() {
            return Ok(());
        }

        let streams = STREAM_INFO.list_streams();

        let mut stream_stats = HashMap::new();

        for stream in &streams {
            let dir = StorageDir::new(stream);
            // walk dir, find all .arrows files and convert to parquet
            // Do not include file which is being written to
            let time = chrono::Utc::now().naive_utc();
            let staging_files = dir.arrow_files_grouped_exclude_time(time);
            if staging_files.is_empty() {
                STAGING_FILES.with_label_values(&[stream]).set(0);
            }

            for (parquet_path, files) in staging_files {
                STAGING_FILES
                    .with_label_values(&[stream])
                    .set(files.len() as i64);

                for file in &files {
                    let file_size = file.metadata().unwrap().len();
                    let file_type = file.extension().unwrap().to_str().unwrap();

                    STORAGE_SIZE
                        .with_label_values(&["staging", stream, file_type])
                        .add(file_size as i64);
                }

                let record_reader = MergedRecordReader::try_new(&files).unwrap();
                let parquet_file = {
                    let mut parquet_table = CACHED_FILES.lock().unwrap();
                    let parquet_file =
                        fs::File::create(&parquet_path).map_err(|_| MoveDataError::Create)?;
                    parquet_table.upsert(&parquet_path);
                    parquet_file
                };
                let props = WriterProperties::builder()
                    .set_max_row_group_size(CONFIG.parseable.row_group_size)
                    .set_compression(CONFIG.parseable.parquet_compression.into())
                    .set_column_encoding(
                        ColumnPath::new(vec!["p_timestamp".to_string()]),
                        Encoding::DELTA_BINARY_PACKED,
                    )
                    .build();
                let merged_schema = record_reader.merged_schema();
                commit_schema_to_storage(stream, merged_schema.clone()).await?;
                let schema = Arc::new(merged_schema);
                let mut writer = ArrowWriter::try_new(parquet_file, schema.clone(), Some(props))?;

                for ref record in record_reader.merged_iter(&schema) {
                    writer.write(record)?;
                }

                writer.close()?;

                for file in files {
                    if fs::remove_file(file).is_err() {
                        log::error!("Failed to delete file. Unstable state");
                        process::abort()
                    }
                }
            }

            for file in dir.parquet_files() {
                let Some(metadata) = CACHED_FILES
                    .lock()
                    .unwrap()
                    .get_mut(&file)
                    .map(|fl| fl.metadata) else { continue };

                if metadata != CacheState::Idle {
                    continue;
                }

                let filename = file
                    .file_name()
                    .expect("only parquet files are returned by iterator")
                    .to_str()
                    .expect("filename is valid string");
                let file_suffix = str::replacen(filename, ".", "/", 3);
                let objectstore_path = format!("{stream}/{file_suffix}");

                CACHED_FILES
                    .lock()
                    .unwrap()
                    .get_mut(&file)
                    .expect("entry checked at the start")
                    .set_metadata(CacheState::Uploading);

                let compressed_size = file.metadata().map_or(0, |meta| meta.len());

                match self.upload_file(&objectstore_path, &file).await {
                    Ok(()) => {
                        CACHED_FILES
                            .lock()
                            .unwrap()
                            .get_mut(&file)
                            .expect("entry checked at the start")
                            .set_metadata(CacheState::Uploaded);
                    }
                    Err(e) => {
                        CACHED_FILES
                            .lock()
                            .unwrap()
                            .get_mut(&file)
                            .expect("entry checked at the start")
                            .set_metadata(CacheState::Idle);

                        return Err(e.into());
                    }
                }

                stream_stats
                    .entry(stream)
                    .and_modify(|size| *size += compressed_size)
                    .or_insert_with(|| compressed_size);

                CACHED_FILES.lock().unwrap().remove(&file);
            }
        }

        for (stream, compressed_size) in stream_stats {
            let stats = STREAM_INFO.read().unwrap().get(stream).map(|metadata| {
                metadata.stats.add_storage_size(compressed_size);
                STORAGE_SIZE
                    .with_label_values(&["data", stream, "parquet"])
                    .add(compressed_size as i64);
                Stats::from(&metadata.stats)
            });

            if let Some(stats) = stats {
                if let Err(e) = self.put_stats(stream, &stats).await {
                    log::warn!("Error updating stats to objectstore due to error [{}]", e);
                }
            }
        }

        Ok(())
    }
}

async fn commit_schema_to_storage(
    stream_name: &str,
    schema: Schema,
) -> Result<(), ObjectStorageError> {
    let storage = CONFIG.storage().get_object_store();
    let stream_schema = storage.get_schema(stream_name).await?;
    let new_schema = Schema::try_merge(vec![schema, stream_schema]).unwrap();
    storage.put_schema(stream_name, &new_schema).await
}

#[derive(Debug)]
pub struct Reader {
    reader: StreamReader<File>,
    timestamp_col_index: usize,
}

impl From<StreamReader<File>> for Reader {
    fn from(reader: StreamReader<File>) -> Self {
        let timestamp_col_index = reader
            .schema()
            .all_fields()
            .binary_search_by(|field| field.name().as_str().cmp("p_timestamp"))
            .expect("schema should have this field");

        Self {
            reader,
            timestamp_col_index,
        }
    }
}

#[derive(Debug)]
pub struct MergedRecordReader {
    pub readers: Vec<Reader>,
}

impl MergedRecordReader {
    pub fn try_new(files: &[PathBuf]) -> Result<Self, MoveDataError> {
        let mut readers = Vec::with_capacity(files.len());

        for file in files {
            let reader = StreamReader::try_new(File::open(file).unwrap(), None)?;
            readers.push(reader.into());
        }

        Ok(Self { readers })
    }

    pub fn merged_iter(self, schema: &Schema) -> impl Iterator<Item = RecordBatch> + '_ {
        let adapted_readers = self.readers.into_iter().map(move |reader| {
            reader
                .reader
                .flatten()
                .zip(std::iter::repeat(reader.timestamp_col_index))
        });

        kmerge_by(
            adapted_readers,
            |(a, a_col): &(RecordBatch, usize), (b, b_col): &(RecordBatch, usize)| {
                let a: &TimestampMillisecondArray = a
                    .column(*a_col)
                    .as_any()
                    .downcast_ref::<TimestampMillisecondArray>()
                    .unwrap();

                let b: &TimestampMillisecondArray = b
                    .column(*b_col)
                    .as_any()
                    .downcast_ref::<TimestampMillisecondArray>()
                    .unwrap();

                a.value(0) < b.value(0)
            },
        )
        .map(|(batch, _)| adapt_batch(schema, batch))
    }

    pub fn merged_schema(&self) -> Schema {
        Schema::try_merge(
            self.readers
                .iter()
                .map(|reader| reader.reader.schema().as_ref().clone()),
        )
        .unwrap()
    }
}

#[inline(always)]
fn to_bytes(any: &(impl ?Sized + serde::Serialize)) -> Bytes {
    serde_json::to_vec(any)
        .map(|any| any.into())
        .expect("serialize cannot fail")
}

#[inline(always)]
fn schema_path(stream_name: &str) -> RelativePathBuf {
    RelativePathBuf::from_iter([stream_name, SCHEMA_FILE_NAME])
}

#[inline(always)]
fn stream_json_path(stream_name: &str) -> RelativePathBuf {
    RelativePathBuf::from_iter([stream_name, STREAM_METADATA_FILE_NAME])
}

#[inline(always)]
fn parseable_json_path() -> RelativePathBuf {
    RelativePathBuf::from(PARSEABLE_METADATA_FILE_NAME)
}

#[inline(always)]
fn alert_json_path(stream_name: &str) -> RelativePathBuf {
    RelativePathBuf::from_iter([stream_name, ALERT_FILE_NAME])
}
