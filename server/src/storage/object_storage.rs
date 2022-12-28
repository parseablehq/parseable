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
    file_link::CacheState, LogStream, MoveDataError, ObjectStorageError, ObjectStoreFormat,
    Permisssion, StorageDir, StorageMetadata, CACHED_FILES,
};
use crate::{alerts::Alerts, metadata::STREAM_INFO, option::CONFIG, query::Query, stats::Stats};

use arrow_schema::Schema;
use async_trait::async_trait;
use bytes::Bytes;
use datafusion::{
    arrow::ipc::reader::StreamReader,
    datasource::listing::ListingTable,
    execution::runtime_env::RuntimeEnv,
    parquet::{arrow::ArrowWriter, file::properties::WriterProperties},
};
use relative_path::RelativePath;
use relative_path::RelativePathBuf;
use serde::Serialize;
use serde_json::Value;

use std::{
    collections::HashMap,
    fs::{self, File},
    path::Path,
    sync::Arc,
};

// metadata file names in a Stream prefix
const STREAM_METADATA_FILE_NAME: &str = ".stream.json";
pub(super) const PARSEABLE_METADATA_FILE_NAME: &str = ".parseable.json";
const SCHEMA_FILE_NAME: &str = ".schema";
const ALERT_FILE_NAME: &str = ".alert.json";

pub trait ObjectStorageProvider {
    fn get_datafusion_runtime(&self) -> Arc<RuntimeEnv>;
    fn get_object_store(&self) -> Arc<dyn ObjectStorage + Send>;
    fn get_endpoint(&self) -> String;
}

#[async_trait]
pub trait ObjectStorage: Sync + 'static {
    async fn get_object(&self, path: &RelativePath) -> Result<Bytes, ObjectStorageError>;
    async fn put_object(
        &self,
        path: &RelativePath,
        resource: Bytes,
    ) -> Result<(), ObjectStorageError>;
    async fn check(&self) -> Result<(), ObjectStorageError>;
    async fn delete_stream(&self, stream_name: &str) -> Result<(), ObjectStorageError>;
    async fn list_streams(&self) -> Result<Vec<LogStream>, ObjectStorageError>;
    async fn upload_file(&self, key: &str, path: &Path) -> Result<(), ObjectStorageError>;
    fn query_table(&self, query: &Query) -> Result<Option<ListingTable>, ObjectStorageError>;

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

        self.put_object(&schema_path(stream_name), "".into())
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

    async fn put_metadata(
        &self,
        parseable_metadata: &StorageMetadata,
    ) -> Result<(), ObjectStorageError> {
        self.put_object(&parseable_json_path(), to_bytes(parseable_metadata))
            .await
    }

    async fn get_schema(&self, stream_name: &str) -> Result<Option<Schema>, ObjectStorageError> {
        let schema = self.get_object(&schema_path(stream_name)).await?;
        let schema = serde_json::from_slice(&schema).ok();
        Ok(schema)
    }

    async fn get_alerts(&self, stream_name: &str) -> Result<Alerts, ObjectStorageError> {
        match self.get_object(&alert_json_path(stream_name)).await {
            Ok(alerts) => Ok(serde_json::from_slice(&alerts).unwrap_or_default()),
            Err(e) => match e {
                ObjectStorageError::NoSuchKey(_) => Ok(Alerts::default()),
                e => Err(e),
            },
        }
    }

    async fn get_stats(&self, stream_name: &str) -> Result<Stats, ObjectStorageError> {
        let stream_metadata = self.get_object(&stream_json_path(stream_name)).await?;
        let stream_metadata: Value =
            serde_json::from_slice(&stream_metadata).expect("parseable config is valid json");

        let stats = &stream_metadata["stats"];

        let stats = serde_json::from_value(stats.clone()).unwrap_or_default();

        Ok(stats)
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

    async fn sync(&self) -> Result<(), MoveDataError> {
        if !Path::new(&CONFIG.staging_dir()).exists() {
            return Ok(());
        }

        let streams = STREAM_INFO.list_streams();

        let mut stream_stats = HashMap::new();

        for stream in &streams {
            // get dir
            let dir = StorageDir::new(stream);
            // walk dir, find all .arrows files and convert to parquet

            let mut arrow_files = dir.arrow_files();
            // Do not include file which is being written to
            let hot_file = dir.path_by_current_time();
            let hot_filename = hot_file.file_name().expect("is a not none filename");

            arrow_files.retain(|file| {
                !file
                    .file_name()
                    .expect("is a not none filename")
                    .eq(hot_filename)
            });

            for file in arrow_files {
                let arrow_file = File::open(&file).map_err(|_| MoveDataError::Open)?;
                let reader = StreamReader::try_new(arrow_file, None)?;
                let schema = reader.schema();
                let records = reader.filter_map(|record| match record {
                    Ok(record) => Some(record),
                    Err(e) => {
                        log::warn!("warning from arrow stream {:?}", e);
                        None
                    }
                });

                let mut parquet_path = file.clone();
                parquet_path.set_extension("parquet");
                let mut parquet_table = CACHED_FILES.lock().unwrap();
                let parquet_file =
                    fs::File::create(&parquet_path).map_err(|_| MoveDataError::Create)?;
                parquet_table.upsert(&parquet_path);

                let props = WriterProperties::builder().build();
                let mut writer = ArrowWriter::try_new(parquet_file, schema, Some(props))?;

                for ref record in records {
                    writer.write(record)?;
                }

                writer.close()?;

                fs::remove_file(file).map_err(|_| MoveDataError::Delete)?;
            }

            for file in dir.parquet_files() {
                let metadata = CACHED_FILES.lock().unwrap().get_mut(&file).metadata;
                if metadata != CacheState::Idle {
                    continue;
                }

                let filename = file
                    .file_name()
                    .expect("only parquet files are returned by iterator")
                    .to_str()
                    .expect("filename is valid string");
                let file_suffix = str::replacen(filename, ".", "/", 3);
                let objectstore_path = format!("{}/{}", stream, file_suffix);
                CACHED_FILES
                    .lock()
                    .unwrap()
                    .get_mut(&file)
                    .set_metadata(CacheState::Uploading);
                let _put_parquet_file = self.upload_file(&objectstore_path, &file).await?;
                CACHED_FILES
                    .lock()
                    .unwrap()
                    .get_mut(&file)
                    .set_metadata(CacheState::Uploaded);

                stream_stats
                    .entry(stream)
                    .and_modify(|size| *size += file.metadata().map_or(0, |meta| meta.len()))
                    .or_insert_with(|| file.metadata().map_or(0, |meta| meta.len()));

                CACHED_FILES.lock().unwrap().remove(&file);
            }
        }

        for (stream, compressed_size) in stream_stats {
            let stats = STREAM_INFO.read().unwrap().get(stream).map(|metadata| {
                metadata.stats.add_storage_size(compressed_size);
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

#[inline(always)]
fn to_bytes(any: &(impl ?Sized + Serialize)) -> Bytes {
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
