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
 *
 */

use std::{
    collections::HashMap,
    fs::{remove_file, write, File, OpenOptions},
    num::NonZeroU32,
    path::{Path, PathBuf},
    process,
    sync::{Arc, Mutex, RwLock},
    time::{SystemTime, UNIX_EPOCH},
};

use arrow_array::RecordBatch;
use arrow_schema::{Field, Fields, Schema};
use chrono::{NaiveDateTime, Timelike, Utc};
use derive_more::{Deref, DerefMut};
use itertools::Itertools;
use parquet::{
    arrow::ArrowWriter,
    basic::Encoding,
    file::{properties::WriterProperties, FOOTER_SIZE},
    format::SortingColumn,
    schema::types::ColumnPath,
};
use rand::distributions::DistString;
use relative_path::RelativePathBuf;
use tokio::task::JoinSet;
use tracing::{error, info, trace, warn};

use crate::{
    cli::Options,
    event::DEFAULT_TIMESTAMP_KEY,
    metadata::{LogStreamMetadata, SchemaVersion},
    metrics,
    option::Mode,
    storage::{object_storage::to_bytes, retention::Retention, StreamType},
    utils::time::{Minute, TimeRange},
    LOCK_EXPECT, OBJECT_STORE_DATA_GRANULARITY,
};

use super::{
    staging::{
        reader::{MergedRecordReader, MergedReverseRecordReader},
        writer::{DiskWriter, Writer},
        StagingError,
    },
    LogStream, ARROW_FILE_EXTENSION,
};

#[derive(Debug, thiserror::Error)]
#[error("Stream not found: {0}")]
pub struct StreamNotFound(pub String);

pub type StreamRef = Arc<Stream>;

/// Gets the unix timestamp for the minute as described by the `SystemTime`
fn minute_from_system_time(time: SystemTime) -> u128 {
    time.duration_since(UNIX_EPOCH)
        .expect("Legitimate time")
        .as_millis()
        / 60000
}

/// All state associated with a single logstream in Parseable.
pub struct Stream {
    pub stream_name: String,
    pub metadata: RwLock<LogStreamMetadata>,
    pub data_path: PathBuf,
    pub options: Arc<Options>,
    pub writer: Mutex<Writer>,
    pub ingestor_id: Option<String>,
}

impl Stream {
    pub fn new(
        options: Arc<Options>,
        stream_name: impl Into<String>,
        metadata: LogStreamMetadata,
        ingestor_id: Option<String>,
    ) -> StreamRef {
        let stream_name = stream_name.into();
        let data_path = options.local_stream_data_path(&stream_name);

        Arc::new(Self {
            stream_name,
            metadata: RwLock::new(metadata),
            data_path,
            options,
            writer: Mutex::new(Writer::default()),
            ingestor_id,
        })
    }

    // Concatenates record batches and puts them in memory store for each event.
    pub fn push(
        &self,
        schema_key: &str,
        record: &RecordBatch,
        parsed_timestamp: NaiveDateTime,
        custom_partition_values: &HashMap<String, String>,
        stream_type: StreamType,
    ) -> Result<(), StagingError> {
        let mut guard = self.writer.lock().unwrap();
        if self.options.mode != Mode::Query || stream_type == StreamType::Internal {
            match guard.disk.get_mut(schema_key) {
                Some(writer) => {
                    writer.write(record)?;
                }
                None => {
                    // entry is not present thus we create it
                    let file_path = self.path_by_current_time(
                        schema_key,
                        parsed_timestamp,
                        custom_partition_values,
                    );
                    std::fs::create_dir_all(&self.data_path)?;

                    let range = TimeRange::granularity_range(
                        parsed_timestamp.and_local_timezone(Utc).unwrap(),
                        OBJECT_STORE_DATA_GRANULARITY,
                    );
                    let mut writer = DiskWriter::try_new(file_path, &record.schema(), range)
                        .expect("File and RecordBatch both are checked");

                    writer.write(record)?;
                    guard.disk.insert(schema_key.to_owned(), writer);
                }
            };
        }

        guard.mem.push(schema_key, record);

        Ok(())
    }

    pub fn path_by_current_time(
        &self,
        stream_hash: &str,
        parsed_timestamp: NaiveDateTime,
        custom_partition_values: &HashMap<String, String>,
    ) -> PathBuf {
        let mut hostname = hostname::get().unwrap().into_string().unwrap();
        if let Some(id) = &self.ingestor_id {
            hostname.push_str(id);
        }
        let filename = format!(
            "{stream_hash}.date={}.hour={:02}.minute={}.{}{hostname}.data.{ARROW_FILE_EXTENSION}",
            parsed_timestamp.date(),
            parsed_timestamp.hour(),
            Minute::from(parsed_timestamp).to_slot(OBJECT_STORE_DATA_GRANULARITY),
            custom_partition_values
                .iter()
                .sorted_by_key(|v| v.0)
                .map(|(key, value)| format!("{key}={value}."))
                .join("")
        );
        self.data_path.join(filename)
    }

    pub fn arrow_files(&self) -> Vec<PathBuf> {
        let Ok(dir) = self.data_path.read_dir() else {
            return vec![];
        };

        let paths = dir
            .flatten()
            .map(|file| file.path())
            .filter(|file| file.extension().is_some_and(|ext| ext.eq("arrows")))
            .sorted_by_key(|f| f.metadata().unwrap().modified().unwrap())
            .collect();

        paths
    }

    /// Groups arrow files which are to be included in one parquet
    ///
    /// Excludes the arrow file being written for the current minute (data is still being written to that one)
    ///
    /// Only includes ones starting from the previous minute
    pub fn arrow_files_grouped_exclude_time(
        &self,
        exclude: SystemTime,
        shutdown_signal: bool,
    ) -> HashMap<PathBuf, Vec<PathBuf>> {
        let mut grouped_arrow_file: HashMap<PathBuf, Vec<PathBuf>> = HashMap::new();
        let mut arrow_files = self.arrow_files();

        // if the shutdown signal is false i.e. normal condition
        // don't keep the ones for the current minute
        if !shutdown_signal {
            arrow_files.retain(|path| {
                let creation = path
                    .metadata()
                    .expect("Arrow file should exist on disk")
                    .created()
                    .expect("Creation time should be accessible");
                // Compare if creation time is actually from previous minute
                minute_from_system_time(creation) < minute_from_system_time(exclude)
            });
        }

        let random_string =
            rand::distributions::Alphanumeric.sample_string(&mut rand::thread_rng(), 15);
        for arrow_file_path in arrow_files {
            if arrow_file_path.metadata().unwrap().len() == 0 {
                error!(
                    "Invalid arrow file {:?} detected for stream {}, removing it",
                    &arrow_file_path, self.stream_name
                );
                remove_file(&arrow_file_path).unwrap();
            } else {
                let key = Self::arrow_path_to_parquet(&arrow_file_path, &random_string);
                grouped_arrow_file
                    .entry(key)
                    .or_default()
                    .push(arrow_file_path);
            }
        }
        grouped_arrow_file
    }

    pub fn parquet_files(&self) -> Vec<PathBuf> {
        let Ok(dir) = self.data_path.read_dir() else {
            return vec![];
        };

        dir.flatten()
            .map(|file| file.path())
            .filter(|file| {
                file.extension().is_some_and(|ext| ext.eq("parquet"))
                    && std::fs::metadata(file).is_ok_and(|meta| meta.len() > FOOTER_SIZE as u64)
            })
            .collect()
    }

    pub fn schema_files(&self) -> Vec<PathBuf> {
        let Ok(dir) = self.data_path.read_dir() else {
            return vec![];
        };

        dir.flatten()
            .map(|file| file.path())
            .filter(|file| file.extension().is_some_and(|ext| ext.eq("schema")))
            .collect()
    }

    pub fn get_schemas_if_present(&self) -> Option<Vec<Schema>> {
        let Ok(dir) = self.data_path.read_dir() else {
            return None;
        };

        let mut schemas: Vec<Schema> = Vec::new();

        for file in dir.flatten() {
            if let Some(ext) = file.path().extension() {
                if ext.eq("schema") {
                    let file = File::open(file.path()).expect("Schema File should exist");

                    let schema = match serde_json::from_reader(file) {
                        Ok(schema) => schema,
                        Err(_) => continue,
                    };
                    schemas.push(schema);
                }
            }
        }

        if !schemas.is_empty() {
            Some(schemas)
        } else {
            None
        }
    }

    fn arrow_path_to_parquet(path: &Path, random_string: &str) -> PathBuf {
        let filename = path.file_stem().unwrap().to_str().unwrap();
        let (_, filename) = filename.split_once('.').unwrap();
        assert!(filename.contains('.'), "contains the delim `.`");
        let filename_with_random_number = format!("{filename}.{random_string}.arrows");
        let mut parquet_path = path.to_owned();
        parquet_path.set_file_name(filename_with_random_number);
        parquet_path.set_extension("parquet");
        parquet_path
    }

    /// Converts arrow files in staging into parquet files, does so only for past minutes when run with `!shutdown_signal`
    pub fn prepare_parquet(&self, shutdown_signal: bool) -> Result<(), StagingError> {
        info!(
            "Starting arrow_conversion job for stream- {}",
            self.stream_name
        );

        let time_partition = self.get_time_partition();
        let custom_partition = self.get_custom_partition();

        // read arrow files on disk
        // convert them to parquet
        let schema = self
            .convert_disk_files_to_parquet(
                time_partition.as_ref(),
                custom_partition.as_ref(),
                shutdown_signal,
            )
            .inspect_err(|err| warn!("Error while converting arrow to parquet- {err:?}"))?;

        // check if there is already a schema file in staging pertaining to this stream
        // if yes, then merge them and save

        if let Some(mut schema) = schema {
            let static_schema_flag = self.get_static_schema_flag();
            if !static_schema_flag {
                // schema is dynamic, read from staging and merge if present

                // need to add something before .schema to make the file have an extension of type `schema`
                let path = RelativePathBuf::from_iter([format!("{}.schema", self.stream_name)])
                    .to_path(&self.data_path);

                let staging_schemas = self.get_schemas_if_present();
                if let Some(mut staging_schemas) = staging_schemas {
                    warn!(
                        "Found {} schemas in staging for stream- {}",
                        staging_schemas.len(),
                        self.stream_name
                    );
                    staging_schemas.push(schema);
                    schema = Schema::try_merge(staging_schemas)?;
                }

                // save the merged schema on staging disk
                // the path should be stream/.ingestor.{id}.schema
                info!("writing schema to path - {path:?}");
                write(path, to_bytes(&schema))?;
            }
        }

        Ok(())
    }

    pub fn recordbatches_cloned(&self, schema: &Arc<Schema>) -> Vec<RecordBatch> {
        self.writer.lock().unwrap().mem.recordbatch_cloned(schema)
    }

    pub fn clear(&self) {
        self.writer.lock().unwrap().mem.clear();
    }

    pub fn flush(&self, forced: bool) {
        let mut writer = self.writer.lock().unwrap();
        // Flush memory
        writer.mem.clear();
        // Drop schema -> disk writer mapping, triggers flush to disk
        writer.disk.retain(|_, w| !forced && w.is_current());
    }

    fn parquet_writer_props(
        &self,
        merged_schema: &Schema,
        time_partition: Option<&String>,
        custom_partition: Option<&String>,
    ) -> WriterProperties {
        // Determine time partition field
        let time_partition_field = time_partition.map_or(DEFAULT_TIMESTAMP_KEY, |tp| tp.as_str());

        // Find time partition index
        let time_partition_idx = merged_schema.index_of(time_partition_field).unwrap_or(0);

        let mut props = WriterProperties::builder()
            .set_max_row_group_size(self.options.row_group_size)
            .set_compression(self.options.parquet_compression.into())
            .set_column_encoding(
                ColumnPath::new(vec![time_partition_field.to_string()]),
                Encoding::DELTA_BINARY_PACKED,
            );

        // Create sorting columns
        let mut sorting_column_vec = vec![SortingColumn {
            column_idx: time_partition_idx as i32,
            descending: true,
            nulls_first: true,
        }];

        // Describe custom partition column encodings and sorting
        if let Some(custom_partition) = custom_partition {
            for partition in custom_partition.split(',') {
                if let Ok(idx) = merged_schema.index_of(partition) {
                    let column_path = ColumnPath::new(vec![partition.to_string()]);
                    props = props.set_column_encoding(column_path, Encoding::DELTA_BYTE_ARRAY);

                    sorting_column_vec.push(SortingColumn {
                        column_idx: idx as i32,
                        descending: true,
                        nulls_first: true,
                    });
                }
            }
        }

        // Set sorting columns
        props.set_sorting_columns(Some(sorting_column_vec)).build()
    }

    /// This function reads arrow files, groups their schemas
    ///
    /// converts them into parquet files and returns a merged schema
    pub fn convert_disk_files_to_parquet(
        &self,
        time_partition: Option<&String>,
        custom_partition: Option<&String>,
        shutdown_signal: bool,
    ) -> Result<Option<Schema>, StagingError> {
        let mut schemas = Vec::new();

        let now = SystemTime::now();
        let staging_files = self.arrow_files_grouped_exclude_time(now, shutdown_signal);
        if staging_files.is_empty() {
            metrics::STAGING_FILES
                .with_label_values(&[&self.stream_name])
                .set(0);
            metrics::STORAGE_SIZE
                .with_label_values(&["staging", &self.stream_name, "arrows"])
                .set(0);
            metrics::STORAGE_SIZE
                .with_label_values(&["staging", &self.stream_name, "parquet"])
                .set(0);
        }

        //find sum of arrow files in staging directory for a stream
        let total_arrow_files = staging_files.values().map(|v| v.len()).sum::<usize>();
        metrics::STAGING_FILES
            .with_label_values(&[&self.stream_name])
            .set(total_arrow_files as i64);

        //find sum of file sizes of all arrow files in staging_files
        let total_arrow_files_size = staging_files
            .values()
            .map(|v| {
                v.iter()
                    .map(|file| file.metadata().unwrap().len())
                    .sum::<u64>()
            })
            .sum::<u64>();
        metrics::STORAGE_SIZE
            .with_label_values(&["staging", &self.stream_name, "arrows"])
            .set(total_arrow_files_size as i64);

        // warn!("staging files-\n{staging_files:?}\n");
        for (parquet_path, arrow_files) in staging_files {
            let record_reader = MergedReverseRecordReader::try_new(&arrow_files);
            if record_reader.readers.is_empty() {
                continue;
            }
            let merged_schema = record_reader.merged_schema();

            let props = self.parquet_writer_props(&merged_schema, time_partition, custom_partition);
            schemas.push(merged_schema.clone());
            let schema = Arc::new(merged_schema);
            let mut part_path = parquet_path.to_owned();
            part_path.set_extension("part");
            let mut part_file = OpenOptions::new()
                .create(true)
                .append(true)
                .open(&part_path)
                .map_err(|_| StagingError::Create)?;
            let mut writer = ArrowWriter::try_new(&mut part_file, schema.clone(), Some(props))?;
            for ref record in record_reader.merged_iter(schema, time_partition.cloned()) {
                writer.write(record)?;
            }
            writer.close()?;

            if part_file.metadata().expect("File was just created").len()
                < parquet::file::FOOTER_SIZE as u64
            {
                error!(
                    "Invalid parquet file {part_path:?} detected for stream {}, removing it",
                    &self.stream_name
                );
                remove_file(part_path).unwrap();
            } else {
                trace!("Parquet file successfully constructed");
                if let Err(e) = std::fs::rename(&part_path, &parquet_path) {
                    error!(
                        "Couldn't rename part file: {part_path:?} -> {parquet_path:?}, error = {e}"
                    );
                }

                for file in arrow_files {
                    let file_size = match file.metadata() {
                        Ok(meta) => meta.len(),
                        Err(err) => {
                            warn!("File ({}) not found; Error = {err}", file.display());
                            continue;
                        }
                    };
                    if remove_file(&file).is_err() {
                        error!("Failed to delete file. Unstable state");
                        process::abort()
                    }
                    metrics::STORAGE_SIZE
                        .with_label_values(&["staging", &self.stream_name, ARROW_FILE_EXTENSION])
                        .sub(file_size as i64);
                }
            }
        }

        if schemas.is_empty() {
            return Ok(None);
        }

        Ok(Some(Schema::try_merge(schemas).unwrap()))
    }

    pub fn updated_schema(&self, current_schema: Schema) -> Schema {
        let staging_files = self.arrow_files();
        let record_reader = MergedRecordReader::try_new(&staging_files).unwrap();
        if record_reader.readers.is_empty() {
            return current_schema;
        }

        let schema = record_reader.merged_schema();

        Schema::try_merge(vec![schema, current_schema]).unwrap()
    }

    /// Stores the provided stream metadata in memory mapping
    pub async fn set_metadata(&self, updated_metadata: LogStreamMetadata) {
        *self.metadata.write().expect(LOCK_EXPECT) = updated_metadata;
    }

    pub fn get_first_event(&self) -> Option<String> {
        self.metadata
            .read()
            .expect(LOCK_EXPECT)
            .first_event_at
            .clone()
    }

    pub fn get_time_partition(&self) -> Option<String> {
        self.metadata
            .read()
            .expect(LOCK_EXPECT)
            .time_partition
            .clone()
    }

    pub fn get_time_partition_limit(&self) -> Option<NonZeroU32> {
        self.metadata
            .read()
            .expect(LOCK_EXPECT)
            .time_partition_limit
    }

    pub fn get_custom_partition(&self) -> Option<String> {
        self.metadata
            .read()
            .expect(LOCK_EXPECT)
            .custom_partition
            .clone()
    }

    pub fn get_static_schema_flag(&self) -> bool {
        self.metadata.read().expect(LOCK_EXPECT).static_schema_flag
    }

    pub fn get_retention(&self) -> Option<Retention> {
        self.metadata.read().expect(LOCK_EXPECT).retention.clone()
    }

    pub fn get_schema_version(&self) -> SchemaVersion {
        self.metadata.read().expect(LOCK_EXPECT).schema_version
    }

    pub fn get_schema(&self) -> Arc<Schema> {
        let metadata = self.metadata.read().expect(LOCK_EXPECT);

        // sort fields on read from hashmap as order of fields can differ.
        // This provides a stable output order if schema is same between calls to this function
        let fields: Fields = metadata
            .schema
            .values()
            .sorted_by_key(|field| field.name())
            .cloned()
            .collect();

        Arc::new(Schema::new(fields))
    }

    pub fn get_schema_raw(&self) -> HashMap<String, Arc<Field>> {
        self.metadata.read().expect(LOCK_EXPECT).schema.clone()
    }

    pub fn set_retention(&self, retention: Retention) {
        self.metadata.write().expect(LOCK_EXPECT).retention = Some(retention);
    }

    pub fn set_first_event_at(&self, first_event_at: &str) {
        self.metadata.write().expect(LOCK_EXPECT).first_event_at = Some(first_event_at.to_owned());
    }

    /// Removes the `first_event_at` timestamp for the specified stream from the LogStreamMetadata.
    ///
    /// This function is called during the retention task, when the parquet files along with the manifest files are deleted from the storage.
    /// The manifest path is removed from the snapshot in the stream.json
    /// and the first_event_at value in the stream.json is removed.
    ///
    /// # Arguments
    ///
    /// * `stream_name` - The name of the stream for which the `first_event_at` timestamp is to be removed.
    ///
    /// # Returns
    ///
    /// * `Result<(), StreamNotFound>` - Returns `Ok(())` if the `first_event_at` timestamp is successfully removed,
    ///   or a `StreamNotFound` if the stream metadata is not found.
    ///
    /// # Examples
    /// ```ignore
    /// ```rust
    /// let result = metadata.remove_first_event_at("my_stream");
    /// match result {
    ///     Ok(()) => println!("first-event-at removed successfully"),
    ///     Err(e) => eprintln!("Error removing first-event-at from PARSEABLE.streams: {}", e),
    /// }
    /// ```
    pub fn reset_first_event_at(&self) {
        self.metadata
            .write()
            .expect(LOCK_EXPECT)
            .first_event_at
            .take();
    }

    pub fn set_time_partition_limit(&self, time_partition_limit: NonZeroU32) {
        self.metadata
            .write()
            .expect(LOCK_EXPECT)
            .time_partition_limit = Some(time_partition_limit);
    }

    pub fn set_custom_partition(&self, custom_partition: Option<&String>) {
        self.metadata.write().expect(LOCK_EXPECT).custom_partition = custom_partition.cloned();
    }

    pub fn set_hot_tier(&self, enable: bool) {
        self.metadata.write().expect(LOCK_EXPECT).hot_tier_enabled = enable;
    }

    pub fn get_stream_type(&self) -> StreamType {
        self.metadata.read().expect(LOCK_EXPECT).stream_type
    }

    /// First flushes arrows onto disk and then converts the arrow into parquet
    pub fn flush_and_convert(&self, shutdown_signal: bool) -> Result<(), StagingError> {
        self.flush(shutdown_signal);

        self.prepare_parquet(shutdown_signal)
    }
}

#[derive(Deref, DerefMut, Default)]
pub struct Streams(RwLock<HashMap<String, StreamRef>>);

// PARSEABLE.streams should be updated
// 1. During server start up
// 2. When a new stream is created (make a new entry in the map)
// 3. When a stream is deleted (remove the entry from the map)
// 4. When first event is sent to stream (update the schema)
// 5. When set alert API is called (update the alert)
impl Streams {
    pub fn create(
        &self,
        options: Arc<Options>,
        stream_name: String,
        metadata: LogStreamMetadata,
        ingestor_id: Option<String>,
    ) -> StreamRef {
        let stream = Stream::new(options, &stream_name, metadata, ingestor_id);
        self.write()
            .expect(LOCK_EXPECT)
            .insert(stream_name, stream.clone());

        stream
    }

    /// TODO: validate possibility of stream continuing to exist despite being deleted
    pub fn delete(&self, stream_name: &str) {
        self.write().expect(LOCK_EXPECT).remove(stream_name);
    }

    pub fn contains(&self, stream_name: &str) -> bool {
        self.read().expect(LOCK_EXPECT).contains_key(stream_name)
    }

    /// Returns the number of logstreams that parseable is aware of
    pub fn len(&self) -> usize {
        self.read().expect(LOCK_EXPECT).len()
    }

    /// Returns true if parseable is not aware of any streams
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Listing of logstream names that parseable is aware of
    pub fn list(&self) -> Vec<LogStream> {
        self.read()
            .expect(LOCK_EXPECT)
            .keys()
            .map(String::clone)
            .collect()
    }

    pub fn list_internal_streams(&self) -> Vec<String> {
        let map = self.read().expect(LOCK_EXPECT);

        map.iter()
            .filter(|(_, stream)| {
                stream.metadata.read().expect(LOCK_EXPECT).stream_type == StreamType::Internal
            })
            .map(|(k, _)| k.clone())
            .collect()
    }

    /// Asynchronously flushes arrows and compacts into parquet data on all streams in staging,
    /// so that it is ready to be pushed onto objectstore.
    pub fn flush_and_convert(
        &self,
        joinset: &mut JoinSet<Result<(), StagingError>>,
        shutdown_signal: bool,
    ) {
        let streams: Vec<Arc<Stream>> = self
            .read()
            .expect(LOCK_EXPECT)
            .values()
            .map(Arc::clone)
            .collect();
        for stream in streams {
            joinset.spawn(async move { stream.flush_and_convert(shutdown_signal) });
        }
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use arrow_array::{Int32Array, StringArray, TimestampMillisecondArray};
    use arrow_schema::{DataType, Field, TimeUnit};
    use chrono::{NaiveDate, TimeDelta, Utc};
    use temp_dir::TempDir;
    use tokio::time::sleep;

    use super::*;

    #[test]
    fn test_staging_new_with_valid_stream() {
        let stream_name = "test_stream";

        let options = Arc::new(Options::default());
        let staging = Stream::new(
            options.clone(),
            stream_name,
            LogStreamMetadata::default(),
            None,
        );

        assert_eq!(
            staging.data_path,
            options.local_stream_data_path(stream_name)
        );
    }

    #[test]
    fn test_staging_with_special_characters() {
        let stream_name = "test_stream_!@#$%^&*()";

        let options = Arc::new(Options::default());
        let staging = Stream::new(
            options.clone(),
            stream_name,
            LogStreamMetadata::default(),
            None,
        );

        assert_eq!(
            staging.data_path,
            options.local_stream_data_path(stream_name)
        );
    }

    #[test]
    fn test_staging_data_path_initialization() {
        let stream_name = "example_stream";

        let options = Arc::new(Options::default());
        let staging = Stream::new(
            options.clone(),
            stream_name,
            LogStreamMetadata::default(),
            None,
        );

        assert_eq!(
            staging.data_path,
            options.local_stream_data_path(stream_name)
        );
    }

    #[test]
    fn test_staging_with_alphanumeric_stream_name() {
        let stream_name = "test123stream";

        let options = Arc::new(Options::default());
        let staging = Stream::new(
            options.clone(),
            stream_name,
            LogStreamMetadata::default(),
            None,
        );

        assert_eq!(
            staging.data_path,
            options.local_stream_data_path(stream_name)
        );
    }

    #[test]
    fn test_arrow_files_empty_directory() {
        let temp_dir = TempDir::new().unwrap();

        let options = Options {
            local_staging_path: temp_dir.path().to_path_buf(),
            ..Default::default()
        };
        let staging = Stream::new(
            Arc::new(options),
            "test_stream",
            LogStreamMetadata::default(),
            None,
        );

        let files = staging.arrow_files();

        assert!(files.is_empty());
    }

    #[test]
    fn generate_correct_path_with_current_time_and_no_custom_partitioning() {
        let stream_name = "test_stream";
        let stream_hash = "abc123";
        let parsed_timestamp = NaiveDate::from_ymd_opt(2023, 10, 1)
            .unwrap()
            .and_hms_opt(12, 30, 0)
            .unwrap();
        let custom_partition_values = HashMap::new();

        let options = Options::default();
        let staging = Stream::new(
            Arc::new(options),
            stream_name,
            LogStreamMetadata::default(),
            None,
        );

        let expected_path = staging.data_path.join(format!(
            "{stream_hash}.date={}.hour={:02}.minute={}.{}.data.{ARROW_FILE_EXTENSION}",
            parsed_timestamp.date(),
            parsed_timestamp.hour(),
            Minute::from(parsed_timestamp).to_slot(OBJECT_STORE_DATA_GRANULARITY),
            hostname::get().unwrap().into_string().unwrap()
        ));

        let generated_path =
            staging.path_by_current_time(stream_hash, parsed_timestamp, &custom_partition_values);

        assert_eq!(generated_path, expected_path);
    }

    #[test]
    fn generate_correct_path_with_current_time_and_custom_partitioning() {
        let stream_name = "test_stream";
        let stream_hash = "abc123";
        let parsed_timestamp = NaiveDate::from_ymd_opt(2023, 10, 1)
            .unwrap()
            .and_hms_opt(12, 30, 0)
            .unwrap();
        let mut custom_partition_values = HashMap::new();
        custom_partition_values.insert("key1".to_string(), "value1".to_string());
        custom_partition_values.insert("key2".to_string(), "value2".to_string());

        let options = Options::default();
        let staging = Stream::new(
            Arc::new(options),
            stream_name,
            LogStreamMetadata::default(),
            None,
        );

        let expected_path = staging.data_path.join(format!(
            "{stream_hash}.date={}.hour={:02}.minute={}.key1=value1.key2=value2.{}.data.{ARROW_FILE_EXTENSION}",
            parsed_timestamp.date(),
            parsed_timestamp.hour(),
            Minute::from(parsed_timestamp).to_slot(OBJECT_STORE_DATA_GRANULARITY),
            hostname::get().unwrap().into_string().unwrap()
        ));

        let generated_path =
            staging.path_by_current_time(stream_hash, parsed_timestamp, &custom_partition_values);

        assert_eq!(generated_path, expected_path);
    }

    #[test]
    fn test_convert_to_parquet_with_empty_staging() -> Result<(), StagingError> {
        let temp_dir = TempDir::new()?;
        let options = Options {
            local_staging_path: temp_dir.path().to_path_buf(),
            ..Default::default()
        };
        let stream = "test_stream".to_string();
        let result = Stream::new(
            Arc::new(options),
            &stream,
            LogStreamMetadata::default(),
            None,
        )
        .convert_disk_files_to_parquet(None, None, false)?;
        assert!(result.is_none());
        // Verify metrics were set to 0
        let staging_files = metrics::STAGING_FILES.with_label_values(&[&stream]).get();
        assert_eq!(staging_files, 0);
        let storage_size_arrows = metrics::STORAGE_SIZE
            .with_label_values(&["staging", &stream, "arrows"])
            .get();
        assert_eq!(storage_size_arrows, 0);
        let storage_size_parquet = metrics::STORAGE_SIZE
            .with_label_values(&["staging", &stream, "parquet"])
            .get();
        assert_eq!(storage_size_parquet, 0);
        Ok(())
    }

    fn write_log(staging: &StreamRef, schema: &Schema, mins: i64) {
        let time: NaiveDateTime = Utc::now()
            .checked_sub_signed(TimeDelta::minutes(mins))
            .unwrap()
            .naive_utc();
        let batch = RecordBatch::try_new(
            Arc::new(schema.clone()),
            vec![
                Arc::new(TimestampMillisecondArray::from(vec![1, 2, 3])),
                Arc::new(Int32Array::from(vec![1, 2, 3])),
                Arc::new(StringArray::from(vec!["a", "b", "c"])),
            ],
        )
        .unwrap();
        staging
            .push(
                "abc",
                &batch,
                time,
                &HashMap::new(),
                StreamType::UserDefined,
            )
            .unwrap();
        staging.flush(true);
    }

    #[test]
    fn different_minutes_multiple_arrow_files_to_parquet() {
        let temp_dir = TempDir::new().unwrap();
        let stream_name = "test_stream";
        let options = Arc::new(Options {
            local_staging_path: temp_dir.path().to_path_buf(),
            row_group_size: 1048576,
            ..Default::default()
        });
        let staging = Stream::new(
            options.clone(),
            stream_name,
            LogStreamMetadata::default(),
            None,
        );

        // Create test arrow files
        let schema = Schema::new(vec![
            Field::new(
                DEFAULT_TIMESTAMP_KEY,
                DataType::Timestamp(TimeUnit::Millisecond, None),
                false,
            ),
            Field::new("id", DataType::Int32, false),
            Field::new("value", DataType::Utf8, false),
        ]);

        for i in 0..3 {
            write_log(&staging, &schema, i);
        }
        // verify the arrow files exist in staging
        assert_eq!(staging.arrow_files().len(), 3);
        drop(staging);

        // Start with a fresh staging
        let staging = Stream::new(options, stream_name, LogStreamMetadata::default(), None);
        let result = staging
            .convert_disk_files_to_parquet(None, None, true)
            .unwrap();

        assert!(result.is_some());
        let result_schema = result.unwrap();
        assert_eq!(result_schema.fields().len(), 3);

        // Verify parquet files were created and the arrow files deleted
        assert_eq!(staging.parquet_files().len(), 3);
        assert_eq!(staging.arrow_files().len(), 0);
    }

    #[test]
    fn same_minute_multiple_arrow_files_to_parquet() {
        let temp_dir = TempDir::new().unwrap();
        let stream_name = "test_stream";
        let options = Arc::new(Options {
            local_staging_path: temp_dir.path().to_path_buf(),
            row_group_size: 1048576,
            ..Default::default()
        });
        let staging = Stream::new(
            options.clone(),
            stream_name,
            LogStreamMetadata::default(),
            None,
        );

        // Create test arrow files
        let schema = Schema::new(vec![
            Field::new(
                DEFAULT_TIMESTAMP_KEY,
                DataType::Timestamp(TimeUnit::Millisecond, None),
                false,
            ),
            Field::new("id", DataType::Int32, false),
            Field::new("value", DataType::Utf8, false),
        ]);

        for _ in 0..3 {
            write_log(&staging, &schema, 0);
        }
        // verify the arrow files exist in staging
        assert_eq!(staging.arrow_files().len(), 1);
        drop(staging);

        // Start with a fresh staging
        let staging = Stream::new(options, stream_name, LogStreamMetadata::default(), None);
        let result = staging
            .convert_disk_files_to_parquet(None, None, true)
            .unwrap();

        assert!(result.is_some());
        let result_schema = result.unwrap();
        assert_eq!(result_schema.fields().len(), 3);

        // Verify parquet files were created and the arrow files deleted
        assert_eq!(staging.parquet_files().len(), 1);
        assert_eq!(staging.arrow_files().len(), 0);
    }

    #[tokio::test]
    async fn miss_current_arrow_file_when_converting_to_parquet() {
        let temp_dir = TempDir::new().unwrap();
        let stream_name = "test_stream";
        let options = Arc::new(Options {
            local_staging_path: temp_dir.path().to_path_buf(),
            row_group_size: 1048576,
            ..Default::default()
        });
        let staging = Stream::new(
            options.clone(),
            stream_name,
            LogStreamMetadata::default(),
            None,
        );

        // Create test arrow files
        let schema = Schema::new(vec![
            Field::new(
                DEFAULT_TIMESTAMP_KEY,
                DataType::Timestamp(TimeUnit::Millisecond, None),
                false,
            ),
            Field::new("id", DataType::Int32, false),
            Field::new("value", DataType::Utf8, false),
        ]);

        // 2 logs in the previous minutes
        for i in 0..2 {
            write_log(&staging, &schema, i);
        }
        sleep(Duration::from_secs(60)).await;

        write_log(&staging, &schema, 0);

        // verify the arrow files exist in staging
        assert_eq!(staging.arrow_files().len(), 3);
        drop(staging);

        // Start with a fresh staging
        let staging = Stream::new(options, stream_name, LogStreamMetadata::default(), None);
        let result = staging
            .convert_disk_files_to_parquet(None, None, false)
            .unwrap();

        assert!(result.is_some());
        let result_schema = result.unwrap();
        assert_eq!(result_schema.fields().len(), 3);

        // Verify parquet files were created and the arrow file left
        assert_eq!(staging.parquet_files().len(), 2);
        assert_eq!(staging.arrow_files().len(), 1);
    }
}
