/*
 * Parseable Server (C) 2022 - 2025 Parseable, Inc.
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
    collections::{HashMap, HashSet},
    fs::{self, File, OpenOptions},
    io::BufWriter,
    path::PathBuf,
    sync::Arc,
};

use arrow_array::RecordBatch;
use arrow_ipc::{
    CompressionType,
    writer::{IpcWriteOptions, StreamWriter},
};
use arrow_schema::Schema;
use arrow_select::concat::concat_batches;
use chrono::{TimeDelta, Utc};
use datafusion::physical_plan::buffer::SizedMessage;
use itertools::Itertools;
use once_cell::sync::Lazy;
use rand::distributions::{Alphanumeric, DistString};
use tracing::error;

use crate::{
    parseable::{ARROW_FILE_EXTENSION, PART_FILE_EXTENSION},
    utils::{arrow::adapt_batch, time::TimeRange},
};

use super::StagingError;

const DISK_WRITE_BATCH_MAX_AGE_SECS_VAR: &str = "DISK_WRITE_BATCH_MAX_AGE_SECS";
static DISK_WRITE_BATCH_MAX_AGE_SECS: Lazy<i64> = Lazy::new(|| {
    if let Ok(var) = std::env::var(DISK_WRITE_BATCH_MAX_AGE_SECS_VAR)
        && let Ok(var) = var.parse::<i64>()
    {
        var
    } else {
        1
    }
});

const ARROW_FLUSH_SIZE_LIMIT_VAR: &str = "ARROW_FLUSH_SIZE_LIMIT";
static ARROW_FLUSH_SIZE_LIMIT: Lazy<usize> = Lazy::new(|| {
    if let Ok(var) = std::env::var(ARROW_FLUSH_SIZE_LIMIT_VAR)
        && let Ok(var) = var.parse::<usize>()
    {
        var
    } else {
        1024 * 1024 * 1024 * 10
    }
});

#[derive(Default)]
pub struct Writer {
    pub mem: MemWriter<4096>,
    pub disk: HashMap<String, DiskWriter>,
    disk_pending: HashMap<String, PendingDiskBatch>,
}

impl Writer {
    #[cfg_attr(feature = "hotpath", hotpath::measure)]
    pub fn push_disk(
        &mut self,
        filename: String,
        rb: &RecordBatch,
        file_path: PathBuf,
        range: TimeRange,
        batch_rows: usize,
    ) -> Result<(), StagingError> {
        let now = Utc::now();
        let pending = self.disk_pending.entry(filename.clone()).or_default();
        pending.rows += rb.num_rows();
        pending.range.get_or_insert(range);
        pending.first_seen.get_or_insert(now);
        pending.batches.push(rb.clone());

        let should_flush = pending.rows >= batch_rows.max(1)
            || pending.is_older_than(now, TimeDelta::seconds(*DISK_WRITE_BATCH_MAX_AGE_SECS));
        if should_flush {
            self.flush_pending_disk(&filename, file_path)?;
        }

        Ok(())
    }

    pub fn flush_pending_disk(
        &mut self,
        filename: &str,
        file_path: PathBuf,
    ) -> Result<(), StagingError> {
        let Some(pending) = self.disk_pending.remove(filename) else {
            return Ok(());
        };
        if pending.batches.is_empty() {
            return Ok(());
        }

        write_pending_disk_batch(&mut self.disk, filename.to_owned(), pending, file_path)?;

        Ok(())
    }

    pub fn take_flushable_disk(
        &mut self,
        forced: bool,
    ) -> (HashMap<String, DiskWriter>, PendingDiskWrites) {
        let mut flushable_disk = HashMap::new();
        let old_disk = std::mem::take(&mut self.disk);
        for (filename, writer) in old_disk {
            if !forced && writer.is_current() {
                self.disk.insert(filename, writer);
            } else {
                flushable_disk.insert(filename, writer);
            }
        }

        let mut flushable_pending = HashMap::new();
        let old_pending = std::mem::take(&mut self.disk_pending);
        let now = Utc::now();
        for (filename, pending) in old_pending {
            if !forced
                && pending.is_current()
                && !pending.is_older_than(now, TimeDelta::seconds(*DISK_WRITE_BATCH_MAX_AGE_SECS))
            {
                self.disk_pending.insert(filename, pending);
            } else {
                flushable_pending.insert(filename, pending);
            }
        }

        (flushable_disk, PendingDiskWrites(flushable_pending))
    }
}

pub struct PendingDiskWrites(HashMap<String, PendingDiskBatch>);

impl PendingDiskWrites {
    pub fn flush_into(
        self,
        disk: &mut HashMap<String, DiskWriter>,
        data_path: &std::path::Path,
    ) -> Result<(), StagingError> {
        for (filename, pending) in self.0 {
            write_pending_disk_batch(disk, filename.clone(), pending, data_path.join(filename))?;
        }
        Ok(())
    }
}

fn write_pending_disk_batch(
    disk: &mut HashMap<String, DiskWriter>,
    filename: String,
    pending: PendingDiskBatch,
    file_path: PathBuf,
) -> Result<(), StagingError> {
    if pending.batches.is_empty() {
        return Ok(());
    }

    let schema = pending.batches[0].schema();
    let batch = concat_batches(&schema, pending.batches.iter())?;
    let s = match disk.get_mut(&filename) {
        Some(writer) => writer.write(&batch)?,
        None => {
            let range = pending.range.expect("pending disk batch must have range");
            if let Some(parent) = file_path.parent() {
                fs::create_dir_all(parent)?;
            }
            let mut writer = DiskWriter::try_new(file_path, &schema, range)?;
            let s = writer.write(&batch)?;
            disk.insert(filename.clone(), writer);
            s
        }
    };
    if s >= *ARROW_FLUSH_SIZE_LIMIT {
        disk.remove(&filename);
    }

    Ok(())
}

#[derive(Default)]
struct PendingDiskBatch {
    rows: usize,
    batches: Vec<RecordBatch>,
    range: Option<TimeRange>,
    first_seen: Option<chrono::DateTime<Utc>>,
}

impl PendingDiskBatch {
    fn is_current(&self) -> bool {
        self.range
            .as_ref()
            .is_some_and(|range| range.contains(Utc::now()))
    }

    fn is_older_than(&self, now: chrono::DateTime<Utc>, age: TimeDelta) -> bool {
        self.first_seen
            .is_some_and(|first_seen| now.signed_duration_since(first_seen) >= age)
    }
}

pub struct DiskWriter {
    inner: StreamWriter<BufWriter<File>>,
    path: PathBuf,
    range: TimeRange,
    size: usize,
}

impl DiskWriter {
    /// Try to create a file to stream arrows into
    pub fn try_new(
        path: impl Into<PathBuf>,
        schema: &Schema,
        range: TimeRange,
    ) -> Result<Self, StagingError> {
        let mut path = path.into();
        path.set_extension(PART_FILE_EXTENSION);
        let file = OpenOptions::new()
            .write(true)
            .truncate(true)
            .create(true)
            .open(&path)?;
        let inner = StreamWriter::try_new_with_options(
            BufWriter::new(file),
            schema,
            IpcWriteOptions::default()
                .try_with_compression(Some(CompressionType::LZ4_FRAME))
                .unwrap(),
        )?;

        let size = 0;

        Ok(Self {
            inner,
            path,
            range,
            size,
        })
    }

    pub fn is_current(&self) -> bool {
        self.range.contains(Utc::now())
    }

    /// Write a single recordbatch into file
    #[cfg_attr(feature = "hotpath", hotpath::measure)]
    pub fn write(&mut self, rb: &RecordBatch) -> Result<usize, StagingError> {
        self.size += rb.size();
        self.inner.write(rb).map_err(StagingError::Arrow)?;
        Ok(self.size)
    }
}

impl Drop for DiskWriter {
    /// Write the continuation bytes and mark the file as done, rename to `.data.arrows`
    fn drop(&mut self) {
        if let Err(err) = self.inner.finish() {
            error!("Couldn't finish arrow file {:?}, error = {err}", self.path);
            return;
        }

        let mut arrow_path = self.path.to_owned();
        arrow_path.set_extension(ARROW_FILE_EXTENSION);

        // If file exists, append a random string before .date to avoid overwriting
        if arrow_path.exists() {
            let file_name = arrow_path.file_name().unwrap().to_string_lossy();
            let date_pos = file_name
                .find(".date")
                .expect("File name should contain .date");
            let random_suffix = Alphanumeric.sample_string(&mut rand::thread_rng(), 8);
            let new_name = format!("{}{}", random_suffix, &file_name[date_pos..]);
            arrow_path.set_file_name(new_name);
        }

        if let Err(err) = std::fs::rename(&self.path, &arrow_path) {
            error!("Couldn't rename file {:?}, error = {err}", self.path);
        }
        tracing::info!(
            "flushing {:?} due to drop with size {}\n",
            self.path,
            self.size
        );
    }
}

/// Structure to keep recordbatches in memory.
///
/// Any new schema is updated in the schema map.
/// Recordbatches are pushed to mutable buffer first and then concated together and pushed to read buffer
#[derive(Debug)]
pub struct MemWriter<const N: usize> {
    schema: Schema,
    // for checking uniqueness of schema
    schema_map: HashSet<String>,
    pub read_buffer: Vec<RecordBatch>,
    pub mutable_buffer: MutableBuffer<N>,
}

impl<const N: usize> Default for MemWriter<N> {
    fn default() -> Self {
        Self {
            schema: Schema::empty(),
            schema_map: HashSet::default(),
            read_buffer: Vec::default(),
            mutable_buffer: MutableBuffer::default(),
        }
    }
}

impl<const N: usize> MemWriter<N> {
    pub fn push(&mut self, schema_key: &str, rb: &RecordBatch) -> Result<(), StagingError> {
        if !self.schema_map.contains(schema_key) {
            self.schema_map.insert(schema_key.to_owned());
            self.schema = Schema::try_merge([self.schema.clone(), (*rb.schema()).clone()])?;
        }

        if let Some(record) = self.mutable_buffer.push(rb) {
            let record = concat_records(&Arc::new(self.schema.clone()), &record)?;
            self.read_buffer.push(record);
        }
        Ok(())
    }

    pub fn clear(&mut self) {
        self.schema = Schema::empty();
        self.schema_map.clear();
        self.read_buffer.clear();
        self.mutable_buffer.inner.clear();
    }

    pub fn recordbatch_cloned(
        &self,
        schema: &Arc<Schema>,
    ) -> Result<Vec<RecordBatch>, StagingError> {
        let mut read_buffer = self.read_buffer.clone();
        if !self.mutable_buffer.inner.is_empty() {
            let rb = concat_records(schema, &self.mutable_buffer.inner)?;
            read_buffer.push(rb)
        }

        Ok(read_buffer
            .into_iter()
            .map(|rb| adapt_batch(schema, &rb))
            .collect())
    }
}

fn concat_records(
    schema: &Arc<Schema>,
    record: &[RecordBatch],
) -> Result<RecordBatch, StagingError> {
    let records = record.iter().map(|x| adapt_batch(schema, x)).collect_vec();
    Ok(concat_batches(schema, records.iter())?)
}

#[derive(Debug, Default)]
pub struct MutableBuffer<const N: usize> {
    pub inner: Vec<RecordBatch>,
}

impl<const N: usize> MutableBuffer<N> {
    fn push(&mut self, rb: &RecordBatch) -> Option<Vec<RecordBatch>> {
        if self.inner.len() + rb.num_rows() >= N {
            let left = N - self.inner.len();
            let right = rb.num_rows() - left;
            let left_slice = rb.slice(0, left);
            let right_slice = if left < rb.num_rows() {
                Some(rb.slice(left, right))
            } else {
                None
            };
            self.inner.push(left_slice);
            // take all records
            let src = Vec::with_capacity(self.inner.len());
            let inner = std::mem::replace(&mut self.inner, src);

            if let Some(right_slice) = right_slice {
                self.inner.push(right_slice);
            }

            Some(inner)
        } else {
            self.inner.push(rb.clone());
            None
        }
    }
}
