/*
 * Parseable Server (C) 2022 Parseable, Inc.
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
use datafusion::arrow;
use datafusion::arrow::datatypes::Schema;
use datafusion::arrow::json;
use datafusion::arrow::json::reader::infer_json_schema;
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::parquet::arrow::{ArrowReader, ArrowWriter, ParquetFileArrowReader};
use datafusion::parquet::file::properties::WriterProperties;
use datafusion::parquet::file::serialized_reader::SerializedFileReader;
use log::error;
use std::fs;
use std::io::BufReader;
use std::sync::Arc;

use crate::metadata;
use crate::option::CONFIG;
use crate::response;
use crate::storage::ObjectStorage;
use crate::Error;

#[derive(Clone)]
pub struct Event {
    pub body: String,
    pub stream_name: String,
}

// Events holds the schema related to a each event for a single log stream

impl Event {
    fn data_file_path(&self) -> String {
        format!(
            "{}/{}",
            CONFIG
                .parseable
                .local_stream_data_path(&self.stream_name)
                .to_string_lossy(),
            "data.parquet"
        )
    }

    pub async fn process(
        &self,
        storage: &impl ObjectStorage,
    ) -> Result<response::EventResponse, Error> {
        let inferred_schema = self.infer_schema().map_err(|e| {
            error!("Failed to infer schema for event. {:?}", e);
            e
        })?;

        let event = self.get_reader(inferred_schema.clone());
        let size = self.body_size();

        let stream_schema = metadata::STREAM_INFO.schema(&self.stream_name)?;
        let is_first_event = stream_schema.is_none();

        let compressed_size = if let Some(existing_schema) = stream_schema {
            // validate schema before processing the event
            if existing_schema != inferred_schema {
                return Err(Error::SchemaMismatch(self.stream_name.clone()));
            } else {
                self.process_event(event)?
            }
        } else {
            // if stream schema is none then it is first event,
            // process first event and store schema in obect store
            self.process_first_event(event, inferred_schema, storage)
                .await?
        };

        if let Err(e) = metadata::STREAM_INFO.update_stats(&self.stream_name, size, compressed_size)
        {
            error!("Couldn't update stream stats. {:?}", e);
        }

        if let Err(e) = metadata::STREAM_INFO.check_alerts(self).await {
            error!("Error checking for alerts. {:?}", e);
        }

        let msg = if is_first_event {
            format!(
                "Intial Event recieved for log stream {}, schema uploaded successfully",
                &self.stream_name,
            )
        } else {
            format!("Event recieved for log stream {}", &self.stream_name)
        };

        Ok(response::EventResponse { msg })
    }

    // This is called when the first event of a log stream is received. The first event is
    // special because we parse this event to generate the schema for the log stream. This
    // schema is then enforced on rest of the events sent to this log stream.
    async fn process_first_event<R: std::io::Read>(
        &self,
        mut event: json::Reader<R>,
        schema: Schema,
        storage: &impl ObjectStorage,
    ) -> Result<u64, Error> {
        let rb = event.next()?.ok_or(Error::MissingRecord)?;

        // Store record batch to Parquet file on local cache
        let compressed_size = self.convert_arrow_parquet(rb)?;

        // Put the inferred schema to object store
        let stream_name = &self.stream_name;

        storage
            .put_schema(stream_name.clone(), &schema)
            .await
            .map_err(|e| response::EventError {
                msg: format!(
                    "Failed to upload schema for log stream {} due to err: {}",
                    self.stream_name, e
                ),
            })?;

        // set the schema in memory for this stream
        metadata::STREAM_INFO
            .set_schema(&self.stream_name, schema)
            .map_err(|e| response::EventError {
                msg: format!(
                    "Failed to set schema for log stream {} due to err: {}",
                    &self.stream_name, e
                ),
            })?;

        Ok(compressed_size)
    }

    // event process all events after the 1st event. Concatenates record batches
    // and puts them in memory store for each event.
    fn process_event<R: std::io::Read>(&self, mut event: json::Reader<R>) -> Result<u64, Error> {
        let next_event_rb = event.next()?.ok_or(Error::MissingRecord)?;

        let compressed_size = match self.convert_parquet_rb_reader() {
            Ok(mut arrow_reader) => {
                let mut total_size = 0;
                let rb = arrow_reader.get_record_reader(2048).unwrap();
                for prev_rb in rb {
                    let new_rb = RecordBatch::concat(
                        &std::sync::Arc::new(arrow_reader.get_schema().unwrap()),
                        &[next_event_rb.clone(), prev_rb.unwrap()],
                    )?;
                    total_size += self.convert_arrow_parquet(new_rb)?;
                }

                total_size
            }
            Err(_) => self.convert_arrow_parquet(next_event_rb)?,
        };

        Ok(compressed_size)
    }

    // inferSchema is a constructor to Schema
    // returns raw arrow schema type and arrow schema to string type.
    fn infer_schema(&self) -> Result<Schema, Error> {
        let reader = self.body.as_bytes();
        let mut buf_reader = BufReader::new(reader);
        let inferred_schema = infer_json_schema(&mut buf_reader, None)?;

        Ok(inferred_schema)
    }

    fn get_reader(&self, arrow_schema: arrow::datatypes::Schema) -> json::Reader<&[u8]> {
        json::Reader::new(
            self.body.as_bytes(),
            Arc::new(arrow_schema),
            json::reader::DecoderOptions::new().with_batch_size(1024),
        )
    }

    fn body_size(&self) -> u64 {
        self.body.as_bytes().len() as u64
    }

    // convert arrow record batch to parquet
    // and write it to local cache path as a data.parquet file.
    fn convert_arrow_parquet(&self, rb: RecordBatch) -> Result<u64, Error> {
        let parquet_path = self.data_file_path();
        let parquet_file = fs::File::create(&parquet_path)?;
        let props = WriterProperties::builder().build();
        let mut writer =
            ArrowWriter::try_new(parquet_file, Arc::new(self.infer_schema()?), Some(props))?;
        writer.write(&rb)?;
        writer.close()?;

        let compressed_size = fs::metadata(parquet_path)?.len();

        Ok(compressed_size)
    }

    pub fn convert_parquet_rb_reader(&self) -> Result<ParquetFileArrowReader, Error> {
        let file = fs::File::open(&self.data_file_path())?;
        let file_reader = SerializedFileReader::new(file)?;
        let arrow_reader = ParquetFileArrowReader::new(Arc::new(file_reader));

        Ok(arrow_reader)
    }
}
