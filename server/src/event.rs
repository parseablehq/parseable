/*
 * Parseable Server (C) 2022 Parseable, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

use arrow::json;
use arrow::json::reader::infer_json_schema;
use arrow::record_batch::RecordBatch;
use bytes::Bytes;
use parquet::arrow::arrow_writer::ArrowWriter;
use parquet::file::properties::WriterProperties;
use std::fs;
use std::io::{BufReader, Cursor, Seek, SeekFrom, Write};
use std::sync::Arc;

use crate::mem_store;
use crate::response;
use crate::storage;

// Event holds all values for server to process into record batch.
pub struct Event {
    pub body: String,
    pub stream_name: String,
    pub path: String,
    pub schema: Bytes,
}

impl Event {
    pub fn initial_event(&self) -> Result<response::EventResponse, response::EventError> {
        let mut c = Cursor::new(Vec::new());
        let reader = self.body.as_bytes();

        c.write_all(reader).unwrap();
        c.seek(SeekFrom::Start(0)).unwrap();
        let buf_reader = BufReader::new(reader);

        let (inferred_schema, str_inferred_schema) = self.return_schema();

        let mut event = json::Reader::new(buf_reader, Arc::new(inferred_schema), 1024, None);
        let b1 = event.next().unwrap().unwrap();
        mem_store::MEM_STREAMS::put(
            self.stream_name.to_string(),
            mem_store::Stream {
                stream_schema: Some(str_inferred_schema.clone()),
                rb: Some(b1.clone()),
            },
        );
        match storage::put_schema(&self.stream_name, str_inferred_schema) {
            Ok(_) => Ok(response::EventResponse {
                msg: format!(
                    "Intial Event recieved for Stream {}, schema uploaded successfully",
                    self.stream_name
                ),
                rb: Some(b1),
                schema: None,
            }),
            Err(e) => Err(response::EventError {
                msg: format!(
                    "Failed to upload schema for Stream {} due to err: {}",
                    self.stream_name, e
                ),
            }),
        }
    }

    pub fn next_event(&self) -> Result<response::EventResponse, response::EventError> {
        // The schema is not empty here, so this stream already has events.
        // Proceed with validating against current schema and adding event to record batch.
        let str_inferred_schema = self.return_schema();
        if self.schema != str_inferred_schema.1 {
            return Err(response::EventError {
                msg: format!(
                    "Event schema doesn't match schema for Stream {}",
                    self.stream_name
                ),
            });
        }

        let mut c = Cursor::new(Vec::new());
        let reader = self.body.as_bytes();
        c.write_all(reader).unwrap();
        c.seek(SeekFrom::Start(0)).unwrap();

        let schema = self.return_schema();
        let schema_clone = schema.clone();

        let mut event = json::Reader::new(self.body.as_bytes(), Arc::new(schema.0), 1024, None);
        let b1 = event.next().unwrap().unwrap();

        Ok(response::EventResponse {
            msg: format!("Event recieved for Stream {}", &self.stream_name),
            rb: Some(b1),
            schema: Some(schema_clone.0),
        })
    }

    fn return_schema(&self) -> (arrow::datatypes::Schema, std::string::String) {
        let reader = self.body.as_bytes();
        let mut buf_reader = BufReader::new(reader);
        let inferred_schema = infer_json_schema(&mut buf_reader, None).unwrap();
        let str_inferred_schema = format!("{}", serde_json::to_string(&inferred_schema).unwrap());
        return (inferred_schema, str_inferred_schema);
    }

    fn create_parquet_file(&self) -> std::fs::File {
        let dir_name = format!("{}{}{}", &self.path, "/", &self.stream_name);
        let file_name = format!("{}{}{}", dir_name, "/", "data.parquet");
        let parquet_file = fs::File::create(file_name).unwrap();
        return parquet_file;
    }

    pub fn convert_arrow_parquet(&self, rb: RecordBatch) {
        let props = WriterProperties::builder().build();
        let mut writer = ArrowWriter::try_new(
            self.create_parquet_file(),
            Arc::new(self.return_schema().0),
            Some(props),
        )
        .unwrap();
        writer.write(&rb).expect("Writing batch");
        writer.close().unwrap();
    }
}
