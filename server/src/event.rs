use actix_web::{web, HttpResponse};
use arrow::json;
use arrow::json::reader::infer_json_schema;
use arrow::record_batch::RecordBatch;
use bytes::Bytes;
use parquet::arrow::arrow_writer::ArrowWriter;
use parquet::file::properties::WriterProperties;
use std::collections::HashMap;
use std::fs;
use std::io::{BufReader, Cursor, Seek, SeekFrom, Write};
use std::sync::{Arc, Mutex};

use crate::handler;
use lazy_static::lazy_static;

lazy_static! {
    pub static ref HASHMAP: Mutex<HashMap<String, RecordBatch>> = {
        let m = HashMap::new();
        Mutex::new(m)
    };
}

// Event holds all values for server to process into record batch.
pub struct Event {
    pub body: web::Json<serde_json::Value>,
    pub stream_name: String,
    pub path: String,
    pub schema: Bytes,
}

impl Event {
    pub fn initial_event(&self) -> HttpResponse {
        let mut map = HASHMAP.lock().unwrap();

        let mut c = Cursor::new(Vec::new());

        let str_body = format!("{}", &self.body);
        let reader = str_body.as_bytes();

        c.write_all(reader).unwrap();
        c.seek(SeekFrom::Start(0)).unwrap();

        let mut buf_reader = BufReader::new(c);
        let buf_reader1 = BufReader::new(reader);

        let inferred_schema = infer_json_schema(&mut buf_reader, None).unwrap();
        let str_inferred_schema = format!("{}", serde_json::to_string(&inferred_schema).unwrap());

        let mut event = json::Reader::new(buf_reader1, Arc::new(inferred_schema), 1024, None);
        let b1 = event.next().unwrap().unwrap();
        map.insert(self.stream_name.to_string(), b1);
        drop(map);

        match handler::put_schema(&self.stream_name, str_inferred_schema) {
            Ok(_) => {
                HttpResponse::Ok().body(format!("Uploading event to Stream {} ", self.stream_name))
            }
            Err(_) => HttpResponse::Ok().body(format!("Stream {} doesn't exist", self.stream_name)),
        }
    }

    pub fn next_event(&self) -> (RecordBatch, arrow::datatypes::Schema, HttpResponse) {
        // The schema is not empty here, so this stream already has events.
        // Proceed with validating against current schema and adding event to record batch.
        let str_inferred_schema = self.return_schema();
        if self.schema != str_inferred_schema.1 {
            // TODO: return nil, nil and invalid HTTP response
        }

        let mut c = Cursor::new(Vec::new());

        let str_body = format!("{}", self.body);
        let reader = str_body.as_bytes();

        c.write_all(reader).unwrap();
        c.seek(SeekFrom::Start(0)).unwrap();
        let buf_reader1 = BufReader::new(reader);

        let schema = self.return_schema();
        let schema_clone = schema.clone();

        let mut event = json::Reader::new(buf_reader1, Arc::new(schema.0), 1024, None);
        let b1 = event.next().unwrap().unwrap();
        return (
            b1,
            schema_clone.0,
            HttpResponse::Ok().body(format!("Schema already present for Stream")),
        );
    }

    fn return_schema(&self) -> (arrow::datatypes::Schema, std::string::String) {
        let str_body = format!("{}", self.body);
        let reader = str_body.as_bytes();
        let mut buf_reader = BufReader::new(reader);
        let inferred_schema = infer_json_schema(&mut buf_reader, None).unwrap();
        let str_inferred_schema = format!("{}", serde_json::to_string(&inferred_schema).unwrap());
        return (inferred_schema, str_inferred_schema);
    }

    fn create_parquet_file(&self) -> std::fs::File {
        let dir_name = format!("{}{}{}", &self.path, "/", &self.stream_name);
        let _res = fs::create_dir_all(dir_name.clone());
        let file_name = format!("{}{}{}", dir_name, "/", "data.parquet");
        let parquet_file = fs::File::create(file_name).unwrap();
        parquet_file
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
