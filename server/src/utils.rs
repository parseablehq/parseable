use actix_web::web;
use flatten_json;
use parquet::arrow::{ArrowReader, ParquetFileArrowReader};
use parquet::file::reader::SerializedFileReader;
use serde_json::{json, Value};
use std::sync::Arc;

pub fn rem_first_and_last(value: &str) -> &str {
    let mut chars = value.chars();
    chars.next();
    chars.next_back();
    chars.as_str()
}

pub fn convert_parquet_rb_reader(
    file: std::fs::File,
) -> parquet::arrow::arrow_reader::ParquetRecordBatchReader {
    let file_reader = SerializedFileReader::new(file).unwrap();
    let mut arrow_reader = ParquetFileArrowReader::new(Arc::new(file_reader));
    let record_batch_reader = arrow_reader.get_record_reader(2048).unwrap();
    record_batch_reader
}

pub fn flatten_json_body(body: web::Json<serde_json::Value>) -> String {
    let mut flat_value: Value = json!({});
    flatten_json::flatten(&body, &mut flat_value, None, true, Some("_")).unwrap();
    format!("{:}", serde_json::to_string(&flat_value).unwrap())
}
