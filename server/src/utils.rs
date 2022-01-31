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

use actix_web::web;

use parquet::arrow::{ArrowReader, ParquetFileArrowReader};
use parquet::file::reader::SerializedFileReader;
use serde_json::{json, Value};
use std::fs;
use std::sync::Arc;

use crate::option;

const API_BASE_PATH: &str = "/api";
const API_VERSION: &str = "/v1";

pub fn stream_path() -> String {
    format!(
        "{}{}{}{}",
        API_BASE_PATH, API_VERSION, "/stream", "/{stream}"
    )
}

pub fn query_path() -> String {
    format!("{}{}{}", API_BASE_PATH, API_VERSION, "/query")
}

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

    arrow_reader.get_record_reader(2048).unwrap()
}

pub fn flatten_json_body(body: web::Json<serde_json::Value>) -> String {
    let mut flat_value: Value = json!({});
    flatten_json::flatten(&body, &mut flat_value, None, true, Some("_")).unwrap();
    serde_json::to_string(&flat_value).unwrap()
}

pub fn validate_stream_name(str_name: &str) -> Result<(), String> {
    if str_name.is_empty() {
        return Err(String::from("stream name cannot be empty"));
    }
    if str_name.contains(' ') {
        return Err(String::from("stream name cannot contain spaces"));
    }
    if !str_name.chars().all(char::is_alphanumeric) {
        return Err(String::from(
            "stream name cannot contain special characters",
        ));
    }
    if str_name.chars().any(|c| c.is_ascii_uppercase()) {
        return Err(String::from(
            "stream name cannot contain uppercase characters",
        ));
    }
    // add more sql keywords here in lower case
    let denied_names = [
        "select", "from", "where", "group", "by", "order", "limit", "offset", "join", "and",
    ];
    if denied_names.contains(&str_name) {
        return Err(String::from("stream name cannot be a sql keyword"));
    }
    Ok(())
}

pub fn get_cache_path(stream_name: &str) -> String {
    format!("{}/{}", option::get_opts().local_disk_path, stream_name)
}

#[allow(clippy::all)]
pub fn unbox<T>(value: Box<T>) -> T {
    *value
}

pub fn read_schema_from_file(stream_name: &str) -> String {
    fs::read_to_string(format!("{}{}", get_cache_path(stream_name), "/.schema")).unwrap()
}
