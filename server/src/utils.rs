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
 */

use actix_web::web;

use parquet::arrow::{ArrowReader, ParquetFileArrowReader};
use parquet::file::reader::SerializedFileReader;
use serde_json::{json, Value};
use std::collections::HashMap;
use std::fs;
use std::sync::Arc;

use crate::option;

const API_BASE_PATH: &str = "/api";
const API_VERSION: &str = "/v1";

pub fn stream_path(stream_name: &str) -> String {
    format!(
        "{}{}{}{}",
        API_BASE_PATH, API_VERSION, "/stream", stream_name
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

pub fn flatten_json_body(body: web::Json<serde_json::Value>, labels: Option<String>) -> String {
    let mut collector_labels = HashMap::new();

    collector_labels.insert("labels".to_string(), labels.unwrap());

    let mut flat_value: Value = json!({});
    let new_body = merge(&body, &collector_labels);
    flatten_json::flatten(&new_body, &mut flat_value, None, true, Some("_")).unwrap();
    serde_json::to_string(&flat_value).unwrap()
}

fn merge(v: &Value, fields: &HashMap<String, String>) -> Value {
    match v {
        Value::Object(m) => {
            let mut m = m.clone();
            for (k, v) in fields {
                m.insert(k.clone(), Value::String(v.clone()));
            }
            Value::Object(m)
        }
        v => v.clone(),
    }
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

pub fn get_scheme() -> String {
    let opt = option::get_opts();
    let mut scheme = "http";
    if let (Some(_), Some(_)) = (opt.tls_cert_path, opt.tls_key_path) {
        scheme = "https";
    }

    scheme.to_string()
}
