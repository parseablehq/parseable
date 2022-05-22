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
use crate::Error;

pub fn rem_first_and_last(value: &str) -> &str {
    let mut chars = value.chars();
    chars.next();
    chars.next_back();

    chars.as_str()
}

pub fn convert_parquet_rb_reader(
    file: std::fs::File,
) -> Result<parquet::arrow::arrow_reader::ParquetRecordBatchReader, Error> {
    let file_reader = SerializedFileReader::new(file)?;
    let mut arrow_reader = ParquetFileArrowReader::new(Arc::new(file_reader));
    let record_reader = arrow_reader.get_record_reader(2048)?;

    Ok(record_reader)
}

pub fn flatten_json_body(
    body: web::Json<serde_json::Value>,
    labels: Option<String>,
) -> Result<String, Error> {
    let mut collector_labels = HashMap::new();

    collector_labels.insert("labels".to_string(), labels.unwrap());

    let mut flat_value: Value = json!({});
    let new_body = merge(&body, &collector_labels);
    flatten_json::flatten(&new_body, &mut flat_value, None, true, Some("_")).unwrap();
    let flattened = serde_json::to_string(&flat_value)?;

    Ok(flattened)
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

// TODO: add more sql keywords here in lower case
const DENIED_NAMES: &[&str] = &[
    "select", "from", "where", "group", "by", "order", "limit", "offset", "join", "and",
];

pub fn validate_stream_name(str_name: &str) -> Result<(), Error> {
    if str_name.is_empty() {
        return Err(Error::EmptyName);
    }

    if str_name.chars().all(char::is_numeric) {
        return Err(Error::NameNumericOnly(str_name.to_owned()));
    }

    for c in str_name.chars() {
        match c {
            ' ' => return Err(Error::NameWhiteSpace(str_name.to_owned())),
            c if !c.is_alphanumeric() => return Err(Error::NameSpecialChar(str_name.to_owned())),
            c if c.is_ascii_uppercase() => return Err(Error::NameUpperCase(str_name.to_owned())),
            _ => {}
        }
    }

    if DENIED_NAMES.contains(&str_name) {
        return Err(Error::SQLKeyword(str_name.to_owned()));
    }

    Ok(())
}

pub fn get_cache_path(stream_name: &str) -> String {
    format!("{}/{}", option::get_opts().local_disk_path, stream_name)
}

pub fn read_schema_from_file(stream_name: &str) -> Result<String, Error> {
    let schema = fs::read_to_string(format!("{}{}", get_cache_path(stream_name), "/.schema"))?;

    Ok(schema)
}

pub fn get_scheme() -> String {
    let opt = option::get_opts();
    let mut scheme = "http";
    if let (Some(_), Some(_)) = (opt.tls_cert_path, opt.tls_key_path) {
        scheme = "https";
    }

    scheme.to_string()
}
