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
 */

use arrow_json::reader::infer_json_schema_from_iterator;
use arrow_schema::Schema;
use once_cell::sync::OnceCell;
use serde::Deserialize;
use serde_json::Value;
use std::{collections::HashMap, fs, path::Path};

use crate::{event::format::update_data_type_to_datetime, utils::json::flatten_json_body};

// Expose some static variables for internal usage
pub static KNOWN_SCHEMA_LIST: OnceCell<HashMap<String, Schema>> = OnceCell::new();
const FORMATS_JSON: &str = include_str!("known-formats/formats.json");

#[derive(Debug, Deserialize)]
struct Format {
    name: String,
    schema_type: String,
    sample_json_path: String,
}

pub fn detect_schema() -> HashMap<String, Schema> {
    let mut known_schema_list: HashMap<String, Schema> = HashMap::new();
    let json_data: serde_json::Value = serde_json::from_str(FORMATS_JSON).unwrap();

    let formats: Vec<Format> =
        serde_json::from_value(json_data).expect("Failed to parse formats.json");

    for format in &formats {
        let sample_path = Path::new(&format.sample_json_path);
        let schema_type = &format.schema_type;
        let _name = &format.name;
        match fs::read_to_string(sample_path) {
            Ok(content) => match serde_json::from_str::<Value>(&content) {
                Ok(json) => {
                    let flattened_json = flatten_json_body(json, None, None, None, false).unwrap();
                    let sample_json_records = [flattened_json.clone()];
                    let mut schema =
                        infer_json_schema_from_iterator(sample_json_records.iter().map(Ok))
                            .unwrap();
                    schema = update_data_type_to_datetime(schema, flattened_json, Vec::new());
                    known_schema_list.insert(schema_type.to_string(), schema);
                }
                Err(err) => eprintln!("Invalid JSON in {}: {}", sample_path.display(), err),
            },
            Err(err) => eprintln!("Failed to read {}: {}", sample_path.display(), err),
        }
    }
    prepare_known_schema_list(known_schema_list.clone());
    known_schema_list
}

pub fn prepare_known_schema_list(known_schema_list: HashMap<String, Schema>) {
    KNOWN_SCHEMA_LIST
        .set(known_schema_list)
        .expect("only set once")
}

pub fn get_known_schema_list() -> &'static HashMap<String, Schema> {
    KNOWN_SCHEMA_LIST
        .get()
        .expect("fetch schema list from static variable")
}

pub fn validate_schema_type(schema: &Schema) -> String {
    let known_schema_list = get_known_schema_list();
    let mut schema_type = String::default();
    for (known_schema_type, known_schema) in known_schema_list.iter() {
        if known_schema == schema {
            schema_type = known_schema_type.to_string();
            break;
        }
    }
    schema_type
}
