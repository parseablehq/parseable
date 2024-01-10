/*
 * Parseable Server (C) 2022 - 2023 Parseable, Inc.
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

use base64::{engine::general_purpose::STANDARD, Engine as _};
use bytes::Bytes;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::BTreeMap;
use std::str;

#[derive(Serialize, Deserialize, Debug)]
struct Message {
    #[serde(rename = "records")]
    records: Vec<Data>,
    #[serde(rename = "requestId")]
    request_id: String,
    timestamp: u64,
}
#[derive(Serialize, Deserialize, Debug)]
struct Data {
    #[serde(rename = "data")]
    data: String,
}

pub fn flatten_kinesis_logs(body: &Bytes) -> Vec<BTreeMap<String, Value>> {
    let body_str = std::str::from_utf8(body).unwrap();
    let message: Message = serde_json::from_str(body_str).unwrap();
    let mut vec_kinesis_json: Vec<BTreeMap<String, Value>> = Vec::new();

    for record in message.records.iter() {
        let bytes = STANDARD.decode(record.data.clone()).unwrap();
        let json_string: String = String::from_utf8(bytes).unwrap();
        let json: serde_json::Value = serde_json::from_str(&json_string).unwrap();
        let mut kinesis_json: BTreeMap<String, Value> = match serde_json::from_value(json) {
            Ok(value) => value,
            Err(error) => panic!("Failed to deserialize JSON: {}", error),
        };

        kinesis_json.insert(
            "requestId".to_owned(),
            Value::String(message.request_id.clone()),
        );
        kinesis_json.insert(
            "timestamp".to_owned(),
            Value::String(message.timestamp.to_string()),
        );

        vec_kinesis_json.push(kinesis_json);
    }
    vec_kinesis_json
}
