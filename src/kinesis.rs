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

use base64::{engine::general_purpose::STANDARD, Engine as _};
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};
use std::str;

#[derive(Serialize, Deserialize, Debug)]
#[serde(rename_all = "camelCase")]
pub struct Message {
    records: Vec<Data>,
    request_id: String,
    timestamp: u64,
}
#[derive(Serialize, Deserialize, Debug)]
struct Data {
    data: String,
}

// Flatten Kinesis logs is used to flatten the Kinesis logs into a queryable JSON format.
// Kinesis logs are in the format
// {
//     "requestId": "9b848d8a-2d89-474b-b073-04b8e5232210",
//     "timestamp": 1705026780451,
//     "records": [
//         {
//             "data": "eyJDSEFOR0UiOi0wLjQ1LCJQUklDRSI6NS4zNiwiVElDS0VSX1NZTUJPTCI6IkRFRyIsIlNFQ1RPUiI6IkVORVJHWSJ9"
//         }
//     ]
// }
// The data field is base64 encoded JSON (there can be multiple data fields), and there is a requestId and timestamp field.
// Kinesis logs are flattened to the following format:
// {
//     "CHANGE": 3.16,
//     "PRICE": 73.76,
//     "SECTOR": "RETAIL",
//     "TICKER_SYMBOL": "WMT",
//     "p_metadata": "",
//     "p_tags": "",
//     "p_timestamp": "2024-01-11T09:08:34.290",
//     "requestId": "b858288a-f5d8-4181-a746-3f3dd716be8a",
//     "timestamp": "1704964113659"
// }
pub fn flatten_kinesis_logs(message: Message) -> Vec<Value> {
    let mut vec_kinesis_json = Vec::new();

    for record in message.records.iter() {
        let bytes = STANDARD.decode(record.data.clone()).unwrap();
        let json_string: String = String::from_utf8(bytes).unwrap();
        let json: serde_json::Value = serde_json::from_str(&json_string).unwrap();
        let mut kinesis_json: Map<String, Value> = match serde_json::from_value(json) {
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

        vec_kinesis_json.push(Value::Object(kinesis_json));
    }

    vec_kinesis_json
}

#[cfg(test)]
mod tests {
    use serde_json::{json, Value};

    use super::{flatten_kinesis_logs, Message};

    #[test]
    fn flatten_kinesis_logs_decodes_base64_data() {
        let message: Message = serde_json::from_value(json!( {
            "requestId": "9b848d8a-2d89-474b-b073-04b8e5232210".to_string(),
            "timestamp": 1705026780451_i64,
            "records": [
                 {
                    "data": "eyJDSEFOR0UiOi0wLjQ1LCJQUklDRSI6NS4zNiwiVElDS0VSX1NZTUJPTCI6IkRFRyIsIlNFQ1RPUiI6IkVORVJHWSJ9".to_string(),
                },
                 {
                    "data": "eyJDSEFOR0UiOjMuMTYsIlBSSUNFIjo3My43NiwiVElDS0VSX1NZTUJPTCI6IldNVCIsIlNFQ1RPUiI6IlJFVEFJTCJ9".to_string(),
                },
            ],
        })).unwrap();

        let result = flatten_kinesis_logs(message);
        assert_eq!(result.len(), 2);

        let Value::Object(map) = &result[0] else {
            panic!("Expected first result to be a JSON object");
        };
        assert_eq!(map.get("CHANGE").unwrap().as_f64().unwrap(), -0.45);
        assert_eq!(map.get("PRICE").unwrap().as_f64().unwrap(), 5.36);
        assert_eq!(map.get("TICKER_SYMBOL").unwrap().as_str().unwrap(), "DEG");
        assert_eq!(map.get("SECTOR").unwrap().as_str().unwrap(), "ENERGY");
        assert_eq!(
            map.get("requestId").unwrap().as_str().unwrap(),
            "9b848d8a-2d89-474b-b073-04b8e5232210"
        );
        assert_eq!(
            map.get("timestamp").unwrap().as_str().unwrap(),
            "1705026780451"
        );

        let Value::Object(map) = &result[1] else {
            panic!("Expected second result to be a JSON object");
        };
        assert_eq!(map.get("CHANGE").unwrap().as_f64().unwrap(), 3.16);
        assert_eq!(map.get("PRICE").unwrap().as_f64().unwrap(), 73.76);
        assert_eq!(map.get("TICKER_SYMBOL").unwrap(), "WMT");
        assert_eq!(map.get("SECTOR").unwrap(), "RETAIL");
        assert_eq!(
            map.get("requestId").unwrap().as_str().unwrap(),
            "9b848d8a-2d89-474b-b073-04b8e5232210"
        );
        assert_eq!(
            map.get("timestamp").unwrap().as_str().unwrap(),
            "1705026780451"
        );
    }

    #[test]
    fn flatten_kinesis_logs_adds_request_id_and_timestamp() {
        let message: Message = serde_json::from_value(json!( {
            "requestId": "9b848d8a-2d89-474b-b073-04b8e5232210".to_string(),
            "timestamp": 1705026780451_i64,
            "records": [
                 {
                    "data": "eyJDSEFOR0UiOi0wLjQ1LCJQUklDRSI6NS4zNiwiVElDS0VSX1NZTUJPTCI6IkRFRyIsIlNFQ1RPUiI6IkVORVJHWSJ9".to_string(),
                },
                 {
                    "data": "eyJDSEFOR0UiOjMuMTYsIlBSSUNFIjo3My43NiwiVElDS0VSX1NZTUJPTCI6IldNVCIsIlNFQ1RPUiI6IlJFVEFJTCJ9".to_string(),
                },
            ],
        })).unwrap();

        let result = flatten_kinesis_logs(message);
        assert_eq!(result.len(), 2);

        let event = result[0].as_object().unwrap();
        assert_eq!(
            event.get("requestId").unwrap().as_str().unwrap(),
            "9b848d8a-2d89-474b-b073-04b8e5232210"
        );
        assert_eq!(
            event.get("timestamp").unwrap().as_str().unwrap(),
            "1705026780451"
        );

        let event = result[1].as_object().unwrap();
        assert_eq!(
            event.get("requestId").unwrap().as_str().unwrap(),
            "9b848d8a-2d89-474b-b073-04b8e5232210"
        );
        assert_eq!(
            event.get("timestamp").unwrap().as_str().unwrap(),
            "1705026780451"
        );
    }

    #[test]
    #[should_panic(expected = "InvalidByte(7, 95)")]
    fn malformed_json_after_base64_decoding() {
        let message: Message = serde_json::from_value(json!({
            "requestId": "9b848d8a-2d89-474b-b073-04b8e5232210".to_string(),
            "timestamp": 1705026780451_i64,
            "records": [ {
                "data": "invalid_base64_data".to_string(),
            }],
        }))
        .unwrap();

        flatten_kinesis_logs(message);
    }
}
