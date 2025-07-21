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

use base64::{Engine as _, engine::general_purpose::STANDARD};
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};
use std::str;

use crate::utils::json::flatten::{generic_flattening, has_more_than_max_allowed_levels};

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
pub async fn flatten_kinesis_logs(message: Message) -> Result<Vec<Value>, anyhow::Error> {
    let mut vec_kinesis_json = Vec::new();

    for record in message.records.iter() {
        let bytes = STANDARD.decode(record.data.clone())?;
        if let Ok(json_string) = String::from_utf8(bytes) {
            let json: serde_json::Value = serde_json::from_str(&json_string)?;
            // Check if the JSON has more than the allowed levels of nesting
            // If it has less than or equal to the allowed levels, we flatten it.
            // If it has more than the allowed levels, we just push it as is
            // without flattening or modifying it.
            if !has_more_than_max_allowed_levels(&json, 1) {
                let flattened_json_arr = generic_flattening(&json)?;
                for flattened_json in flattened_json_arr {
                    let mut kinesis_json: Map<String, Value> =
                        serde_json::from_value(flattened_json)?;
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
            } else {
                // If the JSON has more than the allowed levels, we just push it as is
                // without flattening or modifying it.
                // This is a fallback to ensure we don't lose data.
                tracing::warn!(
                    "Kinesis log with requestId {} and timestamp {} has more than the allowed levels of nesting, skipping flattening for this record.",
                    message.request_id,
                    message.timestamp
                );
                vec_kinesis_json.push(json);
            }
        } else {
            tracing::error!(
                "Failed to decode base64 data for kinesis log with requestId {} and timestamp {}",
                message.request_id,
                message.timestamp
            );
            return Err(anyhow::anyhow!(
                "Failed to decode base64 data for record with requestId {} and timestamp {}",
                message.request_id,
                message.timestamp
            ));
        }
    }

    Ok(vec_kinesis_json)
}
