/*
 * Parseable Server (C) 2022 - 2025 Parseable, Inc.
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

use crate::parseable::PARSEABLE;
use crate::utils::json::flatten::{
    FlattenBudget, FlattenCost, JsonFlattenError, expand_generic_flattening, flatten_pipeline_cost,
    has_more_than_levels, string_json_len,
};

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
// Each decoded data field is one event for `budget`, even when it holds an array, charged
// for its generic rows or, when larger, those rows structurally flattened, plus the metadata.
// It is charged before its rows are built; when that fails, the rows built for the records
// before it are dropped with the error.
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
pub fn flatten_kinesis_logs(
    message: Message,
    budget: &mut FlattenBudget,
) -> Result<Vec<Value>, anyhow::Error> {
    flatten_kinesis_logs_inner(message, budget, PARSEABLE.options.event_flatten_level)
}

fn flatten_kinesis_logs_inner(
    message: Message,
    budget: &mut FlattenBudget,
    max_flatten_level: usize,
) -> Result<Vec<Value>, anyhow::Error> {
    let mut vec_kinesis_json = Vec::new();
    let timestamp = message.timestamp.to_string();
    let metadata_bytes = metadata_json_len(&message.request_id, &timestamp);

    for record in message.records.iter() {
        let bytes = STANDARD.decode(record.data.clone())?;
        if let Ok(json_string) = String::from_utf8(bytes) {
            let json: serde_json::Value = serde_json::from_str(&json_string)?;
            // Check if the JSON has more than the allowed levels of nesting
            // If it has less than or equal to the allowed levels, we flatten it.
            // If it has more than the allowed levels, we just push it as is
            // without flattening or modifying it.
            if !has_more_than_levels(&json, 1, max_flatten_level) {
                // `push_logs` structurally flattens the rows later, charge that now too
                let cost = flatten_pipeline_cost(&json, budget.limits())?.charged();
                // Every row gets its own copy of the metadata, charge it before building any
                let limits = budget.limits();
                let metadata = metadata_bytes
                    .and_then(|bytes| bytes.checked_mul(cost.rows))
                    .ok_or(JsonFlattenError::BytesPerRequestLimitExceeded(
                        limits.max_bytes_per_request,
                    ))?;
                let cost = cost.add(
                    FlattenCost {
                        rows: 0,
                        bytes: metadata,
                    },
                    limits,
                )?;
                budget.charge_event(cost)?;
                let flattened_json_arr = expand_generic_flattening(&json)?;
                for flattened_json in flattened_json_arr {
                    let mut kinesis_json: Map<String, Value> =
                        serde_json::from_value(flattened_json)?;
                    kinesis_json.insert(
                        "requestId".to_owned(),
                        Value::String(message.request_id.clone()),
                    );
                    kinesis_json.insert("timestamp".to_owned(), Value::String(timestamp.clone()));

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

/// Compact JSON bytes the `requestId` and `timestamp` fields add to a row, with their
/// delimiters. Charged in full even for rows that already have these fields, which get
/// overwritten, or are empty and need one comma less.
fn metadata_json_len(request_id: &str, timestamp: &str) -> Option<usize> {
    const FIXED: usize = r#","requestId":,"timestamp":"#.len();
    string_json_len(request_id)?
        .checked_add(string_json_len(timestamp)?)?
        .checked_add(FIXED)
}

#[cfg(test)]
mod tests {
    use base64::{Engine as _, engine::general_purpose::STANDARD};
    use serde_json::{Value, json};

    use super::{Data, Message, flatten_kinesis_logs_inner};
    use crate::utils::json::flatten::{FlattenBudget, FlattenLimits, JsonFlattenError};

    fn message(records: &[Value]) -> Message {
        message_with_id(records, "id")
    }

    fn message_with_id(records: &[Value], request_id: &str) -> Message {
        Message {
            records: records
                .iter()
                .map(|record| Data {
                    data: STANDARD.encode(record.to_string()),
                })
                .collect(),
            request_id: request_id.to_owned(),
            timestamp: 1,
        }
    }

    fn limits(rows_per_event: usize, rows_per_request: usize) -> FlattenBudget {
        FlattenBudget::new(FlattenLimits {
            max_rows_per_event: rows_per_event,
            max_rows_per_request: rows_per_request,
            max_bytes_per_request: 1 << 20,
        })
    }

    fn limit_error(result: anyhow::Result<Vec<Value>>) -> JsonFlattenError {
        result
            .unwrap_err()
            .downcast::<JsonFlattenError>()
            .expect("a flatten limit error")
    }

    // 2 + 3 rows, from elements that are small on their own
    fn record() -> Value {
        json!([{"a": [1, 2]}, {"b": [1, 2, 3]}])
    }

    #[test]
    fn decoded_record_is_one_event() {
        let mut budget = limits(5, 10);
        let rows = flatten_kinesis_logs_inner(message(&[record()]), &mut budget, 10).unwrap();
        assert_eq!(rows.len(), 5);
        assert_eq!(
            rows[0],
            json!({"a": 1, "requestId": "id", "timestamp": "1"})
        );
        assert_eq!(budget.used().rows, 5);

        assert!(matches!(
            limit_error(flatten_kinesis_logs_inner(
                message(&[record()]),
                &mut limits(4, 10),
                10
            )),
            JsonFlattenError::RowsPerEventLimitExceeded(4)
        ));
    }

    #[test]
    fn records_share_the_request_budget() {
        let mut budget = limits(5, 10);
        let rows =
            flatten_kinesis_logs_inner(message(&[record(), record()]), &mut budget, 10).unwrap();
        assert_eq!(rows.len(), 10);

        assert!(matches!(
            limit_error(flatten_kinesis_logs_inner(
                message(&[record(), record()]),
                &mut limits(5, 9),
                10
            )),
            JsonFlattenError::RowsPerRequestLimitExceeded(9)
        ));
    }

    #[test]
    fn records_nested_too_deep_are_kept_and_not_charged() {
        let deep = json!({"a": {"b": [1, 2, 3]}});
        let mut budget = limits(1, 1);
        let rows = flatten_kinesis_logs_inner(message(std::slice::from_ref(&deep)), &mut budget, 1)
            .unwrap();
        assert_eq!(rows, vec![deep]);
        assert_eq!(budget.used().rows, 0);
    }

    fn byte_limit(bytes: usize) -> FlattenBudget {
        FlattenBudget::new(FlattenLimits {
            max_rows_per_event: 1000,
            max_rows_per_request: 10_000,
            max_bytes_per_request: bytes,
        })
    }

    #[test]
    fn metadata_of_every_row_is_charged_exactly() {
        // Escapes in the request id are charged as serialized
        let records = [record(), json!({"c": "x\n\"y"}), json!({})];
        let id = "id-\"quoted\"\t";
        let mut budget = byte_limit(1 << 20);
        let rows =
            flatten_kinesis_logs_inner(message_with_id(&records, id), &mut budget, 10).unwrap();
        assert_eq!(rows.len(), 7);
        let bytes: usize = rows
            .iter()
            .map(|row| serde_json::to_vec(row).unwrap().len())
            .sum();
        // The empty row is charged one comma that it does not get
        assert_eq!(budget.used().bytes, bytes + 1);

        let used = budget.used().bytes;
        flatten_kinesis_logs_inner(message_with_id(&records, id), &mut byte_limit(used), 10)
            .unwrap();
        assert!(matches!(
            limit_error(flatten_kinesis_logs_inner(
                message_with_id(&records, id),
                &mut byte_limit(used - 1),
                10
            )),
            JsonFlattenError::BytesPerRequestLimitExceeded(limit) if limit == used - 1
        ));
    }

    #[test]
    fn large_metadata_repeated_over_many_rows_is_rejected() {
        // 10 records of 100 small rows each: about 7 KiB of rows, but 1000 copies of a
        // 4 KiB request id would be 4 MiB
        let records: Vec<Value> = (0..10)
            .map(|_| json!({"a": (0..100).collect::<Vec<_>>()}))
            .collect();
        let id = "r".repeat(4096);
        let mut budget = byte_limit(64 * 1024);
        assert!(matches!(
            limit_error(flatten_kinesis_logs_inner(
                message_with_id(&records, &id),
                &mut budget,
                10
            )),
            JsonFlattenError::BytesPerRequestLimitExceeded(65536)
        ));
        // A single record is over the limit by itself, so nothing was charged
        assert_eq!(budget.used().bytes, 0);
        // The same rows pass with a short request id
        flatten_kinesis_logs_inner(message(&records), &mut byte_limit(64 * 1024), 10).unwrap();
    }

    #[test]
    fn metadata_bytes_add_up_over_records() {
        // Each record fits in the byte limit on its own, but not both of them
        let records = [json!({"a": [1, 2, 3]}), json!({"a": [1, 2, 3]})];
        let id = "r".repeat(100);
        let mut one = byte_limit(1 << 20);
        flatten_kinesis_logs_inner(message_with_id(&records[..1], &id), &mut one, 10).unwrap();
        let per_record = one.used().bytes;
        assert!(per_record > 3 * 100);

        let mut budget = byte_limit(2 * per_record - 1);
        assert!(matches!(
            limit_error(flatten_kinesis_logs_inner(
                message_with_id(&records, &id),
                &mut budget,
                10
            )),
            JsonFlattenError::BytesPerRequestLimitExceeded(_)
        ));
        assert_eq!(budget.used().bytes, per_record);
        flatten_kinesis_logs_inner(
            message_with_id(&records, &id),
            &mut byte_limit(2 * per_record),
            10,
        )
        .unwrap();
    }

    #[test]
    fn structurally_flattened_field_names_are_charged() {
        // 4 rows of 32 fields below a long parent, whose name `push_logs` later copies into
        // every field: the generic rows and metadata fit in 4096 bytes, those fields do not
        let long_parent = |parent: &str| {
            let fields: serde_json::Map<String, Value> =
                (0..32).map(|i| (format!("f{i}"), json!(i))).collect();
            let mut map = serde_json::Map::new();
            map.insert(parent.to_owned(), Value::Object(fields));
            map.insert("r".to_owned(), json!([0, 1, 2, 3]));
            Value::Object(map)
        };
        let long = long_parent(&"p".repeat(128));
        let generic =
            crate::utils::json::flatten::generic_flattening_cost(&long, &FlattenLimits::DEFAULT)
                .unwrap();
        assert!(generic.bytes + 4 * r#","requestId":"id","timestamp":"1""#.len() <= 4096);
        let mut budget = byte_limit(4096);
        assert!(matches!(
            limit_error(flatten_kinesis_logs_inner(
                message(std::slice::from_ref(&long)),
                &mut budget,
                10
            )),
            JsonFlattenError::BytesPerRequestLimitExceeded(4096)
        ));
        assert_eq!(budget.used().bytes, 0);

        // With a short parent the structurally flattened rows and their metadata are charged
        let mut budget = byte_limit(4096);
        let rows =
            flatten_kinesis_logs_inner(message(&[long_parent("p")]), &mut budget, 10).unwrap();
        let bytes: usize = rows
            .iter()
            .map(|row| {
                let mut row = row.clone();
                crate::utils::json::flatten::flatten(
                    &mut row,
                    "_",
                    None,
                    None,
                    None,
                    false,
                    &mut crate::utils::json::flatten::FlattenBudget::new(
                        crate::utils::json::flatten::GENEROUS_LIMITS,
                    ),
                )
                .unwrap();
                serde_json::to_vec(&row).unwrap().len()
            })
            .sum();
        assert_eq!(budget.used().bytes, bytes);
    }
}
