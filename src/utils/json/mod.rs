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

use std::fmt;
use std::num::NonZeroU32;

use flatten::{
    FlattenBudget, convert_to_array, has_more_than_max_allowed_levels, pipeline_flattening_owned,
};
use serde::de::Visitor;
use serde_json;
use serde_json::Value;
use tracing::info_span;

use crate::event::format::LogSource;
use crate::metadata::SchemaVersion;

pub mod flatten;
pub mod strict;

/// calls the function `flatten_json` which results Vec<Value> or Error
/// in case when Vec<Value> is returned, converts the Vec<Value> to Value of Array
/// this is to ensure recursive flattening does not happen for heavily nested jsons
/// Generic flattening is charged to `budget`, together with the structural flattening of its
/// rows that follows, see `flatten::charge_flatten_pipeline`
#[allow(clippy::too_many_arguments)]
#[cfg_attr(feature = "hotpath", hotpath::measure)]
pub fn flatten_json_body(
    body: Value,
    time_partition: Option<&String>,
    time_partition_limit: Option<NonZeroU32>,
    custom_partition: Option<&String>,
    schema_version: SchemaVersion,
    validation_required: bool,
    log_source: &LogSource,
    budget: &mut FlattenBudget,
) -> Result<Value, anyhow::Error> {
    // Flatten the json body only if new schema and not nested deeper than P_MAX_FLATTEN_LEVEL
    let apply_generic_flattening =
        should_apply_generic_flattening(&body, schema_version, log_source);
    flatten_json_body_inner(
        body,
        apply_generic_flattening,
        time_partition,
        time_partition_limit,
        custom_partition,
        validation_required,
        budget,
    )
}

fn flatten_json_body_inner(
    body: Value,
    apply_generic_flattening: bool,
    time_partition: Option<&String>,
    time_partition_limit: Option<NonZeroU32>,
    custom_partition: Option<&String>,
    validation_required: bool,
    budget: &mut FlattenBudget,
) -> Result<Value, anyhow::Error> {
    let mut nested_value = if apply_generic_flattening {
        convert_to_array(pipeline_flattening_owned(body, budget)?)?
    } else {
        body
    };
    flatten::flatten(
        &mut nested_value,
        "_",
        time_partition,
        time_partition_limit,
        custom_partition,
        validation_required,
        budget,
    )?;
    Ok(nested_value)
}

/// Checks if generic flattening should be applied based on schema version and log source
fn should_apply_generic_flattening(
    value: &Value,
    schema_version: SchemaVersion,
    log_source: &LogSource,
) -> bool {
    schema_version == SchemaVersion::V1
        && !has_more_than_max_allowed_levels(value, 1)
        && matches!(log_source, LogSource::Json | LogSource::Custom(_))
}

/// Applies generic flattening, charged to `budget` together with the structural flattening
/// below, and handles the result for partitioned processing
pub fn apply_generic_flattening_for_partition(
    element: Value,
    time_partition: Option<&String>,
    time_partition_limit: Option<NonZeroU32>,
    custom_partition: Option<&String>,
    budget: &mut FlattenBudget,
) -> Result<Vec<Value>, anyhow::Error> {
    let flattened_json = pipeline_flattening_owned(element, budget)?;

    if flattened_json.len() == 1 {
        // Single result - process normally
        let mut nested_value = flattened_json.into_iter().next().unwrap();
        flatten::flatten(
            &mut nested_value,
            "_",
            time_partition,
            time_partition_limit,
            custom_partition,
            true,
            budget,
        )?;
        Ok(vec![nested_value])
    } else {
        // Multiple results - process each individually
        let mut result = Vec::new();
        for item in flattened_json {
            let mut processed_item = item;
            flatten::flatten(
                &mut processed_item,
                "_",
                time_partition,
                time_partition_limit,
                custom_partition,
                true,
                budget,
            )?;
            result.push(processed_item);
        }
        Ok(result)
    }
}

/// Processes a single element for partitioned arrays
fn process_partitioned_element(
    element: Value,
    time_partition: Option<&String>,
    time_partition_limit: Option<NonZeroU32>,
    custom_partition: Option<&String>,
    schema_version: SchemaVersion,
    log_source: &LogSource,
    budget: &mut FlattenBudget,
) -> Result<Vec<Value>, anyhow::Error> {
    if should_apply_generic_flattening(&element, schema_version, log_source) {
        apply_generic_flattening_for_partition(
            element,
            time_partition,
            time_partition_limit,
            custom_partition,
            budget,
        )
    } else {
        let mut nested_value = element;
        flatten::flatten(
            &mut nested_value,
            "_",
            time_partition,
            time_partition_limit,
            custom_partition,
            true,
            budget,
        )?;
        Ok(vec![nested_value])
    }
}

/// Processes an array when partitioning is enabled
fn process_partitioned_array(
    arr: Vec<Value>,
    time_partition: Option<&String>,
    time_partition_limit: Option<NonZeroU32>,
    custom_partition: Option<&String>,
    schema_version: SchemaVersion,
    log_source: &LogSource,
    budget: &mut FlattenBudget,
) -> Result<Vec<Value>, anyhow::Error> {
    let mut result = Vec::new();

    for element in arr {
        let processed_elements = process_partitioned_element(
            element,
            time_partition,
            time_partition_limit,
            custom_partition,
            schema_version,
            log_source,
            budget,
        )?;
        result.extend(processed_elements);
    }

    Ok(result)
}

/// Processes non-array values when partitioning is enabled
fn process_partitioned_non_array(
    body: Value,
    time_partition: Option<&String>,
    time_partition_limit: Option<NonZeroU32>,
    custom_partition: Option<&String>,
    schema_version: SchemaVersion,
    log_source: &LogSource,
    budget: &mut FlattenBudget,
) -> Result<Vec<Value>, anyhow::Error> {
    // convert to an array for processing
    let arr = vec![body];
    let processed_elements = process_partitioned_array(
        arr,
        time_partition,
        time_partition_limit,
        custom_partition,
        schema_version,
        log_source,
        budget,
    )?;
    Ok(processed_elements)
}

/// Processes data when no partitioning is configured (original logic)
#[cfg_attr(feature = "hotpath", hotpath::measure)]
fn process_non_partitioned(
    body: Value,
    time_partition: Option<&String>,
    time_partition_limit: Option<NonZeroU32>,
    custom_partition: Option<&String>,
    schema_version: SchemaVersion,
    log_source: &LogSource,
    budget: &mut FlattenBudget,
) -> Result<Vec<Value>, anyhow::Error> {
    let data = flatten_json_body(
        body,
        time_partition,
        time_partition_limit,
        custom_partition,
        schema_version,
        true,
        log_source,
        budget,
    )?;

    // For non-partitioned processing, return the flattened data as a single item
    // If it's an array, it should be processed as one batch, not individual items
    Ok(vec![data])
}

#[cfg_attr(feature = "hotpath", hotpath::measure)]
pub fn convert_array_to_object(
    body: Value,
    time_partition: Option<&String>,
    time_partition_limit: Option<NonZeroU32>,
    custom_partition: Option<&String>,
    schema_version: SchemaVersion,
    log_source: &LogSource,
    budget: &mut FlattenBudget,
) -> Result<Vec<Value>, anyhow::Error> {
    let _span = info_span!("convert_array_to_object").entered();
    if time_partition.is_some() || custom_partition.is_some() {
        match body {
            Value::Array(arr) => process_partitioned_array(
                arr,
                time_partition,
                time_partition_limit,
                custom_partition,
                schema_version,
                log_source,
                budget,
            ),
            _ => process_partitioned_non_array(
                body,
                time_partition,
                time_partition_limit,
                custom_partition,
                schema_version,
                log_source,
                budget,
            ),
        }
    } else {
        process_non_partitioned(
            body,
            time_partition,
            time_partition_limit,
            custom_partition,
            schema_version,
            log_source,
            budget,
        )
    }
}

struct TrueFromStr;

impl Visitor<'_> for TrueFromStr {
    type Value = bool;

    fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
        formatter.write_str("a string containing \"true\"")
    }

    fn visit_borrowed_str<E>(self, v: &'_ str) -> Result<Self::Value, E>
    where
        E: serde::de::Error,
    {
        self.visit_str(v)
    }

    fn visit_str<E>(self, s: &str) -> Result<Self::Value, E>
    where
        E: serde::de::Error,
    {
        match s {
            "true" => Ok(true),
            other => Err(E::custom(format!(
                r#"Expected value: "true", got: {other}"#
            ))),
        }
    }
}

/// Used to convert "true" to boolean true and everything else is failed.
/// This is necessary because the default deserializer for bool in serde is not
/// able to handle the value "true", which we have previously written to config.
pub fn deserialize_string_as_true<'de, D>(deserializer: D) -> Result<bool, D::Error>
where
    D: serde::Deserializer<'de>,
{
    deserializer.deserialize_str(TrueFromStr)
}

/// Used to convert boolean true to "true" and everything else is skipped.
pub fn serialize_bool_as_true<S>(value: &bool, serializer: S) -> Result<S::Ok, S::Error>
where
    S: serde::Serializer,
{
    if *value {
        serializer.serialize_str("true")
    } else {
        // Skip serializing this field
        serializer.serialize_none()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use flatten::{FlattenLimits, GENEROUS_LIMITS, JsonFlattenError};
    use serde::{Deserialize, Serialize};
    use serde_json::json;
    #[derive(Serialize, Deserialize)]
    struct TestBool {
        #[serde(
            default,
            deserialize_with = "deserialize_string_as_true",
            serialize_with = "serialize_bool_as_true",
            skip_serializing_if = "std::ops::Not::not"
        )]
        value: bool,
        other_field: String,
    }

    fn legacy_flatten_json_body(
        body: Value,
        apply_generic_flattening: bool,
        custom_partition: Option<&String>,
        validation_required: bool,
    ) -> Result<Value, anyhow::Error> {
        let mut nested_value = if apply_generic_flattening {
            convert_to_array(flatten::legacy::generic_flattening(&body)?)?
        } else {
            body
        };
        flatten::legacy::flatten(
            &mut nested_value,
            "_",
            custom_partition,
            validation_required,
        )?;
        Ok(nested_value)
    }

    fn legacy_apply_generic_flattening_for_partition(
        element: Value,
        custom_partition: Option<&String>,
    ) -> Result<Vec<Value>, anyhow::Error> {
        let mut result = Vec::new();
        for mut item in flatten::legacy::generic_flattening(&element)? {
            flatten::legacy::flatten(&mut item, "_", custom_partition, true)?;
            result.push(item);
        }
        Ok(result)
    }

    fn generous() -> FlattenBudget {
        FlattenBudget::new(GENEROUS_LIMITS)
    }

    fn ordered<T: Serialize>(result: Result<T, anyhow::Error>) -> Result<String, String> {
        result
            .map(|value| serde_json::to_string(&value).unwrap())
            .map_err(|err| err.to_string())
    }

    fn corpus() -> Vec<Value> {
        let mut values = flatten::shapes::fixtures();
        for arrays in [0, 3, 10] {
            let mut generator = flatten::shapes::Gen::new(arrays + 100);
            for _ in 0..1500 {
                values.push(generator.body(arrays));
            }
        }
        values
    }

    #[test]
    fn flatten_json_body_matches_legacy() {
        let partitions = [None, Some("a"), Some("a_b")].map(|p| p.map(str::to_owned));
        for value in corpus() {
            for apply_generic_flattening in [false, true] {
                for partition in &partitions {
                    for validation_required in [false, true] {
                        assert_eq!(
                            ordered(flatten_json_body_inner(
                                value.clone(),
                                apply_generic_flattening,
                                None,
                                None,
                                partition.as_ref(),
                                validation_required,
                                &mut generous(),
                            )),
                            ordered(legacy_flatten_json_body(
                                value.clone(),
                                apply_generic_flattening,
                                partition.as_ref(),
                                validation_required,
                            )),
                            "input: {value} generic: {apply_generic_flattening} partition: {partition:?}"
                        );
                    }
                }
            }
        }
    }

    #[test]
    fn apply_generic_flattening_for_partition_matches_legacy() {
        let partitions = [None, Some("a"), Some("a_b")].map(|p| p.map(str::to_owned));
        for value in corpus() {
            for partition in &partitions {
                assert_eq!(
                    ordered(apply_generic_flattening_for_partition(
                        value.clone(),
                        None,
                        None,
                        partition.as_ref(),
                        &mut generous(),
                    )),
                    ordered(legacy_apply_generic_flattening_for_partition(
                        value.clone(),
                        partition.as_ref(),
                    )),
                    "input: {value} partition: {partition:?}"
                );
            }
        }
    }

    #[test]
    fn flatten_json_body_wraps_flat_object() {
        let body = json!({"a": {"b": 2}, "c": {}, "d": 1});
        assert_eq!(
            flatten_json_body_inner(body.clone(), true, None, None, None, false, &mut generous())
                .unwrap(),
            json!([{"a_b": 2, "d": 1}])
        );
        assert_eq!(
            flatten_json_body_inner(
                json!({"b": 1, "a": 2}),
                true,
                None,
                None,
                None,
                false,
                &mut generous()
            )
            .unwrap(),
            json!([{"b": 1, "a": 2}])
        );
        // Too deeply nested bodies skip generic flattening and are not wrapped
        assert_eq!(
            flatten_json_body_inner(body, false, None, None, None, false, &mut generous()).unwrap(),
            json!({"a_b": 2, "d": 1})
        );
    }

    #[test]
    fn deserialize_true() {
        let json = r#"{"value": "true", "other_field": "test"}"#;
        let test_bool: TestBool = serde_json::from_str(json).unwrap();
        assert!(test_bool.value);
    }

    #[test]
    fn deserialize_none_as_false() {
        let json = r#"{"other_field": "test"}"#;
        let test_bool: TestBool = serde_json::from_str(json).unwrap();
        assert!(!test_bool.value);
    }

    #[test]
    fn fail_to_deserialize_invalid_value_including_false_or_raw_bool() {
        let json = r#"{"value": "false", "other_field": "test"}"#;
        assert!(serde_json::from_str::<TestBool>(json).is_err());

        let json = r#"{"value": true, "other_field": "test"}"#;
        assert!(serde_json::from_str::<TestBool>(json).is_err());

        let json = r#"{"value": false, "other_field": "test"}"#;
        assert!(serde_json::from_str::<TestBool>(json).is_err());

        let json = r#"{"value": "invalid", "other_field": "test"}"#;
        assert!(serde_json::from_str::<TestBool>(json).is_err());

        let json = r#"{"value": 123}"#;
        assert!(serde_json::from_str::<TestBool>(json).is_err());

        let json = r#"{"value": null}"#;
        assert!(serde_json::from_str::<TestBool>(json).is_err());
    }

    #[test]
    fn serialize_true_value() {
        let test_bool = TestBool {
            value: true,
            other_field: "test".to_string(),
        };
        let json = serde_json::to_string(&test_bool).unwrap();
        assert_eq!(json, r#"{"value":"true","other_field":"test"}"#);
    }

    #[test]
    fn serialize_false_value_skips_field() {
        let test_bool = TestBool {
            value: false,
            other_field: "test".to_string(),
        };
        let json = serde_json::to_string(&test_bool).unwrap();
        assert_eq!(json, r#"{"other_field":"test"}"#);
    }

    #[test]
    fn roundtrip_true() {
        let original = TestBool {
            value: true,
            other_field: "test".to_string(),
        };
        let json = serde_json::to_string(&original).unwrap();
        let deserialized: TestBool = serde_json::from_str(&json).unwrap();
        assert_eq!(deserialized.value, original.value);
        assert_eq!(deserialized.other_field, original.other_field);
    }

    #[test]
    fn roundtrip_false() {
        let original = TestBool {
            value: false,
            other_field: "test".to_string(),
        };
        let json = serde_json::to_string(&original).unwrap();
        let deserialized: TestBool = serde_json::from_str(&json).unwrap();
        assert_eq!(deserialized.value, original.value);
        assert_eq!(deserialized.other_field, original.other_field);
    }

    #[test]
    fn non_object_arr_is_err() {
        let json = json!([1]);

        assert!(
            flatten_json_body(
                json,
                None,
                None,
                None,
                SchemaVersion::V0,
                false,
                &crate::event::format::LogSource::default(),
                &mut generous(),
            )
            .is_err()
        )
    }

    #[test]
    fn arr_obj_with_nested_type() {
        let json = json!([
            {
                "a": 1,
                "b": "hello",
            },
            {
                "a": 1,
                "b": "hello",
            },
            {
                "a": 1,
                "b": "hello",
                "c": [{"a": 1}]
            },
            {
                "a": 1,
                "b": "hello",
                "c": [{"a": 1, "b": 2}]
            },
        ]);
        let flattened_json = flatten_json_body(
            json,
            None,
            None,
            None,
            SchemaVersion::V0,
            false,
            &crate::event::format::LogSource::default(),
            &mut generous(),
        )
        .unwrap();

        assert_eq!(
            json!([
                {
                    "a": 1,
                    "b": "hello",
                },
                {
                    "a": 1,
                    "b": "hello",
                },
                {
                    "a": 1,
                    "b": "hello",
                    "c_a": [1],
                },
                {
                    "a": 1,
                    "b": "hello",
                    "c_a": [1],
                    "c_b": [2],
                },
            ]),
            flattened_json
        );
    }

    #[test]
    fn test_convert_array_to_object_without_time_partition() {
        let json = json!([
            {
                "a": "b",
                "source_time": "2025-08-01T00:00:00.000Z"
            },
            {
                "a": "b",
                "source_time": "2025-08-01T00:01:00.000Z"
            }
        ]);

        let result = convert_array_to_object(
            json.clone(),
            None,
            None,
            None,
            SchemaVersion::V0,
            &crate::event::format::LogSource::default(),
            &mut generous(),
        );

        assert!(result.is_ok());
        let objects = result.unwrap();

        // Should return 1 item containing the whole array as a single batch
        assert_eq!(objects.len(), 1);
        assert_eq!(objects[0], json);
    }

    fn limits(rows_per_event: usize, rows_per_request: usize, bytes: usize) -> FlattenBudget {
        FlattenBudget::new(FlattenLimits {
            max_rows_per_event: rows_per_event,
            max_rows_per_request: rows_per_request,
            max_bytes_per_request: bytes,
        })
    }

    fn limit_error(result: Result<impl Sized, anyhow::Error>) -> JsonFlattenError {
        match result {
            Ok(_) => panic!("expected a flatten limit error"),
            Err(err) => match err.downcast::<JsonFlattenError>() {
                Ok(err) if err.is_limit_exceeded() => err,
                other => panic!("expected a flatten limit error, got {other:?}"),
            },
        }
    }

    #[test]
    fn flatten_json_body_rejects_expansion_over_limits() {
        let body = json!([{"a": [1, 2, 3], "b": [1, 2]}, {"c": [1, 2, 3, 4, 5, 6]}]);
        let rows = flatten_json_body_inner(
            body.clone(),
            true,
            None,
            None,
            None,
            false,
            &mut limits(6, 12, 1 << 20),
        )
        .unwrap();
        assert_eq!(rows.as_array().unwrap().len(), 12);
        assert!(matches!(
            limit_error(flatten_json_body_inner(
                body.clone(),
                true,
                None,
                None,
                None,
                false,
                &mut limits(5, 12, 1 << 20),
            )),
            JsonFlattenError::RowsPerEventLimitExceeded(5)
        ));
        assert!(matches!(
            limit_error(flatten_json_body_inner(
                body.clone(),
                true,
                None,
                None,
                None,
                false,
                &mut limits(6, 11, 1 << 20),
            )),
            JsonFlattenError::RowsPerRequestLimitExceeded(11)
        ));
        // Bodies too deeply nested for generic flattening are not charged
        let mut budget = limits(1, 1, 1);
        flatten_json_body_inner(body, false, None, None, None, false, &mut budget).unwrap();
        assert_eq!(budget.used(), flatten::FlattenCost::default());
    }

    #[test]
    fn noop_fast_path_is_charged() {
        let body = json!([{"a": 1}, {"b": {"c": "x"}}, {}]);
        let bytes = r#"{"a":1}{"b":{"c":"x"}}{}"#.len();
        let mut budget = limits(1, 3, bytes);
        let flattened =
            flatten_json_body_inner(body.clone(), true, None, None, None, false, &mut budget)
                .unwrap();
        assert_eq!(flattened, json!([{"a": 1}, {"b_c": "x"}, {}]));
        assert_eq!(budget.used(), flatten::FlattenCost { rows: 3, bytes });
        assert!(matches!(
            limit_error(flatten_json_body_inner(
                body.clone(),
                true,
                None,
                None,
                None,
                false,
                &mut limits(1, 2, bytes),
            )),
            JsonFlattenError::RowsPerRequestLimitExceeded(2)
        ));
        assert!(matches!(
            limit_error(flatten_json_body_inner(
                body.clone(),
                true,
                None,
                None,
                None,
                false,
                &mut limits(1, 3, bytes - 1),
            )),
            JsonFlattenError::BytesPerRequestLimitExceeded(_)
        ));
        // A single array-free object takes the same fast path
        assert!(matches!(
            limit_error(flatten_json_body_inner(
                json!({"a": "xyz"}),
                true,
                None,
                None,
                None,
                false,
                &mut limits(1, 1, r#"{"a":"xyz"}"#.len() - 1),
            )),
            JsonFlattenError::BytesPerRequestLimitExceeded(_)
        ));
    }

    #[test]
    fn partitioned_elements_share_the_request_budget() {
        let partition = Some("a".to_owned());
        let element = json!({"a": "x", "b": [1, 2, 3]});
        let flat_element = json!({"a": "x", "b": 1});
        // Partitioned processing charges every element of the request to one budget
        let mut budget = limits(3, 8, 1 << 20);
        for element in [&element, &flat_element, &element] {
            apply_generic_flattening_for_partition(
                element.clone(),
                None,
                None,
                partition.as_ref(),
                &mut budget,
            )
            .unwrap();
        }
        assert_eq!(budget.used().rows, 7);
        // The fast path for array-free elements is charged too
        apply_generic_flattening_for_partition(
            flat_element.clone(),
            None,
            None,
            partition.as_ref(),
            &mut budget,
        )
        .unwrap();
        assert!(matches!(
            limit_error(apply_generic_flattening_for_partition(
                flat_element,
                None,
                None,
                partition.as_ref(),
                &mut budget,
            )),
            JsonFlattenError::RowsPerRequestLimitExceeded(8)
        ));
        assert!(matches!(
            limit_error(apply_generic_flattening_for_partition(
                element,
                None,
                None,
                partition.as_ref(),
                &mut limits(2, 8, 1 << 20),
            )),
            JsonFlattenError::RowsPerEventLimitExceeded(2)
        ));
    }

    // 32 fields below a parent named `parent`, in 4 rows when `repeated`
    fn long_parent(parent: &str, repeated: bool) -> Value {
        let fields: serde_json::Map<String, Value> =
            (0..32).map(|i| (format!("f{i}"), json!(i))).collect();
        let mut map = serde_json::Map::new();
        map.insert(parent.to_owned(), Value::Object(fields));
        if repeated {
            map.insert("r".to_owned(), json!([0, 1, 2, 3]));
        }
        Value::Object(map)
    }

    fn flatten_body(body: Value, budget: &mut FlattenBudget) -> Result<Value, anyhow::Error> {
        flatten_json_body_inner(body, true, None, None, None, false, budget)
    }

    #[test]
    fn structural_field_names_are_charged_before_expansion() {
        // The configuration of the HTTP smoke test
        let small = FlattenLimits {
            max_rows_per_event: 4,
            max_rows_per_request: 6,
            max_bytes_per_request: 4096,
        };
        let long = long_parent(&"p".repeat(128), true);
        // The generic rows alone fit
        assert!(
            flatten::generic_flattening_cost(&long, &small)
                .unwrap()
                .bytes
                <= 4096
        );
        let mut budget = FlattenBudget::new(small);
        assert!(matches!(
            limit_error(flatten_body(long.clone(), &mut budget)),
            JsonFlattenError::BytesPerRequestLimitExceeded(4096)
        ));
        let partition = Some("r".to_owned());
        assert!(matches!(
            limit_error(apply_generic_flattening_for_partition(
                long,
                None,
                None,
                partition.as_ref(),
                &mut budget,
            )),
            JsonFlattenError::BytesPerRequestLimitExceeded(4096)
        ));
        assert_eq!(budget.used(), flatten::FlattenCost::default());

        // A short parent name fits, charged for the structurally flattened rows
        let rows = flatten_body(long_parent("p", true), &mut budget).unwrap();
        let rows = rows.as_array().unwrap();
        assert_eq!(rows.len(), 4);
        assert_eq!(rows[0]["p_f31"], json!(31));
        let bytes: usize = rows
            .iter()
            .map(|row| serde_json::to_vec(row).unwrap().len())
            .sum();
        assert_eq!(budget.used(), flatten::FlattenCost { rows: 4, bytes });

        // An array-free event that fits on its own, but not in what is left of the request
        let flat = long_parent(&"q".repeat(100), false);
        flatten_body(flat.clone(), &mut FlattenBudget::new(small)).unwrap();
        assert!(matches!(
            limit_error(flatten_body(flat, &mut budget)),
            JsonFlattenError::BytesPerRequestLimitExceeded(4096)
        ));
        assert_eq!(budget.used(), flatten::FlattenCost { rows: 4, bytes });
    }
}
