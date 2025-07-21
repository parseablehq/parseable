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

use std::fmt;
use std::num::NonZeroU32;

use flatten::{convert_to_array, generic_flattening, has_more_than_max_allowed_levels};
use serde::de::Visitor;
use serde_json;
use serde_json::Value;

use crate::event::format::LogSource;
use crate::metadata::SchemaVersion;

pub mod flatten;
pub mod strict;

/// calls the function `flatten_json` which results Vec<Value> or Error
/// in case when Vec<Value> is returned, converts the Vec<Value> to Value of Array
/// this is to ensure recursive flattening does not happen for heavily nested jsons
pub fn flatten_json_body(
    body: Value,
    time_partition: Option<&String>,
    time_partition_limit: Option<NonZeroU32>,
    custom_partition: Option<&String>,
    schema_version: SchemaVersion,
    validation_required: bool,
    log_source: &LogSource,
) -> Result<Value, anyhow::Error> {
    // Flatten the json body only if new schema and has less than 4 levels of nesting
    let mut nested_value = if schema_version == SchemaVersion::V1
        && !has_more_than_max_allowed_levels(&body, 1)
        && matches!(log_source, LogSource::Json | LogSource::Custom(_))
    {
        let flattened_json = generic_flattening(&body)?;
        convert_to_array(flattened_json)?
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
    )?;
    Ok(nested_value)
}

pub fn convert_array_to_object(
    body: Value,
    time_partition: Option<&String>,
    time_partition_limit: Option<NonZeroU32>,
    custom_partition: Option<&String>,
    schema_version: SchemaVersion,
    log_source: &LogSource,
) -> Result<Vec<Value>, anyhow::Error> {
    let data = flatten_json_body(
        body,
        time_partition,
        time_partition_limit,
        custom_partition,
        schema_version,
        true,
        log_source,
    )?;
    let value_arr = match data {
        Value::Array(arr) => arr,
        value @ Value::Object(_) => vec![value],
        _ => unreachable!("flatten would have failed beforehand"),
    };
    Ok(value_arr)
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
                &crate::event::format::LogSource::default()
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
}
