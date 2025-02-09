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

use serde::de::Visitor;

pub mod flatten;

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
                r#"Expected value: "true", got: {}"#,
                other
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
    use crate::event::format::{json::Event, LogSource};

    use super::*;
    use serde::{Deserialize, Serialize};
    use serde_json::json;

    #[test]
    fn hierarchical_json_flattening_success() {
        let mut event = Event {
            data: json!({"a":{"b":{"e":["a","b"]}}}),
            source: LogSource::default(),
        };
        let expected = json!([{"a_b_e": "a"}, {"a_b_e": "b"}]);
        event
            .flatten_json_body(None, None, None, crate::metadata::SchemaVersion::V1, false)
            .unwrap();
        assert_eq!(event.data, expected);
    }

    #[test]
    fn hierarchical_json_flattening_failure() {
        let mut event = Event {
            data: json!({"a":{"b":{"c":{"d":{"e":["a","b"]}}}}}),
            source: LogSource::default(),
        };
        let expected = json!({"a_b_c_d_e": ["a","b"]});
        event
            .flatten_json_body(None, None, None, crate::metadata::SchemaVersion::V1, false)
            .unwrap();
        assert_eq!(event.data, expected);
    }

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
}
