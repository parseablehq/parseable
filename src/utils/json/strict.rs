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

use serde::{
    Deserialize, Serialize,
    de::{Error as _, Visitor},
};

#[derive(Debug, Clone, Eq, PartialEq, Hash)]
pub struct StrictValue {
    inner: serde_json::Value,
}

impl StrictValue {
    pub fn into_inner(self) -> serde_json::Value {
        self.inner
    }
}

impl Serialize for StrictValue {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        self.inner.serialize(serializer)
    }
}

impl<'de> Deserialize<'de> for StrictValue {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        deserializer.deserialize_any(StrictValueVisitor)
    }
}

struct StrictValueVisitor;

impl<'de> Visitor<'de> for StrictValueVisitor {
    type Value = StrictValue;

    fn expecting(&self, formatter: &mut std::fmt::Formatter) -> std::fmt::Result {
        formatter.write_str("any valid JSON value")
    }

    fn visit_bool<E>(self, value: bool) -> Result<Self::Value, E> {
        Ok(StrictValue {
            inner: serde_json::Value::Bool(value),
        })
    }

    fn visit_i64<E>(self, value: i64) -> Result<Self::Value, E> {
        Ok(StrictValue {
            inner: serde_json::Value::Number(value.into()),
        })
    }

    fn visit_i128<E>(self, value: i128) -> Result<Self::Value, E> {
        let number = serde_json::Number::from_i128(value).unwrap_or_else(|| (value as i64).into());
        Ok(StrictValue {
            inner: serde_json::Value::Number(number),
        })
    }

    fn visit_u64<E>(self, value: u64) -> Result<Self::Value, E> {
        Ok(StrictValue {
            inner: serde_json::Value::Number(value.into()),
        })
    }

    fn visit_u128<E>(self, value: u128) -> Result<Self::Value, E> {
        let number = serde_json::Number::from_u128(value).unwrap_or_else(|| (value as u64).into());
        Ok(StrictValue {
            inner: serde_json::Value::Number(number),
        })
    }

    fn visit_f64<E>(self, value: f64) -> Result<Self::Value, E>
    where
        E: serde::de::Error,
    {
        let number = serde_json::Number::from_f64(value)
            .ok_or_else(|| E::custom("not a valid JSON number"))?;
        Ok(StrictValue {
            inner: serde_json::Value::Number(number),
        })
    }

    fn visit_str<E>(self, value: &str) -> Result<Self::Value, E> {
        Ok(StrictValue {
            inner: serde_json::Value::String(value.to_owned()),
        })
    }

    fn visit_string<E>(self, value: String) -> Result<Self::Value, E> {
        Ok(StrictValue {
            inner: serde_json::Value::String(value),
        })
    }

    fn visit_none<E>(self) -> Result<Self::Value, E> {
        Ok(StrictValue {
            inner: serde_json::Value::Null,
        })
    }

    fn visit_some<D>(self, deserializer: D) -> Result<Self::Value, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        Deserialize::deserialize(deserializer)
    }

    fn visit_unit<E>(self) -> Result<Self::Value, E> {
        Ok(StrictValue {
            inner: serde_json::Value::Null,
        })
    }

    fn visit_seq<A>(self, mut seq: A) -> Result<Self::Value, A::Error>
    where
        A: serde::de::SeqAccess<'de>,
    {
        let mut values = Vec::with_capacity(seq.size_hint().unwrap_or(0));
        while let Some(value) = seq.next_element::<StrictValue>()? {
            values.push(value.inner);
        }
        Ok(StrictValue {
            inner: serde_json::Value::Array(values),
        })
    }

    fn visit_map<A>(self, mut map: A) -> Result<Self::Value, A::Error>
    where
        A: serde::de::MapAccess<'de>,
    {
        let mut object = serde_json::Map::with_capacity(map.size_hint().unwrap_or(0));
        while let Some(key) = map.next_key::<String>()? {
            if object.contains_key(&key) {
                return Err(A::Error::custom("duplicate key in JSON"));
            }
            let value = map.next_value::<StrictValue>()?;
            object.insert(key, value.inner);
        }
        Ok(StrictValue {
            inner: serde_json::Value::Object(object),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn test_deserialize_string() {
        let raw_json = r#""hello""#;
        let val: StrictValue = serde_json::from_str(raw_json).unwrap();
        assert_eq!(val.into_inner(), json!("hello"));
    }

    #[test]
    fn test_deserialize_number() {
        let raw_json = "123";
        let val: StrictValue = serde_json::from_str(raw_json).unwrap();
        assert_eq!(val.into_inner(), json!(123));
    }

    #[test]
    fn test_deserialize_object() {
        let raw_json = r#"{"key": "value"}"#;
        let val: StrictValue = serde_json::from_str(raw_json).unwrap();
        assert_eq!(val.into_inner(), json!({"key": "value"}));
    }

    #[test]
    fn test_deserialize_array() {
        let raw_json = r#"[1, 2, 3]"#;
        let val: StrictValue = serde_json::from_str(raw_json).unwrap();
        assert_eq!(val.into_inner(), json!([1, 2, 3]));
    }

    #[test]
    fn test_deserialize_boolean() {
        let raw_json = "true";
        let val: StrictValue = serde_json::from_str(raw_json).unwrap();
        assert_eq!(val.into_inner(), json!(true));
    }

    #[test]
    fn test_deserialize_null() {
        let raw_json = "null";
        let val: StrictValue = serde_json::from_str(raw_json).unwrap();
        assert_eq!(val.into_inner(), json!(null));
    }

    #[test]
    fn test_deserialize_nested_object() {
        let raw_json = r#"{
            "user": {
                "id": 1,
                "info": {
                    "name": "Alice",
                    "emails": ["alice@example.com", "alice@work.com"]
                }
            }
        }"#;
        let val: StrictValue = serde_json::from_str(raw_json).unwrap();
        let expected = json!({
            "user": {
                "id": 1,
                "info": {
                    "name": "Alice",
                    "emails": ["alice@example.com", "alice@work.com"]
                }
            }
        });
        assert_eq!(val.into_inner(), expected);
    }

    #[test]
    fn test_deserialize_nested_array() {
        let raw_json = r#"[[1, 2], [3, 4, [5, 6]], []]"#;
        let val: StrictValue = serde_json::from_str(raw_json).unwrap();
        let expected = json!([[1, 2], [3, 4, [5, 6]], []]);
        assert_eq!(val.into_inner(), expected);
    }

    #[test]
    fn test_deserialize_mixed_structure() {
        let raw_json = r#"{
            "status": "ok",
            "data": [
                {"id": 1, "value": null},
                {"id": 2, "value": [true, false]},
                {"id": 3, "value": {"nested": "yes"}}
            ]
        }"#;
        let val: StrictValue = serde_json::from_str(raw_json).unwrap();
        let expected = json!({
            "status": "ok",
            "data": [
                {"id": 1, "value": null},
                {"id": 2, "value": [true, false]},
                {"id": 3, "value": {"nested": "yes"}}
            ]
        });
        assert_eq!(val.into_inner(), expected);
    }

    #[test]
    fn test_deserialize_deep_nesting() {
        let raw_json = r#"
        {
            "a": {
                "b": {
                    "c": {
                        "d": [1, {"e": "f"}]
                    }
                }
            }
        }"#;
        let val: StrictValue = serde_json::from_str(raw_json).unwrap();
        let expected = json!({
            "a": {
                "b": {
                    "c": {
                        "d": [1, {"e": "f"}]
                    }
                }
            }
        });
        assert_eq!(val.into_inner(), expected);
    }

    #[test]
    fn test_duplicate_keys_should_error() {
        let raw_json = r#"
        {
            "key": "value1",
            "key": "value2"
        }
        "#;

        // Strict version should error
        let custom_result: Result<StrictValue, _> = serde_json::from_str(raw_json);
        assert!(
            custom_result.is_err(),
            "Expected strict Value to error on duplicate keys"
        );

        // serde_json::Value should succeed, keeping the last key
        let serde_result: Result<serde_json::Value, _> = serde_json::from_str(raw_json);
        assert!(
            serde_result.is_ok(),
            "Expected serde_json::Value to allow duplicate keys"
        );

        let deserialized = serde_result.unwrap();
        assert_eq!(deserialized, json!({"key": "value2"}));
    }
}
