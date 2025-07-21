use serde::{
    Deserialize, Serialize,
    de::{Error as _, Visitor},
};

enum InterimValue {
    Null,
    Bool(bool),
    I64(i64),
    I128(i128),
    U64(u64),
    U128(u128),
    F64(f64),
    String(String),
    Array(Vec<InterimValue>),
    Object(Vec<(String, InterimValue)>),
}

impl TryFrom<InterimValue> for serde_json::Value {
    type Error = &'static str;

    fn try_from(value: InterimValue) -> Result<Self, Self::Error> {
        Ok(match value {
            InterimValue::Null => Self::Null,
            InterimValue::Bool(b) => Self::Bool(b),
            InterimValue::I64(i) => Self::Number(i.into()),
            InterimValue::I128(i) => {
                Self::Number(if let Some(num) = serde_json::Number::from_i128(i) {
                    num
                } else {
                    // Number is a i64
                    (i as i64).into()
                })
            }
            InterimValue::U64(u) => Self::Number(u.into()),
            InterimValue::U128(u) => {
                Self::Number(if let Some(num) = serde_json::Number::from_u128(u) {
                    num
                } else {
                    // Number is a u64
                    (u as u64).into()
                })
            }
            InterimValue::F64(f) => {
                Self::Number(serde_json::Number::from_f64(f).ok_or("not a valid JSON number")?)
            }
            InterimValue::String(s) => Self::String(s),
            InterimValue::Array(a) => Self::Array(
                a.into_iter()
                    .map(TryInto::try_into)
                    .collect::<Result<_, _>>()?,
            ),
            InterimValue::Object(o) => {
                let mut map = serde_json::Map::with_capacity(o.len());
                for (k, v) in o {
                    if map.contains_key(&k) {
                        return Err("duplicate key in JSON");
                    }
                    map.insert(k, v.try_into()?);
                }
                Self::Object(map)
            }
        })
    }
}

impl<'de> Deserialize<'de> for InterimValue {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        struct StrictValueVisitor;

        impl<'de> Visitor<'de> for StrictValueVisitor {
            type Value = InterimValue;

            fn expecting(&self, formatter: &mut std::fmt::Formatter) -> std::fmt::Result {
                formatter.write_str("any valid JSON value")
            }

            fn visit_bool<E>(self, v: bool) -> Result<Self::Value, E>
            where
                E: serde::de::Error,
            {
                Ok(InterimValue::Bool(v))
            }

            fn visit_i64<E>(self, v: i64) -> Result<Self::Value, E>
            where
                E: serde::de::Error,
            {
                Ok(InterimValue::I64(v))
            }

            fn visit_i128<E>(self, v: i128) -> Result<Self::Value, E>
            where
                E: serde::de::Error,
            {
                Ok(InterimValue::I128(v))
            }

            fn visit_u64<E>(self, v: u64) -> Result<Self::Value, E>
            where
                E: serde::de::Error,
            {
                Ok(InterimValue::U64(v))
            }

            fn visit_u128<E>(self, v: u128) -> Result<Self::Value, E>
            where
                E: serde::de::Error,
            {
                Ok(InterimValue::U128(v))
            }

            fn visit_f64<E>(self, v: f64) -> Result<Self::Value, E>
            where
                E: serde::de::Error,
            {
                Ok(InterimValue::F64(v))
            }

            fn visit_str<E>(self, v: &str) -> Result<Self::Value, E>
            where
                E: serde::de::Error,
            {
                Ok(InterimValue::String(v.to_owned()))
            }

            fn visit_string<E>(self, v: String) -> Result<Self::Value, E>
            where
                E: serde::de::Error,
            {
                Ok(InterimValue::String(v))
            }

            fn visit_none<E>(self) -> Result<Self::Value, E> {
                Ok(InterimValue::Null)
            }

            fn visit_some<D>(self, deserializer: D) -> Result<Self::Value, D::Error>
            where
                D: serde::Deserializer<'de>,
            {
                Deserialize::deserialize(deserializer)
            }

            fn visit_unit<E>(self) -> Result<Self::Value, E>
            where
                E: serde::de::Error,
            {
                Ok(InterimValue::Null)
            }

            fn visit_seq<A>(self, mut seq: A) -> Result<Self::Value, A::Error>
            where
                A: serde::de::SeqAccess<'de>,
            {
                let mut arr = Vec::new();

                while let Some(v) = seq.next_element::<InterimValue>()? {
                    arr.push(v);
                }

                Ok(InterimValue::Array(arr))
            }

            fn visit_map<A>(self, mut map: A) -> Result<Self::Value, A::Error>
            where
                A: serde::de::MapAccess<'de>,
            {
                let mut obj = Vec::new();

                while let Some((key, value)) = map.next_entry::<String, InterimValue>()? {
                    obj.push((key, value));
                }

                Ok(InterimValue::Object(obj))
            }
        }

        deserializer.deserialize_any(StrictValueVisitor)
    }
}

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
        let interim: InterimValue = Deserialize::deserialize(deserializer)?;
        Ok(Self {
            inner: interim.try_into().map_err(D::Error::custom)?,
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
