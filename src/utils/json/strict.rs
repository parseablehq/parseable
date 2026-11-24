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
    de::{DeserializeSeed, Error as _, Visitor},
};

// A shared deferred error keeps serde_json's syntax errors and their locations
// identical to the former parse-then-convert implementation.
#[derive(Default)]
struct Scratch {
    objects: Vec<Vec<(String, serde_json::Value)>>,
}

struct ValueSeed<'a> {
    error: &'a mut Option<&'static str>,
    scratch: &'a mut Scratch,
}

impl<'de> serde::de::DeserializeSeed<'de> for ValueSeed<'_> {
    type Value = serde_json::Value;

    fn deserialize<D>(self, deserializer: D) -> Result<Self::Value, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        deserializer.deserialize_any(self)
    }
}

impl<'de> Visitor<'de> for ValueSeed<'_> {
    type Value = serde_json::Value;

    fn expecting(&self, formatter: &mut std::fmt::Formatter) -> std::fmt::Result {
        formatter.write_str("any valid JSON value")
    }

    fn visit_bool<E>(self, v: bool) -> Result<Self::Value, E>
    where
        E: serde::de::Error,
    {
        Ok(serde_json::Value::Bool(v))
    }

    fn visit_i64<E>(self, v: i64) -> Result<Self::Value, E>
    where
        E: serde::de::Error,
    {
        Ok(serde_json::Value::Number(v.into()))
    }

    fn visit_i128<E>(self, v: i128) -> Result<Self::Value, E>
    where
        E: serde::de::Error,
    {
        Ok(serde_json::Value::Number(
            serde_json::Number::from_i128(v).unwrap_or_else(|| (v as i64).into()),
        ))
    }

    fn visit_u64<E>(self, v: u64) -> Result<Self::Value, E>
    where
        E: serde::de::Error,
    {
        Ok(serde_json::Value::Number(v.into()))
    }

    fn visit_u128<E>(self, v: u128) -> Result<Self::Value, E>
    where
        E: serde::de::Error,
    {
        Ok(serde_json::Value::Number(
            serde_json::Number::from_u128(v).unwrap_or_else(|| (v as u64).into()),
        ))
    }

    fn visit_f64<E>(self, v: f64) -> Result<Self::Value, E>
    where
        E: serde::de::Error,
    {
        match serde_json::Number::from_f64(v) {
            Some(number) => Ok(serde_json::Value::Number(number)),
            None => {
                self.error.get_or_insert("not a valid JSON number");
                Ok(serde_json::Value::Null)
            }
        }
    }

    fn visit_str<E>(self, v: &str) -> Result<Self::Value, E>
    where
        E: serde::de::Error,
    {
        Ok(serde_json::Value::String(v.to_owned()))
    }

    fn visit_string<E>(self, v: String) -> Result<Self::Value, E>
    where
        E: serde::de::Error,
    {
        Ok(serde_json::Value::String(v))
    }

    fn visit_none<E>(self) -> Result<Self::Value, E> {
        Ok(serde_json::Value::Null)
    }

    fn visit_some<D>(self, deserializer: D) -> Result<Self::Value, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        self.deserialize(deserializer)
    }

    fn visit_unit<E>(self) -> Result<Self::Value, E>
    where
        E: serde::de::Error,
    {
        Ok(serde_json::Value::Null)
    }

    fn visit_seq<A>(self, mut seq: A) -> Result<Self::Value, A::Error>
    where
        A: serde::de::SeqAccess<'de>,
    {
        let mut arr = Vec::new();
        while let Some(v) = seq.next_element_seed(ValueSeed {
            error: self.error,
            scratch: self.scratch,
        })? {
            arr.push(v);
        }
        arr.shrink_to_fit();
        Ok(serde_json::Value::Array(arr))
    }

    fn visit_map<A>(self, mut map: A) -> Result<Self::Value, A::Error>
    where
        A: serde::de::MapAccess<'de>,
    {
        // Most log objects fit on the stack. Larger objects reuse a spill
        // buffer, keeping heap scratch bounded by concurrent nesting.
        let mut inline = [const { None }; 16];
        let mut len = 0;
        let mut entries = self.scratch.objects.pop().unwrap_or_default();
        let had_error = self.error.is_some();
        // The first entry whose value recorded a deferred error
        let mut first_value_error = None;
        while let Some(key) = map.next_key::<String>()? {
            let value = map.next_value_seed(ValueSeed {
                error: self.error,
                scratch: self.scratch,
            })?;
            if !had_error && first_value_error.is_none() && self.error.is_some() {
                first_value_error = Some(len);
            }
            if len < inline.len() {
                inline[len] = Some((key, value));
            } else {
                entries.push((key, value));
            }
            len += 1;
        }
        let mut obj = serde_json::Map::with_capacity(len);
        for (index, (key, value)) in inline
            .iter_mut()
            .filter_map(Option::take)
            .chain(entries.drain(..))
            .enumerate()
        {
            match obj.entry(key) {
                serde_json::map::Entry::Occupied(_) => {
                    // Finish parsing before exposing this error: syntax wins.
                    // The former converter went through an object entry by entry,
                    // checking the key before converting the value. Keep its
                    // choice of error: a duplicate at or before the first entry
                    // with a value error is reported instead of that error. An
                    // error from before this object stays.
                    if !had_error && first_value_error.is_none_or(|first| index <= first) {
                        *self.error = Some("duplicate key in JSON");
                    }
                }
                serde_json::map::Entry::Vacant(entry) => {
                    entry.insert(value);
                }
            }
        }
        if entries.capacity() != 0 {
            self.scratch.objects.push(entries);
        }
        Ok(serde_json::Value::Object(obj))
    }
}

#[cfg(test)]
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

#[cfg(test)]
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
                    match map.entry(k) {
                        serde_json::map::Entry::Occupied(_) => return Err("duplicate key in JSON"),
                        serde_json::map::Entry::Vacant(entry) => {
                            entry.insert(v.try_into()?);
                        }
                    }
                }
                Self::Object(map)
            }
        })
    }
}

#[cfg(test)]
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
        let mut error = None;
        let inner = ValueSeed {
            error: &mut error,
            scratch: &mut Scratch::default(),
        }
        .deserialize(deserializer)?;
        if let Some(error) = error {
            return Err(D::Error::custom(error));
        }
        Ok(Self { inner })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    // serde_json::Map hides IndexMap's capacity. Compare retained allocation
    // bytes with the oracle as well as inspecting Vec capacities directly.
    use crate::test_alloc as allocations;

    #[test]
    fn final_container_capacities_match_oracle() {
        fn tight_arrays(value: &serde_json::Value) {
            match value {
                serde_json::Value::Array(a) => {
                    assert_eq!(a.capacity(), a.len());
                    a.iter().for_each(tight_arrays);
                }
                serde_json::Value::Object(m) => m.values().for_each(tight_arrays),
                _ => {}
            }
        }
        for width in [0, 1, 3, 22, 32, 33, 120] {
            let flat = format!(
                "{{{}}}",
                (0..width)
                    .map(|i| format!("\"k{i}\":{i}"))
                    .collect::<Vec<_>>()
                    .join(",")
            );
            let (_, new_bytes) = allocations::retained(|| {
                serde_json::from_str::<StrictValue>(&flat)
                    .unwrap()
                    .into_inner()
            });
            let (_, old_bytes) =
                allocations::retained(|| serde_json::from_str::<Oracle>(&flat).unwrap().0);
            assert_eq!(new_bytes, old_bytes, "map width {width}");
            let object = (0..width)
                .map(|i| format!("\"k{i}\":[1,2,3]"))
                .collect::<Vec<_>>()
                .join(",");
            let raw = format!("[{{{object}}},{{\"nested\":[{{{object}}},[],[1,2,3,4,5]]}}]");
            let (new, new_bytes) = allocations::retained(|| {
                serde_json::from_str::<StrictValue>(&raw)
                    .unwrap()
                    .into_inner()
            });
            let (old, old_bytes) =
                allocations::retained(|| serde_json::from_str::<Oracle>(&raw).unwrap().0);
            assert_eq!(new, old);
            tight_arrays(&new);
            assert!(
                new_bytes <= old_bytes,
                "width {width}: {new_bytes} > {old_bytes}"
            );
        }
    }

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

    #[test]
    fn duplicate_detection_covers_escaped_keys_and_nested_objects() {
        for raw in [
            r#"{"x":1,"\u0078":2}"#,
            r#"[{"outer":{"x":1,"x":2}}]"#,
            r#"{"x":null,"x":{"nested":true}}"#,
        ] {
            let error = serde_json::from_str::<StrictValue>(raw).err().unwrap();
            assert!(error.to_string().contains("duplicate key in JSON"));
        }
    }

    #[test]
    fn strict_objects_preserve_values_and_insertion_order() {
        for width in [0, 1, 16, 64, 256] {
            let mut object = serde_json::Map::new();
            for i in (0..width).rev() {
                object.insert(format!("field_{i}"), json!({"v": [i, null, true, "text"]}));
            }
            let raw = serde_json::to_string(&object).unwrap();
            let result = serde_json::from_str::<StrictValue>(&raw)
                .unwrap()
                .into_inner();
            assert_eq!(serde_json::to_string(&result).unwrap(), raw);
        }
    }

    #[derive(Debug)]
    struct Oracle(serde_json::Value);

    impl<'de> Deserialize<'de> for Oracle {
        fn deserialize<D: serde::Deserializer<'de>>(d: D) -> Result<Self, D::Error> {
            let interim = InterimValue::deserialize(d)?;
            Ok(Self(interim.try_into().map_err(D::Error::custom)?))
        }
    }

    fn corpus() -> Vec<Vec<u8>> {
        let mut cases: Vec<Vec<u8>> = [
            r#""hello""#, "123", "true", "false", "null", "{}", "[]",
            r#"{"key":"value"}"#, "[1,2,3]", "[[1,2],[3,4,[5,6]],[]]",
            r#"{"user":{"id":1,"info":{"name":"Alice","emails":["a","b"]}}}"#,
            r#"{"status":"ok","data":[{"id":1,"value":null},{"id":2,"value":[true,false]},{"id":3,"value":{"nested":"yes"}}]}"#,
            r#"{"a":{"b":{"c":{"d":[1,{"e":"f"}]}}}}"#,
            r#"{"x":1,"x":2}"#, r#"{"x":1,"\u0078":2}"#,
            r#"[{"outer":{"x":1,"x":2}}]"#, r#"{"x":null,"x":{"nested":true}}"#,
            r#"{"x":1,"x":2,"y":3,"y":4}"#,
            r#"{"x":1,"x":2,"later":[}"#, r#"{"bad":],"x":1,"x":2}"#,
            r#"{"x":1,"x":2} trailing"#, "true false", "{}[]", "", "[", "{",
            r#""\u0000""#, r#""\uD834\uDD1E""#, r#""\uD800""#, r#""\uDC00""#,
            r#""\x""#, r#""\uZZZZ""#, r#"{"z":1,"a":2,"m":3}"#,
            "-9223372036854775809", "-9223372036854775808", "9223372036854775807",
            "9223372036854775808", "18446744073709551615", "18446744073709551616",
            "340282366920938463463374607431768211455", "-0", "-0.0", "0", "0.0",
            "1e400", "1e-400", "1e999999999999999999", "-1e400", "1e+", "01", "1.",
            "1.23456789012345678901234567890123456789", "2.2250738585072014e-308",
            "1.7976931348623157e308", "1.7976931348623159e308",
        ].into_iter().map(|s| s.as_bytes().to_vec()).collect();
        cases.push(vec![b'"', 0xff, b'"']);
        cases.push(b"\"raw\x00control\"".to_vec());
        for depth in [126, 127, 128, 129, 256] {
            cases.push(format!("{}0{}", "[".repeat(depth), "]".repeat(depth)).into_bytes());
            cases.push(format!("{}0{}", "{\"a\":".repeat(depth), "}".repeat(depth)).into_bytes());
        }
        // Fixed-seed generated batches, with varying widths, order, numbers and nesting.
        let mut seed = 7_u64;
        for batch in 0..40 {
            let mut rows = Vec::new();
            for row in 0..(batch * 7 + 1) {
                let mut obj = serde_json::Map::new();
                for field in (0..(batch % 20 + 1)).rev() {
                    seed = seed.wrapping_mul(6364136223846793005).wrapping_add(1);
                    obj.insert(
                        format!("field_{field}"),
                        json!([seed, row, -0.0, {"s":"a\0𝄞"}]),
                    );
                }
                rows.push(obj);
            }
            cases.push(serde_json::to_vec(&rows).unwrap());
        }
        // Every truncation of a mixed document, including a deferred duplicate.
        for raw in [
            r#"{"a":[true,null,"\uD834\uDD1E",-2.34e+56],"b":{}}"#,
            r#"{"a":1,"a":2,"b":["later",{"c":false}]}"#,
        ] {
            for n in 0..raw.len() {
                cases.push(raw.as_bytes()[..n].to_vec());
            }
        }
        cases
    }

    fn fingerprint(value: serde_json::Value) -> String {
        // Debug exposes Number's integer/float representation, serialization exposes order.
        format!("{value:?}|{}", serde_json::to_string(&value).unwrap())
    }

    #[actix_web::test]
    async fn differential_corpus() {
        use actix_web::{
            FromRequest,
            test::TestRequest,
            web::{Json, JsonConfig},
        };
        for raw in corpus() {
            let old = serde_json::from_slice::<Oracle>(&raw)
                .map(|v| fingerprint(v.0))
                .map_err(|e| e.to_string());
            let new = serde_json::from_slice::<StrictValue>(&raw)
                .map(|v| fingerprint(v.inner))
                .map_err(|e| e.to_string());
            assert_eq!(new, old, "slice: {:?}", String::from_utf8_lossy(&raw));
            if let Ok(raw) = std::str::from_utf8(&raw) {
                let old = serde_json::from_str::<Oracle>(raw)
                    .map(|v| fingerprint(v.0))
                    .map_err(|e| e.to_string());
                let new = serde_json::from_str::<StrictValue>(raw)
                    .map(|v| fingerprint(v.inner))
                    .map_err(|e| e.to_string());
                assert_eq!(new, old, "str: {raw}");
            }
            let (req, mut payload) = TestRequest::post()
                .insert_header(("content-type", "application/json"))
                .app_data(JsonConfig::default().limit(16 * 1024 * 1024))
                .set_payload(raw.clone())
                .to_http_parts();
            let old = Json::<Oracle>::from_request(&req, &mut payload)
                .await
                .map(|v| fingerprint(v.0.0))
                .map_err(|e| e.to_string());
            let (req, mut payload) = TestRequest::post()
                .insert_header(("content-type", "application/json"))
                .app_data(JsonConfig::default().limit(16 * 1024 * 1024))
                .set_payload(raw)
                .to_http_parts();
            let new = Json::<StrictValue>::from_request(&req, &mut payload)
                .await
                .map(|v| fingerprint(v.into_inner().inner))
                .map_err(|e| e.to_string());
            assert_eq!(new, old, "extractor");
        }
    }
    #[test]
    fn non_json_numeric_visitors_match_oracle() {
        use serde::de::value::{Error, F64Deserializer, I128Deserializer, U128Deserializer};
        macro_rules! compare {
            ($deserializer:expr) => {
                assert_eq!(
                    StrictValue::deserialize($deserializer)
                        .map(|v| fingerprint(v.inner))
                        .map_err(|e| e.to_string()),
                    Oracle::deserialize($deserializer)
                        .map(|v| fingerprint(v.0))
                        .map_err(|e| e.to_string()),
                );
            };
        }
        for v in [i128::MIN, i64::MIN as i128, 0, i64::MAX as i128, i128::MAX] {
            compare!(I128Deserializer::<Error>::new(v));
        }
        for v in [0, u64::MAX as u128, u64::MAX as u128 + 1, u128::MAX] {
            compare!(U128Deserializer::<Error>::new(v));
        }
        for v in [
            -0.0,
            f64::MIN,
            f64::MAX,
            f64::NAN,
            f64::INFINITY,
            f64::NEG_INFINITY,
        ] {
            compare!(F64Deserializer::<Error>::new(v));
        }
    }

    #[test]
    fn mixed_deferred_errors_match_oracle() {
        use serde::de::value::{Error, MapDeserializer};
        let nan = f64::NAN;
        let cases: [(&[(&str, f64)], &str); 6] = [
            (
                &[("a", 1.0), ("a", 2.0), ("b", nan)],
                "duplicate key in JSON",
            ),
            (&[("a", 1.0), ("a", nan)], "duplicate key in JSON"),
            (&[("a", nan), ("a", 1.0)], "not a valid JSON number"),
            (
                &[("a", nan), ("b", 1.0), ("b", 2.0)],
                "not a valid JSON number",
            ),
            (
                &[("a", 1.0), ("b", nan), ("a", 2.0)],
                "not a valid JSON number",
            ),
            (
                &[("a", 1.0), ("b", nan), ("b", 2.0)],
                "not a valid JSON number",
            ),
        ];
        for (entries, expected) in cases {
            let input = || MapDeserializer::<_, Error>::new(entries.iter().copied());
            let new = StrictValue::deserialize(input())
                .map(|v| fingerprint(v.inner))
                .map_err(|e| e.to_string());
            let old = Oracle::deserialize(input())
                .map(|v| fingerprint(v.0))
                .map_err(|e| e.to_string());
            assert_eq!(new, old, "{entries:?}");
            assert_eq!(new.unwrap_err(), expected, "{entries:?}");
        }
    }
}
