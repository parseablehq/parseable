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

use chrono::{DateTime, Datelike, Timelike};
use opentelemetry_proto::tonic::common::v1::{
    AnyValue, ArrayValue, KeyValue, KeyValueList, any_value::Value as OtelValue,
};
use serde::de::DeserializeOwned;
use serde_json::{Map, Value};

/// Deserializes an OTLP/JSON payload and optionally retries after restoring
/// omitted empty `values` fields in `ArrayValue` and `KeyValueList` messages.
pub fn deserialize_otel_json<T>(
    mut json: Value,
    normalise_on_failure: bool,
) -> Result<T, serde_json::Error>
where
    T: DeserializeOwned,
{
    if !normalise_on_failure {
        return serde_json::from_value(json);
    }

    match T::deserialize(&json) {
        Ok(value) => Ok(value),
        Err(original_error) => {
            if !normalise_missing_otel_collection_values(&mut json) {
                return Err(original_error);
            }

            serde_json::from_value(json)
        }
    }
}

fn normalise_missing_otel_collection_values(value: &mut Value) -> bool {
    match value {
        Value::Object(map) => {
            let mut changed = false;

            for collection_key in ["arrayValue", "kvlistValue"] {
                if let Some(Value::Object(collection)) = map.get_mut(collection_key)
                    && !collection.contains_key("values")
                {
                    collection.insert("values".to_string(), Value::Array(Vec::new()));
                    changed = true;
                }
            }

            for child in map.values_mut() {
                changed |= normalise_missing_otel_collection_values(child);
            }

            changed
        }
        Value::Array(values) => {
            let mut changed = false;
            for child in values {
                changed |= normalise_missing_otel_collection_values(child);
            }
            changed
        }
        _ => false,
    }
}

// Value can be one of types - String, Bool, Int, Double, ArrayValue, AnyValue, KeyValueList, Byte
pub fn collect_json_from_value(key: &str, value: OtelValue) -> Map<String, Value> {
    let mut value_json: Map<String, Value> = Map::new();
    insert_json_from_value(&mut value_json, key, value);
    value_json
}

/// Inserts `value` under `key` (or, for a key-value list, its own fields) into
/// `map`, taking the value by reference: the scalars and nested values are copied
/// once into their JSON form, instead of the whole OTEL value being cloned first
/// and then converted.
fn insert_json_from_ref(map: &mut Map<String, Value>, key: &str, value: &OtelValue) {
    match value {
        OtelValue::StringValue(str_val) => {
            map.insert(key.to_string(), Value::String(str_val.clone()));
        }
        OtelValue::BoolValue(bool_val) => {
            map.insert(key.to_string(), Value::Bool(*bool_val));
        }
        OtelValue::IntValue(int_val) => {
            map.insert(key.to_string(), Value::String(int_val.to_string()));
        }
        OtelValue::DoubleValue(double_val) => insert_json_double(map, key, *double_val),
        OtelValue::ArrayValue(array_val) => insert_json_array(map, key, array_val),
        OtelValue::KvlistValue(kv_list_val) => {
            // Create a JSON object to store the key-value list
            let kv_object = collect_json_from_key_value_list(kv_list_val);
            map.extend(kv_object);
        }
        OtelValue::BytesValue(bytes_val) => {
            map.insert(
                key.to_string(),
                Value::String(String::from_utf8_lossy(bytes_val).to_string()),
            );
        }
        OtelValue::StringValueStrindex(str_index) => {
            tracing::warn!(
                "StringValueStrindex is not supported in key-value lists, index: {str_index}"
            );
        }
    }
}

fn insert_json_double(map: &mut Map<String, Value>, key: &str, double_val: f64) {
    if let Some(number) = serde_json::Number::from_f64(double_val) {
        map.insert(key.to_string(), Value::Number(number));
    }
}

fn insert_json_array(map: &mut Map<String, Value>, key: &str, array_val: &ArrayValue) {
    let json_array_value = collect_json_from_array_value(array_val);
    // Convert the array to a JSON string
    let json_array_string = match serde_json::to_string(&json_array_value) {
        Ok(s) => s,
        Err(e) => {
            tracing::warn!("failed to serialise array value: {e}");
            String::default()
        }
    };
    // Insert the array into the result map
    map.insert(key.to_string(), Value::String(json_array_string));
}

fn insert_json_from_value(map: &mut Map<String, Value>, key: &str, value: OtelValue) {
    match value {
        OtelValue::StringValue(str_val) => {
            map.insert(key.to_string(), Value::String(str_val));
        }
        OtelValue::BoolValue(bool_val) => {
            map.insert(key.to_string(), Value::Bool(bool_val));
        }
        OtelValue::IntValue(int_val) => {
            map.insert(key.to_string(), Value::String(int_val.to_string()));
        }
        OtelValue::DoubleValue(double_val) => insert_json_double(map, key, double_val),
        OtelValue::ArrayValue(array_val) => insert_json_array(map, key, &array_val),
        OtelValue::KvlistValue(kv_list_val) => {
            // Create a JSON object to store the key-value list
            let kv_object = collect_json_from_key_value_list(&kv_list_val);
            map.extend(kv_object);
        }
        OtelValue::BytesValue(bytes_val) => {
            map.insert(
                key.to_string(),
                Value::String(String::from_utf8_lossy(&bytes_val).to_string()),
            );
        }
        OtelValue::StringValueStrindex(str_index) => {
            tracing::warn!(
                "StringValueStrindex is not supported in key-value lists, index: {str_index}"
            );
        }
    }
}

/// Recursively converts an ArrayValue into a JSON Value
/// This handles nested array values and key-value lists by recursively
/// converting them to JSON
fn collect_json_from_array_value(array_value: &ArrayValue) -> Value {
    // Sized by the values that will be pushed, as `attribute_capacity` does for maps
    let mut json_array = Vec::with_capacity(
        array_value
            .values
            .iter()
            .filter(|value| value.value.is_some())
            .take(ATTRIBUTE_CAPACITY_HINT_MAX)
            .count(),
    );
    for value in &array_value.values {
        if let Some(val) = &value.value {
            match val {
                OtelValue::StringValue(s) => json_array.push(Value::String(s.clone())),
                OtelValue::BoolValue(b) => json_array.push(Value::Bool(*b)),
                OtelValue::IntValue(i) => {
                    json_array.push(Value::Number(serde_json::Number::from(*i)))
                }
                OtelValue::DoubleValue(d) => {
                    if let Some(n) = serde_json::Number::from_f64(*d) {
                        json_array.push(Value::Number(n));
                    }
                }
                OtelValue::BytesValue(b) => {
                    json_array.push(Value::String(String::from_utf8_lossy(b).to_string()));
                }
                OtelValue::ArrayValue(arr) => {
                    // Recursively collect JSON from nested array values
                    let nested_json = collect_json_from_array_value(arr);
                    json_array.push(nested_json);
                }
                OtelValue::KvlistValue(kv_list) => {
                    // Recursively collect JSON from nested key-value lists
                    let nested_json = collect_json_from_key_value_list(kv_list);
                    json_array.push(Value::Object(nested_json));
                }
                OtelValue::StringValueStrindex(str_index) => {
                    tracing::warn!(
                        "StringValueStrindex is not supported in array values, index: {str_index}"
                    );
                }
            }
        }
    }
    Value::Array(json_array)
}

/// Recursively converts an OpenTelemetry KeyValueList into a JSON Map
/// The function iterates through the key-value pairs in the list
/// and collects their JSON representations into a single Map
fn collect_json_from_key_value_list(key_value_list: &KeyValueList) -> Map<String, Value> {
    let mut kv_list_json: Map<String, Value> =
        Map::with_capacity(attribute_capacity(&key_value_list.values));
    for key_value in &key_value_list.values {
        if let Some(val) = &key_value.value {
            if let Some(val) = &val.value {
                insert_json_from_ref(&mut kv_list_json, &key_value.key, val);
            } else {
                tracing::warn!("Key '{}' has no value in key-value list", key_value.key);
            }
        }
    }
    kv_list_json
}

pub fn collect_json_from_anyvalue(key: &str, value: AnyValue) -> Map<String, Value> {
    collect_json_from_value(key, value.value.unwrap())
}

// traverse through Value by calling function ollect_json_from_any_value
pub fn collect_json_from_values(values: &Option<AnyValue>, key: &str) -> Map<String, Value> {
    let mut value_json: Map<String, Value> = Map::with_capacity(1);

    for value in values.iter() {
        if let Some(value) = value.value.as_ref() {
            insert_json_from_ref(&mut value_json, key, value);
        }
    }

    value_json
}

pub fn value_to_string(value: serde_json::Value) -> String {
    match value {
        e @ Value::Number(_) | e @ Value::Bool(_) => e.to_string(),
        Value::String(s) => s,
        _ => "".to_string(),
    }
}

pub fn flatten_attributes(attributes: &[KeyValue]) -> Map<String, Value> {
    let mut attributes_json: Map<String, Value> =
        Map::with_capacity(attribute_capacity(attributes));
    for attribute in attributes {
        if let Some(value) = attribute.value.as_ref()
            && let Some(value) = value.value.as_ref()
        {
            insert_json_from_ref(&mut attributes_json, &attribute.key, value);
        }
    }
    attributes_json
}

pub fn insert_if_some<T: ToString>(map: &mut Map<String, Value>, key: &str, option: &Option<T>) {
    if let Some(value) = option {
        map.insert(key.to_string(), Value::String(value.to_string()));
    }
}

pub fn insert_number_if_some(map: &mut Map<String, Value>, key: &str, option: &Option<f64>) {
    if let Some(value) = option
        && let Some(number) = serde_json::Number::from_f64(*value)
    {
        map.insert(key.to_string(), Value::Number(number));
    }
}

pub fn insert_bool_if_some(map: &mut Map<String, Value>, key: &str, option: &Option<bool>) {
    if let Some(value) = option {
        map.insert(key.to_string(), Value::Bool(*value));
    }
}

/// Largest capacity hint taken from the attribute count of one message
const ATTRIBUTE_CAPACITY_HINT_MAX: usize = 128;

/// Capacity hint for a map that `insert_attributes` fills from `attributes`.
///
/// The count comes from the request, and resource, scope and span maps sized by
/// it are cloned into every row they belong to, capacity included. So only the
/// attributes that will be inserted are counted (one without a value is two
/// bytes on the wire), and the hint is capped: a larger map still grows as its
/// entries arrive.
pub fn attribute_capacity(attributes: &[KeyValue]) -> usize {
    attributes
        .iter()
        .filter(|attribute| {
            attribute
                .value
                .as_ref()
                .is_some_and(|value| value.value.is_some())
        })
        .take(ATTRIBUTE_CAPACITY_HINT_MAX)
        .count()
}

/// Inserts `attributes` into `map` like `insert_attributes`, for a map created
/// with room for `capacity` entries, `attribute_capacity(attributes)` of them for
/// these attributes. When the attributes make fewer entries than that (repeated
/// keys), the map is rebuilt with room for that many fewer.
///
/// Resource, scope, span and metric maps are cloned into rows, or are rows, and
/// `IndexMap::clone` keeps the capacity, while `row_bytes` counts the entries: a
/// scope of 128 attributes with one key made every row of it hold about 10 times
/// what was counted. Attributes with distinct keys rebuild nothing.
pub fn insert_attributes_fitted(
    map: &mut Map<String, Value>,
    attributes: &[KeyValue],
    capacity: usize,
) {
    let counted = attribute_capacity(attributes);
    let before = map.len();
    insert_attributes(map, attributes);
    fit_map(map, capacity, counted, map.len().saturating_sub(before));
}

/// Rebuilds `map`, created with room for `capacity` entries of which `counted`
/// were for entries that only `added` turned out to make (repeated keys, or keys
/// its own fields already had), with room for that many fewer. Does nothing when
/// `added` is at least `counted`, so attributes with distinct keys rebuild nothing.
pub fn fit_map(map: &mut Map<String, Value>, capacity: usize, counted: usize, added: usize) {
    if added < counted {
        let mut fitted = Map::with_capacity(capacity.saturating_sub(counted - added));
        fitted.extend(std::mem::take(map));
        *map = fitted;
    }
}

pub fn insert_attributes(map: &mut Map<String, Value>, attributes: &[KeyValue]) {
    for attribute in attributes {
        if let Some(value) = attribute.value.as_ref()
            && let Some(value) = value.value.as_ref()
        {
            insert_json_from_ref(map, &attribute.key, value);
        }
    }
}

pub fn convert_epoch_nano_to_timestamp(epoch_ns: i64) -> String {
    if epoch_ns == 0 {
        return chrono::Utc::now().format(TIMESTAMP_FORMAT).to_string();
    }
    let timestamp = DateTime::from_timestamp_nanos(epoch_ns).naive_utc();
    // The strftime machinery costs about 200 ns per call; writing the same
    // fields of the already-converted timestamp costs a couple of ns. `%Y`
    // needs a sign outside 0..=9999 (which an i64 of nanoseconds cannot reach),
    // so anything outside keeps using the formatter.
    if !(0..=9999).contains(&timestamp.year()) {
        return timestamp.format(TIMESTAMP_FORMAT).to_string();
    }
    format_timestamp(&timestamp)
}

/// `%Y-%m-%dT%H:%M:%S%.9fZ` for a timestamp whose year has four digits
fn format_timestamp(timestamp: &chrono::NaiveDateTime) -> String {
    let mut buf = [0u8; TIMESTAMP_LEN];
    let mut pos = 0;
    let year = timestamp.year() as u32;
    write_two_digits(&mut buf, &mut pos, year / 100);
    write_two_digits(&mut buf, &mut pos, year % 100);
    buf[pos] = b'-';
    pos += 1;
    write_two_digits(&mut buf, &mut pos, timestamp.month());
    buf[pos] = b'-';
    pos += 1;
    write_two_digits(&mut buf, &mut pos, timestamp.day());
    buf[pos] = b'T';
    pos += 1;
    write_two_digits(&mut buf, &mut pos, timestamp.hour());
    buf[pos] = b':';
    pos += 1;
    write_two_digits(&mut buf, &mut pos, timestamp.minute());
    buf[pos] = b':';
    pos += 1;
    write_two_digits(&mut buf, &mut pos, timestamp.second());
    buf[pos] = b'.';
    pos += 1;
    let nanos = timestamp.nanosecond();
    buf[pos] = b'0' + (nanos / 100_000_000) as u8;
    pos += 1;
    write_two_digits(&mut buf, &mut pos, (nanos / 1_000_000) % 100);
    write_two_digits(&mut buf, &mut pos, (nanos / 10_000) % 100);
    write_two_digits(&mut buf, &mut pos, (nanos / 100) % 100);
    write_two_digits(&mut buf, &mut pos, nanos % 100);
    buf[pos] = b'Z';
    pos += 1;
    debug_assert_eq!(pos, TIMESTAMP_LEN);

    String::from_utf8(buf.to_vec()).expect("a timestamp is ASCII")
}

/// `YYYY-MM-DDTHH:MM:SS.nnnnnnnnnZ`
const TIMESTAMP_FORMAT: &str = "%Y-%m-%dT%H:%M:%S%.9fZ";
const TIMESTAMP_LEN: usize = 30;

/// Two-digit decimal pairs `00`..`99`
const DIGIT_PAIRS: [u8; 200] = {
    let mut pairs = [0u8; 200];
    let mut value = 0;
    while value < 100 {
        pairs[value * 2] = b'0' + (value / 10) as u8;
        pairs[value * 2 + 1] = b'0' + (value % 10) as u8;
        value += 1;
    }
    pairs
};

fn write_two_digits(buf: &mut [u8; TIMESTAMP_LEN], pos: &mut usize, value: u32) {
    let pair = value as usize * 2;
    buf[*pos] = DIGIT_PAIRS[pair];
    buf[*pos + 1] = DIGIT_PAIRS[pair + 1];
    *pos += 2;
}

#[cfg(test)]
mod tests {
    use super::*;
    use opentelemetry_proto::tonic::logs::v1::LogsData;

    #[test]
    fn zero_epoch_uses_current_utc_time() {
        let before = chrono::Utc::now();
        let timestamp = convert_epoch_nano_to_timestamp(0);
        let after = chrono::Utc::now();
        let parsed = chrono::DateTime::parse_from_rfc3339(&timestamp)
            .expect("generated timestamp must be RFC 3339")
            .with_timezone(&chrono::Utc);

        assert!(parsed >= before);
        assert!(parsed <= after);
    }

    #[test]
    fn nonzero_epoch_is_preserved() {
        assert_eq!(
            convert_epoch_nano_to_timestamp(1_000_000_000),
            "1970-01-01T00:00:01.000000000Z"
        );
    }

    // A capacity hint that followed the raw attribute count would let a request
    // of valueless attributes (two bytes each) size a scope map that is then
    // cloned, capacity and all, into every row.
    #[test]
    fn attribute_capacity_counts_inserted_attributes_and_is_capped() {
        let valued = |key: &str| KeyValue {
            key: key.to_string(),
            value: Some(AnyValue {
                value: Some(OtelValue::StringValue("v".to_string())),
            }),
            ..Default::default()
        };
        let empty_any = KeyValue {
            key: "k".to_string(),
            value: Some(AnyValue { value: None }),
            ..Default::default()
        };

        assert_eq!(attribute_capacity(&[]), 0);
        assert_eq!(attribute_capacity(&vec![KeyValue::default(); 200_000]), 0);
        assert_eq!(attribute_capacity(&vec![empty_any; 1_000]), 0);
        assert_eq!(
            attribute_capacity(&[valued("a"), KeyValue::default(), valued("b")]),
            2
        );
        assert_eq!(
            attribute_capacity(&vec![valued("a"); 10_000]),
            ATTRIBUTE_CAPACITY_HINT_MAX
        );

        // The hint never changes what is inserted
        let attributes = vec![valued("a"), KeyValue::default(), valued("b")];
        let mut map = Map::with_capacity(attribute_capacity(&attributes));
        insert_attributes(&mut map, &attributes);
        assert_eq!(map.len(), 2);
    }

    /// The direct formatter must produce exactly what the strftime path produces,
    /// for every kind of value an i64 of nanoseconds can hold: the ends of the
    /// range, the whole of one day second by second, hours across the range
    /// around the epoch and deterministic pseudorandom values.
    #[test]
    fn fast_timestamp_matches_strftime() {
        let slow = |ns: i64| {
            chrono::DateTime::from_timestamp_nanos(ns)
                .naive_utc()
                .format("%Y-%m-%dT%H:%M:%S%.9fZ")
                .to_string()
        };
        let mut values = vec![
            i64::MIN,
            i64::MIN + 1,
            i64::MIN + 999,
            i64::MAX,
            i64::MAX - 1,
            i64::MIN / 2,
            i64::MAX / 2,
            1,
            -1,
            999_999_999,
            1_000_000_000,
            -999_999_999,
            -1_000_000_000,
            // leap day, year boundaries, month ends
            951_782_400_000_000_000,
            1_709_164_799_999_999_999,
            1_709_164_800_000_000_000,
            1_735_689_599_999_999_999,
            1_735_689_600_000_000_000,
        ];
        // every second of one day
        for second in 0..86_400 {
            values.push(1_700_000_000_000_000_000 + second * 1_000_000_000);
        }
        // every hour of the first and last thousand hours the range can hold,
        // and of the thousand hours around the epoch
        for hour in 0..1_000 {
            values.push(i64::MIN + hour * 3_600_000_000_000);
            values.push(i64::MAX - hour * 3_600_000_000_000);
            values.push(hour * 3_600_000_000_000);
            values.push(-hour * 3_600_000_000_000);
        }
        // SplitMix64, as the deterministic-simulation conventions use
        let mut state = 0x9e37_79b9_7f4a_7c15u64;
        for _ in 0..20_000 {
            state = state.wrapping_add(0x9e37_79b9_7f4a_7c15);
            let mut z = state;
            z = (z ^ (z >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
            z = (z ^ (z >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
            values.push((z ^ (z >> 31)) as i64);
        }

        for ns in values {
            if ns == 0 {
                continue; // filled from the wall clock
            }
            assert_eq!(
                convert_epoch_nano_to_timestamp(ns),
                slow(ns),
                "timestamp for {ns} ns differs"
            );
        }
    }

    fn logs_with_omitted_empty_collection_values() -> Value {
        serde_json::json!({
            "resourceLogs": [{
                "scopeLogs": [{
                    "logRecords": [{
                        "attributes": [
                            {
                                "key": "input",
                                "value": { "kvlistValue": {} }
                            },
                            {
                                "key": "tags",
                                "value": { "arrayValue": {} }
                            }
                        ]
                    }]
                }]
            }]
        })
    }

    #[test]
    fn otel_payload_normalisation_is_disabled_by_default() {
        let error =
            deserialize_otel_json::<LogsData>(logs_with_omitted_empty_collection_values(), false)
                .unwrap_err();

        assert!(error.to_string().contains("missing field"));
        assert!(error.to_string().contains("values"));
    }

    #[test]
    fn otel_payload_normalisation_retries_failed_deserialization() {
        let logs =
            deserialize_otel_json::<LogsData>(logs_with_omitted_empty_collection_values(), true)
                .unwrap();

        let attributes = &logs.resource_logs[0].scope_logs[0].log_records[0].attributes;
        assert!(matches!(
            attributes[0]
                .value
                .as_ref()
                .and_then(|value| value.value.as_ref()),
            Some(OtelValue::KvlistValue(KeyValueList { values })) if values.is_empty()
        ));
        assert!(matches!(
            attributes[1]
                .value
                .as_ref()
                .and_then(|value| value.value.as_ref()),
            Some(OtelValue::ArrayValue(ArrayValue { values })) if values.is_empty()
        ));
    }

    // Release-profile micro-benchmarks, see `crate::otel::bench`. Run with a filter:
    //   cargo test --release --lib -- --ignored --exact otel::otel_utils::tests::benches::bench_convert_epoch_nano_to_timestamp --nocapture
    mod benches {
        use super::super::{convert_epoch_nano_to_timestamp, insert_attributes};
        use crate::otel::bench;
        use opentelemetry_proto::tonic::common::v1::{AnyValue, KeyValue, any_value};

        fn string_kv(key: &str, value: &str) -> KeyValue {
            KeyValue {
                key: key.to_string(),
                value: Some(AnyValue {
                    value: Some(any_value::Value::StringValue(value.to_string())),
                }),
                ..Default::default()
            }
        }

        fn bench_attributes() -> Vec<KeyValue> {
            vec![
                string_kv("http.method", "GET"),
                KeyValue {
                    key: "http.status_code".to_string(),
                    value: Some(AnyValue {
                        value: Some(any_value::Value::IntValue(200)),
                    }),
                    ..Default::default()
                },
                string_kv("http.target", "/api/v1/items/42"),
                string_kv("net.host.name", "checkout.example.com"),
                string_kv("user_agent.original", "Mozilla/5.0 (compatible; bench)"),
                KeyValue {
                    key: "cache.hit".to_string(),
                    value: Some(AnyValue {
                        value: Some(any_value::Value::BoolValue(true)),
                    }),
                    ..Default::default()
                },
                string_kv("http.request.header.x-request-id", "req-00000000deadbeef"),
                string_kv("messaging.system", "kafka"),
            ]
        }

        #[test]
        #[ignore = "performance benchmark"]
        fn bench_convert_epoch_nano_to_timestamp() {
            let mut len = 0usize;
            // Every call formats a distinct nanosecond, as a real batch does
            bench::time_calls("bench_convert_epoch_nano_to_timestamp", 200_000, |i| {
                len += convert_epoch_nano_to_timestamp(
                    1_700_000_000_000_000_000 + i as i64 * 1_000_000,
                )
                .len();
            });
            assert_eq!(len, 200_000 * 30);
        }

        #[test]
        #[ignore = "performance benchmark"]
        fn bench_insert_attributes() {
            let attributes = bench_attributes();
            let mut sum = 0usize;
            bench::time_calls("bench_insert_attributes", 200_000, |_| {
                let mut map = serde_json::Map::with_capacity(attributes.len());
                insert_attributes(&mut map, &attributes);
                sum += map.len();
            });
            assert_eq!(sum, 200_000 * attributes.len());
        }
    }
}
