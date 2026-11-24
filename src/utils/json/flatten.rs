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

use std::collections::BTreeMap;
use std::num::NonZeroU32;
use std::sync::Mutex;

use chrono::{DateTime, Duration, Utc};
use serde_json::map::Map;
use serde_json::value::Value;

use thiserror::Error;

use crate::cli::{
    DEFAULT_MAX_FLATTEN_BYTES_PER_REQUEST, DEFAULT_MAX_FLATTEN_ROWS_PER_EVENT,
    DEFAULT_MAX_FLATTEN_ROWS_PER_REQUEST, Options,
};
use crate::parseable::PARSEABLE;

// Global variable to track the first timestamp encountered during validation
static REFERENCE_TIMESTAMP: Mutex<Option<DateTime<Utc>>> = Mutex::new(None);

// Tests use a scoped age setting instead of initializing the server CLI singleton.
#[cfg(test)]
thread_local! {
    static TEST_MAX_CHUNK_AGE_HOURS: std::cell::Cell<Option<i64>> = const { std::cell::Cell::new(None) };
}

// Serialize and restore tests that replace the process-wide reference timestamp.
#[cfg(test)]
static TIME_PARTITION_TEST_LOCK: Mutex<()> = Mutex::new(());
#[cfg(test)]
pub(crate) struct TimePartitionTestState {
    _lock: std::sync::MutexGuard<'static, ()>,
    reference: Option<DateTime<Utc>>,
    max_age_hours: Option<i64>,
}

#[cfg(test)]
impl Drop for TimePartitionTestState {
    fn drop(&mut self) {
        *REFERENCE_TIMESTAMP.lock().unwrap() = self.reference;
        TEST_MAX_CHUNK_AGE_HOURS.set(self.max_age_hours);
    }
}

#[cfg(test)]
pub(crate) fn time_partition_test_state(max_age_hours: i64) -> TimePartitionTestState {
    let lock = TIME_PARTITION_TEST_LOCK.lock().unwrap();
    TimePartitionTestState {
        _lock: lock,
        reference: REFERENCE_TIMESTAMP.lock().unwrap().take(),
        max_age_hours: TEST_MAX_CHUNK_AGE_HOURS.replace(Some(max_age_hours)),
    }
}

fn max_chunk_age_hours() -> i64 {
    #[cfg(test)]
    if let Some(hours) = TEST_MAX_CHUNK_AGE_HOURS.get() {
        return hours;
    }
    PARSEABLE.options.event_max_chunk_age as i64
}

#[derive(Error, Debug)]
pub enum JsonFlattenError {
    #[error("Cannot flatten this JSON")]
    CannotFlatten,
    #[error("Ingestion failed as field {0} is not part of the log")]
    FieldNotPartOfLog(String),
    #[error("Ingestion failed as field {0} is empty or 'null'")]
    FieldEmptyOrNull(String),
    #[error("Ingestion failed as field {0} is an object")]
    FieldIsObject(String),
    #[error("Ingestion failed as field {0} is an array")]
    FieldIsArray(String),
    #[error("Ingestion failed as field {0} contains a period in the value")]
    FieldContainsPeriod(String),
    #[error("Ingestion failed as field {0} is not a string")]
    FieldNotString(String),
    #[error("Field {0} is not in the correct datetime format")]
    InvalidDatetimeFormat(String),
    #[error("Field {0} value '{2}' is more than {1} days old")]
    TimestampTooOld(String, i64, DateTime<Utc>),
    #[error(
        "Field {0} timestamp '{2}' is more than {1} hours older than reference timestamp '{3}'"
    )]
    TimestampTooOldRelative(String, i64, DateTime<Utc>, DateTime<Utc>),
    #[error("Expected object in array of objects")]
    ExpectedObjectInArray,
    #[error("Found non-object element while flattening array of objects")]
    NonObjectInArray,
    #[error(
        "Flattening a single event would produce more than {0} rows (P_MAX_FLATTEN_ROWS_PER_EVENT). Reduce the number or size of nested arrays in the event, or split it into several events"
    )]
    RowsPerEventLimitExceeded(usize),
    #[error(
        "Flattening the request would produce more than {0} rows (P_MAX_FLATTEN_ROWS_PER_REQUEST). Split the batch into smaller requests, or reduce the number or size of nested arrays in its events"
    )]
    RowsPerRequestLimitExceeded(usize),
    #[error(
        "Flattening the request would produce more than {0} bytes of JSON (P_MAX_FLATTEN_BYTES_PER_REQUEST). Split the batch into smaller requests, or reduce the number or size of nested arrays in its events"
    )]
    BytesPerRequestLimitExceeded(usize),
}

impl JsonFlattenError {
    /// True for the errors of an event or request that generic flattening would expand
    /// beyond the configured `FlattenLimits`
    pub fn is_limit_exceeded(&self) -> bool {
        matches!(
            self,
            Self::RowsPerEventLimitExceeded(_)
                | Self::RowsPerRequestLimitExceeded(_)
                | Self::BytesPerRequestLimitExceeded(_)
        )
    }
}

// Recursively flattens JSON objects and arrays, e.g. with the separator `.`, starting from the TOP
// `{"key": "value", "nested_key": {"key":"value"}}` becomes `{"key": "value", "nested_key.key": "value"}`
#[cfg_attr(feature = "hotpath", hotpath::measure)]
pub fn flatten(
    nested_value: &mut Value,
    separator: &str,
    time_partition: Option<&String>,
    time_partition_limit: Option<NonZeroU32>,
    custom_partition: Option<&String>,
    validation_required: bool,
    budget: &mut FlattenBudget,
) -> Result<(), JsonFlattenError> {
    match nested_value {
        Value::Object(nested_dict) => {
            if validation_required {
                validate_time_partition(nested_dict, time_partition, time_partition_limit)?;
                validate_custom_partition(nested_dict, custom_partition)?;
            }
            // Rebuilding a map without nested objects or arrays of objects re-inserts every
            // key unchanged and in the same order, so skip it.
            if !object_needs_flattening(nested_dict) {
                return Ok(());
            }
            let original = std::mem::take(nested_dict);
            let mut map = Map::with_capacity(original.len());
            flatten_object(&mut map, None, original, separator, budget)?;
            *nested_dict = map;
        }
        Value::Array(arr) => {
            for nested_value in arr {
                // Recursively flatten each element, ONLY in the TOP array
                flatten(
                    nested_value,
                    separator,
                    time_partition,
                    time_partition_limit,
                    custom_partition,
                    validation_required,
                    budget,
                )?;
            }
        }
        _ => return Err(JsonFlattenError::CannotFlatten),
    }

    Ok(())
}

// Validates the presence and content of custom partition fields, that it is
// not null, empty, an object , an array, or contain a `.` when serialized
pub fn validate_custom_partition(
    value: &Map<String, Value>,
    custom_partition: Option<&String>,
) -> Result<(), JsonFlattenError> {
    let Some(custom_partition) = custom_partition else {
        return Ok(());
    };
    let custom_partition_list = custom_partition.split(',').collect::<Vec<&str>>();

    for field in custom_partition_list {
        let trimmed_field = field.trim();
        let Some(field_value) = value.get(trimmed_field) else {
            return Err(JsonFlattenError::FieldNotPartOfLog(
                trimmed_field.to_owned(),
            ));
        };

        // The field should not be null, empty, an object, an array or contain a `.` in the value
        match field_value {
            Value::Null => {
                return Err(JsonFlattenError::FieldEmptyOrNull(trimmed_field.to_owned()));
            }
            Value::String(s) if s.is_empty() => {
                return Err(JsonFlattenError::FieldEmptyOrNull(trimmed_field.to_owned()));
            }
            Value::Object(_) => {
                return Err(JsonFlattenError::FieldIsObject(trimmed_field.to_owned()));
            }
            Value::Array(_) => {
                return Err(JsonFlattenError::FieldIsArray(trimmed_field.to_owned()));
            }
            Value::String(s) if s.contains('.') => {
                return Err(JsonFlattenError::FieldContainsPeriod(
                    trimmed_field.to_owned(),
                ));
            }
            Value::Number(n) if n.is_f64() => {
                return Err(JsonFlattenError::FieldContainsPeriod(
                    trimmed_field.to_owned(),
                ));
            }
            _ => {}
        }
    }

    Ok(())
}

// Validates time partitioning constraints, checking if a timestamp is a string
// that can be parsed as datetime within the configured time limit
pub fn validate_time_partition(
    value: &Map<String, Value>,
    time_partition: Option<&String>,
    time_partition_limit: Option<NonZeroU32>,
) -> Result<(), JsonFlattenError> {
    let Some(partition_key) = time_partition else {
        return Ok(());
    };

    let limit_days = time_partition_limit.map_or(30, |days| days.get() as i64);

    let Some(timestamp_value) = value.get(partition_key) else {
        return Err(JsonFlattenError::FieldNotPartOfLog(
            partition_key.to_owned(),
        ));
    };

    let Value::String(timestamp_str) = timestamp_value else {
        return Err(JsonFlattenError::FieldNotString(partition_key.to_owned()));
    };
    let Ok(parsed_timestamp) = timestamp_str.parse::<DateTime<Utc>>() else {
        return Err(JsonFlattenError::InvalidDatetimeFormat(
            partition_key.to_owned(),
        ));
    };

    // Access the global reference timestamp and handle poisoning
    let mut reference_timestamp = REFERENCE_TIMESTAMP
        .lock()
        .unwrap_or_else(|p| p.into_inner());

    match *reference_timestamp {
        None => {
            // First timestamp encountered - validate against cutoff date
            let cutoff_ts = Utc::now() - Duration::days(limit_days);
            if parsed_timestamp >= cutoff_ts {
                // Set the reference timestamp
                *reference_timestamp = Some(parsed_timestamp);
                Ok(())
            } else {
                Err(JsonFlattenError::TimestampTooOld(
                    partition_key.to_owned(),
                    limit_days,
                    parsed_timestamp,
                ))
            }
        }
        Some(ref_timestamp) => {
            // Subsequent timestamps - validate they're not more than configured hours older than reference
            let max_age_hours = max_chunk_age_hours();
            let max_age_before_ref = ref_timestamp - Duration::hours(max_age_hours);
            if parsed_timestamp >= max_age_before_ref {
                Ok(())
            } else {
                Err(JsonFlattenError::TimestampTooOldRelative(
                    partition_key.to_owned(),
                    max_age_hours,
                    parsed_timestamp,
                    ref_timestamp,
                ))
            }
        }
    }
}

// Returns true when `flatten_object` would change the map, i.e. it has a nested object
// (including an empty one, which is dropped) or an array containing an object
fn object_needs_flattening(map: &Map<String, Value>) -> bool {
    map.values().any(|value| match value {
        Value::Object(_) => true,
        Value::Array(arr) => arr.iter().any(Value::is_object),
        _ => false,
    })
}

// Flattens a nested JSON Object/Map into another target Map
fn flatten_object(
    output_map: &mut Map<String, Value>,
    parent_key: Option<&str>,
    nested_map: Map<String, Value>,
    separator: &str,
    budget: &mut FlattenBudget,
) -> Result<(), JsonFlattenError> {
    for (key, mut value) in nested_map {
        let new_key = match parent_key {
            Some(parent) => format!("{parent}{separator}{key}"),
            None => key,
        };

        match &mut value {
            Value::Object(obj) => {
                let obj = std::mem::take(obj);
                flatten_object(output_map, Some(&new_key), obj, separator, budget)?;
            }
            Value::Array(arr) if arr.iter().any(Value::is_object) => {
                flatten_array_objects(output_map, &new_key, arr, separator, budget)?;
            }
            _ => {
                output_map.insert(new_key, value);
            }
        }
    }
    Ok(())
}

// Flattens a nested JSON Array into the parent Map: one column (array) per distinct key of
// its objects, with a cell for every element, null where an element lacks the key. Every
// null cell is a `Value` of its own, so the cells added are charged to `budget` at that
// size before the columns are built: 3,600 objects with one distinct key each would
// otherwise hold 13 million cells, about 900 MiB.
pub fn flatten_array_objects(
    output_map: &mut Map<String, Value>,
    parent_key: &str,
    arr: &mut [Value],
    separator: &str,
    budget: &mut FlattenBudget,
) -> Result<(), JsonFlattenError> {
    // Each element flattened on its own first, to count the cells of the table
    let mut elements: Vec<Option<Map<String, Value>>> = Vec::with_capacity(arr.len());
    let mut keys: BTreeMap<&str, ()> = BTreeMap::new();
    let mut present = 0usize;
    for value in arr.iter_mut() {
        match value {
            Value::Object(nested_object) => {
                let mut output_map = Map::new();
                let nested_object = std::mem::take(nested_object);
                flatten_object(
                    &mut output_map,
                    Some(parent_key),
                    nested_object,
                    separator,
                    budget,
                )?;
                present += output_map.len();
                elements.push(Some(output_map));
            }
            Value::Null => elements.push(None),
            _ => return Err(JsonFlattenError::NonObjectInArray),
        }
    }
    for element in elements.iter().flatten() {
        keys.extend(element.keys().map(|key| (key.as_str(), ())));
    }
    let nulls = keys
        .len()
        .saturating_mul(elements.len())
        .saturating_sub(present);
    budget.charge_event(FlattenCost {
        rows: 0,
        bytes: nulls.saturating_mul(std::mem::size_of::<Value>()),
    })?;
    drop(keys);

    let mut columns: BTreeMap<String, Vec<Value>> = BTreeMap::new();
    for (index, element) in elements.into_iter().enumerate() {
        match element {
            Some(output_map) => {
                for (key, value) in output_map {
                    let column = columns
                        .entry(key)
                        .or_insert_with(|| vec![Value::Null; index]);
                    column.push(value);
                }
            }
            None => {
                for column in columns.values_mut() {
                    column.push(Value::Null);
                }
            }
        }

        // Ensure all columns are extended with null values if they weren't updated in this iteration
        let max_len = index + 1;
        for column in columns.values_mut() {
            while column.len() < max_len {
                column.push(Value::Null);
            }
        }
    }

    // Update the main map with new keys and their corresponding arrays
    for (key, values) in columns {
        output_map.insert(key, Value::Array(values));
    }

    Ok(())
}

/// Caps on the work generic flattening may do, see `FlattenBudget`
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct FlattenLimits {
    /// Rows a single event may expand into
    pub max_rows_per_event: usize,
    /// Rows all events of a request may expand into together
    pub max_rows_per_request: usize,
    /// Compact JSON bytes of all rows a request may expand into together. This measures the
    /// expanded JSON, not the memory actually used to hold it
    pub max_bytes_per_request: usize,
}

impl FlattenLimits {
    pub const DEFAULT: Self = Self {
        max_rows_per_event: DEFAULT_MAX_FLATTEN_ROWS_PER_EVENT,
        max_rows_per_request: DEFAULT_MAX_FLATTEN_ROWS_PER_REQUEST,
        max_bytes_per_request: DEFAULT_MAX_FLATTEN_BYTES_PER_REQUEST,
    };

    pub fn from_options(options: &Options) -> Self {
        Self {
            max_rows_per_event: options.max_flatten_rows_per_event,
            max_rows_per_request: options.max_flatten_rows_per_request,
            max_bytes_per_request: options.max_flatten_bytes_per_request,
        }
    }

    /// Limits configured for this server
    pub fn from_config() -> Self {
        Self::from_options(&PARSEABLE.options)
    }

    fn check_event_rows(&self, rows: Option<usize>) -> Result<usize, JsonFlattenError> {
        rows.filter(|&rows| rows <= self.max_rows_per_event).ok_or(
            JsonFlattenError::RowsPerEventLimitExceeded(self.max_rows_per_event),
        )
    }

    fn check_bytes(&self, bytes: Option<usize>) -> Result<usize, JsonFlattenError> {
        bytes
            .filter(|&bytes| bytes <= self.max_bytes_per_request)
            .ok_or(JsonFlattenError::BytesPerRequestLimitExceeded(
                self.max_bytes_per_request,
            ))
    }
}

/// Rows generic flattening produces, and the sum of their compact JSON lengths
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct FlattenCost {
    pub rows: usize,
    pub bytes: usize,
}

impl FlattenCost {
    /// Adds up the costs of two parts of one event, failing like `generic_flattening_cost`
    /// once the total is over the limits
    pub fn add(self, other: Self, limits: &FlattenLimits) -> Result<Self, JsonFlattenError> {
        Ok(Self {
            rows: limits.check_event_rows(self.rows.checked_add(other.rows))?,
            bytes: limits.check_bytes(self.bytes.checked_add(other.bytes))?,
        })
    }
}

/// Tracks the generic flattening work of one request (or internally generated payload)
/// against its `FlattenLimits`. Create one per request and pass it to every event of it.
#[derive(Debug, Clone)]
pub struct FlattenBudget {
    limits: FlattenLimits,
    used: FlattenCost,
}

impl FlattenBudget {
    pub fn new(limits: FlattenLimits) -> Self {
        Self {
            limits,
            used: FlattenCost::default(),
        }
    }

    pub fn limits(&self) -> &FlattenLimits {
        &self.limits
    }

    pub fn used(&self) -> FlattenCost {
        self.used
    }

    /// Charges the cost of one event, leaving the budget unchanged when that is over a limit
    pub fn charge_event(&mut self, cost: FlattenCost) -> Result<(), JsonFlattenError> {
        let limits = &self.limits;
        limits.check_event_rows(Some(cost.rows))?;
        let rows = self
            .used
            .rows
            .checked_add(cost.rows)
            .filter(|&rows| rows <= limits.max_rows_per_request)
            .ok_or(JsonFlattenError::RowsPerRequestLimitExceeded(
                limits.max_rows_per_request,
            ))?;
        let bytes = limits.check_bytes(self.used.bytes.checked_add(cost.bytes))?;
        self.used = FlattenCost { rows, bytes };
        Ok(())
    }
}

/// Recursively flattens a JSON value.
/// - If the value is an array, it flattens all elements of the array.
/// - If the value is an object, it flattens all nested objects and arrays.
/// - Otherwise, it returns the value itself in a vector.
///
/// Every element left after splicing nested top level arrays is one event charged to
/// `budget`, see `charge_generic_flattening`. Nothing is built when a limit is exceeded.
/// Callers skip this for values nested deeper than `P_MAX_FLATTEN_LEVEL`. Callers that
/// structurally flatten the rows afterwards use `pipeline_flattening` instead.
///
/// Examples:
/// 1. `{"a": 1}` ~> `[{"a": 1}]`
/// 2. `[{"a": 1}, {"b": 2}]` ~> `[{"a": 1}, {"b": 2}]`
/// 3. `[{"a": [{"b": 1}, {"c": 2}]}]` ~> `[{"a": {"b": 1)}}, {"a": {"c": 2)}}]`
/// 4. `{"a": [{"b": 1}, {"c": 2}], "d": {"e": 4}}` ~> `[{"a": {"b":1}, "d": {"e":4}}, {"a": {"c":2}, "d": {"e":4}}]`
pub fn generic_flattening(
    value: &Value,
    budget: &mut FlattenBudget,
) -> Result<Vec<Value>, JsonFlattenError> {
    flattening::<false>(value, budget)
}

/// `generic_flattening` with a budget of its own
pub fn generic_flattening_with_limits(
    value: &Value,
    limits: FlattenLimits,
) -> Result<Vec<Value>, JsonFlattenError> {
    generic_flattening(value, &mut FlattenBudget::new(limits))
}

/// Charges `budget` for `generic_flattening(value)`, treating every element left after
/// splicing nested top level arrays as one event
pub fn charge_generic_flattening(
    value: &Value,
    budget: &mut FlattenBudget,
) -> Result<(), JsonFlattenError> {
    charge_events::<false>(value, budget)
}

/// Counts the rows `generic_flattening` builds for `value` taken as a single event, and
/// the sum of their compact JSON lengths, without building or cloning anything. Fails as
/// soon as the rows are over `max_rows_per_event` or the bytes over `max_bytes_per_request`,
/// including when counting them would overflow.
pub fn generic_flattening_cost(
    value: &Value,
    limits: &FlattenLimits,
) -> Result<FlattenCost, JsonFlattenError> {
    let expansion = value_cost::<false>(value, false, limits)?;
    Ok(FlattenCost {
        rows: expansion.rows,
        bytes: expansion.bytes,
    })
}

/// Builds the rows of generic flattening without charging any budget. Only call it for a
/// value whose `generic_flattening_cost` (or `flatten_pipeline_cost`) was charged first.
pub(crate) fn expand_generic_flattening(value: &Value) -> Result<Vec<Value>, JsonFlattenError> {
    generic_flattening_inner(value, false)
}

/// `generic_flattening(&value, budget)` that consumes `value`, charged to `budget` the same
/// way. An object, or an array of objects, with no arrays nested anywhere inside is left
/// unchanged by generic flattening, so it is returned as is instead of being copied, and
/// checked and charged in a single walk.
pub fn generic_flattening_owned(
    value: Value,
    budget: &mut FlattenBudget,
) -> Result<Vec<Value>, JsonFlattenError> {
    flattening_owned::<false>(value, budget)
}

/// What generic flattening followed by structural flattening (`flatten` with the `_`
/// separator, which every server caller of generic flattening runs on its rows) costs for
/// one event, see `flatten_pipeline_cost`
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct PipelineCost {
    /// Rows generic flattening builds. Structural flattening keeps them one for one.
    pub rows: usize,
    /// Compact JSON bytes of the generic rows, exactly as `generic_flattening_cost` counts them
    pub generic_bytes: usize,
    /// Compact JSON bytes of the generic rows once structurally flattened. Every scalar of a
    /// row is counted as a field of its own, under its joined name, even when that name
    /// collides with another field and overwrites it, so collisions make this an overcount.
    pub structural_bytes: usize,
    /// Fields of the structurally flattened rows that are objects, collisions included
    pub structural_fields: usize,
}

impl PipelineCost {
    /// What to charge for the event: its rows, with the larger of the bytes of its two
    /// representations
    pub fn charged(&self) -> FlattenCost {
        FlattenCost {
            rows: self.rows,
            bytes: self.generic_bytes.max(self.structural_bytes),
        }
    }
}

/// `generic_flattening_cost` that also counts, in the same walk, the rows structurally
/// flattened afterwards, whose field names repeat the names of their parents. Never builds
/// a row or a field name. Fails like `generic_flattening_cost`, and also as soon as the
/// structurally flattened bytes are over `max_bytes_per_request`.
pub fn flatten_pipeline_cost(
    value: &Value,
    limits: &FlattenLimits,
) -> Result<PipelineCost, JsonFlattenError> {
    let expansion = value_cost::<true>(value, false, limits)?;
    Ok(PipelineCost {
        rows: expansion.rows,
        generic_bytes: expansion.bytes,
        structural_bytes: limits.check_bytes(expansion.structural_bytes())?,
        structural_fields: expansion.keyed_fields,
    })
}

/// `charge_generic_flattening` charging every event `flatten_pipeline_cost(event).charged()`
pub fn charge_flatten_pipeline(
    value: &Value,
    budget: &mut FlattenBudget,
) -> Result<(), JsonFlattenError> {
    charge_events::<true>(value, budget)
}

/// `generic_flattening` for callers that structurally flatten the rows afterwards: the same
/// rows, charged with `charge_flatten_pipeline`
pub fn pipeline_flattening(
    value: &Value,
    budget: &mut FlattenBudget,
) -> Result<Vec<Value>, JsonFlattenError> {
    flattening::<true>(value, budget)
}

/// `generic_flattening_owned` for callers that structurally flatten the rows afterwards:
/// the same rows, charged with `charge_flatten_pipeline`. The single walk of the array-free
/// fast path counts the structurally flattened bytes as well.
pub fn pipeline_flattening_owned(
    value: Value,
    budget: &mut FlattenBudget,
) -> Result<Vec<Value>, JsonFlattenError> {
    flattening_owned::<true>(value, budget)
}

// `STRUCTURAL` selects the charge of `charge_flatten_pipeline` over the one of
// `charge_generic_flattening`, here and below
fn flattening<const STRUCTURAL: bool>(
    value: &Value,
    budget: &mut FlattenBudget,
) -> Result<Vec<Value>, JsonFlattenError> {
    charge_events::<STRUCTURAL>(value, budget)?;
    expand_generic_flattening(value)
}

fn charge_events<const STRUCTURAL: bool>(
    value: &Value,
    budget: &mut FlattenBudget,
) -> Result<(), JsonFlattenError> {
    match value {
        Value::Array(arr) => arr
            .iter()
            .try_for_each(|item| charge_events::<STRUCTURAL>(item, budget)),
        event => {
            let cost = if STRUCTURAL {
                flatten_pipeline_cost(event, budget.limits())?.charged()
            } else {
                generic_flattening_cost(event, budget.limits())?
            };
            budget.charge_event(cost)
        }
    }
}

fn flattening_owned<const STRUCTURAL: bool>(
    value: Value,
    budget: &mut FlattenBudget,
) -> Result<Vec<Value>, JsonFlattenError> {
    match value {
        Value::Object(map) => match array_free_charge::<STRUCTURAL>(&map) {
            Some(len) => {
                charge_array_free(len, budget)?;
                Ok(vec![Value::Object(map)])
            }
            None => flattening::<STRUCTURAL>(&Value::Object(map), budget),
        },
        Value::Array(arr) => {
            for (index, item) in arr.iter().enumerate() {
                let len = match item {
                    Value::Object(map) => array_free_charge::<STRUCTURAL>(map),
                    _ => None,
                };
                let Some(len) = len else {
                    // The events before this one are charged already
                    for item in &arr[index..] {
                        charge_events::<STRUCTURAL>(item, budget)?;
                    }
                    return expand_generic_flattening(&Value::Array(arr));
                };
                charge_array_free(len, budget)?;
            }
            Ok(arr)
        }
        value => flattening::<STRUCTURAL>(&value, budget),
    }
}

// Charges an array-free object of `len` bytes, failing the way `charge_events` does for
// it: its cost walk only fails on the bytes of such an object (with a positive row limit),
// and only once they are over the limit
fn charge_array_free(len: usize, budget: &mut FlattenBudget) -> Result<(), JsonFlattenError> {
    budget.limits.check_bytes(Some(len))?;
    budget.charge_event(FlattenCost {
        rows: 1,
        bytes: len,
    })
}

// Bytes `charge_events` charges for an array-free object: its compact JSON length, or the
// larger of that and its structurally flattened length. `None` when an array is nested
// anywhere inside it (or a length overflows), leaving it to the full cost walk.
fn array_free_charge<const STRUCTURAL: bool>(map: &Map<String, Value>) -> Option<usize> {
    let cost = array_free_cost::<STRUCTURAL>(map)?;
    if !STRUCTURAL {
        return Some(cost.json_len);
    }
    // A single row that is an object, see `Expansion::structural_bytes`
    let structural = if cost.fields == 0 {
        "{}".len()
    } else {
        cost.fields
            .checked_mul(4)?
            .checked_add(1)?
            .checked_add(cost.field_bytes)?
    };
    Some(cost.json_len.max(structural))
}

// An array-free object, and the fields structural flattening makes of it, counted like
// `Expansion::fields` and `Expansion::field_bytes` (only when `STRUCTURAL`)
struct ArrayFree {
    json_len: usize,
    fields: usize,
    field_bytes: usize,
}

fn array_free_cost<const STRUCTURAL: bool>(map: &Map<String, Value>) -> Option<ArrayFree> {
    // `{`, `}`, the commas and a `:` per key. A map entry takes more memory than that, so
    // this cannot overflow
    let mut len = 2 + map.len().saturating_sub(1) + map.len();
    let mut fields = 0usize;
    let mut field_bytes = 0usize;
    for (key, value) in map {
        let key_len = string_json_len(key)?;
        let value_len = match value {
            Value::Array(_) => return None,
            Value::Object(nested) => {
                let nested = array_free_cost::<STRUCTURAL>(nested)?;
                if STRUCTURAL {
                    // Every field below is named `{key}_{name}`
                    fields = fields.checked_add(nested.fields)?;
                    field_bytes = nested
                        .fields
                        .checked_mul(key_len - 2 + 1)?
                        .checked_add(nested.field_bytes)?
                        .checked_add(field_bytes)?;
                }
                nested.json_len
            }
            scalar => {
                let value_len = scalar_json_len(scalar)?;
                if STRUCTURAL {
                    fields = fields.checked_add(1)?;
                    field_bytes = field_bytes
                        .checked_add(key_len - 2)?
                        .checked_add(value_len)?;
                }
                value_len
            }
        };
        len = len.checked_add(key_len)?.checked_add(value_len)?;
    }
    Some(ArrayFree {
        json_len: len,
        fields,
        field_bytes,
    })
}

// The rows a value expands into under generic flattening: as alternatives for the key
// holding it when inside an object, or as the rows of an event at the top. Besides the rows
// and their compact JSON bytes, the walk counts, when `STRUCTURAL`, what structural
// flattening makes of those rows: a field per scalar in a row, named after the keys on the
// path to it joined with `_`, with empty objects dropped. Field names are counted without
// building them: a name below a key is that key's escaped content, a `_` and the name
// below it, and escaping a joined name escapes each part on its own.
#[derive(Clone, Copy, Default)]
struct Expansion {
    rows: usize,
    bytes: usize,
    // The fields below are only counted when `STRUCTURAL`
    /// Scalars over all rows, each a field once structurally flattened
    fields: usize,
    /// Those of them inside an object, whose name gets a `_` and the name of the key holding
    /// the value; the others are scalar rows of their own
    keyed_fields: usize,
    /// Escaped name content (below the key holding the value) and value bytes of all fields
    field_bytes: usize,
    /// Rows with no field, which structural flattening leaves as `{}`
    empty_rows: usize,
}

impl Expansion {
    fn scalar(len: usize) -> Self {
        Self {
            rows: 1,
            bytes: len,
            fields: 1,
            keyed_fields: 0,
            field_bytes: len,
            empty_rows: 0,
        }
    }

    // All rows of both, as the items of an array
    fn sum<const STRUCTURAL: bool>(
        self,
        other: Self,
        limits: &FlattenLimits,
    ) -> Result<Self, JsonFlattenError> {
        let rows = limits.check_event_rows(self.rows.checked_add(other.rows))?;
        let bytes = limits.check_bytes(self.bytes.checked_add(other.bytes))?;
        if !STRUCTURAL {
            return Ok(Self {
                rows,
                bytes,
                ..Self::default()
            });
        }
        // Every field takes a byte at least for its value, so `field_bytes` bounds the
        // field counts, and rows bound the empty rows
        let field_bytes = limits.check_bytes(self.field_bytes.checked_add(other.field_bytes))?;
        Ok(Self {
            rows,
            bytes,
            fields: self.fields + other.fields,
            keyed_fields: self.keyed_fields + other.keyed_fields,
            field_bytes,
            empty_rows: self.empty_rows + other.empty_rows,
        })
    }

    // Compact JSON bytes of the rows once structurally flattened, with a `{}` around each
    // row that is an object, a `,` between its fields, and `"` `"` `:` around each name.
    // A row that is a scalar is its only, unkeyed field, and stays as it is.
    fn structural_bytes(&self) -> Option<usize> {
        let scalar_rows = self.fields.checked_sub(self.keyed_fields)?;
        let object_rows = self.rows.checked_sub(scalar_rows)?;
        // `{}` and the commas of the rows with fields, `{}` of the empty ones
        object_rows
            .checked_add(self.empty_rows)?
            .checked_add(self.keyed_fields.checked_mul(4)?)?
            .checked_add(self.field_bytes)
    }
}

// Mirrors `generic_flattening_inner`. No branch there yields an empty set of rows for an
// object or for a value inside one, so every partial count below is a lower bound of the
// final one and going over a limit (or overflowing) on the way is final. Partial structural
// bytes are lower bounds of the final structural bytes too, which are charged when larger
// than the generic ones.
fn value_cost<const STRUCTURAL: bool>(
    value: &Value,
    normalize_empty_array: bool,
    limits: &FlattenLimits,
) -> Result<Expansion, JsonFlattenError> {
    match value {
        Value::Array(arr) if arr.is_empty() && normalize_empty_array => {
            Ok(Expansion::scalar(NULL_JSON_LEN))
        }
        Value::Array(arr) => arr.iter().try_fold(Expansion::default(), |total, item| {
            total.sum::<STRUCTURAL>(
                value_cost::<STRUCTURAL>(item, normalize_empty_array, limits)?,
                limits,
            )
        }),
        Value::Object(map) => object_cost::<STRUCTURAL>(map, limits),
        scalar => Ok(Expansion::scalar(
            limits.check_bytes(scalar_json_len(scalar))?,
        )),
    }
}

// Every row of an object holds each of its keys once, with one of the alternatives that
// key's value expands into, so the rows are the product of the alternatives per key
fn object_cost<const STRUCTURAL: bool>(
    map: &Map<String, Value>,
    limits: &FlattenLimits,
) -> Result<Expansion, JsonFlattenError> {
    // `{`, `}`, the commas and every `"key":`
    let mut row_overhead = Some(2 + map.len().saturating_sub(1));
    let mut rows = 1usize;
    // Bytes of the values in all rows so far
    let mut value_bytes = 0usize;
    // Structurally flattened fields of all rows so far, see `Expansion`
    let mut fields = 0usize;
    let mut field_bytes = 0usize;
    let mut empty_rows = 1usize;
    for (key, value) in map {
        let key_len = string_json_len(key);
        row_overhead = row_overhead
            .zip(key_len)
            .and_then(|(overhead, key)| overhead.checked_add(key)?.checked_add(1));
        let alternatives = match value {
            // Arrays use the normalization of array items, which objects ignore
            Value::Array(_) | Value::Object(_) => value_cost::<STRUCTURAL>(value, true, limits)?,
            scalar => Expansion::scalar(limits.check_bytes(scalar_json_len(scalar))?),
        };
        let previous_rows = rows;
        if matches!(value, Value::Array(_) | Value::Object(_)) {
            // Each row so far is repeated for every alternative, and each alternative
            // appears once per row so far
            value_bytes = limits.check_bytes(
                value_bytes
                    .checked_mul(alternatives.rows)
                    .zip(alternatives.bytes.checked_mul(rows))
                    .and_then(|(old, new)| old.checked_add(new)),
            )?;
            rows = limits.check_event_rows(rows.checked_mul(alternatives.rows))?;
        } else {
            // A scalar is the only alternative, so it appears once in each row
            value_bytes = limits.check_bytes(
                alternatives
                    .bytes
                    .checked_mul(rows)
                    .and_then(|bytes| value_bytes.checked_add(bytes)),
            )?;
        }
        if STRUCTURAL {
            // Under this key a field that is the value itself is named `{key}`, a field
            // below it `{key}_{name}`
            let placed = key_len
                .and_then(|len| alternatives.fields.checked_mul(len - 2))
                .and_then(|names| names.checked_add(alternatives.keyed_fields))
                .and_then(|names| names.checked_add(alternatives.field_bytes));
            // Repeated like the value bytes above
            field_bytes = limits.check_bytes(
                field_bytes
                    .checked_mul(alternatives.rows)
                    .zip(placed.and_then(|placed| placed.checked_mul(previous_rows)))
                    .and_then(|(old, new)| old.checked_add(new)),
            )?;
            // Every field takes a byte at least for its value, so these are no more than
            // the `field_bytes` just checked, and the empty rows no more than the rows
            fields = fields * alternatives.rows + alternatives.fields * previous_rows;
            empty_rows *= alternatives.empty_rows;
        }
    }
    let bytes = row_overhead
        .and_then(|overhead| rows.checked_mul(overhead))
        .and_then(|overhead| overhead.checked_add(value_bytes));
    Ok(Expansion {
        rows,
        bytes: limits.check_bytes(bytes)?,
        fields,
        keyed_fields: fields,
        field_bytes,
        empty_rows,
    })
}

const NULL_JSON_LEN: usize = "null".len();

// Length of a scalar serialized as compact JSON
fn scalar_json_len(value: &Value) -> Option<usize> {
    match value {
        Value::Null => Some(NULL_JSON_LEN),
        Value::Bool(true) => Some("true".len()),
        Value::Bool(false) => Some("false".len()),
        Value::String(s) => string_json_len(s),
        Value::Number(number) => Some(number_json_len(number)),
        Value::Array(_) | Value::Object(_) => None,
    }
}

// Length of a number serialized as compact JSON. Integers are written as plain decimal
// digits; floats are counted by formatting them the way serde_json does
fn number_json_len(number: &serde_json::Number) -> usize {
    let digits = |n: u64| n.checked_ilog10().map_or(1, |log| log as usize + 1);
    if let Some(n) = number.as_u64() {
        digits(n)
    } else if let Some(n) = number.as_i64() {
        // Only negative integers get here, as `as_u64` takes all others
        1 + digits(n.unsigned_abs())
    } else {
        let mut counter = ByteCounter(0);
        // Writing to a counter cannot fail, and a finite float is a few dozen bytes
        serde_json::to_writer(&mut counter, number).expect("counting never fails");
        counter.0
    }
}

/// Length of `s` serialized as a JSON string, quotes included, escaped the way serde_json
/// does it. `None` only when the length overflows.
pub(crate) fn string_json_len(s: &str) -> Option<usize> {
    let quoted = s.len().checked_add(2)?;
    // Escapes are rare, and this check (unlike the count below) vectorizes
    let needs_escape = |byte: u8| (byte < 0x20) | (byte == b'"') | (byte == b'\\');
    let escaped = s.as_bytes().chunks(64).any(|chunk| {
        chunk
            .iter()
            .fold(false, |found, &byte| found | needs_escape(byte))
    });
    if !escaped {
        return Some(quoted);
    }
    s.bytes().try_fold(quoted, |len, byte| {
        len.checked_add(match byte {
            b'"' | b'\\' | b'\x08' | b'\x0c' | b'\n' | b'\r' | b'\t' => 1,
            0x00..=0x1f => 5,
            _ => 0,
        })
    })
}

struct ByteCounter(usize);

impl std::io::Write for ByteCounter {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        self.0 += buf.len();
        Ok(buf.len())
    }

    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

fn generic_flattening_inner(
    value: &Value,
    normalize_empty_array: bool,
) -> Result<Vec<Value>, JsonFlattenError> {
    match value {
        // Without arrays the loop below rebuilds an exact copy of the object
        Value::Object(map) if !map_contains_array(map) => Ok(vec![value.clone()]),
        Value::Array(arr) if arr.is_empty() && normalize_empty_array => Ok(vec![Value::Null]),
        Value::Array(arr) => {
            let mut flattened = Vec::new();
            for item in arr {
                flattened.extend(generic_flattening_inner(item, normalize_empty_array)?);
            }
            Ok(flattened)
        }
        Value::Object(map) => {
            let mut results = vec![Map::new()];
            for (key, val) in map {
                // A non-empty array expands into the rows of its items, an object into its
                // own rows; every row so far is repeated for each of them
                let items = match val {
                    // Generic flattening expands non-empty arrays into scalar rows. Keep
                    // empty arrays consistent with that representation: they contain no
                    // concrete value and must not create `List(Null)` schema fields.
                    Value::Array(arr) if arr.is_empty() => {
                        set_in_rows(&mut results, key, Value::Null);
                        continue;
                    }
                    Value::Array(arr) => arr.as_slice(),
                    Value::Object(_) => std::slice::from_ref(val),
                    _ => {
                        set_in_rows(&mut results, key, val.clone());
                        continue;
                    }
                };
                let Some((first, rest)) = items.split_first() else {
                    continue;
                };
                let mut alternatives = generic_flattening_inner(first, true)?;
                if rest.is_empty() && alternatives.len() == 1 {
                    // The only alternative goes into the rows as they are
                    set_in_rows(
                        &mut results,
                        key,
                        alternatives.pop().expect("one alternative"),
                    );
                    continue;
                }
                // Items are expanded one at a time rather than all up front
                let mut expanded = Vec::new();
                repeat_rows(&mut expanded, &results, key, alternatives);
                for item in rest {
                    repeat_rows(
                        &mut expanded,
                        &results,
                        key,
                        generic_flattening_inner(item, true)?,
                    );
                }
                results = expanded;
            }

            Ok(results.into_iter().map(Value::Object).collect())
        }
        _ => Ok(vec![value.clone()]),
    }
}

// Sets `key` to `value` in every row
fn set_in_rows(rows: &mut [Map<String, Value>], key: &str, value: Value) {
    if let Some((last, rest)) = rows.split_last_mut() {
        for row in rest {
            row.insert(key.to_owned(), value.clone());
        }
        last.insert(key.to_owned(), value);
    }
}

// Appends to `expanded`, for each alternative in turn, a copy of every row in `rows`
// with `key` set to that alternative
fn repeat_rows(
    expanded: &mut Vec<Map<String, Value>>,
    rows: &[Map<String, Value>],
    key: &str,
    alternatives: Vec<Value>,
) {
    let Some((last, rest)) = rows.split_last() else {
        return;
    };
    expanded.reserve(alternatives.len().saturating_mul(rows.len()));
    for alternative in alternatives {
        for row in rest {
            let mut row = row.clone();
            row.insert(key.to_owned(), alternative.clone());
            expanded.push(row);
        }
        let mut row = last.clone();
        row.insert(key.to_owned(), alternative);
        expanded.push(row);
    }
}

// Returns true if any value nested anywhere below this map is an array
fn map_contains_array(map: &Map<String, Value>) -> bool {
    map.values().any(|value| match value {
        Value::Array(_) => true,
        Value::Object(nested) => map_contains_array(nested),
        _ => false,
    })
}

/// recursively checks the level of nesting for the serde Value
/// if Value has more than configured `P_MAX_FLATTEN_LEVEL` levels of hierarchy, returns true
/// example - if `P_MAX_FLATTEN_LEVEL` is 4, and the JSON is
/// 1. `{"a":{"b":{"c":{"d":{"e":["a","b"]}}}}}` ~> returns true
/// 2. `{"a": [{"b": 1}, {"c": 2}], "d": {"e": 4}}` ~> returns false
pub fn has_more_than_max_allowed_levels(value: &Value, current_level: usize) -> bool {
    has_more_than_levels(value, current_level, PARSEABLE.options.event_flatten_level)
}

/// `has_more_than_max_allowed_levels` with the maximum level given explicitly
pub fn has_more_than_levels(value: &Value, current_level: usize, max_level: usize) -> bool {
    if current_level > max_level {
        return true;
    }
    match value {
        Value::Array(arr) => arr
            .iter()
            .any(|item| has_more_than_levels(item, current_level, max_level)),
        Value::Object(map) => map
            .values()
            .any(|val| has_more_than_levels(val, current_level + 1, max_level)),
        _ => false,
    }
}

// Converts a Vector of values into a `Value::Array`, as long as all of them are objects
pub fn convert_to_array(flattened: Vec<Value>) -> Result<Value, JsonFlattenError> {
    if flattened.iter().any(|item| !item.is_object()) {
        return Err(JsonFlattenError::ExpectedObjectInArray);
    }

    Ok(Value::Array(flattened))
}

/// Limits that none of the test corpus comes close to, for tests about the shape of the
/// output rather than the limits
#[cfg(test)]
pub(crate) const GENEROUS_LIMITS: FlattenLimits = FlattenLimits {
    max_rows_per_event: 1 << 24,
    max_rows_per_request: 1 << 24,
    max_bytes_per_request: 1 << 30,
};

/// Verbatim copies of the flattening functions before the no-op fast paths, kept as a
/// differential oracle for tests.
#[cfg(test)]
pub(crate) mod legacy {
    use std::collections::BTreeMap;

    use serde_json::{Map, Value};

    use super::{JsonFlattenError, validate_custom_partition};

    // Time partition validation depends on global state, so the oracle only covers custom
    // partitions; the fast path runs after both validations and does not change them.
    pub fn flatten(
        nested_value: &mut Value,
        separator: &str,
        custom_partition: Option<&String>,
        validation_required: bool,
    ) -> Result<(), JsonFlattenError> {
        match nested_value {
            Value::Object(nested_dict) => {
                if validation_required {
                    validate_custom_partition(nested_dict, custom_partition)?;
                }
                let original = std::mem::take(nested_dict);
                let mut map = Map::with_capacity(original.len());
                flatten_object(&mut map, None, original, separator)?;
                *nested_dict = map;
            }
            Value::Array(arr) => {
                for nested_value in arr {
                    flatten(
                        nested_value,
                        separator,
                        custom_partition,
                        validation_required,
                    )?;
                }
            }
            _ => return Err(JsonFlattenError::CannotFlatten),
        }

        Ok(())
    }

    fn flatten_object(
        output_map: &mut Map<String, Value>,
        parent_key: Option<&str>,
        nested_map: Map<String, Value>,
        separator: &str,
    ) -> Result<(), JsonFlattenError> {
        for (key, mut value) in nested_map {
            let new_key = match parent_key {
                Some(parent) => format!("{parent}{separator}{key}"),
                None => key,
            };

            match &mut value {
                Value::Object(obj) => {
                    let obj = std::mem::take(obj);
                    flatten_object(output_map, Some(&new_key), obj, separator)?;
                }
                Value::Array(arr) if arr.iter().any(Value::is_object) => {
                    flatten_array_objects(output_map, &new_key, arr, separator)?;
                }
                _ => {
                    output_map.insert(new_key, value);
                }
            }
        }
        Ok(())
    }

    fn flatten_array_objects(
        output_map: &mut Map<String, Value>,
        parent_key: &str,
        arr: &mut [Value],
        separator: &str,
    ) -> Result<(), JsonFlattenError> {
        let mut columns: BTreeMap<String, Vec<Value>> = BTreeMap::new();

        for (index, value) in arr.iter_mut().enumerate() {
            match value {
                Value::Object(nested_object) => {
                    let mut output_map = Map::new();
                    let nested_object = std::mem::take(nested_object);
                    flatten_object(&mut output_map, Some(parent_key), nested_object, separator)?;
                    for (key, value) in output_map {
                        let column = columns
                            .entry(key)
                            .or_insert_with(|| vec![Value::Null; index]);
                        column.push(value);
                    }
                }
                Value::Null => {
                    for column in columns.values_mut() {
                        column.push(Value::Null);
                    }
                }
                _ => return Err(JsonFlattenError::NonObjectInArray),
            }

            let max_len = index + 1;
            for column in columns.values_mut() {
                while column.len() < max_len {
                    column.push(Value::Null);
                }
            }
        }

        for (key, values) in columns {
            output_map.insert(key, Value::Array(values));
        }

        Ok(())
    }

    pub fn generic_flattening(value: &Value) -> Result<Vec<Value>, JsonFlattenError> {
        generic_flattening_inner(value, false)
    }

    fn generic_flattening_inner(
        value: &Value,
        normalize_empty_array: bool,
    ) -> Result<Vec<Value>, JsonFlattenError> {
        match value {
            Value::Array(arr) if arr.is_empty() && normalize_empty_array => Ok(vec![Value::Null]),
            Value::Array(arr) => Ok(arr
                .iter()
                .flat_map(|flatten_item| {
                    generic_flattening_inner(flatten_item, normalize_empty_array)
                        .unwrap_or_default()
                })
                .collect()),
            Value::Object(map) => {
                let results = map
                    .iter()
                    .fold(vec![Map::new()], |results, (key, val)| match val {
                        Value::Array(arr) => {
                            if arr.is_empty() {
                                results
                                    .into_iter()
                                    .map(|mut result| {
                                        result.insert(key.clone(), Value::Null);
                                        result
                                    })
                                    .collect()
                            } else {
                                arr.iter()
                                    .flat_map(|flatten_item| {
                                        generic_flattening_inner(flatten_item, true)
                                            .unwrap_or_default()
                                    })
                                    .flat_map(|flattened_item| {
                                        results.iter().map(move |result| {
                                            let mut new_obj = result.clone();
                                            new_obj.insert(key.clone(), flattened_item.clone());
                                            new_obj
                                        })
                                    })
                                    .collect()
                            }
                        }
                        Value::Object(_) => generic_flattening(val)
                            .unwrap_or_default()
                            .iter()
                            .flat_map(|nested_result| {
                                results.iter().map(move |result| {
                                    let mut new_obj = result.clone();
                                    new_obj.insert(key.clone(), nested_result.clone());
                                    new_obj
                                })
                            })
                            .collect(),
                        _ => results
                            .into_iter()
                            .map(|mut result| {
                                result.insert(key.clone(), val.clone());
                                result
                            })
                            .collect(),
                    });

                Ok(results.into_iter().map(Value::Object).collect())
            }
            _ => Ok(vec![value.clone()]),
        }
    }
}

/// Deterministic generator of varied JSON shapes for differential tests.
#[cfg(test)]
pub(crate) mod shapes {
    use serde_json::{Map, Value, json};

    pub struct Gen(u64);

    // Keys chosen so that nested keys collide after flattening with `_`
    const KEYS: &[&str] = &["a", "b", "a_b", "b_a", "a_b_a", "c", "", "_", "a.b"];

    impl Gen {
        pub fn new(seed: u64) -> Self {
            Self(seed.wrapping_mul(0x9E37_79B9_7F4A_7C15) | 1)
        }

        pub fn next(&mut self, n: u64) -> u64 {
            self.0 ^= self.0 << 13;
            self.0 ^= self.0 >> 7;
            self.0 ^= self.0 << 17;
            self.0 % n
        }

        pub fn scalar(&mut self) -> Value {
            match self.next(8) {
                0 => Value::Null,
                1 => json!(true),
                2 => json!(-3),
                3 => json!(1.5),
                4 => json!(""),
                5 => json!("x.y"),
                6 => json!(u64::MAX),
                _ => json!("v"),
            }
        }

        // `arrays` controls how likely arrays are, so that both the array-free fast path
        // and the generic path get exercised
        pub fn value(&mut self, depth: u32, arrays: u64) -> Value {
            if depth == 0 {
                return self.scalar();
            }
            let roll = self.next(10);
            if roll < 4 {
                self.scalar()
            } else if roll < 7 || self.next(10) >= arrays {
                self.object(depth - 1, arrays)
            } else {
                let len = self.next(4);
                let mut arr = Vec::new();
                for _ in 0..len {
                    arr.push(match self.next(5) {
                        0 => Value::Null,
                        1 => self.scalar(),
                        2 => Value::Array(vec![]),
                        _ => self.object(depth - 1, arrays),
                    });
                }
                if self.next(3) == 0 {
                    // Homogeneous arrays of objects, the case flatten turns into columns
                    arr.retain(|v| v.is_object() || v.is_null());
                }
                Value::Array(arr)
            }
        }

        pub fn object(&mut self, depth: u32, arrays: u64) -> Value {
            let len = self.next(5);
            let mut map = Map::new();
            for _ in 0..len {
                let key = KEYS[self.next(KEYS.len() as u64) as usize];
                map.insert(key.to_owned(), self.value(depth, arrays));
            }
            Value::Object(map)
        }

        pub fn body(&mut self, arrays: u64) -> Value {
            match self.next(6) {
                0 => {
                    let len = self.next(4);
                    let mut arr = Vec::new();
                    for _ in 0..len {
                        arr.push(match self.next(8) {
                            0 => self.value(3, arrays),
                            _ => self.object(3, arrays),
                        });
                    }
                    Value::Array(arr)
                }
                1 => self.value(4, arrays),
                _ => self.object(4, arrays),
            }
        }
    }

    pub fn fixtures() -> Vec<Value> {
        vec![
            json!({}),
            json!([]),
            json!([{}]),
            json!([[]]),
            json!([[{"a": 1}], {"b": 2}]),
            json!([1]),
            json!(1),
            json!(null),
            json!("s"),
            json!({"a": 1, "b": "x", "c": null, "d": true, "e": 1.5}),
            json!({"a": {}}),
            json!({"a": {"b": {}}, "c": 1}),
            json!({"a": {"b": {"c": {"d": 1}}}}),
            json!({"a_b": 1, "a": {"b": 2}}),
            json!({"a": {"b": 2}, "a_b": 1}),
            json!({"a": {"b_c": 1}, "a_b": {"c": 2}}),
            json!({"a_b": 0, "a": [{"b": 1}, {"b": 2}]}),
            json!({"a": [{"b": 1}, {"b": 2}], "a_b": 0}),
            json!({"a": []}),
            json!({"a": [[]]}),
            json!({"a": [null]}),
            json!({"a": [1, 2], "b": [3, 4]}),
            json!({"a": [{"b": 1}, 2]}),
            json!({"a": [{"b": []}]}),
            json!({"a": [{}, {}]}),
            json!({"a": {"b": [1, {"c": 2}]}}),
            json!({"a": {"b": [{"c": [{"d": 1}]}]}}),
            json!([{"a": 1}, {"a": {"b": 2}}, {"a_b": 3, "a": {"b": 4}}]),
            json!([{"a": 1}, {"b": [1, 2]}]),
            json!([{"a": 1}, 2]),
            json!([{"a": 1}, null]),
        ]
    }
}

#[cfg(test)]
mod tests {
    use crate::utils::json::flatten::{flatten_array_objects, generic_flattening_with_limits};

    use super::{
        FlattenBudget, FlattenCost, FlattenLimits, GENEROUS_LIMITS, JsonFlattenError,
        charge_flatten_pipeline, charge_generic_flattening, flatten, flatten_pipeline_cost,
        generic_flattening, generic_flattening_cost, generic_flattening_owned, pipeline_flattening,
        pipeline_flattening_owned,
    };
    use serde_json::{Map, Value, json};

    #[test]
    fn time_validation_applies_initial_limit_then_reference_age() {
        let _state = super::time_partition_test_state(3);
        let key = "ts".to_owned();
        let now = chrono::Utc::now();
        let mut old = json!({"ts": (now - chrono::Duration::days(2)).to_rfc3339()});
        assert!(matches!(
            flatten(
                &mut old,
                "_",
                Some(&key),
                std::num::NonZeroU32::new(1),
                None,
                true,
                &mut crate::utils::json::flatten::FlattenBudget::new(
                    crate::utils::json::flatten::GENEROUS_LIMITS
                )
            ),
            Err(JsonFlattenError::TimestampTooOld(_, 1, _))
        ));
        let mut valid = json!({"ts": now.to_rfc3339()});
        flatten(
            &mut valid,
            "_",
            Some(&key),
            std::num::NonZeroU32::new(1),
            None,
            true,
            &mut crate::utils::json::flatten::FlattenBudget::new(
                crate::utils::json::flatten::GENEROUS_LIMITS,
            ),
        )
        .unwrap();
        let mut old = json!({"ts": (now - chrono::Duration::hours(4)).to_rfc3339()});
        assert!(matches!(
            flatten(
                &mut old,
                "_",
                Some(&key),
                std::num::NonZeroU32::new(1),
                None,
                true,
                &mut crate::utils::json::flatten::FlattenBudget::new(
                    crate::utils::json::flatten::GENEROUS_LIMITS
                )
            ),
            Err(JsonFlattenError::TimestampTooOldRelative(_, 3, _, _))
        ));
    }

    // Serialization keeps key order, which `==` on maps may ignore
    fn ordered(result: Result<Vec<Value>, JsonFlattenError>) -> Result<String, String> {
        result
            .map(|values| serde_json::to_string(&values).unwrap())
            .map_err(|err| err.to_string())
    }

    fn flatten_ordered(
        mut value: Value,
        separator: &str,
        custom_partition: Option<&String>,
        validation_required: bool,
        legacy: bool,
    ) -> Result<String, String> {
        let result = if legacy {
            super::legacy::flatten(&mut value, separator, custom_partition, validation_required)
        } else {
            flatten(
                &mut value,
                separator,
                None,
                None,
                custom_partition,
                validation_required,
                &mut crate::utils::json::flatten::FlattenBudget::new(
                    crate::utils::json::flatten::GENEROUS_LIMITS,
                ),
            )
        };
        result
            .map(|()| serde_json::to_string(&value).unwrap())
            .map_err(|err| format!("{err:?}"))
    }

    fn corpus() -> Vec<Value> {
        let mut values = super::shapes::fixtures();
        for arrays in [0, 3, 10] {
            let mut generator = super::shapes::Gen::new(arrays + 1);
            for _ in 0..1500 {
                values.push(generator.body(arrays));
            }
        }
        values
    }

    #[test]
    fn generic_flattening_matches_legacy() {
        for value in corpus() {
            assert_eq!(
                ordered(generic_flattening_with_limits(&value, GENEROUS_LIMITS)),
                ordered(super::legacy::generic_flattening(&value)),
                "input: {value}"
            );
        }
    }

    type Owned = fn(Value, &mut FlattenBudget) -> Result<Vec<Value>, JsonFlattenError>;
    type Borrowed = fn(&Value, &mut FlattenBudget) -> Result<Vec<Value>, JsonFlattenError>;

    #[test]
    fn generic_flattening_owned_matches_generic_flattening() {
        let modes: [(Owned, Borrowed); 2] = [
            (generic_flattening_owned, generic_flattening),
            (pipeline_flattening_owned, pipeline_flattening),
        ];
        for (owned_flattening, flattening) in modes {
            owned_matches_borrowed(owned_flattening, flattening);
        }
    }

    fn owned_matches_borrowed(owned_flattening: Owned, flattening: Borrowed) {
        let tight = [
            GENEROUS_LIMITS,
            limits(1, 1, 1 << 20),
            limits(2, 3, 1 << 20),
            limits(4, 1 << 20, 64),
            limits(1 << 20, 1 << 20, 16),
            limits(3, 5, 40),
        ];
        let mut unchanged = 0;
        for value in corpus() {
            for limits in tight {
                // Also start from a partly used budget, so that the request limits matter
                for used in [0, 1] {
                    let mut owned_budget = FlattenBudget::new(limits);
                    let mut budget = FlattenBudget::new(limits);
                    for _ in 0..used {
                        owned_budget
                            .charge_event(FlattenCost { rows: 1, bytes: 8 })
                            .unwrap();
                        budget
                            .charge_event(FlattenCost { rows: 1, bytes: 8 })
                            .unwrap();
                    }
                    let owned = owned_flattening(value.clone(), &mut owned_budget);
                    if limits == GENEROUS_LIMITS
                        && used == 0
                        && owned.as_ref().is_ok_and(|rows| match &value {
                            Value::Array(arr) => rows == arr,
                            object => rows.as_slice() == std::slice::from_ref(object),
                        })
                    {
                        unchanged += 1;
                    }
                    assert_eq!(
                        ordered(owned),
                        ordered(flattening(&value, &mut budget)),
                        "input: {value} limits: {limits:?}"
                    );
                    assert_eq!(owned_budget.used(), budget.used(), "input: {value}");
                }
            }
        }
        assert!(
            unchanged > 1000,
            "unchanged fast path exercised only {unchanged} times"
        );
    }

    #[test]
    fn generic_flattening_owned_keeps_or_expands_shapes() {
        for (value, expected) in [
            (json!({"a": {"b": {}}}), json!([{"a": {"b": {}}}])),
            (json!([]), json!([])),
            (json!([{"a": 1}, {"b": "x"}]), json!([{"a": 1}, {"b": "x"}])),
            (json!({"a": []}), json!([{"a": null}])),
            (
                json!({"a": {"b": [1, 2]}}),
                json!([{"a": {"b": 1}}, {"a": {"b": 2}}]),
            ),
            (json!([{"a": 1}, [{"b": 2}]]), json!([{"a": 1}, {"b": 2}])),
            (
                json!([{"a": 1}, {"b": [1, 2]}]),
                json!([{"a": 1}, {"b": 1}, {"b": 2}]),
            ),
            (json!([{"a": 1}, 1]), json!([{"a": 1}, 1])),
            (json!(1), json!([1])),
            (json!(null), json!([null])),
        ] {
            let rows =
                generic_flattening_owned(value.clone(), &mut FlattenBudget::new(GENEROUS_LIMITS))
                    .unwrap();
            assert_eq!(Value::Array(rows), expected, "input: {value}");
        }
    }

    #[test]
    fn scalar_json_len_matches_serde_json() {
        let mut strings: Vec<String> = (0u8..=0x7f).map(|b| char::from(b).to_string()).collect();
        strings.extend(
            [
                "",
                "é",
                "日本",
                "\u{10ffff}",
                "\u{2028}",
                "a\"b\\c",
                "\u{7f}",
            ]
            .map(str::to_owned),
        );
        // Escapes on either side of the 64 byte chunks the fast check scans
        for len in [63, 64, 65, 127, 128, 129] {
            strings.push("x".repeat(len));
            for at in [0, len / 2, len - 1] {
                let mut s = "x".repeat(len);
                s.replace_range(at..at + 1, "\n");
                strings.push(s.clone());
                s.replace_range(at..at + 1, "\u{1}");
                strings.push(s);
            }
        }
        let mut values: Vec<Value> = strings.into_iter().map(Value::String).collect();
        values.extend(
            [
                0,
                1,
                9,
                10,
                99,
                100,
                999_999,
                1_000_000,
                u64::MAX / 10,
                u64::MAX,
            ]
            .map(|n| json!(n)),
        );
        values.extend([-1, -9, -10, -99, -100, i64::MIN + 1, i64::MIN, i64::MAX].map(|n| json!(n)));
        values.extend(
            [
                0.0,
                -0.0,
                1.5,
                -1.5,
                0.1,
                1e15,
                1e16,
                1e21,
                1e22,
                1e-7,
                123_456_789.125,
                1.0 / 3.0,
                0.1 + 0.2,
                f64::MAX,
                f64::MIN,
                f64::MIN_POSITIVE,
                5e-324,
                -5e-324,
                9_007_199_254_740_992.0,
            ]
            .map(|n| json!(n)),
        );
        values.extend([json!(true), json!(false), Value::Null]);
        for value in values {
            assert_eq!(
                super::scalar_json_len(&value),
                Some(serde_json::to_vec(&value).unwrap().len()),
                "value: {value}"
            );
            if let Value::String(s) = &value {
                assert_eq!(
                    super::string_json_len(s),
                    Some(serde_json::to_vec(s).unwrap().len())
                );
            }
        }
    }

    #[test]
    fn flatten_matches_legacy() {
        let partitions =
            [None, Some("a"), Some("a, b"), Some("a_b"), Some("c")].map(|p| p.map(str::to_owned));
        let mut unchanged = 0;
        for value in corpus() {
            // Exercise flatten on both raw bodies and generic flattening output
            let mut inputs = vec![value.clone()];
            if let Ok(rows) = super::legacy::generic_flattening(&value) {
                inputs.push(Value::Array(rows));
            }
            for input in inputs {
                for separator in ["_", "."] {
                    for partition in &partitions {
                        for validation_required in [false, true] {
                            let result = flatten_ordered(
                                input.clone(),
                                separator,
                                partition.as_ref(),
                                validation_required,
                                false,
                            );
                            assert_eq!(
                                result,
                                flatten_ordered(
                                    input.clone(),
                                    separator,
                                    partition.as_ref(),
                                    validation_required,
                                    true,
                                ),
                                "input: {input} separator: {separator} partition: {partition:?}"
                            );
                            if result == Ok(serde_json::to_string(&input).unwrap()) {
                                unchanged += 1;
                            }
                        }
                    }
                }
            }
        }
        assert!(
            unchanged > 1000,
            "no-op flatten exercised only {unchanged} times"
        );
    }

    #[test]
    fn flatten_preserves_collision_order() {
        // The later key wins, at the position where the key was first inserted (with the
        // `preserve_order` map that the server builds with)
        let preserves_order =
            json!({"b": 0, "a": 0}).as_object().unwrap().keys().next() == Some(&"b".to_owned());
        for (input, expected) in [
            (
                json!({"a_b": 1, "a": {"b": 2}, "c": 3}),
                json!({"a_b": 2, "c": 3}),
            ),
            (json!({"a": {"b": 2}, "a_b": 1}), json!({"a_b": 1})),
        ] {
            let mut obj = input.clone();
            flatten(
                &mut obj,
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
            if preserves_order {
                assert_eq!(obj, expected);
            }
            assert_eq!(
                Ok(serde_json::to_string(&obj).unwrap()),
                flatten_ordered(input, "_", None, false, true)
            );
        }
    }

    #[test]
    fn flatten_drops_nested_empty_object() {
        let mut obj = json!({"a": {}, "b": {"c": {}}, "d": 1});
        flatten(
            &mut obj,
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
        assert_eq!(obj, json!({"d": 1}));
    }

    #[test]
    fn flatten_flat_object_still_validates() {
        let mut obj = json!({"a": "x.y", "b": 1});
        assert!(matches!(
            flatten(
                &mut obj,
                "_",
                None,
                None,
                Some(&"a".to_owned()),
                true,
                &mut crate::utils::json::flatten::FlattenBudget::new(
                    crate::utils::json::flatten::GENEROUS_LIMITS
                )
            ),
            Err(JsonFlattenError::FieldContainsPeriod(_))
        ));
        let mut obj = json!({"b": 1});
        assert!(matches!(
            flatten(
                &mut obj,
                "_",
                None,
                None,
                Some(&"a".to_owned()),
                true,
                &mut crate::utils::json::flatten::FlattenBudget::new(
                    crate::utils::json::flatten::GENEROUS_LIMITS
                )
            ),
            Err(JsonFlattenError::FieldNotPartOfLog(_))
        ));
        let mut obj = json!(1);
        assert!(matches!(
            flatten(
                &mut obj,
                "_",
                None,
                None,
                None,
                false,
                &mut crate::utils::json::flatten::FlattenBudget::new(
                    crate::utils::json::flatten::GENEROUS_LIMITS
                )
            ),
            Err(JsonFlattenError::CannotFlatten)
        ));
    }

    #[test]
    fn flatten_single_key_string() {
        let mut obj = json!({"key": "value"});
        let expected = obj.clone();
        flatten(
            &mut obj,
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
        assert_eq!(obj, expected);
    }

    #[test]
    fn flatten_single_key_int() {
        let mut obj = json!({"key": 1});
        let expected = obj.clone();
        flatten(
            &mut obj,
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
        assert_eq!(obj, expected);
    }

    #[test]
    fn flatten_multiple_key_value() {
        let mut obj = json!({"key1": 1, "key2": "value2"});
        let expected = obj.clone();
        flatten(
            &mut obj,
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
        assert_eq!(obj, expected);
    }

    #[test]
    fn flatten_nested_single_key_value() {
        let mut obj = json!({"key": "value", "nested_key": {"key":"value"}});
        let expected = json!({"key": "value", "nested_key.key": "value"});
        flatten(
            &mut obj,
            ".",
            None,
            None,
            None,
            false,
            &mut crate::utils::json::flatten::FlattenBudget::new(
                crate::utils::json::flatten::GENEROUS_LIMITS,
            ),
        )
        .unwrap();
        assert_eq!(obj, expected);
    }

    #[test]
    fn nested_multiple_key_value() {
        let mut obj = json!({"key": "value", "nested_key": {"key1":"value1", "key2": "value2"}});
        let expected =
            json!({"key": "value", "nested_key.key1": "value1", "nested_key.key2": "value2"});
        flatten(
            &mut obj,
            ".",
            None,
            None,
            None,
            false,
            &mut crate::utils::json::flatten::FlattenBudget::new(
                crate::utils::json::flatten::GENEROUS_LIMITS,
            ),
        )
        .unwrap();
        assert_eq!(obj, expected);
    }

    #[test]
    fn nested_key_value_with_array() {
        let mut obj = json!({"key": "value", "nested_key": {"key1":[1,2,3]}});
        let expected = json!({"key": "value", "nested_key.key1": [1,2,3]});
        flatten(
            &mut obj,
            ".",
            None,
            None,
            None,
            false,
            &mut crate::utils::json::flatten::FlattenBudget::new(
                crate::utils::json::flatten::GENEROUS_LIMITS,
            ),
        )
        .unwrap();
        assert_eq!(obj, expected);
    }

    #[test]
    fn nested_obj_array() {
        let mut obj = json!({"key": [{"a": "value0"}, {"a": "value1"}]});
        let expected = json!({"key.a": ["value0", "value1"]});
        flatten(
            &mut obj,
            ".",
            None,
            None,
            None,
            false,
            &mut crate::utils::json::flatten::FlattenBudget::new(
                crate::utils::json::flatten::GENEROUS_LIMITS,
            ),
        )
        .unwrap();
        assert_eq!(obj, expected);
    }

    #[test]
    fn nested_obj_array_nulls() {
        let mut obj = json!({"key": [{"a": "value0"}, {"a": "value1", "b": "value1"}]});
        let expected = json!({"key.a": ["value0", "value1"], "key.b": [null, "value1"]});
        flatten(
            &mut obj,
            ".",
            None,
            None,
            None,
            false,
            &mut crate::utils::json::flatten::FlattenBudget::new(
                crate::utils::json::flatten::GENEROUS_LIMITS,
            ),
        )
        .unwrap();
        assert_eq!(obj, expected);
    }

    #[test]
    fn nested_obj_array_nulls_reversed() {
        let mut obj = json!({"key": [{"a": "value0", "b": "value0"}, {"a": "value1"}]});
        let expected = json!({"key.a": ["value0", "value1"], "key.b": ["value0", null]});
        flatten(
            &mut obj,
            ".",
            None,
            None,
            None,
            false,
            &mut crate::utils::json::flatten::FlattenBudget::new(
                crate::utils::json::flatten::GENEROUS_LIMITS,
            ),
        )
        .unwrap();
        assert_eq!(obj, expected);
    }

    #[test]
    fn nested_obj_array_nested_obj() {
        let mut obj = json!({"key": [{"a": {"p": 0}, "b": "value0"}, {"b": "value1"}]});
        let expected = json!({"key.a.p": [0, null], "key.b": ["value0", "value1"]});
        flatten(
            &mut obj,
            ".",
            None,
            None,
            None,
            false,
            &mut crate::utils::json::flatten::FlattenBudget::new(
                crate::utils::json::flatten::GENEROUS_LIMITS,
            ),
        )
        .unwrap();
        assert_eq!(obj, expected);
    }

    #[test]
    fn nested_obj_array_nested_obj_array() {
        let mut obj = json!({"key": [{"a": [{"p": "value0", "q": "value0"}, {"p": "value1", "q": null}], "b": "value0"}, {"b": "value1"}]});
        let expected = json!({"key.a.p": [["value0", "value1"], null], "key.a.q": [["value0", null], null], "key.b": ["value0", "value1"]});
        flatten(
            &mut obj,
            ".",
            None,
            None,
            None,
            false,
            &mut crate::utils::json::flatten::FlattenBudget::new(
                crate::utils::json::flatten::GENEROUS_LIMITS,
            ),
        )
        .unwrap();
        assert_eq!(obj, expected);
    }

    #[test]
    fn flatten_mixed_object() {
        let mut obj = json!({"a": 42, "arr": ["1", {"key": "2"}, {"key": {"nested": "3"}}]});
        assert!(
            flatten(
                &mut obj,
                ".",
                None,
                None,
                None,
                false,
                &mut crate::utils::json::flatten::FlattenBudget::new(
                    crate::utils::json::flatten::GENEROUS_LIMITS
                )
            )
            .is_err()
        );
    }

    #[test]
    fn flatten_array_nulls_at_start() {
        let Value::Array(mut arr) = json!([
            null,
            {"p": 2, "q": 2},
            {"q": 3},
        ]) else {
            unreachable!()
        };

        let mut map = Map::new();
        flatten_array_objects(
            &mut map,
            "key",
            &mut arr,
            ".",
            &mut FlattenBudget::new(GENEROUS_LIMITS),
        )
        .unwrap();

        assert_eq!(map.len(), 2);
        assert_eq!(map.get("key.p").unwrap(), &json!([null, 2, null]));
        assert_eq!(map.get("key.q").unwrap(), &json!([null, 2, 3]));
    }

    #[test]
    fn flatten_array_objects_nulls_at_end() {
        let Value::Array(mut arr) = json!([{"a": 1, "b": 1}, {"a": 2}, null]) else {
            unreachable!()
        };

        let mut map = Map::new();
        flatten_array_objects(
            &mut map,
            "key",
            &mut arr,
            ".",
            &mut FlattenBudget::new(GENEROUS_LIMITS),
        )
        .unwrap();

        assert_eq!(map.len(), 2);
        assert_eq!(map.get("key.a").unwrap(), &json!([1, 2, null]));
        assert_eq!(map.get("key.b").unwrap(), &json!([1, null, null]));
    }

    #[test]
    fn flatten_array_objects_nulls_in_middle() {
        let Value::Array(mut arr) = json!([{"a": 1, "b": 1}, null, {"a": 3, "c": 3}]) else {
            unreachable!()
        };

        let mut map = Map::new();
        flatten_array_objects(
            &mut map,
            "key",
            &mut arr,
            ".",
            &mut FlattenBudget::new(GENEROUS_LIMITS),
        )
        .unwrap();

        assert_eq!(map.len(), 3);
        assert_eq!(map.get("key.a").unwrap(), &json!([1, null, 3]));
        assert_eq!(map.get("key.b").unwrap(), &json!([1, null, null]));
        assert_eq!(map.get("key.c").unwrap(), &json!([null, null, 3]));
    }

    #[test]
    fn flatten_array_test() {
        let Value::Array(mut arr) = json!([
            {"p": 1, "q": 1},
            {"r": 2, "q": 2},
            {"p": 3, "r": 3}
        ]) else {
            unreachable!()
        };

        let mut map = Map::new();
        flatten_array_objects(
            &mut map,
            "key",
            &mut arr,
            ".",
            &mut FlattenBudget::new(GENEROUS_LIMITS),
        )
        .unwrap();

        assert_eq!(map.len(), 3);
        assert_eq!(map.get("key.p").unwrap(), &json!([1, null, 3]));
        assert_eq!(map.get("key.q").unwrap(), &json!([1, 2, null]));
        assert_eq!(map.get("key.r").unwrap(), &json!([null, 2, 3]));
    }

    #[test]
    fn flatten_array_nested_test() {
        let Value::Array(mut arr) = json!([
            {"p": 1, "q": [{"x": 1}, {"x": 2}]},
            {"r": 2, "q": [{"x": 1}]},
            {"p": 3, "r": 3}
        ]) else {
            unreachable!()
        };

        let mut map = Map::new();
        flatten_array_objects(
            &mut map,
            "key",
            &mut arr,
            ".",
            &mut FlattenBudget::new(GENEROUS_LIMITS),
        )
        .unwrap();

        assert_eq!(map.len(), 3);
        assert_eq!(map.get("key.p").unwrap(), &json!([1, null, 3]));
        assert_eq!(map.get("key.q.x").unwrap(), &json!([[1, 2], [1], null]));
        assert_eq!(map.get("key.r").unwrap(), &json!([null, 2, 3]));
    }

    #[test]
    fn acceptable_value_custom_parition_test() {
        let mut value = json!({
            "a": 1,
        });
        assert!(
            flatten(
                &mut value,
                "_",
                None,
                None,
                Some(&"a".to_string()),
                true,
                &mut crate::utils::json::flatten::FlattenBudget::new(
                    crate::utils::json::flatten::GENEROUS_LIMITS
                )
            )
            .is_ok()
        );

        let mut value = json!({
            "a": true,
        });
        assert!(
            flatten(
                &mut value,
                "_",
                None,
                None,
                Some(&"a".to_string()),
                true,
                &mut crate::utils::json::flatten::FlattenBudget::new(
                    crate::utils::json::flatten::GENEROUS_LIMITS
                )
            )
            .is_ok()
        );

        let mut value = json!({
            "a": "yes",
        });
        assert!(
            flatten(
                &mut value,
                "_",
                None,
                None,
                Some(&"a".to_string()),
                true,
                &mut crate::utils::json::flatten::FlattenBudget::new(
                    crate::utils::json::flatten::GENEROUS_LIMITS
                )
            )
            .is_ok()
        );

        let mut value = json!({
            "a": -1,
        });
        assert!(
            flatten(
                &mut value,
                "_",
                None,
                None,
                Some(&"a".to_string()),
                true,
                &mut crate::utils::json::flatten::FlattenBudget::new(
                    crate::utils::json::flatten::GENEROUS_LIMITS
                )
            )
            .is_ok()
        );
    }

    #[test]
    fn unacceptable_value_custom_partition_test() {
        let mut value = json!({
            "a": null,
        });
        matches!(
            flatten(
                &mut value,
                "_",
                None,
                None,
                Some(&"a".to_string()),
                true,
                &mut crate::utils::json::flatten::FlattenBudget::new(
                    crate::utils::json::flatten::GENEROUS_LIMITS
                )
            )
            .unwrap_err(),
            JsonFlattenError::FieldEmptyOrNull(_)
        );

        let mut value = json!({
            "a": "",
        });
        matches!(
            flatten(
                &mut value,
                "_",
                None,
                None,
                Some(&"a".to_string()),
                true,
                &mut crate::utils::json::flatten::FlattenBudget::new(
                    crate::utils::json::flatten::GENEROUS_LIMITS
                )
            )
            .unwrap_err(),
            JsonFlattenError::FieldEmptyOrNull(_)
        );

        let mut value = json!({
            "a": {"b": 1},
        });
        matches!(
            flatten(
                &mut value,
                "_",
                None,
                None,
                Some(&"a".to_string()),
                true,
                &mut crate::utils::json::flatten::FlattenBudget::new(
                    crate::utils::json::flatten::GENEROUS_LIMITS
                )
            )
            .unwrap_err(),
            JsonFlattenError::FieldIsObject(_)
        );

        let mut value = json!({
            "a": ["b", "c"],
        });
        matches!(
            flatten(
                &mut value,
                "_",
                None,
                None,
                Some(&"a".to_string()),
                true,
                &mut crate::utils::json::flatten::FlattenBudget::new(
                    crate::utils::json::flatten::GENEROUS_LIMITS
                )
            )
            .unwrap_err(),
            JsonFlattenError::FieldIsArray(_)
        );

        let mut value = json!({
            "a": "b.c",
        });
        matches!(
            flatten(
                &mut value,
                "_",
                None,
                None,
                Some(&"a".to_string()),
                true,
                &mut crate::utils::json::flatten::FlattenBudget::new(
                    crate::utils::json::flatten::GENEROUS_LIMITS
                )
            )
            .unwrap_err(),
            JsonFlattenError::FieldContainsPeriod(_)
        );

        let mut value = json!({
            "a": 1.0,
        });
        matches!(
            flatten(
                &mut value,
                "_",
                None,
                None,
                Some(&"a".to_string()),
                true,
                &mut crate::utils::json::flatten::FlattenBudget::new(
                    crate::utils::json::flatten::GENEROUS_LIMITS
                )
            )
            .unwrap_err(),
            JsonFlattenError::FieldContainsPeriod(_)
        );
    }

    #[test]
    fn flatten_json() {
        let value = json!({"a":{"b":{"e":["a","b"]}}});
        let expected = vec![json!({"a":{"b":{"e":"a"}}}), json!({"a":{"b":{"e":"b"}}})];
        assert_eq!(
            generic_flattening_with_limits(&value, GENEROUS_LIMITS).unwrap(),
            expected
        );
    }

    #[test]
    fn generic_flattening_treats_empty_array_as_unknown_value() {
        let value = json!({"a":{"b":{"e":[]}}});
        let expected = vec![json!({"a":{"b":{"e":null}}})];

        assert_eq!(
            generic_flattening_with_limits(&value, GENEROUS_LIMITS).unwrap(),
            expected
        );
    }

    #[test]
    fn generic_flattening_treats_empty_nested_array_item_as_unknown_value() {
        let value = json!({"a": [[]]});
        let expected = vec![json!({"a": null})];

        let flattened = generic_flattening_with_limits(&value, GENEROUS_LIMITS).unwrap();
        assert!(flattened.iter().all(Value::is_object));
        assert_eq!(flattened, expected);
    }

    #[test]
    fn generic_flattening_preserves_empty_top_level_array() {
        let value = json!([]);

        assert_eq!(
            generic_flattening_with_limits(&value, GENEROUS_LIMITS).unwrap(),
            Vec::<Value>::new()
        );
    }

    fn limits(rows_per_event: usize, rows_per_request: usize, bytes: usize) -> FlattenLimits {
        FlattenLimits {
            max_rows_per_event: rows_per_event,
            max_rows_per_request: rows_per_request,
            max_bytes_per_request: bytes,
        }
    }

    fn rows_cost(rows: &[Value]) -> FlattenCost {
        FlattenCost {
            rows: rows.len(),
            bytes: rows
                .iter()
                .map(|row| serde_json::to_vec(row).unwrap().len())
                .sum(),
        }
    }

    // Splits a value into events the way `charge_generic_flattening` does
    fn events(value: &Value) -> Vec<&Value> {
        match value {
            Value::Array(arr) => arr.iter().flat_map(events).collect(),
            event => vec![event],
        }
    }

    fn cube(len: usize) -> Value {
        let arr: Vec<Value> = (0..len).map(|i| json!(i)).collect();
        json!({"a": arr, "b": arr, "c": arr})
    }

    #[test]
    fn preflight_cost_matches_built_rows() {
        let mut values = corpus();
        values.extend([
            json!({"k\"": "a\nb\u{1}\\\u{7f}é\t\r\u{8}\u{c}/", "a": [1, 2]}),
            json!({"a": [1.5, -0.0, 1e300, u64::MAX, i64::MIN], "b": [true, false, null]}),
            json!({"a": [[[[1, 2], [3]], [4]]], "b": [[], [[]]]}),
            json!({"a": [1, 2, 3], "b": {"c": [1, 2], "d": [{"e": [1, 2]}, {"e": [3]}]}}),
            json!([[{"a": [1, 2]}], [], [[]], {"b": [{"c": []}, null]}]),
            json!({"a": {"b": {"c": {}}}, "": [""]}),
        ]);
        for value in values {
            let built = generic_flattening_with_limits(&value, GENEROUS_LIMITS).unwrap();
            let mut budget = FlattenBudget::new(GENEROUS_LIMITS);
            charge_generic_flattening(&value, &mut budget).unwrap();
            assert_eq!(budget.used(), rows_cost(&built), "input: {value}");
            for event in events(&value) {
                let built = generic_flattening_with_limits(event, GENEROUS_LIMITS).unwrap();
                assert_eq!(
                    generic_flattening_cost(event, &GENEROUS_LIMITS).unwrap(),
                    rows_cost(&built),
                    "event: {event}"
                );
            }
        }
    }

    #[test]
    fn preflight_cost_of_empty_null_and_scalar_values() {
        let cases = [
            (json!({}), 1, "{}"),
            (json!({"a": []}), 1, r#"{"a":null}"#),
            (json!({"a": [[]]}), 1, r#"{"a":null}"#),
            (json!({"a": [null]}), 1, r#"{"a":null}"#),
            (json!({"a": null}), 1, r#"{"a":null}"#),
            (json!(null), 1, "null"),
            (json!("s"), 1, r#""s""#),
            (json!(12), 1, "12"),
            (json!([]), 0, ""),
            (json!([[]]), 0, ""),
        ];
        for (value, rows, json) in cases {
            let mut budget = FlattenBudget::new(GENEROUS_LIMITS);
            charge_generic_flattening(&value, &mut budget).unwrap();
            assert_eq!(
                budget.used(),
                FlattenCost {
                    rows,
                    bytes: json.len()
                },
                "input: {value}"
            );
        }
    }

    #[test]
    fn rows_per_event_limit_is_exact() {
        // 10 x 10 sibling arrays
        let value =
            json!({"a": [0, 1, 2, 3, 4, 5, 6, 7, 8, 9], "b": [0, 1, 2, 3, 4, 5, 6, 7, 8, 9]});
        assert_eq!(
            generic_flattening_with_limits(&value, limits(100, 100, 1 << 20))
                .unwrap()
                .len(),
            100
        );
        assert!(matches!(
            generic_flattening_with_limits(&value, limits(99, 100, 1 << 20)),
            Err(JsonFlattenError::RowsPerEventLimitExceeded(99))
        ));

        // 2 x (2 x 3) nested product
        let value = json!({"a": [1, 2], "b": {"c": [1, 2], "d": [{"e": [1, 2]}, {"e": [3]}]}});
        assert_eq!(
            generic_flattening_cost(&value, &limits(12, 12, 1 << 20))
                .unwrap()
                .rows,
            12
        );
        assert!(matches!(
            generic_flattening_cost(&value, &limits(11, 12, 1 << 20)),
            Err(JsonFlattenError::RowsPerEventLimitExceeded(11))
        ));

        // Deeply nested arrays are spliced into the rows of their parent key
        let value = json!({"a": [[[[1, 2], [3]], [4]]], "b": [[[5]], 6]});
        assert_eq!(
            generic_flattening_cost(&value, &limits(8, 8, 1 << 20))
                .unwrap()
                .rows,
            8
        );
        assert!(generic_flattening_cost(&value, &limits(7, 8, 1 << 20)).is_err());
    }

    #[test]
    fn rows_per_request_limit_adds_up_individually_legal_events() {
        let event = json!({"a": [1, 2, 3, 4, 5], "b": [1, 2]});
        let batch = Value::Array(vec![event.clone(); 3]);
        assert_eq!(
            generic_flattening_with_limits(&batch, limits(10, 30, 1 << 20))
                .unwrap()
                .len(),
            30
        );
        assert!(matches!(
            generic_flattening_with_limits(&batch, limits(10, 29, 1 << 20)),
            Err(JsonFlattenError::RowsPerRequestLimitExceeded(29))
        ));

        // Nested top level arrays are spliced, so their elements are events of their own
        let batch = json!([[event.clone(), [event.clone()]], event]);
        assert!(generic_flattening_with_limits(&batch, limits(10, 30, 1 << 20)).is_ok());
        assert!(matches!(
            generic_flattening_with_limits(&batch, limits(29, 29, 1 << 20)),
            Err(JsonFlattenError::RowsPerRequestLimitExceeded(29))
        ));
    }

    #[test]
    fn budget_is_shared_and_unchanged_by_a_rejected_event() {
        let event = json!({"a": [1, 2, 3]});
        let cost = generic_flattening_cost(&event, &GENEROUS_LIMITS).unwrap();
        let mut budget = FlattenBudget::new(limits(3, 7, 1 << 20));
        generic_flattening(&event, &mut budget).unwrap();
        generic_flattening(&event, &mut budget).unwrap();
        assert_eq!(budget.used().rows, 6);
        assert!(matches!(
            generic_flattening(&event, &mut budget),
            Err(JsonFlattenError::RowsPerRequestLimitExceeded(7))
        ));
        assert_eq!(budget.used().rows, 6);
        // A smaller event still fits in what is left
        generic_flattening(&json!({"a": 1}), &mut budget).unwrap();
        assert_eq!(budget.used().rows, 7);
        assert_eq!(budget.used().bytes, 2 * cost.bytes + r#"{"a":1}"#.len());

        let mut budget = FlattenBudget::new(limits(3, 7, 2 * cost.bytes));
        budget.charge_event(cost).unwrap();
        assert!(matches!(
            budget.charge_event(FlattenCost {
                rows: 1,
                bytes: cost.bytes + 1
            }),
            Err(JsonFlattenError::BytesPerRequestLimitExceeded(_))
        ));
        assert_eq!(budget.used(), cost);
        budget.charge_event(cost).unwrap();
        assert_eq!(budget.used().bytes, 2 * cost.bytes);
    }

    #[test]
    fn bytes_limit_counts_repeated_and_escaped_strings_exactly() {
        let long = "x".repeat(1000);
        let value = json!({"s": long, "a": [1, 2, 3]});
        let row_len = serde_json::to_vec(&json!({"s": long, "a": 1}))
            .unwrap()
            .len();
        let bytes = 3 * row_len;
        assert_eq!(
            generic_flattening_cost(&value, &limits(3, 3, bytes)).unwrap(),
            FlattenCost { rows: 3, bytes }
        );
        assert!(matches!(
            generic_flattening_with_limits(&value, limits(3, 3, bytes - 1)),
            Err(JsonFlattenError::BytesPerRequestLimitExceeded(limit)) if limit == bytes - 1
        ));

        // Escapes take more room than the characters they stand for
        let value = json!({"k\"\n": "\"\\\u{1}\u{1f}é", "a": [1, 2]});
        let bytes =
            rows_cost(&generic_flattening_with_limits(&value, GENEROUS_LIMITS).unwrap()).bytes;
        assert_eq!(bytes, 2 * r#"{"k\"\n":"\"\\\u0001\u001fé","a":1}"#.len());
        assert!(generic_flattening_cost(&value, &limits(2, 2, bytes)).is_ok());
        assert!(generic_flattening_cost(&value, &limits(2, 2, bytes - 1)).is_err());
    }

    #[test]
    fn cartesian_product_is_rejected_before_it_is_built() {
        // 1000 x 1000 x 1000 = 10^9 rows from an 11 KiB event
        let value = cube(1000);
        assert!(matches!(
            generic_flattening_cost(&value, &FlattenLimits::DEFAULT),
            Err(JsonFlattenError::RowsPerEventLimitExceeded(1000))
        ));
        let mut budget = FlattenBudget::new(FlattenLimits::DEFAULT);
        assert!(generic_flattening(&value, &mut budget).is_err());
        assert_eq!(budget.used(), FlattenCost::default());

        // A cube of 10 x 10 x 10 is exactly at the default limit
        assert_eq!(
            generic_flattening_with_limits(&cube(10), FlattenLimits::DEFAULT)
                .unwrap()
                .len(),
            1000
        );
        assert!(generic_flattening_with_limits(&cube(11), FlattenLimits::DEFAULT).is_err());
    }

    #[test]
    fn overflowing_counts_are_limit_errors() {
        let unlimited = limits(usize::MAX, usize::MAX, usize::MAX);
        let pairs = |keys: usize| {
            Value::Object(
                (0..keys)
                    .map(|key| (key.to_string(), json!([0, 1])))
                    .collect(),
            )
        };
        // The values in the 2^60 rows of 60 keys take more than usize::MAX bytes, and since
        // every row takes at least two bytes these overflow before the rows do
        assert!(matches!(
            generic_flattening_cost(&pairs(60), &unlimited),
            Err(JsonFlattenError::BytesPerRequestLimitExceeded(usize::MAX))
        ));
        // Rows are checked against the event limit as they are multiplied
        assert!(matches!(
            generic_flattening_cost(&pairs(65), &limits(1 << 20, usize::MAX, usize::MAX)),
            Err(JsonFlattenError::RowsPerEventLimitExceeded(_))
        ));
        // And the same for the structurally flattened fields
        assert!(matches!(
            flatten_pipeline_cost(&pairs(60), &unlimited),
            Err(JsonFlattenError::BytesPerRequestLimitExceeded(usize::MAX))
        ));
        assert!(matches!(
            flatten_pipeline_cost(&pairs(65), &limits(1 << 20, usize::MAX, usize::MAX)),
            Err(JsonFlattenError::RowsPerEventLimitExceeded(_))
        ));
    }

    // Compact JSON bytes and fields of `rows` once structurally flattened, as the callers
    // of `pipeline_flattening` do it. Rows that are not objects stay as they are.
    fn structural_cost(rows: &[Value]) -> (usize, usize) {
        let mut bytes = 0;
        let mut fields = 0;
        for row in rows {
            let mut row = row.clone();
            if let Value::Object(_) = row {
                flatten(
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
                fields += row.as_object().unwrap().len();
            }
            bytes += serde_json::to_vec(&row).unwrap().len();
        }
        (bytes, fields)
    }

    fn pipeline_fixtures() -> Vec<Value> {
        vec![
            long_parent("p", true),
            long_parent("p\"\n\\\u{1}é", true),
            json!({"p\"\n": {"a": 1, "b\\": {"c": "x"}}}),
            json!({"a": {"b": {"c": 1, "d": {"e": "x"}}}, "f": null}),
            json!({"a": {}, "b": {"c": {}}}),
            json!({"a": {}, "x": [1, 2]}),
            json!({"": {"": [1, {"": 2}]}}),
            json!({"a": [{"x": 1}, {"y": 2, "z": {"w": 3}}, {}, null, 4, []]}),
            json!({"a": [{"b": 1}, {"c": 2}], "d": {"e": [1, 2], "f": {}}}),
            json!({"a": [{"b": {}}, {"b": {"c": [{}, {"d": 1}]}}], "e": [[], [[{"f": 1}]]]}),
            json!([{"a": {"b": 1}}, [{"c": {"d": [1, 2]}}], 3, null, [[]]]),
        ]
    }

    // `parent` holding 32 fields, in 4 rows when `repeated`
    fn long_parent(parent: &str, repeated: bool) -> Value {
        let fields: Map<String, Value> = (0..32).map(|i| (format!("f{i}"), json!(i))).collect();
        let mut map = Map::new();
        map.insert(parent.to_owned(), Value::Object(fields));
        if repeated {
            map.insert("r".to_owned(), json!([0, 1, 2, 3]));
        }
        Value::Object(map)
    }

    #[test]
    fn pipeline_cost_matches_generic_and_structural_output() {
        let (mut exact, mut collided) = (0, 0);
        for value in corpus().into_iter().chain(pipeline_fixtures()) {
            let mut charged = FlattenCost::default();
            for event in events(&value) {
                let built = generic_flattening_with_limits(event, GENEROUS_LIMITS).unwrap();
                let generic = rows_cost(&built);
                let cost = flatten_pipeline_cost(event, &GENEROUS_LIMITS).unwrap();
                // The generic part is exactly `generic_flattening_cost`
                assert_eq!(
                    (cost.rows, cost.generic_bytes),
                    (generic.rows, generic.bytes),
                    "event: {event}"
                );
                let (bytes, fields) = structural_cost(&built);
                if fields == cost.structural_fields {
                    assert_eq!(cost.structural_bytes, bytes, "event: {event}");
                    exact += 1;
                } else {
                    // Fields whose joined names collide are overwritten, but counted
                    assert!(
                        fields < cost.structural_fields && bytes < cost.structural_bytes,
                        "event: {event}"
                    );
                    collided += 1;
                }
                assert_eq!(
                    cost.charged(),
                    FlattenCost {
                        rows: generic.rows,
                        bytes: generic.bytes.max(cost.structural_bytes)
                    }
                );
                charged = charged.add(cost.charged(), &GENEROUS_LIMITS).unwrap();
            }
            let mut budget = FlattenBudget::new(GENEROUS_LIMITS);
            charge_flatten_pipeline(&value, &mut budget).unwrap();
            assert_eq!(budget.used(), charged, "input: {value}");
            // The rows are those of generic flattening
            assert_eq!(
                ordered(pipeline_flattening(
                    &value,
                    &mut FlattenBudget::new(GENEROUS_LIMITS)
                )),
                ordered(generic_flattening_with_limits(&value, GENEROUS_LIMITS)),
            );
        }
        assert!(exact > 1000, "exact for {exact} events");
        assert!(collided > 10, "collisions in {collided} events");
    }

    #[test]
    fn structural_fields_are_named_after_their_parents() {
        for (value, expected) in [
            (
                json!({"p\"\n": {"a": 1, "b\\": {"c": "x"}}}),
                json!([{"p\"\n_a": 1, "p\"\n_b\\_c": "x"}]),
            ),
            (
                json!({"a": {"b": {"c": 1, "d": {"e": "x"}}}, "f": null}),
                json!([{"a_b_c": 1, "a_b_d_e": "x", "f": null}]),
            ),
            (json!({"a": {}, "b": {"c": {}}}), json!([{}])),
            (json!({"a": {}, "x": [1, 2]}), json!([{"x": 1}, {"x": 2}])),
            (
                json!({"": {"": [1, {"": 2}]}}),
                json!([{"_": 1}, {"__": 2}]),
            ),
            (
                json!({"a": [{"x": 1}, {"y": 2, "z": {"w": 3}}, {}, null, 4, []]}),
                json!([{"a_x": 1}, {"a_y": 2, "a_z_w": 3}, {}, {"a": null}, {"a": 4}, {"a": null}]),
            ),
        ] {
            let built = generic_flattening_with_limits(&value, GENEROUS_LIMITS).unwrap();
            let mut flattened = Value::Array(built.clone());
            flatten(
                &mut flattened,
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
            assert_eq!(flattened, expected, "input: {value}");
            let cost = flatten_pipeline_cost(&value, &GENEROUS_LIMITS).unwrap();
            let expected = expected.as_array().unwrap();
            assert_eq!(
                cost.structural_bytes,
                rows_cost(expected).bytes,
                "input: {value}"
            );
            assert_eq!(
                cost.structural_fields,
                expected
                    .iter()
                    .map(|row| row.as_object().unwrap().len())
                    .sum::<usize>(),
                "input: {value}"
            );
        }
    }

    #[test]
    fn long_parent_names_repeated_in_fields_are_rejected_before_expansion() {
        // The configuration of the HTTP smoke test
        let small = limits(4, 6, 4096);
        for repeated in [true, false] {
            let value = long_parent(&"p".repeat(128), repeated);
            // The generic rows fit, the structurally flattened ones do not
            let generic = generic_flattening_cost(&value, &small).unwrap();
            assert!(generic.bytes <= 4096, "{generic:?}");
            assert!(matches!(
                flatten_pipeline_cost(&value, &small),
                Err(JsonFlattenError::BytesPerRequestLimitExceeded(4096))
            ));
            let mut budget = FlattenBudget::new(small);
            assert!(matches!(
                pipeline_flattening_owned(value.clone(), &mut budget),
                Err(JsonFlattenError::BytesPerRequestLimitExceeded(4096))
            ));
            assert!(matches!(
                pipeline_flattening(&value, &mut budget),
                Err(JsonFlattenError::BytesPerRequestLimitExceeded(4096))
            ));
            assert!(charge_flatten_pipeline(&value, &mut budget).is_err());
            assert_eq!(budget.used(), FlattenCost::default());
            // Generic flattening alone is charged its own rows only
            generic_flattening_owned(value, &mut budget).unwrap();

            // A short parent name fits, charged for the larger of both representations
            let value = long_parent("p", repeated);
            let mut budget = FlattenBudget::new(small);
            let rows = pipeline_flattening_owned(value, &mut budget).unwrap();
            let generic = rows_cost(&rows);
            let (structural, _) = structural_cost(&rows);
            assert!(structural > generic.bytes);
            assert_eq!(
                budget.used(),
                FlattenCost {
                    rows: generic.rows,
                    bytes: structural
                }
            );
        }
    }

    #[test]
    fn structural_bytes_add_up_over_events() {
        let event = long_parent("p", true);
        let cost = flatten_pipeline_cost(&event, &GENEROUS_LIMITS)
            .unwrap()
            .charged();
        assert!(
            cost.bytes
                > generic_flattening_cost(&event, &GENEROUS_LIMITS)
                    .unwrap()
                    .bytes
        );
        // Also through the array-free fast path
        let flat = long_parent("p", false);
        let flat_cost = flatten_pipeline_cost(&flat, &GENEROUS_LIMITS)
            .unwrap()
            .charged();
        let batch = json!([flat, event]);
        let bytes = flat_cost.bytes + cost.bytes;

        let mut budget = FlattenBudget::new(limits(4, 8, bytes - 1));
        assert!(matches!(
            pipeline_flattening_owned(batch.clone(), &mut budget),
            Err(JsonFlattenError::BytesPerRequestLimitExceeded(_))
        ));
        // The first event stays charged, the rejected one is not
        assert_eq!(budget.used(), flat_cost);

        let mut budget = FlattenBudget::new(limits(4, 8, bytes));
        assert_eq!(
            pipeline_flattening_owned(batch, &mut budget).unwrap().len(),
            5
        );
        assert_eq!(budget.used().bytes, bytes);
    }

    #[test]
    fn limit_errors_are_identified() {
        assert!(JsonFlattenError::RowsPerEventLimitExceeded(1).is_limit_exceeded());
        assert!(JsonFlattenError::RowsPerRequestLimitExceeded(1).is_limit_exceeded());
        assert!(JsonFlattenError::BytesPerRequestLimitExceeded(1).is_limit_exceeded());
        assert!(!JsonFlattenError::CannotFlatten.is_limit_exceeded());
        assert!(!JsonFlattenError::ExpectedObjectInArray.is_limit_exceeded());
        let message = JsonFlattenError::RowsPerEventLimitExceeded(1000).to_string();
        assert!(message.contains("1000"), "{message}");
        assert!(
            message.contains("P_MAX_FLATTEN_ROWS_PER_EVENT"),
            "{message}"
        );
        let message = JsonFlattenError::RowsPerRequestLimitExceeded(10).to_string();
        assert!(message.contains("Split the batch"), "{message}");
    }
}
