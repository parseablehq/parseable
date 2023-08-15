/*
 * Parseable Server (C) 2022 - 2023 Parseable, Inc.
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

use arrow_array::{cast::as_string_array, RecordBatch};
use datafusion::arrow::datatypes::Schema;
use itertools::Itertools;
use serde::{
    de::{MapAccess, Visitor},
    Deserialize, Deserializer,
};
use std::{
    fmt,
    marker::PhantomData,
    str::FromStr,
    sync::atomic::{AtomicU32, Ordering},
};

use self::base::{
    ops::{NumericOperator, StringOperator},
    NumericRule, StringRule,
};

use super::AlertState;

#[derive(Debug, serde::Serialize, serde::Deserialize)]
#[serde(tag = "type", content = "config")]
#[serde(rename_all = "camelCase")]
pub enum Rule {
    Column(ColumnRule),
    #[serde(deserialize_with = "string_or_struct", serialize_with = "to_string")]
    Composite(CompositeRule),
}

impl Rule {
    pub fn resolves(&self, event: RecordBatch) -> Vec<AlertState> {
        match self {
            Rule::Column(rule) => rule.resolves(event),
            Rule::Composite(rule) => rule
                .resolves(event)
                .iter()
                .map(|x| {
                    if *x {
                        AlertState::SetToFiring
                    } else {
                        AlertState::Listening
                    }
                })
                .collect(),
        }
    }

    pub fn valid_for_schema(&self, schema: &Schema) -> bool {
        match self {
            Rule::Column(rule) => rule.valid_for_schema(schema),
            Rule::Composite(rule) => rule.valid_for_schema(schema),
        }
    }

    pub fn trigger_reason(&self) -> String {
        match self {
            Rule::Column(rule) => rule.trigger_reason(),
            Rule::Composite(rule) => format!("matched rule {}", rule),
        }
    }
}

#[derive(Debug, serde::Serialize, serde::Deserialize)]
#[serde(untagged)]
pub enum ColumnRule {
    ConsecutiveNumeric(ConsecutiveNumericRule),
    ConsecutiveString(ConsecutiveStringRule),
}

impl ColumnRule {
    fn resolves(&self, event: RecordBatch) -> Vec<AlertState> {
        match self {
            Self::ConsecutiveNumeric(rule) => rule.resolves(event),
            Self::ConsecutiveString(rule) => rule.resolves(event),
        }
    }

    fn valid_for_schema(&self, schema: &Schema) -> bool {
        match self {
            Self::ConsecutiveNumeric(ConsecutiveNumericRule {
                base_rule: rule, ..
            }) => rule.valid_for_schema(schema),
            Self::ConsecutiveString(ConsecutiveStringRule {
                base_rule: rule, ..
            }) => rule.valid_for_schema(schema),
        }
    }

    fn trigger_reason(&self) -> String {
        match self {
            Self::ConsecutiveNumeric(ConsecutiveNumericRule {
                base_rule:
                    NumericRule {
                        column,
                        operator,
                        value,
                    },
                state: ConsecutiveRepeatState { repeats, .. },
                ..
            }) => format!(
                "{} column was {} {}, {} times",
                column,
                match operator {
                    NumericOperator::EqualTo => "equal to",
                    NumericOperator::NotEqualTo => " not equal to",
                    NumericOperator::GreaterThan => "greater than",
                    NumericOperator::GreaterThanEquals => "greater than or equal to",
                    NumericOperator::LessThan => "less than",
                    NumericOperator::LessThanEquals => "less than or equal to",
                },
                value,
                repeats
            ),
            Self::ConsecutiveString(ConsecutiveStringRule {
                base_rule:
                    StringRule {
                        column,
                        operator,
                        value,
                        ..
                    },
                state: ConsecutiveRepeatState { repeats, .. },
                ..
            }) => format!(
                "{} column {} {}, {} times",
                column,
                match operator {
                    StringOperator::Exact => "equal to",
                    StringOperator::NotExact => "not equal to",
                    StringOperator::Contains => "contains",
                    StringOperator::NotContains => "does not contain",
                    StringOperator::Regex => "matches regex",
                },
                value,
                repeats
            ),
        }
    }
}

// Rules for alerts

#[derive(Debug, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ConsecutiveNumericRule {
    #[serde(flatten)]
    pub base_rule: base::NumericRule,
    #[serde(flatten)]
    pub state: ConsecutiveRepeatState,
}

impl ConsecutiveNumericRule {
    fn resolves(&self, event: RecordBatch) -> Vec<AlertState> {
        let Some(column) = event.column_by_name(&self.base_rule.column) else {
            return Vec::new();
        };

        let base_matches = self.base_rule.resolves(column);

        base_matches
            .into_iter()
            .map(|matches| {
                if matches {
                    self.state.update_and_fetch_state()
                } else {
                    self.state.fetch_state()
                }
            })
            .collect()
    }
}

#[derive(Debug, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ConsecutiveStringRule {
    #[serde(flatten)]
    pub base_rule: base::StringRule,
    #[serde(flatten)]
    pub state: ConsecutiveRepeatState,
}

impl ConsecutiveStringRule {
    fn resolves(&self, event: RecordBatch) -> Vec<AlertState> {
        let Some(column) = event.column_by_name(&self.base_rule.column) else {
            return Vec::new();
        };

        let base_matches = self.base_rule.resolves(as_string_array(column));

        base_matches
            .into_iter()
            .map(|matches| {
                if matches {
                    self.state.update_and_fetch_state()
                } else {
                    self.state.fetch_state()
                }
            })
            .collect()
    }
}

fn one() -> u32 {
    1
}

#[derive(Debug, serde::Serialize, serde::Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub enum CompositeRule {
    And(Vec<CompositeRule>),
    Or(Vec<CompositeRule>),
    Not(Box<CompositeRule>),
    Numeric(NumericRule),
    String(StringRule),
}

impl CompositeRule {
    fn resolves(&self, event: RecordBatch) -> Vec<bool> {
        match self {
            CompositeRule::And(rules) => {
                // get individual evaluation for each subrule
                let mut evaluations = rules
                    .iter()
                    .map(|x| x.resolves(event.clone()))
                    .collect_vec();
                // They all must be of same length otherwise some columns was missing in evaluation
                let is_same_len = evaluations.iter().map(|x| x.len()).all_equal();
                // if there are more than one rule then we go through all evaluations and compare them side by side
                if is_same_len && evaluations.len() > 1 {
                    (0..evaluations[0].len())
                        .map(|idx| evaluations.iter().all(|x| x[idx]))
                        .collect()
                } else if is_same_len && evaluations.len() == 1 {
                    evaluations.pop().expect("length one")
                } else {
                    vec![]
                }
            }
            CompositeRule::Or(rules) => {
                // get individual evaluation for each subrule
                let evaluations: Vec<Vec<bool>> = rules
                    .iter()
                    .map(|x| x.resolves(event.clone()))
                    .collect_vec();
                let mut evaluation_iterators = evaluations.iter().map(|x| x.iter()).collect_vec();
                let mut res = vec![];

                loop {
                    let mut continue_iteration = false;
                    let mut accumulator = false;
                    for iter in &mut evaluation_iterators {
                        if let Some(val) = iter.next() {
                            accumulator = accumulator || *val;
                            continue_iteration = true
                        }
                    }
                    if !continue_iteration {
                        break;
                    } else {
                        res.push(accumulator)
                    }
                }

                res
            }
            CompositeRule::Numeric(rule) => {
                let Some(column) = event.column_by_name(&rule.column) else {
                    return Vec::new();
                };
                rule.resolves(column)
            }
            CompositeRule::String(rule) => {
                let Some(column) = event.column_by_name(&rule.column) else {
                    return Vec::new();
                };
                rule.resolves(as_string_array(column))
            }
            CompositeRule::Not(rule) => {
                let mut res = rule.resolves(event);
                res.iter_mut().for_each(|x| *x = !*x);
                res
            }
        }
    }

    fn valid_for_schema(&self, schema: &Schema) -> bool {
        match self {
            CompositeRule::And(rules) => rules.iter().all(|rule| rule.valid_for_schema(schema)),
            CompositeRule::Or(rules) => rules.iter().all(|rule| rule.valid_for_schema(schema)),
            CompositeRule::Not(rule) => rule.valid_for_schema(schema),
            CompositeRule::Numeric(rule) => rule.valid_for_schema(schema),
            CompositeRule::String(rule) => rule.valid_for_schema(schema),
        }
    }
}

impl fmt::Display for CompositeRule {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let v = match self {
            CompositeRule::And(rules) => {
                let rules_str: Vec<String> = rules.iter().map(|rule| rule.to_string()).collect();
                format!("({})", rules_str.join(" and "))
            }
            CompositeRule::Or(rules) => {
                let rules_str: Vec<String> = rules.iter().map(|rule| rule.to_string()).collect();
                format!("({})", rules_str.join(" or "))
            }
            CompositeRule::Not(rule) => format!("!({})", rule),
            CompositeRule::Numeric(numeric_rule) => numeric_rule.to_string(),
            CompositeRule::String(string_rule) => string_rule.to_string(),
        };
        write!(f, "{}", v)
    }
}

#[derive(Debug, serde::Serialize, serde::Deserialize)]
pub struct ConsecutiveRepeatState {
    #[serde(default = "one")]
    pub repeats: u32,
    #[serde(skip)]
    repeated: AtomicU32,
}

impl ConsecutiveRepeatState {
    fn update_and_fetch_state(&self) -> AlertState {
        self._fetch_state(true)
    }

    fn fetch_state(&self) -> AlertState {
        self._fetch_state(false)
    }

    fn _fetch_state(&self, update: bool) -> AlertState {
        let mut repeated = self.repeated.load(Ordering::Acquire);
        let mut state = AlertState::Listening;

        let firing = repeated >= self.repeats;

        if firing {
            if update {
                state = AlertState::Firing;
            } else {
                // did not match, i.e resolved
                repeated = 0;
                state = AlertState::Resolved;
            }
        } else if update {
            repeated += 1;
            if repeated == self.repeats {
                state = AlertState::SetToFiring;
            }
        }

        self.repeated.store(repeated, Ordering::Release);
        state
    }
}

fn string_or_struct<'de, T, D>(deserializer: D) -> Result<T, D::Error>
where
    T: Deserialize<'de> + FromStr<Err = Box<dyn std::error::Error>>,
    D: Deserializer<'de>,
{
    // This is a Visitor that forwards string types to T's `FromStr` impl and
    // forwards map types to T's `Deserialize` impl. The `PhantomData` is to
    // keep the compiler from complaining about T being an unused generic type
    // parameter. We need T in order to know the Value type for the Visitor
    // impl.
    struct StringOrStruct<T>(PhantomData<fn() -> T>);

    impl<'de, T> Visitor<'de> for StringOrStruct<T>
    where
        T: Deserialize<'de> + FromStr<Err = Box<dyn std::error::Error>>,
    {
        type Value = T;

        fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
            formatter.write_str("string or map")
        }

        fn visit_str<E>(self, value: &str) -> Result<T, E>
        where
            E: serde::de::Error,
        {
            FromStr::from_str(value).map_err(|x| serde::de::Error::custom(x))
        }

        fn visit_map<M>(self, map: M) -> Result<T, M::Error>
        where
            M: MapAccess<'de>,
        {
            // `MapAccessDeserializer` is a wrapper that turns a `MapAccess`
            // into a `Deserializer`, allowing it to be used as the input to T's
            // `Deserialize` implementation. T then deserializes itself using
            // the entries from the map visitor.
            Deserialize::deserialize(serde::de::value::MapAccessDeserializer::new(map))
        }
    }

    deserializer.deserialize_any(StringOrStruct(PhantomData))
}

fn to_string<S>(ty: &CompositeRule, serializer: S) -> Result<S::Ok, S::Error>
where
    S: serde::Serializer,
{
    serializer.serialize_str(&ty.to_string())
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::AtomicU32;

    use rstest::*;

    use super::{AlertState, ConsecutiveRepeatState};

    #[fixture]
    pub fn rule(#[default(5)] repeats: u32, #[default(0)] repeated: u32) -> ConsecutiveRepeatState {
        ConsecutiveRepeatState {
            repeats,
            repeated: AtomicU32::new(repeated),
        }
    }

    #[rstest]
    fn numeric_consecutive_rule_repeats_1(#[with(1, 0)] rule: ConsecutiveRepeatState) {
        assert_eq!(rule.update_and_fetch_state(), AlertState::SetToFiring);
        assert_eq!(rule.update_and_fetch_state(), AlertState::Firing);
        assert_eq!(rule.update_and_fetch_state(), AlertState::Firing);
        assert_eq!(rule.fetch_state(), AlertState::Resolved);
        assert_eq!(rule.fetch_state(), AlertState::Listening);
        assert_eq!(rule.update_and_fetch_state(), AlertState::SetToFiring);
    }

    #[rstest]
    fn numeric_consecutive_rule_repeats_2(#[with(2, 1)] rule: ConsecutiveRepeatState) {
        assert_eq!(rule.update_and_fetch_state(), AlertState::SetToFiring);
        assert_eq!(rule.update_and_fetch_state(), AlertState::Firing);
        assert_eq!(rule.fetch_state(), AlertState::Resolved);
        assert_eq!(rule.fetch_state(), AlertState::Listening);
        assert_eq!(rule.update_and_fetch_state(), AlertState::Listening);
        assert_eq!(rule.update_and_fetch_state(), AlertState::SetToFiring);
    }
}

pub mod base {
    use std::fmt::Display;

    use arrow_array::{
        cast::as_primitive_array,
        types::{Float64Type, Int64Type, UInt64Type},
        Array, ArrowPrimitiveType, PrimitiveArray, StringArray,
    };
    use arrow_schema::{DataType, Schema};
    use itertools::Itertools;

    use self::ops::{NumericOperator, StringOperator};
    use regex::Regex;

    #[derive(Debug, serde::Serialize, serde::Deserialize, PartialEq, Eq)]
    #[serde(rename_all = "camelCase")]
    pub struct NumericRule {
        pub column: String,
        /// Field that determines what comparison operator is to be used
        #[serde(default)]
        pub operator: NumericOperator,
        pub value: serde_json::Number,
    }

    impl NumericRule {
        pub fn resolves(&self, event: &dyn Array) -> Vec<bool> {
            let datatype = event.data_type();
            match datatype {
                arrow_schema::DataType::Int64 => Self::eval_op(
                    self.operator,
                    self.value.as_i64().unwrap(),
                    as_primitive_array::<Int64Type>(event),
                ),
                arrow_schema::DataType::UInt64 => Self::eval_op(
                    self.operator,
                    self.value.as_u64().unwrap(),
                    as_primitive_array::<UInt64Type>(event),
                ),
                arrow_schema::DataType::Float64 => Self::eval_op(
                    self.operator,
                    self.value.as_f64().unwrap(),
                    as_primitive_array::<Float64Type>(event),
                ),
                _ => unreachable!(),
            }
        }

        fn eval_op<T: ArrowPrimitiveType>(
            op: NumericOperator,
            value: T::Native,
            arr: &PrimitiveArray<T>,
        ) -> Vec<bool> {
            arr.iter()
                .map(|number| {
                    let Some(number) = number else { return false };
                    match op {
                        NumericOperator::EqualTo => number == value,
                        NumericOperator::NotEqualTo => number != value,
                        NumericOperator::GreaterThan => number > value,
                        NumericOperator::GreaterThanEquals => number >= value,
                        NumericOperator::LessThan => number < value,
                        NumericOperator::LessThanEquals => number <= value,
                    }
                })
                .collect()
        }

        pub fn valid_for_schema(&self, schema: &Schema) -> bool {
            match schema.column_with_name(&self.column) {
                Some((_, column)) => matches!(
                    column.data_type(),
                    DataType::Int8
                        | DataType::Int16
                        | DataType::Int32
                        | DataType::Int64
                        | DataType::UInt8
                        | DataType::UInt16
                        | DataType::UInt32
                        | DataType::UInt64
                        | DataType::Float16
                        | DataType::Float32
                        | DataType::Float64
                ),
                None => false,
            }
        }
    }

    impl Display for NumericRule {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            write!(f, "{} {} {}", self.column, self.operator, self.value)
        }
    }

    #[derive(Debug, serde::Serialize, serde::Deserialize, PartialEq, Eq)]
    #[serde(rename_all = "camelCase")]
    pub struct StringRule {
        pub column: String,
        #[serde(default)]
        pub operator: StringOperator,
        pub ignore_case: Option<bool>,
        pub value: String,
    }

    impl StringRule {
        pub fn resolves(&self, event: &StringArray) -> Vec<bool> {
            event
                .iter()
                .map(|string| {
                    let Some(string) = string else { return false };
                    Self::matches(
                        self.operator,
                        string,
                        &self.value,
                        self.ignore_case.unwrap_or_default(),
                    )
                })
                .collect_vec()
        }

        fn matches(op: StringOperator, string: &str, value: &str, ignore_case: bool) -> bool {
            if ignore_case {
                match op {
                    StringOperator::Exact => string.eq_ignore_ascii_case(value),
                    StringOperator::NotExact => !string.eq_ignore_ascii_case(value),
                    StringOperator::Contains => string
                        .to_ascii_lowercase()
                        .contains(&value.to_ascii_lowercase()),
                    StringOperator::NotContains => !string
                        .to_ascii_lowercase()
                        .contains(&value.to_ascii_lowercase()),
                    StringOperator::Regex => {
                        let re: Regex = regex::Regex::new(value).unwrap();
                        re.is_match(string)
                    }
                }
            } else {
                match op {
                    StringOperator::Exact => string.eq(value),
                    StringOperator::NotExact => !string.eq(value),
                    StringOperator::Contains => string.contains(value),
                    StringOperator::NotContains => !string.contains(value),
                    StringOperator::Regex => {
                        let re: Regex = regex::Regex::new(value).unwrap();
                        re.is_match(string)
                    }
                }
            }
        }

        pub fn valid_for_schema(&self, schema: &Schema) -> bool {
            match schema.column_with_name(&self.column) {
                Some((_, column)) => matches!(column.data_type(), DataType::Utf8),
                None => false,
            }
        }
    }

    impl Display for StringRule {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            write!(f, "{} {} \"{}\"", self.column, self.operator, self.value)
        }
    }

    pub mod ops {
        use std::fmt::Display;

        #[derive(
            Debug, Default, Copy, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize,
        )]
        #[serde(rename_all = "camelCase")]
        pub enum NumericOperator {
            #[default]
            #[serde(alias = "=")]
            EqualTo,
            #[serde(alias = "!=")]
            NotEqualTo,
            #[serde(alias = ">")]
            GreaterThan,
            #[serde(alias = ">=")]
            GreaterThanEquals,
            #[serde(alias = "<")]
            LessThan,
            #[serde(alias = "<=")]
            LessThanEquals,
        }

        impl Display for NumericOperator {
            fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                write!(
                    f,
                    "{}",
                    match self {
                        NumericOperator::EqualTo => "=",
                        NumericOperator::NotEqualTo => "!=",
                        NumericOperator::GreaterThan => ">",
                        NumericOperator::GreaterThanEquals => ">=",
                        NumericOperator::LessThan => "<",
                        NumericOperator::LessThanEquals => "<=",
                    }
                )
            }
        }

        #[derive(
            Debug, Default, Copy, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize,
        )]
        #[serde(rename_all = "camelCase")]
        pub enum StringOperator {
            #[serde(alias = "=")]
            Exact,
            #[serde(alias = "!=")]
            NotExact,
            #[default]
            #[serde(alias = "=%")]
            Contains,
            #[serde(alias = "!%")]
            NotContains,
            #[serde(alias = "~")]
            Regex,
        }

        impl Display for StringOperator {
            fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                write!(
                    f,
                    "{}",
                    match self {
                        StringOperator::Exact => "=",
                        StringOperator::NotExact => "!=",
                        StringOperator::Contains => "=%",
                        StringOperator::NotContains => "!%",
                        StringOperator::Regex => "~",
                    }
                )
            }
        }
    }
}
