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
use datafusion::arrow::datatypes::{DataType, Schema};
use std::sync::atomic::{AtomicU32, Ordering};

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
}

impl Rule {
    pub fn resolves(&self, event: RecordBatch) -> Vec<AlertState> {
        match self {
            Rule::Column(rule) => rule.resolves(event),
        }
    }

    pub fn valid_for_schema(&self, schema: &Schema) -> bool {
        match self {
            Rule::Column(rule) => rule.valid_for_schema(schema),
        }
    }

    pub fn trigger_reason(&self) -> String {
        match self {
            Rule::Column(rule) => rule.trigger_reason(),
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
            }) => match schema.column_with_name(&rule.column) {
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
            },
            Self::ConsecutiveString(ConsecutiveStringRule {
                base_rule: rule, ..
            }) => match schema.column_with_name(&rule.column) {
                Some((_, column)) => matches!(column.data_type(), DataType::Utf8),
                None => false,
            },
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
    use arrow_array::{
        cast::as_primitive_array,
        types::{Float64Type, Int64Type, UInt64Type},
        Array, ArrowPrimitiveType, PrimitiveArray, StringArray,
    };
    use itertools::Itertools;

    use self::ops::{NumericOperator, StringOperator};
    use regex::Regex;

    #[derive(Debug, serde::Serialize, serde::Deserialize)]
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
    }

    #[derive(Debug, serde::Serialize, serde::Deserialize)]
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
    }

    pub mod ops {
        #[derive(Debug, Copy, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
        #[serde(rename_all = "camelCase")]
        pub enum NumericOperator {
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

        impl Default for NumericOperator {
            fn default() -> Self {
                Self::EqualTo
            }
        }

        #[derive(Debug, Copy, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
        #[serde(rename_all = "camelCase")]
        pub enum StringOperator {
            #[serde(alias = "=")]
            Exact,
            #[serde(alias = "!=")]
            NotExact,
            #[serde(alias = "=%")]
            Contains,
            #[serde(alias = "!%")]
            NotContains,
            #[serde(alias = "~")]
            Regex,
        }

        impl Default for StringOperator {
            fn default() -> Self {
                Self::Contains
            }
        }
    }
}
