/*
 * Parseable Server (C) 2022 Parseable, Inc.
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

use serde::{Deserialize, Serialize};
use std::sync::atomic::{AtomicU32, Ordering};

#[derive(Debug, Serialize, Deserialize)]
#[serde(untagged)]
pub enum Rule {
    Numeric(NumericRule),
    String(StringRule),
}

impl Rule {
    pub(super) fn resolves(&self, event: &serde_json::Value) -> bool {
        match self {
            Rule::Numeric(rule) => rule.resolves(event),
            Rule::String(rule) => rule.resolves(event),
        }
    }

    pub fn valid_for_schema(&self, schema: &arrow_schema::Schema) -> bool {
        match self {
            Rule::Numeric(NumericRule { column, .. }) => match schema.column_with_name(column) {
                Some((_, column)) => matches!(
                    column.data_type(),
                    arrow_schema::DataType::Int8
                        | arrow_schema::DataType::Int16
                        | arrow_schema::DataType::Int32
                        | arrow_schema::DataType::Int64
                        | arrow_schema::DataType::UInt8
                        | arrow_schema::DataType::UInt16
                        | arrow_schema::DataType::UInt32
                        | arrow_schema::DataType::UInt64
                        | arrow_schema::DataType::Float16
                        | arrow_schema::DataType::Float32
                        | arrow_schema::DataType::Float64
                ),
                None => false,
            },
            Rule::String(StringRule { column, .. }) => match schema.column_with_name(column) {
                Some((_, column)) => matches!(column.data_type(), arrow_schema::DataType::Utf8),
                None => false,
            },
        }
    }

    pub(super) fn trigger_reason(&self) -> String {
        match self {
            Rule::Numeric(NumericRule {
                column,
                operator,
                value,
                repeats,
                ..
            }) => match operator {
                NumericOperator::EqualTo => format!(
                    "{} column was equal to {}, {} times",
                    column, value, repeats
                ),
                NumericOperator::NotEqualTo => format!(
                    "{} column was not equal to {}, {} times",
                    column, value, repeats
                ),
                NumericOperator::GreaterThan => format!(
                    "{} column was greater than {}, {} times",
                    column, value, repeats
                ),
                NumericOperator::GreaterThanEquals => format!(
                    "{} column was greater than or equal to {}, {} times",
                    column, value, repeats
                ),
                NumericOperator::LessThan => format!(
                    "{} column was less than {}, {} times",
                    column, value, repeats
                ),
                NumericOperator::LessThanEquals => format!(
                    "{} column was less than or equal to {}, {} times",
                    column, value, repeats
                ),
            },
            Rule::String(StringRule {
                column,
                operator,
                value,
                ..
            }) => match operator {
                StringOperator::Exact => format!("{} column value is {}", column, value),
                StringOperator::NotExact => format!("{} column value is not {}", column, value),
                StringOperator::Contains => format!("{} column contains {}", column, value),
                StringOperator::NotContains => {
                    format!("{} column does not contains {}", column, value)
                }
            },
        }
    }
}

// Rules for alerts

#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct NumericRule {
    pub column: String,
    /// Field that determines what comparison operator is to be used
    #[serde(default)]
    pub operator: NumericOperator,
    pub value: serde_json::Number,
    pub repeats: u32,
    #[serde(skip)]
    repeated: AtomicU32,
}

impl NumericRule {
    fn resolves(&self, event: &serde_json::Value) -> bool {
        let number = match event.get(&self.column).expect("column exists") {
            serde_json::Value::Number(number) => number,
            _ => unreachable!("right rule is set for right column type"),
        };

        let comparison = match self.operator {
            NumericOperator::EqualTo => number == &self.value,
            NumericOperator::NotEqualTo => number != &self.value,
            NumericOperator::GreaterThan => number.as_f64().unwrap() > self.value.as_f64().unwrap(),
            NumericOperator::GreaterThanEquals => {
                number.as_f64().unwrap() >= self.value.as_f64().unwrap()
            }
            NumericOperator::LessThan => number.as_f64().unwrap() < self.value.as_f64().unwrap(),
            NumericOperator::LessThanEquals => {
                number.as_f64().unwrap() <= self.value.as_f64().unwrap()
            }
        };

        // If truthy, increment count of repeated
        // acquire lock and load
        let mut repeated = self.repeated.load(Ordering::Acquire);

        if comparison {
            repeated += 1
        }

        // If enough repetitions made, return true
        let ret = if repeated >= self.repeats {
            repeated = 0;
            true
        } else {
            false
        };
        // store the value back to repeated and release
        self.repeated.store(repeated, Ordering::Release);

        ret
    }
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct StringRule {
    pub column: String,
    #[serde(default)]
    pub operator: StringOperator,
    pub ignore_case: Option<bool>,
    pub value: String,
}

impl StringRule {
    pub fn resolves(&self, event: &serde_json::Value) -> bool {
        let string = match event.get(&self.column).expect("column exists") {
            serde_json::Value::String(s) => s,
            _ => unreachable!("right rule is set for right column type"),
        };

        if self.ignore_case.unwrap_or_default() {
            match self.operator {
                StringOperator::Exact => string.eq_ignore_ascii_case(&self.value),
                StringOperator::NotExact => !string.eq_ignore_ascii_case(&self.value),
                StringOperator::Contains => string
                    .to_ascii_lowercase()
                    .contains(&self.value.to_ascii_lowercase()),
                StringOperator::NotContains => !string
                    .to_ascii_lowercase()
                    .contains(&self.value.to_ascii_lowercase()),
            }
        } else {
            match self.operator {
                StringOperator::Exact => string.eq(&self.value),
                StringOperator::NotExact => !string.eq(&self.value),
                StringOperator::Contains => string.contains(&self.value),
                StringOperator::NotContains => !string.contains(&self.value),
            }
        }
    }
}
// Operator for comparing values

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
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

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum StringOperator {
    #[serde(alias = "=")]
    Exact,
    #[serde(alias = "!=")]
    NotExact,
    Contains,
    NotContains,
}

impl Default for StringOperator {
    fn default() -> Self {
        Self::Contains
    }
}
