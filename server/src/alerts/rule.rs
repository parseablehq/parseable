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
}

impl Rule {
    pub(super) fn resolves(&self, event: &serde_json::Value) -> bool {
        match self {
            Rule::Numeric(rule) => rule.resolves(event),
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
