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

use std::cmp::{max, min};

use arrow_schema::DataType;
use datafusion::scalar::ScalarValue;
use parquet::file::statistics::Statistics;

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct BoolType {
    pub min: bool,
    pub max: bool,
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct Float64Type {
    pub min: f64,
    pub max: f64,
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct Int64Type {
    pub min: i64,
    pub max: i64,
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct Utf8ViewType {
    pub min: String,
    pub max: String,
}

// Typed statistics are typed variant of statistics
// Currently all parquet types are casted down to these 4 types
// Binary types are assumed to be of valid Utf8
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub enum TypedStatistics {
    Bool(BoolType),
    Int(Int64Type),
    Float(Float64Type),
    String(Utf8ViewType),
}

impl TypedStatistics {
    pub fn update(self, other: Self) -> Self {
        match (self, other) {
            (TypedStatistics::Bool(this), TypedStatistics::Bool(other)) => {
                TypedStatistics::Bool(BoolType {
                    min: min(this.min, other.min),
                    max: max(this.max, other.max),
                })
            }
            (TypedStatistics::Float(this), TypedStatistics::Float(other)) => {
                TypedStatistics::Float(Float64Type {
                    min: this.min.min(other.min),
                    max: this.max.max(other.max),
                })
            }
            (TypedStatistics::Int(this), TypedStatistics::Int(other)) => {
                TypedStatistics::Int(Int64Type {
                    min: min(this.min, other.min),
                    max: max(this.max, other.max),
                })
            }
            (TypedStatistics::String(this), TypedStatistics::String(other)) => {
                TypedStatistics::String(Utf8ViewType {
                    min: min(this.min, other.min),
                    max: max(this.max, other.max),
                })
            }
            _ => panic!("Cannot update wrong types"),
        }
    }

    pub fn min_max_as_scalar(self, datatype: &DataType) -> Option<(ScalarValue, ScalarValue)> {
        let (min, max) = match (self, datatype) {
            (TypedStatistics::Bool(stats), DataType::Boolean) => (
                ScalarValue::Boolean(Some(stats.min)),
                ScalarValue::Boolean(Some(stats.max)),
            ),
            (TypedStatistics::Int(stats), DataType::Int32) => (
                ScalarValue::Int32(Some(stats.min as i32)),
                ScalarValue::Int32(Some(stats.max as i32)),
            ),
            (TypedStatistics::Int(stats), DataType::Int64) => (
                ScalarValue::Int64(Some(stats.min)),
                ScalarValue::Int64(Some(stats.max)),
            ),
            (TypedStatistics::Float(stats), DataType::Float32) => (
                ScalarValue::Float32(Some(stats.min as f32)),
                ScalarValue::Float32(Some(stats.max as f32)),
            ),
            (TypedStatistics::Float(stats), DataType::Float64) => (
                ScalarValue::Float64(Some(stats.min)),
                ScalarValue::Float64(Some(stats.max)),
            ),
            (TypedStatistics::String(stats), DataType::Utf8View) => (
                ScalarValue::Utf8View(Some(stats.min)),
                ScalarValue::Utf8View(Some(stats.max)),
            ),
            _ => {
                return None;
            }
        };

        Some((min, max))
    }
}

/// Column statistics are used to track statistics for a column in a given file.
/// This is similar to and derived from parquet statistics.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct Column {
    pub name: String,
    pub stats: Option<TypedStatistics>,
    pub uncompressed_size: u64,
    pub compressed_size: u64,
}

impl TryFrom<&Statistics> for TypedStatistics {
    type Error = parquet::errors::ParquetError;
    fn try_from(value: &Statistics) -> Result<Self, Self::Error> {
        if value.min_bytes_opt().is_none() || value.max_bytes_opt().is_none() {
            return Err(parquet::errors::ParquetError::General(
                "min max is not set".to_string(),
            ));
        }

        let res = match value {
            Statistics::Boolean(stats) => TypedStatistics::Bool(BoolType {
                min: *stats.min_opt().expect("Boolean stats min not set"),
                max: *stats.max_opt().expect("Boolean stats max not set"),
            }),
            Statistics::Int32(stats) => TypedStatistics::Int(Int64Type {
                min: *stats.min_opt().expect("Int32 stats min not set") as i64,
                max: *stats.max_opt().expect("Int32 stats max not set") as i64,
            }),
            Statistics::Int64(stats) => TypedStatistics::Int(Int64Type {
                min: *stats.min_opt().expect("Int64 stats min not set"),
                max: *stats.max_opt().expect("Int64 stats max not set"),
            }),
            Statistics::Int96(stats) => TypedStatistics::Int(Int64Type {
                min: stats.min_opt().expect("Int96 stats min not set").to_i64(),
                max: stats.max_opt().expect("Int96 stats max not set").to_i64(),
            }),
            Statistics::Float(stats) => TypedStatistics::Float(Float64Type {
                min: *stats.min_opt().expect("Float32 stats min not set") as f64,
                max: *stats.max_opt().expect("Float32 stats max not set") as f64,
            }),
            Statistics::Double(stats) => TypedStatistics::Float(Float64Type {
                min: *stats.min_opt().expect("Float64 stats min not set"),
                max: *stats.max_opt().expect("Float64 stats max not set"),
            }),
            Statistics::ByteArray(stats) => TypedStatistics::String(Utf8ViewType {
                min: stats
                    .min_opt()
                    .expect("Utf8 stats min not set")
                    .as_utf8()?
                    .to_owned(),
                max: stats
                    .max_opt()
                    .expect("Utf8 stats max not set")
                    .as_utf8()?
                    .to_owned(),
            }),
            Statistics::FixedLenByteArray(stats) => TypedStatistics::String(Utf8ViewType {
                min: stats
                    .min_opt()
                    .expect("Utf8 stats min not set")
                    .as_utf8()?
                    .to_owned(),
                max: stats
                    .max_opt()
                    .expect("Utf8 stats max not set")
                    .as_utf8()?
                    .to_owned(),
            }),
        };

        Ok(res)
    }
}
