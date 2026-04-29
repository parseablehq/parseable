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

use std::cmp::{max, min};

use arrow_schema::DataType;
use datafusion::scalar::ScalarValue;
use parquet::file::statistics::Statistics;
use tracing::warn;

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
pub struct Utf8Type {
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
    String(Utf8Type),
}

impl TypedStatistics {
    /// Variant name used in logs when the two operands disagree on type.
    fn variant_name(&self) -> &'static str {
        match self {
            TypedStatistics::Bool(_) => "bool",
            TypedStatistics::Int(_) => "int",
            TypedStatistics::Float(_) => "float",
            TypedStatistics::String(_) => "string",
        }
    }

    /// Merge two stat ranges. Returns `None` when the operands disagree on
    /// variant (which can happen if the same column was historically written
    /// under two different Arrow types — e.g. timestamp vs utf8 — in different
    /// parquet files). In that case the caller should drop stats for the
    /// column rather than treat them as authoritative; planners then fall
    /// back to scanning without min/max pushdown.
    pub fn update(self, other: Self) -> Option<Self> {
        match (self, other) {
            (TypedStatistics::Bool(this), TypedStatistics::Bool(other)) => {
                Some(TypedStatistics::Bool(BoolType {
                    min: min(this.min, other.min),
                    max: max(this.max, other.max),
                }))
            }
            (TypedStatistics::Float(this), TypedStatistics::Float(other)) => {
                Some(TypedStatistics::Float(Float64Type {
                    min: this.min.min(other.min),
                    max: this.max.max(other.max),
                }))
            }
            (TypedStatistics::Int(this), TypedStatistics::Int(other)) => {
                Some(TypedStatistics::Int(Int64Type {
                    min: min(this.min, other.min),
                    max: max(this.max, other.max),
                }))
            }
            (TypedStatistics::String(this), TypedStatistics::String(other)) => {
                Some(TypedStatistics::String(Utf8Type {
                    min: min(this.min, other.min),
                    max: max(this.max, other.max),
                }))
            }
            (this, other) => {
                warn!(
                    "Dropping incompatible column stats: existing={} new={}",
                    this.variant_name(),
                    other.variant_name()
                );
                None
            }
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
            (TypedStatistics::String(stats), DataType::Utf8) => (
                ScalarValue::Utf8(Some(stats.min)),
                ScalarValue::Utf8(Some(stats.max)),
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
                min: int96_to_i64_nanos(stats.min_opt().expect("Int96 stats min not set")),
                max: int96_to_i64_nanos(stats.max_opt().expect("Int96 stats max not set")),
            }),
            Statistics::Float(stats) => TypedStatistics::Float(Float64Type {
                min: *stats.min_opt().expect("Float32 stats min not set") as f64,
                max: *stats.max_opt().expect("Float32 stats max not set") as f64,
            }),
            Statistics::Double(stats) => TypedStatistics::Float(Float64Type {
                min: *stats.min_opt().expect("Float64 stats min not set"),
                max: *stats.max_opt().expect("Float64 stats max not set"),
            }),
            Statistics::ByteArray(stats) => TypedStatistics::String(Utf8Type {
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
            Statistics::FixedLenByteArray(stats) => TypedStatistics::String(Utf8Type {
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

// Int96 is a deprecated timestamp format used by legacy Impala files
// Convert to i64 nanoseconds since Unix epoch for statistics
fn int96_to_i64_nanos(int96: &parquet::data_type::Int96) -> i64 {
    const JULIAN_DAY_OF_EPOCH: i64 = 2_440_588; // Julian day for 1970-01-01
    const SECONDS_PER_DAY: i64 = 86_400;
    const NANOS_PER_SECOND: i64 = 1_000_000_000;

    // Extract nanoseconds from first 8 bytes (little-endian)
    let nanos_of_day = int96.data()[0] as i64 | ((int96.data()[1] as i64) << 32);

    // Extract Julian day from last 4 bytes
    let julian_day = int96.data()[2] as i64;

    // Convert to nanoseconds since Unix epoch
    let days_since_epoch = julian_day - JULIAN_DAY_OF_EPOCH;
    days_since_epoch * SECONDS_PER_DAY * NANOS_PER_SECOND + nanos_of_day
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn update_merges_compatible_int_stats() {
        let a = TypedStatistics::Int(Int64Type { min: 5, max: 10 });
        let b = TypedStatistics::Int(Int64Type { min: 1, max: 7 });
        let merged = a.update(b).expect("same variant should merge");
        match merged {
            TypedStatistics::Int(s) => {
                assert_eq!(s.min, 1);
                assert_eq!(s.max, 10);
            }
            _ => panic!("expected Int variant"),
        }
    }

    #[test]
    fn update_merges_compatible_string_stats() {
        let a = TypedStatistics::String(Utf8Type {
            min: "b".into(),
            max: "y".into(),
        });
        let b = TypedStatistics::String(Utf8Type {
            min: "a".into(),
            max: "z".into(),
        });
        let merged = a.update(b).expect("same variant should merge");
        match merged {
            TypedStatistics::String(s) => {
                assert_eq!(s.min, "a");
                assert_eq!(s.max, "z");
            }
            _ => panic!("expected String variant"),
        }
    }

    #[test]
    fn update_returns_none_on_type_mismatch_instead_of_panicking() {
        // Reproduces the "Cannot update wrong types" panic scenario: a column
        // that was historically written as Utf8 in some files and Timestamp(ms)
        // in others. Timestamps map to Int stats internally.
        let str_stats = TypedStatistics::String(Utf8Type {
            min: "2025-01-01".into(),
            max: "2025-12-31".into(),
        });
        let int_stats = TypedStatistics::Int(Int64Type {
            min: 1_700_000_000_000,
            max: 1_800_000_000_000,
        });

        assert!(str_stats.clone().update(int_stats.clone()).is_none());
        assert!(int_stats.update(str_stats).is_none());
    }

    #[test]
    fn update_returns_none_for_each_cross_variant_pair() {
        let bool_s = TypedStatistics::Bool(BoolType {
            min: false,
            max: true,
        });
        let int_s = TypedStatistics::Int(Int64Type { min: 0, max: 1 });
        let float_s = TypedStatistics::Float(Float64Type { min: 0.0, max: 1.0 });
        let str_s = TypedStatistics::String(Utf8Type {
            min: "a".into(),
            max: "b".into(),
        });

        assert!(bool_s.clone().update(int_s.clone()).is_none());
        assert!(int_s.clone().update(float_s.clone()).is_none());
        assert!(float_s.clone().update(str_s.clone()).is_none());
        assert!(str_s.update(bool_s).is_none());
    }
}
