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

use std::{collections::HashMap, fmt::Display};

use actix_web::Either;
use arrow_array::{Array, Float64Array, Int64Array, RecordBatch};
use datafusion::{
    logical_expr::{Literal, LogicalPlan},
    prelude::{Expr, lit},
};
use tracing::trace;

use crate::{
    alerts::{
        AlertTrait, LogicalOperator, WhereConfigOperator,
        alert_structs::{AlertQueryResult, Conditions, GroupResult},
        extract_aggregate_aliases,
    },
    handlers::http::{
        cluster::send_query_request,
        query::{Query, create_streams_for_distributed},
    },
    option::Mode,
    parseable::PARSEABLE,
    query::{QUERY_SESSION, execute, resolve_stream_names},
    utils::time::TimeRange,
};

use super::{ALERTS, AlertError, AlertOperator, AlertState};

/// accept the alert
///
/// alert contains query and the threshold_config
///
/// execute the query and compute the output of the query
///
/// compare the output with the threshold_config
///
/// collect the results in the end
///
/// check whether notification needs to be triggered or not
pub async fn evaluate_alert(alert: &dyn AlertTrait) -> Result<(), AlertError> {
    trace!("RUNNING EVAL TASK FOR- {alert:?}");

    let message = alert.eval_alert().await?;

    update_alert_state(alert, message).await
}

/// Extract time range from alert evaluation configuration
pub fn extract_time_range(eval_config: &super::EvalConfig) -> Result<TimeRange, AlertError> {
    let (start_time, end_time) = match eval_config {
        super::EvalConfig::RollingWindow(rolling_window) => (&rolling_window.eval_start, "now"),
    };

    TimeRange::parse_human_time(start_time, end_time)
        .map_err(|err| AlertError::CustomError(err.to_string()))
}

/// Execute the alert query based on the current mode and return structured group results
pub async fn execute_alert_query(
    query: &str,
    time_range: &TimeRange,
) -> Result<AlertQueryResult, AlertError> {
    match PARSEABLE.options.mode {
        Mode::All | Mode::Query => execute_local_query(query, time_range).await,
        Mode::Prism => execute_remote_query(query, time_range).await,
        _ => Err(AlertError::CustomError(format!(
            "Unsupported mode '{:?}' for alert evaluation",
            PARSEABLE.options.mode
        ))),
    }
}

/// Execute alert query locally (Query/All mode)
async fn execute_local_query(
    query: &str,
    time_range: &TimeRange,
) -> Result<AlertQueryResult, AlertError> {
    let session_state = QUERY_SESSION.state();

    let tables = resolve_stream_names(query)?;
    create_streams_for_distributed(tables.clone())
        .await
        .map_err(|err| AlertError::CustomError(format!("Failed to create streams: {err}")))?;

    let raw_logical_plan = session_state.create_logical_plan(query).await?;
    let query = crate::query::Query {
        raw_logical_plan: raw_logical_plan.clone(),
        time_range: time_range.clone(),
        filter_tag: None,
    };

    let (records, _) = execute(query, &tables[0], false)
        .await
        .map_err(|err| AlertError::CustomError(format!("Failed to execute query: {err}")))?;

    let records = match records {
        Either::Left(rbs) => rbs,
        Either::Right(_) => {
            return Err(AlertError::CustomError(
                "Query returned no results".to_string(),
            ));
        }
    };

    Ok(extract_group_results(records, raw_logical_plan))
}

/// Execute alert query remotely (Prism mode)
async fn execute_remote_query(
    query: &str,
    time_range: &TimeRange,
) -> Result<AlertQueryResult, AlertError> {
    let session_state = QUERY_SESSION.state();
    let raw_logical_plan = session_state.create_logical_plan(query).await?;

    let query_request = Query {
        query: query.to_string(),
        start_time: time_range.start.to_rfc3339(),
        end_time: time_range.end.to_rfc3339(),
        streaming: false,
        send_null: false,
        fields: false,
        filter_tags: None,
    };

    let (result_value, _) = send_query_request(&query_request)
        .await
        .map_err(|err| AlertError::CustomError(format!("Failed to send query request: {err}")))?;

    convert_result_to_group_results(result_value, raw_logical_plan)
}

/// Convert JSON result value to AlertQueryResult
/// Handles both simple queries and GROUP BY queries with multiple rows
fn convert_result_to_group_results(
    result_value: serde_json::Value,
    plan: LogicalPlan,
) -> Result<AlertQueryResult, AlertError> {
    let array_val = result_value
        .as_array()
        .ok_or_else(|| AlertError::CustomError("Expected array in query result".to_string()))?;

    let aggregate_aliases = extract_aggregate_aliases(&plan);

    if array_val.is_empty() || aggregate_aliases.is_empty() {
        return Ok(AlertQueryResult {
            groups: vec![],
            is_simple_query: true,
        });
    }

    // take the first entry and extract the column name / alias
    let (agg_condition, alias) = &aggregate_aliases[0];

    let aggregate_key = if let Some(alias) = alias {
        alias
    } else {
        agg_condition
    };

    // Find the aggregate column from the first row
    let first_row = array_val[0]
        .as_object()
        .ok_or_else(|| AlertError::CustomError("Expected object in query result".to_string()))?;

    let is_simple_query = first_row.len() == 1;
    let mut groups = Vec::new();

    // Process each row as a separate group
    for row in array_val {
        if let Some(object) = row.as_object() {
            let mut group_values = HashMap::new();
            let mut aggregate_value = 0.0;

            for (key, value) in object {
                if key == aggregate_key {
                    aggregate_value = value.as_f64().ok_or_else(|| {
                        AlertError::CustomError(format!(
                            "Non-numeric value found in aggregate column '{}'",
                            aggregate_key
                        ))
                    })?;
                } else {
                    // This is a GROUP BY column
                    group_values
                        .insert(key.clone(), value.to_string().trim_matches('"').to_string());
                }
            }

            groups.push(GroupResult {
                group_values,
                aggregate_value,
            });
        }
    }

    Ok(AlertQueryResult {
        groups,
        is_simple_query,
    })
}

/// Extract numeric value from an Arrow array at the given row index
fn extract_numeric_value(column: &dyn Array, row_index: usize) -> f64 {
    if let Some(float_array) = column.as_any().downcast_ref::<Float64Array>() {
        if !float_array.is_null(row_index) {
            return float_array.value(row_index);
        }
    } else if let Some(int_array) = column.as_any().downcast_ref::<Int64Array>()
        && !int_array.is_null(row_index)
    {
        return int_array.value(row_index) as f64;
    }
    0.0
}

/// Extract string value from an Arrow array at the given row index
fn extract_string_value(column: &dyn Array, row_index: usize) -> String {
    use arrow_array::StringArray;

    if let Some(string_array) = column.as_any().downcast_ref::<StringArray>() {
        if !string_array.is_null(row_index) {
            return string_array.value(row_index).to_string();
        }
    } else if let Some(int_array) = column.as_any().downcast_ref::<Int64Array>() {
        if !int_array.is_null(row_index) {
            return int_array.value(row_index).to_string();
        }
    } else if let Some(float_array) = column.as_any().downcast_ref::<Float64Array>()
        && !float_array.is_null(row_index)
    {
        return float_array.value(row_index).to_string();
    }
    "null".to_string()
}

pub fn evaluate_condition(operator: &AlertOperator, actual: f64, expected: f64) -> bool {
    match operator {
        AlertOperator::GreaterThan => actual > expected,
        AlertOperator::LessThan => actual < expected,
        AlertOperator::Equal => actual == expected,
        AlertOperator::NotEqual => actual != expected,
        AlertOperator::GreaterThanOrEqual => actual >= expected,
        AlertOperator::LessThanOrEqual => actual <= expected,
    }
}

async fn update_alert_state(
    alert: &dyn AlertTrait,
    message: Option<String>,
) -> Result<(), AlertError> {
    // Get the alert manager reference while holding the lock briefly
    let alerts = {
        let guard = ALERTS.read().await;
        if let Some(alerts) = guard.as_ref() {
            alerts.clone()
        } else {
            return Err(AlertError::CustomError("No AlertManager set".into()));
        }
    };

    // Now perform the state update
    if let Some(msg) = message {
        alerts
            .update_state(*alert.get_id(), AlertState::Triggered, Some(msg))
            .await
    } else if alerts
        .get_state(*alert.get_id())
        .await?
        .eq(&AlertState::Triggered)
    {
        alerts
            .update_state(*alert.get_id(), AlertState::NotTriggered, Some("".into()))
            .await
    } else {
        alerts
            .update_state(*alert.get_id(), AlertState::NotTriggered, None)
            .await
    }
}

/// Extract group results from record batches, supporting both simple and GROUP BY queries
fn extract_group_results(records: Vec<RecordBatch>, plan: LogicalPlan) -> AlertQueryResult {
    trace!("records-\n{records:?}");

    let aggregate_aliases = extract_aggregate_aliases(&plan);

    // since there is going to be only one aggregate, we'll check if it is empty
    if aggregate_aliases.is_empty() || records.is_empty() {
        return AlertQueryResult {
            groups: vec![],
            is_simple_query: true,
        };
    }

    // take the first entry and extract the column name / alias
    let (agg_condition, alias) = &aggregate_aliases[0];

    let alias = if let Some(alias) = alias {
        alias
    } else {
        agg_condition
    };

    let first_batch = &records[0];
    let schema = first_batch.schema();

    // Determine if this is a simple query (no GROUP BY) or a grouped query
    let is_simple_query = schema.fields().len() == 1;

    let mut groups = Vec::new();

    for batch in &records {
        for row_index in 0..batch.num_rows() {
            let mut group_values = HashMap::new();
            let mut aggregate_value = 0.0;

            // Extract values for each column
            for (col_index, field) in schema.fields().iter().enumerate() {
                let column = batch.column(col_index);
                if field.name().eq(alias) {
                    aggregate_value = extract_numeric_value(column, row_index)
                } else {
                    // This is a GROUP BY column
                    let value = extract_string_value(column, row_index);
                    group_values.insert(field.name().clone(), value);
                }
            }

            groups.push(GroupResult {
                group_values,
                aggregate_value,
            });
        }
    }

    AlertQueryResult {
        groups,
        is_simple_query,
    }
}

pub fn get_filter_string(where_clause: &Conditions) -> Result<String, String> {
    match &where_clause.operator {
        Some(op) => match op {
            &LogicalOperator::And => {
                let mut exprs = vec![];
                for condition in &where_clause.condition_config {
                    if condition.value.as_ref().is_some_and(|v| !v.is_empty()) {
                        // ad-hoc error check in case value is some and operator is either `is null` or `is not null`
                        if condition.operator.eq(&WhereConfigOperator::IsNull)
                            || condition.operator.eq(&WhereConfigOperator::IsNotNull)
                        {
                            return Err("value must be null when operator is either `is null` or `is not null`"
                                .into());
                        }

                        let value = condition.value.as_ref().unwrap();

                        let operator_and_value = match condition.operator {
                            WhereConfigOperator::Contains => {
                                let escaped_value = value
                                    .replace("'", "\\'")
                                    .replace('%', "\\%")
                                    .replace('_', "\\_");
                                format!("LIKE '%{escaped_value}%' ESCAPE '\\'")
                            }
                            WhereConfigOperator::DoesNotContain => {
                                let escaped_value = value
                                    .replace("'", "\\'")
                                    .replace('%', "\\%")
                                    .replace('_', "\\_");
                                format!("NOT LIKE '%{escaped_value}%' ESCAPE '\\'")
                            }
                            WhereConfigOperator::ILike => {
                                let escaped_value = value
                                    .replace("'", "\\'")
                                    .replace('%', "\\%")
                                    .replace('_', "\\_");
                                format!("ILIKE '%{escaped_value}%' ESCAPE '\\'")
                            }
                            WhereConfigOperator::BeginsWith => {
                                let escaped_value = value
                                    .replace("'", "\\'")
                                    .replace('%', "\\%")
                                    .replace('_', "\\_");
                                format!("LIKE '{escaped_value}%' ESCAPE '\\'")
                            }
                            WhereConfigOperator::DoesNotBeginWith => {
                                let escaped_value = value
                                    .replace("'", "\\'")
                                    .replace('%', "\\%")
                                    .replace('_', "\\_");
                                format!("NOT LIKE '{escaped_value}%' ESCAPE '\\'")
                            }
                            WhereConfigOperator::EndsWith => {
                                let escaped_value = value
                                    .replace("'", "\\'")
                                    .replace('%', "\\%")
                                    .replace('_', "\\_");
                                format!("LIKE '%{escaped_value}' ESCAPE '\\'")
                            }
                            WhereConfigOperator::DoesNotEndWith => {
                                let escaped_value = value
                                    .replace("'", "\\'")
                                    .replace('%', "\\%")
                                    .replace('_', "\\_");
                                format!("NOT LIKE '%{escaped_value}' ESCAPE '\\'")
                            }
                            _ => {
                                let value = match ValueType::from_string(value.to_owned()) {
                                    ValueType::Number(val) => format!("{val}"),
                                    ValueType::Boolean(val) => format!("{val}"),
                                    ValueType::String(val) => {
                                        format!("'{val}'")
                                    }
                                };
                                format!("{} {}", condition.operator, value)
                            }
                        };
                        exprs.push(format!("\"{}\" {}", condition.column, operator_and_value))
                    } else {
                        exprs.push(format!("\"{}\" {}", condition.column, condition.operator))
                    }
                }

                Ok(exprs.join(" AND "))
            }
            _ => Err(String::from("Invalid option 'or', only 'and' is supported")),
        },
        _ => Err(String::from(
            "Invalid option 'null', only 'and' is supported",
        )),
    }
}

enum ValueType {
    Number(f64),
    String(String),
    Boolean(bool),
}

impl Literal for ValueType {
    fn lit(&self) -> Expr {
        match self {
            ValueType::Number(expr) => lit(*expr),
            ValueType::String(expr) => lit(expr.clone()),
            ValueType::Boolean(expr) => lit(*expr),
        }
    }
}
impl ValueType {
    fn from_string(value: String) -> Self {
        if let Ok(num) = value.parse::<f64>() {
            ValueType::Number(num)
        } else if let Ok(boolean) = value.parse::<bool>() {
            ValueType::Boolean(boolean)
        } else {
            ValueType::String(value)
        }
    }
}

impl Display for ValueType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ValueType::Number(v) => write!(f, "{v}"),
            ValueType::String(v) => write!(f, "{v}"),
            ValueType::Boolean(v) => write!(f, "{v}"),
        }
    }
}
