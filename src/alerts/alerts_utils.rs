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

use std::fmt::Display;

use actix_web::Either;
use arrow_array::{Float64Array, Int64Array, RecordBatch};
use datafusion::{
    logical_expr::Literal,
    prelude::{Expr, lit},
};
use itertools::Itertools;
use tracing::{trace, warn};

use crate::{
    alerts::{AlertTrait, Conditions, LogicalOperator, WhereConfigOperator},
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

    let (result, final_value) = alert.eval_alert().await?;

    update_alert_state(alert, result, final_value).await
}

/// Extract time range from alert evaluation configuration
pub fn extract_time_range(eval_config: &super::EvalConfig) -> Result<TimeRange, AlertError> {
    let (start_time, end_time) = match eval_config {
        super::EvalConfig::RollingWindow(rolling_window) => (&rolling_window.eval_start, "now"),
    };

    TimeRange::parse_human_time(start_time, end_time)
        .map_err(|err| AlertError::CustomError(err.to_string()))
}

/// Execute the alert query based on the current mode and return the final value
pub async fn execute_alert_query(query: &str, time_range: &TimeRange) -> Result<f64, AlertError> {
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
async fn execute_local_query(query: &str, time_range: &TimeRange) -> Result<f64, AlertError> {
    let session_state = QUERY_SESSION.state();

    let tables = resolve_stream_names(query)?;
    create_streams_for_distributed(tables.clone())
        .await
        .map_err(|err| AlertError::CustomError(format!("Failed to create streams: {err}")))?;

    let raw_logical_plan = session_state.create_logical_plan(query).await?;
    let query = crate::query::Query {
        raw_logical_plan,
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

    Ok(get_final_value(records))
}

/// Execute alert query remotely (Prism mode)
async fn execute_remote_query(query: &str, time_range: &TimeRange) -> Result<f64, AlertError> {
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

    convert_result_to_f64(result_value)
}

/// Convert JSON result value to f64
fn convert_result_to_f64(result_value: serde_json::Value) -> Result<f64, AlertError> {
    warn!(result_value=?result_value);
    // due to the previous validations, we can be sure that we get an array of objects with just one entry
    // [{"countField": Number(1120.251)}]
    if let Some(array_val) = result_value.as_array()
        && let Some(object) = array_val[0].as_object()
    {
        let values = object.values().map(|v| v.as_f64().unwrap()).collect_vec();
        Ok(values[0])
    } else {
        Err(AlertError::CustomError(
            "Query result is not a number".to_string(),
        ))
    }
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
    final_res: bool,
    actual_value: f64,
) -> Result<(), AlertError> {
    let guard = ALERTS.write().await;
    let alerts = if let Some(alerts) = guard.as_ref() {
        alerts
    } else {
        return Err(AlertError::CustomError("No AlertManager set".into()));
    };

    if final_res {
        let message = format!(
            "Alert Triggered: {}\n\nThreshold: ({} {})\nCurrent Value: {}\nEvaluation Window: {} | Frequency: {}\n\nQuery:\n{}",
            alert.get_id(),
            alert.get_threshold_config().operator,
            alert.get_threshold_config().value,
            actual_value,
            alert.get_eval_window(),
            alert.get_eval_frequency(),
            alert.get_query()
        );

        alerts
            .update_state(*alert.get_id(), AlertState::Triggered, Some(message))
            .await
    } else if alerts
        .get_state(*alert.get_id())
        .await?
        .eq(&AlertState::Triggered)
    {
        alerts
            .update_state(*alert.get_id(), AlertState::Resolved, Some("".into()))
            .await
    } else {
        alerts
            .update_state(*alert.get_id(), AlertState::Resolved, None)
            .await
    }
}

fn get_final_value(records: Vec<RecordBatch>) -> f64 {
    trace!("records-\n{records:?}");

    if let Some(f) = records
        .first()
        .and_then(|batch| {
            trace!("batch.column(0)-\n{:?}", batch.column(0));
            batch.column(0).as_any().downcast_ref::<Float64Array>()
        })
        .map(|array| {
            trace!("array-\n{array:?}");
            array.value(0)
        })
    {
        f
    } else {
        records
            .first()
            .and_then(|batch| {
                trace!("batch.column(0)-\n{:?}", batch.column(0));
                batch.column(0).as_any().downcast_ref::<Int64Array>()
            })
            .map(|array| {
                trace!("array-\n{array:?}");
                array.value(0)
            })
            .unwrap_or_default() as f64
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
