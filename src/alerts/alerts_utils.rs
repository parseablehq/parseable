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

use arrow_array::{Float64Array, Int64Array, RecordBatch};
use datafusion::{
    functions_aggregate::{
        count::{count, count_distinct},
        expr_fn::avg,
        min_max::{max, min},
        sum::sum,
    },
    logical_expr::{BinaryExpr, Literal, Operator},
    prelude::{DataFrame, Expr, col, lit},
};
use tracing::trace;

use crate::{
    alerts::LogicalOperator,
    handlers::http::query::{create_streams_for_distributed, update_schema_when_distributed},
    parseable::PARSEABLE,
    query::{QUERY_SESSION, resolve_stream_names},
    utils::time::TimeRange,
};

use super::{
    ALERTS, AggregateConfig, AggregateFunction, AggregateResult, Aggregates, AlertConfig,
    AlertError, AlertOperator, AlertState, ConditionConfig, Conditions, WhereConfigOperator,
};

/// accept the alert
///
/// alert contains aggregate_config
///
/// aggregate_config contains the filters which need to be applied
///
/// iterate over each agg config, apply filters, the evaluate for that config
///
/// collect the results in the end
///
/// check whether notification needs to be triggered or not
pub async fn evaluate_alert(alert: &AlertConfig) -> Result<(), AlertError> {
    trace!("RUNNING EVAL TASK FOR- {alert:?}");

    let query = prepare_query(alert).await?;
    let select_query = alert.get_base_query();
    let base_df = execute_base_query(&query, &select_query).await?;
    let agg_results = evaluate_aggregates(&alert.aggregates, &base_df).await?;
    let final_res = calculate_final_result(&alert.aggregates, &agg_results);

    update_alert_state(alert, final_res, &agg_results).await?;
    Ok(())
}

async fn prepare_query(alert: &AlertConfig) -> Result<crate::query::Query, AlertError> {
    let (start_time, end_time) = match &alert.eval_config {
        super::EvalConfig::RollingWindow(rolling_window) => (&rolling_window.eval_start, "now"),
    };

    let session_state = QUERY_SESSION.state();
    let select_query = alert.get_base_query();
    let time_range = TimeRange::parse_human_time(start_time, end_time)
        .map_err(|err| AlertError::CustomError(err.to_string()))?;

    let tables = resolve_stream_names(&select_query)?;
    //check or load streams in memory
    create_streams_for_distributed(tables.clone())
        .await
        .map_err(|err| AlertError::CustomError(format!("Failed to create streams: {err}")))?;
    let raw_logical_plan = session_state.create_logical_plan(&select_query).await?;
    Ok(crate::query::Query {
        raw_logical_plan,
        time_range,
        filter_tag: None,
    })
}

async fn execute_base_query(
    query: &crate::query::Query,
    original_query: &str,
) -> Result<DataFrame, AlertError> {
    let streams = resolve_stream_names(original_query)?;
    let stream_name = streams.first().ok_or_else(|| {
        AlertError::CustomError(format!("Table name not found in query- {original_query}"))
    })?;
    update_schema_when_distributed(&streams)
        .await
        .map_err(|err| {
            AlertError::CustomError(format!(
                "Failed to update schema for distributed streams: {err}"
            ))
        })?;
    let time_partition = PARSEABLE.get_stream(stream_name)?.get_time_partition();
    query
        .get_dataframe(time_partition.as_ref())
        .await
        .map_err(|err| AlertError::CustomError(err.to_string()))
}

async fn evaluate_aggregates(
    agg_config: &Aggregates,
    base_df: &DataFrame,
) -> Result<Vec<AggregateResult>, AlertError> {
    let agg_filter_exprs = get_exprs(agg_config);
    let mut results = Vec::new();

    let conditions = match &agg_config.operator {
        Some(_) => &agg_config.aggregate_config[0..2],
        None => &agg_config.aggregate_config[0..1],
    };

    for ((agg_expr, filter), agg) in agg_filter_exprs.into_iter().zip(conditions) {
        let result = evaluate_single_aggregate(base_df, filter, agg_expr, agg).await?;
        results.push(result);
    }

    Ok(results)
}

async fn evaluate_single_aggregate(
    base_df: &DataFrame,
    filter: Option<Expr>,
    agg_expr: Expr,
    agg: &AggregateConfig,
) -> Result<AggregateResult, AlertError> {
    let filtered_df = if let Some(filter) = filter {
        base_df.clone().filter(filter)?
    } else {
        base_df.clone()
    };

    let aggregated_rows = filtered_df
        .aggregate(vec![], vec![agg_expr])?
        .collect()
        .await?;

    let final_value = get_final_value(aggregated_rows);
    let result = evaluate_condition(&agg.operator, final_value, agg.value);

    let message = if result {
        agg.conditions
            .as_ref()
            .map(|config| config.generate_filter_message())
            .or(None)
    } else {
        None
    };

    Ok(AggregateResult {
        result,
        message,
        config: agg.clone(),
        value: final_value,
    })
}

fn evaluate_condition(operator: &AlertOperator, actual: f64, expected: f64) -> bool {
    match operator {
        AlertOperator::GreaterThan => actual > expected,
        AlertOperator::LessThan => actual < expected,
        AlertOperator::Equal => actual == expected,
        AlertOperator::NotEqual => actual != expected,
        AlertOperator::GreaterThanOrEqual => actual >= expected,
        AlertOperator::LessThanOrEqual => actual <= expected,
    }
}

fn calculate_final_result(agg_config: &Aggregates, results: &[AggregateResult]) -> bool {
    match &agg_config.operator {
        Some(LogicalOperator::And) => results.iter().all(|r| r.result),
        Some(LogicalOperator::Or) => results.iter().any(|r| r.result),
        None => results.first().is_some_and(|r| r.result),
    }
}

async fn update_alert_state(
    alert: &AlertConfig,
    final_res: bool,
    agg_results: &[AggregateResult],
) -> Result<(), AlertError> {
    if final_res {
        let message = format_alert_message(agg_results);
        let message = format!(
            "{message}\nEvaluation Window: {}\nEvaluation Frequency: {}m",
            alert.get_eval_window(),
            alert.get_eval_frequency()
        );
        ALERTS
            .update_state(alert.id, AlertState::Triggered, Some(message))
            .await
    } else if ALERTS.get_state(alert.id).await?.eq(&AlertState::Triggered) {
        ALERTS
            .update_state(alert.id, AlertState::Resolved, Some("".into()))
            .await
    } else {
        ALERTS
            .update_state(alert.id, AlertState::Resolved, None)
            .await
    }
}

fn format_alert_message(agg_results: &[AggregateResult]) -> String {
    let mut message = String::default();
    for result in agg_results {
        if let Some(msg) = &result.message {
            message.extend([format!(
                "\nCondition: {}({}) WHERE ({}) {} {}\nActualValue: {}\n",
                result.config.aggregate_function,
                result.config.column,
                msg,
                result.config.operator,
                result.config.value,
                result.value
            )]);
        } else {
            message.extend([format!(
                "\nCondition: {}({}) {} {}\nActualValue: {}\n",
                result.config.aggregate_function,
                result.config.column,
                result.config.operator,
                result.config.value,
                result.value
            )]);
        }
    }
    message
}

fn get_final_value(aggregated_rows: Vec<RecordBatch>) -> f64 {
    trace!("aggregated_rows-\n{aggregated_rows:?}");

    if let Some(f) = aggregated_rows
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
        aggregated_rows
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

/// This function accepts aggregate_config and
/// returns a tuple of (aggregate expressions, filter expressions)
///
/// It calls get_filter_expr() to get filter expressions
fn get_exprs(aggregate_config: &Aggregates) -> Vec<(Expr, Option<Expr>)> {
    let mut agg_expr = Vec::new();

    match &aggregate_config.operator {
        Some(op) => match op {
            LogicalOperator::And | LogicalOperator::Or => {
                let agg1 = &aggregate_config.aggregate_config[0];
                let agg2 = &aggregate_config.aggregate_config[1];

                for agg in [agg1, agg2] {
                    let filter_expr = if let Some(where_clause) = &agg.conditions {
                        let fe = get_filter_expr(where_clause);

                        trace!("filter_expr-\n{fe:?}");

                        Some(fe)
                    } else {
                        None
                    };

                    let e = match_aggregate_operation(agg);
                    agg_expr.push((e, filter_expr));
                }
            }
        },
        None => {
            let agg = &aggregate_config.aggregate_config[0];

            let filter_expr = if let Some(where_clause) = &agg.conditions {
                let fe = get_filter_expr(where_clause);

                trace!("filter_expr-\n{fe:?}");

                Some(fe)
            } else {
                None
            };

            let e = match_aggregate_operation(agg);
            agg_expr.push((e, filter_expr));
        }
    }
    agg_expr
}

fn get_filter_expr(where_clause: &Conditions) -> Expr {
    match &where_clause.operator {
        Some(op) => match op {
            LogicalOperator::And => {
                let mut expr = Expr::Literal(datafusion::scalar::ScalarValue::Boolean(Some(true)));

                let expr1 = &where_clause.condition_config[0];
                let expr2 = &where_clause.condition_config[1];

                for e in [expr1, expr2] {
                    let ex = match_alert_operator(e);
                    expr = expr.and(ex);
                }
                expr
            }
            LogicalOperator::Or => {
                let mut expr = Expr::Literal(datafusion::scalar::ScalarValue::Boolean(Some(false)));

                let expr1 = &where_clause.condition_config[0];
                let expr2 = &where_clause.condition_config[1];

                for e in [expr1, expr2] {
                    let ex = match_alert_operator(e);
                    expr = expr.or(ex);
                }
                expr
            }
        },
        None => {
            let expr = &where_clause.condition_config[0];
            match_alert_operator(expr)
        }
    }
}

pub fn get_filter_string(where_clause: &Conditions) -> Result<String, String> {
    match &where_clause.operator {
        Some(op) => match op {
            LogicalOperator::And => {
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
                                let value = match NumberOrString::from_string(value.to_owned()) {
                                    NumberOrString::Number(val) => format!("{val}"),
                                    NumberOrString::String(val) => {
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

fn match_alert_operator(expr: &ConditionConfig) -> Expr {
    // the form accepts value as a string
    // if it can be parsed as a number, then parse it
    // else keep it as a string
    if expr.value.as_ref().is_some_and(|v| !v.is_empty()) {
        let string_value = expr
            .value
            .as_ref()
            .unwrap()
            .replace("'", "\\'")
            .replace('%', "\\%")
            .replace('_', "\\_");
        let value = NumberOrString::from_string(string_value.clone());

        // for maintaining column case
        let column = format!(r#""{}""#, expr.column);
        match expr.operator {
            WhereConfigOperator::Equal => col(column).eq(lit(value)),
            WhereConfigOperator::NotEqual => col(column).not_eq(lit(value)),
            WhereConfigOperator::LessThan => col(column).lt(lit(value)),
            WhereConfigOperator::GreaterThan => col(column).gt(lit(value)),
            WhereConfigOperator::LessThanOrEqual => col(column).lt_eq(lit(value)),
            WhereConfigOperator::GreaterThanOrEqual => col(column).gt_eq(lit(value)),
            WhereConfigOperator::ILike => col(column).ilike(lit(string_value)),
            WhereConfigOperator::Contains => col(column).like(lit(format!("%{string_value}%"))),
            WhereConfigOperator::BeginsWith => Expr::BinaryExpr(BinaryExpr::new(
                Box::new(col(column)),
                Operator::RegexIMatch,
                Box::new(lit(format!("^{string_value}"))),
            )),
            WhereConfigOperator::EndsWith => Expr::BinaryExpr(BinaryExpr::new(
                Box::new(col(column)),
                Operator::RegexIMatch,
                Box::new(lit(format!("{string_value}$"))),
            )),
            WhereConfigOperator::DoesNotContain => col(column).not_ilike(lit(string_value)),
            WhereConfigOperator::DoesNotBeginWith => Expr::BinaryExpr(BinaryExpr::new(
                Box::new(col(column)),
                Operator::RegexNotIMatch,
                Box::new(lit(format!("^{string_value}"))),
            )),
            WhereConfigOperator::DoesNotEndWith => Expr::BinaryExpr(BinaryExpr::new(
                Box::new(col(column)),
                Operator::RegexNotIMatch,
                Box::new(lit(format!("{string_value}$"))),
            )),
            _ => unreachable!(
                "value must not be null for operators other than `is null` and `is not null`. Should've been caught in validation"
            ),
        }
    } else {
        // for maintaining column case
        let column = format!(r#""{}""#, expr.column);
        match expr.operator {
            WhereConfigOperator::IsNull => col(column).is_null(),
            WhereConfigOperator::IsNotNull => col(column).is_not_null(),
            _ => unreachable!(
                "value must be null for `is null` and `is not null`. Should've been caught in validation"
            ),
        }
    }
}

fn match_aggregate_operation(agg: &AggregateConfig) -> Expr {
    // for maintaining column case
    let column = format!(r#""{}""#, agg.column);
    match agg.aggregate_function {
        AggregateFunction::Avg => avg(col(column)),
        AggregateFunction::CountDistinct => count_distinct(col(column)),
        AggregateFunction::Count => count(col(column)),
        AggregateFunction::Min => min(col(column)),
        AggregateFunction::Max => max(col(column)),
        AggregateFunction::Sum => sum(col(column)),
    }
}

enum NumberOrString {
    Number(f64),
    String(String),
}

impl Literal for NumberOrString {
    fn lit(&self) -> Expr {
        match self {
            NumberOrString::Number(expr) => lit(*expr),
            NumberOrString::String(expr) => lit(expr.clone()),
        }
    }
}
impl NumberOrString {
    fn from_string(value: String) -> Self {
        if let Ok(num) = value.parse::<f64>() {
            NumberOrString::Number(num)
        } else {
            NumberOrString::String(value)
        }
    }
}

impl Display for NumberOrString {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            NumberOrString::Number(v) => write!(f, "{v}"),
            NumberOrString::String(v) => write!(f, "{v}"),
        }
    }
}
