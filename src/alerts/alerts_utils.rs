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

use arrow_array::{Float64Array, Int64Array, RecordBatch};
use datafusion::{
    common::tree_node::TreeNode,
    functions_aggregate::{
        count::count,
        expr_fn::avg,
        min_max::{max, min},
        sum::sum,
    },
    prelude::{col, lit, Expr},
};
use tracing::trace;

use crate::{
    query::{TableScanVisitor, QUERY_SESSION},
    rbac::{
        map::SessionKey,
        role::{Action, Permission},
        Users,
    },
    utils::time::TimeRange,
};

use super::{
    AggregateOperation, Aggregations, AlertConfig, AlertError, AlertOperator, AlertState,
    Conditions, ALERTS,
};

async fn get_tables_from_query(query: &str) -> Result<TableScanVisitor, AlertError> {
    let session_state = QUERY_SESSION.state();
    let raw_logical_plan = session_state.create_logical_plan(query).await?;

    let mut visitor = TableScanVisitor::default();
    let _ = raw_logical_plan.visit(&mut visitor);
    Ok(visitor)
}

pub async fn user_auth_for_query(session_key: &SessionKey, query: &str) -> Result<(), AlertError> {
    let tables = get_tables_from_query(query).await?;
    let permissions = Users.get_permissions(session_key);

    for table_name in tables.into_inner().iter() {
        let mut authorized = false;

        // in permission check if user can run query on the stream.
        // also while iterating add any filter tags for this stream
        for permission in permissions.iter() {
            match permission {
                Permission::Stream(Action::All, _) => {
                    authorized = true;
                    break;
                }
                Permission::StreamWithTag(Action::Query, ref stream, _)
                    if stream == table_name || stream == "*" =>
                {
                    authorized = true;
                }
                _ => (),
            }
        }

        if !authorized {
            return Err(AlertError::Unauthorized);
        }
    }

    Ok(())
}

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

    let (start_time, end_time) = match &alert.eval_type {
        super::EvalConfig::RollingWindow(rolling_window) => {
            (&rolling_window.eval_start, &rolling_window.eval_end)
        }
    };

    let session_state = QUERY_SESSION.state();
    let raw_logical_plan = session_state.create_logical_plan(&alert.query).await?;

    // TODO: Filter tags should be taken care of!!!
    let time_range = TimeRange::parse_human_time(start_time, end_time)
        .map_err(|err| AlertError::CustomError(err.to_string()))?;

    let query = crate::query::Query {
        raw_logical_plan,
        time_range,
        filter_tag: None,
    };

    // for now proceed in a similar fashion as we do in query
    // TODO: in case of multiple table query does the selection of time partition make a difference? (especially when the tables don't have overlapping data)
    let stream_name = if let Some(stream_name) = query.first_table_name() {
        stream_name
    } else {
        return Err(AlertError::CustomError(format!(
            "Table name not found in query- {}",
            alert.query
        )));
    };

    let base_df = query
        .get_dataframe(stream_name)
        .await
        .map_err(|err| AlertError::CustomError(err.to_string()))?;

    let mut agg_results = vec![];

    let agg_filter_exprs = get_exprs(&alert.aggregate_config);

    let final_res = match &alert.aggregate_config {
        crate::alerts::Aggregations::AND((agg1, agg2))
        | crate::alerts::Aggregations::OR((agg1, agg2)) => {
            for ((agg_expr, filter), agg) in agg_filter_exprs.into_iter().zip([agg1, agg2]) {
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

                // now compare
                let res = match &agg.operator {
                    AlertOperator::GreaterThan => final_value > agg.value,
                    AlertOperator::LessThan => final_value < agg.value,
                    AlertOperator::EqualTo => final_value == agg.value,
                    AlertOperator::NotEqualTo => final_value != agg.value,
                    AlertOperator::GreaterThanEqualTo => final_value >= agg.value,
                    AlertOperator::LessThanEqualTo => final_value <= agg.value,
                    _ => unreachable!(),
                };

                let message = if res {
                    if agg.condition_config.is_some() {
                        Some(
                            agg.condition_config
                                .as_ref()
                                .unwrap()
                                .generate_filter_message(),
                        )
                    } else {
                        Some(String::default())
                    }
                } else {
                    None
                };

                agg_results.push((res, message, agg, final_value));
            }

            let res = match &alert.aggregate_config {
                Aggregations::AND(_) => agg_results.iter().all(|(r, _, _, _)| *r),
                Aggregations::OR(_) => agg_results.iter().any(|(r, _, _, _)| *r),
                _ => unreachable!(),
            };

            res
        }
        crate::alerts::Aggregations::Single(agg) => {
            let (agg_expr, filter) = &agg_filter_exprs[0];
            let filtered_df = if let Some(filter) = filter {
                base_df.clone().filter(filter.clone())?
            } else {
                base_df
            };

            let aggregated_rows = filtered_df
                .aggregate(vec![], vec![agg_expr.clone()])?
                .collect()
                .await?;

            let final_value = get_final_value(aggregated_rows);

            // now compare
            let res = match &agg.operator {
                AlertOperator::GreaterThan => final_value > agg.value,
                AlertOperator::LessThan => final_value < agg.value,
                AlertOperator::EqualTo => final_value == agg.value,
                AlertOperator::NotEqualTo => final_value != agg.value,
                AlertOperator::GreaterThanEqualTo => final_value >= agg.value,
                AlertOperator::LessThanEqualTo => final_value <= agg.value,
                _ => unreachable!(),
            };

            let message = if res {
                if agg.condition_config.is_some() {
                    Some(
                        agg.condition_config
                            .as_ref()
                            .unwrap()
                            .generate_filter_message(),
                    )
                } else {
                    Some(String::default())
                }
            } else {
                None
            };

            agg_results.push((res, message, agg, final_value));

            res
        }
    };

    if final_res {
        trace!("ALERT!!!!!!");

        let mut message = String::default();
        for (_, filter_msg, agg_config, final_value) in agg_results {
            if let Some(msg) = filter_msg {
                message.extend([format!(
                    "|{}({}) WHERE ({}) {} {} (ActualValue: {})|",
                    agg_config.agg,
                    agg_config.column,
                    msg,
                    agg_config.operator,
                    agg_config.value,
                    final_value
                )]);
            } else {
                message.extend([format!(
                    "|{}({}) {} {} (ActualValue: {})",
                    agg_config.agg,
                    agg_config.column,
                    agg_config.operator,
                    agg_config.value,
                    final_value
                )]);
            }
        }

        // let outbound_message = format!("AlertName: {}, Triggered TimeStamp: {}, Severity: {}, Message: {}",alert.title, Utc::now().to_rfc3339(), alert.severity, message);

        // update state
        ALERTS
            .update_state(&alert.id.to_string(), AlertState::Triggered, Some(message))
            .await?;
    } else if alert.state.eq(&AlertState::Triggered) {
        ALERTS
            .update_state(&alert.id.to_string(), AlertState::Resolved, Some("".into()))
            .await?;
    } else {
        ALERTS
            .update_state(&alert.id.to_string(), AlertState::Resolved, None)
            .await?;
    }

    Ok(())
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
fn get_exprs(aggregate_config: &Aggregations) -> Vec<(Expr, Option<Expr>)> {
    let mut agg_expr = Vec::new();

    match aggregate_config {
        Aggregations::OR((agg1, agg2)) | Aggregations::AND((agg1, agg2)) => {
            for agg in [agg1, agg2] {
                let filter_expr = if let Some(where_clause) = &agg.condition_config {
                    let fe = get_filter_expr(where_clause);

                    trace!("filter_expr-\n{fe:?}");

                    Some(fe)
                } else {
                    None
                };

                let e = match agg.agg {
                    AggregateOperation::Avg => avg(col(&agg.column)),
                    AggregateOperation::Count => count(col(&agg.column)),
                    AggregateOperation::Min => min(col(&agg.column)),
                    AggregateOperation::Max => max(col(&agg.column)),
                    AggregateOperation::Sum => sum(col(&agg.column)),
                };
                agg_expr.push((e, filter_expr));
            }
        }
        Aggregations::Single(agg) => {
            let filter_expr = if let Some(where_clause) = &agg.condition_config {
                let fe = get_filter_expr(where_clause);

                trace!("filter_expr-\n{fe:?}");

                Some(fe)
            } else {
                None
            };

            let e = match agg.agg {
                AggregateOperation::Avg => avg(col(&agg.column)),
                AggregateOperation::Count => count(col(&agg.column)),
                AggregateOperation::Min => min(col(&agg.column)),
                AggregateOperation::Max => max(col(&agg.column)),
                AggregateOperation::Sum => sum(col(&agg.column)),
            };
            agg_expr.push((e, filter_expr));
        }
    }
    agg_expr
}

fn get_filter_expr(where_clause: &Conditions) -> Expr {
    match where_clause {
        crate::alerts::Conditions::AND((expr1, expr2)) => {
            let mut expr = Expr::Literal(datafusion::scalar::ScalarValue::Boolean(Some(true)));
            for e in [expr1, expr2] {
                let ex = match e.operator {
                    AlertOperator::GreaterThan => col(&e.column).gt(lit(&e.value)),
                    AlertOperator::LessThan => col(&e.column).lt(lit(&e.value)),
                    AlertOperator::EqualTo => col(&e.column).eq(lit(&e.value)),
                    AlertOperator::NotEqualTo => col(&e.column).not_eq(lit(&e.value)),
                    AlertOperator::GreaterThanEqualTo => col(&e.column).gt_eq(lit(&e.value)),
                    AlertOperator::LessThanEqualTo => col(&e.column).lt_eq(lit(&e.value)),
                    AlertOperator::Like => col(&e.column).like(lit(&e.value)),
                    AlertOperator::NotLike => col(&e.column).not_like(lit(&e.value)),
                };
                expr = expr.and(ex);
            }
            expr
        }
        crate::alerts::Conditions::OR((expr1, expr2)) => {
            let mut expr = Expr::Literal(datafusion::scalar::ScalarValue::Boolean(Some(true)));
            for e in [expr1, expr2] {
                let ex = match e.operator {
                    AlertOperator::GreaterThan => col(&e.column).gt(lit(&e.value)),
                    AlertOperator::LessThan => col(&e.column).lt(lit(&e.value)),
                    AlertOperator::EqualTo => col(&e.column).eq(lit(&e.value)),
                    AlertOperator::NotEqualTo => col(&e.column).not_eq(lit(&e.value)),
                    AlertOperator::GreaterThanEqualTo => col(&e.column).gt_eq(lit(&e.value)),
                    AlertOperator::LessThanEqualTo => col(&e.column).lt_eq(lit(&e.value)),
                    AlertOperator::Like => col(&e.column).like(lit(&e.value)),
                    AlertOperator::NotLike => col(&e.column).not_like(lit(&e.value)),
                };
                expr = expr.or(ex);
            }
            expr
        }
        crate::alerts::Conditions::Condition(expr) => match expr.operator {
            AlertOperator::GreaterThan => col(&expr.column).gt(lit(&expr.value)),
            AlertOperator::LessThan => col(&expr.column).lt(lit(&expr.value)),
            AlertOperator::EqualTo => col(&expr.column).eq(lit(&expr.value)),
            AlertOperator::NotEqualTo => col(&expr.column).not_eq(lit(&expr.value)),
            AlertOperator::GreaterThanEqualTo => col(&expr.column).gt_eq(lit(&expr.value)),
            AlertOperator::LessThanEqualTo => col(&expr.column).lt_eq(lit(&expr.value)),
            AlertOperator::Like => col(&expr.column).like(lit(&expr.value)),
            AlertOperator::NotLike => col(&expr.column).not_like(lit(&expr.value)),
        },
    }
}

pub async fn update_alert_state(
    alert_id: &str,
    session_key: &SessionKey,
    new_state: AlertState,
) -> Result<(), AlertError> {
    // check if alert id exists in map
    let alert = ALERTS.get_alert_by_id(alert_id).await?;

    // validate that the user has access to the tables mentioned
    user_auth_for_query(session_key, &alert.query).await?;

    // get current state
    let current_state = ALERTS.get_state(alert_id).await?;

    match current_state {
        AlertState::Triggered => {
            if new_state == AlertState::Triggered {
                let msg = format!("Not allowed to manually go from Triggered to {new_state}");
                return Err(AlertError::InvalidStateChange(msg));
            } else {
                // update state on disk and in memory
                ALERTS
                    .update_state(alert_id, new_state, Some("".into()))
                    .await?;
            }
        }
        AlertState::Silenced => {
            // from here, the user can only go to Resolved
            if new_state == AlertState::Resolved {
                // update state on disk and in memory
                ALERTS
                    .update_state(alert_id, new_state, Some("".into()))
                    .await?;
            } else {
                let msg = format!("Not allowed to manually go from Silenced to {new_state}");
                return Err(AlertError::InvalidStateChange(msg));
            }
        }
        AlertState::Resolved => {
            // user shouldn't logically be changing states if current state is Resolved
            let msg = format!("Not allowed to go manually from Resolved to {new_state}");
            return Err(AlertError::InvalidStateChange(msg));
        }
    }
    Ok(())
}
