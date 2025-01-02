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

use arrow_array::{Float64Array, Int64Array};
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
    Aggregate, AlertConfig, AlertError, AlertOperator, AlertState, Conditions, ALERTS
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

    trace!("got base_df");

    let mut agg_results = vec![];
    for agg_config in &alert.aggregate_config {
        // agg expression
        let mut aggr_expr: Vec<Expr> = vec![];

        let filtered_df = if let Some(where_clause) = &agg_config.condition_config {

            let filter_expr = get_filter_expr(where_clause);

            trace!("filter_expr-\n{filter_expr:?}");

            base_df.clone().filter(filter_expr)?
        } else {
            base_df.clone()
        };

        trace!("got filter_df");

        aggr_expr.push(match agg_config.agg {
            Aggregate::Avg => avg(col(&agg_config.column)), //.alias(&agg_config.column),
            Aggregate::Count => count(col(&agg_config.column)), //.alias(&agg_config.column),
            Aggregate::Min => min(col(&agg_config.column)), //.alias(&agg_config.column),
            Aggregate::Max => max(col(&agg_config.column)), //.alias(&agg_config.column),
            Aggregate::Sum => sum(col(&agg_config.column)), //.alias(&agg_config.column),
        });

        trace!("Aggregating");
        // now that base_df has been filtered, apply aggregate
        let row = filtered_df.aggregate(vec![], aggr_expr)?.collect().await?;

        trace!("row-\n{row:?}");

        let final_value = if let Some(f) = row
            .first()
            .and_then(|batch| {
                trace!("batch.column(0)-\n{:?}", batch.column(0));
                batch.column(0).as_any().downcast_ref::<Float64Array>()
            })
            .map(|array| {
                trace!("array-\n{array:?}");
                array.value(0)
            }) {
            f
        } else {
            let final_value = row
                .first()
                .and_then(|batch| {
                    trace!("batch.column(0)-\n{:?}", batch.column(0));
                    batch.column(0).as_any().downcast_ref::<Int64Array>()
                })
                .map(|array| {
                    trace!("array-\n{array:?}");
                    array.value(0)
                })
                .unwrap_or_default();
            final_value as f64
        };

        // let final_value = String::from_utf8(final_value.to_vec()).unwrap().parse::<f64>().unwrap();

        // now compare
        let res = match &agg_config.operator {
            AlertOperator::GreaterThan => final_value > agg_config.value,
            AlertOperator::LessThan => final_value < agg_config.value,
            AlertOperator::EqualTo => final_value == agg_config.value,
            AlertOperator::NotEqualTo => final_value != agg_config.value,
            AlertOperator::GreaterThanEqualTo => final_value >= agg_config.value,
            AlertOperator::LessThanEqualTo => final_value <= agg_config.value,
            _ => unreachable!(),
        };

        let message = if res {
            if agg_config.condition_config.is_some() {
                Some(
                    agg_config
                        .condition_config
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

        agg_results.push((res, message, agg_config, final_value));
    }

    trace!("agg_results-\n{agg_results:?}");

    // this is the final result of this evaluation
    let res = if let Some(agg_condition) = &alert.agg_condition {
        match agg_condition {
            crate::alerts::AggregateCondition::AND => agg_results.iter().all(|(res, _, _, _)| *res),
            crate::alerts::AggregateCondition::OR => agg_results.iter().any(|(res, _, _, _)| *res),
        }
    } else {
        assert!(agg_results.len() == 1);
        agg_results[0].0
    };

    if res {
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
    } else {
        ALERTS
            .update_state(&alert.id.to_string(), AlertState::Resolved, None)
            .await?;
    }

    Ok(())
}

// /// This function contains the logic to run the alert evaluation task
// pub async fn evaluate_alert2(alert: &AlertConfig) -> Result<(), AlertError> {
//     trace!("RUNNING EVAL TASK FOR- {alert:?}");

//     let (start_time, end_time) = match &alert.eval_type {
//         super::EvalConfig::RollingWindow(rolling_window) => {
//             (&rolling_window.eval_start, &rolling_window.eval_end)
//         }
//     };

//     let session_state = QUERY_SESSION.state();
//     let raw_logical_plan = session_state.create_logical_plan(&alert.query).await?;

//     // TODO: Filter tags should be taken care of!!!
//     let time_range = TimeRange::parse_human_time(start_time, end_time)
//         .map_err(|err| AlertError::CustomError(err.to_string()))?;

//     let query = crate::query::Query {
//         raw_logical_plan,
//         time_range,
//         filter_tag: None,
//     };

//     // for now proceed in a similar fashion as we do in query
//     // TODO: in case of multiple table query does the selection of time partition make a difference? (especially when the tables don't have overlapping data)
//     let stream_name = if let Some(stream_name) = query.first_table_name() {
//         stream_name
//     } else {
//         return Err(AlertError::CustomError(format!(
//             "Table name not found in query- {}",
//             alert.query
//         )));
//     };

//     let df = query
//         .get_dataframe(stream_name)
//         .await
//         .map_err(|err| AlertError::CustomError(err.to_string()))?;

//     for agg_config in &alert.thresholds {
//         if let Some(condition) = &agg_config.condition_config {
//             match condition {
//                 crate::alerts::Conditions::AND((expr1,expr2)) => {
//                     let mut expr = Expr::Literal(datafusion::scalar::ScalarValue::Boolean(Some(true)));
//                     for e in [expr1, expr2] {
//                         let ex = match e.operator {
//                             AlertOperator::GreaterThan => expr.and(col(e.column).gt(lit(e.value))),
//                             AlertOperator::LessThan => expr.and(col(e.column).lt(lit(e.value))),
//                             AlertOperator::EqualTo => expr.and(col(e.column).eq(lit(e.value))),
//                             AlertOperator::NotEqualTo => expr.and(col(e.column).not_eq(lit(e.value))),
//                             AlertOperator::GreaterThanEqualTo => expr.and(col(e.column).gt_eq(lit(e.value))),
//                             AlertOperator::LessThanEqualTo => expr.and(col(e.column).lt_eq(lit(e.value))),
//                             AlertOperator::Like => expr.and(col(e.column).like(lit(e.value))),
//                             AlertOperator::NotLike => expr.and(col(e.column).not_like(lit(e.value))),
//                         };
//                         expr = expr.and(ex);
//                     }
//                     expr
//                 }
//                 crate::alerts::Conditions::OR((expr1,expr2)) => {
//                     let mut expr = Expr::Literal(datafusion::scalar::ScalarValue::Boolean(Some(true)));
//                     for e in [expr1, expr2] {
//                         let ex = match e.operator {
//                             AlertOperator::GreaterThan => expr.and(col(e.column).gt(lit(e.value))),
//                             AlertOperator::LessThan => expr.and(col(e.column).lt(lit(e.value))),
//                             AlertOperator::EqualTo => expr.and(col(e.column).eq(lit(e.value))),
//                             AlertOperator::NotEqualTo => expr.and(col(e.column).not_eq(lit(e.value))),
//                             AlertOperator::GreaterThanEqualTo => expr.and(col(e.column).gt_eq(lit(e.value))),
//                             AlertOperator::LessThanEqualTo => expr.and(col(e.column).lt_eq(lit(e.value))),
//                             AlertOperator::Like => expr.and(col(e.column).like(lit(e.value))),
//                             AlertOperator::NotLike => expr.and(col(e.column).not_like(lit(e.value))),
//                         };
//                         expr = expr.or(ex);
//                     }
//                     expr
//                 },
//                 crate::alerts::Conditions::Condition(expr) => {
//                     let expr = match expr.operator {
//                         AlertOperator::GreaterThan => col(expr.column).gt(lit(expr.value)),
//                         AlertOperator::LessThan => col(expr.column).lt(lit(expr.value)),
//                         AlertOperator::EqualTo => col(expr.column).eq(lit(expr.value)),
//                         AlertOperator::NotEqualTo => col(expr.column).not_eq(lit(expr.value)),
//                         AlertOperator::GreaterThanEqualTo => col(expr.column).gt_eq(lit(expr.value)),
//                         AlertOperator::LessThanEqualTo => col(expr.column).lt_eq(lit(expr.value)),
//                         AlertOperator::Like => col(expr.column).like(lit(expr.value)),
//                         AlertOperator::NotLike => col(expr.column).not_like(lit(expr.value)),
//                     };
//                     expr
//                 },
//             }
//         }
//     }

//     let (group_expr, aggr_expr, filter_expr) = get_exprs(&alert.thresholds);
//     let df = df.aggregate(group_expr, aggr_expr)?;

//     let nrows = df.clone().filter(filter_expr)?.count().await?;
//     trace!("dataframe-\n{:?}", df.collect().await);

//     if nrows > 0 {
//         trace!("ALERT!!!!!!");

//         // update state
//         ALERTS
//             .update_state(&alert.id.to_string(), AlertState::Triggered, true)
//             .await?;
//     } else {
//         ALERTS
//             .update_state(&alert.id.to_string(), AlertState::Resolved, false)
//             .await?;
//     }

//     Ok(())
// }

fn get_filter_expr(where_clause: &Conditions) -> Expr {
    match where_clause {
        crate::alerts::Conditions::AND((expr1, expr2)) => {
            let mut expr =
                Expr::Literal(datafusion::scalar::ScalarValue::Boolean(Some(true)));
            for e in [expr1, expr2] {
                let ex = match e.operator {
                    AlertOperator::GreaterThan => col(&e.column).gt(lit(&e.value)),
                    AlertOperator::LessThan => col(&e.column).lt(lit(&e.value)),
                    AlertOperator::EqualTo => col(&e.column).eq(lit(&e.value)),
                    AlertOperator::NotEqualTo => col(&e.column).not_eq(lit(&e.value)),
                    AlertOperator::GreaterThanEqualTo => {
                        col(&e.column).gt_eq(lit(&e.value))
                    }
                    AlertOperator::LessThanEqualTo => col(&e.column).lt_eq(lit(&e.value)),
                    AlertOperator::Like => col(&e.column).like(lit(&e.value)),
                    AlertOperator::NotLike => col(&e.column).not_like(lit(&e.value)),
                };
                expr = expr.and(ex);
            }
            expr
        }
        crate::alerts::Conditions::OR((expr1, expr2)) => {
            let mut expr =
                Expr::Literal(datafusion::scalar::ScalarValue::Boolean(Some(true)));
            for e in [expr1, expr2] {
                let ex = match e.operator {
                    AlertOperator::GreaterThan => col(&e.column).gt(lit(&e.value)),
                    AlertOperator::LessThan => col(&e.column).lt(lit(&e.value)),
                    AlertOperator::EqualTo => col(&e.column).eq(lit(&e.value)),
                    AlertOperator::NotEqualTo => col(&e.column).not_eq(lit(&e.value)),
                    AlertOperator::GreaterThanEqualTo => {
                        col(&e.column).gt_eq(lit(&e.value))
                    }
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
