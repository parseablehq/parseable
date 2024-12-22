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

use datafusion::{
    common::tree_node::TreeNode,
    prelude::{col, lit, Expr},
};
use tracing::trace;

use crate::{
    handlers::http::alerts::{AlertState, ALERTS},
    query::{TableScanVisitor, QUERY_SESSION},
    rbac::{
        map::SessionKey,
        role::{Action, Permission},
        Users,
    },
    utils::time::TimeRange,
};

use super::{AlertConfig, AlertError};

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

/// This function contains the logic to run the alert evaluation task
pub async fn evaluate_alert(alert: AlertConfig) -> Result<(), AlertError> {
    println!("RUNNING EVAL TASK FOR- {alert:?}");

    let (start_time, end_time) = match &alert.eval_type {
        super::EvalConfig::RollingWindow(rolling_window) => {
            (&rolling_window.eval_start, &rolling_window.eval_end)
        }
    };

    let session_state = QUERY_SESSION.state();
    let raw_logical_plan = session_state
        .create_logical_plan(&alert.query)
        .await
        .unwrap();

    // TODO: Filter tags should be taken care of!!!
    let time_range = TimeRange::parse_human_time(start_time, end_time).unwrap();
    let query = crate::query::Query {
        raw_logical_plan,
        time_range,
        filter_tag: None,
    };

    // for now proceed in a similar fashion as we do in query
    // TODO: in case of multiple table query does the selection of time partition make a difference? (especially when the tables don't have overlapping data)
    let stream_name = query.first_table_name().unwrap();

    let df = query.get_dataframe(stream_name).await.unwrap();

    // let df = DataFrame::new(session_state, raw_logical_plan);

    let mut expr = Expr::Literal(datafusion::scalar::ScalarValue::Boolean(Some(true)));
    for threshold in &alert.thresholds {
        let res = match threshold.operator {
            crate::handlers::http::alerts::AlertOperator::GreaterThan => {
                col(&threshold.column).gt(lit(threshold.value))
            }
            crate::handlers::http::alerts::AlertOperator::LessThan => {
                col(&threshold.column).lt(lit(threshold.value))
            }
            crate::handlers::http::alerts::AlertOperator::EqualTo => {
                col(&threshold.column).eq(lit(threshold.value))
            }
            crate::handlers::http::alerts::AlertOperator::NotEqualTo => {
                col(&threshold.column).not_eq(lit(threshold.value))
            }
            crate::handlers::http::alerts::AlertOperator::GreaterThanEqualTo => {
                col(&threshold.column).gt_eq(lit(threshold.value))
            }
            crate::handlers::http::alerts::AlertOperator::LessThanEqualTo => {
                col(&threshold.column).lt_eq(lit(threshold.value))
            }
            crate::handlers::http::alerts::AlertOperator::Like => {
                col(&threshold.column).like(lit(threshold.value))
            }
            crate::handlers::http::alerts::AlertOperator::NotLike => {
                col(&threshold.column).not_like(lit(threshold.value))
            }
        };

        expr = expr.and(res);
    }

    let nrows = df.clone().filter(expr).unwrap().count().await.unwrap();
    trace!("dataframe-\n{:?}", df.collect().await);

    if nrows > 0 {
        trace!("ALERT!!!!!!");

        // update state
        ALERTS
            .update_state(&alert.id.to_string(), AlertState::Triggered, true)
            .await?;
    } else {
        ALERTS
            .update_state(&alert.id.to_string(), AlertState::Resolved, false)
            .await?;
    }

    Ok(())
}
