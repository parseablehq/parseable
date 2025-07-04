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

pub mod actix;
pub mod arrow;
pub mod header_parsing;
pub mod human_size;
pub mod json;
pub mod time;
pub mod uid;
pub mod update;

use crate::handlers::http::rbac::RBACError;
use crate::query::{TableScanVisitor, QUERY_SESSION};
use crate::rbac::map::SessionKey;
use crate::rbac::role::{Action, Permission};
use crate::rbac::Users;
use actix::extract_session_key_from_req;
use actix_web::HttpRequest;
use chrono::{NaiveDate, NaiveDateTime, NaiveTime, Utc};
use datafusion::common::tree_node::TreeNode;
use regex::Regex;
use sha2::{Digest, Sha256};

pub const DATASET_STATS_STREAM_NAME: &str = "pstats";

pub fn get_node_id() -> String {
    let now = Utc::now().to_rfc3339();
    let id = get_hash(&now).to_string().split_at(15).0.to_string();
    id
}

pub fn extract_datetime(path: &str) -> Option<NaiveDateTime> {
    let re = Regex::new(r"date=(\d{4}-\d{2}-\d{2})/hour=(\d{2})/minute=(\d{2})").unwrap();
    if let Some(caps) = re.captures(path) {
        let date_str = caps.get(1)?.as_str();
        let hour_str = caps.get(2)?.as_str();
        let minute_str = caps.get(3)?.as_str();

        let date = NaiveDate::parse_from_str(date_str, "%Y-%m-%d").ok()?;
        let time = NaiveTime::parse_from_str(&format!("{hour_str}:{minute_str}"), "%H:%M").ok()?;
        Some(NaiveDateTime::new(date, time))
    } else {
        None
    }
}

pub fn get_user_from_request(req: &HttpRequest) -> Result<String, RBACError> {
    let session_key = extract_session_key_from_req(req).unwrap();
    let user_id = Users.get_username_from_session(&session_key);
    if user_id.is_none() {
        return Err(RBACError::UserDoesNotExist);
    }
    let user_id = user_id.unwrap();
    Ok(user_id)
}

pub fn get_hash(key: &str) -> String {
    let mut hasher = Sha256::new();
    hasher.update(key);
    let result = format!("{:x}", hasher.finalize());
    result
}

async fn get_tables_from_query(query: &str) -> Result<TableScanVisitor, actix_web::error::Error> {
    let session_state = QUERY_SESSION.state();
    let raw_logical_plan = session_state
        .create_logical_plan(query)
        .await
        .map_err(|e| actix_web::error::ErrorInternalServerError(format!("Query error: {e}")))?;

    let mut visitor = TableScanVisitor::default();
    let _ = raw_logical_plan.visit(&mut visitor);
    Ok(visitor)
}

pub async fn user_auth_for_query(
    session_key: &SessionKey,
    query: &str,
) -> Result<(), actix_web::error::Error> {
    let tables = get_tables_from_query(query).await?.into_inner();
    let permissions = Users.get_permissions(session_key);
    user_auth_for_datasets(&permissions, &tables)
}

pub fn user_auth_for_datasets(
    permissions: &[Permission],
    tables: &[String],
) -> Result<(), actix_web::error::Error> {
    for table_name in tables {
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
            return Err(actix_web::error::ErrorUnauthorized(format!(
                "User does not have access to stream- {table_name}"
            )));
        }
    }

    Ok(())
}

/// A function to extract table names from a SQL string
pub async fn extract_tables(sql: &str) -> Option<Vec<String>> {
    let session_state = QUERY_SESSION.state();

    // get the logical plan and extract the table name
    let raw_logical_plan = match session_state.create_logical_plan(sql).await {
        Ok(plan) => plan,
        Err(_) => return None,
    };

    // create a visitor to extract the table name
    let mut visitor = TableScanVisitor::default();
    let _ = raw_logical_plan.visit(&mut visitor);

    let tables = visitor.into_inner();

    Some(tables)
}
