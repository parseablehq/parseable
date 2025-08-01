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

use std::{collections::HashMap, str::FromStr};

use crate::{
    alerts::{AlertType, alert_types::ThresholdAlert, traits::AlertTrait},
    parseable::PARSEABLE,
    storage::object_storage::alert_json_path,
    utils::{actix::extract_session_key_from_req, user_auth_for_query},
};
use actix_web::{
    HttpRequest, Responder,
    web::{self, Json, Path},
};
use bytes::Bytes;
use ulid::Ulid;

use crate::alerts::{ALERTS, AlertConfig, AlertError, AlertRequest, AlertState, Severity};

// GET /alerts
/// User needs at least a read access to the stream(s) that is being referenced in an alert
/// Read all alerts then return alerts which satisfy the condition
/// Supports pagination with optional query parameters:
/// - tags: comma-separated list of tags to filter alerts
/// - offset: number of alerts to skip (default: 0)
/// - limit: maximum number of alerts to return (default: 100, max: 1000)
pub async fn list(req: HttpRequest) -> Result<impl Responder, AlertError> {
    let session_key = extract_session_key_from_req(&req)?;
    let query_map = web::Query::<HashMap<String, String>>::from_query(req.query_string())
        .map_err(|_| AlertError::InvalidQueryParameter("malformed query parameters".to_string()))?;

    let mut tags_list = Vec::new();
    let mut offset = 0usize;
    let mut limit = 100usize; // Default limit
    const MAX_LIMIT: usize = 1000; // Maximum allowed limit

    // Parse query parameters
    if !query_map.is_empty() {
        // Parse tags parameter
        if let Some(tags) = query_map.get("tags") {
            tags_list = tags
                .split(',')
                .map(|s| s.trim().to_string())
                .filter(|s| !s.is_empty())
                .collect();
            if tags_list.is_empty() {
                return Err(AlertError::InvalidQueryParameter(
                    "empty tags not allowed with query param tags".to_string(),
                ));
            }
        }

        // Parse offset parameter
        if let Some(offset_str) = query_map.get("offset") {
            offset = offset_str.parse().map_err(|_| {
                AlertError::InvalidQueryParameter("offset is not a valid number".to_string())
            })?;
        }

        // Parse limit parameter
        if let Some(limit_str) = query_map.get("limit") {
            limit = limit_str.parse().map_err(|_| {
                AlertError::InvalidQueryParameter("limit is not a valid number".to_string())
            })?;

            // Validate limit bounds
            if limit == 0 || limit > MAX_LIMIT {
                return Err(AlertError::InvalidQueryParameter(
                    "limit should be between 1 and 1000".to_string(),
                ));
            }
        }
    }

    let guard = ALERTS.read().await;
    let alerts = if let Some(alerts) = guard.as_ref() {
        alerts
    } else {
        return Err(AlertError::CustomError("No AlertManager set".into()));
    };

    let alerts = alerts.list_alerts_for_user(session_key, tags_list).await?;
    let mut alerts_summary = alerts
        .iter()
        .map(|alert| alert.to_summary())
        .collect::<Vec<_>>();

    // Sort by state priority (Triggered > Silenced > Resolved) then by severity (Critical > High > Medium > Low)
    alerts_summary.sort_by(|a, b| {
        // Parse state and severity from JSON values back to enums
        let state_a = a
            .get("state")
            .and_then(|v| v.as_str())
            .and_then(|s| s.parse::<AlertState>().ok())
            .unwrap_or(AlertState::Resolved); // Default to lowest priority

        let state_b = b
            .get("state")
            .and_then(|v| v.as_str())
            .and_then(|s| s.parse::<AlertState>().ok())
            .unwrap_or(AlertState::Resolved);

        let severity_a = a
            .get("severity")
            .and_then(|v| v.as_str())
            .and_then(|s| s.parse::<Severity>().ok())
            .unwrap_or(Severity::Low); // Default to lowest priority

        let severity_b = b
            .get("severity")
            .and_then(|v| v.as_str())
            .and_then(|s| s.parse::<Severity>().ok())
            .unwrap_or(Severity::Low);

        // First sort by state, then by severity
        state_a
            .cmp(&state_b)
            .then_with(|| severity_a.cmp(&severity_b))
    });

    let paginated_alerts = alerts_summary
        .into_iter()
        .skip(offset)
        .take(limit)
        .collect::<Vec<_>>();

    Ok(web::Json(paginated_alerts))
}

// POST /alerts
pub async fn post(
    req: HttpRequest,
    Json(alert): Json<AlertRequest>,
) -> Result<impl Responder, AlertError> {
    let alert: AlertConfig = alert.into().await?;

    let threshold_alert;
    let alert: &dyn AlertTrait = match &alert.alert_type {
        AlertType::Threshold => {
            threshold_alert = ThresholdAlert::from(alert);
            &threshold_alert
        }
        AlertType::Anomaly => {
            return Err(AlertError::NotPresentInOSS("anomaly".into()));
        }
        AlertType::Forecast => {
            return Err(AlertError::NotPresentInOSS("forecast".into()));
        }
    };

    let guard = ALERTS.write().await;
    let alerts = if let Some(alerts) = guard.as_ref() {
        alerts
    } else {
        return Err(AlertError::CustomError("No AlertManager set".into()));
    };

    // validate the incoming alert query
    // does the user have access to these tables or not?
    let session_key = extract_session_key_from_req(&req)?;

    alert.validate(&session_key).await?;

    // now that we've validated that the user can run this query
    // move on to saving the alert in ObjectStore
    alerts.update(alert).await;

    let path = alert_json_path(*alert.get_id());

    let store = PARSEABLE.storage.get_object_store();
    let alert_bytes = serde_json::to_vec(&alert.to_alert_config())?;
    store.put_object(&path, Bytes::from(alert_bytes)).await?;

    // start the task
    alerts.start_task(alert.clone_box()).await?;

    Ok(web::Json(alert.to_alert_config()))
}

// GET /alerts/{alert_id}
pub async fn get(req: HttpRequest, alert_id: Path<Ulid>) -> Result<impl Responder, AlertError> {
    let session_key = extract_session_key_from_req(&req)?;
    let alert_id = alert_id.into_inner();

    let guard = ALERTS.read().await;
    let alerts = if let Some(alerts) = guard.as_ref() {
        alerts
    } else {
        return Err(AlertError::CustomError("No AlertManager set".into()));
    };

    let alert = alerts.get_alert_by_id(alert_id).await?;
    // validate that the user has access to the tables mentioned in the query
    user_auth_for_query(&session_key, alert.get_query()).await?;

    Ok(web::Json(alert.to_alert_config()))
}

// DELETE /alerts/{alert_id}
/// Deletion should happen from disk, sheduled tasks, then memory
pub async fn delete(req: HttpRequest, alert_id: Path<Ulid>) -> Result<impl Responder, AlertError> {
    let session_key = extract_session_key_from_req(&req)?;
    let alert_id = alert_id.into_inner();

    let guard = ALERTS.write().await;
    let alerts = if let Some(alerts) = guard.as_ref() {
        alerts
    } else {
        return Err(AlertError::CustomError("No AlertManager set".into()));
    };

    let alert = alerts.get_alert_by_id(alert_id).await?;

    // validate that the user has access to the tables mentioned in the query
    user_auth_for_query(&session_key, alert.get_query()).await?;

    let store = PARSEABLE.storage.get_object_store();
    let alert_path = alert_json_path(alert_id);

    // delete from Object Store
    store
        .delete_object(&alert_path)
        .await
        .map_err(AlertError::ObjectStorage)?;

    // delete from memory
    alerts.delete(alert_id).await?;

    // delete the scheduled task
    alerts.delete_task(alert_id).await?;

    Ok(format!("Deleted alert with ID- {alert_id}"))
}

// PUT /alerts/{alert_id}
/// first save on disk, then in memory
/// then modify scheduled task
pub async fn update_state(
    req: HttpRequest,
    alert_id: Path<Ulid>,
) -> Result<impl Responder, AlertError> {
    let session_key = extract_session_key_from_req(&req)?;
    let alert_id = alert_id.into_inner();

    let guard = ALERTS.write().await;
    let alerts = if let Some(alerts) = guard.as_ref() {
        alerts
    } else {
        return Err(AlertError::CustomError("No AlertManager set".into()));
    };

    // check if alert id exists in map
    let mut alert = alerts.get_alert_by_id(alert_id).await?;
    // validate that the user has access to the tables mentioned in the query
    user_auth_for_query(&session_key, alert.get_query()).await?;

    let query_string = req.query_string();

    if query_string.is_empty() {
        return Err(AlertError::InvalidStateChange(
            "No query string provided".to_string(),
        ));
    }

    let tokens = query_string.split('=').collect::<Vec<&str>>();
    let state_key = tokens[0];
    let state_value = tokens[1];
    if state_key != "state" {
        return Err(AlertError::InvalidStateChange(
            "Invalid query parameter".to_string(),
        ));
    }

    let new_state = AlertState::from_str(state_value)?;
    alert.update_state(true, new_state, Some("".into())).await?;

    alerts.update(&*alert).await;

    Ok(web::Json(alert.to_alert_config()))
}

pub async fn list_tags() -> Result<impl Responder, AlertError> {
    let guard = ALERTS.read().await;
    let alerts = if let Some(alerts) = guard.as_ref() {
        alerts
    } else {
        return Err(AlertError::CustomError("No AlertManager set".into()));
    };
    let tags = alerts.list_tags().await;
    Ok(web::Json(tags))
}
