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
    alerts::{
        ALERTS, AlertError, AlertState, Severity,
        alert_enums::{AlertType, NotificationState},
        alert_structs::{AlertConfig, AlertRequest, AlertStateEntry, NotificationStateRequest},
        alert_traits::AlertTrait,
        alert_types::ThresholdAlert,
        target::Retry,
    },
    metastore::metastore_traits::MetastoreObject,
    parseable::PARSEABLE,
    utils::{actix::extract_session_key_from_req, user_auth_for_query},
};
use actix_web::{
    HttpRequest, Responder,
    web::{self, Json, Path},
};
use chrono::{DateTime, Utc};
use ulid::Ulid;

// Reserved query parameter names that are not treated as other_fields filters
const RESERVED_PARAMS: [&str; 3] = ["tags", "offset", "limit"];
const MAX_LIMIT: usize = 1000;
const DEFAULT_LIMIT: usize = 100;

/// Query parameters for listing alerts
pub struct ListQueryParams {
    /// Comma-separated tag filters; empty if no tag filtering
    pub tags_list: Vec<String>,
    /// Number of results to skip (default: 0)
    pub offset: usize,
    /// Maximum results to return (1-1000, default: 100)
    pub limit: usize,
    /// Additional field filters not covered by reserved params
    pub other_fields_filters: HashMap<String, String>,
}

/// Parse and validate query parameters for listing alerts
pub fn parse_list_query_params(
    query_map: &HashMap<String, String>,
) -> Result<ListQueryParams, AlertError> {
    let mut tags_list = Vec::new();
    let mut offset = 0usize;
    let mut limit = DEFAULT_LIMIT;
    let mut other_fields_filters: HashMap<String, String> = HashMap::new();

    if query_map.is_empty() {
        return Ok(ListQueryParams {
            tags_list,
            offset,
            limit,
            other_fields_filters,
        });
    }

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

    // Collect all other query parameters as potential other_fields filters
    for (key, value) in query_map.iter() {
        if !RESERVED_PARAMS.contains(&key.as_str()) {
            other_fields_filters.insert(key.clone(), value.clone());
        }
    }

    Ok(ListQueryParams {
        tags_list,
        offset,
        limit,
        other_fields_filters,
    })
}

/// Filter alerts by other_fields
fn filter_by_other_fields(
    mut alerts_summary: Vec<serde_json::Map<String, serde_json::Value>>,
    filters: &HashMap<String, String>,
) -> Vec<serde_json::Map<String, serde_json::Value>> {
    if filters.is_empty() {
        return alerts_summary;
    }

    alerts_summary.retain(|alert_summary| {
        // Check if all specified other_fields filters match
        filters.iter().all(|(filter_key, filter_value)| {
            alert_summary
                .get(filter_key)
                .map(|v| {
                    // Convert JSON value to string for comparison
                    let value_as_string = if v.is_string() {
                        // For strings, use the raw string value without quotes
                        v.as_str().unwrap_or("").to_string()
                    } else {
                        // For numbers, booleans, arrays, objects, use JSON representation
                        v.to_string()
                    };
                    value_as_string == *filter_value
                })
                .unwrap_or(false)
        })
    });

    alerts_summary
}

/// Sort alerts by state, severity, and title
fn sort_alerts(alerts_summary: &mut [serde_json::Map<String, serde_json::Value>]) {
    alerts_summary.sort_by(|a, b| {
        // Parse state and severity from JSON values back to enums
        let state_a = a
            .get("state")
            .and_then(|v| v.as_str())
            .and_then(|s| s.parse::<AlertState>().ok())
            .unwrap_or(AlertState::NotTriggered); // Default to lowest priority

        let state_b = b
            .get("state")
            .and_then(|v| v.as_str())
            .and_then(|s| s.parse::<AlertState>().ok())
            .unwrap_or(AlertState::NotTriggered);

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

        let title_a = a.get("title").and_then(|v| v.as_str()).unwrap_or("");
        let title_b = b.get("title").and_then(|v| v.as_str()).unwrap_or("");

        // First sort by state, then by severity, then by title
        state_a
            .cmp(&state_b)
            .then_with(|| severity_a.cmp(&severity_b))
            .then_with(|| title_a.cmp(title_b))
    });
}

/// Paginate alerts
fn paginate_alerts(
    alerts_summary: Vec<serde_json::Map<String, serde_json::Value>>,
    offset: usize,
    limit: usize,
) -> Vec<serde_json::Map<String, serde_json::Value>> {
    alerts_summary
        .into_iter()
        .skip(offset)
        .take(limit)
        .collect()
}

// GET /alerts
/// User needs at least a read access to the stream(s) that is being referenced in an alert
/// Read all alerts then return alerts which satisfy the condition
pub async fn list(req: HttpRequest) -> Result<impl Responder, AlertError> {
    let session_key = extract_session_key_from_req(&req)?;
    let query_map = web::Query::<HashMap<String, String>>::from_query(req.query_string())
        .map_err(|_| AlertError::InvalidQueryParameter("malformed query parameters".to_string()))?;

    // Parse and validate query parameters
    let params = parse_list_query_params(&query_map)?;

    // Get alerts from the manager
    let guard = ALERTS.read().await;
    let alerts = if let Some(alerts) = guard.as_ref() {
        alerts
    } else {
        return Err(AlertError::CustomError("No AlertManager set".into()));
    };

    // Fetch alerts for the user
    let alerts = alerts
        .list_alerts_for_user(session_key, params.tags_list)
        .await?;
    let mut alerts_summary = alerts
        .iter()
        .map(|alert| alert.to_summary())
        .collect::<Vec<_>>();

    // Filter by other_fields
    alerts_summary = filter_by_other_fields(alerts_summary, &params.other_fields_filters);

    // Sort alerts
    sort_alerts(&mut alerts_summary);

    // Paginate results
    let paginated_alerts = paginate_alerts(alerts_summary, params.offset, params.limit);

    Ok(web::Json(paginated_alerts))
}

// POST /alerts
pub async fn post(
    req: HttpRequest,
    Json(alert): Json<AlertRequest>,
) -> Result<impl Responder, AlertError> {
    let mut alert: AlertConfig = alert.into().await?;

    if alert.notification_config.interval > alert.get_eval_frequency() {
        return Err(AlertError::ValidationFailure(
            "Notification interval cannot exceed evaluation frequency".into(),
        ));
    }

    if alert.get_eval_frequency().eq(&0) {
        return Err(AlertError::ValidationFailure(
            "Eval frequency cannot be 0".into(),
        ));
    }
    if alert.notification_config.interval.eq(&0) {
        return Err(AlertError::ValidationFailure(
            "Notification interval cannot be 0".into(),
        ));
    }

    // calculate the `times` for notification config
    let eval_freq = alert.get_eval_frequency();
    let notif_freq = alert.notification_config.interval;
    let times = if (eval_freq / notif_freq) == 0 {
        1
    } else {
        (eval_freq / notif_freq) as usize
    };

    alert.notification_config.times = Retry::Finite(times);

    let threshold_alert;
    let alert: &dyn AlertTrait = match &alert.alert_type {
        AlertType::Threshold => {
            threshold_alert = ThresholdAlert::from(alert);
            &threshold_alert
        }
        AlertType::Anomaly(_) => {
            return Err(AlertError::NotPresentInOSS("anomaly"));
        }
        AlertType::Forecast(_) => {
            return Err(AlertError::NotPresentInOSS("forecast"));
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

    // update persistent storage first
    PARSEABLE
        .metastore
        .put_alert(&alert.to_alert_config())
        .await?;

    // create initial alert state entry (default to NotTriggered)
    let state_entry = AlertStateEntry::new(*alert.get_id(), AlertState::NotTriggered);
    PARSEABLE
        .metastore
        .put_alert_state(&state_entry as &dyn MetastoreObject)
        .await?;

    // update in memory
    alerts.update(alert).await;

    // start the task
    alerts.start_task(alert.clone_box()).await?;

    Ok(web::Json(alert.to_alert_config().to_response()))
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

    Ok(web::Json(alert.to_alert_config().to_response()))
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

    PARSEABLE.metastore.delete_alert(&*alert).await?;

    // delete the associated alert state
    let state_to_delete = AlertStateEntry::new(alert_id, AlertState::NotTriggered); // state doesn't matter for deletion
    PARSEABLE
        .metastore
        .delete_alert_state(&state_to_delete as &dyn MetastoreObject)
        .await?;

    // delete from memory
    alerts.delete(alert_id).await?;

    // delete the scheduled task
    alerts.delete_task(alert_id).await?;

    Ok(format!("Deleted alert with ID- {alert_id}"))
}

// PATCH /alerts/{alert_id}/update_notification_state
/// first save on disk, then in memory
/// then modify scheduled task
pub async fn update_notification_state(
    req: HttpRequest,
    alert_id: Path<Ulid>,
    Json(new_notification_state): Json<NotificationStateRequest>,
) -> Result<impl Responder, AlertError> {
    let session_key = extract_session_key_from_req(&req)?;
    let alert_id = alert_id.into_inner();

    let new_notification_state = match new_notification_state.state.as_str() {
        "notify" => NotificationState::Notify,
        "indefinite" => NotificationState::Mute("indefinite".into()),
        _ => {
            // either human time or datetime in UTC
            let till_time = if let Ok(duration) =
                humantime::parse_duration(&new_notification_state.state)
            {
                (Utc::now() + duration).to_rfc3339()
            } else if let Ok(timestamp) = DateTime::<Utc>::from_str(&new_notification_state.state) {
                // must be datetime utc then
                if timestamp < Utc::now() {
                    return Err(AlertError::InvalidStateChange(
                        "Invalid notification state change request. Provided time is < Now".into(),
                    ));
                }
                timestamp.to_rfc3339()
            } else {
                return Err(AlertError::InvalidStateChange(format!(
                    "Invalid notification state change request. Expected `notify`, `indefinite` or human-time or UTC datetime. Got `{}`",
                    &new_notification_state.state
                )));
            };
            NotificationState::Mute(till_time)
        }
    };

    let guard = ALERTS.write().await;
    let alerts = if let Some(alerts) = guard.as_ref() {
        alerts
    } else {
        return Err(AlertError::CustomError("No AlertManager set".into()));
    };

    // check if alert id exists in map
    let alert = alerts.get_alert_by_id(alert_id).await?;
    // validate that the user has access to the tables mentioned in the query
    user_auth_for_query(&session_key, alert.get_query()).await?;

    alerts
        .update_notification_state(alert_id, new_notification_state)
        .await?;
    let alert = alerts.get_alert_by_id(alert_id).await?;

    Ok(web::Json(alert.to_alert_config().to_response()))
}

// PATCH /alerts/{alert_id}/disable
/// first save on disk, then in memory
/// then modify scheduled task
pub async fn disable_alert(
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
    let alert = alerts.get_alert_by_id(alert_id).await?;
    // validate that the user has access to the tables mentioned in the query
    user_auth_for_query(&session_key, alert.get_query()).await?;

    alerts
        .update_state(alert_id, AlertState::Disabled, Some("".into()))
        .await?;
    let alert = alerts.get_alert_by_id(alert_id).await?;

    Ok(web::Json(alert.to_alert_config().to_response()))
}

// PATCH /alerts/{alert_id}/enable
/// first save on disk, then in memory
/// then modify scheduled task
pub async fn enable_alert(
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
    let alert = alerts.get_alert_by_id(alert_id).await?;

    // only run if alert is disabled
    if alert.get_state().ne(&AlertState::Disabled) {
        return Err(AlertError::InvalidStateChange(
            "Can't enable an alert which is not currently disabled".into(),
        ));
    }

    // validate that the user has access to the tables mentioned in the query
    user_auth_for_query(&session_key, alert.get_query()).await?;

    alerts
        .update_state(alert_id, AlertState::NotTriggered, Some("".into()))
        .await?;
    let alert = alerts.get_alert_by_id(alert_id).await?;

    Ok(web::Json(alert.to_alert_config().to_response()))
}

// PUT /alerts/{alert_id}
/// first save on disk, then in memory
/// then modify scheduled task
pub async fn modify_alert(
    req: HttpRequest,
    alert_id: Path<Ulid>,
    Json(alert_request): Json<AlertRequest>,
) -> Result<impl Responder, AlertError> {
    let session_key = extract_session_key_from_req(&req)?;
    let alert_id = alert_id.into_inner();

    // Get alerts manager reference without holding the global lock
    let alerts = {
        let guard = ALERTS.read().await;
        if let Some(alerts) = guard.as_ref() {
            alerts.clone()
        } else {
            return Err(AlertError::CustomError("No AlertManager set".into()));
        }
    };

    // Validate and prepare the new alert
    let alert = alerts.get_alert_by_id(alert_id).await?;
    user_auth_for_query(&session_key, alert.get_query()).await?;

    let mut new_config = alert_request.into().await?;
    if &new_config.alert_type != alert.get_alert_type() {
        return Err(AlertError::InvalidAlertModifyRequest);
    }

    user_auth_for_query(&session_key, &new_config.query).await?;

    // Calculate notification config
    let eval_freq = new_config.get_eval_frequency();
    let notif_freq = new_config.notification_config.interval;
    let times = if (eval_freq / notif_freq) == 0 {
        1
    } else {
        (eval_freq / notif_freq) as usize
    };
    new_config.notification_config.times = Retry::Finite(times);

    // Prepare the updated config
    let mut old_config = alert.to_alert_config();
    old_config.threshold_config = new_config.threshold_config;
    old_config.datasets = new_config.datasets;
    old_config.eval_config = new_config.eval_config;
    old_config.notification_config = new_config.notification_config;
    old_config.query = new_config.query;
    old_config.severity = new_config.severity;
    old_config.tags = new_config.tags;
    old_config.targets = new_config.targets;
    old_config.title = new_config.title;

    let new_alert: Box<dyn AlertTrait> = match &new_config.alert_type {
        AlertType::Threshold => Box::new(ThresholdAlert::from(old_config)) as Box<dyn AlertTrait>,
        AlertType::Anomaly(_) => {
            return Err(AlertError::NotPresentInOSS("anomaly"));
        }
        AlertType::Forecast(_) => {
            return Err(AlertError::NotPresentInOSS("forecast"));
        }
    };

    new_alert.validate(&session_key).await?;

    // Perform I/O operations
    PARSEABLE
        .metastore
        .put_alert(&new_alert.to_alert_config())
        .await?;

    let is_disabled = new_alert.get_state().eq(&AlertState::Disabled);
    // Now perform the atomic operations
    alerts.delete_task(alert_id).await?;
    alerts.delete(alert_id).await?;
    alerts.update(&*new_alert).await;

    // only restart the task if the state was not set to disabled
    if !is_disabled {
        alerts.start_task(new_alert.clone_box()).await?;
    }

    let config = new_alert.to_alert_config().to_response();
    Ok(web::Json(config))
}

// PUT /alerts/{alert_id}/evaluate_alert
pub async fn evaluate_alert(
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

    let alert = alerts.get_alert_by_id(alert_id).await?;

    user_auth_for_query(&session_key, alert.get_query()).await?;

    let config = alert.to_alert_config().to_response();

    // remove task
    alerts.delete_task(alert_id).await?;

    // add the task back again so that it evaluates right now
    alerts.start_task(alert).await?;

    Ok(Json(config))
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
