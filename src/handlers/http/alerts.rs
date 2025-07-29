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

use crate::alerts::{ALERTS, AlertConfig, AlertError, AlertRequest, AlertState};

// GET /alerts
/// User needs at least a read access to the stream(s) that is being referenced in an alert
/// Read all alerts then return alerts which satisfy the condition
pub async fn list(req: HttpRequest) -> Result<impl Responder, AlertError> {
    let session_key = extract_session_key_from_req(&req)?;
    let query_map = web::Query::<HashMap<String, String>>::from_query(req.query_string())
        .map_err(|_| AlertError::InvalidQueryParameter)?;
    let mut tags_list = Vec::new();
    if !query_map.is_empty() {
        if let Some(tags) = query_map.get("tags") {
            tags_list = tags
                .split(',')
                .map(|s| s.trim().to_string())
                .filter(|s| !s.is_empty())
                .collect();
            if tags_list.is_empty() {
                return Err(AlertError::InvalidQueryParameter);
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
    let alerts_summary = alerts
        .iter()
        .map(|alert| alert.to_summary())
        .collect::<Vec<_>>();
    Ok(web::Json(alerts_summary))
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
    user_auth_for_query(&session_key, &alert.query).await?;

    Ok(web::Json(alert))
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
    user_auth_for_query(&session_key, &alert.query).await?;

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
    let alert = alerts.get_alert_by_id(alert_id).await?;
    // validate that the user has access to the tables mentioned in the query
    user_auth_for_query(&session_key, &alert.query).await?;

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

    // get current state
    let current_state = alerts.get_state(alert_id).await?;

    let new_state = AlertState::from_str(state_value)?;

    current_state.update_state(new_state, alert_id).await?;
    let alert = alerts.get_alert_by_id(alert_id).await?;
    Ok(web::Json(alert))
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
