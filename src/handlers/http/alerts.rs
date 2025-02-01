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

use crate::{
    parseable::PARSEABLE, storage::object_storage::alert_json_path, sync::schedule_alert_task,
    utils::actix::extract_session_key_from_req,
};
use actix_web::{
    web::{self, Json, Path},
    HttpRequest, Responder,
};
use bytes::Bytes;
use ulid::Ulid;

use crate::alerts::{
    alerts_utils::user_auth_for_query, AlertConfig, AlertError, AlertRequest, AlertState, ALERTS,
};

// GET /alerts
/// User needs at least a read access to the stream(s) that is being referenced in an alert
/// Read all alerts then return alerts which satisfy the condition
pub async fn list(req: HttpRequest) -> Result<impl Responder, AlertError> {
    let session_key = extract_session_key_from_req(&req)?;
    let alerts = ALERTS.list_alerts_for_user(session_key).await?;

    Ok(web::Json(alerts))
}

// POST /alerts
pub async fn post(
    req: HttpRequest,
    Json(alert): Json<AlertRequest>,
) -> Result<impl Responder, AlertError> {
    let alert: AlertConfig = alert.into();
    alert.validate().await?;

    // validate the incoming alert query
    // does the user have access to these tables or not?
    let session_key = extract_session_key_from_req(&req)?;
    user_auth_for_query(&session_key, &alert.query).await?;

    // create scheduled tasks
    let (handle, rx, tx) = schedule_alert_task(alert.get_eval_frequency(), alert.clone()).await?;

    // now that we've validated that the user can run this query
    // move on to saving the alert in ObjectStore
    ALERTS.update(&alert).await;

    let path = alert_json_path(alert.id);

    let store = PARSEABLE.storage().get_object_store();
    let alert_bytes = serde_json::to_vec(&alert)?;
    store.put_object(&path, Bytes::from(alert_bytes)).await?;

    ALERTS.update_task(alert.id, handle, rx, tx).await;

    Ok(web::Json(alert))
}

// GET /alerts/{alert_id}
pub async fn get(req: HttpRequest, alert_id: Path<Ulid>) -> Result<impl Responder, AlertError> {
    let session_key = extract_session_key_from_req(&req)?;
    let alert_id = alert_id.into_inner();

    let alert = ALERTS.get_alert_by_id(alert_id).await?;
    // validate that the user has access to the tables mentioned
    user_auth_for_query(&session_key, &alert.query).await?;

    Ok(web::Json(alert))
}

// DELETE /alerts/{alert_id}
/// Deletion should happen from disk, sheduled tasks, then memory
pub async fn delete(req: HttpRequest, alert_id: Path<Ulid>) -> Result<impl Responder, AlertError> {
    let session_key = extract_session_key_from_req(&req)?;
    let alert_id = alert_id.into_inner();

    let alert = ALERTS.get_alert_by_id(alert_id).await?;

    // validate that the user has access to the tables mentioned
    user_auth_for_query(&session_key, &alert.query).await?;

    let store = PARSEABLE.storage().get_object_store();
    let alert_path = alert_json_path(alert_id);

    // delete from disk
    store
        .delete_object(&alert_path)
        .await
        .map_err(AlertError::ObjectStorage)?;

    // delete from disk and memory
    ALERTS.delete(alert_id).await?;

    // delete the scheduled task
    ALERTS.delete_task(alert_id).await?;

    Ok(format!("Deleted alert with ID- {alert_id}"))
}

// PUT /alerts/{alert_id}
/// first save on disk, then in memory
/// then modify scheduled task
pub async fn modify(
    req: HttpRequest,
    alert_id: Path<Ulid>,
    Json(alert_request): Json<AlertRequest>,
) -> Result<impl Responder, AlertError> {
    let session_key = extract_session_key_from_req(&req)?;
    let alert_id = alert_id.into_inner();

    // check if alert id exists in map
    let mut alert = ALERTS.get_alert_by_id(alert_id).await?;

    // validate that the user has access to the tables mentioned
    // in the old as well as the modified alert
    user_auth_for_query(&session_key, &alert.query).await?;
    user_auth_for_query(&session_key, &alert_request.query).await?;

    alert.modify(alert_request);
    alert.validate().await?;

    // modify task
    let (handle, rx, tx) = schedule_alert_task(alert.get_eval_frequency(), alert.clone()).await?;

    // modify on disk
    PARSEABLE
        .storage()
        .get_object_store()
        .put_alert(alert.id, &alert)
        .await?;

    // modify in memory
    ALERTS.update(&alert).await;

    ALERTS.update_task(alert.id, handle, rx, tx).await;

    Ok(web::Json(alert))
}

// PUT /alerts/{alert_id}/update_state
pub async fn update_state(
    req: HttpRequest,
    alert_id: Path<Ulid>,
    state: String,
) -> Result<impl Responder, AlertError> {
    let session_key = extract_session_key_from_req(&req)?;
    let alert_id = alert_id.into_inner();

    // check if alert id exists in map
    let alert = ALERTS.get_alert_by_id(alert_id).await?;

    // validate that the user has access to the tables mentioned
    user_auth_for_query(&session_key, &alert.query).await?;

    // get current state
    let current_state = ALERTS.get_state(alert_id).await?;

    let new_state: AlertState = serde_json::from_str(&state)?;

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

    Ok("")
}
