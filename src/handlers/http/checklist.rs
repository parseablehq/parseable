/*
 * Parseable Server (C) 2022 - 2025 Parseable, Inc.
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

use actix_web::{HttpResponse, Responder, http::StatusCode, web};
use serde::{Deserialize, Serialize};

use crate::{alerts::ALERTS, handlers::TelemetryType, parseable::PARSEABLE, rbac::map::users};

#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ChecklistResponse {
    pub data_ingested: bool,
    pub metrics_dataset: bool,
    pub keystone_created: bool,
    pub alert_created: bool,
    pub user_added: bool,
}

#[derive(Debug, thiserror::Error)]
pub enum ChecklistError {
    #[error("Failed to list streams: {0}")]
    StreamError(String),
    #[error("Internal error: {0}")]
    InternalError(String),
}

impl actix_web::ResponseError for ChecklistError {
    fn status_code(&self) -> StatusCode {
        StatusCode::INTERNAL_SERVER_ERROR
    }

    fn error_response(&self) -> HttpResponse {
        HttpResponse::build(self.status_code()).json(serde_json::json!({
            "error": self.to_string()
        }))
    }
}

/// GET /home/checklist
/// Returns a checklist of boolean values indicating setup status
/// Note: keystoneCreated is always false for OSS (enterprise-only feature)
pub async fn get_checklist() -> Result<impl Responder, ChecklistError> {
    // Check if any streams exist (dataIngested)
    let streams = PARSEABLE
        .metastore
        .list_streams()
        .await
        .map_err(|e| ChecklistError::StreamError(e.to_string()))?;

    let data_ingested = !streams.is_empty();

    // Check if any stream has telemetry_type = Metrics (metricsDataset)
    let mut metrics_dataset = false;
    for stream_name in &streams {
        if PARSEABLE.check_or_load_stream(stream_name).await
            && let Ok(stream) = PARSEABLE.get_stream(stream_name)
        {
            match stream.metadata.read() {
                Ok(metadata_guard) => {
                    if metadata_guard.telemetry_type == TelemetryType::Metrics {
                        metrics_dataset = true;
                        break;
                    }
                }
                Err(e) => {
                    // Optionally log the error, skip this stream
                    tracing::warn!(
                        "Failed to read metadata for stream '{}': {}",
                        stream_name,
                        e
                    );
                    continue;
                }
            }
        }
    }

    // keystoneCreated is always false for OSS (enterprise-only feature)
    let keystone_created = false;

    // Check if any alerts exist (alertCreated)
    let alert_created = {
        let guard = ALERTS.read().await;
        if let Some(alerts) = guard.as_ref() {
            let all_alerts = alerts.get_all_alerts().await;
            !all_alerts.is_empty()
        } else {
            false
        }
    };

    // Check if any users exist (userAdded)
    let user_count = users().len();
    let user_added = user_count > 1; // more than just the default admin user

    let response = ChecklistResponse {
        data_ingested,
        metrics_dataset,
        keystone_created,
        alert_created,
        user_added,
    };

    Ok(web::Json(response))
}
