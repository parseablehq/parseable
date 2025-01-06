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

use actix_web::web::Json;
use serde_json::json;

use crate::{
    about::{self, get_latest_release},
    option::{Mode, CONFIG},
    storage::StorageMetadata,
};
use std::path::PathBuf;

/// {
///     "version": current_version,
///     "uiVersion": ui_version,
///     "commit": commit,
///     "deploymentId": deployment_id,
///     "updateAvailable": update_available,
///     "latestVersion": latest_release,
///     "llmActive": is_llm_active,
///     "llmProvider": llm_provider,
///     "oidcActive": is_oidc_active,
///     "license": "AGPL-3.0-only",
///     "mode": mode,
///     "staging": staging,
///     "grpcPort": grpc_port,
///     "store": {
///         "type": CONFIG.get_storage_mode_string(),
///         "path": store_endpoint
///     }
/// }
pub async fn about() -> Json<serde_json::Value> {
    let meta = StorageMetadata::global();

    let current_release = about::current();
    let latest_release = get_latest_release();
    let (update_available, latest_release) = match latest_release {
        Some(latest_release) => (
            latest_release.version > current_release.released_version,
            Some(format!("v{}", latest_release.version)),
        ),
        None => (false, None),
    };

    let current_version = format!("v{}", current_release.released_version);
    let commit = current_release.commit_hash;
    let deployment_id = meta.deployment_id.to_string();
    let mode = CONFIG.get_server_mode_string();
    let staging = if CONFIG.parseable.mode == Mode::Query {
        "".to_string()
    } else {
        CONFIG.staging_dir().display().to_string()
    };
    let grpc_port = CONFIG.parseable.grpc_port;

    let store_endpoint = CONFIG.storage().get_endpoint();
    let is_llm_active = &CONFIG.parseable.open_ai_key.is_some();
    let llm_provider = is_llm_active.then_some("OpenAI");
    let is_oidc_active = CONFIG.parseable.openid.is_some();
    let ui_version = option_env!("UI_VERSION").unwrap_or("development");

    let hot_tier_details: String = if CONFIG.hot_tier_dir().is_none() {
        "Disabled".to_string()
    } else {
        let hot_tier_dir: &Option<PathBuf> = CONFIG.hot_tier_dir();
        format!(
            "Enabled, Path: {}",
            hot_tier_dir.as_ref().unwrap().display(),
        )
    };

    let ms_clarity_tag = &CONFIG.parseable.ms_clarity_tag;
    let mut query_engine = "Parseable".to_string();
    if let (Some(_), Some(_), Some(_), Some(_)) = (
        CONFIG.parseable.trino_endpoint.as_ref(),
        CONFIG.parseable.trino_catalog.as_ref(),
        CONFIG.parseable.trino_schema.as_ref(),
        CONFIG.parseable.trino_username.as_ref(),
    ) {
        // Trino is enabled
        query_engine = "Trino".to_string();
    }

    Json(json!({
        "version": current_version,
        "uiVersion": ui_version,
        "commit": commit,
        "deploymentId": deployment_id,
        "updateAvailable": update_available,
        "latestVersion": latest_release,
        "llmActive": is_llm_active,
        "llmProvider": llm_provider,
        "oidcActive": is_oidc_active,
        "license": "AGPL-3.0-only",
        "mode": mode,
        "staging": staging,
        "hotTier": hot_tier_details,
        "grpcPort": grpc_port,
        "store": {
            "type": CONFIG.get_storage_mode_string(),
            "path": store_endpoint
        },
        "analytics": {
            "clarityTag": ms_clarity_tag
        },
        "queryEngine": query_engine

    }))
}
