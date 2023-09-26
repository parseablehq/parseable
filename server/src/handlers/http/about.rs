/*
 * Parseable Server (C) 2022 - 2023 Parseable, Inc.
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

use crate::{about, option::CONFIG, storage::StorageMetadata, utils::update};

pub async fn about() -> Json<serde_json::Value> {
    let meta = StorageMetadata::global();

    let current_release = about::current();
    let latest_release = update::get_latest(&meta.deployment_id).await;

    let (update_available, latest_release) = match latest_release {
        Ok(latest_release) => (
            latest_release.version > current_release.released_version,
            Some(format!("v{}", latest_release.version)),
        ),
        Err(_) => (false, None),
    };

    let current_version = format!("v{}", current_release.released_version);
    let commit = current_release.commit_hash;
    let deployment_id = meta.deployment_id.to_string();
    let mode = CONFIG.mode_string();
    let staging = CONFIG.staging_dir();

    let store = CONFIG.storage().get_endpoint();
    let is_llm_active = &CONFIG.parseable.open_ai_key.is_some();
    let llm_provider = is_llm_active.then_some("OpenAI");
    let ui_version = option_env!("UI_VERSION").unwrap_or("development");

    Json(json!({
        "version": current_version,
        "uiVersion": ui_version,
        "commit": commit,
        "deploymentId": deployment_id,
        "updateAvailable": update_available,
        "latestVersion": latest_release,
        "llmActive": is_llm_active,
        "llmProvider": llm_provider,
        "license": "AGPL-3.0-only",
        "mode": mode,
        "staging": staging,
        "store": store
    }))
}
