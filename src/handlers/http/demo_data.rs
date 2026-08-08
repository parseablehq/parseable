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

use crate::{
    handlers::http::{
        cluster::{get_node_info, utils::check_liveness},
        ingest::PostError,
        modal::{NodeMetadata, NodeType},
    },
    option::Mode,
    parseable::PARSEABLE,
    utils::get_tenant_id_from_request,
};
use actix_web::{HttpRequest, HttpResponse, web};
use std::{collections::HashMap, fs, process::Command};
use tracing::error;

#[cfg(unix)]
use std::os::unix::fs::PermissionsExt;

// Embed the scripts at compile time
const DEMO_SCRIPT: &str = include_str!("../../../resources/ingest_demo_data.sh");

pub async fn get_demo_data(req: HttpRequest) -> Result<HttpResponse, PostError> {
    let query_map = web::Query::<HashMap<String, String>>::from_query(req.query_string())
        .map_err(|_| PostError::InvalidQueryParameter)?;

    if query_map.is_empty() {
        return Err(PostError::MissingQueryParameter);
    }

    let action = query_map
        .get("action")
        .cloned()
        .ok_or(PostError::MissingQueryParameter)?;

    let username = PARSEABLE.options.username.clone();
    let password = PARSEABLE.options.password.clone();
    let scheme = PARSEABLE.options.get_scheme();
    let standalone_url = format!("{scheme}://{}", PARSEABLE.options.address);
    let tenant_id = get_tenant_id_from_request(&req);
    match action.as_str() {
        "ingest" => match PARSEABLE.options.mode {
            Mode::All => {
                spawn_demo_script(action, standalone_url, username, password);

                Ok(HttpResponse::Accepted().finish())
            }
            Mode::Query | Mode::Prism => {
                let ingestor_url = get_live_ingestor_url(&tenant_id).await?;
                // Execute on Query/Prism; the script sends data to the ingestor.
                spawn_demo_script(action, ingestor_url, username, password);

                Ok(HttpResponse::Accepted().finish())
            }
            _ => Err(PostError::Invalid(anyhow::anyhow!(
                "Demo data is not available in this mode"
            ))),
        },
        "filters" | "alerts" | "dashboards" => {
            spawn_demo_script(action, standalone_url, username, password);

            Ok(HttpResponse::Accepted().finish())
        }
        _ => Err(PostError::InvalidQueryParameter),
    }
}

async fn get_live_ingestor_url(tenant_id: &Option<String>) -> Result<String, PostError> {
    let mut ingestors: Vec<NodeMetadata> = get_node_info(NodeType::Ingestor, tenant_id)
        .await
        .map_err(PostError::Invalid)?;
    ingestors.sort_by(|left, right| left.domain_name.cmp(&right.domain_name));

    for ingestor in ingestors {
        if check_liveness(&ingestor.domain_name).await {
            return Ok(ingestor.domain_name.trim_end_matches('/').to_string());
        }
    }

    Err(PostError::Invalid(anyhow::anyhow!(
        "No live ingestors found"
    )))
}

fn spawn_demo_script(action: String, url: String, username: String, password: String) {
    tokio::spawn(async move {
        let result = tokio::task::spawn_blocking(move || {
            execute_demo_script(&action, &url, &username, &password)
        })
        .await;

        match result {
            Ok(Ok(())) => {}
            Ok(Err(error)) => error!(%error, "demo data script failed"),
            Err(error) => error!(%error, "demo data script task failed"),
        }
    });
}

fn execute_demo_script(
    action: &str,
    url: &str,
    username: &str,
    password: &str,
) -> Result<(), anyhow::Error> {
    // Create a temporary file to write the script
    let temp_file = tempfile::NamedTempFile::new()
        .map_err(|e| anyhow::anyhow!("Failed to create temporary file: {}", e))?;

    let temp_path = temp_file.path();
    // Write the script content to the temporary file
    fs::write(temp_path, DEMO_SCRIPT)
        .map_err(|e| anyhow::anyhow!("Failed to write script to temp file: {}", e))?;

    // Make the temporary file executable (Unix only)
    #[cfg(unix)]
    {
        let mut permissions = fs::metadata(temp_path)
            .map_err(|e| anyhow::anyhow!("Failed to read temp file metadata: {}", e))?
            .permissions();
        permissions.set_mode(0o755);
        fs::set_permissions(temp_path, permissions)
            .map_err(|e| anyhow::anyhow!("Failed to set temp file permissions: {}", e))?;
    }

    let output = Command::new("bash")
        .arg(temp_path)
        .env("P_URL", url)
        .env("P_USERNAME", username)
        .env("P_PASSWORD", password)
        .env("ACTION", action)
        .output()
        .map_err(|e| {
            anyhow::anyhow!(
                "Failed to execute script: {}. Make sure bash is available.",
                e
            )
        })?;

    drop(temp_file);

    if !output.status.success() {
        let stderr = String::from_utf8_lossy(&output.stderr);
        let stdout = String::from_utf8_lossy(&output.stdout);
        return Err(anyhow::anyhow!(
            "Script execution failed. Exit code: {:?}, stdout: {}, stderr: {}",
            output.status.code(),
            stdout,
            stderr
        ));
    }

    Ok(())
}
