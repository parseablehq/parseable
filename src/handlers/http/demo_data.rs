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
    handlers::http::{cluster::get_demo_data_from_ingestor, ingest::PostError},
    option::Mode,
    parseable::PARSEABLE,
};
use actix_web::{web, HttpRequest, HttpResponse};
use std::{collections::HashMap, env, os::unix::fs::PermissionsExt, process::Command};

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

    let url = &PARSEABLE.options.address;
    let username = &PARSEABLE.options.username;
    let password = &PARSEABLE.options.password;
    let scheme = PARSEABLE.options.get_scheme();
    let url = format!("{scheme}://{url}");

    match action.as_str() {
        "ingest" => match PARSEABLE.options.mode {
            Mode::Ingest | Mode::All => {
                let script_path = get_script_path(&action)?;

                // Fire the script execution asynchronously
                tokio::spawn(async move {
                    if let Err(e) =
                        execute_demo_script(&script_path, &url, username, password).await
                    {
                        tracing::warn!("Failed to execute demo script: {}", e);
                    }
                });

                Ok(HttpResponse::Accepted().finish())
            }
            Mode::Query | Mode::Prism => {
                // Forward the request to ingestor asynchronously
                tokio::spawn(async move {
                    if let Err(e) = get_demo_data_from_ingestor(&action).await {
                        tracing::warn!("Failed to forward request to ingestor: {}", e);
                    }
                });

                Ok(HttpResponse::Accepted().finish())
            }
            _ => Err(PostError::Invalid(anyhow::anyhow!(
                "Demo data is not available in this mode"
            ))),
        },
        "filters" => {
            let script_path = get_script_path(&action)?;

            // Fire the script execution asynchronously
            tokio::spawn(async move {
                if let Err(e) = execute_demo_script(&script_path, &url, username, password).await {
                    tracing::warn!("Failed to execute demo script: {}", e);
                }
            });

            Ok(HttpResponse::Accepted().finish())
        }
        _ => Err(PostError::InvalidQueryParameter),
    }
}

async fn execute_demo_script(
    script_path: &str,
    url: &str,
    username: &str,
    password: &str,
) -> Result<(), anyhow::Error> {
    // Ensure the script exists and has correct permissions set during deployment
    let metadata = std::fs::metadata(script_path)
        .map_err(|e| anyhow::anyhow!("Failed to read script metadata: {}", e))?;

    if metadata.permissions().mode() & 0o111 == 0 {
        return Err(anyhow::anyhow!("Script is not executable: {}", script_path));
    }
    // Execute the script with environment variables
    if let Err(e) = Command::new("bash")
        .arg(script_path)
        .arg("--silent")
        .env("P_URL", url)
        .env("P_USERNAME", username)
        .env("P_PASSWORD", password)
        .output()
    {
        return Err(anyhow::anyhow!("Failed to execute script: {}", e));
    }

    Ok(())
}

fn get_script_path(action: &str) -> Result<String, anyhow::Error> {
    // Get the current working directory (where cargo run is executed from)
    let current_dir = env::current_dir()
        .map_err(|e| anyhow::anyhow!("Failed to get current directory: {}", e))?;
    // Construct the path to the script based on the action
    let path = match action {
        "ingest" => "resources/ingest_demo_data.sh",
        "filters" => "resources/filters_demo_data.sh",
        _ => return Err(anyhow::anyhow!("Invalid action: {}", action)),
    };
    let full_path = current_dir.join(path);
    if full_path.exists() {
        return Ok(full_path.to_string_lossy().to_string());
    }

    Err(anyhow::anyhow!("Script not found"))
}
