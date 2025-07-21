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
use actix_web::{HttpRequest, HttpResponse, web};
use std::{collections::HashMap, fs, process::Command};

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

    let url = &PARSEABLE.options.address;
    let username = &PARSEABLE.options.username;
    let password = &PARSEABLE.options.password;
    let scheme = PARSEABLE.options.get_scheme();
    let url = format!("{scheme}://{url}");

    match action.as_str() {
        "ingest" => match PARSEABLE.options.mode {
            Mode::Ingest | Mode::All => {
                // Fire the script execution asynchronously
                tokio::spawn(async move {
                    execute_demo_script(&action, &url, username, password).await
                });

                Ok(HttpResponse::Accepted().finish())
            }
            Mode::Query | Mode::Prism => {
                // Forward the request to ingestor asynchronously
                match get_demo_data_from_ingestor(&action).await {
                    Ok(()) => Ok(HttpResponse::Accepted().finish()),
                    Err(e) => Err(e),
                }
            }
            _ => Err(PostError::Invalid(anyhow::anyhow!(
                "Demo data is not available in this mode"
            ))),
        },
        "filters" | "alerts" | "dashboards" => {
            // Fire the script execution asynchronously
            tokio::spawn(
                async move { execute_demo_script(&action, &url, username, password).await },
            );

            Ok(HttpResponse::Accepted().finish())
        }
        _ => Err(PostError::InvalidQueryParameter),
    }
}

async fn execute_demo_script(
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
