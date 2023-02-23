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

use crate::banner::about::current;
use std::env;
use std::{path::Path, time::Duration};

use anyhow::anyhow;
use chrono::{DateTime, Utc};
use ulid::Ulid;

use crate::storage::StorageMetadata;

static K8S_ENV_TO_CHECK: &str = "KUBERNETES_SERVICE_HOST";

#[derive(Debug)]
pub struct LatestRelease {
    pub version: semver::Version,
    pub date: DateTime<Utc>,
}

fn is_docker() -> bool {
    Path::new("/.dockerenv").exists()
}

fn is_k8s() -> bool {
    env::var(K8S_ENV_TO_CHECK).is_ok()
}

fn platform() -> &'static str {
    if is_k8s() {
        "Kubernetes"
    } else if is_docker() {
        "Docker"
    } else {
        "Native"
    }
}

// User Agent for Download API call
// Format: Parseable/<UID>/<version>/<commit_hash> (<OS>; <Platform>)
fn user_agent(uid: &Ulid) -> String {
    let info = os_info::get();
    format!(
        "Parseable/{}/{}/{} ({}; {})",
        uid,
        current().0,
        current().1,
        info.os_type(),
        platform()
    )
}

pub async fn get_latest(meta: &StorageMetadata) -> Result<LatestRelease, anyhow::Error> {
    let agent = reqwest::ClientBuilder::new()
        .user_agent(user_agent(&meta.deployment_id))
        .timeout(Duration::from_secs(8))
        .build()
        .expect("client can be built on this system");

    let json: serde_json::Value = agent
        .get("https://download.parseable.io/latest-version")
        .send()
        .await?
        .json()
        .await?;

    let version = json["tag_name"]
        .as_str()
        .and_then(|ver| ver.strip_prefix('v'))
        .and_then(|ver| semver::Version::parse(ver).ok())
        .ok_or_else(|| anyhow!("Failed parsing version"))?;

    let date = json["published_at"]
        .as_str()
        .ok_or_else(|| anyhow!("Failed parsing published date"))?;

    let date = chrono::DateTime::parse_from_rfc3339(date)
        .expect("date-time from github is in rfc3339 format")
        .into();

    Ok(LatestRelease { version, date })
}
