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

use std::time::Duration;

use anyhow::anyhow;
use chrono::{DateTime, Utc};
use http::header;

use crate::{about, parseable::PARSEABLE};

use super::uid;

#[derive(Debug, Clone)]
pub struct LatestRelease {
    pub version: semver::Version,
    pub date: DateTime<Utc>,
}

pub async fn get_latest(deployment_id: &uid::Uid) -> Result<LatestRelease, anyhow::Error> {
    let send_analytics = PARSEABLE.options.send_analytics.to_string();
    let mut headers = header::HeaderMap::new();
    headers.insert(
        "P_SEND_ANONYMOUS_USAGE_DATA",
        header::HeaderValue::from_str(send_analytics.as_str()).expect("valid header value"),
    );
    let agent = reqwest::ClientBuilder::new()
        .user_agent(about::user_agent(deployment_id))
        .default_headers(headers)
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
