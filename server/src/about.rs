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
 *
 */

use chrono::Duration;
use chrono_humanize::{Accuracy, Tense};
use crossterm::style::Stylize;
use std::env;
use std::path::Path;
use sysinfo::SystemExt;
use ulid::Ulid;

use crate::analytics;
use crate::option::Config;
use crate::storage::StorageMetadata;
use crate::utils::update;

static K8S_ENV_TO_CHECK: &str = "KUBERNETES_SERVICE_HOST";

fn is_docker() -> bool {
    Path::new("/.dockerenv").exists()
}

fn is_k8s() -> bool {
    env::var(K8S_ENV_TO_CHECK).is_ok()
}

pub fn platform() -> &'static str {
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
pub fn user_agent(uid: &Ulid) -> String {
    analytics::refresh_sys_info();
    format!(
        "Parseable/{}/{}/{} ({:?}; {})",
        uid,
        current().released_version,
        current().commit_hash,
        analytics::SYS_INFO
            .lock()
            .unwrap()
            .name()
            .unwrap_or_default(),
        platform()
    )
}

pub struct ParseableVersion {
    pub released_version: semver::Version,
    pub commit_hash: String,
}

impl ParseableVersion {
    pub fn new(version: semver::Version, commit_hash: String) -> Self {
        ParseableVersion {
            released_version: version,
            commit_hash,
        }
    }
}

pub fn print_about(
    current_version: semver::Version,
    latest_release: Option<update::LatestRelease>,
    commit_hash: String,
) {
    eprint!(
        "
    {}
        Version:            \"v{}\"",
        "About:".to_string().bold(),
        current_version,
    );

    if let Some(latest_release) = latest_release {
        if latest_release.version > current_version {
            print_latest_release(latest_release);
        }
    }

    eprintln!(
        "
        Commit:             \"{commit_hash}\"
        Docs:               \"https://logg.ing/docs\""
    );
}

fn print_latest_release(latest_release: update::LatestRelease) {
    let time_since_latest_release = chrono::Utc::now() - latest_release.date;
    let time_since_latest_release = humanize_time(time_since_latest_release);
    let fmt_latest_version = format!(
        " ( v{} released {} ago. Download new release from https://github.com/parseablehq/parseable/releases/latest )",
        latest_release.version, time_since_latest_release
    );
    eprint!("{}", fmt_latest_version.red());
}

pub async fn print(config: &Config, meta: &StorageMetadata) {
    // print current version
    let current = current();
    let latest_release = if config.parseable.check_update {
        update::get_latest(&meta.deployment_id).await.ok()
    } else {
        None
    };

    print_about(
        current.released_version,
        latest_release,
        current.commit_hash,
    );
}

pub fn current() -> ParseableVersion {
    // CARGO_PKG_VERSION is set from Cargol.toml file at build time
    // We need to ensure [package].version in Cargo.toml is always valid semver
    let build_semver = env!("CARGO_PKG_VERSION");
    // VERGEN_GIT_SHA is set from build.rs at build time
    let sha_hash = env!("VERGEN_GIT_SHA");

    ParseableVersion::new(
        semver::Version::parse(build_semver).expect("CARGO_PKG_VERSION is always valid semver"),
        sha_hash.to_string(),
    )
}

fn humanize_time(time_passed: Duration) -> String {
    chrono_humanize::HumanTime::from(time_passed).to_text_en(Accuracy::Rough, Tense::Present)
}
