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
 *
 */

use chrono::Duration;
use chrono_humanize::{Accuracy, Tense};
use crossterm::style::Stylize;
use std::path::Path;
use std::{env, fmt};
use sysinfo::SystemExt;
use ulid::Ulid;

use crate::analytics;
use crate::option::Config;
use crate::storage::StorageMetadata;
use crate::utils::update;

static K8S_ENV_TO_CHECK: &str = "KUBERNETES_SERVICE_HOST";
pub enum ParseableVersion {
    Version(semver::Version),
    Prerelease(semver::Prerelease),
}

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

pub struct Version {
    pub released_version: ParseableVersion,
    pub commit_hash: String,
}

impl Version {
    pub fn new(version: ParseableVersion, commit_hash: String) -> Self {
        Version {
            released_version: version,
            commit_hash,
        }
    }
}

impl fmt::Display for ParseableVersion {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ParseableVersion::Version(v) => write!(f, "{v}"),
            ParseableVersion::Prerelease(p) => write!(f, "{p}"),
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
        Docs:               \"https://www.parseable.io/docs\""
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
        update::get_latest(meta).await.ok()
    } else {
        None
    };

    match current.released_version {
        ParseableVersion::Version(current_version) => {
            print_about(current_version, latest_release, current.commit_hash);
        }
        ParseableVersion::Prerelease(current_prerelease) => {
            eprintln!(
                "
{} {} ",
                "Current Version:".to_string().blue().bold(),
                current_prerelease
            );
        }
    }
}

pub fn current() -> Version {
    let build_semver = env!("VERGEN_BUILD_SEMVER");
    let sha_hash = env!("VERGEN_GIT_SHA_SHORT");
    let mut git_semver = env!("VERGEN_GIT_SEMVER");

    if &git_semver[..1] == "v" {
        git_semver = &git_semver[1..];
    }

    if build_semver == git_semver {
        Version::new(
            ParseableVersion::Version(
                semver::Version::parse(build_semver)
                    .expect("VERGEN_BUILD_SEMVER is always valid semver"),
            ),
            sha_hash.to_string(),
        )
    } else {
        Version::new(
            ParseableVersion::Prerelease(
                semver::Prerelease::new(git_semver)
                    .expect("VERGEN_GIT_SEMVER is always valid semver"),
            ),
            sha_hash.to_string(),
        )
    }
}

fn humanize_time(time_passed: Duration) -> String {
    chrono_humanize::HumanTime::from(time_passed).to_text_en(Accuracy::Rough, Tense::Present)
}
