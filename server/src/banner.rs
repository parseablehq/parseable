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

use crossterm::style::Stylize;
use sysinfo::{System, SystemExt};

pub fn system_info() {
    let system = System::new_all();
    eprintln!(
        "
    {}
        OS: {}
        Processor: {} logical, {} physical
        Memory: {:.2} GiB total",
        "System:".to_string().blue().bold(),
        os_info::get(),
        num_cpus::get(),
        num_cpus::get_physical(),
        system.total_memory() as f32 / (1024 * 1024 * 1024) as f32
    )
}

pub fn warning_line() {
    eprint!(
        "
    {}",
        "Warning:".to_string().red().bold(),
    );
}

pub mod version {
    use chrono::Duration;
    use chrono_humanize::{Accuracy, Tense};
    use crossterm::style::Stylize;
    use std::fmt;

    use crate::utils::update;

    pub enum ParseableVersion {
        Version(semver::Version),
        Prerelease(semver::Prerelease),
    }

    impl fmt::Display for ParseableVersion {
        fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
            match self {
                ParseableVersion::Version(v) => write!(f, "{}", v),
                ParseableVersion::Prerelease(p) => write!(f, "{}", p),
            }
        }
    }

    pub fn print() {
        // print current version
        let current = current();

        match current.0 {
            ParseableVersion::Version(current_version) => {
                // not eprintln because if it is old release then time passed with be displayed beside it
                eprint!(
                    "
    {} {} ",
                    "Current Version:".to_string().blue().bold(),
                    current_version
                );

                // check for latest release, if it cannot be fetched then print error as warn and return
                let latest_release = match update::get_latest() {
                    Ok(latest_release) => latest_release,
                    Err(e) => {
                        log::warn!("{}", e);
                        return;
                    }
                };

                if latest_release.version > current_version {
                    let time_since_latest_release = chrono::Utc::now() - latest_release.date;
                    let time_since_latest_release = humanize_time(time_since_latest_release);

                    let fmt_latest_version = format!(
                        "( v{} released {} ago )",
                        latest_release.version, time_since_latest_release
                    );

                    eprint!("{}", fmt_latest_version.yellow().bold());
                    eprintln!(
                        "
    Download latest version from https://github.com/parseablehq/parseable/releases/latest"
                    );
                } else {
                    eprintln!();
                }
            }
            ParseableVersion::Prerelease(current_prerelease) => {
                eprint!(
                    "
    {} {} ",
                    "Current Version:".to_string().blue().bold(),
                    current_prerelease
                );
            }
        }
    }

    pub fn current() -> (ParseableVersion, String) {
        let build_semver = env!("VERGEN_BUILD_SEMVER");
        let sha_hash = env!("VERGEN_GIT_SHA_SHORT");
        let mut git_semver = env!("VERGEN_GIT_SEMVER");

        if &git_semver[..1] == "v" {
            git_semver = &git_semver[1..];
        }

        if build_semver == git_semver {
            (
                ParseableVersion::Version(
                    semver::Version::parse(build_semver)
                        .expect("VERGEN_BUILD_SEMVER is always valid semver"),
                ),
                sha_hash.to_string(),
            )
        } else {
            (
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
}
