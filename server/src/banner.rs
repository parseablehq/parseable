/*
 * Parseable Server (C) 2022 Parseable, Inc.
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
        Memory: {} GiB total",
        "System:".to_string().blue().bold(),
        os_info::get(),
        num_cpus::get(),
        num_cpus::get_physical(),
        system.total_memory() / (1024 * 1024)
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
    use semver::Version;

    use crate::utils::github;

    pub fn print() {
        // print current version
        let current_version = current_version();
        // not eprintln because if it is old release then time passed with be displayed beside it
        eprint!(
            "
    {} {} ",
            "Current Version:".to_string().blue().bold(),
            current_version
        );

        // check for latest release, if it cannot be fetched then print error as warn and return
        let latest_release = match github::get_latest() {
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

    fn current_version() -> Version {
        let current_version = env!("VERGEN_BUILD_SEMVER");
        semver::Version::parse(current_version).expect("VERGEN_BUILD_SEMVER is always valid semver")
    }

    fn humanize_time(time_passed: Duration) -> String {
        chrono_humanize::HumanTime::from(time_passed).to_text_en(Accuracy::Rough, Tense::Present)
    }
}
