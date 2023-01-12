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

use crate::utils::uid::Uid;
use crate::{option::Config, storage::StorageMetadata};

pub fn print(config: &Config, meta: StorageMetadata) {
    print_ascii_art();
    let scheme = config.parseable.get_scheme();
    status_info(config, &scheme, meta.deployment_id);
    storage_info(config);
    about::print(meta);
    println!();
}

fn print_ascii_art() {
    let ascii_name = r#"
    `7MM"""Mq.                                             *MM        `7MM           
      MM   `MM.                                             MM          MM           
      MM   ,M9  ,6"Yb.  `7Mb,od8 ,pP"Ybd  .gP"Ya   ,6"Yb.   MM,dMMb.    MM   .gP"Ya  
      MMmmdM9  8)   MM    MM' "' 8I   `" ,M'   Yb 8)   MM   MM    `Mb   MM  ,M'   Yb 
      MM        ,pm9MM    MM     `YMMMa. 8M""""""  ,pm9MM   MM     M8   MM  8M"""""" 
      MM       8M   MM    MM     L.   I8 YM.    , 8M   MM   MM.   ,M9   MM  YM.    , 
    .JMML.     `Moo9^Yo..JMML.   M9mmmP'  `Mbmmd' `Moo9^Yo. P^YbmdP'  .JMML. `Mbmmd'       
    "#;

    eprint!("{}", ascii_name);
    eprintln!(
        "
    Welcome to Parseable Server!"
    );
}

fn status_info(config: &Config, scheme: &str, id: Uid) {
    let url = format!("\"{}://{}\"", scheme, config.parseable.address).underlined();
    let mut credentials =
        String::from("\"As set in P_USERNAME and P_PASSWORD environment variables\"");

    if config.is_default_creds() {
        credentials = "\"Using default creds admin, admin. Please set credentials with P_USERNAME and P_PASSWORD.\"".red().to_string();
    }

    eprintln!(
        "
    {}
        URL:                {}
        Credentials:        {}
        Deployment UID:     \"{}\"",
        "Server:".to_string().bold(),
        url,
        credentials,
        id.to_string(),
    );
}

fn storage_info(config: &Config) {
    let mut mode = "S3 bucket";
    if config.storage_name == "drive" {
        mode = "Local drive";
    }
    eprintln!(
        "
    {}
        Mode:               \"{}\"
        Staging:            \"{}\"
        Store:              \"{}\"",
        "Storage:".to_string().cyan().bold(),
        mode,
        config.staging_dir().to_string_lossy(),
        config.storage().get_endpoint(),
    )
}

pub mod about {
    use chrono::Duration;
    use chrono_humanize::{Accuracy, Tense};
    use crossterm::style::Stylize;
    use std::fmt;

    use crate::storage::StorageMetadata;
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

    pub fn print_about(
        current_version: semver::Version,
        commit_hash: String,
        meta: StorageMetadata,
    ) {
        eprint!(
            "
    {}
        Version:            \"v{}\"",
            "About:".to_string().bold(),
            current_version,
        );

        if let Ok(latest_release) = update::get_latest(meta) {
            if latest_release.version > current_version {
                print_latest_release(latest_release);
            }
        }

        eprintln!(
            "
        Commit:             \"{}\"
        Docs:               \"https://www.parseable.io/docs\"",
            commit_hash
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

    pub fn print(meta: StorageMetadata) {
        // print current version
        let current = current();

        match current.0 {
            ParseableVersion::Version(current_version) => {
                print_about(current_version, current.1, meta);
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
