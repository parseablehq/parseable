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

pub mod actix;
pub mod arrow;
pub mod header_parsing;
pub mod human_size;
pub mod json;
pub mod time;
pub mod uid;
pub mod update;

use crate::handlers::http::rbac::RBACError;
use crate::option::CONFIG;
use crate::rbac::role::{Action, Permission};
use crate::rbac::Users;
use actix::extract_session_key_from_req;
use actix_web::HttpRequest;
use chrono::{DateTime, NaiveDate, NaiveDateTime, NaiveTime, TimeDelta, Timelike, Utc};
use regex::Regex;
use sha2::{Digest, Sha256};
use std::env;
use tracing::debug;
use url::Url;

/// Convert minutes to a slot range
/// e.g. given minute = 15 and OBJECT_STORE_DATA_GRANULARITY = 10 returns "10-19"
pub fn minute_to_slot(minute: u32, data_granularity: u32) -> Option<String> {
    if minute >= 60 {
        return None;
    }

    let block_n = minute / data_granularity;
    let block_start = block_n * data_granularity;
    if data_granularity == 1 {
        return Some(format!("{block_start:02}"));
    }

    let block_end = (block_n + 1) * data_granularity - 1;
    Some(format!("{block_start:02}-{block_end:02}"))
}

type Prefix = String;

#[derive(Clone, Copy)]
struct TimeBounds {
    start_date: NaiveDate,
    start_hour: u32,
    start_minute: u32,
    end_date: NaiveDate,
    end_hour: u32,
    end_minute: u32,
}

impl TimeBounds {
    fn spans_full_day(&self) -> bool {
        self.end_hour - self.start_hour >= 24
    }
}

/// Representation of a time period using which files can be retreived from object storage
pub struct TimePeriod {
    start: DateTime<Utc>,
    end: DateTime<Utc>,
    data_granularity: u32,
}

impl TimePeriod {
    pub fn new(start: DateTime<Utc>, end: DateTime<Utc>, data_granularity: u32) -> Self {
        Self {
            data_granularity,
            start,
            end,
        }
    }

    /// Generates prefixes for the time period, e.g: 
    /// 1. ("2022-06-11T23:00:01+00:00", "2022-06-12T01:59:59+00:00") => ["date=2022-06-11/hour=23/", "date=2022-06-12/hour=00/", "date=2022-06-12/hour=01/""]
    /// 2. ("2022-06-11T15:59:00+00:00", "2022-06-11T17:01:00+00:00") => ["date=2022-06-11/hour=15/minute=59/", "date=2022-06-11/hour=16/", "date=2022-06-11/hour=17/minute=00/"]
    pub fn generate_prefixes(self) -> Vec<Prefix> {
        let mut prefixes = vec![];
        let time_bounds = self.calculate_time_bounds();
        let mut current_date = time_bounds.start_date;

        while current_date <= time_bounds.end_date {
            self.process_date(current_date, time_bounds, &mut prefixes);
            current_date += TimeDelta::days(1);
        }

        prefixes
    }

    fn calculate_time_bounds(&self) -> TimeBounds {
        TimeBounds {
            start_date: self.start.date_naive(),
            start_hour: self.start.hour(),
            start_minute: self.start.minute(),
            end_date: self.end.date_naive(),
            end_hour: self.end.hour(),
            end_minute: self.end.minute() + u32::from(self.end.second() > 0),
        }
    }

    fn process_date(&self, date: NaiveDate, bounds: TimeBounds, prefixes: &mut Vec<String>) {
        let prefix = format!("date={date}/");
        let is_start = date == bounds.start_date;
        let is_end = date == bounds.end_date;

        if !is_start && !is_end {
            prefixes.push(prefix);
            return;
        }

        let time_bounds = self.get_time_bounds(is_start, is_end, bounds);
        if time_bounds.spans_full_day() {
            prefixes.push(prefix);
            return;
        }

        self.process_hours(prefix, time_bounds, prefixes);
    }

    fn process_hours(
        &self,
        date_prefix: String,
        time_bounds: TimeBounds,
        prefixes: &mut Vec<String>,
    ) {
        for hour in time_bounds.start_hour..=time_bounds.end_hour {
            if hour == 24 {
                break;
            }

            let hour_prefix = format!("{date_prefix}hour={hour:02}/");
            let is_start_hour = hour == time_bounds.start_hour;
            let is_end_hour = hour == time_bounds.end_hour;

            if !is_start_hour && !is_end_hour {
                prefixes.push(hour_prefix);
                continue;
            }

            self.process_minutes(
                hour_prefix,
                is_start_hour,
                is_end_hour,
                time_bounds,
                prefixes,
            );
        }
    }

    fn process_minutes(
        &self,
        hour_prefix: String,
        is_start_hour: bool,
        is_end_hour: bool,
        mut time_bounds: TimeBounds,
        prefixes: &mut Vec<String>,
    ) {
        if !is_start_hour {
            time_bounds.start_minute = 0;
        }
        if !is_end_hour {
            time_bounds.end_minute = 60;
        };

        if time_bounds.start_minute == time_bounds.end_minute {
            return;
        }

        let (start_block, end_block) = (
            time_bounds.start_minute / self.data_granularity,
            time_bounds.end_minute / self.data_granularity,
        );

        let forbidden_block = 60 / self.data_granularity;
        if end_block - start_block >= forbidden_block {
            prefixes.push(hour_prefix);
            return;
        }

        self.generate_minute_prefixes(hour_prefix, start_block, end_block, prefixes);
    }

    fn generate_minute_prefixes(
        &self,
        hour_prefix: String,
        start_block: u32,
        end_block: u32,
        prefixes: &mut Vec<String>,
    ) {
        let mut push_prefix = |block: u32| {
            if let Some(minute_slot) =
                minute_to_slot(block * self.data_granularity, self.data_granularity)
            {
                let prefix = format!("{hour_prefix}minute={minute_slot}/");
                prefixes.push(prefix);
            }
        };

        for block in start_block..end_block {
            push_prefix(block);
        }

        // Handle last block for granularity > 1
        if self.data_granularity > 1 {
            push_prefix(end_block);
        }
    }

    fn get_time_bounds(
        &self,
        is_start: bool,
        is_end: bool,
        mut time_bounds: TimeBounds,
    ) -> TimeBounds {
        if !is_start {
            time_bounds.start_hour = 0;
            time_bounds.start_minute = 0;
        }

        if !is_end {
            time_bounds.end_hour = 24;
            time_bounds.end_minute = 60;
        }
        time_bounds
    }
}

pub fn get_url() -> Url {
    if CONFIG.options.ingestor_endpoint.is_empty() {
        return format!(
            "{}://{}",
            CONFIG.options.get_scheme(),
            CONFIG.options.address
        )
        .parse::<Url>() // if the value was improperly set, this will panic before hand
        .unwrap_or_else(|err| {
            panic!("{err}, failed to parse `{}` as Url. Please set the environment variable `P_ADDR` to `<ip address>:<port>` without the scheme (e.g., 192.168.1.1:8000). Please refer to the documentation: https://logg.ing/env for more details.", CONFIG.options.address)
        });
    }

    let ingestor_endpoint = &CONFIG.options.ingestor_endpoint;

    if ingestor_endpoint.starts_with("http") {
        panic!("Invalid value `{}`, please set the environement variable `P_INGESTOR_ENDPOINT` to `<ip address / DNS>:<port>` without the scheme (e.g., 192.168.1.1:8000 or example.com:8000). Please refer to the documentation: https://logg.ing/env for more details.", ingestor_endpoint);
    }

    let addr_from_env = ingestor_endpoint.split(':').collect::<Vec<&str>>();

    if addr_from_env.len() != 2 {
        panic!("Invalid value `{}`, please set the environement variable `P_INGESTOR_ENDPOINT` to `<ip address / DNS>:<port>` without the scheme (e.g., 192.168.1.1:8000 or example.com:8000). Please refer to the documentation: https://logg.ing/env for more details.", ingestor_endpoint);
    }

    let mut hostname = addr_from_env[0].to_string();
    let mut port = addr_from_env[1].to_string();

    // if the env var value fits the pattern $VAR_NAME:$VAR_NAME
    // fetch the value from the specified env vars
    if hostname.starts_with('$') {
        let var_hostname = hostname[1..].to_string();
        hostname = get_from_env(&var_hostname);

        if hostname.is_empty() {
            panic!("The environement variable `{}` is not set, please set as <ip address / DNS> without the scheme (e.g., 192.168.1.1 or example.com). Please refer to the documentation: https://logg.ing/env for more details.", var_hostname);
        }
        if hostname.starts_with("http") {
            panic!("Invalid value `{}`, please set the environement variable `{}` to `<ip address / DNS>` without the scheme (e.g., 192.168.1.1 or example.com). Please refer to the documentation: https://logg.ing/env for more details.", hostname, var_hostname);
        } else {
            hostname = format!("{}://{}", CONFIG.options.get_scheme(), hostname);
        }
    }

    if port.starts_with('$') {
        let var_port = port[1..].to_string();
        port = get_from_env(&var_port);

        if port.is_empty() {
            panic!(
                "Port is not set in the environement variable `{}`. Please refer to the documentation: https://logg.ing/env for more details.",
                var_port
            );
        }
    }

    format!("{}://{}:{}", CONFIG.options.get_scheme(), hostname, port)
        .parse::<Url>()
        .expect("Valid URL")
}

/// util fuction to fetch value from an env var
fn get_from_env(var_to_fetch: &str) -> String {
    env::var(var_to_fetch).unwrap_or_else(|_| "".to_string())
}

pub fn get_ingestor_id() -> String {
    let now = Utc::now().to_rfc3339().to_string();
    let mut hasher = Sha256::new();
    hasher.update(now);
    let result = format!("{:x}", hasher.finalize());
    let result = result.split_at(15).0.to_string();
    debug!("Ingestor ID: {}", &result);
    result
}

pub fn extract_datetime(path: &str) -> Option<NaiveDateTime> {
    let re = Regex::new(r"date=(\d{4}-\d{2}-\d{2})/hour=(\d{2})/minute=(\d{2})").unwrap();
    if let Some(caps) = re.captures(path) {
        let date_str = caps.get(1)?.as_str();
        let hour_str = caps.get(2)?.as_str();
        let minute_str = caps.get(3)?.as_str();

        let date = NaiveDate::parse_from_str(date_str, "%Y-%m-%d").ok()?;
        let time =
            NaiveTime::parse_from_str(&format!("{}:{}", hour_str, minute_str), "%H:%M").ok()?;
        Some(NaiveDateTime::new(date, time))
    } else {
        None
    }
}

pub fn get_user_from_request(req: &HttpRequest) -> Result<String, RBACError> {
    let session_key = extract_session_key_from_req(req).unwrap();
    let user_id = Users.get_username_from_session(&session_key);
    if user_id.is_none() {
        return Err(RBACError::UserDoesNotExist);
    }
    let user_id = user_id.unwrap();
    Ok(user_id)
}

pub fn get_hash(key: &str) -> String {
    let mut hasher = Sha256::new();
    hasher.update(key);
    let result = format!("{:x}", hasher.finalize());
    result
}

pub fn user_auth_for_query(
    permissions: &[Permission],
    tables: &[String],
) -> Result<(), actix_web::error::Error> {
    for table_name in tables {
        let mut authorized = false;

        // in permission check if user can run query on the stream.
        // also while iterating add any filter tags for this stream
        for permission in permissions.iter() {
            match permission {
                Permission::Stream(Action::All, _) => {
                    authorized = true;
                    break;
                }
                Permission::StreamWithTag(Action::Query, ref stream, _)
                    if stream == table_name || stream == "*" =>
                {
                    authorized = true;
                }
                _ => (),
            }
        }

        if !authorized {
            return Err(actix_web::error::ErrorUnauthorized(format!(
                "User does not have access to stream- {table_name}"
            )));
        }
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use chrono::DateTime;
    use rstest::*;

    use super::TimePeriod;

    fn time_period_from_str(start: &str, end: &str) -> TimePeriod {
        TimePeriod::new(
            DateTime::parse_from_rfc3339(start).unwrap().into(),
            DateTime::parse_from_rfc3339(end).unwrap().into(),
            1,
        )
    }

    #[rstest]
    #[case::same_minute(
        "2022-06-11T16:30:00+00:00", "2022-06-11T16:30:59+00:00",
        &["date=2022-06-11/hour=16/minute=30/"]
    )]
    #[case::same_hour_different_minute(
        "2022-06-11T16:57:00+00:00", "2022-06-11T16:59:00+00:00",
        &[
            "date=2022-06-11/hour=16/minute=57/",
            "date=2022-06-11/hour=16/minute=58/"
        ]
    )]
    #[case::same_hour_with_00_to_59_minute_block(
        "2022-06-11T16:00:00+00:00", "2022-06-11T16:59:59+00:00",
        &["date=2022-06-11/hour=16/"]
    )]
    #[case::same_date_different_hours_coherent_minute(
        "2022-06-11T15:00:00+00:00", "2022-06-11T17:00:00+00:00",
       &[
            "date=2022-06-11/hour=15/",
            "date=2022-06-11/hour=16/"
        ]
    )]
    #[case::same_date_different_hours_incoherent_minutes(
        "2022-06-11T15:59:00+00:00", "2022-06-11T16:01:00+00:00",
        &[
            "date=2022-06-11/hour=15/minute=59/",
            "date=2022-06-11/hour=16/minute=00/"
        ]
    )]
    #[case::same_date_different_hours_whole_hours_between_incoherent_minutes(
        "2022-06-11T15:59:00+00:00", "2022-06-11T17:01:00+00:00",
        &[
            "date=2022-06-11/hour=15/minute=59/",
            "date=2022-06-11/hour=16/",
            "date=2022-06-11/hour=17/minute=00/"
        ]
    )]
    #[case::different_date_coherent_hours_and_minutes(
        "2022-06-11T00:00:00+00:00", "2022-06-13T00:00:00+00:00",
        &[
            "date=2022-06-11/",
            "date=2022-06-12/"
        ]
    )]
    #[case::different_date_incoherent_hours_coherent_minutes(
        "2022-06-11T23:00:01+00:00", "2022-06-12T01:59:59+00:00",
        &[
            "date=2022-06-11/hour=23/",
            "date=2022-06-12/hour=00/",
            "date=2022-06-12/hour=01/"
        ]
    )]
    #[case::different_date_incoherent_hours_incoherent_minutes(
        "2022-06-11T23:59:59+00:00", "2022-06-12T00:01:00+00:00",
        &[
            "date=2022-06-11/hour=23/minute=59/",
            "date=2022-06-12/hour=00/minute=00/"
        ]
    )]
    fn prefix_generation(#[case] start: &str, #[case] end: &str, #[case] right: &[&str]) {
        let time_period = time_period_from_str(start, end);
        let prefixes = time_period.generate_prefixes();
        let left = prefixes.iter().map(String::as_str).collect::<Vec<&str>>();
        assert_eq!(left.as_slice(), right);
    }
}
