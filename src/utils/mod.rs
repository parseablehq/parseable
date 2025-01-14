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
use chrono::{DateTime, NaiveDate, NaiveDateTime, NaiveTime, Timelike, Utc};
use itertools::Itertools;
use regex::Regex;
use sha2::{Digest, Sha256};
use std::collections::HashMap;
use std::env;
use tracing::debug;
use url::Url;
#[allow(dead_code)]
pub fn hostname() -> Option<String> {
    hostname::get()
        .ok()
        .and_then(|hostname| hostname.into_string().ok())
}

pub fn hostname_unchecked() -> String {
    hostname::get().unwrap().into_string().unwrap()
}

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

pub fn date_to_prefix(date: NaiveDate) -> String {
    let date = format!("date={date}/");
    date.replace("UTC", "")
}

pub fn custom_partition_to_prefix(custom_partition: &HashMap<String, String>) -> String {
    let mut prefix = String::default();
    for (key, value) in custom_partition.iter().sorted_by_key(|v| v.0) {
        prefix.push_str(&format!("{key}={value}/", key = key, value = value));
    }
    prefix
}

pub fn hour_to_prefix(hour: u32) -> String {
    format!("hour={hour:02}/")
}

pub fn minute_to_prefix(minute: u32, data_granularity: u32) -> Option<String> {
    Some(format!(
        "minute={}/",
        minute_to_slot(minute, data_granularity)?
    ))
}

pub struct TimePeriod {
    start: DateTime<Utc>,
    end: DateTime<Utc>,
    data_granularity: u32,
}

#[allow(dead_code)]
impl TimePeriod {
    pub fn new(start: DateTime<Utc>, end: DateTime<Utc>, data_granularity: u32) -> Self {
        Self {
            data_granularity,
            start,
            end,
        }
    }

    pub fn generate_prefixes(&self) -> Vec<String> {
        let end_minute = self.end.minute() + u32::from(self.end.second() > 0);
        self.generate_date_prefixes(
            self.start.date_naive(),
            self.end.date_naive(),
            (self.start.hour(), self.start.minute()),
            (self.end.hour(), end_minute),
        )
    }

    pub fn generate_minute_prefixes(
        &self,
        prefix: &str,
        start_minute: u32,
        end_minute: u32,
    ) -> Vec<String> {
        if start_minute == end_minute {
            return vec![];
        }

        let (start_block, end_block) = (
            start_minute / self.data_granularity,
            end_minute / self.data_granularity,
        );

        let forbidden_block = 60 / self.data_granularity;

        // ensure both start and end are within the same hour, else return prefix as is
        if end_block - start_block >= forbidden_block {
            return vec![prefix.to_owned()];
        }

        let mut prefixes = vec![];

        let push_prefix = |block: u32, prefixes: &mut Vec<_>| {
            if let Some(minute_prefix) =
                minute_to_prefix(block * self.data_granularity, self.data_granularity)
            {
                let prefix = prefix.to_owned() + &minute_prefix;
                prefixes.push(prefix);
            }
        };

        for block in start_block..end_block {
            push_prefix(block, &mut prefixes);
        }

        // NOTE: for block sizes larger than a minute ensure
        // ensure last block is considered
        if self.data_granularity > 1 {
            push_prefix(end_block, &mut prefixes);
        }

        prefixes
    }

    pub fn generate_hour_prefixes(
        &self,
        prefix: &str,
        start_hour: u32,
        start_minute: u32,
        end_hour: u32,
        end_minute: u32,
    ) -> Vec<String> {
        // ensure both start and end are within the same day
        if end_hour - start_hour >= 24 {
            return vec![prefix.to_owned()];
        }

        let mut prefixes = vec![];

        for hour in start_hour..=end_hour {
            if hour == 24 {
                break;
            }
            let prefix = prefix.to_owned() + &hour_to_prefix(hour);
            let is_start = hour == start_hour;
            let is_end = hour == end_hour;

            if is_start || is_end {
                let minute_prefixes = self.generate_minute_prefixes(
                    &prefix,
                    if is_start { start_minute } else { 0 },
                    if is_end { end_minute } else { 60 },
                );
                prefixes.extend(minute_prefixes);
            } else {
                prefixes.push(prefix);
            }
        }

        prefixes
    }

    pub fn generate_date_prefixes(
        &self,
        start_date: NaiveDate,
        end_date: NaiveDate,
        start_time: (u32, u32),
        end_time: (u32, u32),
    ) -> Vec<String> {
        let mut prefixes = vec![];
        let mut date = start_date;

        while date <= end_date {
            let prefix = date_to_prefix(date);
            let is_start = date == start_date;
            let is_end = date == end_date;

            if is_start || is_end {
                let ((start_hour, start_minute), (end_hour, end_minute)) = (
                    if is_start { start_time } else { (0, 0) },
                    if is_end { end_time } else { (24, 60) },
                );
                let hour_prefixes = self.generate_hour_prefixes(
                    &prefix,
                    start_hour,
                    start_minute,
                    end_hour,
                    end_minute,
                );
                prefixes.extend(hour_prefixes);
            } else {
                prefixes.push(prefix);
            }
            date = date.succ_opt().unwrap();
        }

        prefixes
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
        .unwrap_or_else(|err| panic!("{}, failed to parse `{}` as Url. Please set the environment variable `P_ADDR` to `<ip address>:<port>` without the scheme (e.g., 192.168.1.1:8000). Please refer to the documentation: https://logg.ing/env for more details.",
            err, CONFIG.options.address));
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
