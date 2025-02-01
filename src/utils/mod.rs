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
use chrono::{NaiveDate, NaiveDateTime, NaiveTime, Utc};
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
