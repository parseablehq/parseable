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
 */

use actix_web::web;
use chrono::{Date, DateTime, Timelike, Utc};
use serde_json::{json, Value};
use std::collections::HashMap;

use crate::Error;

pub fn flatten_json_body(body: web::Json<serde_json::Value>) -> Result<String, Error> {
    let mut flat_value: Value = json!({});
    flatten_json::flatten(&body, &mut flat_value, None, true, Some("_")).unwrap();
    let flattened = serde_json::to_string(&flat_value)?;

    Ok(flattened)
}

pub fn merge(value: Value, fields: HashMap<String, String>) -> Value {
    match value {
        Value::Object(mut m) => {
            for (k, v) in fields {
                match m.get_mut(&k) {
                    Some(val) => {
                        let mut final_val = String::new();
                        final_val.push_str(val.as_str().unwrap());
                        final_val.push(',');
                        final_val.push_str(&v);
                        *val = Value::String(final_val);
                    }
                    None => {
                        m.insert(k, Value::String(v));
                    }
                }
            }
            Value::Object(m)
        }
        value => value,
    }
}

pub mod header_parsing {
    const MAX_HEADERS_ALLOWED: usize = 10;
    use actix_web::{HttpRequest, HttpResponse, ResponseError};

    pub fn collect_labelled_headers(
        req: &HttpRequest,
        prefix: &str,
        kv_separator: char,
    ) -> Result<String, ParseHeaderError> {
        // filter out headers which has right prefix label and convert them into str;
        let headers = req.headers().iter().filter_map(|(key, value)| {
            let key = key.as_str().strip_prefix(prefix)?;
            Some((key, value))
        });

        let mut labels: Vec<String> = Vec::new();

        for (key, value) in headers {
            let value = value.to_str().map_err(|_| ParseHeaderError::InvalidValue)?;
            if key.is_empty() {
                return Err(ParseHeaderError::Emptykey);
            }
            if key.contains(kv_separator) {
                return Err(ParseHeaderError::SeperatorInKey(kv_separator));
            }
            if value.contains(kv_separator) {
                return Err(ParseHeaderError::SeperatorInValue(kv_separator));
            }

            labels.push(format!("{}={}", key, value));
        }

        if labels.len() > MAX_HEADERS_ALLOWED {
            return Err(ParseHeaderError::MaxHeadersLimitExceeded);
        }

        Ok(labels.join(&kv_separator.to_string()))
    }

    #[derive(Debug, thiserror::Error)]
    pub enum ParseHeaderError {
        #[error("Too many headers received. Limit is of 5 headers")]
        MaxHeadersLimitExceeded,
        #[error("A value passed in header is not formattable to plain visible ASCII")]
        InvalidValue,
        #[error("Invalid Key was passed which terminated just after the end of prefix")]
        Emptykey,
        #[error("A key passed in header contains reserved char {0}")]
        SeperatorInKey(char),
        #[error("A value passed in header contains reserved char {0}")]
        SeperatorInValue(char),
    }

    impl ResponseError for ParseHeaderError {
        fn status_code(&self) -> http::StatusCode {
            http::StatusCode::BAD_REQUEST
        }

        fn error_response(&self) -> HttpResponse {
            HttpResponse::build(self.status_code()).body(self.to_string())
        }
    }
}

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
        return Some(format!("{:02}", block_start));
    }

    let block_end = (block_n + 1) * data_granularity - 1;
    Some(format!("{:02}-{:02}", block_start, block_end))
}

pub fn date_to_prefix(date: Date<Utc>) -> String {
    let date = format!("date={}/", date);
    date.replace("UTC", "")
}

pub fn hour_to_prefix(hour: u32) -> String {
    format!("hour={:02}/", hour)
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

impl TimePeriod {
    pub fn new(start: DateTime<Utc>, end: DateTime<Utc>, data_granularity: u32) -> Self {
        Self {
            data_granularity,
            start,
            end,
        }
    }

    pub fn generate_prefixes(&self, prefix: &str) -> Vec<String> {
        let prefix = format!("{}/", prefix);

        let end_minute = self.end.minute() + if self.end.second() > 0 { 1 } else { 0 };

        self.generate_date_prefixes(
            &prefix,
            self.start.date(),
            self.end.date(),
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
        prefix: &str,
        start_date: Date<Utc>,
        end_date: Date<Utc>,
        start_time: (u32, u32),
        end_time: (u32, u32),
    ) -> Vec<String> {
        let mut prefixes = vec![];
        let mut date = start_date;

        while date <= end_date {
            let prefix = prefix.to_owned() + &date_to_prefix(date);
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
        &["stream_name/date=2022-06-11/hour=16/minute=30/"]
    )]
    #[case::same_hour_different_minute(
        "2022-06-11T16:57:00+00:00", "2022-06-11T16:59:00+00:00",
        &[
            "stream_name/date=2022-06-11/hour=16/minute=57/",
            "stream_name/date=2022-06-11/hour=16/minute=58/"
        ]
    )]
    #[case::same_hour_with_00_to_59_minute_block(
        "2022-06-11T16:00:00+00:00", "2022-06-11T16:59:59+00:00",   
        &["stream_name/date=2022-06-11/hour=16/"]
    )]
    #[case::same_date_different_hours_coherent_minute(
        "2022-06-11T15:00:00+00:00", "2022-06-11T17:00:00+00:00",
       &[
            "stream_name/date=2022-06-11/hour=15/",
            "stream_name/date=2022-06-11/hour=16/"
        ]
    )]
    #[case::same_date_different_hours_incoherent_minutes(
        "2022-06-11T15:59:00+00:00", "2022-06-11T16:01:00+00:00", 
        &[
            "stream_name/date=2022-06-11/hour=15/minute=59/",
            "stream_name/date=2022-06-11/hour=16/minute=00/"
        ]
    )]
    #[case::same_date_different_hours_whole_hours_between_incoherent_minutes(
        "2022-06-11T15:59:00+00:00", "2022-06-11T17:01:00+00:00", 
        &[
            "stream_name/date=2022-06-11/hour=15/minute=59/",
            "stream_name/date=2022-06-11/hour=16/",
            "stream_name/date=2022-06-11/hour=17/minute=00/"
        ]
    )]
    #[case::different_date_coherent_hours_and_minutes(
        "2022-06-11T00:00:00+00:00", "2022-06-13T00:00:00+00:00", 
        &[
            "stream_name/date=2022-06-11/",
            "stream_name/date=2022-06-12/"
        ]
    )]
    #[case::different_date_incoherent_hours_coherent_minutes(
        "2022-06-11T23:00:01+00:00", "2022-06-12T01:59:59+00:00", 
        &[
            "stream_name/date=2022-06-11/hour=23/",
            "stream_name/date=2022-06-12/hour=00/",
            "stream_name/date=2022-06-12/hour=01/"
        ]
    )]
    #[case::different_date_incoherent_hours_incoherent_minutes(
        "2022-06-11T23:59:59+00:00", "2022-06-12T00:01:00+00:00", 
        &[
            "stream_name/date=2022-06-11/hour=23/minute=59/",
            "stream_name/date=2022-06-12/hour=00/minute=00/"
        ]
    )]
    fn prefix_generation(#[case] start: &str, #[case] end: &str, #[case] right: &[&str]) {
        let time_period = time_period_from_str(start, end);
        let prefixes = time_period.generate_prefixes("stream_name");
        let left = prefixes.iter().map(String::as_str).collect::<Vec<&str>>();
        assert_eq!(left.as_slice(), right);
    }
}
