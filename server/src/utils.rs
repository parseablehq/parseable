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
use actix_web::HttpRequest;
use chrono::{Date, DateTime, Timelike, Utc};
use rand::{distributions::Alphanumeric, Rng};
use serde_json::{json, Value};
use std::collections::HashMap;

use crate::Error;

const META_LABEL: &str = "x-p-meta";

pub fn flatten_json_body(
    body: web::Json<serde_json::Value>,
    labels: Option<String>,
) -> Result<String, Error> {
    let mut collector_labels = HashMap::new();

    collector_labels.insert("labels".to_string(), labels.unwrap());

    let mut flat_value: Value = json!({});
    let new_body = merge(&body, &collector_labels);
    flatten_json::flatten(&new_body, &mut flat_value, None, true, Some("_")).unwrap();
    let flattened = serde_json::to_string(&flat_value)?;

    Ok(flattened)
}

fn merge(v: &Value, fields: &HashMap<String, String>) -> Value {
    match v {
        Value::Object(m) => {
            let mut m = m.clone();
            for (k, v) in fields {
                match m.get(k) {
                    Some(curr_val) => {
                        let mut final_val = String::new();
                        final_val.push_str(curr_val.as_str().unwrap());
                        final_val.push(',');
                        final_val.push_str(v);
                        m.insert(k.clone(), Value::String(final_val));
                    }
                    None => {
                        m.insert(k.clone(), Value::String(v.to_string()));
                    }
                }
            }
            Value::Object(m)
        }
        v => v.clone(),
    }
}

pub fn random_string() -> String {
    rand::thread_rng()
        .sample_iter(&Alphanumeric)
        .take(7)
        .map(char::from)
        .collect()
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

/// collect labels passed from http headers
/// format: labels = "app=k8s, cloud=gcp"
pub fn collect_labels(req: &HttpRequest) -> Option<String> {
    let keys = req.headers().keys().cloned().collect::<Vec<_>>();

    let mut labels_vec = Vec::with_capacity(100);
    for key in keys {
        if key.to_string().to_lowercase().starts_with(META_LABEL) {
            let value = req.headers().get(&key)?.to_str().ok();
            let remove_meta_char = format!("{}-", META_LABEL);
            let kv = format! {"{}={}", key.to_string().replace(&remove_meta_char.to_string(), ""), value.unwrap()};
            labels_vec.push(kv);
        }
    }

    Some(labels_vec.join(","))
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
        let prefix = prefix.to_string() + "/";

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

    use super::TimePeriod;

    fn time_period_from_str(start: &str, end: &str) -> TimePeriod {
        TimePeriod::new(
            DateTime::parse_from_rfc3339(start).unwrap().into(),
            DateTime::parse_from_rfc3339(end).unwrap().into(),
            1,
        )
    }

    #[test]
    fn prefix_generation_same_minute() {
        let time_period =
            time_period_from_str("2022-06-11T16:30:00+00:00", "2022-06-11T16:30:59+00:00");

        assert_eq!(
            time_period.generate_prefixes("stream_name"),
            vec!["stream_name/date=2022-06-11/hour=16/minute=30/".to_string()]
        );
    }

    #[test]
    fn prefix_generation_same_hour_different_minute() {
        let time_period =
            time_period_from_str("2022-06-11T16:57:00+00:00", "2022-06-11T16:59:00+00:00");

        assert_eq!(
            time_period.generate_prefixes("stream_name"),
            vec![
                "stream_name/date=2022-06-11/hour=16/minute=57/".to_string(),
                "stream_name/date=2022-06-11/hour=16/minute=58/".to_string(),
            ]
        );
    }

    #[test]
    fn prefix_generation_same_hour_with_00_to_59_minute_block() {
        let time_period =
            time_period_from_str("2022-06-11T16:00:00+00:00", "2022-06-11T16:59:59+00:00");

        assert_eq!(
            time_period.generate_prefixes("stream_name"),
            vec!["stream_name/date=2022-06-11/hour=16/".to_string(),]
        );
    }

    #[test]
    fn prefix_generation_same_date_different_hours_coherent_minute() {
        let time_period =
            time_period_from_str("2022-06-11T15:00:00+00:00", "2022-06-11T17:00:00+00:00");

        assert_eq!(
            time_period.generate_prefixes("stream_name"),
            vec![
                "stream_name/date=2022-06-11/hour=15/".to_string(),
                "stream_name/date=2022-06-11/hour=16/".to_string(),
            ]
        );
    }

    #[test]
    fn prefix_generation_same_date_different_hours_incoherent_minutes() {
        let time_period =
            time_period_from_str("2022-06-11T15:59:00+00:00", "2022-06-11T16:01:00+00:00");

        assert_eq!(
            time_period.generate_prefixes("stream_name"),
            vec![
                "stream_name/date=2022-06-11/hour=15/minute=59/".to_string(),
                "stream_name/date=2022-06-11/hour=16/minute=00/".to_string(),
            ]
        );
    }

    #[test]
    fn prefix_generation_same_date_different_hours_whole_hours_between_incoherent_minutes() {
        let time_period =
            time_period_from_str("2022-06-11T15:59:00+00:00", "2022-06-11T17:01:00+00:00");

        assert_eq!(
            time_period.generate_prefixes("stream_name"),
            vec![
                "stream_name/date=2022-06-11/hour=15/minute=59/".to_string(),
                "stream_name/date=2022-06-11/hour=16/".to_string(),
                "stream_name/date=2022-06-11/hour=17/minute=00/".to_string(),
            ]
        );
    }

    #[test]
    fn prefix_generation_different_date_coherent_hours_and_minutes() {
        let time_period =
            time_period_from_str("2022-06-11T00:00:00+00:00", "2022-06-13T00:00:00+00:00");

        assert_eq!(
            time_period.generate_prefixes("stream_name"),
            vec![
                "stream_name/date=2022-06-11/".to_string(),
                "stream_name/date=2022-06-12/".to_string(),
            ]
        );
    }

    #[test]
    fn prefix_generation_different_date_incoherent_hours_coherent_minutes() {
        let time_period =
            time_period_from_str("2022-06-11T23:00:01+00:00", "2022-06-12T01:59:59+00:00");

        assert_eq!(
            time_period.generate_prefixes("stream_name"),
            vec![
                "stream_name/date=2022-06-11/hour=23/".to_string(),
                "stream_name/date=2022-06-12/hour=00/".to_string(),
                "stream_name/date=2022-06-12/hour=01/".to_string(),
            ]
        );
    }

    #[test]
    fn prefix_generation_different_date_incoherent_hours_incoherent_minutes() {
        let time_period =
            time_period_from_str("2022-06-11T23:59:59+00:00", "2022-06-12T00:01:00+00:00");

        assert_eq!(
            time_period.generate_prefixes("stream_name"),
            vec![
                "stream_name/date=2022-06-11/hour=23/minute=59/".to_string(),
                "stream_name/date=2022-06-12/hour=00/minute=00/".to_string(),
            ]
        );
    }
}
