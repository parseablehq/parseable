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

use chrono::{DateTime, NaiveDate, TimeDelta, Timelike, Utc};

use super::minute_to_slot;

#[derive(Debug, thiserror::Error)]
pub enum TimeParseError {
    #[error("Parsing humantime")]
    HumanTime(#[from] humantime::DurationError),
    #[error("Out of Range")]
    OutOfRange(#[from] chrono::OutOfRangeError),
    #[error("Error parsing time: {0}")]
    Chrono(#[from] chrono::ParseError),
    #[error("Start time cannot be greater than the end time")]
    StartTimeAfterEndTime,
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

/// Represents a range of time with a start and end point.
#[derive(Debug)]
pub struct TimeRange {
    pub start: DateTime<Utc>,
    pub end: DateTime<Utc>,
}

impl TimeRange {
    pub fn new(start: DateTime<Utc>, end: DateTime<Utc>) -> Self {
        TimeRange { start, end }
    }

    /// Parses human-readable time strings into a `TimeRange` object.
    ///
    /// # Arguments
    /// - `start_time`: A string representing the start of the time range. This can either be
    ///   a human-readable duration (e.g., `"2 hours"`) or an RFC 3339 formatted timestamp.
    /// - `end_time`: A string representing the end of the time range. This can either be
    ///   the keyword `"now"` (to represent the current time) or an RFC 3339 formatted timestamp.
    ///
    /// # Errors
    /// - `TimeParseError::StartTimeAfterEndTime`: Returned when the parsed start time is later than the end time.
    /// - Any error that might occur during parsing of durations or RFC 3339 timestamps.
    ///
    /// # Example
    /// ```ignore
    /// let range = TimeRange::parse_human_time("2 hours", "now");
    /// let range = TimeRange::parse_human_time("2023-01-01T12:00:00Z", "2023-01-01T15:00:00Z");
    /// ```
    pub fn parse_human_time(start_time: &str, end_time: &str) -> Result<Self, TimeParseError> {
        let start: DateTime<Utc>;
        let end: DateTime<Utc>;

        if end_time == "now" {
            end = Utc::now();
            start = end - chrono::Duration::from_std(humantime::parse_duration(start_time)?)?;
        } else {
            start = DateTime::parse_from_rfc3339(start_time)?.into();
            end = DateTime::parse_from_rfc3339(end_time)?.into();
        };

        if start > end {
            return Err(TimeParseError::StartTimeAfterEndTime);
        }

        Ok(Self { start, end })
    }

    /// Generates prefixes for the time period, e.g:
    /// 1. ("2022-06-11T23:00:01+00:00", "2022-06-12T01:59:59+00:00") => ["date=2022-06-11/hour=23/", "date=2022-06-12/hour=00/", "date=2022-06-12/hour=01/""]
    /// 2. ("2022-06-11T15:59:00+00:00", "2022-06-11T17:01:00+00:00") => ["date=2022-06-11/hour=15/minute=59/", "date=2022-06-11/hour=16/", "date=2022-06-11/hour=17/minute=00/"]
    pub fn generate_prefixes(self, data_granularity: u32) -> Vec<Prefix> {
        let mut prefixes = vec![];
        let time_bounds = self.calculate_time_bounds();
        let mut current_date = time_bounds.start_date;

        while current_date <= time_bounds.end_date {
            self.process_date(data_granularity, current_date, time_bounds, &mut prefixes);
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

    fn process_date(
        &self,
        data_granularity: u32,
        date: NaiveDate,
        bounds: TimeBounds,
        prefixes: &mut Vec<String>,
    ) {
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

        self.process_hours(data_granularity, prefix, time_bounds, prefixes);
    }

    fn process_hours(
        &self,
        data_granularity: u32,
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
                data_granularity,
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
        data_granularity: u32,
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
            time_bounds.start_minute / data_granularity,
            time_bounds.end_minute / data_granularity,
        );

        let forbidden_block = 60 / data_granularity;
        if end_block - start_block >= forbidden_block {
            prefixes.push(hour_prefix);
            return;
        }

        self.generate_minute_prefixes(
            data_granularity,
            hour_prefix,
            start_block,
            end_block,
            prefixes,
        );
    }

    fn generate_minute_prefixes(
        &self,
        data_granularity: u32,
        hour_prefix: String,
        start_block: u32,
        end_block: u32,
        prefixes: &mut Vec<String>,
    ) {
        let mut push_prefix = |block: u32| {
            if let Some(minute_slot) = minute_to_slot(block * data_granularity, data_granularity) {
                let prefix = format!("{hour_prefix}minute={minute_slot}/");
                prefixes.push(prefix);
            }
        };

        for block in start_block..end_block {
            push_prefix(block);
        }

        // Handle last block for granularity > 1
        if data_granularity > 1 {
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

#[cfg(test)]
mod tests {
    use super::*;

    use chrono::{Duration, SecondsFormat, Utc};
    use rstest::*;

    #[test]
    fn valid_rfc3339_timestamps() {
        let start_time = "2023-01-01T12:00:00Z";
        let end_time = "2023-01-01T13:00:00Z";

        let result = TimeRange::parse_human_time(start_time, end_time);
        let parsed = result.unwrap();

        assert_eq!(
            parsed.start.to_rfc3339_opts(SecondsFormat::Secs, true),
            start_time
        );
        assert_eq!(
            parsed.end.to_rfc3339_opts(SecondsFormat::Secs, true),
            end_time
        );
    }

    #[test]
    fn end_time_now_with_valid_duration() {
        let start_time = "1h";
        let end_time = "now";

        let result = TimeRange::parse_human_time(start_time, end_time);
        let parsed = result.unwrap();

        assert!(parsed.end <= Utc::now());
        assert_eq!(parsed.end - parsed.start, Duration::hours(1));

        let start_time = "30 minutes";
        let end_time = "now";

        let result = TimeRange::parse_human_time(start_time, end_time);
        let parsed = result.unwrap();

        assert!(parsed.end <= Utc::now());
        assert_eq!(parsed.end - parsed.start, Duration::minutes(30));
    }

    #[test]
    fn start_time_after_end_time() {
        let start_time = "2023-01-01T14:00:00Z";
        let end_time = "2023-01-01T13:00:00Z";

        let result = TimeRange::parse_human_time(start_time, end_time);
        assert!(matches!(result, Err(TimeParseError::StartTimeAfterEndTime)));
    }

    #[test]
    fn invalid_start_time_format() {
        let start_time = "not-a-valid-time";
        let end_time = "2023-01-01T13:00:00Z";

        let result = TimeRange::parse_human_time(start_time, end_time);
        assert!(matches!(result, Err(TimeParseError::Chrono(_))));
    }

    #[test]
    fn invalid_end_time_format() {
        let start_time = "2023-01-01T12:00:00Z";
        let end_time = "not-a-valid-time";

        let result = TimeRange::parse_human_time(start_time, end_time);
        assert!(matches!(result, Err(TimeParseError::Chrono(_))));
    }

    #[test]
    fn invalid_duration_with_end_time_now() {
        let start_time = "not-a-duration";
        let end_time = "now";

        let result = TimeRange::parse_human_time(start_time, end_time);
        assert!(matches!(result, Err(TimeParseError::HumanTime(_))));
    }

    fn time_period_from_str(start: &str, end: &str) -> TimeRange {
        TimeRange {
            start: DateTime::parse_from_rfc3339(start).unwrap().into(),
            end: DateTime::parse_from_rfc3339(end).unwrap().into(),
        }
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
        let prefixes = time_period.generate_prefixes(1);
        let left = prefixes.iter().map(String::as_str).collect::<Vec<&str>>();
        assert_eq!(left.as_slice(), right);
    }
}
