/*
 * Parseable Server (C) 2022 - 2025 Parseable, Inc.
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

use chrono::{DateTime, Datelike, NaiveDate, NaiveDateTime, TimeDelta, TimeZone, Timelike, Utc};

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
#[derive(Debug, Clone)]
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
        let mut start: DateTime<Utc>;
        let mut end: DateTime<Utc>;

        if end_time == "now" {
            end = Utc::now();
            start = end - chrono::Duration::from_std(humantime::parse_duration(start_time)?)?;
        } else {
            start = DateTime::parse_from_rfc3339(start_time)?.into();
            end = DateTime::parse_from_rfc3339(end_time)?.into();
        };

        // Truncate seconds, milliseconds, and nanoseconds to zero
        // to ensure that the time range is aligned to the minute
        start = truncate_to_minute(start);
        end = truncate_to_minute(end);

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
            if let Ok(minute) = Minute::try_from(block * data_granularity) {
                let prefix = format!("{hour_prefix}minute={}/", minute.to_slot(data_granularity));
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

    /// Returns a time range of `data_granularity` length which incorporates provided timestamp
    pub fn granularity_range(timestamp: DateTime<Utc>, data_granularity: u32) -> Self {
        let time = timestamp
            .time()
            .with_second(0)
            .and_then(|time| time.with_nanosecond(0))
            .expect("Within expected time range");
        let timestamp = timestamp.with_time(time).unwrap();
        let block_n = timestamp.minute() / data_granularity;
        let block_start = block_n * data_granularity;
        let start = timestamp
            .with_minute(block_start)
            .expect("Within minute range");
        let end = start + TimeDelta::minutes(data_granularity as i64);

        Self { start, end }
    }

    /// Returns true if the provided timestamp is within the timerange
    pub fn contains(&self, time: DateTime<Utc>) -> bool {
        self.start <= time && self.end > time
    }
}

pub fn truncate_to_minute(dt: DateTime<Utc>) -> DateTime<Utc> {
    // Get the date and time components we want to keep
    let year = dt.year();
    let month = dt.month();
    let day = dt.day();
    let hour = dt.hour();
    let minute = dt.minute();

    // Create a new DateTime with seconds, milliseconds, and nanoseconds set to 0
    Utc.with_ymd_and_hms(year, month, day, hour, minute, 0)
        .unwrap() // This should never fail with valid components
}

/// Represents a minute value (0-59) and provides methods for converting it to a slot range.
///
/// # Examples
///
/// ```
/// use parseable::utils::time::Minute;
///
/// let minute = Minute::try_from(15).unwrap();
/// assert_eq!(minute.to_slot(10), "10-19");
/// ```
#[derive(Debug, Clone, Copy)]
pub struct Minute {
    block: u32,
}

impl TryFrom<u32> for Minute {
    type Error = u32;

    /// Returns a Minute if block is an acceptable minute value, else returns it as is
    fn try_from(block: u32) -> Result<Self, Self::Error> {
        if block >= 60 {
            return Err(block);
        }

        Ok(Self { block })
    }
}

impl From<NaiveDateTime> for Minute {
    fn from(timestamp: NaiveDateTime) -> Self {
        Self {
            block: timestamp.minute(),
        }
    }
}

impl Minute {
    /// Convert minutes to a slot range
    /// e.g. given minute = 15 and OBJECT_STORE_DATA_GRANULARITY = 10 returns "10-19"
    ///
    /// ### PANICS
    /// If the provided `data_granularity` value isn't cleanly divisble from 60
    pub fn to_slot(self, data_granularity: u32) -> String {
        assert!(60 % data_granularity == 0);
        let block_n = self.block / data_granularity;
        let block_start = block_n * data_granularity;
        if data_granularity == 1 {
            return format!("{block_start:02}");
        }

        let block_end = (block_n + 1) * data_granularity - 1;
        format!("{block_start:02}-{block_end:02}")
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use chrono::{Duration, SecondsFormat, TimeZone, Utc};
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

    #[test]
    fn valid_minute_to_minute_slot() {
        let res = Minute::try_from(10);
        assert!(res.is_ok());
        assert_eq!(res.unwrap().to_slot(1), "10");
    }

    #[test]
    fn invalid_minute() {
        assert!(Minute::try_from(100).is_err());
    }

    #[test]
    fn minute_from_timestamp() {
        let timestamp =
            NaiveDateTime::parse_from_str("2025-01-01 02:03", "%Y-%m-%d %H:%M").unwrap();
        assert_eq!(Minute::from(timestamp).to_slot(1), "03");
    }

    #[test]
    fn slot_5_min_from_timestamp() {
        let timestamp =
            NaiveDateTime::parse_from_str("2025-01-01 02:03", "%Y-%m-%d %H:%M").unwrap();
        assert_eq!(Minute::from(timestamp).to_slot(5), "00-04");
    }

    #[test]
    fn slot_30_min_from_timestamp() {
        let timestamp =
            NaiveDateTime::parse_from_str("2025-01-01 02:33", "%Y-%m-%d %H:%M").unwrap();
        assert_eq!(Minute::from(timestamp).to_slot(30), "30-59");
    }

    #[test]
    #[should_panic]
    fn illegal_slot_granularity() {
        Minute::try_from(0).unwrap().to_slot(40);
    }

    #[test]
    fn test_granularity_one_minute() {
        // Test with 1-minute granularity
        let timestamp = Utc.with_ymd_and_hms(2023, 1, 1, 12, 30, 45).unwrap();
        let range = TimeRange::granularity_range(timestamp, 1);

        assert_eq!(range.start.minute(), 30);
        assert_eq!(range.end.minute(), 31);
        assert_eq!(range.start.hour(), 12);
        assert_eq!(range.end.hour(), 12);
    }

    #[test]
    fn test_granularity_five_minutes() {
        // Test with 5-minute granularity
        let timestamp = Utc.with_ymd_and_hms(2023, 1, 1, 12, 17, 45).unwrap();
        let range = TimeRange::granularity_range(timestamp, 5);

        // For minute 17, with granularity 5, block_n = 17 / 5 = 3
        // block_start = 3 * 5 = 15
        // block_end = (3 + 1) * 5 = 20
        assert_eq!(range.start.minute(), 15);
        assert_eq!(range.end.minute(), 20);
    }

    #[test]
    fn test_granularity_fifteen_minutes() {
        // Test with 15-minute granularity
        let timestamp = Utc.with_ymd_and_hms(2023, 1, 1, 12, 29, 0).unwrap();
        let range = TimeRange::granularity_range(timestamp, 15);

        // For minute 29, with granularity 15, block_n = 29 / 15 = 1
        // block_start = 1 * 15 = 15
        // block_end = (1 + 1) * 15 = 30
        assert_eq!(range.start.minute(), 15);
        assert_eq!(range.end.minute(), 30);
    }

    #[test]
    fn test_granularity_thirty_minutes() {
        // Test with 30-minute granularity
        let timestamp = Utc.with_ymd_and_hms(2023, 1, 1, 12, 31, 0).unwrap();
        let range = TimeRange::granularity_range(timestamp, 30);

        // For minute 31, with granularity 30, block_n = 31 / 30 = 1
        // block_start = 1 * 30 = 30
        // block_end = (1 + 1) * 30 = 60, which should wrap to 0 in the next hour
        assert_eq!(range.start.minute(), 30);
        assert_eq!(range.end.minute(), 0);
        assert_eq!(range.start.hour(), 12);
        assert_eq!(range.end.hour(), 13); // Should be next hour
    }

    #[test]
    fn test_granularity_edge_case() {
        // Test edge case where minute is exactly at granularity boundary
        let timestamp = Utc.with_ymd_and_hms(2023, 1, 1, 12, 15, 0).unwrap();
        let range = TimeRange::granularity_range(timestamp, 15);

        assert_eq!(range.start.minute(), 15);
        assert_eq!(range.end.minute(), 30);
    }

    #[test]
    fn test_granularity_hour_boundary() {
        // Test case where end would exceed hour boundary
        let timestamp = Utc.with_ymd_and_hms(2023, 1, 1, 12, 59, 59).unwrap();
        let range = TimeRange::granularity_range(timestamp, 20);

        // For minute 59, block_n = 59 / 20 = 2
        // block_start = 2 * 20 = 40
        // block_end = (2 + 1) * 20 = 60, which should wrap to 0 in the next hour
        assert_eq!(range.start.minute(), 40);
        assert_eq!(range.end.minute(), 0);
        assert_eq!(range.start.hour(), 12);
        assert_eq!(range.end.hour(), 13);
    }
}
