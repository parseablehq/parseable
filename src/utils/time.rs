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

use chrono::{DateTime, Utc};

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

/// Represents a range of time with a start and end point.
#[derive(Debug)]
pub struct TimeRange {
    pub start: DateTime<Utc>,
    pub end: DateTime<Utc>,
}

impl TimeRange {
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
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::{Duration, SecondsFormat, Utc};

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
}
