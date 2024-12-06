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

#[derive(Debug)]
pub struct TimeRange {
    pub start: DateTime<Utc>,
    pub end: DateTime<Utc>,
}

impl TimeRange {
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
