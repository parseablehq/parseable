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
 */

use crate::alerts::rule::base::{NumericRule, StringRule};
use crate::alerts::rule::{ColumnRule, ConsecutiveNumericRule, ConsecutiveStringRule};
use crate::alerts::{Alerts, Rule};
use crate::metadata::STREAM_INFO;
use crate::query::Query;
use chrono::{DateTime, Utc};

use self::error::{
    AlertValidationError, QueryValidationError, StreamNameValidationError, UsernameValidationError,
};

// Add more sql keywords here in lower case
const DENIED_NAMES: &[&str] = &[
    "select", "from", "where", "group", "by", "order", "limit", "offset", "join", "and",
];

pub fn alert(alerts: &Alerts) -> Result<(), AlertValidationError> {
    for alert in &alerts.alerts {
        if alert.name.is_empty() {
            return Err(AlertValidationError::EmptyName);
        }
        if alert.message.message.is_empty() {
            return Err(AlertValidationError::EmptyMessage);
        }
        if alert.targets.is_empty() {
            return Err(AlertValidationError::NoTarget);
        }

        if let Rule::Column(ref column_rule) = alert.rule {
            match column_rule {
                ColumnRule::ConsecutiveNumeric(ConsecutiveNumericRule {
                    base_rule: NumericRule { ref column, .. },
                    ref state,
                })
                | ColumnRule::ConsecutiveString(ConsecutiveStringRule {
                    base_rule: StringRule { ref column, .. },
                    ref state,
                }) => {
                    if column.is_empty() {
                        return Err(AlertValidationError::EmptyRuleField);
                    }
                    if state.repeats == 0 {
                        return Err(AlertValidationError::InvalidRuleRepeat);
                    }
                }
            }
        }
    }
    Ok(())
}

pub fn stream_name(stream_name: &str) -> Result<(), StreamNameValidationError> {
    if stream_name.is_empty() {
        return Err(StreamNameValidationError::EmptyName);
    }

    if stream_name.chars().all(char::is_numeric) {
        return Err(StreamNameValidationError::NameNumericOnly(
            stream_name.to_owned(),
        ));
    }

    if stream_name.chars().next().unwrap().is_numeric() {
        return Err(StreamNameValidationError::NameCantStartWithNumber(
            stream_name.to_owned(),
        ));
    }

    for c in stream_name.chars() {
        match c {
            ' ' => {
                return Err(StreamNameValidationError::NameWhiteSpace(
                    stream_name.to_owned(),
                ))
            }
            c if !c.is_alphanumeric() => {
                return Err(StreamNameValidationError::NameSpecialChar(
                    stream_name.to_owned(),
                ))
            }
            c if c.is_ascii_uppercase() => {
                return Err(StreamNameValidationError::NameUpperCase(
                    stream_name.to_owned(),
                ))
            }
            _ => {}
        }
    }

    if DENIED_NAMES.contains(&stream_name) {
        return Err(StreamNameValidationError::SQLKeyword(
            stream_name.to_owned(),
        ));
    }

    Ok(())
}

// validate if username is valid
// username should be between 3 and 64 characters long
// username should contain only alphanumeric characters or underscores
// username should be lowercase
pub fn user_name(username: &str) -> Result<(), UsernameValidationError> {
    // Check if the username meets the required criteria
    if username.len() < 3 || username.len() > 64 {
        return Err(UsernameValidationError::InvalidLength);
    }
    // Username should contain only alphanumeric characters or underscores
    if !username
        .chars()
        .all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || c == '_')
    {
        return Err(UsernameValidationError::SpecialChar);
    }

    Ok(())
}

pub fn query(query: &str, start_time: &str, end_time: &str) -> Result<Query, QueryValidationError> {
    if query.is_empty() {
        return Err(QueryValidationError::EmptyQuery);
    }

    // convert query to lower case for validation only
    // if validation succeeds, we use the original query
    // since table names/fields are case sensitive
    let query_lower = query.to_lowercase();

    let tokens = query_lower.split(' ').collect::<Vec<&str>>();
    if tokens.contains(&"join") {
        return Err(QueryValidationError::ContainsJoin(query.to_string()));
    }
    if tokens.len() < 4 {
        return Err(QueryValidationError::IncompleteQuery(query.to_string()));
    }
    if start_time.is_empty() {
        return Err(QueryValidationError::EmptyStartTime);
    }
    if end_time.is_empty() {
        return Err(QueryValidationError::EmptyEndTime);
    }

    // log stream name is located after the `from` keyword
    let stream_name_index = tokens.iter().position(|&x| x == "from").unwrap() + 1;
    // we currently don't support queries like "select name, address from stream1 and stream2"
    // so if there is an `and` after the first log stream name, we return an error.
    if tokens.len() > stream_name_index + 1 && tokens[stream_name_index + 1] == "and" {
        return Err(QueryValidationError::MultipleStreams(query.to_string()));
    }

    let start: DateTime<Utc>;
    let end: DateTime<Utc>;

    if end_time == "now" {
        end = Utc::now();
        start = end - chrono::Duration::from_std(humantime::parse_duration(start_time)?)?;
    } else {
        start = DateTime::parse_from_rfc3339(start_time)
            .map_err(|_| QueryValidationError::StartTimeParse)?
            .into();
        end = DateTime::parse_from_rfc3339(end_time)
            .map_err(|_| QueryValidationError::EndTimeParse)?
            .into();
    };

    if start.timestamp() > end.timestamp() {
        return Err(QueryValidationError::StartTimeAfterEndTime);
    }

    let stream_name = tokens[stream_name_index].to_string();

    if !STREAM_INFO.stream_initialized(&stream_name)? {
        return Err(QueryValidationError::UninitializedStream);
    }

    let schema = STREAM_INFO.schema(&stream_name)?;

    Ok(Query {
        stream_name: tokens[stream_name_index].to_string(),
        start,
        end,
        query: query.to_string(),
        schema,
        filter_tag: None,
        fill_null: false,
    })
}

pub mod error {
    use crate::metadata::error::stream_info::MetadataError;

    #[derive(Debug, thiserror::Error)]
    pub enum AlertValidationError {
        #[error("Alert name cannot be empty")]
        EmptyName,
        #[error("Alert message cannot be empty")]
        EmptyMessage,
        #[error("Alert's rule.column cannot be empty")]
        EmptyRuleField,
        #[error("Alert's rule.repeats can't be set to 0")]
        InvalidRuleRepeat,
        #[error("Alert must have at least one target")]
        NoTarget,
    }

    #[derive(Debug, thiserror::Error)]
    pub enum QueryValidationError {
        #[error("Query cannot be empty")]
        EmptyQuery,
        #[error("Start time cannot be empty")]
        EmptyStartTime,
        #[error("End time cannot be empty")]
        EmptyEndTime,
        #[error("Could not parse start time correctly")]
        StartTimeParse,
        #[error("Could not parse end time correctly")]
        EndTimeParse,
        #[error("While generating times for 'now' failed to parse duration")]
        NotValidDuration(#[from] humantime::DurationError),
        #[error("Parsed duration out of range")]
        OutOfRange(#[from] chrono::OutOfRangeError),
        #[error("Start time cannot be greater than the end time")]
        StartTimeAfterEndTime,
        #[error("Stream is not initialized yet. Post an event first.")]
        UninitializedStream,
        #[error("Query {0} is incomplete")]
        IncompleteQuery(String),
        #[error("Query contains join keyword which is not supported yet.")]
        ContainsJoin(String),
        #[error("Querying multiple streams is not supported yet")]
        MultipleStreams(String),
        #[error("Metadata Error: {0}")]
        Metadata(#[from] MetadataError),
    }

    #[derive(Debug, thiserror::Error)]
    pub enum StreamNameValidationError {
        #[error("Stream name cannot be empty")]
        EmptyName,
        #[error("Invalid stream name with numeric values only")]
        NameNumericOnly(String),
        #[error("Stream name cannot start with a number")]
        NameCantStartWithNumber(String),
        #[error("Stream name cannot contain whitespace")]
        NameWhiteSpace(String),
        #[error("Stream name cannot contain special characters")]
        NameSpecialChar(String),
        #[error("Uppercase character in stream name")]
        NameUpperCase(String),
        #[error("SQL keyword cannot be used as stream name")]
        SQLKeyword(String),
    }

    #[derive(Debug, thiserror::Error)]
    pub enum UsernameValidationError {
        #[error("Username should be between 3 and 64 chars long")]
        InvalidLength,
        #[error(
            "Username contains invalid characters. Please use lowercase, alphanumeric or underscore"
        )]
        SpecialChar,
    }
}
