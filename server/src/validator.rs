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

use chrono::NaiveDate;
use error::HotTierValidationError;

use self::error::{AlertValidationError, StreamNameValidationError, UsernameValidationError};
use crate::alerts::rule::base::{NumericRule, StringRule};
use crate::alerts::rule::{ColumnRule, ConsecutiveNumericRule, ConsecutiveStringRule};
use crate::alerts::{Alerts, Rule};
use crate::handlers::http::cluster::INTERNAL_STREAM_NAME;
use crate::option::validation::{cache_size, human_size_to_bytes};

// Add more sql keywords here in lower case
const DENIED_NAMES: &[&str] = &[
    "select", "from", "where", "group", "by", "order", "limit", "offset", "join", "and",
];

pub fn alert(alerts: &Alerts) -> Result<(), AlertValidationError> {
    let alert_name: Vec<&str> = alerts.alerts.iter().map(|a| a.name.as_str()).collect();
    let mut alert_name_dedup = alert_name.clone();
    alert_name_dedup.sort();
    alert_name_dedup.dedup();

    if alert_name.len() != alert_name_dedup.len() {
        return Err(AlertValidationError::ExistingName);
    }
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

    if stream_name == INTERNAL_STREAM_NAME {
        return Err(StreamNameValidationError::InternalStream(
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

pub fn hot_tier(
    start_date: &str,
    end_date: &str,
    size: &str,
) -> Result<(), HotTierValidationError> {
    if human_size_to_bytes(size).is_err() {
        return Err(HotTierValidationError::Size);
    }
    cache_size(size).map_err(|_| HotTierValidationError::Size)?;

    let start_date: NaiveDate = start_date
        .parse()
        .map_err(|_| HotTierValidationError::StartDate)?;
    let end_date: NaiveDate = end_date
        .parse()
        .map_err(|_| HotTierValidationError::EndDate)?;

    if start_date > end_date {
        return Err(HotTierValidationError::DateRange);
    }

    Ok(())
}
pub mod error {

    #[derive(Debug, thiserror::Error)]
    pub enum AlertValidationError {
        #[error("Alert name cannot be empty")]
        EmptyName,
        #[error("Alert with the same name already exists")]
        ExistingName,
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
        #[error("`pmeta` is an internal stream name and cannot be used.")]
        InternalStream(String),
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

    #[derive(Debug, thiserror::Error)]
    pub enum HotTierValidationError {
        #[error("Invalid size given for hot tier, please provide size in human readable format, e.g 1GiB, 2GiB")]
        Size,
        #[error(
            "Invalid start date given for hot tier, please provide the date in yyyy-mm-dd format"
        )]
        StartDate,
        #[error(
            "Invalid end date given for hot tier, please provide the date in yyyy-mm-dd format"
        )]
        EndDate,
        #[error("End date should be greater than start date")]
        DateRange,
    }
}
