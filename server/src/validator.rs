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

use crate::alerts::rule::base::{NumericRule, StringRule};
use crate::alerts::rule::{ColumnRule, ConsecutiveNumericRule, ConsecutiveStringRule};
use crate::alerts::{Alerts, Rule};

use self::error::{AlertValidationError, StreamNameValidationError, UsernameValidationError};

// Add more sql keywords here in lower case
const DENIED_NAMES: &[&str] = &[
    "select", "from", "where", "group", "by", "order", "limit", "offset", "join", "and", "meta",
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

pub mod error {

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
