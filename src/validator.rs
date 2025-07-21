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

use error::HotTierValidationError;

use self::error::{StreamNameValidationError, UsernameValidationError};
use crate::hottier::MIN_STREAM_HOT_TIER_SIZE_BYTES;
use crate::storage::StreamType;
use crate::utils::human_size::bytes_to_human_size;

// Add more sql keywords here in lower case
const DENIED_NAMES: &[&str] = &[
    "select", "from", "where", "group", "by", "order", "limit", "offset", "join", "and", "sql",
];

const ALLOWED_SPECIAL_CHARS: &[char] = &['-', '_'];

pub fn stream_name(
    stream_name: &str,
    stream_type: StreamType,
) -> Result<(), StreamNameValidationError> {
    if stream_name.is_empty() {
        return Err(StreamNameValidationError::EmptyName);
    }

    for c in stream_name.chars() {
        match c {
            ' ' => {
                return Err(StreamNameValidationError::NameWhiteSpace(
                    stream_name.to_owned(),
                ));
            }
            c if !c.is_alphanumeric() && !ALLOWED_SPECIAL_CHARS.contains(&c) => {
                return Err(StreamNameValidationError::NameSpecialChar { c });
            }
            _ => {}
        }
    }

    if DENIED_NAMES.contains(&stream_name) {
        return Err(StreamNameValidationError::SQLKeyword(
            stream_name.to_owned(),
        ));
    }

    if stream_type == StreamType::Internal {
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

pub fn hot_tier(size: &str) -> Result<(), HotTierValidationError> {
    let Ok(size) = size.parse::<u64>() else {
        return Err(HotTierValidationError::InvalidFormat);
    };
    if size < MIN_STREAM_HOT_TIER_SIZE_BYTES {
        return Err(HotTierValidationError::Size(bytes_to_human_size(
            MIN_STREAM_HOT_TIER_SIZE_BYTES,
        )));
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
        #[error("Stream name cannot contain whitespace")]
        NameWhiteSpace(String),
        #[error("Stream name cannot contain special character: {c}")]
        NameSpecialChar { c: char },
        #[error("SQL keyword cannot be used as stream name")]
        SQLKeyword(String),
        #[error(
            "The stream {0} is reserved for internal use and cannot be used for user defined streams"
        )]
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
        #[error("Please provide size in bytes")]
        InvalidFormat,

        #[error("Stream should have atleast {0} size")]
        Size(String),

        #[error("While generating times for 'now' failed to parse duration")]
        NotValidDuration(#[from] humantime::DurationError),

        #[error("Parsed duration out of range")]
        OutOfRange(#[from] chrono::OutOfRangeError),

        #[error("Hot tier not found for stream {0}")]
        NotFound(String),
    }
}
