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

use std::collections::HashSet;

use error::HotTierValidationError;
use once_cell::sync::Lazy;

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

static RESERVED_NAMES: Lazy<HashSet<&'static str>> = Lazy::new(|| {
    [
        "admin",
        "user",
        "role",
        "null",
        "undefined",
        "none",
        "empty",
        "password",
        "username",
    ]
    .into_iter()
    .collect()
});

pub fn user_role_name(name: &str) -> Result<(), UsernameValidationError> {
    // Normalize username to lowercase for validation
    let name = name.to_lowercase();

    // Check length constraints
    if name.len() < 3 || name.len() > 64 {
        return Err(UsernameValidationError::InvalidLength);
    }

    // Check if name is reserved
    if RESERVED_NAMES.contains(name.as_str()) {
        return Err(UsernameValidationError::ReservedName);
    }

    let chars: Vec<char> = name.chars().collect();

    // Check first character (must be alphanumeric)
    if let Some(first_char) = chars.first()
        && !first_char.is_ascii_alphanumeric()
    {
        return Err(UsernameValidationError::InvalidStartChar);
    }

    // Check last character (must be alphanumeric)
    if let Some(last_char) = chars.last()
        && !last_char.is_ascii_alphanumeric()
    {
        return Err(UsernameValidationError::InvalidEndChar);
    }

    // Check all characters and consecutive special chars
    let mut prev_was_special = false;
    for &ch in &chars {
        match ch {
            // Allow alphanumeric
            c if c.is_ascii_alphanumeric() => {
                prev_was_special = false;
            }
            // Allow specific special characters
            '_' | '-' | '.' => {
                if prev_was_special {
                    return Err(UsernameValidationError::ConsecutiveSpecialChars);
                }
                prev_was_special = true;
            }
            // Reject any other character
            _ => {
                return Err(UsernameValidationError::InvalidCharacter);
            }
        }
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
            "Username contains invalid characters. Only alphanumeric characters and special characters (underscore, hyphen and dot) are allowed"
        )]
        SpecialChar,
        #[error("Username should start with an alphanumeric character")]
        InvalidStartChar,
        #[error("Username should end with an alphanumeric character")]
        InvalidEndChar,
        #[error(
            "Username contains invalid characters. Only alphanumeric characters and special characters (underscore, hyphen and dot) are allowed"
        )]
        InvalidCharacter,
        #[error("Username contains consecutive special characters")]
        ConsecutiveSpecialChars,
        #[error("Username is reserved")]
        ReservedName,
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
