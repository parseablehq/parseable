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

use serde_derive::Deserialize;
use serde_derive::Serialize;

use crate::Error;

// TODO: add more sql keywords here in lower case
const DENIED_NAMES: &[&str] = &[
    "select", "from", "where", "group", "by", "order", "limit", "offset", "join", "and",
];

#[derive(Default, Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Alerts {
    pub alerts: Vec<Alert>,
}

#[derive(Default, Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Alert {
    pub name: String,
    pub message: String,
    pub rule: Rule,
    pub target: Vec<Target>,
}

#[derive(Default, Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Rule {
    pub field: String,
    pub contains: String,
    pub repeats: u32,
    pub within: String,
}

#[derive(Default, Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Target {
    pub name: String,
    #[serde(rename = "server_url")]
    pub server_url: String,
    #[serde(rename = "api_key")]
    pub api_key: String,
}

pub fn alert(body: String) -> Result<(), Error> {
    let alerts: Alerts = serde_json::from_str(body.as_str())?;
    for alert in alerts.alerts {
        if alert.name.is_empty() {
            return Err(Error::InvalidAlert(
                "alert name cannot be empty".to_string(),
            ));
        }
        if alert.message.is_empty() {
            return Err(Error::InvalidAlert(
                "alert message cannot be empty".to_string(),
            ));
        }
        if alert.rule.contains.is_empty() {
            return Err(Error::InvalidAlert("rule.contains must be set".to_string()));
        }
        if alert.rule.field.is_empty() {
            return Err(Error::InvalidAlert("rule.field must be set".to_string()));
        }
        if alert.rule.within.is_empty() {
            return Err(Error::InvalidAlert("rule.within must be set".to_string()));
        }
        if alert.rule.repeats == 0 {
            return Err(Error::InvalidAlert(
                "rule.repeats can't be set to 0".to_string(),
            ));
        }
        if alert.target.is_empty() {
            return Err(Error::InvalidAlert(
                "alert must have at least one target".to_string(),
            ));
        }
    }
    Ok(())
}

pub fn stream_name(str_name: &str) -> Result<(), Error> {
    if str_name.is_empty() {
        return Err(Error::EmptyName);
    }

    if str_name.chars().all(char::is_numeric) {
        return Err(Error::NameNumericOnly(str_name.to_owned()));
    }

    if str_name.chars().next().unwrap().is_numeric() {
        return Err(Error::NameCantStartWithNumber(str_name.to_owned()));
    }

    for c in str_name.chars() {
        match c {
            ' ' => return Err(Error::NameWhiteSpace(str_name.to_owned())),
            c if !c.is_alphanumeric() => return Err(Error::NameSpecialChar(str_name.to_owned())),
            c if c.is_ascii_uppercase() => return Err(Error::NameUpperCase(str_name.to_owned())),
            _ => {}
        }
    }

    if DENIED_NAMES.contains(&str_name) {
        return Err(Error::SQLKeyword(str_name.to_owned()));
    }

    Ok(())
}
