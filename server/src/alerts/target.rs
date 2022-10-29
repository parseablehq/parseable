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

use serde::{Deserialize, Serialize};

use super::{AlertState, CallableTarget, Context};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
#[serde(tag = "type")]
pub enum Target {
    Slack(SlackWebHook),
    #[serde(alias = "webhook")]
    Other(OtherWebHook),
}

impl Target {
    pub fn call(&self, payload: impl AsRef<Context>) {
        match self {
            Target::Slack(target) => target.call(payload.as_ref()),
            Target::Other(target) => target.call(payload.as_ref()),
        }
    }
}

#[derive(Default, Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SlackWebHook {
    #[serde(rename = "server_url")]
    server_url: String,
}

impl CallableTarget for SlackWebHook {
    fn call(&self, payload: &Context) {
        let alert = match payload.alert_state {
            AlertState::SetToFiring => ureq::json!({ "text": payload.default_alert_string() }),
            AlertState::Resolved => ureq::json!({ "text": payload.default_resolved_string() }),
            _ => unreachable!(),
        };

        if let Err(e) = ureq::post(&self.server_url)
            .set("Content-Type", "application/json")
            .send_json(alert)
        {
            log::error!("Couldn't make call to webhook, error: {}", e)
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(untagged)]
pub enum OtherWebHook {
    ApiKey {
        #[serde(rename = "server_url")]
        server_url: String,
        #[serde(rename = "api_key")]
        api_key: String,
    },
    Simple {
        #[serde(rename = "server_url")]
        server_url: String,
    },
}

impl CallableTarget for OtherWebHook {
    fn call(&self, payload: &Context) {
        let alert = match payload.alert_state {
            AlertState::SetToFiring => payload.default_alert_string(),
            AlertState::Resolved => payload.default_resolved_string(),
            _ => unreachable!(),
        };

        let res = match self {
            OtherWebHook::Simple { server_url } => ureq::post(server_url)
                .set("Content-Type", "text/plain; charset=iso-8859-1")
                .send_string(&alert),
            OtherWebHook::ApiKey {
                server_url,
                api_key,
            } => ureq::post(server_url)
                .set("Content-Type", "text/plain; charset=iso-8859-1")
                .set("X-API-Key", api_key)
                .send_string(&alert),
        };

        if let Err(e) = res {
            log::error!("Couldn't make call to webhook, error: {}", e)
        }
    }
}
