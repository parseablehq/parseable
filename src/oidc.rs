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

use std::collections::HashMap;

use openid::{Client, CompactJson, CustomClaims, Discovered, StandardClaims};
use url::Url;

pub type DiscoveredClient = Client<Discovered, Claims>;

// If domain is not configured then
// we can assume running in a development mode or private environment
#[derive(Debug, Clone)]
pub enum Origin {
    // socket address
    Local { socket_addr: String, https: bool },
    // domain url
    Production(Url),
}

/// Configuration for OpenID Connect
#[derive(Debug, Clone)]
pub struct OpenidConfig {
    /// Client id
    pub id: String,
    /// Client Secret
    pub secret: String,
    /// OP host address over which discovery can be done
    pub issuer: Url,
    /// Current client host address which will be used for redirects  
    pub origin: Origin,
}

impl OpenidConfig {
    /// Create a new oidc client from server configuration.
    /// redirect_suffix
    pub async fn connect(
        self,
        redirect_to: &str,
    ) -> Result<DiscoveredClient, openid::error::Error> {
        let redirect_uri = match self.origin {
            Origin::Local { socket_addr, https } => {
                let protocol = if https { "https" } else { "http" };
                url::Url::parse(&format!("{protocol}://{socket_addr}")).expect("valid url")
            }
            Origin::Production(url) => url,
        };

        let redirect_uri = redirect_uri.join(redirect_to).expect("valid suffix");
        DiscoveredClient::discover(self.id, self.secret, redirect_uri.to_string(), self.issuer)
            .await
    }
}

#[derive(Debug, serde::Serialize, serde::Deserialize, Clone)]
pub struct Claims {
    #[serde(flatten)]
    pub standard: StandardClaims,
    #[serde(flatten)]
    pub other: HashMap<String, serde_json::Value>,
}

impl CustomClaims for Claims {
    fn standard_claims(&self) -> &StandardClaims {
        &self.standard
    }
}

impl CompactJson for Claims {}
