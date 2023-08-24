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

use openid::DiscoveredClient;

use crate::option::Config;

/// Create a new oidc client from server configuration.
/// redirect_suffix
pub async fn get_oidc_client(
    config: &Config,
    redirect_suffix: &str,
) -> Option<Result<openid::Client, openid::error::Error>> {
    let id = config.parseable.openid_client_id.to_owned();
    let secret = config.parseable.openid_client_secret.to_owned();
    let issuer = config.parseable.openid_issuer.to_owned();
    let redirect_uri = config
        .parseable
        .domain_address
        .to_owned()
        .map(|url| url.join(redirect_suffix).expect("valid suffix"))
        .unwrap_or_else(|| {
            let socket_addr = config.parseable.address.clone();
            let host = config.parseable.get_scheme();
            url::Url::parse(&format!("{host}://{socket_addr}")).expect("valid url")
        });

    if let (Some(id), Some(secret), Some(issuer)) = (id, secret, issuer) {
        Some(DiscoveredClient::discover(id, secret, redirect_uri.to_string(), issuer).await)
    } else {
        None
    }
}
