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

pub async fn get_oidc_client(
    config: &Config,
    redirect_suffix: &str,
) -> Option<Result<openid::Client, openid::error::Error>> {
    if let (Some(id), Some(secret), Some(issuer), Some(redirect)) = (
        config.parseable.openid_client_id.to_owned(),
        config.parseable.openid_client_secret.to_owned(),
        config.parseable.openid_issuer.to_owned(),
        config
            .parseable
            .openid_redirect_uri
            .to_owned()
            .and_then(|url| url.join(redirect_suffix).ok()),
    ) {
        Some(DiscoveredClient::discover(id, secret, redirect.to_string(), issuer).await)
    } else {
        None
    }
}
