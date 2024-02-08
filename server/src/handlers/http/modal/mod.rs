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

pub mod ingest_server;
pub mod query_server;
pub mod server;
pub mod ssl_acceptor;

use std::sync::Arc;

use actix_web_prometheus::PrometheusMetrics;
use async_trait::async_trait;
use openid::Discovered;

use crate::oidc;
use serde::Deserialize;
use serde::Serialize;
pub type OpenIdClient = Arc<openid::Client<Discovered, oidc::Claims>>;

// to be decided on what the Default version should be
pub const DEFAULT_VERSION: &str = "v3";
pub const INGESTOR_FILE_EXTENSION: &str = "ingestor.json";

include!(concat!(env!("OUT_DIR"), "/generated.rs"));

#[async_trait(?Send)]
pub trait ParseableServer {
    // async fn validate(&self) -> Result<(), ObjectStorageError>;

    /// configure the server
    async fn start(
        &mut self,
        prometheus: PrometheusMetrics,
        oidc_client: Option<crate::oidc::OpenidConfig>,
    ) -> anyhow::Result<()>;
}

#[derive(Serialize, Debug, Deserialize, Default)]
pub struct IngesterMetadata {
    pub version: String,
    pub address: String,
    pub port: String,
    pub origin: String, // domain
    pub bucket_name: String,
}

impl IngesterMetadata {
    pub fn new(
        address: String,
        port: String,
        origin: String,
        version: String,
        bucket_name: String,
    ) -> Self {
        Self {
            address,
            port,
            origin,
            version,
            bucket_name,
        }
    }
}

#[cfg(test)]
mod test {
    use actix_web::body::MessageBody;
    use rstest::rstest;

    use super::{IngesterMetadata, DEFAULT_VERSION};

    #[rstest]
    fn check_resource() {
        let im = IngesterMetadata::new(
            "0.0.0.0".to_string(),
            "8000".to_string(),
            "https://localhost:8000".to_string(),
            DEFAULT_VERSION.to_string(),
            "somebucket".to_string(),
        );

        let lhs = serde_json::to_string(&im)
            .unwrap()
            .try_into_bytes()
            .unwrap();
        let rhs = br#"{"version":"v3",
"address":"0.0.0.0",
"port":"8000",
"origin":"https://localhost:8000",
"bucket_name":"somebucket"}"#
            .try_into_bytes()
            .unwrap();

        assert_eq!(lhs, rhs);
    }
}
