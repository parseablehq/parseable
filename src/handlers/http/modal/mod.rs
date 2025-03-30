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

use std::{path::Path, sync::Arc};

use actix_web::{middleware::from_fn, web::ServiceConfig, App, HttpServer};
use actix_web_prometheus::PrometheusMetrics;
use anyhow::Context;
use async_trait::async_trait;
use base64::{prelude::BASE64_STANDARD, Engine};
use bytes::Bytes;
use futures::future;
use openid::Discovered;
use relative_path::RelativePathBuf;
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};
use ssl_acceptor::get_ssl_acceptor;
use tokio::sync::oneshot;
use tracing::{error, info, warn};

use crate::{
    alerts::ALERTS,
    cli::Options,
    correlation::CORRELATIONS,
    oidc::Claims,
    option::Mode,
    parseable::PARSEABLE,
    storage::{ObjectStorageProvider, PARSEABLE_ROOT_DIRECTORY},
    users::{dashboards::DASHBOARDS, filters::FILTERS},
    utils::{get_indexer_id, get_ingestor_id},
};

use super::{audit, cross_origin_config, health_check, API_BASE_PATH, API_VERSION};

pub mod ingest;
pub mod ingest_server;
pub mod query;
pub mod query_server;
pub mod server;
pub mod ssl_acceptor;
pub mod utils;

pub type OpenIdClient = Arc<openid::Client<Discovered, Claims>>;

// to be decided on what the Default version should be
pub const DEFAULT_VERSION: &str = "v3";

include!(concat!(env!("OUT_DIR"), "/generated.rs"));

#[async_trait]
pub trait ParseableServer {
    /// configure the router
    fn configure_routes(config: &mut ServiceConfig, oidc_client: Option<OpenIdClient>)
    where
        Self: Sized;

    /// load metadata/configuration from persistence for previous sessions of parseable
    async fn load_metadata(&self) -> anyhow::Result<Option<Bytes>>;

    /// code that describes starting and setup procedures for each type of server
    async fn init(
        &self,
        prometheus: &PrometheusMetrics,
        shutdown_rx: oneshot::Receiver<()>,
    ) -> anyhow::Result<()>;

    /// configure the server
    async fn start(
        &self,
        shutdown_rx: oneshot::Receiver<()>,
        prometheus: PrometheusMetrics,
        oidc_client: Option<crate::oidc::OpenidConfig>,
    ) -> anyhow::Result<()>
    where
        Self: Sized,
    {
        let oidc_client = match oidc_client {
            Some(config) => {
                let client = config
                    .connect(&format!("{API_BASE_PATH}/{API_VERSION}/o/code"))
                    .await?;
                Some(Arc::new(client))
            }

            None => None,
        };

        // get the ssl stuff
        let ssl = get_ssl_acceptor(
            &PARSEABLE.options.tls_cert_path,
            &PARSEABLE.options.tls_key_path,
            &PARSEABLE.options.trusted_ca_certs_path,
        )?;

        // fn that creates the app
        let create_app_fn = move || {
            App::new()
                .wrap(prometheus.clone())
                .configure(|config| Self::configure_routes(config, oidc_client.clone()))
                .wrap(from_fn(health_check::check_shutdown_middleware))
                .wrap(from_fn(audit::audit_log_middleware))
                .wrap(actix_web::middleware::Logger::default())
                .wrap(actix_web::middleware::Compress::default())
                .wrap(cross_origin_config())
        };

        // Create the HTTP server
        let http_server = HttpServer::new(create_app_fn)
            .workers(num_cpus::get())
            .shutdown_timeout(60);

        // Start the server with or without TLS
        let srv = if let Some(config) = ssl {
            http_server
                .bind_rustls_0_22(&PARSEABLE.options.address, config)?
                .run()
        } else {
            http_server.bind(&PARSEABLE.options.address)?.run()
        };

        // Graceful shutdown handling
        let srv_handle = srv.handle();

        let sync_task = tokio::spawn(async move {
            // Wait for the shutdown signal
            let _ = shutdown_rx.await;

            health_check::shutdown().await;

            // Initiate graceful shutdown
            info!("Graceful shutdown of HTTP server triggered");
            srv_handle.stop(true).await;
        });

        // Await the HTTP server to run
        let server_result = srv.await;

        // Wait for the sync task to complete before exiting
        if let Err(e) = sync_task.await {
            error!("Error in sync task: {:?}", e);
        } else {
            info!("Sync task completed successfully.");
        }

        // Return the result of the server
        server_result?;

        Ok(())
    }
}

pub async fn load_on_init() -> anyhow::Result<()> {
    // Run all loading operations concurrently
    let (correlations_result, filters_result, dashboards_result, alerts_result) = future::join4(
        async {
            CORRELATIONS
                .load()
                .await
                .context("Failed to load correlations")
        },
        async { FILTERS.load().await.context("Failed to load filters") },
        async { DASHBOARDS.load().await.context("Failed to load dashboards") },
        async { ALERTS.load().await.context("Failed to load alerts") },
    )
    .await;

    // Handle errors from each operation
    if let Err(e) = correlations_result {
        error!("{e}");
    }

    if let Err(err) = filters_result {
        error!("{err}");
    }

    if let Err(err) = dashboards_result {
        error!("{err}");
    }

    if let Err(err) = alerts_result {
        error!("{err}");
    }

    Ok(())
}

#[derive(Debug, Serialize, Deserialize, Default, Clone, Eq, PartialEq)]
pub struct IngestorMetadata {
    pub version: String,
    pub port: String,
    pub domain_name: String,
    pub bucket_name: String,
    pub token: String,
    pub ingestor_id: String,
    pub flight_port: String,
}

impl IngestorMetadata {
    pub fn new(
        port: String,
        domain_name: String,
        bucket_name: String,
        username: &str,
        password: &str,
        ingestor_id: String,
        flight_port: String,
    ) -> Self {
        let token = base64::prelude::BASE64_STANDARD.encode(format!("{username}:{password}"));

        Self {
            port,
            domain_name,
            version: DEFAULT_VERSION.to_string(),
            bucket_name,
            token: format!("Basic {token}"),
            ingestor_id,
            flight_port,
        }
    }

    /// Capture metadata information by either loading it from staging or starting fresh
    pub fn load(options: &Options, storage: &dyn ObjectStorageProvider) -> Arc<Self> {
        // all the files should be in the staging directory root
        let entries = options
            .staging_dir()
            .read_dir()
            .expect("Couldn't read from file");
        let url = options.get_url(Mode::Ingest);
        let port = url.port().unwrap_or(80).to_string();
        let url = url.to_string();
        let Options {
            username, password, ..
        } = options;
        let staging_path = options.staging_dir();
        let flight_port = options.flight_port.to_string();

        for entry in entries {
            // cause the staging directory will have only one file with ingestor in the name
            // so the JSON Parse should not error unless the file is corrupted
            let path = entry.expect("Should be a directory entry").path();
            if !path
                .file_name()
                .and_then(|s| s.to_str())
                .is_some_and(|s| s.contains("ingestor"))
            {
                continue;
            }

            // get the ingestor metadata from staging
            let bytes = std::fs::read(path).expect("File should be present");
            let mut meta =
                Self::from_bytes(&bytes, options.flight_port).expect("Extracted ingestor metadata");

            // compare url endpoint and port, update
            if meta.domain_name != url {
                info!(
                    "Domain Name was Updated. Old: {} New: {}",
                    meta.domain_name, url
                );
                meta.domain_name = url;
            }

            if meta.port != port {
                info!("Port was Updated. Old: {} New: {}", meta.port, port);
                meta.port = port;
            }

            let token = format!(
                "Basic {}",
                BASE64_STANDARD.encode(format!("{username}:{password}"))
            );
            if meta.token != token {
                // TODO: Update the message to be more informative with username and password
                warn!(
                    "Credentials were Updated. Tokens updated; Old: {} New: {}",
                    meta.token, token
                );
                meta.token = token;
            }
            meta.put_on_disk(staging_path)
                .expect("Couldn't write to disk");

            return Arc::new(meta);
        }

        let storage = storage.get_object_store();
        let meta = Self::new(
            port,
            url,
            storage.get_bucket_name(),
            username,
            password,
            get_ingestor_id(),
            flight_port,
        );

        meta.put_on_disk(staging_path)
            .expect("Should Be valid Json");
        Arc::new(meta)
    }

    pub fn get_ingestor_id(&self) -> String {
        self.ingestor_id.clone()
    }

    #[inline(always)]
    pub fn file_path(&self) -> RelativePathBuf {
        RelativePathBuf::from_iter([
            PARSEABLE_ROOT_DIRECTORY,
            &format!("ingestor.{}.json", self.get_ingestor_id()),
        ])
    }

    /// Updates json with `flight_port` field if not already present
    fn from_bytes(bytes: &[u8], flight_port: u16) -> anyhow::Result<Self> {
        let mut json: Map<String, Value> = serde_json::from_slice(bytes)?;
        json.entry("flight_port")
            .or_insert_with(|| Value::String(flight_port.to_string()));

        Ok(serde_json::from_value(Value::Object(json))?)
    }

    pub async fn migrate(&self) -> anyhow::Result<Option<IngestorMetadata>> {
        let imp = self.file_path();
        let bytes = match PARSEABLE.storage.get_object_store().get_object(&imp).await {
            Ok(bytes) => bytes,
            Err(_) => {
                return Ok(None);
            }
        };

        let resource = Self::from_bytes(&bytes, PARSEABLE.options.flight_port)?;
        let bytes = Bytes::from(serde_json::to_vec(&resource)?);

        resource.put_on_disk(PARSEABLE.options.staging_dir())?;

        PARSEABLE
            .storage
            .get_object_store()
            .put_object(&imp, bytes)
            .await?;

        Ok(Some(resource))
    }

    /// Puts the ingestor info into the staging.
    ///
    /// This function takes the ingestor info as a parameter and stores it in staging.
    /// # Parameters
    ///
    /// * `staging_path`: Staging root directory.
    pub fn put_on_disk(&self, staging_path: &Path) -> anyhow::Result<()> {
        let file_name = format!("ingestor.{}.json", self.ingestor_id);
        let file_path = staging_path.join(file_name);

        std::fs::write(file_path, serde_json::to_vec(&self)?)?;

        Ok(())
    }
}

#[derive(Debug, Serialize, Deserialize, Default, Clone, Eq, PartialEq)]
pub struct IndexerMetadata {
    pub version: String,
    pub port: String,
    pub domain_name: String,
    pub bucket_name: String,
    pub token: String,
    pub indexer_id: String,
    pub flight_port: String,
}

impl IndexerMetadata {
    pub fn new(
        port: String,
        domain_name: String,
        bucket_name: String,
        username: &str,
        password: &str,
        indexer_id: String,
        flight_port: String,
    ) -> Self {
        let token = base64::prelude::BASE64_STANDARD.encode(format!("{username}:{password}"));

        Self {
            port,
            domain_name,
            version: DEFAULT_VERSION.to_string(),
            bucket_name,
            token: format!("Basic {token}"),
            indexer_id,
            flight_port,
        }
    }

    /// Capture metadata information by either loading it from staging or starting fresh
    pub fn load(options: &Options, storage: &dyn ObjectStorageProvider) -> Arc<Self> {
        // all the files should be in the staging directory root
        let entries = options
            .staging_dir()
            .read_dir()
            .expect("Couldn't read from file");
        let url = options.get_url(Mode::Index);
        let port = url.port().unwrap_or(80).to_string();
        let url = url.to_string();
        let Options {
            username, password, ..
        } = options;
        let staging_path = options.staging_dir();
        let flight_port = options.flight_port.to_string();

        for entry in entries {
            // cause the staging directory will have only one file with indexer in the name
            // so the JSON Parse should not error unless the file is corrupted
            let path = entry.expect("Should be a directory entry").path();
            if !path
                .file_name()
                .and_then(|s| s.to_str())
                .is_some_and(|s| s.contains("indexer"))
            {
                continue;
            }

            // get the indexer metadata from staging
            let bytes = std::fs::read(path).expect("File should be present");
            let mut meta =
                Self::from_bytes(&bytes, options.flight_port).expect("Extracted indexer metadata");

            // compare url endpoint and port, update
            if meta.domain_name != url {
                info!(
                    "Domain Name was Updated. Old: {} New: {}",
                    meta.domain_name, url
                );
                meta.domain_name = url;
            }

            if meta.port != port {
                info!("Port was Updated. Old: {} New: {}", meta.port, port);
                meta.port = port;
            }

            let token = format!(
                "Basic {}",
                BASE64_STANDARD.encode(format!("{username}:{password}"))
            );
            if meta.token != token {
                // TODO: Update the message to be more informative with username and password
                warn!(
                    "Credentials were Updated. Tokens updated; Old: {} New: {}",
                    meta.token, token
                );
                meta.token = token;
            }
            meta.put_on_disk(staging_path)
                .expect("Couldn't write to disk");

            return Arc::new(meta);
        }

        let storage = storage.get_object_store();
        let meta = Self::new(
            port,
            url,
            storage.get_bucket_name(),
            username,
            password,
            get_indexer_id(),
            flight_port,
        );

        meta.put_on_disk(staging_path)
            .expect("Should Be valid Json");
        Arc::new(meta)
    }

    pub fn get_indexer_id(&self) -> String {
        self.indexer_id.clone()
    }

    #[inline(always)]
    pub fn file_path(&self) -> RelativePathBuf {
        RelativePathBuf::from_iter([
            PARSEABLE_ROOT_DIRECTORY,
            &format!("indexer.{}.json", self.get_indexer_id()),
        ])
    }

    /// Updates json with `flight_port` field if not already present
    fn from_bytes(bytes: &[u8], flight_port: u16) -> anyhow::Result<Self> {
        let mut json: Map<String, Value> = serde_json::from_slice(bytes)?;
        json.entry("flight_port")
            .or_insert_with(|| Value::String(flight_port.to_string()));

        Ok(serde_json::from_value(Value::Object(json))?)
    }

    pub async fn migrate(&self) -> anyhow::Result<Option<IndexerMetadata>> {
        let imp = self.file_path();
        let bytes = match PARSEABLE.storage.get_object_store().get_object(&imp).await {
            Ok(bytes) => bytes,
            Err(_) => {
                return Ok(None);
            }
        };

        let resource = Self::from_bytes(&bytes, PARSEABLE.options.flight_port)?;
        let bytes = Bytes::from(serde_json::to_vec(&resource)?);

        resource.put_on_disk(PARSEABLE.options.staging_dir())?;

        PARSEABLE
            .storage
            .get_object_store()
            .put_object(&imp, bytes)
            .await?;

        Ok(Some(resource))
    }

    /// Puts the indexer info into the staging.
    ///
    /// This function takes the indexer info as a parameter and stores it in staging.
    /// # Parameters
    ///
    /// * `staging_path`: Staging root directory.
    pub fn put_on_disk(&self, staging_path: &Path) -> anyhow::Result<()> {
        let file_name = format!("indexer.{}.json", self.indexer_id);
        let file_path = staging_path.join(file_name);

        std::fs::write(file_path, serde_json::to_vec(&self)?)?;

        Ok(())
    }
}

pub trait Metadata {
    fn domain_name(&self) -> &str;
    fn token(&self) -> &str;
    fn node_type(&self) -> &str;
}

impl Metadata for IngestorMetadata {
    fn domain_name(&self) -> &str {
        &self.domain_name
    }

    fn token(&self) -> &str {
        &self.token
    }
    fn node_type(&self) -> &str {
        "ingestor"
    }
}

impl Metadata for IndexerMetadata {
    fn domain_name(&self) -> &str {
        &self.domain_name
    }

    fn token(&self) -> &str {
        &self.token
    }
    fn node_type(&self) -> &str {
        "indexer"
    }
}
#[cfg(test)]
mod test {
    use actix_web::body::MessageBody;
    use bytes::Bytes;
    use rstest::rstest;

    use super::IngestorMetadata;

    #[rstest]
    fn test_deserialize_resource() {
        let lhs: IngestorMetadata = IngestorMetadata::new(
            "8000".to_string(),
            "https://localhost:8000".to_string(),
            "somebucket".to_string(),
            "admin",
            "admin",
            "ingestor_id".to_owned(),
            "8002".to_string(),
        );

        let rhs = serde_json::from_slice::<IngestorMetadata>(br#"{"version":"v3","port":"8000","domain_name":"https://localhost:8000","bucket_name":"somebucket","token":"Basic YWRtaW46YWRtaW4=", "ingestor_id": "ingestor_id","flight_port": "8002"}"#).unwrap();

        assert_eq!(rhs, lhs);
    }

    #[rstest]
    fn test_from_bytes_adds_flight_port() {
        let json = br#"{"version":"v3","port":"8000","domain_name":"https://localhost:8000","bucket_name":"somebucket","token":"Basic YWRtaW46YWRtaW4=","ingestor_id":"ingestor_id"}"#;
        let meta = IngestorMetadata::from_bytes(json, 8002).unwrap();
        assert_eq!(meta.flight_port, "8002");
    }

    #[rstest]
    fn test_from_bytes_preserves_existing_flight_port() {
        let json = br#"{"version":"v3","port":"8000","domain_name":"https://localhost:8000","bucket_name":"somebucket","token":"Basic YWRtaW46YWRtaW4=","ingestor_id":"ingestor_id","flight_port":"9000"}"#;
        let meta = IngestorMetadata::from_bytes(json, 8002).unwrap();
        assert_eq!(meta.flight_port, "9000");
    }

    #[rstest]
    fn test_serialize_resource() {
        let im = IngestorMetadata::new(
            "8000".to_string(),
            "https://localhost:8000".to_string(),
            "somebucket".to_string(),
            "admin",
            "admin",
            "ingestor_id".to_owned(),
            "8002".to_string(),
        );

        let lhs = Bytes::from(serde_json::to_vec(&im).unwrap());
        let rhs = br#"{"version":"v3","port":"8000","domain_name":"https://localhost:8000","bucket_name":"somebucket","token":"Basic YWRtaW46YWRtaW4=","ingestor_id":"ingestor_id","flight_port":"8002"}"#
                .try_into_bytes()
                .unwrap();

        assert_eq!(lhs, rhs);
    }
}
