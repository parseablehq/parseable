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
use async_trait::async_trait;
use base64::Engine;
use bytes::Bytes;
use openid::Discovered;
use relative_path::RelativePathBuf;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use ssl_acceptor::get_ssl_acceptor;
use tokio::sync::oneshot;
use tracing::{error, info, warn};

use crate::{
    cli::Options,
    oidc::Claims,
    parseable::PARSEABLE,
    storage::PARSEABLE_ROOT_DIRECTORY,
    utils::{get_ingestor_id, get_url},
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

            // Perform S3 sync and wait for completion
            info!("Starting data sync to S3...");
            if let Err(e) = PARSEABLE.storage.get_object_store().sync(true).await {
                warn!("Failed to sync local data with object store. {:?}", e);
            } else {
                info!("Successfully synced all data to S3.");
            }

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
    pub fn load() -> Arc<Self> {
        // all the files should be in the staging directory root
        let entries = std::fs::read_dir(&PARSEABLE.options.local_staging_path)
            .expect("Couldn't read from file");
        let url = get_url();
        let port = url.port().unwrap_or(80).to_string();
        let url = url.to_string();
        let Options {
            username, password, ..
        } = PARSEABLE.options.as_ref();
        let staging_path = PARSEABLE.staging_dir();
        let flight_port = PARSEABLE.options.flight_port.to_string();

        for entry in entries {
            // cause the staging directory will have only one file with ingestor in the name
            // so the JSON Parse should not error unless the file is corrupted
            let path = entry.expect("Should be a directory entry").path();
            let flag = path
                .file_name()
                .unwrap_or_default()
                .to_str()
                .unwrap_or_default()
                .contains("ingestor");

            if flag {
                // get the ingestor metadata from staging
                let text = std::fs::read(path).expect("File should be present");
                let mut meta: Value = serde_json::from_slice(&text).expect("Valid JSON");

                // migrate the staging meta
                let obj = meta
                    .as_object_mut()
                    .expect("Could Not parse Ingestor Metadata Json");

                if obj.get("flight_port").is_none() {
                    obj.insert(
                        "flight_port".to_owned(),
                        Value::String(PARSEABLE.options.flight_port.to_string()),
                    );
                }

                let mut meta: IngestorMetadata =
                    serde_json::from_value(meta).expect("Couldn't write to disk");

                // compare url endpoint and port
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

                let token =
                    base64::prelude::BASE64_STANDARD.encode(format!("{}:{}", username, password));

                let token = format!("Basic {}", token);

                if meta.token != token {
                    // TODO: Update the message to be more informative with username and password
                    info!(
                        "Credentials were Updated. Old: {} New: {}",
                        meta.token, token
                    );
                    meta.token = token;
                }

                meta.put_on_disk(staging_path)
                    .expect("Couldn't write to disk");
                return Arc::new(meta);
            }
        }

        let storage = PARSEABLE.storage.get_object_store();
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

    pub async fn migrate(&self) -> anyhow::Result<Option<IngestorMetadata>> {
        let imp = self.file_path();
        let bytes = match PARSEABLE.storage.get_object_store().get_object(&imp).await {
            Ok(bytes) => bytes,
            Err(_) => {
                return Ok(None);
            }
        };
        let mut json = serde_json::from_slice::<Value>(&bytes)?;
        let meta = json
            .as_object_mut()
            .ok_or_else(|| anyhow::anyhow!("Unable to parse Ingester Metadata"))?;
        let fp = meta.get("flight_port");

        if fp.is_none() {
            meta.insert(
                "flight_port".to_owned(),
                Value::String(PARSEABLE.options.flight_port.to_string()),
            );
        }
        let bytes = Bytes::from(serde_json::to_vec(&json)?);

        let resource: IngestorMetadata = serde_json::from_value(json)?;
        resource.put_on_disk(PARSEABLE.staging_dir())?;

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
