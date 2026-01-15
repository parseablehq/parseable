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

use std::{fmt, path::Path, sync::Arc};

use actix_web::{App, HttpServer, middleware::from_fn, web::ServiceConfig};
use actix_web_prometheus::PrometheusMetrics;
use anyhow::Context;
use async_trait::async_trait;
use base64::{Engine, prelude::BASE64_STANDARD};
use bytes::Bytes;
use futures::future;
use once_cell::sync::OnceCell;
use openid::Discovered;
use relative_path::RelativePathBuf;
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};
use ssl_acceptor::get_ssl_acceptor;
use tokio::sync::{RwLock, oneshot};
use tracing::{error, info, warn};

use crate::{
    alerts::{ALERTS, get_alert_manager, target::TARGETS},
    cli::Options,
    correlation::CORRELATIONS,
    hottier::{HotTierManager, StreamHotTier},
    metastore::metastore_traits::MetastoreObject,
    oidc::{Claims, DiscoveredClient},
    option::Mode,
    parseable::{DEFAULT_TENANT, PARSEABLE},
    storage::{ObjectStorageProvider, PARSEABLE_ROOT_DIRECTORY},
    users::{dashboards::DASHBOARDS, filters::FILTERS},
    utils::get_node_id,
};

use super::{API_BASE_PATH, API_VERSION, cross_origin_config, health_check, resource_check};

pub mod ingest;
pub mod ingest_server;
pub mod query;
pub mod query_server;
pub mod server;
pub mod ssl_acceptor;
pub mod utils;

pub type OpenIdClient = Arc<openid::Client<Discovered, Claims>>;

pub static OIDC_CLIENT: OnceCell<Arc<RwLock<GlobalClient>>> = OnceCell::new();

#[derive(Debug)]
pub struct GlobalClient {
    client: DiscoveredClient,
}

impl GlobalClient {
    pub fn set(&mut self, client: DiscoveredClient) {
        self.client = client;
    }

    pub fn client(&self) -> &DiscoveredClient {
        &self.client
    }

    pub fn new(client: DiscoveredClient) -> Self {
        Self { client }
    }
}

// to be decided on what the Default version should be
pub const DEFAULT_VERSION: &str = "v4";

include!(concat!(env!("OUT_DIR"), "/generated.rs"));

#[async_trait]
pub trait ParseableServer {
    /// configure the router
    fn configure_routes(config: &mut ServiceConfig)
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
        if let Some(config) = oidc_client {
            let client = config
                .connect(&format!("{API_BASE_PATH}/{API_VERSION}/o/code"))
                .await?;
            OIDC_CLIENT.get_or_init(|| Arc::new(RwLock::new(GlobalClient::new(client))));
        }

        // get the ssl stuff
        let ssl = get_ssl_acceptor(
            &PARSEABLE.options.tls_cert_path,
            &PARSEABLE.options.tls_key_path,
            &PARSEABLE.options.trusted_ca_certs_path,
        )?;

        // Start resource monitor
        let (resource_shutdown_tx, resource_shutdown_rx) = oneshot::channel();
        resource_check::spawn_resource_monitor(resource_shutdown_rx);

        // fn that creates the app
        let create_app_fn = move || {
            App::new()
                .wrap(prometheus.clone())
                .configure(|config| Self::configure_routes(config))
                .wrap(from_fn(health_check::check_shutdown_middleware))
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
                .bind_rustls_0_23(&PARSEABLE.options.address, config)?
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

            // Shutdown resource monitor
            let _ = resource_shutdown_tx.send(());

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
    let (correlations_result, filters_result, dashboards_result, alerts_result, targets_result) =
        future::join5(
            async {
                CORRELATIONS
                    .load()
                    .await
                    .context("Failed to load correlations")
            },
            async { FILTERS.load().await.context("Failed to load filters") },
            async { DASHBOARDS.load().await.context("Failed to load dashboards") },
            async {
                get_alert_manager().await;
                let guard = ALERTS.write().await;
                let alerts = if let Some(alerts) = guard.as_ref() {
                    alerts
                } else {
                    return Err(anyhow::Error::msg("No AlertManager set"));
                };
                alerts.load().await
            },
            async { TARGETS.load().await.context("Failed to load targets") },
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

    if let Err(err) = targets_result {
        error!("{err}");
    }

    Ok(())
}

/// NodeType represents the type of node in the cluster
#[derive(Debug, Serialize, Deserialize, Clone, Eq, PartialEq, Default)]
#[serde(rename_all = "lowercase")]
pub enum NodeType {
    #[default]
    Ingestor,
    Indexer,
    Querier,
    Prism,
    All,
}

impl NodeType {
    fn as_str(&self) -> &'static str {
        match self {
            NodeType::Ingestor => "ingestor",
            NodeType::Indexer => "indexer",
            NodeType::Querier => "querier",
            NodeType::Prism => "prism",
            NodeType::All => "all",
        }
    }

    fn to_mode(&self) -> Mode {
        match self {
            NodeType::Ingestor => Mode::Ingest,
            NodeType::Indexer => Mode::Index,
            NodeType::Querier => Mode::Query,
            NodeType::Prism => Mode::Prism,
            NodeType::All => Mode::All,
        }
    }
}

impl fmt::Display for NodeType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.as_str())
    }
}

#[derive(Debug, Serialize, Deserialize, Default, Clone, Eq, PartialEq)]
pub struct NodeMetadata {
    pub version: String,
    pub port: String,
    pub domain_name: String,
    pub bucket_name: String,
    pub token: String,
    pub node_id: String,
    pub flight_port: String,
    pub node_type: NodeType,
}

impl MetastoreObject for NodeMetadata {
    fn get_object_path(&self) -> String {
        self.file_path().to_string()
    }

    fn get_object_id(&self) -> String {
        self.node_id.clone()
    }
}

impl NodeMetadata {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        port: String,
        domain_name: String,
        bucket_name: String,
        username: &str,
        password: &str,
        node_id: String,
        flight_port: String,
        node_type: NodeType,
    ) -> Self {
        let token = base64::prelude::BASE64_STANDARD.encode(format!("{username}:{password}"));

        Self {
            port,
            domain_name,
            version: DEFAULT_VERSION.to_string(),
            bucket_name,
            token: format!("Basic {token}"),
            node_id,
            flight_port,
            node_type,
        }
    }

    pub async fn load_node_metadata(
        node_type: NodeType,
        tenant_id: &Option<String>,
    ) -> anyhow::Result<Arc<Self>> {
        let staging_path = PARSEABLE.options.staging_dir();
        let node_type_str = node_type.as_str();

        // Attempt to load metadata from staging
        if let Some(meta) = Self::load_from_staging(staging_path, node_type_str, &PARSEABLE.options)
        {
            return Self::process_and_store_metadata(meta, staging_path, node_type).await;
        }

        // Attempt to load metadata from storage
        let storage_metas = Self::load_from_storage(node_type.clone(), tenant_id).await;
        let url = PARSEABLE.options.get_url(node_type.to_mode());
        let port = url.port().unwrap_or(80).to_string();
        let url = url.to_string();

        for storage_meta in storage_metas {
            if storage_meta.domain_name == url && storage_meta.port == port {
                return Self::process_and_store_metadata(storage_meta, staging_path, node_type)
                    .await;
            }
        }

        // If no metadata is found, create a new one
        let meta = Self::create_new_metadata(&PARSEABLE.options, &*PARSEABLE.storage, node_type);
        Self::store_new_metadata(meta, staging_path).await
    }

    /// Process and store metadata
    async fn process_and_store_metadata(
        mut meta: Self,
        staging_path: &Path,
        node_type: NodeType,
    ) -> anyhow::Result<Arc<Self>> {
        Self::update_metadata(&mut meta, &PARSEABLE.options, node_type);
        meta.put_on_disk(staging_path)
            .expect("Couldn't write updated metadata to disk");

        PARSEABLE.metastore.put_node_metadata(&meta).await?;

        Ok(Arc::new(meta))
    }

    /// Store new metadata
    async fn store_new_metadata(meta: Self, staging_path: &Path) -> anyhow::Result<Arc<Self>> {
        meta.put_on_disk(staging_path)
            .expect("Couldn't write new metadata to disk");

        PARSEABLE.metastore.put_node_metadata(&meta).await?;

        Ok(Arc::new(meta))
    }

    async fn load_from_storage(
        node_type: NodeType,
        tenant_id: &Option<String>,
    ) -> Vec<NodeMetadata> {
        let obs = PARSEABLE
            .metastore
            .get_node_metadata(node_type, tenant_id)
            .await;

        let mut metadata = vec![];
        if let Ok(obs) = obs {
            for object in obs {
                //convert to NodeMetadata
                match serde_json::from_slice::<NodeMetadata>(&object) {
                    Ok(node_metadata) => metadata.push(node_metadata),
                    Err(e) => error!("Failed to deserialize NodeMetadata: {:?}", e),
                }
            }
        } else {
            error!("Couldn't read from storage");
        }
        // Return the metadata
        metadata
    }

    /// Load metadata from the staging directory
    fn load_from_staging(
        staging_path: &Path,
        node_type_str: &str,
        options: &Options,
    ) -> Option<Self> {
        let entries = match staging_path.read_dir() {
            Ok(entries) => entries,
            Err(e) => {
                error!("Couldn't read from staging directory: {}", e);
                return None;
            }
        };

        for entry in entries {
            let path = match entry {
                Ok(entry) => entry.path(),
                Err(e) => {
                    error!("Error reading directory entry: {}", e);
                    continue;
                }
            };
            if !Self::is_valid_metadata_file(&path, node_type_str) {
                continue;
            }

            let bytes = std::fs::read(&path).expect("File should be present");
            match Self::from_bytes(&bytes, options.flight_port) {
                Ok(meta) => return Some(meta),
                Err(e) => {
                    error!("Failed to extract {} metadata: {}", node_type_str, e);
                    return None;
                }
            }
        }

        None
    }

    /// Check if a file is a valid metadata file for the given node type
    fn is_valid_metadata_file(path: &Path, node_type_str: &str) -> bool {
        path.file_name()
            .and_then(|s| s.to_str())
            .is_some_and(|s| s.contains(node_type_str))
    }

    /// Update metadata fields if they differ from the current configuration
    fn update_metadata(meta: &mut Self, options: &Options, node_type: NodeType) {
        let url = options.get_url(node_type.to_mode());
        let port = url.port().unwrap_or(80).to_string();
        let url = url.to_string();

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

        let token = Self::generate_token(&options.username, &options.password);
        if meta.token != token {
            warn!(
                "Credentials were Updated. Tokens updated; Old: {} New: {}",
                meta.token, token
            );
            meta.token = token;
        }

        meta.node_type = node_type;
    }

    /// Create a new metadata instance
    fn create_new_metadata(
        options: &Options,
        storage: &dyn ObjectStorageProvider,
        node_type: NodeType,
    ) -> Self {
        let url = options.get_url(node_type.to_mode());
        let port = url.port().unwrap_or(80).to_string();
        let url = url.to_string();

        Self::new(
            port,
            url,
            storage.get_object_store().get_bucket_name(),
            &options.username,
            &options.password,
            get_node_id(),
            options.flight_port.to_string(),
            node_type,
        )
    }

    /// Generate a token from the username and password
    fn generate_token(username: &str, password: &str) -> String {
        format!(
            "Basic {}",
            BASE64_STANDARD.encode(format!("{username}:{password}"))
        )
    }

    pub fn get_node_id(&self) -> String {
        self.node_id.clone()
    }

    #[inline(always)]
    pub fn file_path(&self) -> RelativePathBuf {
        RelativePathBuf::from_iter([
            PARSEABLE_ROOT_DIRECTORY,
            &format!("{}.{}.json", self.node_type.as_str(), self.get_node_id()),
        ])
    }

    /// Updates json with `flight_port` field if not already present
    fn from_bytes(bytes: &[u8], flight_port: u16) -> anyhow::Result<Self> {
        let mut json: Map<String, Value> = serde_json::from_slice(bytes)?;

        // Check version
        let version = json.get("version").and_then(|version| version.as_str());

        if version == Some("v3") {
            fn migrate_legacy_id(
                json: &mut Map<String, Value>,
                legacy_id_key: &str,
                node_type_str: &str,
            ) -> bool {
                if json.contains_key(legacy_id_key) {
                    if let Some(id) = json.remove(legacy_id_key) {
                        json.insert("node_id".to_string(), id);
                        json.insert(
                            "version".to_string(),
                            Value::String(DEFAULT_VERSION.to_string()),
                        );
                    }
                    json.insert(
                        "node_type".to_string(),
                        Value::String(node_type_str.to_string()),
                    );
                    true
                } else {
                    false
                }
            }

            if !migrate_legacy_id(&mut json, "ingestor_id", "ingestor") {
                migrate_legacy_id(&mut json, "indexer_id", "indexer");
            }
        }
        // Determine node type and perform migration if needed

        // Add flight_port if missing
        json.entry("flight_port")
            .or_insert_with(|| Value::String(flight_port.to_string()));

        // Parse the JSON to our struct
        let metadata: Self = serde_json::from_value(Value::Object(json))?;

        Ok(metadata)
    }

    /// Puts the node info into the staging.
    ///
    /// This function takes the node info as a parameter and stores it in staging.
    /// # Parameters
    ///
    /// * `staging_path`: Staging root directory.
    pub fn put_on_disk(&self, staging_path: &Path) -> anyhow::Result<()> {
        let file_name = format!("{}.{}.json", self.node_type.as_str(), self.node_id);
        let file_path = staging_path.join(file_name);

        std::fs::write(file_path, serde_json::to_vec(&self)?)?;

        Ok(())
    }
}

pub trait Metadata {
    fn domain_name(&self) -> &str;
    fn token(&self) -> &str;
    fn node_type(&self) -> &NodeType;
    fn file_path(&self) -> RelativePathBuf;
}

impl Metadata for NodeMetadata {
    fn domain_name(&self) -> &str {
        &self.domain_name
    }

    fn token(&self) -> &str {
        &self.token
    }

    fn node_type(&self) -> &NodeType {
        &self.node_type
    }

    fn file_path(&self) -> RelativePathBuf {
        self.file_path()
    }
}

// Aliases for different node types
pub type IngestorMetadata = NodeMetadata;
pub type IndexerMetadata = NodeMetadata;
pub type QuerierMetadata = NodeMetadata;
pub type PrismMetadata = NodeMetadata;

/// Initialize hot tier metadata files for streams that have hot tier configuration
/// in their stream metadata but don't have local hot tier metadata files yet.
/// This function is called once during query server startup.
pub async fn initialize_hot_tier_metadata_on_startup(
    hot_tier_manager: &HotTierManager,
) -> anyhow::Result<()> {
    // Collect hot tier configurations from streams before doing async operations
    let hot_tier_configs: Vec<(String, Option<String>, StreamHotTier)> = {
        let tenants_guard = PARSEABLE.streams.read().unwrap();
        tenants_guard
            .iter()
            .flat_map(|(tenant_id, streams)| {
                streams.iter().filter_map(|(stream_name, stream)| {
                    // Skip if hot tier metadata file already exists for this stream
                    let tenant_id = if tenant_id.eq(DEFAULT_TENANT) {
                        None
                    } else {
                        Some(tenant_id.clone())
                    };
                    if hot_tier_manager.check_stream_hot_tier_exists(stream_name, &tenant_id) {
                        return None;
                    }

                    // Get the hot tier configuration from the in-memory stream metadata
                    stream
                        .get_hot_tier()
                        .map(|config| (stream_name.clone(), tenant_id, config.clone()))
                })
            })
            .collect()
        // let streams_guard = PARSEABLE.streams.read().unwrap();
        // streams_guard
        //     .iter()
        //     .filter_map(|(stream_name, stream)| {
        //         // Skip if hot tier metadata file already exists for this stream
        //         if hot_tier_manager.check_stream_hot_tier_exists(stream_name) {
        //             return None;
        //         }

        //         // Get the hot tier configuration from the in-memory stream metadata
        //         stream
        //             .get_hot_tier()
        //             .map(|config| (stream_name.clone(), config))
        //     })
        //     .collect()
    };

    for (stream_name, tenant_id, hot_tier_config) in hot_tier_configs {
        // Create the hot tier metadata file with the configuration from stream metadata
        let mut hot_tier_metadata = hot_tier_config;
        hot_tier_metadata.used_size = 0;
        hot_tier_metadata.available_size = hot_tier_metadata.size;
        hot_tier_metadata.oldest_date_time_entry = None;
        if hot_tier_metadata.version.is_none() {
            hot_tier_metadata.version = Some(crate::hottier::CURRENT_HOT_TIER_VERSION.to_string());
        }

        if let Err(e) = hot_tier_manager
            .put_hot_tier(&stream_name, &mut hot_tier_metadata, &tenant_id)
            .await
        {
            warn!(
                "Failed to initialize hot tier metadata for stream {}: {}",
                stream_name, e
            );
        }
    }

    Ok(())
}

#[cfg(test)]
mod test {
    use actix_web::body::MessageBody;
    use bytes::Bytes;
    use rstest::rstest;

    use crate::handlers::http::modal::NodeType;

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
            NodeType::Ingestor,
        );

        let rhs = serde_json::from_slice::<IngestorMetadata>(br#"{"version":"v4","port":"8000","domain_name":"https://localhost:8000","bucket_name":"somebucket","token":"Basic YWRtaW46YWRtaW4=","node_id": "ingestor_id","flight_port": "8002","node_type":"ingestor"}"#).unwrap();

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
            NodeType::Ingestor,
        );

        let lhs = Bytes::from(serde_json::to_vec(&im).unwrap());
        let rhs = br#"{"version":"v4","port":"8000","domain_name":"https://localhost:8000","bucket_name":"somebucket","token":"Basic YWRtaW46YWRtaW4=","node_id":"ingestor_id","flight_port":"8002","node_type":"ingestor"}"#
                .try_into_bytes()
                .unwrap();

        assert_eq!(lhs, rhs);
    }
}
