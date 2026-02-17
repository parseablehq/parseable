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

use clap::Parser;
use std::{env, fs, path::PathBuf};

use url::Url;

#[cfg(feature = "kafka")]
use crate::connectors::kafka::config::KafkaConfig;

use crate::{
    oidc::{self, OpenidConfig},
    option::{Compression, Mode, validation},
    storage::{AzureBlobConfig, FSConfig, GcsConfig, S3Config},
};

/// Default username and password for Parseable server, used by default for local mode.
/// NOTE: obviously not recommended for production
pub const DEFAULT_USERNAME: &str = "admin";
pub const DEFAULT_PASSWORD: &str = "admin";

pub const DATASET_FIELD_COUNT_LIMIT: usize = 1000;
#[derive(Parser)]
#[command(
    name = "parseable",
    bin_name = "parseable",
    about = "Cloud Native, log analytics platform for modern applications.",
    long_about = r#"
Cloud Native, log analytics platform for modern applications.

Usage:
parseable [command] [options..]


Help:
parseable [command] --help

"#,
    arg_required_else_help = true,
    color = clap::ColorChoice::Always,
    version = env!("CARGO_PKG_VERSION"),
    propagate_version = true,
    next_line_help = false,
    help_template = r#"{name} v{version}
{about}
Join the community at https://logg.ing/community.

{all-args}
        "#,
    subcommand_required = true,
)]
pub struct Cli {
    #[command(subcommand)]
    pub storage: StorageOptions,
}

#[derive(Parser)]
pub enum StorageOptions {
    #[command(name = "local-store")]
    Local(LocalStoreArgs),

    #[command(name = "s3-store")]
    S3(S3StoreArgs),

    #[command(name = "blob-store")]
    Blob(BlobStoreArgs),

    #[command(name = "gcs-store")]
    Gcs(GcsStoreArgs),
}

#[derive(Parser)]
pub struct LocalStoreArgs {
    #[command(flatten)]
    pub options: Options,
    #[command(flatten)]
    pub storage: FSConfig,
    #[cfg(feature = "kafka")]
    #[command(flatten)]
    pub kafka: KafkaConfig,
}

#[derive(Parser)]
pub struct S3StoreArgs {
    #[command(flatten)]
    pub options: Options,
    #[command(flatten)]
    pub storage: S3Config,
    #[cfg(feature = "kafka")]
    #[command(flatten)]
    pub kafka: KafkaConfig,
}

#[derive(Parser)]
pub struct BlobStoreArgs {
    #[command(flatten)]
    pub options: Options,
    #[command(flatten)]
    pub storage: AzureBlobConfig,
    #[cfg(feature = "kafka")]
    #[command(flatten)]
    pub kafka: KafkaConfig,
}

#[derive(Parser)]
pub struct GcsStoreArgs {
    #[command(flatten)]
    pub options: Options,
    #[command(flatten)]
    pub storage: GcsConfig,
    #[cfg(feature = "kafka")]
    #[command(flatten)]
    pub kafka: KafkaConfig,
}

#[derive(Parser, Debug, Default)]
pub struct Options {
    // Authentication
    #[arg(long, env = "P_USERNAME", help = "Admin username to be set for this Parseable server", default_value = DEFAULT_USERNAME)]
    pub username: String,

    #[arg(long, env = "P_PASSWORD", help = "Admin password to be set for this Parseable server", default_value = DEFAULT_PASSWORD)]
    pub password: String,

    // Server configuration
    #[arg(
        long,
        env = "P_ADDR",
        default_value = "0.0.0.0:8000",
        value_parser = validation::socket_addr,
        help = "Address and port for Parseable HTTP(s) server"
    )]
    pub address: String,

    #[arg(
        long = "origin",
        env = "P_ORIGIN_URI",
        value_parser = validation::url,
        help = "Parseable server global domain address"
    )]
    pub domain_address: Option<Url>,

    #[arg(
        long,
        env = "P_MODE",
        default_value = "all",
        value_parser = validation::mode,
        help = "Mode of operation"
    )]
    pub mode: Mode,

    #[arg(
        long,
        env = "P_CORS",
        default_value = "true",
        help = "Enable/Disable CORS, default disabled"
    )]
    pub cors: bool,

    #[arg(
        long,
        env = "P_CHECK_UPDATE",
        default_value = "true",
        help = "Enable/Disable checking for new Parseable release"
    )]
    pub check_update: bool,

    #[arg(
        long,
        env = "P_SEND_ANONYMOUS_USAGE_DATA",
        default_value = "true",
        help = "Enable/Disable anonymous telemetry data collection"
    )]
    pub send_analytics: bool,

    #[arg(
        long,
        env = "P_MASK_PII",
        default_value = "false",
        help = "mask PII data when sending to Prism"
    )]
    pub mask_pii: bool,

    #[arg(
        long,
        env = "P_METRICS_ENDPOINT_AUTH",
        default_value = "true",
        help = "Enable/Disable authentication for /v1/metrics endpoint"
    )]
    pub metrics_endpoint_auth: bool,

    // TLS/Security
    #[arg(
        long,
        env = "P_TLS_CERT_PATH",
        value_parser = validation::file_path,
        help = "Local path on this device where certificate file is located. Required to enable TLS"
    )]
    pub tls_cert_path: Option<PathBuf>,

    #[arg(
        long,
        env = "P_TLS_KEY_PATH",
        value_parser = validation::file_path,
        help = "Local path on this device where private key file is located. Required to enable TLS"
    )]
    pub tls_key_path: Option<PathBuf>,

    #[arg(
        long,
        env = "P_TRUSTED_CA_CERTS_DIR",
        value_parser = validation::canonicalize_path,
        help = "Local path on this device where all trusted certificates are located"
    )]
    pub trusted_ca_certs_path: Option<PathBuf>,

    /// Allows invalid TLS certificates for intra-cluster communication.
    /// This is needed when nodes connect to each other via IP addresses
    /// which don't match the domain names in their certificates.
    /// SECURITY NOTE: Only enable this for trusted internal networks.
    #[arg(
        long,
        env = "P_TLS_SKIP_VERIFY",
        value_name = "bool",
        default_value = "false"
    )]
    pub tls_skip_verify: bool,

    // Storage configuration
    #[arg(
        long,
        env = "P_STAGING_DIR",
        default_value = "./staging",
        value_parser = validation::canonicalize_path,
        help = "Local path on this device to be used as landing point for incoming events"
    )]
    pub local_staging_path: PathBuf,

    #[arg(
        long = "hot-tier-path",
        env = "P_HOT_TIER_DIR",
        value_parser = validation::canonicalize_path,
        help = "Local path on this device to be used for hot tier data"
    )]
    pub hot_tier_storage_path: Option<PathBuf>,

    //TODO: remove this when smart cache is implemented
    #[arg(
        long = "index-storage-path",
        env = "P_INDEX_DIR",
        value_parser = validation::canonicalize_path,
        help = "Local path on this indexer used for indexing"
    )]
    pub index_storage_path: Option<PathBuf>,

    #[arg(
        long,
        env = "P_MAX_DISK_USAGE_PERCENT",
        default_value = "80.0",
        value_parser = validation::validate_disk_usage,
        help = "Maximum allowed disk usage in percentage e.g 90.0 for 90%"
    )]
    pub max_disk_usage: f64,

    // Service ports
    #[arg(
        long,
        env = "P_GRPC_PORT",
        default_value = "8001",
        help = "Port for gRPC server"
    )]
    pub grpc_port: u16,

    #[arg(
        long,
        env = "P_FLIGHT_PORT",
        default_value = "8002",
        help = "Port for Arrow Flight Querying Engine"
    )]
    pub flight_port: u16,

    // Performance settings
    #[arg(
        long,
        long = "livetail-capacity",
        env = "P_LIVETAIL_CAPACITY",
        default_value = "1000",
        help = "Number of rows in livetail channel"
    )]
    pub livetail_channel_capacity: usize,

    #[arg(
        long,
        long = "query-mempool-size",
        env = "P_QUERY_MEMORY_LIMIT",
        help = "Set a fixed memory limit for query in GiB"
    )]
    pub query_memory_pool_size: Option<usize>,
    // reduced the max row group size from 1048576
    // smaller row groups help in faster query performance in multi threaded query
    #[arg(
        long,
        env = "P_PARQUET_ROW_GROUP_SIZE",
        default_value = "262144",
        help = "Number of rows in a row group"
    )]
    pub row_group_size: usize,

    #[arg(
        long,
        env = "P_EXECUTION_BATCH_SIZE",
        default_value = "20000",
        help = "batch size for query execution"
    )]
    pub execution_batch_size: usize,

    #[arg(
        long = "compression-algo",
        env = "P_PARQUET_COMPRESSION_ALGO",
        default_value = "lz4_raw",
        value_parser = validation::compression,
        help = "Parquet compression algorithm"
    )]
    pub parquet_compression: Compression,

    // Resource monitoring
    #[arg(
        long,
        env = "P_RESOURCE_CHECK_INTERVAL",
        default_value = "15",
        value_parser = validation::validate_seconds,
        help = "Resource monitoring check interval in seconds"
    )]
    pub resource_check_interval: u64,

    #[arg(
        long,
        env = "P_CPU_THRESHOLD",
        default_value = "100.0",
        value_parser = validation::validate_percentage,
        help = "CPU utilization threshold percentage (0.0-100.0) for resource monitoring"
    )]
    pub cpu_utilization_threshold: f32,

    #[arg(
        long,
        env = "P_MEMORY_THRESHOLD",
        default_value = "100.0",
        value_parser = validation::validate_percentage,
        help = "Memory utilization threshold percentage (0.0-100.0) for resource monitoring"
    )]
    pub memory_utilization_threshold: f32,

    // Integration features
    #[arg(
        long,
        env = "P_OPENAI_API_KEY",
        help = "OpenAI key to enable llm features"
    )]
    pub open_ai_key: Option<String>,

    #[arg(
        long,
        env = "P_INGESTOR_ENDPOINT",
        default_value = "",
        help = "URL to connect to this specific ingestor. Default is the address of the server"
    )]
    pub ingestor_endpoint: String,

    #[arg(
        long,
        env = "P_INDEXER_ENDPOINT",
        default_value = "",
        help = "URL to connect to this specific indexer. Default is the address of the server"
    )]
    pub indexer_endpoint: String,

    #[arg(
        long,
        env = "P_QUERIER_ENDPOINT",
        default_value = "",
        help = "URL to connect to this specific querier. Default is the address of the server"
    )]
    pub querier_endpoint: String,

    #[command(flatten)]
    pub oidc: Option<OidcConfig>,

    #[arg(long, env = "P_MS_CLARITY_TAG", help = "Tag for MS Clarity")]
    pub ms_clarity_tag: Option<String>,

    #[arg(
        long,
        env = "P_DATASET_FIELD_COUNT_LIMIT",
        default_value_t = DATASET_FIELD_COUNT_LIMIT,
        value_parser = validation::validate_dataset_fields_allowed_limit,
        help = "total number of fields recommended in a dataset"
    )]
    pub dataset_fields_allowed_limit: usize,

    // maximum level of flattening allowed for events
    // this is to prevent nested list type fields from getting created
    #[arg(
        long,
        env = "P_MAX_FLATTEN_LEVEL",
        default_value = "10",
        help = "Maximum level of flattening allowed for events"
    )]
    pub event_flatten_level: usize,

    // maximum limit to store the statistics for a field
    #[arg(
        long,
        env = "P_MAX_FIELD_STATISTICS",
        default_value = "50",
        help = "Maximum number of field statistics to store"
    )]
    pub max_field_statistics: usize,

    #[arg(
        long,
        env = "P_MAX_EVENT_PAYLOAD_SIZE",
        default_value = "10485760",
        value_parser = validation::validate_payload_size,
        help = "Maximum allowed event payload size in bytes for ingest endpoints"
    )]
    pub max_event_payload_size: usize,

    // the duration during which local sync should be completed
    #[arg(
        long,
        env = "P_LOCAL_SYNC_THRESHOLD",
        default_value = "30",
        help = "Local sync threshold in seconds"
    )]
    pub local_sync_threshold: u64,
    // the duration during which object store sync should be completed
    #[arg(
        long,
        env = "P_OBJECT_STORE_SYNC_THRESHOLD",
        default_value = "15",
        help = "Object store sync threshold in seconds"
    )]
    pub object_store_sync_threshold: u64,
    // the oidc scope
    #[arg(
        long = "oidc-scope",
        name = "oidc-scope",
        env = "P_OIDC_SCOPE",
        default_value = "openid profile email offline_access",
        required = false,
        help = "OIDC scope to request (default: openid profile email offline_access)"
    )]
    pub scope: String,

    // event's maximum chunk age in hours
    #[arg(
        long,
        env = "P_EVENT_MAX_CHUNK_AGE",
        // Accept 0 to disallow older-than-reference events; cap to one week by default.
        value_parser = clap::value_parser!(u64).range(0..=168),
        default_value = "1",
        help = "Max allowed age gap (in hours) between events within the same node, relative to the reference event"
    )]
    pub event_max_chunk_age: u64,
}

#[derive(Parser, Debug)]
pub struct OidcConfig {
    #[arg(
        long = "oidc-client",
        name = "oidc-client",
        env = "P_OIDC_CLIENT_ID",
        required = false,
        help = "Client id for OIDC provider"
    )]
    pub client_id: String,

    #[arg(
        long = "oidc-client-secret",
        name = "oidc-client-secret",
        env = "P_OIDC_CLIENT_SECRET",
        required = false,
        help = "Client secret for OIDC provider"
    )]
    pub secret: String,

    #[arg(
        long = "oidc-issuer",
        name = "oidc-issuer",
        env = "P_OIDC_ISSUER",
        required = false,
        value_parser = validation::url,
        help = "OIDC provider's host address"
    )]
    pub issuer: Url,
}

impl Options {
    pub fn local_stream_data_path(&self, stream_name: &str) -> PathBuf {
        self.local_staging_path.join(stream_name)
    }

    pub fn get_scheme(&self) -> String {
        if self.tls_cert_path.is_some() && self.tls_key_path.is_some() {
            "https".to_string()
        } else {
            "http".to_string()
        }
    }

    pub fn openid(&self) -> Option<OpenidConfig> {
        let OidcConfig {
            secret,
            client_id,
            issuer,
        } = self.oidc.as_ref()?;
        let origin = if let Some(url) = self.domain_address.clone() {
            oidc::Origin::Production(url)
        } else {
            oidc::Origin::Local {
                socket_addr: self.address.clone(),
                https: self.tls_cert_path.is_some() && self.tls_key_path.is_some(),
            }
        };
        Some(OpenidConfig {
            id: client_id.clone(),
            secret: secret.clone(),
            issuer: issuer.clone(),
            origin,
        })
    }

    pub fn is_default_creds(&self) -> bool {
        self.username == DEFAULT_USERNAME && self.password == DEFAULT_PASSWORD
    }

    /// Path to staging directory, ensures that it exists or panics
    pub fn staging_dir(&self) -> &PathBuf {
        fs::create_dir_all(&self.local_staging_path)
            .expect("Should be able to create dir if doesn't exist");

        &self.local_staging_path
    }

    /// Path to index directory, ensures that it exists or returns the PathBuf
    pub fn index_dir(&self) -> Option<&PathBuf> {
        if let Some(path) = &self.index_storage_path {
            fs::create_dir_all(path)
                .expect("Should be able to create index directory if it doesn't exist");
            Some(path)
        } else {
            None
        }
    }

    /// get the address of the server
    /// based on the mode
    pub fn get_url(&self, mode: Mode) -> Url {
        let endpoint = match mode {
            Mode::Ingest => self.get_endpoint(&self.ingestor_endpoint, "P_INGESTOR_ENDPOINT"),
            Mode::Index => self.get_endpoint(&self.indexer_endpoint, "P_INDEXER_ENDPOINT"),
            Mode::Query => self.get_endpoint(&self.querier_endpoint, "P_QUERIER_ENDPOINT"),
            _ => return self.build_url(&self.address),
        };

        self.parse_endpoint(&endpoint)
    }

    /// get the endpoint for the server
    /// if env var is empty, use the address, else use the env var
    fn get_endpoint(&self, endpoint: &str, env_var: &str) -> String {
        if endpoint.is_empty() {
            self.address.to_string()
        } else {
            if endpoint.starts_with("http") {
                panic!(
                    "Invalid value `{endpoint}`, please set the environment variable `{env_var}` to `<ip address / DNS>:<port>` without the scheme (e.g., 192.168.1.1:8000 or example.com:8000). Please refer to the documentation: https://logg.ing/env for more details.",
                );
            }
            endpoint.to_string()
        }
    }

    /// parse the endpoint to get the address and port
    /// if the address is an env var, resolve it
    /// if the port is an env var, resolve it
    fn parse_endpoint(&self, endpoint: &str) -> Url {
        let addr_parts: Vec<&str> = endpoint.split(':').collect();

        if addr_parts.len() != 2 {
            panic!(
                "Invalid value `{endpoint}`, please set the environment variable to `<ip address / DNS>:<port>` without the scheme (e.g., 192.168.1.1:8000 or example.com:8000). Please refer to the documentation: https://logg.ing/env for more details."
            );
        }

        let hostname = self.resolve_env_var(addr_parts[0]);
        let port = self.resolve_env_var(addr_parts[1]);

        self.build_url(&format!("{hostname}:{port}"))
    }

    /// resolve the env var
    /// if the env var is not set, panic
    /// if the env var is set, return the value
    fn resolve_env_var(&self, value: &str) -> String {
        if let Some(env_var) = value.strip_prefix('$') {
            let resolved_value = env::var(env_var).unwrap_or_else(|_| {
                panic!(
                    "The environment variable `{env_var}` is not set. Please set it to a valid value. Refer to the documentation: https://logg.ing/env for more details."
                );
            });

            if resolved_value.starts_with("http") {
                panic!(
                    "Invalid value `{resolved_value}`, please set the environment variable `{env_var}` to `<ip address / DNS>` without the scheme (e.g., 192.168.1.1 or example.com). Please refer to the documentation: https://logg.ing/env for more details.",
                );
            }

            resolved_value
        } else {
            value.to_string()
        }
    }

    /// build the url from the address
    fn build_url(&self, address: &str) -> Url {
        format!("{}://{}", self.get_scheme(), address)
            .parse::<Url>()
            .unwrap_or_else(|err| {
                panic!(
                    "{err}, failed to parse `{address}` as Url. Please set the environment variable `P_ADDR` to `<ip address>:<port>` without the scheme (e.g., 192.168.1.1:8000). Please refer to the documentation: https://logg.ing/env for more details."
                );
            })
    }
}
