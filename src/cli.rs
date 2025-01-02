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

use clap::{value_parser, Arg, ArgGroup, Command, FromArgMatches};
use std::path::PathBuf;

use url::Url;

use crate::{
    kafka::SslProtocol,
    oidc::{self, OpenidConfig},
    option::{validation, Compression, Mode},
};

#[derive(Debug, Default)]
pub struct Cli {
    /// The location of TLS Cert file
    pub tls_cert_path: Option<PathBuf>,

    /// The location of TLS Private Key file
    pub tls_key_path: Option<PathBuf>,

    /// The location of other certificates to accept
    pub trusted_ca_certs_path: Option<PathBuf>,

    /// The address on which the http server will listen.
    pub address: String,

    /// Base domain under which server is hosted.
    /// This information is used by OIDC to refer redirects
    pub domain_address: Option<Url>,

    /// The local staging path is used as a temporary landing point
    /// for incoming events
    pub local_staging_path: PathBuf,

    /// Username for the basic authentication on the server
    pub username: String,

    /// Password for the basic authentication on the server
    pub password: String,

    /// OpenId configuration
    pub openid: Option<oidc::OpenidConfig>,

    /// Server should check for update or not
    pub check_update: bool,

    /// Server should send anonymous analytics or not
    pub send_analytics: bool,

    /// Open AI access key
    pub open_ai_key: Option<String>,

    /// Livetail port
    pub grpc_port: u16,

    /// Livetail channel capacity
    pub livetail_channel_capacity: usize,

    /// Rows in Parquet Rowgroup
    pub row_group_size: usize,

    /// Query memory limit in bytes
    pub query_memory_pool_size: Option<usize>,

    /// Parquet compression algorithm
    pub parquet_compression: Compression,

    /// Mode of operation
    pub mode: Mode,

    /// public address for the parseable server ingestor
    pub ingestor_endpoint: String,

    /// port use by airplane(flight query service)
    pub flight_port: u16,

    /// CORS behaviour
    pub cors: bool,

    /// The local hot_tier path is used for optimising the query performance in the distributed systems
    pub hot_tier_storage_path: Option<PathBuf>,

    ///maximum disk usage allowed
    pub max_disk_usage: f64,

    pub ms_clarity_tag: Option<String>,

    // Trino vars
    pub trino_endpoint: Option<String>,
    pub trino_username: Option<String>,
    pub trino_auth: Option<String>,
    pub trino_schema: Option<String>,
    pub trino_catalog: Option<String>,

    // Kafka specific env vars
    pub kafka_topics: Option<String>,
    pub kafka_host: Option<String>,
    pub kafka_group: Option<String>,
    pub kafka_client_id: Option<String>,
    pub kafka_security_protocol: Option<SslProtocol>,
    pub kafka_partitions: Option<String>,
}

impl Cli {
    // identifiers for arguments
    pub const TLS_CERT: &'static str = "tls-cert-path";
    pub const TLS_KEY: &'static str = "tls-key-path";
    pub const TRUSTED_CA_CERTS_PATH: &'static str = "trusted-ca-certs-path";
    pub const ADDRESS: &'static str = "address";
    pub const DOMAIN_URI: &'static str = "origin";
    pub const STAGING: &'static str = "local-staging-path";
    pub const USERNAME: &'static str = "username";
    pub const PASSWORD: &'static str = "password";
    pub const CHECK_UPDATE: &'static str = "check-update";
    pub const SEND_ANALYTICS: &'static str = "send-analytics";
    pub const OPEN_AI_KEY: &'static str = "open-ai-key";
    pub const OPENID_CLIENT_ID: &'static str = "oidc-client";
    pub const OPENID_CLIENT_SECRET: &'static str = "oidc-client-secret";
    pub const OPENID_ISSUER: &'static str = "oidc-issuer";
    pub const GRPC_PORT: &'static str = "grpc-port";
    pub const LIVETAIL_CAPACITY: &'static str = "livetail-capacity";
    // todo : what should this flag be
    pub const QUERY_MEM_POOL_SIZE: &'static str = "query-mempool-size";
    pub const ROW_GROUP_SIZE: &'static str = "row-group-size";
    pub const PARQUET_COMPRESSION_ALGO: &'static str = "compression-algo";
    pub const MODE: &'static str = "mode";
    pub const INGESTOR_ENDPOINT: &'static str = "ingestor-endpoint";
    pub const DEFAULT_USERNAME: &'static str = "admin";
    pub const DEFAULT_PASSWORD: &'static str = "admin";
    pub const FLIGHT_PORT: &'static str = "flight-port";
    pub const CORS: &'static str = "cors";
    pub const HOT_TIER_PATH: &'static str = "hot-tier-path";
    pub const MAX_DISK_USAGE: &'static str = "max-disk-usage";
    pub const MS_CLARITY_TAG: &'static str = "ms-clarity-tag";

    // Trino specific env vars
    pub const TRINO_ENDPOINT: &'static str = "p-trino-end-point";
    pub const TRINO_CATALOG_NAME: &'static str = "p-trino-catalog-name";
    pub const TRINO_USER_NAME: &'static str = "p-trino-user-name";
    pub const TRINO_AUTHORIZATION: &'static str = "p-trino-authorization";
    pub const TRINO_SCHEMA: &'static str = "p-trino-schema";

    // Kafka specific env vars
    pub const KAFKA_TOPICS: &'static str = "kafka-topics";
    pub const KAFKA_HOST: &'static str = "kafka-host";
    pub const KAFKA_GROUP: &'static str = "kafka-group";
    pub const KAFKA_CLIENT_ID: &'static str = "kafka-client-id";
    pub const KAFKA_SECURITY_PROTOCOL: &'static str = "kafka-security-protocol";
    pub const KAFKA_PARTITIONS: &'static str = "kafka-partitions";

    pub fn local_stream_data_path(&self, stream_name: &str) -> PathBuf {
        self.local_staging_path.join(stream_name)
    }

    pub fn get_scheme(&self) -> String {
        if self.tls_cert_path.is_some() && self.tls_key_path.is_some() {
            return "https".to_string();
        }
        "http".to_string()
    }

    pub fn create_cli_command_with_clap(name: &'static str) -> Command {
        Command::new(name).next_line_help(false)
        .arg(
                 Arg::new(Self::KAFKA_TOPICS)
                     .long(Self::KAFKA_TOPICS)
                     .env("P_KAFKA_TOPICS")
                     .value_name("STRING")
                     .help("Kafka topics to subscribe to"),
             )
             .arg(
                Arg::new(Self::KAFKA_HOST)
                    .long(Self::KAFKA_HOST)
                    .env("P_KAFKA_HOST")
                    .value_name("STRING")
                    .help("Address and port for Kafka server"),
            )
            .arg(
                Arg::new(Self::KAFKA_GROUP)
                    .long(Self::KAFKA_GROUP)
                    .env("P_KAFKA_GROUP")
                    .value_name("STRING")
                    .help("Kafka group"),
            )
            .arg(
                Arg::new(Self::KAFKA_CLIENT_ID)
                    .long(Self::KAFKA_CLIENT_ID)
                    .env("P_KAFKA_CLIENT_ID")
                    .value_name("STRING")
                    .help("Kafka client id"),
            )
            .arg(
                Arg::new(Self::KAFKA_SECURITY_PROTOCOL)
                    .long(Self::KAFKA_SECURITY_PROTOCOL)
                    .env("P_KAFKA_SECURITY_PROTOCOL")
                    .value_name("STRING")
                    .help("Kafka security protocol"),
            )
            .arg(
                Arg::new(Self::KAFKA_PARTITIONS)
                    .long(Self::KAFKA_PARTITIONS)
                    .env("P_KAFKA_PARTITIONS")
                    .value_name("STRING")
                    .help("Kafka partitions"),
            )
             .arg(
                 Arg::new(Self::TRINO_ENDPOINT)
                     .long(Self::TRINO_ENDPOINT)
                     .env("P_TRINO_ENDPOINT")
                     .value_name("STRING")
                     .help("Address and port for Trino HTTP(s) server"),
             )
             .arg(
                 Arg::new(Self::TRINO_CATALOG_NAME)
                     .long(Self::TRINO_CATALOG_NAME)
                     .env("P_TRINO_CATALOG_NAME")
                     .value_name("STRING")
                     .help("Name of the catalog to be queried (Translates to X-Trino-Catalog)"),
             )
             .arg(
                 Arg::new(Self::TRINO_SCHEMA)
                     .long(Self::TRINO_SCHEMA)
                     .env("P_TRINO_SCHEMA")
                     .value_name("STRING")
                     .help("Name of schema to be queried (Translates to X-Trino-Schema)"),
             )
             .arg(
                 Arg::new(Self::TRINO_USER_NAME)
                     .long(Self::TRINO_USER_NAME)
                     .env("P_TRINO_USER_NAME")
                     .value_name("STRING")
                     .help("Name of Trino user (Translates to X-Trino-User)"),
             )
             .arg(
                 Arg::new(Self::TRINO_AUTHORIZATION)
                     .long(Self::TRINO_AUTHORIZATION)
                     .env("P_TRINO_AUTHORIZATION")
                     .value_name("STRING")
                     .help("Base 64 encoded in the format username:password"),
             )
             .arg(
                 Arg::new(Self::TLS_CERT)
                     .long(Self::TLS_CERT)
                     .env("P_TLS_CERT_PATH")
                     .value_name("PATH")
                     .value_parser(validation::file_path)
                     .help("Local path on this device where certificate file is located. Required to enable TLS"),
             )
             .arg(
                 Arg::new(Self::TLS_KEY)
                     .long(Self::TLS_KEY)
                     .env("P_TLS_KEY_PATH")
                     .value_name("PATH")
                     .value_parser(validation::file_path)
                     .help("Local path on this device where private key file is located. Required to enable TLS"),
             )
            .arg(
                Arg::new(Self::TRUSTED_CA_CERTS_PATH)
                    .long(Self::TRUSTED_CA_CERTS_PATH)
                    .env("P_TRUSTED_CA_CERTS_DIR")
                    .value_name("DIR")
                    .value_parser(validation::canonicalize_path)
                    .help("Local path on this device where all trusted certificates are located.")
            )
             .arg(
                 Arg::new(Self::ADDRESS)
                     .long(Self::ADDRESS)
                     .env("P_ADDR")
                     .value_name("ADDR:PORT")
                     .default_value("0.0.0.0:8000")
                     .value_parser(validation::socket_addr)
                     .help("Address and port for Parseable HTTP(s) server"),
             )
             .arg(
                 Arg::new(Self::STAGING)
                     .long(Self::STAGING)
                     .env("P_STAGING_DIR")
                     .value_name("DIR")
                     .default_value("./staging")
                     .value_parser(validation::canonicalize_path)
                     .help("Local path on this device to be used as landing point for incoming events")
                     .next_line_help(true),
             )
            .arg(
                 Arg::new(Self::USERNAME)
                     .long(Self::USERNAME)
                     .env("P_USERNAME")
                     .value_name("STRING")
                     .required(true)
                     .help("Admin username to be set for this Parseable server"),
             )
             .arg(
                 Arg::new(Self::PASSWORD)
                     .long(Self::PASSWORD)
                     .env("P_PASSWORD")
                     .value_name("STRING")
                     .required(true)
                     .help("Admin password to be set for this Parseable server"),
             )
             .arg(
                 Arg::new(Self::CHECK_UPDATE)
                     .long(Self::CHECK_UPDATE)
                     .env("P_CHECK_UPDATE")
                     .value_name("BOOL")
                     .required(false)
                     .default_value("true")
                     .value_parser(value_parser!(bool))
                     .help("Enable/Disable checking for new Parseable release"),
             )
             .arg(
                 Arg::new(Self::SEND_ANALYTICS)
                     .long(Self::SEND_ANALYTICS)
                     .env("P_SEND_ANONYMOUS_USAGE_DATA")
                     .value_name("BOOL")
                     .required(false)
                     .default_value("true")
                     .value_parser(value_parser!(bool))
                     .help("Enable/Disable anonymous telemetry data collection"),
             )
             .arg(
                 Arg::new(Self::OPEN_AI_KEY)
                     .long(Self::OPEN_AI_KEY)
                     .env("P_OPENAI_API_KEY")
                     .value_name("STRING")
                     .required(false)
                     .help("OpenAI key to enable llm features"),
             )
             .arg(
                 Arg::new(Self::OPENID_CLIENT_ID)
                     .long(Self::OPENID_CLIENT_ID)
                     .env("P_OIDC_CLIENT_ID")
                     .value_name("STRING")
                     .required(false)
                     .help("Client id for OIDC provider"),
             )
             .arg(
                 Arg::new(Self::OPENID_CLIENT_SECRET)
                     .long(Self::OPENID_CLIENT_SECRET)
                     .env("P_OIDC_CLIENT_SECRET")
                     .value_name("STRING")
                     .required(false)
                     .help("Client secret for OIDC provider"),
             )
             .arg(
                 Arg::new(Self::OPENID_ISSUER)
                     .long(Self::OPENID_ISSUER)
                     .env("P_OIDC_ISSUER")
                     .value_name("URL")
                     .required(false)
                     .value_parser(validation::url)
                     .help("OIDC provider's host address"),
             )
             .arg(
                 Arg::new(Self::DOMAIN_URI)
                     .long(Self::DOMAIN_URI)
                     .env("P_ORIGIN_URI")
                     .value_name("URL")
                     .required(false)
                     .value_parser(validation::url)
                     .help("Parseable server global domain address"),
             )
             .arg(
                 Arg::new(Self::GRPC_PORT)
                     .long(Self::GRPC_PORT)
                     .env("P_GRPC_PORT")
                     .value_name("PORT")
                     .default_value("8001")
                     .required(false)
                     .value_parser(value_parser!(u16))
                     .help("Port for gRPC server"),
             )
             .arg(
                 Arg::new(Self::FLIGHT_PORT)
                     .long(Self::FLIGHT_PORT)
                     .env("P_FLIGHT_PORT")
                     .value_name("PORT")
                     .default_value("8002")
                     .required(false)
                     .value_parser(value_parser!(u16))
                     .help("Port for Arrow Flight Querying Engine"),
             )
             .arg(
                 Arg::new(Self::CORS)
                 .long(Self::CORS)
                 .env("P_CORS")
                 .value_name("BOOL")
                 .required(false)
                 .default_value("true")
                 .value_parser(value_parser!(bool))
                 .help("Enable/Disable CORS, default disabled"),
             )
             .arg(
                 Arg::new(Self::LIVETAIL_CAPACITY)
                     .long(Self::LIVETAIL_CAPACITY)
                     .env("P_LIVETAIL_CAPACITY")
                     .value_name("NUMBER")
                     .default_value("1000")
                     .required(false)
                     .value_parser(value_parser!(usize))
                     .help("Number of rows in livetail channel"),
             )
             .arg(
                 Arg::new(Self::QUERY_MEM_POOL_SIZE)
                     .long(Self::QUERY_MEM_POOL_SIZE)
                     .env("P_QUERY_MEMORY_LIMIT")
                     .value_name("Gib")
                     .required(false)
                     .value_parser(value_parser!(u8))
                     .help("Set a fixed memory limit for query"),
             )
             .arg(
                 // RowGroupSize controls the number of rows present in one row group
                 // More rows = better compression but HIGHER Memory consumption during read/write
                 // 1048576 is the default value for DataFusion
                 Arg::new(Self::ROW_GROUP_SIZE)
                     .long(Self::ROW_GROUP_SIZE)
                     .env("P_PARQUET_ROW_GROUP_SIZE")
                     .value_name("NUMBER")
                     .required(false)
                     .default_value("1048576")
                     .value_parser(value_parser!(usize))
                     .help("Number of rows in a row group"),
             ).arg(
                 Arg::new(Self::MODE)
                     .long(Self::MODE)
                     .env("P_MODE")
                     .value_name("STRING")
                     .required(false)
                     .default_value("all")
                     .value_parser([
                         "query",
                         "ingest",
                         "all"])
                     .help("Mode of operation"),
             )
             .arg(
                 Arg::new(Self::INGESTOR_ENDPOINT)
                     .long(Self::INGESTOR_ENDPOINT)
                     .env("P_INGESTOR_ENDPOINT")
                     .value_name("URL")
                     .required(false)
                     .help("URL to connect to this specific ingestor. Default is the address of the server.")
             )
             .arg(
                 Arg::new(Self::PARQUET_COMPRESSION_ALGO)
                     .long(Self::PARQUET_COMPRESSION_ALGO)
                     .env("P_PARQUET_COMPRESSION_ALGO")
                     .value_name("[UNCOMPRESSED, SNAPPY, GZIP, LZO, BROTLI, LZ4, ZSTD]")
                     .required(false)
                     .default_value("lz4")
                     .value_parser([
                         "uncompressed",
                         "snappy",
                         "gzip",
                         "lzo",
                         "brotli",
                         "lz4",
                         "zstd"])
                     .help("Parquet compression algorithm"),
             )
             .arg(
                 Arg::new(Self::HOT_TIER_PATH)
                     .long(Self::HOT_TIER_PATH)
                     .env("P_HOT_TIER_DIR")
                     .value_name("DIR")
                     .value_parser(validation::canonicalize_path)
                     .help("Local path on this device to be used for hot tier data")
                     .next_line_help(true),
             )
             .arg(
                 Arg::new(Self::MAX_DISK_USAGE)
                     .long(Self::MAX_DISK_USAGE)
                     .env("P_MAX_DISK_USAGE_PERCENT")
                     .value_name("percentage")
                     .default_value("80.0")
                     .value_parser(validation::validate_disk_usage)
                     .help("Maximum allowed disk usage in percentage e.g 90.0 for 90%")
                     .next_line_help(true),
             )
             .arg(
                 Arg::new(Self::MS_CLARITY_TAG)
                     .long(Self::MS_CLARITY_TAG)
                     .env("P_MS_CLARITY_TAG")
                     .value_name("STRING")
                     .required(false)
                     .help("Tag for MS Clarity"),
             )
             .group(
                 ArgGroup::new("oidc")
                     .args([Self::OPENID_CLIENT_ID, Self::OPENID_CLIENT_SECRET, Self::OPENID_ISSUER])
                     .requires_all([Self::OPENID_CLIENT_ID, Self::OPENID_CLIENT_SECRET, Self::OPENID_ISSUER])
                     .multiple(true)
         )
    }
}

impl FromArgMatches for Cli {
    fn from_arg_matches(m: &clap::ArgMatches) -> Result<Self, clap::Error> {
        let mut s: Self = Self::default();
        s.update_from_arg_matches(m)?;
        Ok(s)
    }

    fn update_from_arg_matches(&mut self, m: &clap::ArgMatches) -> Result<(), clap::Error> {
        self.trino_catalog = m.get_one::<String>(Self::TRINO_CATALOG_NAME).cloned();
        self.trino_endpoint = m.get_one::<String>(Self::TRINO_ENDPOINT).cloned();
        self.trino_auth = m.get_one::<String>(Self::TRINO_AUTHORIZATION).cloned();
        self.trino_schema = m.get_one::<String>(Self::TRINO_SCHEMA).cloned();
        self.trino_username = m.get_one::<String>(Self::TRINO_USER_NAME).cloned();

        self.kafka_topics = m.get_one::<String>(Self::KAFKA_TOPICS).cloned();
        self.kafka_host = m.get_one::<String>(Self::KAFKA_HOST).cloned();
        self.kafka_group = m.get_one::<String>(Self::KAFKA_GROUP).cloned();
        self.kafka_client_id = m.get_one::<String>(Self::KAFKA_CLIENT_ID).cloned();
        self.kafka_security_protocol = m
            .get_one::<SslProtocol>(Self::KAFKA_SECURITY_PROTOCOL)
            .cloned();
        self.kafka_partitions = m.get_one::<String>(Self::KAFKA_PARTITIONS).cloned();

        self.tls_cert_path = m.get_one::<PathBuf>(Self::TLS_CERT).cloned();
        self.tls_key_path = m.get_one::<PathBuf>(Self::TLS_KEY).cloned();
        self.trusted_ca_certs_path = m.get_one::<PathBuf>(Self::TRUSTED_CA_CERTS_PATH).cloned();
        self.domain_address = m.get_one::<Url>(Self::DOMAIN_URI).cloned();

        self.address = m
            .get_one::<String>(Self::ADDRESS)
            .cloned()
            .expect("default value for address");

        self.ingestor_endpoint = m
            .get_one::<String>(Self::INGESTOR_ENDPOINT)
            .cloned()
            .unwrap_or_else(String::default);

        self.local_staging_path = m
            .get_one::<PathBuf>(Self::STAGING)
            .cloned()
            .expect("default value for staging");
        self.username = m
            .get_one::<String>(Self::USERNAME)
            .cloned()
            .expect("default for username");
        self.password = m
            .get_one::<String>(Self::PASSWORD)
            .cloned()
            .expect("default for password");
        self.check_update = m
            .get_one::<bool>(Self::CHECK_UPDATE)
            .cloned()
            .expect("default for check update");
        self.send_analytics = m
            .get_one::<bool>(Self::SEND_ANALYTICS)
            .cloned()
            .expect("default for send analytics");
        self.open_ai_key = m.get_one::<String>(Self::OPEN_AI_KEY).cloned();
        self.grpc_port = m
            .get_one::<u16>(Self::GRPC_PORT)
            .cloned()
            .expect("default for livetail port");
        self.flight_port = m
            .get_one::<u16>(Self::FLIGHT_PORT)
            .cloned()
            .expect("default for flight port");
        self.cors = m
            .get_one::<bool>(Self::CORS)
            .cloned()
            .expect("default for CORS");
        self.livetail_channel_capacity = m
            .get_one::<usize>(Self::LIVETAIL_CAPACITY)
            .cloned()
            .expect("default for livetail capacity");
        // converts Gib to bytes before assigning
        self.query_memory_pool_size = m
            .get_one::<u8>(Self::QUERY_MEM_POOL_SIZE)
            .cloned()
            .map(|gib| gib as usize * 1024usize.pow(3));
        self.row_group_size = m
            .get_one::<usize>(Self::ROW_GROUP_SIZE)
            .cloned()
            .expect("default for row_group size");
        self.parquet_compression = serde_json::from_str(&format!(
            "{:?}",
            m.get_one::<String>(Self::PARQUET_COMPRESSION_ALGO)
                .expect("default for compression algo")
        ))
        .expect("unexpected compression algo");

        let openid_client_id = m.get_one::<String>(Self::OPENID_CLIENT_ID).cloned();
        let openid_client_secret = m.get_one::<String>(Self::OPENID_CLIENT_SECRET).cloned();
        let openid_issuer = m.get_one::<Url>(Self::OPENID_ISSUER).cloned();

        self.openid = match (openid_client_id, openid_client_secret, openid_issuer) {
            (Some(id), Some(secret), Some(issuer)) => {
                let origin = if let Some(url) = self.domain_address.clone() {
                    oidc::Origin::Production(url)
                } else {
                    oidc::Origin::Local {
                        socket_addr: self.address.clone(),
                        https: self.tls_cert_path.is_some() && self.tls_key_path.is_some(),
                    }
                };
                Some(OpenidConfig {
                    id,
                    secret,
                    issuer,
                    origin,
                })
            }
            _ => None,
        };

        self.mode = match m
            .get_one::<String>(Self::MODE)
            .expect("Mode not set")
            .as_str()
        {
            "query" => Mode::Query,
            "ingest" => Mode::Ingest,
            "all" => Mode::All,
            _ => unreachable!(),
        };

        self.hot_tier_storage_path = m.get_one::<PathBuf>(Self::HOT_TIER_PATH).cloned();
        self.max_disk_usage = m
            .get_one::<f64>(Self::MAX_DISK_USAGE)
            .cloned()
            .expect("default for max disk usage");

        self.ms_clarity_tag = m.get_one::<String>(Self::MS_CLARITY_TAG).cloned();

        Ok(())
    }
}
