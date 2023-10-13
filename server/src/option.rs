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

use clap::error::ErrorKind;
use clap::{command, value_parser, Arg, ArgGroup, Args, Command, FromArgMatches};

use once_cell::sync::Lazy;
use parquet::basic::{BrotliLevel, GzipLevel, ZstdLevel};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use url::Url;

use crate::oidc::{self, OpenidConfig};
use crate::storage::{FSConfig, ObjectStorageProvider, S3Config, LOCAL_SYNC_INTERVAL};
use crate::utils::validate_path_is_writeable;

pub static CONFIG: Lazy<Arc<Config>> = Lazy::new(|| Arc::new(Config::new()));

#[derive(Debug)]
pub struct Config {
    pub parseable: Server,
    storage: Arc<dyn ObjectStorageProvider + Send + Sync>,
    pub storage_name: &'static str,
}

impl Config {
    fn new() -> Self {
        let cli = parseable_cli_command().get_matches();

        match cli.subcommand() {
            Some(("local-store", m)) => {
                let server = match Server::from_arg_matches(m) {
                    Ok(server) => server,
                    Err(err) => err.exit(),
                };
                let storage = match FSConfig::from_arg_matches(m) {
                    Ok(server) => server,
                    Err(err) => err.exit(),
                };

                if server.local_staging_path == storage.root {
                    parseable_cli_command()
                        .error(
                            ErrorKind::ValueValidation,
                            "Cannot use same path for storage and staging",
                        )
                        .exit()
                }

                Config {
                    parseable: server,
                    storage: Arc::new(storage),
                    storage_name: "drive",
                }
            }
            Some(("s3-store", m)) => {
                let server = match Server::from_arg_matches(m) {
                    Ok(server) => server,
                    Err(err) => err.exit(),
                };
                let storage = match S3Config::from_arg_matches(m) {
                    Ok(server) => server,
                    Err(err) => err.exit(),
                };

                Config {
                    parseable: server,
                    storage: Arc::new(storage),
                    storage_name: "s3",
                }
            }
            _ => unreachable!(),
        }
    }

    pub fn validate(&self) {
        if CONFIG.parseable.upload_interval < LOCAL_SYNC_INTERVAL {
            panic!("object storage upload_interval (P_STORAGE_UPLOAD_INTERVAL) must be 60 seconds or more");
        }
    }

    pub fn validate_staging(&self) -> anyhow::Result<()> {
        let staging_path = self.staging_dir();
        validate_path_is_writeable(staging_path)
    }

    pub fn storage(&self) -> Arc<dyn ObjectStorageProvider + Send + Sync> {
        self.storage.clone()
    }

    pub fn staging_dir(&self) -> &Path {
        &self.parseable.local_staging_path
    }

    pub fn is_default_creds(&self) -> bool {
        self.parseable.username == Server::DEFAULT_USERNAME
            && self.parseable.password == Server::DEFAULT_PASSWORD
    }

    // returns the string representation of the storage mode
    // drive --> Local drive
    // s3 --> S3 bucket
    pub fn mode_string(&self) -> &str {
        let mut mode = "S3 bucket";
        if self.storage_name == "drive" {
            mode = "Local drive";
        }
        mode
    }
}

fn parseable_cli_command() -> Command {
    let local = Server::get_clap_command("local-store");
    let local = <FSConfig as Args>::augment_args_for_update(local);

    let local = local
        .mut_arg(Server::USERNAME, |arg| {
            arg.required(false).default_value(Server::DEFAULT_USERNAME)
        })
        .mut_arg(Server::PASSWORD, |arg| {
            arg.required(false).default_value(Server::DEFAULT_PASSWORD)
        });
    let s3 = Server::get_clap_command("s3-store");
    let s3 = <S3Config as Args>::augment_args_for_update(s3);

    command!()
        .name("Parseable")
        .bin_name("parseable")
        .about("Parseable is a log storage and observability platform.")
        .propagate_version(true)
        .next_line_help(false)
        .help_template(
            r#"
{about} Join the community at https://launchpass.com/parseable.

{all-args}
        "#,
        )
        .subcommand_required(true)
        .subcommands([local, s3])
}

#[derive(Debug, Default)]
pub struct Server {
    /// The location of TLS Cert file
    pub tls_cert_path: Option<PathBuf>,

    /// The location of TLS Private Key file
    pub tls_key_path: Option<PathBuf>,

    /// The address on which the http server will listen.
    pub address: String,

    /// Base domain under which server is hosted.
    /// This information is used by OIDC to refer redirects
    pub domain_address: Option<Url>,

    /// The local staging path is used as a temporary landing point
    /// for incoming events and local cache
    pub local_staging_path: PathBuf,

    /// Interval in seconds after which uncommited data would be
    /// uploaded to the storage platform.
    pub upload_interval: u64,

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
}

impl FromArgMatches for Server {
    fn from_arg_matches(m: &clap::ArgMatches) -> Result<Self, clap::Error> {
        let mut s: Self = Self::default();
        s.update_from_arg_matches(m)?;
        Ok(s)
    }

    fn update_from_arg_matches(&mut self, m: &clap::ArgMatches) -> Result<(), clap::Error> {
        self.tls_cert_path = m.get_one::<PathBuf>(Self::TLS_CERT).cloned();
        self.tls_key_path = m.get_one::<PathBuf>(Self::TLS_KEY).cloned();
        self.domain_address = m.get_one::<Url>(Self::DOMAIN_URI).cloned();
        let openid_client_id = m.get_one::<String>(Self::OPENID_CLIENT_ID).cloned();
        let openid_client_secret = m.get_one::<String>(Self::OPENID_CLIENT_SECRET).cloned();
        let openid_issuer = m.get_one::<Url>(Self::OPENID_ISSUER).cloned();

        self.address = m
            .get_one::<String>(Self::ADDRESS)
            .cloned()
            .expect("default value for address");
        self.local_staging_path = m
            .get_one::<PathBuf>(Self::STAGING)
            .cloned()
            .expect("default value for staging");
        self.upload_interval = m
            .get_one::<u64>(Self::UPLOAD_INTERVAL)
            .cloned()
            .expect("default value for upload");
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
        self.livetail_channel_capacity = m
            .get_one::<usize>(Self::LIVETAIL_CAPACITY)
            .cloned()
            .expect("default for livetail port");
        // converts Gib to bytes before assigning
        self.query_memory_pool_size = m
            .get_one::<u8>(Self::QUERY_MEM_POOL_SIZE)
            .cloned()
            .map(|gib| gib as usize * 1024usize.pow(3));
        self.row_group_size = m
            .get_one::<usize>(Self::ROW_GROUP_SIZE)
            .cloned()
            .expect("default for row_group size");
        self.parquet_compression = match m
            .get_one::<String>(Self::PARQUET_COMPRESSION_ALGO)
            .expect("default for compression algo")
            .as_str()
        {
            "uncompressed" => Compression::UNCOMPRESSED,
            "snappy" => Compression::SNAPPY,
            "gzip" => Compression::GZIP,
            "lzo" => Compression::LZO,
            "brotli" => Compression::BROTLI,
            "lz4" => Compression::LZ4,
            "zstd" => Compression::ZSTD,
            _ => unreachable!(),
        };

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

        Ok(())
    }
}

impl Server {
    // identifiers for arguments
    pub const TLS_CERT: &str = "tls-cert-path";
    pub const TLS_KEY: &str = "tls-key-path";
    pub const ADDRESS: &str = "address";
    pub const DOMAIN_URI: &str = "origin";
    pub const STAGING: &str = "local-staging-path";
    pub const UPLOAD_INTERVAL: &str = "upload-interval";
    pub const USERNAME: &str = "username";
    pub const PASSWORD: &str = "password";
    pub const CHECK_UPDATE: &str = "check-update";
    pub const SEND_ANALYTICS: &str = "send-analytics";
    pub const OPEN_AI_KEY: &str = "open-ai-key";
    pub const OPENID_CLIENT_ID: &str = "oidc-client";
    pub const OPENID_CLIENT_SECRET: &str = "oidc-client-secret";
    pub const OPENID_ISSUER: &str = "oidc-issuer";
    pub const GRPC_PORT: &str = "grpc-port";
    pub const LIVETAIL_CAPACITY: &str = "livetail-capacity";
    // todo : what should this flag be
    pub const QUERY_MEM_POOL_SIZE: &str = "query-mempool-size";
    pub const ROW_GROUP_SIZE: &str = "row-group-size";
    pub const PARQUET_COMPRESSION_ALGO: &str = "compression-algo";
    pub const DEFAULT_USERNAME: &str = "admin";
    pub const DEFAULT_PASSWORD: &str = "admin";

    pub fn local_stream_data_path(&self, stream_name: &str) -> PathBuf {
        self.local_staging_path.join(stream_name)
    }

    pub fn get_scheme(&self) -> String {
        if self.tls_cert_path.is_some() && self.tls_key_path.is_some() {
            return "https".to_string();
        }
        "http".to_string()
    }

    pub fn get_clap_command(name: &'static str) -> Command {
        Command::new(name).next_line_help(false)
            .arg(
                Arg::new(Self::TLS_CERT)
                    .long(Self::TLS_CERT)
                    .env("P_TLS_CERT_PATH")
                    .value_name("PATH")
                    .value_parser(validation::file_path)
                    .help("The location of TLS Cert file"),
            )
            .arg(
                Arg::new(Self::TLS_KEY)
                    .long(Self::TLS_KEY)
                    .env("P_TLS_KEY_PATH")
                    .value_name("PATH")
                    .value_parser(validation::file_path)
                    .help("The location of TLS Private Key file"),
            )
            .arg(
                Arg::new(Self::ADDRESS)
                    .long(Self::ADDRESS)
                    .env("P_ADDR")
                    .value_name("ADDR:PORT")
                    .default_value("0.0.0.0:8000")
                    .value_parser(validation::socket_addr)
                    .help("The address on which the http server will listen."),
            )
            .arg(
                Arg::new(Self::STAGING)
                    .long(Self::STAGING)
                    .env("P_STAGING_DIR")
                    .value_name("DIR")
                    .default_value("./staging")
                    .value_parser(validation::canonicalize_path)
                    .help("The local staging path is used as a temporary landing point for incoming events and local cache")
                    .next_line_help(true),
            )
            .arg(
                Arg::new(Self::UPLOAD_INTERVAL)
                    .long(Self::UPLOAD_INTERVAL)
                    .env("P_STORAGE_UPLOAD_INTERVAL")
                    .value_name("SECONDS")
                    .default_value("60")
                    .value_parser(value_parser!(u64))
                    .help("Interval in seconds after which un-committed data would be sent to the storage")
                    .next_line_help(true),
            )
            .arg(
                Arg::new(Self::USERNAME)
                    .long(Self::USERNAME)
                    .env("P_USERNAME")
                    .value_name("STRING")
                    .required(true)
                    .help("Username for the basic authentication on the server"),
            )
            .arg(
                Arg::new(Self::PASSWORD)
                    .long(Self::PASSWORD)
                    .env("P_PASSWORD")
                    .value_name("STRING")
                    .required(true)
                    .help("Password for the basic authentication on the server"),
            )
            .arg(
                Arg::new(Self::CHECK_UPDATE)
                    .long(Self::CHECK_UPDATE)
                    .env("P_CHECK_UPDATE")
                    .value_name("BOOL")
                    .required(false)
                    .default_value("true")
                    .value_parser(value_parser!(bool))
                    .help("Disable/Enable checking for updates"),
            )
            .arg(
                Arg::new(Self::SEND_ANALYTICS)
                    .long(Self::SEND_ANALYTICS)
                    .env("P_SEND_ANONYMOUS_USAGE_DATA")
                    .value_name("BOOL")
                    .required(false)
                    .default_value("true")
                    .value_parser(value_parser!(bool))
                    .help("Disable/Enable sending anonymous user data"),
            )
            .arg(
                Arg::new(Self::OPEN_AI_KEY)
                    .long(Self::OPEN_AI_KEY)
                    .env("P_OPENAI_API_KEY")
                    .value_name("STRING")
                    .required(false)
                    .help("Set OpenAI key to enable llm feature"),
            )
            .arg(
                Arg::new(Self::OPENID_CLIENT_ID)
                    .long(Self::OPENID_CLIENT_ID)
                    .env("P_OIDC_CLIENT_ID")
                    .value_name("STRING")
                    .required(false)
                    .help("Set client id for oidc provider"),
            )
            .arg(
                Arg::new(Self::OPENID_CLIENT_SECRET)
                    .long(Self::OPENID_CLIENT_SECRET)
                    .env("P_OIDC_CLIENT_SECRET")
                    .value_name("STRING")
                    .required(false)
                    .help("Set client secret for oidc provider"),
            )
            .arg(
                Arg::new(Self::OPENID_ISSUER)
                    .long(Self::OPENID_ISSUER)
                    .env("P_OIDC_ISSUER")
                    .value_name("URl")
                    .required(false)
                    .value_parser(validation::url)
                    .help("Set OIDC provider's host address."),
            )
            .arg(
                Arg::new(Self::DOMAIN_URI)
                    .long(Self::DOMAIN_URI)
                    .env("P_ORIGIN_URI")
                    .value_name("URL")
                    .required(false)
                    .value_parser(validation::url)
                    .help("Set host global domain address"),
            )
            .arg(
                Arg::new(Self::GRPC_PORT)
                    .long(Self::GRPC_PORT)
                    .env("P_GRPC_PORT")
                    .value_name("PORT")
                    .default_value("8001")
                    .required(false)
                    .value_parser(value_parser!(u16))
                    .help("Set port for livetail arrow flight server"),
            )
            .arg(
                Arg::new(Self::LIVETAIL_CAPACITY)
                    .long(Self::LIVETAIL_CAPACITY)
                    .env("P_LIVETAIL_CAPACITY")
                    .value_name("NUMBER")
                    .default_value("1000")
                    .required(false)
                    .value_parser(value_parser!(usize))
                    .help("Set port for livetail arrow flight server"),
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
                Arg::new(Self::ROW_GROUP_SIZE)
                    .long(Self::ROW_GROUP_SIZE)
                    .env("P_PARQUET_ROW_GROUP_SIZE")
                    .value_name("NUMBER")
                    .required(false)
                    .default_value("16384")
                    .value_parser(value_parser!(usize))
                    .help("Number of rows in a row groups"),
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
            ).group(
                ArgGroup::new("oidc")
                    .args([Self::OPENID_CLIENT_ID, Self::OPENID_CLIENT_SECRET, Self::OPENID_ISSUER])
                    .requires_all([Self::OPENID_CLIENT_ID, Self::OPENID_CLIENT_SECRET, Self::OPENID_ISSUER])
                    .multiple(true)
        )
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
#[allow(non_camel_case_types, clippy::upper_case_acronyms)]
pub enum Compression {
    UNCOMPRESSED,
    SNAPPY,
    GZIP,
    LZO,
    BROTLI,
    #[default]
    LZ4,
    ZSTD,
}

impl From<Compression> for parquet::basic::Compression {
    fn from(value: Compression) -> Self {
        match value {
            Compression::UNCOMPRESSED => parquet::basic::Compression::UNCOMPRESSED,
            Compression::SNAPPY => parquet::basic::Compression::SNAPPY,
            Compression::GZIP => parquet::basic::Compression::GZIP(GzipLevel::default()),
            Compression::LZO => parquet::basic::Compression::LZO,
            Compression::BROTLI => parquet::basic::Compression::BROTLI(BrotliLevel::default()),
            Compression::LZ4 => parquet::basic::Compression::LZ4,
            Compression::ZSTD => parquet::basic::Compression::ZSTD(ZstdLevel::default()),
        }
    }
}

pub mod validation {
    use std::{
        fs::{canonicalize, create_dir_all},
        net::ToSocketAddrs,
        path::PathBuf,
    };

    pub fn file_path(s: &str) -> Result<PathBuf, String> {
        if s.is_empty() {
            return Err("empty path".to_owned());
        }

        let path = PathBuf::from(s);

        if !path.is_file() {
            return Err("path specified does not point to an accessible file".to_string());
        }

        Ok(path)
    }

    pub fn canonicalize_path(s: &str) -> Result<PathBuf, String> {
        let path = PathBuf::from(s);

        create_dir_all(&path)
            .map_err(|err| err.to_string())
            .and_then(|_| {
                canonicalize(&path)
                    .map_err(|_| "Cannot use the path provided as an absolute path".to_string())
            })
    }

    pub fn socket_addr(s: &str) -> Result<String, String> {
        s.to_socket_addrs()
            .is_ok()
            .then_some(s.to_string())
            .ok_or_else(|| "Socket Address for server is invalid".to_string())
    }

    pub fn url(s: &str) -> Result<url::Url, String> {
        url::Url::parse(s).map_err(|_| "Invalid URL provided".to_string())
    }
}
