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

use clap::error::ErrorKind;
use clap::{command, value_parser, Arg, ArgGroup, Args, Command, FromArgMatches};

use once_cell::sync::Lazy;
use parquet::basic::{BrotliLevel, GzipLevel, ZstdLevel};
use std::env;
use std::path::PathBuf;
use std::sync::Arc;
use url::Url;

use crate::oidc::{self, OpenidConfig};
use crate::storage::{FSConfig, ObjectStorageError, ObjectStorageProvider, S3Config};

pub const MIN_CACHE_SIZE_BYTES: u64 = 1000u64.pow(3); // 1 GiB
pub const JOIN_COMMUNITY: &str =
    "Join us on Parseable Slack community for questions : https://logg.ing/community";
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
                    Ok(storage) => storage,
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

                if server.local_cache_path.is_some() {
                    parseable_cli_command()
                        .error(
                            ErrorKind::ValueValidation,
                            "Cannot use cache with local-store subcommand.",
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
                    Ok(storage) => storage,
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

    pub async fn validate(&self) -> Result<(), ObjectStorageError> {
        let obj_store = self.storage.get_object_store();
        let rel_path = relative_path::RelativePathBuf::from(".parseable.json");

        let has_parseable_json = obj_store.get_object(&rel_path).await.is_ok();

        // Lists all the directories in the root of the bucket/directory
        // can be a stream (if it contains .stream.json file) or not
        let has_dirs = match obj_store.list_dirs().await {
            Ok(dirs) => !dirs.is_empty(),
            Err(_) => false,
        };

        let has_streams = obj_store.list_streams().await.is_ok();

        if has_streams || !has_dirs && !has_parseable_json {
            return Ok(());
        }

        if self.mode_string() == "Local drive" {
            return Err(ObjectStorageError::Custom(format!("Could not start the server because directory '{}' contains stale data, please use an empty directory, and restart the server.\n{}", self.storage.get_endpoint(), JOIN_COMMUNITY)));
        }

        // S3 bucket mode
        Err(ObjectStorageError::Custom(format!("Could not start the server because bucket '{}' contains stale data, please use an empty bucket and restart the server.\n{}", self.storage.get_endpoint(), JOIN_COMMUNITY)))
    }

    pub fn storage(&self) -> Arc<dyn ObjectStorageProvider + Send + Sync> {
        self.storage.clone()
    }

    pub fn staging_dir(&self) -> &PathBuf {
        &self.parseable.local_staging_path
    }

    pub fn cache_size(&self) -> u64 {
        self.parseable.local_cache_size
    }

    pub fn cache_dir(&self) -> &Option<PathBuf> {
        &self.parseable.local_cache_path
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
{about} Join the community at https://logg.ing/community.

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

    /// The local cache path is used for speeding up query on latest data
    pub local_cache_path: Option<PathBuf>,

    /// Size for local cache
    pub local_cache_size: u64,

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
        self.local_cache_path = m.get_one::<PathBuf>(Self::CACHE).cloned();
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
        self.local_cache_size = m
            .get_one::<u64>(Self::CACHE_SIZE)
            .cloned()
            .expect("default value for cache size");
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
    pub const TLS_CERT: &'static str = "tls-cert-path";
    pub const TLS_KEY: &'static str = "tls-key-path";
    pub const ADDRESS: &'static str = "address";
    pub const DOMAIN_URI: &'static str = "origin";
    pub const STAGING: &'static str = "local-staging-path";
    pub const CACHE: &'static str = "cache-path";
    pub const CACHE_SIZE: &'static str = "cache-size";
    pub const UPLOAD_INTERVAL: &'static str = "upload-interval";
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
    pub const DEFAULT_USERNAME: &'static str = "admin";
    pub const DEFAULT_PASSWORD: &'static str = "admin";

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
                Arg::new(Self::CACHE)
                    .long(Self::CACHE)
                    .env("P_CACHE_DIR")
                    .value_name("DIR")
                    .value_parser(validation::canonicalize_path)
                    .help("Local path on this device to be used for caching data")
                    .next_line_help(true),
            )
             .arg(
                Arg::new(Self::CACHE_SIZE)
                    .long(Self::CACHE_SIZE)
                    .env("P_CACHE_SIZE")
                    .value_name("size")
                    .default_value("1GiB")
                    .value_parser(validation::cache_size)
                    .help("Maximum allowed cache size for all streams combined (In human readable format, e.g 1GiB, 2GiB, 100MB)")
                    .next_line_help(true),
            )
            .arg(
                Arg::new(Self::UPLOAD_INTERVAL)
                    .long(Self::UPLOAD_INTERVAL)
                    .env("P_STORAGE_UPLOAD_INTERVAL")
                    .value_name("SECONDS")
                    .default_value("60")
                    .value_parser(validation::upload_interval)
                    .help("Interval in seconds after which staging data would be sent to the storage")
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
                    .value_name("URl")
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
                Arg::new(Self::ROW_GROUP_SIZE)
                    .long(Self::ROW_GROUP_SIZE)
                    .env("P_PARQUET_ROW_GROUP_SIZE")
                    .value_name("NUMBER")
                    .required(false)
                    .default_value("16384")
                    .value_parser(value_parser!(usize))
                    .help("Number of rows in a row group"),
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
        env, io,
        net::ToSocketAddrs,
        path::{Path, PathBuf},
        str::FromStr,
    };

    use path_clean::PathClean;

    use crate::option::MIN_CACHE_SIZE_BYTES;
    use crate::storage::LOCAL_SYNC_INTERVAL;
    use human_size::{multiples, SpecificSize};

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
    pub fn absolute_path(path: impl AsRef<Path>) -> io::Result<PathBuf> {
        let path = path.as_ref();

        let absolute_path = if path.is_absolute() {
            path.to_path_buf()
        } else {
            env::current_dir()?.join(path)
        }
        .clean();

        Ok(absolute_path)
    }

    pub fn canonicalize_path(s: &str) -> Result<PathBuf, String> {
        let path = PathBuf::from(s);
        Ok(absolute_path(path).unwrap())
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

    fn human_size_to_bytes(s: &str) -> Result<u64, String> {
        fn parse_and_map<T: human_size::Multiple>(
            s: &str,
        ) -> Result<u64, human_size::ParsingError> {
            SpecificSize::<T>::from_str(s).map(|x| x.to_bytes())
        }

        let size = parse_and_map::<multiples::Mebibyte>(s)
            .or(parse_and_map::<multiples::Megabyte>(s))
            .or(parse_and_map::<multiples::Gigibyte>(s))
            .or(parse_and_map::<multiples::Gigabyte>(s))
            .or(parse_and_map::<multiples::Tebibyte>(s))
            .or(parse_and_map::<multiples::Terabyte>(s))
            .map_err(|_| "Could not parse given size".to_string())?;

        if size < MIN_CACHE_SIZE_BYTES {
            return Err(
                "Specified value of cache size is smaller than current minimum of 1GiB".to_string(),
            );
        }

        Ok(size)
    }

    pub fn cache_size(s: &str) -> Result<u64, String> {
        let size = human_size_to_bytes(s)?;
        if size < MIN_CACHE_SIZE_BYTES {
            return Err(
                "Specified value of cache size is smaller than current minimum of 1GiB".to_string(),
            );
        }
        Ok(size)
    }

    pub fn upload_interval(s: &str) -> Result<u64, String> {
        let u = s
            .parse::<u64>()
            .map_err(|_| "invalid upload interval".to_string())?;
        if u < LOCAL_SYNC_INTERVAL {
            return Err("object storage upload interval must be 60 seconds or more".to_string());
        }
        Ok(u)
    }
}
