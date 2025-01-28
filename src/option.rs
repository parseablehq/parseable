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

use crate::cli::{Cli, Options, StorageOptions, DEFAULT_PASSWORD, DEFAULT_USERNAME};
use crate::storage::object_storage::parseable_json_path;
use crate::storage::{ObjectStorageError, ObjectStorageProvider};
use bytes::Bytes;
use clap::error::ErrorKind;
use clap::Parser;
use once_cell::sync::Lazy;
use parquet::basic::{BrotliLevel, GzipLevel, ZstdLevel};
use serde::{Deserialize, Serialize};
use std::path::PathBuf;
use std::sync::Arc;

pub const JOIN_COMMUNITY: &str =
    "Join us on Parseable Slack community for questions : https://logg.ing/community";
pub static CONFIG: Lazy<Arc<Config>> = Lazy::new(|| Arc::new(Config::new()));

#[derive(Debug)]
pub struct Config {
    pub options: Options,
    storage: Arc<dyn ObjectStorageProvider>,
    pub storage_name: &'static str,
}

impl Config {
    fn new() -> Self {
        match Cli::parse().storage {
            StorageOptions::Local(args) => {
                if args.options.local_staging_path == args.storage.root {
                    clap::Error::raw(
                        ErrorKind::ValueValidation,
                        "Cannot use same path for storage and staging",
                    )
                    .exit();
                }

                if args.options.hot_tier_storage_path.is_some() {
                    clap::Error::raw(
                        ErrorKind::ValueValidation,
                        "Cannot use hot tier with local-store subcommand.",
                    )
                    .exit();
                }

                Config {
                    options: args.options,
                    storage: Arc::new(args.storage),
                    storage_name: "drive",
                }
            }
            StorageOptions::S3(args) => Config {
                options: args.options,
                storage: Arc::new(args.storage),
                storage_name: "s3",
            },
            StorageOptions::Blob(args) => Config {
                options: args.options,
                storage: Arc::new(args.storage),
                storage_name: "blob_store",
            },
        }
    }

    // validate the storage, if the proper path for staging directory is provided
    // if the proper data directory is provided, or s3 bucket is provided etc
    pub async fn validate_storage(&self) -> Result<Option<Bytes>, ObjectStorageError> {
        let obj_store = self.storage.get_object_store();
        let rel_path = parseable_json_path();
        let mut has_parseable_json = false;
        let parseable_json_result = obj_store.get_object(&rel_path).await;
        if parseable_json_result.is_ok() {
            has_parseable_json = true;
        }

        // Lists all the directories in the root of the bucket/directory
        // can be a stream (if it contains .stream.json file) or not
        let has_dirs = match obj_store.list_dirs().await {
            Ok(dirs) => !dirs.is_empty(),
            Err(_) => false,
        };

        let has_streams = obj_store.list_streams().await.is_ok();
        if !has_dirs && !has_parseable_json {
            return Ok(None);
        }
        if has_streams {
            return Ok(Some(parseable_json_result.unwrap()));
        }

        if self.get_storage_mode_string() == "Local drive" {
            return Err(ObjectStorageError::Custom(format!("Could not start the server because directory '{}' contains stale data, please use an empty directory, and restart the server.\n{}", self.storage.get_endpoint(), JOIN_COMMUNITY)));
        }

        // S3 bucket mode
        Err(ObjectStorageError::Custom(format!("Could not start the server because bucket '{}' contains stale data, please use an empty bucket and restart the server.\n{}", self.storage.get_endpoint(), JOIN_COMMUNITY)))
    }

    pub fn storage(&self) -> Arc<dyn ObjectStorageProvider> {
        self.storage.clone()
    }

    pub fn staging_dir(&self) -> &PathBuf {
        &self.options.local_staging_path
    }

    pub fn hot_tier_dir(&self) -> &Option<PathBuf> {
        &self.options.hot_tier_storage_path
    }

    pub fn is_default_creds(&self) -> bool {
        self.options.username == DEFAULT_USERNAME && self.options.password == DEFAULT_PASSWORD
    }

    // returns the string representation of the storage mode
    // drive --> Local drive
    // s3 --> S3 bucket
    // azure_blob --> Azure Blob Storage
    pub fn get_storage_mode_string(&self) -> &str {
        if self.storage_name == "drive" {
            return "Local drive";
        } else if self.storage_name == "s3" {
            return "S3 bucket";
        } else if self.storage_name == "blob_store" {
            return "Azure Blob Storage";
        }
        "Unknown"
    }

    pub fn get_server_mode_string(&self) -> &str {
        match self.options.mode {
            Mode::Query => "Distributed (Query)",
            Mode::Ingest => "Distributed (Ingest)",
            Mode::All => "Standalone",
        }
    }
}

#[derive(Debug, Default, Eq, PartialEq, Clone, Copy, Serialize, Deserialize)]
pub enum Mode {
    Query,
    Ingest,
    #[default]
    All,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Compression {
    Uncompressed,
    Snappy,
    Gzip,
    Lzo,
    Brotli,
    #[default]
    Lz4,
    Zstd,
}

impl From<Compression> for parquet::basic::Compression {
    fn from(value: Compression) -> Self {
        match value {
            Compression::Uncompressed => parquet::basic::Compression::UNCOMPRESSED,
            Compression::Snappy => parquet::basic::Compression::SNAPPY,
            Compression::Gzip => parquet::basic::Compression::GZIP(GzipLevel::default()),
            Compression::Lzo => parquet::basic::Compression::LZO,
            Compression::Brotli => parquet::basic::Compression::BROTLI(BrotliLevel::default()),
            Compression::Lz4 => parquet::basic::Compression::LZ4,
            Compression::Zstd => parquet::basic::Compression::ZSTD(ZstdLevel::default()),
        }
    }
}

pub mod validation {
    use std::{
        env, io,
        net::ToSocketAddrs,
        path::{Path, PathBuf},
        time::Duration,
    };

    use path_clean::PathClean;

    #[cfg(any(
        all(target_os = "linux", target_arch = "x86_64"),
        all(target_os = "macos", target_arch = "aarch64")
    ))]
    use crate::kafka::SslProtocol;

    use super::{Compression, Mode};

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

    #[cfg(any(
        all(target_os = "linux", target_arch = "x86_64"),
        all(target_os = "macos", target_arch = "aarch64")
    ))]
    pub fn kafka_security_protocol(s: &str) -> Result<SslProtocol, String> {
        match s {
            "plaintext" => Ok(SslProtocol::Plaintext),
            "ssl" => Ok(SslProtocol::Ssl),
            "sasl_plaintext" => Ok(SslProtocol::SaslPlaintext),
            "sasl_ssl" => Ok(SslProtocol::SaslSsl),
            _ => Err("Invalid Kafka Security Protocol provided".to_string()),
        }
    }

    pub fn mode(s: &str) -> Result<Mode, String> {
        match s {
            "query" => Ok(Mode::Query),
            "ingest" => Ok(Mode::Ingest),
            "all" => Ok(Mode::All),
            _ => Err("Invalid MODE provided".to_string()),
        }
    }

    pub fn duration(secs: &str) -> Result<Duration, String> {
        let Ok(secs) = secs.parse() else {
            return Err("Couldn't pass as a number".to_string());
        };

        Ok(Duration::from_secs(secs))
    }

    pub fn compression(s: &str) -> Result<Compression, String> {
        match s {
            "uncompressed" => Ok(Compression::Uncompressed),
            "snappy" => Ok(Compression::Snappy),
            "gzip" => Ok(Compression::Gzip),
            "lzo" => Ok(Compression::Lzo),
            "brotli" => Ok(Compression::Brotli),
            "lz4" => Ok(Compression::Lz4),
            "zstd" => Ok(Compression::Zstd),
            _ => Err("Invalid COMPRESSION provided".to_string()),
        }
    }

    pub fn validate_disk_usage(max_disk_usage: &str) -> Result<f64, String> {
        if let Ok(max_disk_usage) = max_disk_usage.parse::<f64>() {
            if (0.0..=100.0).contains(&max_disk_usage) {
                Ok(max_disk_usage)
            } else {
                Err("Invalid value for max disk usage. It should be between 0 and 100".to_string())
            }
        } else {
            Err("Invalid value for max disk usage. It should be given as 90.0 for 90%".to_string())
        }
    }
}
