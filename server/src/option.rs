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
use clap::{command, Args, Command, FromArgMatches};

use once_cell::sync::Lazy;
use parquet::basic::{BrotliLevel, GzipLevel, ZstdLevel};
use std::env;
use std::path::PathBuf;
use std::sync::Arc;

use crate::cli::Cli;
use crate::storage::PARSEABLE_METADATA_FILE_NAME;
use crate::storage::{FSConfig, ObjectStorageError, ObjectStorageProvider, S3Config};
pub const MIN_CACHE_SIZE_BYTES: u64 = 1000u64.pow(3); // 1 GiB
pub const JOIN_COMMUNITY: &str =
    "Join us on Parseable Slack community for questions : https://logg.ing/community";
pub static CONFIG: Lazy<Arc<Config>> = Lazy::new(|| Arc::new(Config::new()));

#[derive(Debug)]
pub struct Config {
    pub parseable: Cli,
    storage: Arc<dyn ObjectStorageProvider + Send + Sync>,
    pub storage_name: &'static str,
}

impl Config {
    fn new() -> Self {
        let cli = create_parseable_cli_command().get_matches();
        match cli.subcommand() {
            Some(("local-store", m)) => {
                let cli = match Cli::from_arg_matches(m) {
                    Ok(cli) => cli,
                    Err(err) => err.exit(),
                };
                let storage = match FSConfig::from_arg_matches(m) {
                    Ok(storage) => storage,
                    Err(err) => err.exit(),
                };

                if cli.local_staging_path == storage.root {
                    create_parseable_cli_command()
                        .error(
                            ErrorKind::ValueValidation,
                            "Cannot use same path for storage and staging",
                        )
                        .exit()
                }

                if cli.local_cache_path.is_some() {
                    create_parseable_cli_command()
                        .error(
                            ErrorKind::ValueValidation,
                            "Cannot use cache with local-store subcommand.",
                        )
                        .exit()
                }

                Config {
                    parseable: cli,
                    storage: Arc::new(storage),
                    storage_name: "drive",
                }
            }
            Some(("s3-store", m)) => {
                let cli = match Cli::from_arg_matches(m) {
                    Ok(cli) => cli,
                    Err(err) => err.exit(),
                };
                let storage = match S3Config::from_arg_matches(m) {
                    Ok(storage) => storage,
                    Err(err) => err.exit(),
                };

                Config {
                    parseable: cli,
                    storage: Arc::new(storage),
                    storage_name: "s3",
                }
            }
            _ => unreachable!(),
        }
    }

    // validate the storage, if the proper path for staging directory is provided
    // if the proper data directory is provided, or s3 bucket is provided etc
    // probably change name to validate_storage or something
    pub async fn validate(&self) -> Result<(), ObjectStorageError> {
        let obj_store = self.storage.get_object_store();
        let rel_path = relative_path::RelativePathBuf::from(PARSEABLE_METADATA_FILE_NAME);

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

        if self.get_storage_mode_string() == "Local drive" {
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
        self.parseable.username == Cli::DEFAULT_USERNAME
            && self.parseable.password == Cli::DEFAULT_PASSWORD
    }

    // returns the string representation of the storage mode
    // drive --> Local drive
    // s3 --> S3 bucket
    pub fn get_storage_mode_string(&self) -> &str {
        if self.storage_name == "drive" {
            return "Local drive";
        }
        "S3 bucket"
    }
}

fn create_parseable_cli_command() -> Command {
    let local = Cli::create_cli_command_with_clap("local-store");
    let local = <FSConfig as Args>::augment_args_for_update(local);

    let local = local
        .mut_arg(Cli::USERNAME, |arg| {
            arg.required(false).default_value(Cli::DEFAULT_USERNAME)
        })
        .mut_arg(Cli::PASSWORD, |arg| {
            arg.required(false).default_value(Cli::DEFAULT_PASSWORD)
        });
    let s3 = Cli::create_cli_command_with_clap("s3-store");
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

#[derive(Debug, Default, Eq, PartialEq)]
pub enum Mode {
    Query,
    Ingest,
    #[default]
    All,
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
}
