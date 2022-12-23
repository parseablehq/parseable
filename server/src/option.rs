/*
 * Parseable Server (C) 2022 Parseable, Inc.
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

use clap::{Parser, Subcommand};
use crossterm::style::Stylize;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use crate::banner;
use crate::storage::{
    FSConfig, ObjectStorage, ObjectStorageError, ObjectStorageProvider, S3Config,
    LOCAL_SYNC_INTERVAL,
};
use crate::utils::capitalize_ascii;

lazy_static::lazy_static! {
    #[derive(Debug)]
    pub static ref CONFIG: Arc<Config> = Arc::new(Config::new());
}

pub const USERNAME_ENV: &str = "P_USERNAME";
pub const PASSWORD_ENV: &str = "P_PASSWORD";

pub struct Config {
    pub parseable: Server,
    storage: Arc<dyn ObjectStorageProvider + Send + Sync>,
    pub storage_name: &'static str,
}

impl Config {
    fn new() -> Self {
        let cli = Cli::parse();
        match cli.command {
            SubCmd::ServerS3 { server, storage } => Config {
                parseable: server,
                storage: Arc::new(storage),
                storage_name: "s3",
            },
            SubCmd::ServerDrive { server, storage } => Config {
                parseable: server,
                storage: Arc::new(storage),
                storage_name: "drive",
            },
        }
    }

    pub fn print(&self) {
        let scheme = CONFIG.parseable.get_scheme();
        self.status_info(&scheme);
        banner::version::print();
        self.storage_info();
        banner::system_info();
        println!();
    }

    pub fn validate(&self) {
        if CONFIG.parseable.upload_interval < LOCAL_SYNC_INTERVAL {
            panic!("object storage upload_interval (P_STORAGE_UPLOAD_INTERVAL) must be 60 seconds or more");
        }
    }

    pub async fn validate_storage(&self, storage: &(impl ObjectStorage + ?Sized)) {
        match storage.check().await {
            Ok(_) => (),
            Err(ObjectStorageError::ConnectionError(inner)) => panic!(
                "Failed to connect to the Object Storage Service on {url}\nCaused by: {cause}",
                url = self.storage().get_endpoint(),
                cause = inner
            ),
            Err(ObjectStorageError::AuthenticationError(inner)) => panic!(
                "Failed to authenticate. Please ensure credentials are valid\n Caused by: {cause}",
                cause = inner
            ),
            Err(error) => {
                panic!("{error}")
            }
        }
    }

    fn status_info(&self, scheme: &str) {
        let url = format!("{}://{}", scheme, self.parseable.address).underlined();
        eprintln!(
            "
    {}
    {}
    {}",
            format!("Parseable server started at: {}", url).bold(),
            format!("Username: {}", self.parseable.username).bold(),
            format!("Password: {}", self.parseable.password).bold(),
        )
    }

    fn storage_info(&self) {
        eprintln!(
            "
    {}
        Local Staging Path: {}
        {} Storage: {}",
            "Storage:".to_string().blue().bold(),
            self.staging_dir().to_string_lossy(),
            capitalize_ascii(self.storage_name),
            self.storage().get_endpoint(),
        )
    }

    pub fn storage(&self) -> Arc<dyn ObjectStorageProvider + Send + Sync> {
        self.storage.clone()
    }

    pub fn staging_dir(&self) -> &Path {
        &self.parseable.local_staging_path
    }
}

impl Default for Config {
    fn default() -> Self {
        Self::new()
    }
}

#[derive(Parser)] // requires `derive` feature
#[command(
    name = "Parseable",
    bin_name = "parseable",
    about = "Parseable is a log storage and observability platform.",
    version
)]
struct Cli {
    #[command(subcommand)]
    command: SubCmd,
}

#[derive(Subcommand, Clone)]
enum SubCmd {
    #[command(name = "--s3-store")]
    ServerS3 {
        #[command(flatten)]
        server: Server,
        #[command(flatten)]
        storage: S3Config,
    },
    #[command(name = "--local-store")]
    ServerDrive {
        #[command(flatten)]
        server: Server,
        #[command(flatten)]
        storage: FSConfig,
    },
}

#[derive(clap::Args, Debug, Clone)]
#[clap(name = "server", about = "Start the Parseable server")]
pub struct Server {
    /// The location of TLS Cert file
    #[arg(
        long,
        env = "P_TLS_CERT_PATH",
        value_name = "path",
        value_parser = validation::file_path
    )]
    pub tls_cert_path: Option<PathBuf>,

    /// The location of TLS Private Key file
    #[arg(
        long,
        env = "P_TLS_KEY_PATH",
        value_name = "path",
        value_parser = validation::file_path
    )]
    pub tls_key_path: Option<PathBuf>,

    /// The address on which the http server will listen.
    #[arg(
        long,
        env = "P_ADDR",
        default_value = "0.0.0.0:8000",
        value_name = "url"
    )]
    pub address: String,

    /// The local staging path is used as a temporary landing point
    /// for incoming events and local cache
    #[arg(
        long,
        env = "P_STAGING_DIR",
        default_value = "./data",
        value_name = "path"
    )]
    pub local_staging_path: PathBuf,

    /// Interval in seconds after which uncommited data would be
    /// uploaded to the storage platform.
    #[arg(
        long,
        env = "P_STORAGE_UPLOAD_INTERVAL",
        default_value = "60",
        value_name = "seconds"
    )]
    pub upload_interval: u64,

    /// Username for the basic authentication on the server
    #[arg(
        long,
        env = USERNAME_ENV,
        value_name = "username",
    )]
    pub username: String,

    /// Password for the basic authentication on the server
    #[arg(
        long,
        env = PASSWORD_ENV,
        value_name = "password",
    )]
    pub password: String,
}

impl Server {
    pub fn local_stream_data_path(&self, stream_name: &str) -> PathBuf {
        self.local_staging_path.join(stream_name)
    }

    pub fn get_scheme(&self) -> String {
        if self.tls_cert_path.is_some() && self.tls_key_path.is_some() {
            return "https".to_string();
        }

        "http".to_string()
    }
}

pub mod validation {
    use std::path::PathBuf;

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
}
