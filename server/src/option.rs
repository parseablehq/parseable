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

use clap::builder::ArgPredicate;
use clap::{Parser, Subcommand};
use crossterm::style::Stylize;
use std::path::PathBuf;
use std::sync::Arc;

use crate::banner;
use crate::storage::{
    FSConfig, ObjectStorage, ObjectStorageError, ObjectStorageProvider, S3Config,
    LOCAL_SYNC_INTERVAL,
};

lazy_static::lazy_static! {
    #[derive(Debug)]
    pub static ref CONFIG: Arc<Config> = Arc::new(Config::new());
}

pub const USERNAME_ENV: &str = "P_USERNAME";
pub const PASSWORD_ENV: &str = "P_PASSWORD";
pub const DEFAULT_USERNAME: &str = "parseable";
pub const DEFAULT_PASSWORD: &str = "parseable";

pub struct Config {
    pub parseable: Server,
    storage: Arc<dyn ObjectStorageProvider + Send + Sync>,
}

impl Config {
    fn new() -> Self {
        let cli = Cli::parse();
        match cli.command {
            SubCmd::ServerS3 { server, storage } => Config {
                parseable: server,
                storage: Arc::new(storage),
            },
            SubCmd::ServerDrive { server, storage } => Config {
                parseable: server,
                storage: Arc::new(storage),
            },
        }
    }

    pub fn print(&self) {
        let scheme = CONFIG.parseable.get_scheme();
        self.status_info(&scheme);
        banner::version::print();
        self.demo();
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
        Local Data Path: {}
        Object Storage: {}",
            "Storage:".to_string().blue().bold(),
            self.parseable.local_disk_path.to_string_lossy(),
            self.storage().get_endpoint(),
        )
    }

    fn demo(&self) {
        if self.is_demo() {
            banner::warning_line();
            eprintln!(
                "
        {}",
                "Parseable is in demo mode with default credentials and open object store. Please use this for demo purposes only."
                    .to_string()
                    .red(),
                )
        }
    }

    fn is_demo(&self) -> bool {
        self.parseable.demo
    }

    pub fn storage(&self) -> Arc<dyn ObjectStorageProvider + Send + Sync> {
        self.storage.clone()
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
    #[command(name = "--s3")]
    ServerS3 {
        #[command(flatten)]
        server: Server,
        #[command(flatten)]
        storage: S3Config,
    },
    #[command(name = "--drive")]
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

    /// The local storage path is used as temporary landing point
    /// for incoming events and local cache while querying data pulled
    /// from object storage backend
    #[arg(
        long,
        env = "P_LOCAL_STORAGE",
        default_value = "./data",
        value_name = "path"
    )]
    pub local_disk_path: PathBuf,

    /// Optional interval after which server would upload uncommited data to
    /// remote object storage platform. Defaults to 1min.
    #[arg(
        long,
        env = "P_STORAGE_UPLOAD_INTERVAL",
        default_value = "60",
        value_name = "seconds"
    )]
    pub upload_interval: u64,

    /// Optional username to enable basic auth on the server
    #[arg(
        long,
        env = USERNAME_ENV,
        value_name = "username",
        default_value_if("demo", ArgPredicate::IsPresent, DEFAULT_USERNAME)
    )]
    pub username: String,

    /// Optional password to enable basic auth on the server
    #[arg(
        long,
        env = PASSWORD_ENV,
        value_name = "password",
        default_value_if("demo", ArgPredicate::IsPresent, DEFAULT_PASSWORD)
    )]
    pub password: String,

    /// Run Parseable in demo mode with default credentials and open object store
    #[arg(short, long, exclusive = true)]
    pub demo: bool,
}

impl Server {
    pub fn local_stream_data_path(&self, stream_name: &str) -> PathBuf {
        self.local_disk_path.join(stream_name)
    }

    pub fn get_scheme(&self) -> String {
        if self.tls_cert_path.is_some() && self.tls_key_path.is_some() {
            return "https".to_string();
        }

        "http".to_string()
    }
}

pub(self) mod validation {
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
