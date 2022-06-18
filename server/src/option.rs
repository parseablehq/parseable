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

use crossterm::style::Stylize;
use std::path::PathBuf;
use std::sync::Arc;
use structopt::StructOpt;

use crate::banner::{system_info, warning_line};
use crate::s3::S3Config;

lazy_static::lazy_static! {
    #[derive(Debug)]
    pub static ref CONFIG: Arc<Config> = {
        let storage = Box::new(S3Config::from_args());
        Arc::new(Config::new(storage))
    };
}

pub const USERNAME_ENV: &str = "P_USERNAME";
pub const PASSOWRD_ENV: &str = "P_PASSWORD";
pub const DEFAULT_USERNAME: &str = "parseable";
pub const DEFAULT_PASSWORD: &str = "parseable";

pub trait StorageOpt: Sync + Send {
    fn bucket_name(&self) -> &str;
    fn endpoint_url(&self) -> &str;
    fn warning(&self);
    fn is_default_url(&self) -> bool;
}

pub struct Config {
    pub parseable: Opt,
    pub storage: Box<dyn StorageOpt>,
}

impl Config {
    fn new(storage: Box<dyn StorageOpt>) -> Config {
        Config {
            parseable: Opt::from_args(),
            storage,
        }
    }

    pub fn print(&self) {
        let scheme = CONFIG.parseable.get_scheme();
        self.status_info(&scheme);
        self.warning();
        self.storage_info();
        system_info();
        println!();
    }

    fn status_info(&self, scheme: &str) {
        let url = format!("{}://{}", scheme, CONFIG.parseable.address).underlined();
        eprintln!(
            "
    {}
    {}
    {}",
            format!("Parseable server started at: {}", url).bold(),
            format!("Username: {}", CONFIG.parseable.username).bold(),
            format!("Password: {}", CONFIG.parseable.password).bold(),
        )
    }

    fn storage_info(&self) {
        eprintln!(
            "
    {}
        Local Data Path: {}
        Object Storage: {}/{}",
            "Storage:".to_string().blue().bold(),
            self.parseable.local_disk_path,
            self.storage.endpoint_url(),
            self.storage.bucket_name()
        )
    }

    fn warning(&self) {
        match (self.storage.is_default_url(), self.is_default_cred()) {
            (true, true) => {
                warning_line();
                self.cred_warning();
                self.storage.warning();
            }
            (true, _) => {
                warning_line();
                self.storage.warning();
            }
            (_, true) => {
                warning_line();
                self.cred_warning();
            }
            _ => {}
        }
    }

    fn is_default_cred(&self) -> bool {
        CONFIG.parseable.username == DEFAULT_USERNAME
            && CONFIG.parseable.password == DEFAULT_PASSWORD
    }

    fn cred_warning(&self) {
        if self.is_default_cred() {
            eprintln!(
                "
        {}
        {}",
                "Parseable server is using default credentials."
                    .to_string()
                    .red(),
                format!(
                    "Setup your credentials with {} and {} before storing production logs.",
                    USERNAME_ENV, PASSOWRD_ENV
                )
                .red()
            )
        }
    }
}

#[derive(Debug, Clone, StructOpt)]
#[structopt(
    name = "Parseable config",
    about = "configuration for Parseable server"
)]
pub struct Opt {
    /// The location of TLS Cert file
    #[structopt(long, env = "P_TLS_CERT_PATH")]
    pub tls_cert_path: Option<PathBuf>,

    /// The location of TLS Private Key file
    #[structopt(long, env = "P_TLS_KEY_PATH")]
    pub tls_key_path: Option<PathBuf>,

    /// The address on which the http server will listen.
    #[structopt(long, env = "P_ADDR", default_value = "127.0.0.1:5678")]
    pub address: String,

    /// The local storage path is used as temporary landing point
    /// for incoming events and local cache while querying data pulled
    /// from object storage backend
    #[structopt(long, env = "P_LOCAL_STORAGE", default_value = "./data")]
    pub local_disk_path: String,

    /// Optional duration after which server would send uncommited data to remote object
    /// storage platform. Defaults to 10min.
    #[structopt(long, env = "P_STORAGE_SYNC_DURATION", default_value = "600")]
    pub sync_duration: u64,

    /// Optional username to enable basic auth on the server
    #[structopt(long, env = USERNAME_ENV, default_value = DEFAULT_USERNAME)]
    pub username: String,

    /// Optional password to enable basic auth on the server
    #[structopt(long, env = PASSOWRD_ENV, default_value = DEFAULT_PASSWORD)]
    pub password: String,
}

impl Opt {
    pub fn get_cache_path(&self, stream_name: &str) -> String {
        format!("{}/{}", self.local_disk_path, stream_name)
    }

    pub fn local_stream_data_path(&self, stream_name: &str) -> String {
        format!("{}/{}", self.local_disk_path, stream_name)
    }

    pub fn get_scheme(&self) -> String {
        if self.tls_cert_path.is_some() && self.tls_key_path.is_some() {
            return "https".to_string();
        }

        "http".to_string()
    }
}
