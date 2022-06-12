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

use std::path::PathBuf;
use std::sync::Arc;
use structopt::StructOpt;

lazy_static::lazy_static! {
    #[derive(Debug)]
    pub static ref CONFIG: Arc<Opt> = Arc::new(Opt::from_args());
}

pub const DEFAULT_S3_URL: &str = "http://127.0.0.1:9000";
pub const S3_URL_ENV_VAR: &str = "P_S3_URL";
pub const USERNAME_ENV: &str = "P_USERNAME";
pub const PASSOWRD_ENV: &str = "P_PASSWORD";
pub const DEFAULT_USERNAME: &str = "parseable";
pub const DEFAULT_PASSWORD: &str = "parseable";

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

    /// The endpoint to AWS S3 or compatible object storage platform
    #[structopt(long, env = S3_URL_ENV_VAR, default_value = DEFAULT_S3_URL )]
    pub s3_endpoint_url: String,

    /// The access key for AWS S3 or compatible object storage platform
    #[structopt(long, env = "P_S3_ACCESS_KEY", default_value = "minioadmin")]
    pub s3_access_key_id: String,

    /// The secret key for AWS S3 or compatible object storage platform
    #[structopt(long, env = "P_S3_SECRET_KEY", default_value = "minioadmin")]
    pub s3_secret_key: String,

    /// The region for AWS S3 or compatible object storage platform
    #[structopt(long, env = "P_S3_REGION", default_value = "us-east-1")]
    pub s3_default_region: String,

    /// Optional duration after which server would send uncommited data to remote object
    /// storage platform. Defaults to 10min.
    #[structopt(long, env = "P_STORAGE_SYNC_DURATION", default_value = "600")]
    pub sync_duration: u64,

    /// The AWS S3 or compatible object storage bucket to be used for storage
    #[structopt(long, env = "P_S3_BUCKET", default_value = "logstorage")]
    pub s3_bucket_name: String,

    /// Optional username to enable basic auth on the server
    #[structopt(long, env = USERNAME_ENV, default_value = DEFAULT_USERNAME)]
    pub username: String,

    /// Optional password to enable basic auth on the server
    #[structopt(long, env = PASSOWRD_ENV, default_value = DEFAULT_PASSWORD)]
    pub password: String,
}
