/*
 * Parseable Server (C) 2022 Parseable, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

use std::path::PathBuf;
use structopt::StructOpt;

#[derive(Debug, Clone, StructOpt)]
#[structopt(
    name = "Parseable config",
    about = "the config setup for Parseable server"
)]
pub struct Opt {
    /// The location of TLS Cert file
    #[structopt(long, env = "P_TLS_CERT_PATH")]
    pub tls_cert_path: Option<PathBuf>,

    /// The location of TLS Private Key file
    #[structopt(long, env = "P_TLS_KEY_PATH")]
    pub tls_key_path: Option<PathBuf>,

    /// The address on which the http server will listen.
    #[structopt(long, env = "P_ADDR", default_value = "0.0.0.0:5678")]
    pub address: String,

    /// The local storage path is used as temporary landing point
    /// for incoming events and local cache while querying data pulled
    /// from object storage backend
    #[structopt(long, env = "P_LOCAL_STORAGE", default_value = "./data")]
    pub local_disk_path: String,

    /// The endpoint to AWS S3 or compatible object storage platform
    #[structopt(long, env = "P_S3_URL", default_value = "http://127.0.0.1:9000")]
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
    /// storage platform. Defaults to 10s.
    #[structopt(long, env = "P_STORAGE_SYNC_DURATION", default_value = "600")]
    pub sync_duration: u64,

    /// The AWS S3 or compatible object storage bucket to be used for storage
    #[structopt(
        long,
        env = "P_S3_BUCKET",
        default_value = "67111b0f870e443ca59200b51221243b"
    )]
    pub s3_bucket_name: String,

    /// Optional username to enable basic auth on the server
    #[structopt(long, env = "P_USERNAME")]
    pub username: Option<String>,

    /// Optional password to enable basic auth on the server
    #[structopt(long, env = "P_PASSWORD")]
    pub password: Option<String>,
}

pub fn get_opts() -> Opt {
    Opt::from_args()
}
