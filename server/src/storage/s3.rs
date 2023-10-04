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

use async_trait::async_trait;
use bytes::Bytes;
use datafusion::datasource::listing::ListingTableUrl;
use datafusion::datasource::object_store::{
    DefaultObjectStoreRegistry, ObjectStoreRegistry, ObjectStoreUrl,
};
use datafusion::execution::runtime_env::RuntimeConfig;
use futures::stream::FuturesUnordered;
use futures::{StreamExt, TryStreamExt};
use object_store::aws::{AmazonS3, AmazonS3Builder, AmazonS3ConfigKey, Checksum};
use object_store::limit::LimitStore;
use object_store::path::Path as StorePath;
use object_store::{ClientOptions, ObjectStore};
use relative_path::RelativePath;
use tokio::fs::OpenOptions;
use tokio::io::{AsyncReadExt, AsyncWriteExt};

use std::iter::Iterator;
use std::path::Path as StdPath;
use std::sync::Arc;
use std::time::{Duration, Instant};

use crate::metrics::storage::{s3::REQUEST_RESPONSE_TIME, StorageMetrics};
use crate::storage::{LogStream, ObjectStorage, ObjectStorageError};

use super::metrics_layer::MetricLayer;
use super::{object_storage, ObjectStorageProvider};

// in bytes
const MULTIPART_UPLOAD_SIZE: usize = 1024 * 1024 * 100;
const CONNECT_TIMEOUT_SECS: u64 = 5;
const AWS_CONTAINER_CREDENTIALS_RELATIVE_URI: &str = "AWS_CONTAINER_CREDENTIALS_RELATIVE_URI";

#[derive(Debug, Clone, clap::Args)]
#[command(
    name = "S3 config",
    about = "Start Parseable with S3 or compatible as storage",
    help_template = "\
{about-section}
{all-args}
"
)]
pub struct S3Config {
    /// The endpoint to AWS S3 or compatible object storage platform
    #[arg(long, env = "P_S3_URL", value_name = "url", required = true)]
    pub endpoint_url: String,

    /// The access key for AWS S3 or compatible object storage platform
    #[arg(long, env = "P_S3_ACCESS_KEY", value_name = "access-key")]
    pub access_key_id: Option<String>,

    /// The secret key for AWS S3 or compatible object storage platform
    #[arg(long, env = "P_S3_SECRET_KEY", value_name = "secret-key")]
    pub secret_key: Option<String>,

    /// The region for AWS S3 or compatible object storage platform
    #[arg(long, env = "P_S3_REGION", value_name = "region", required = true)]
    pub region: String,

    /// The AWS S3 or compatible object storage bucket to be used for storage
    #[arg(long, env = "P_S3_BUCKET", value_name = "bucket-name", required = true)]
    pub bucket_name: String,

    /// Set client to send checksum header on every put request
    #[arg(
        long,
        env = "P_S3_CHECKSUM",
        value_name = "bool",
        default_value = "false"
    )]
    pub set_checksum: bool,

    /// Set client to use virtual hosted style acess
    #[arg(
        long,
        env = "P_S3_PATH_STYLE",
        value_name = "bool",
        default_value = "true"
    )]
    pub use_path_style: bool,

    /// Set client to skip tls verification
    #[arg(
        long,
        env = "P_S3_TLS_SKIP_VERIFY",
        value_name = "bool",
        default_value = "false"
    )]
    pub skip_tls: bool,

    /// Set client to fallback to imdsv1
    #[arg(
        long,
        env = "P_AWS_IMDSV1_FALLBACK",
        value_name = "bool",
        default_value = "false"
    )]
    pub imdsv1_fallback: bool,

    /// Set instance metadata endpoint to use.
    #[arg(
        long,
        env = "P_AWS_METADATA_ENDPOINT",
        value_name = "url",
        required = false
    )]
    pub metadata_endpoint: Option<String>,
}

impl S3Config {
    fn get_default_builder(&self) -> AmazonS3Builder {
        let mut client_options = ClientOptions::default()
            .with_allow_http(true)
            .with_connect_timeout(Duration::from_secs(CONNECT_TIMEOUT_SECS));

        if self.skip_tls {
            client_options = client_options.with_allow_invalid_certificates(true)
        }

        let mut builder = AmazonS3Builder::new()
            .with_region(&self.region)
            .with_endpoint(&self.endpoint_url)
            .with_bucket_name(&self.bucket_name)
            .with_virtual_hosted_style_request(!self.use_path_style)
            .with_allow_http(true);

        if self.set_checksum {
            builder = builder.with_checksum_algorithm(Checksum::SHA256)
        }

        if let Some((access_key, secret_key)) =
            self.access_key_id.as_ref().zip(self.secret_key.as_ref())
        {
            builder = builder
                .with_access_key_id(access_key)
                .with_secret_access_key(secret_key);
        }

        if let Ok(relative_uri) = std::env::var(AWS_CONTAINER_CREDENTIALS_RELATIVE_URI) {
            builder = builder.with_config(
                AmazonS3ConfigKey::ContainerCredentialsRelativeUri,
                relative_uri,
            );
        }

        if self.imdsv1_fallback {
            builder = builder.with_imdsv1_fallback()
        }

        if let Some(metadata_endpoint) = &self.metadata_endpoint {
            builder = builder.with_metadata_endpoint(metadata_endpoint)
        }

        builder.with_client_options(client_options)
    }
}

impl ObjectStorageProvider for S3Config {
    fn get_datafusion_runtime(&self) -> RuntimeConfig {
        let s3 = self.get_default_builder().build().unwrap();

        // limit objectstore to a concurrent request limit
        let s3 = LimitStore::new(s3, super::MAX_OBJECT_STORE_REQUESTS);
        let s3 = MetricLayer::new(s3);

        let object_store_registry: DefaultObjectStoreRegistry = DefaultObjectStoreRegistry::new();
        let url = ObjectStoreUrl::parse(format!("s3://{}", &self.bucket_name)).unwrap();
        object_store_registry.register_store(url.as_ref(), Arc::new(s3));

        RuntimeConfig::new().with_object_store_registry(Arc::new(object_store_registry))
    }

    fn get_object_store(&self) -> Arc<dyn ObjectStorage + Send> {
        let s3 = self.get_default_builder().build().unwrap();

        // limit objectstore to a concurrent request limit
        let s3 = LimitStore::new(s3, super::MAX_OBJECT_STORE_REQUESTS);

        Arc::new(S3 {
            client: s3,
            bucket: self.bucket_name.clone(),
        })
    }

    fn get_endpoint(&self) -> String {
        format!("{}/{}", self.endpoint_url, self.bucket_name)
    }

    fn register_store_metrics(&self, handler: &actix_web_prometheus::PrometheusMetrics) {
        self.register_metrics(handler)
    }
}

fn to_path(path: &RelativePath) -> StorePath {
    StorePath::from(path.as_str())
}

pub struct S3 {
    client: LimitStore<AmazonS3>,
    bucket: String,
}

impl S3 {
    async fn _get_object(&self, path: &RelativePath) -> Result<Bytes, ObjectStorageError> {
        let instant = Instant::now();

        let resp = self.client.get(&to_path(path)).await;

        match resp {
            Ok(resp) => {
                let time = instant.elapsed().as_secs_f64();
                REQUEST_RESPONSE_TIME
                    .with_label_values(&["GET", "200"])
                    .observe(time);
                let body = resp.bytes().await.unwrap();
                Ok(body)
            }
            Err(err) => {
                let time = instant.elapsed().as_secs_f64();
                REQUEST_RESPONSE_TIME
                    .with_label_values(&["GET", "400"])
                    .observe(time);
                Err(err.into())
            }
        }
    }

    async fn _put_object(
        &self,
        path: &RelativePath,
        resource: Bytes,
    ) -> Result<(), ObjectStorageError> {
        let time = Instant::now();
        let resp = self.client.put(&to_path(path), resource).await;
        let status = if resp.is_ok() { "200" } else { "400" };
        let time = time.elapsed().as_secs_f64();
        REQUEST_RESPONSE_TIME
            .with_label_values(&["PUT", status])
            .observe(time);

        resp.map(|_| ()).map_err(|err| err.into())
    }

    async fn _delete_prefix(&self, key: &str) -> Result<(), ObjectStorageError> {
        let object_stream = self.client.list(Some(&(key.into()))).await?;

        object_stream
            .for_each_concurrent(None, |x| async {
                match x {
                    Ok(obj) => {
                        if (self.client.delete(&obj.location).await).is_err() {
                            log::error!("Failed to fetch object during delete stream");
                        }
                    }
                    Err(_) => {
                        log::error!("Failed to fetch object during delete stream");
                    }
                };
            })
            .await;

        Ok(())
    }

    async fn _list_streams(&self) -> Result<Vec<LogStream>, ObjectStorageError> {
        let resp = self.client.list_with_delimiter(None).await?;

        let common_prefixes = resp.common_prefixes;

        // return prefixes at the root level
        let dirs: Vec<_> = common_prefixes
            .iter()
            .filter_map(|path| path.parts().next())
            .map(|name| name.as_ref().to_string())
            .collect();

        let stream_json_check = FuturesUnordered::new();

        for dir in &dirs {
            let key = format!("{}/{}", dir, object_storage::STREAM_METADATA_FILE_NAME);
            let task = async move { self.client.head(&StorePath::from(key)).await.map(|_| ()) };
            stream_json_check.push(task);
        }

        stream_json_check.try_collect().await?;

        Ok(dirs.into_iter().map(|name| LogStream { name }).collect())
    }

    async fn _list_dates(&self, stream: &str) -> Result<Vec<String>, ObjectStorageError> {
        let resp = self
            .client
            .list_with_delimiter(Some(&(stream.into())))
            .await?;

        let common_prefixes = resp.common_prefixes;

        // return prefixes at the root level
        let dates: Vec<_> = common_prefixes
            .iter()
            .filter_map(|path| path.as_ref().strip_prefix(&format!("{stream}/")))
            .map(String::from)
            .collect();

        Ok(dates)
    }

    async fn _upload_file(&self, key: &str, path: &StdPath) -> Result<(), ObjectStorageError> {
        let instant = Instant::now();

        let should_multipart = std::fs::metadata(path)?.len() > MULTIPART_UPLOAD_SIZE as u64;

        let res = if should_multipart {
            self._upload_multipart(key, path).await
        } else {
            let bytes = tokio::fs::read(path).await?;
            self.client
                .put(&key.into(), bytes.into())
                .await
                .map_err(|err| err.into())
        };

        let status = if res.is_ok() { "200" } else { "400" };
        let time = instant.elapsed().as_secs_f64();
        REQUEST_RESPONSE_TIME
            .with_label_values(&["UPLOAD_PARQUET", status])
            .observe(time);

        res
    }

    async fn _upload_multipart(&self, key: &str, path: &StdPath) -> Result<(), ObjectStorageError> {
        let mut buf = vec![0u8; MULTIPART_UPLOAD_SIZE / 2];
        let mut file = OpenOptions::new().read(true).open(path).await?;

        let (multipart_id, mut async_writer) = self.client.put_multipart(&key.into()).await?;

        let close_multipart = |err| async move {
            log::error!("multipart upload failed. {:?}", err);
            self.client
                .abort_multipart(&key.into(), &multipart_id)
                .await
        };

        loop {
            match file.read(&mut buf).await {
                Ok(len) => {
                    if len == 0 {
                        break;
                    }
                    if let Err(err) = async_writer.write_all(&buf[0..len]).await {
                        close_multipart(err).await?;
                        break;
                    }
                    if let Err(err) = async_writer.flush().await {
                        close_multipart(err).await?;
                        break;
                    }
                }
                Err(err) => {
                    close_multipart(err).await?;
                    break;
                }
            }
        }

        async_writer.shutdown().await?;

        Ok(())
    }
}

#[async_trait]
impl ObjectStorage for S3 {
    async fn get_object(&self, path: &RelativePath) -> Result<Bytes, ObjectStorageError> {
        Ok(self._get_object(path).await?)
    }

    async fn put_object(
        &self,
        path: &RelativePath,
        resource: Bytes,
    ) -> Result<(), ObjectStorageError> {
        self._put_object(path, resource)
            .await
            .map_err(|err| ObjectStorageError::ConnectionError(Box::new(err)))?;

        Ok(())
    }

    async fn delete_prefix(&self, path: &RelativePath) -> Result<(), ObjectStorageError> {
        self._delete_prefix(path.as_ref()).await?;

        Ok(())
    }

    async fn check(&self) -> Result<(), ObjectStorageError> {
        Ok(self
            .client
            .head(&object_storage::PARSEABLE_METADATA_FILE_NAME.into())
            .await
            .map(|_| ())?)
    }

    async fn delete_stream(&self, stream_name: &str) -> Result<(), ObjectStorageError> {
        self._delete_prefix(stream_name).await?;

        Ok(())
    }

    async fn list_streams(&self) -> Result<Vec<LogStream>, ObjectStorageError> {
        let streams = self._list_streams().await?;

        Ok(streams)
    }

    async fn list_dates(&self, stream_name: &str) -> Result<Vec<String>, ObjectStorageError> {
        let streams = self._list_dates(stream_name).await?;

        Ok(streams)
    }

    async fn upload_file(&self, key: &str, path: &StdPath) -> Result<(), ObjectStorageError> {
        self._upload_file(key, path).await?;

        Ok(())
    }

    fn query_prefixes(&self, prefixes: Vec<String>) -> Vec<ListingTableUrl> {
        prefixes
            .into_iter()
            .map(|prefix| {
                let path = format!("s3://{}/{}", &self.bucket, prefix);
                ListingTableUrl::parse(path).unwrap()
            })
            .collect()
    }
}

impl From<object_store::Error> for ObjectStorageError {
    fn from(error: object_store::Error) -> Self {
        match error {
            object_store::Error::Generic { source, .. } => {
                ObjectStorageError::UnhandledError(source)
            }
            object_store::Error::NotFound { path, .. } => ObjectStorageError::NoSuchKey(path),
            err => ObjectStorageError::UnhandledError(Box::new(err)),
        }
    }
}

impl From<serde_json::Error> for ObjectStorageError {
    fn from(error: serde_json::Error) -> Self {
        ObjectStorageError::UnhandledError(Box::new(error))
    }
}
