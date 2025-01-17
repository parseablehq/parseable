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

use async_trait::async_trait;
use bytes::Bytes;
use datafusion::datasource::listing::ListingTableUrl;
use datafusion::datasource::object_store::{
    DefaultObjectStoreRegistry, ObjectStoreRegistry, ObjectStoreUrl,
};
use datafusion::execution::runtime_env::RuntimeEnvBuilder;
use futures::stream::FuturesUnordered;
use futures::{StreamExt, TryStreamExt};
use object_store::aws::{AmazonS3, AmazonS3Builder, AmazonS3ConfigKey, Checksum};
use object_store::limit::LimitStore;
use object_store::path::Path as StorePath;
use object_store::{BackoffConfig, ClientOptions, ObjectStore, PutPayload, RetryConfig};
use relative_path::{RelativePath, RelativePathBuf};
use tracing::{error, info};

use std::collections::BTreeMap;
use std::fmt::Display;
use std::iter::Iterator;
use std::path::Path as StdPath;
use std::str::FromStr;
use std::sync::Arc;
use std::time::{Duration, Instant};

use super::metrics_layer::MetricLayer;
use super::object_storage::parseable_json_path;
use super::{
    ObjectStorageProvider, SCHEMA_FILE_NAME, STREAM_METADATA_FILE_NAME, STREAM_ROOT_DIRECTORY,
};
use crate::handlers::http::users::USERS_ROOT_DIR;
use crate::metrics::storage::{s3::REQUEST_RESPONSE_TIME, StorageMetrics};
use crate::storage::{LogStream, ObjectStorage, ObjectStorageError, PARSEABLE_ROOT_DIRECTORY};
use std::collections::HashMap;

#[allow(dead_code)]
// in bytes
const MULTIPART_UPLOAD_SIZE: usize = 1024 * 1024 * 100;
const CONNECT_TIMEOUT_SECS: u64 = 5;
const REQUEST_TIMEOUT_SECS: u64 = 300;
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

    /// Server side encryption to use for operations with objects.
    /// Currently, this only supports SSE-C. Value should be
    /// like SSE-C:AES256:<base64_encoded_encryption_key>.
    #[arg(
        long,
        env = "P_S3_SSEC_ENCRYPTION_KEY",
        value_name = "ssec-encryption-key"
    )]
    pub ssec_encryption_key: Option<SSECEncryptionKey>,

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

/// This represents the server side encryption to be
/// used when working with S3 objects.
#[derive(Debug, Clone)]
pub enum SSECEncryptionKey {
    /// https://docs.aws.amazon.com/AmazonS3/latest/userguide/ServerSideEncryptionCustomerKeys.html
    SseC {
        // algorithm unused but being tracked separately to maintain
        // consistent interface via CLI if AWS adds any new algorithms
        // in future.
        _algorithm: ObjectEncryptionAlgorithm,
        base64_encryption_key: String,
    },
}

#[derive(Debug, thiserror::Error)]
pub enum SSEError {
    #[error("Expected SSE-C:AES256:<base64_encryption_key>")]
    UnexpectedKey,
    #[error("Only SSE-C is supported for object encryption for now")]
    UnexpectedProtocol,
    #[error("Invalid SSE algorithm. Following are supported: AES256")]
    InvalidAlgorithm,
}

impl FromStr for SSECEncryptionKey {
    type Err = SSEError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let parts = s.split(':').collect::<Vec<_>>();
        if parts.len() != 3 {
            return Err(SSEError::UnexpectedKey);
        }
        let sse_type = parts[0];
        if sse_type != "SSE-C" {
            return Err(SSEError::UnexpectedProtocol);
        }

        let algorithm = parts[1];
        let encryption_key = parts[2];

        let alg = ObjectEncryptionAlgorithm::from_str(algorithm)?;

        Ok(match alg {
            ObjectEncryptionAlgorithm::Aes256 => SSECEncryptionKey::SseC {
                _algorithm: alg,
                base64_encryption_key: encryption_key.to_owned(),
            },
        })
    }
}

#[derive(Debug, Clone, Copy)]
pub enum ObjectEncryptionAlgorithm {
    Aes256,
}

impl FromStr for ObjectEncryptionAlgorithm {
    type Err = SSEError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "AES256" => Ok(ObjectEncryptionAlgorithm::Aes256),
            _ => Err(SSEError::InvalidAlgorithm),
        }
    }
}

impl Display for ObjectEncryptionAlgorithm {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ObjectEncryptionAlgorithm::Aes256 => write!(f, "AES256"),
        }
    }
}

impl S3Config {
    fn get_default_builder(&self) -> AmazonS3Builder {
        let mut client_options = ClientOptions::default()
            .with_allow_http(true)
            .with_connect_timeout(Duration::from_secs(CONNECT_TIMEOUT_SECS))
            .with_timeout(Duration::from_secs(REQUEST_TIMEOUT_SECS));

        if self.skip_tls {
            client_options = client_options.with_allow_invalid_certificates(true)
        }
        let retry_config = RetryConfig {
            max_retries: 5,
            retry_timeout: Duration::from_secs(30),
            backoff: BackoffConfig::default(),
        };

        let mut builder = AmazonS3Builder::new()
            .with_region(&self.region)
            .with_endpoint(&self.endpoint_url)
            .with_bucket_name(&self.bucket_name)
            .with_virtual_hosted_style_request(!self.use_path_style)
            .with_allow_http(true)
            .with_retry(retry_config);

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

        if let Some(ssec_encryption_key) = &self.ssec_encryption_key {
            match ssec_encryption_key {
                SSECEncryptionKey::SseC {
                    _algorithm,
                    base64_encryption_key,
                } => {
                    builder = builder.with_ssec_encryption(base64_encryption_key);
                }
            }
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
    fn get_datafusion_runtime(&self) -> RuntimeEnvBuilder {
        let s3 = self.get_default_builder().build().unwrap();

        // limit objectstore to a concurrent request limit
        let s3 = LimitStore::new(s3, super::MAX_OBJECT_STORE_REQUESTS);
        let s3 = MetricLayer::new(s3);

        let object_store_registry: DefaultObjectStoreRegistry = DefaultObjectStoreRegistry::new();
        let url = ObjectStoreUrl::parse(format!("s3://{}", &self.bucket_name)).unwrap();
        object_store_registry.register_store(url.as_ref(), Arc::new(s3));

        RuntimeEnvBuilder::new().with_object_store_registry(Arc::new(object_store_registry))
    }

    fn construct_client(&self) -> Arc<dyn ObjectStorage> {
        let s3 = self.get_default_builder().build().unwrap();

        // limit objectstore to a concurrent request limit
        let s3 = LimitStore::new(s3, super::MAX_OBJECT_STORE_REQUESTS);

        Arc::new(S3 {
            client: s3,
            bucket: self.bucket_name.clone(),
            root: StorePath::from(""),
        })
    }

    fn get_endpoint(&self) -> String {
        format!("{}/{}", self.endpoint_url, self.bucket_name)
    }

    fn register_store_metrics(&self, handler: &actix_web_prometheus::PrometheusMetrics) {
        self.register_metrics(handler)
    }
}

fn to_object_store_path(path: &RelativePath) -> StorePath {
    StorePath::from(path.as_str())
}

#[derive(Debug)]
pub struct S3 {
    client: LimitStore<AmazonS3>,
    bucket: String,
    root: StorePath,
}

impl S3 {
    async fn _get_object(&self, path: &RelativePath) -> Result<Bytes, ObjectStorageError> {
        let instant = Instant::now();

        let resp = self.client.get(&to_object_store_path(path)).await;

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
        resource: PutPayload,
    ) -> Result<(), ObjectStorageError> {
        let time = Instant::now();
        let resp = self.client.put(&to_object_store_path(path), resource).await;
        let status = if resp.is_ok() { "200" } else { "400" };
        let time = time.elapsed().as_secs_f64();
        REQUEST_RESPONSE_TIME
            .with_label_values(&["PUT", status])
            .observe(time);

        if let Err(object_store::Error::NotFound { source, .. }) = &resp {
            let source_str = source.to_string();
            if source_str.contains("<Code>NoSuchBucket</Code>") {
                return Err(ObjectStorageError::Custom(
                    format!("Bucket '{}' does not exist in S3.", self.bucket).to_string(),
                ));
            }
        }

        resp.map(|_| ()).map_err(|err| err.into())
    }

    async fn _delete_prefix(&self, key: &str) -> Result<(), ObjectStorageError> {
        let object_stream = self.client.list(Some(&(key.into())));

        object_stream
            .for_each_concurrent(None, |x| async {
                match x {
                    Ok(obj) => {
                        if (self.client.delete(&obj.location).await).is_err() {
                            error!("Failed to fetch object during delete stream");
                        }
                    }
                    Err(_) => {
                        error!("Failed to fetch object during delete stream");
                    }
                };
            })
            .await;

        Ok(())
    }

    async fn _list_streams(&self) -> Result<Vec<LogStream>, ObjectStorageError> {
        let mut result_file_list: Vec<LogStream> = Vec::new();
        let resp = self.client.list_with_delimiter(None).await?;

        let streams = resp
            .common_prefixes
            .iter()
            .flat_map(|path| path.parts())
            .map(|name| name.as_ref().to_string())
            .filter(|name| name != PARSEABLE_ROOT_DIRECTORY && name != USERS_ROOT_DIR)
            .collect::<Vec<_>>();

        for stream in streams {
            let stream_path =
                object_store::path::Path::from(format!("{}/{}", &stream, STREAM_ROOT_DIRECTORY));
            let resp = self.client.list_with_delimiter(Some(&stream_path)).await?;
            if resp
                .objects
                .iter()
                .any(|name| name.location.filename().unwrap().ends_with("stream.json"))
            {
                result_file_list.push(LogStream { name: stream });
            }
        }

        Ok(result_file_list)
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

    async fn _list_manifest_files(
        &self,
        stream: &str,
    ) -> Result<BTreeMap<String, Vec<String>>, ObjectStorageError> {
        let mut result_file_list: BTreeMap<String, Vec<String>> = BTreeMap::new();
        let resp = self
            .client
            .list_with_delimiter(Some(&(stream.into())))
            .await?;

        let dates = resp
            .common_prefixes
            .iter()
            .flat_map(|path| path.parts())
            .filter(|name| name.as_ref() != stream && name.as_ref() != STREAM_ROOT_DIRECTORY)
            .map(|name| name.as_ref().to_string())
            .collect::<Vec<_>>();
        for date in dates {
            let date_path = object_store::path::Path::from(format!("{}/{}", stream, &date));
            let resp = self.client.list_with_delimiter(Some(&date_path)).await?;
            let manifests: Vec<String> = resp
                .objects
                .iter()
                .filter(|name| name.location.filename().unwrap().ends_with("manifest.json"))
                .map(|name| name.location.to_string())
                .collect();
            result_file_list.entry(date).or_default().extend(manifests);
        }
        Ok(result_file_list)
    }
    async fn _upload_file(&self, key: &str, path: &StdPath) -> Result<(), ObjectStorageError> {
        let instant = Instant::now();

        // // TODO: Uncomment this when multipart is fixed
        // let should_multipart = std::fs::metadata(path)?.len() > MULTIPART_UPLOAD_SIZE as u64;

        let should_multipart = false;

        let res = if should_multipart {
            // self._upload_multipart(key, path).await
            // this branch will never get executed
            Ok(())
        } else {
            let bytes = tokio::fs::read(path).await?;
            let result = self.client.put(&key.into(), bytes.into()).await?;
            info!("Uploaded file to S3: {:?}", result);
            Ok(())
        };

        let status = if res.is_ok() { "200" } else { "400" };
        let time = instant.elapsed().as_secs_f64();
        REQUEST_RESPONSE_TIME
            .with_label_values(&["UPLOAD_PARQUET", status])
            .observe(time);

        res
    }

    // TODO: introduce parallel, multipart-uploads if required
    // async fn _upload_multipart(&self, key: &str, path: &StdPath) -> Result<(), ObjectStorageError> {
    //     let mut buf = vec![0u8; MULTIPART_UPLOAD_SIZE / 2];
    //     let mut file = OpenOptions::new().read(true).open(path).await?;

    //     // let (multipart_id, mut async_writer) = self.client.put_multipart(&key.into()).await?;
    //     let mut async_writer = self.client.put_multipart(&key.into()).await?;

    //     /* `abort_multipart()` has been removed */
    //     // let close_multipart = |err| async move {
    //     //     error!("multipart upload failed. {:?}", err);
    //     //     self.client
    //     //         .abort_multipart(&key.into(), &multipart_id)
    //     //         .await
    //     // };

    //     loop {
    //         match file.read(&mut buf).await {
    //             Ok(len) => {
    //                 if len == 0 {
    //                     break;
    //                 }
    //                 if let Err(err) = async_writer.write_all(&buf[0..len]).await {
    //                     // close_multipart(err).await?;
    //                     break;
    //                 }
    //                 if let Err(err) = async_writer.flush().await {
    //                     // close_multipart(err).await?;
    //                     break;
    //                 }
    //             }
    //             Err(err) => {
    //                 // close_multipart(err).await?;
    //                 break;
    //             }
    //         }
    //     }

    //     async_writer.shutdown().await?;

    //     Ok(())
    // }
}

#[async_trait]
impl ObjectStorage for S3 {
    async fn get_object(&self, path: &RelativePath) -> Result<Bytes, ObjectStorageError> {
        Ok(self._get_object(path).await?)
    }

    async fn get_objects(
        &self,
        base_path: Option<&RelativePath>,
        filter_func: Box<dyn Fn(String) -> bool + Send>,
    ) -> Result<Vec<Bytes>, ObjectStorageError> {
        let instant = Instant::now();

        let prefix = if let Some(base_path) = base_path {
            to_object_store_path(base_path)
        } else {
            self.root.clone()
        };

        let mut list_stream = self.client.list(Some(&prefix));

        let mut res = vec![];

        while let Some(meta) = list_stream.next().await.transpose()? {
            let ingestor_file = filter_func(meta.location.filename().unwrap().to_string());

            if !ingestor_file {
                continue;
            }

            let byts = self
                .get_object(
                    RelativePath::from_path(meta.location.as_ref())
                        .map_err(ObjectStorageError::PathError)?,
                )
                .await?;

            res.push(byts);
        }

        let instant = instant.elapsed().as_secs_f64();
        REQUEST_RESPONSE_TIME
            .with_label_values(&["GET", "200"])
            .observe(instant);

        Ok(res)
    }

    async fn get_ingestor_meta_file_paths(
        &self,
    ) -> Result<Vec<RelativePathBuf>, ObjectStorageError> {
        let time = Instant::now();
        let mut path_arr = vec![];
        let mut object_stream = self.client.list(Some(&self.root));

        while let Some(meta) = object_stream.next().await.transpose()? {
            let flag = meta.location.filename().unwrap().starts_with("ingestor");

            if flag {
                path_arr.push(RelativePathBuf::from(meta.location.as_ref()));
            }
        }

        let time = time.elapsed().as_secs_f64();
        REQUEST_RESPONSE_TIME
            .with_label_values(&["GET", "200"])
            .observe(time);

        Ok(path_arr)
    }

    async fn get_stream_file_paths(
        &self,
        stream_name: &str,
    ) -> Result<Vec<RelativePathBuf>, ObjectStorageError> {
        let time = Instant::now();
        let mut path_arr = vec![];
        let path = to_object_store_path(&RelativePathBuf::from(stream_name));
        let mut object_stream = self.client.list(Some(&path));

        while let Some(meta) = object_stream.next().await.transpose()? {
            let flag = meta.location.filename().unwrap().starts_with(".ingestor");

            if flag {
                path_arr.push(RelativePathBuf::from(meta.location.as_ref()));
            }
        }

        path_arr.push(RelativePathBuf::from_iter([
            stream_name,
            STREAM_METADATA_FILE_NAME,
        ]));
        path_arr.push(RelativePathBuf::from_iter([stream_name, SCHEMA_FILE_NAME]));

        let time = time.elapsed().as_secs_f64();
        REQUEST_RESPONSE_TIME
            .with_label_values(&["GET", "200"])
            .observe(time);

        Ok(path_arr)
    }

    async fn put_object(
        &self,
        path: &RelativePath,
        resource: Bytes,
    ) -> Result<(), ObjectStorageError> {
        self._put_object(path, resource.into())
            .await
            .map_err(|err| ObjectStorageError::ConnectionError(Box::new(err)))?;

        Ok(())
    }

    async fn delete_prefix(&self, path: &RelativePath) -> Result<(), ObjectStorageError> {
        self._delete_prefix(path.as_ref()).await?;

        Ok(())
    }

    async fn delete_object(&self, path: &RelativePath) -> Result<(), ObjectStorageError> {
        Ok(self.client.delete(&to_object_store_path(path)).await?)
    }

    async fn check(&self) -> Result<(), ObjectStorageError> {
        Ok(self
            .client
            .head(&to_object_store_path(&parseable_json_path()))
            .await
            .map(|_| ())?)
    }

    async fn delete_stream(&self, stream_name: &str) -> Result<(), ObjectStorageError> {
        self._delete_prefix(stream_name).await?;

        Ok(())
    }

    async fn try_delete_ingestor_meta(
        &self,
        ingestor_filename: String,
    ) -> Result<(), ObjectStorageError> {
        let file = RelativePathBuf::from(&ingestor_filename);
        match self.client.delete(&to_object_store_path(&file)).await {
            Ok(_) => Ok(()),
            Err(err) => {
                // if the object is not found, it is not an error
                // the given url path was incorrect
                if matches!(err, object_store::Error::NotFound { .. }) {
                    error!("Node does not exist");
                    Err(err.into())
                } else {
                    error!("Error deleting ingestor meta file: {:?}", err);
                    Err(err.into())
                }
            }
        }
    }

    async fn list_streams(&self) -> Result<Vec<LogStream>, ObjectStorageError> {
        let streams = self._list_streams().await?;

        Ok(streams)
    }

    async fn list_old_streams(&self) -> Result<Vec<LogStream>, ObjectStorageError> {
        let resp = self.client.list_with_delimiter(None).await?;

        let common_prefixes = resp.common_prefixes; // get all dirs

        // return prefixes at the root level
        let dirs: Vec<_> = common_prefixes
            .iter()
            .filter_map(|path| path.parts().next())
            .map(|name| name.as_ref().to_string())
            .filter(|x| x != PARSEABLE_ROOT_DIRECTORY)
            .collect();

        let stream_json_check = FuturesUnordered::new();

        for dir in &dirs {
            let key = format!("{}/{}", dir, STREAM_METADATA_FILE_NAME);
            let task = async move { self.client.head(&StorePath::from(key)).await.map(|_| ()) };
            stream_json_check.push(task);
        }

        stream_json_check.try_collect::<()>().await?;

        Ok(dirs.into_iter().map(|name| LogStream { name }).collect())
    }

    async fn list_dates(&self, stream_name: &str) -> Result<Vec<String>, ObjectStorageError> {
        let streams = self._list_dates(stream_name).await?;

        Ok(streams)
    }

    async fn list_manifest_files(
        &self,
        stream_name: &str,
    ) -> Result<BTreeMap<String, Vec<String>>, ObjectStorageError> {
        let files = self._list_manifest_files(stream_name).await?;

        Ok(files)
    }

    async fn upload_file(&self, key: &str, path: &StdPath) -> Result<(), ObjectStorageError> {
        self._upload_file(key, path).await?;

        Ok(())
    }

    fn absolute_url(&self, prefix: &RelativePath) -> object_store::path::Path {
        object_store::path::Path::parse(prefix).unwrap()
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

    fn store_url(&self) -> url::Url {
        url::Url::parse(&format!("s3://{}", self.bucket)).unwrap()
    }

    async fn list_dirs(&self) -> Result<Vec<String>, ObjectStorageError> {
        let pre = object_store::path::Path::from("/");
        let resp = self.client.list_with_delimiter(Some(&pre)).await?;

        Ok(resp
            .common_prefixes
            .iter()
            .flat_map(|path| path.parts())
            .map(|name| name.as_ref().to_string())
            .collect::<Vec<_>>())
    }

    async fn get_all_dashboards(
        &self,
    ) -> Result<HashMap<RelativePathBuf, Vec<Bytes>>, ObjectStorageError> {
        let mut dashboards: HashMap<RelativePathBuf, Vec<Bytes>> = HashMap::new();
        let users_root_path = object_store::path::Path::from(USERS_ROOT_DIR);
        let resp = self
            .client
            .list_with_delimiter(Some(&users_root_path))
            .await?;

        let users = resp
            .common_prefixes
            .iter()
            .flat_map(|path| path.parts())
            .filter(|name| name.as_ref() != USERS_ROOT_DIR)
            .map(|name| name.as_ref().to_string())
            .collect::<Vec<_>>();
        for user in users {
            let user_dashboard_path =
                object_store::path::Path::from(format!("{USERS_ROOT_DIR}/{user}/dashboards"));
            let dashboards_path = RelativePathBuf::from(&user_dashboard_path);
            let dashboard_bytes = self
                .get_objects(
                    Some(&dashboards_path),
                    Box::new(|file_name| file_name.ends_with(".json")),
                )
                .await?;

            dashboards
                .entry(dashboards_path)
                .or_default()
                .extend(dashboard_bytes);
        }
        Ok(dashboards)
    }

    async fn get_all_saved_filters(
        &self,
    ) -> Result<HashMap<RelativePathBuf, Vec<Bytes>>, ObjectStorageError> {
        let mut filters: HashMap<RelativePathBuf, Vec<Bytes>> = HashMap::new();
        let users_root_path = object_store::path::Path::from(USERS_ROOT_DIR);
        let resp = self
            .client
            .list_with_delimiter(Some(&users_root_path))
            .await?;

        let users = resp
            .common_prefixes
            .iter()
            .flat_map(|path| path.parts())
            .filter(|name| name.as_ref() != USERS_ROOT_DIR)
            .map(|name| name.as_ref().to_string())
            .collect::<Vec<_>>();
        for user in users {
            let user_filters_path =
                object_store::path::Path::from(format!("{USERS_ROOT_DIR}/{user}/filters",));
            let resp = self
                .client
                .list_with_delimiter(Some(&user_filters_path))
                .await?;
            let streams = resp
                .common_prefixes
                .iter()
                .filter(|name| name.as_ref() != USERS_ROOT_DIR)
                .map(|name| name.as_ref().to_string())
                .collect::<Vec<_>>();
            for stream in streams {
                let filters_path = RelativePathBuf::from(&stream);
                let filter_bytes = self
                    .get_objects(
                        Some(&filters_path),
                        Box::new(|file_name| file_name.ends_with(".json")),
                    )
                    .await?;
                filters
                    .entry(filters_path)
                    .or_default()
                    .extend(filter_bytes);
            }
        }
        Ok(filters)
    }

    ///fetch all correlations stored in object store
    /// return the correlation file path and all correlation json bytes for each file path
    async fn get_all_correlations(
        &self,
    ) -> Result<HashMap<RelativePathBuf, Vec<Bytes>>, ObjectStorageError> {
        let mut correlations: HashMap<RelativePathBuf, Vec<Bytes>> = HashMap::new();
        let users_root_path = object_store::path::Path::from(USERS_ROOT_DIR);
        let resp = self
            .client
            .list_with_delimiter(Some(&users_root_path))
            .await?;

        let users = resp
            .common_prefixes
            .iter()
            .flat_map(|path| path.parts())
            .filter(|name| name.as_ref() != USERS_ROOT_DIR)
            .map(|name| name.as_ref().to_string())
            .collect::<Vec<_>>();
        for user in users {
            let user_correlation_path =
                object_store::path::Path::from(format!("{USERS_ROOT_DIR}/{user}/correlations",));
            let correlations_path = RelativePathBuf::from(&user_correlation_path);
            let correlation_bytes = self
                .get_objects(
                    Some(&correlations_path),
                    Box::new(|file_name| file_name.ends_with(".json")),
                )
                .await?;

            correlations
                .entry(correlations_path)
                .or_default()
                .extend(correlation_bytes);
        }
        Ok(correlations)
    }

    fn get_bucket_name(&self) -> String {
        self.bucket.clone()
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
