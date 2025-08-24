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

use std::{
    collections::HashSet,
    fmt::Display,
    path::Path,
    str::FromStr,
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
    time::{Duration, Instant},
};

use async_trait::async_trait;
use bytes::Bytes;
use datafusion::{
    datasource::listing::ListingTableUrl,
    execution::{
        object_store::{DefaultObjectStoreRegistry, ObjectStoreRegistry, ObjectStoreUrl},
        runtime_env::RuntimeEnvBuilder,
    },
};
use futures::{StreamExt, TryStreamExt, stream::FuturesUnordered};
use object_store::{
    BackoffConfig, ClientOptions, ListResult, ObjectMeta, ObjectStore, PutPayload, RetryConfig,
    aws::{AmazonS3, AmazonS3Builder, AmazonS3ConfigKey, Checksum},
    buffered::BufReader,
    limit::LimitStore,
    path::Path as StorePath,
};
use relative_path::{RelativePath, RelativePathBuf};
use tokio::{fs::OpenOptions, io::AsyncReadExt};
use tracing::{error, info};

use crate::{
    metrics::storage::{STORAGE_FILES_SCANNED, STORAGE_REQUEST_RESPONSE_TIME, StorageMetrics},
    parseable::LogStream,
};

use super::{
    CONNECT_TIMEOUT_SECS, MIN_MULTIPART_UPLOAD_SIZE, ObjectStorage, ObjectStorageError,
    ObjectStorageProvider, PARSEABLE_ROOT_DIRECTORY, REQUEST_TIMEOUT_SECS,
    STREAM_METADATA_FILE_NAME, metrics_layer::MetricLayer, metrics_layer::error_to_status_code,
    object_storage::parseable_json_path, to_object_store_path,
};

// in bytes
// const MULTIPART_UPLOAD_SIZE: usize = 1024 * 1024 * 100;
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
    fn name(&self) -> &'static str {
        "s3"
    }

    fn get_datafusion_runtime(&self) -> RuntimeEnvBuilder {
        let s3 = self.get_default_builder().build().unwrap();

        // limit objectstore to a concurrent request limit
        let s3 = LimitStore::new(s3, super::MAX_OBJECT_STORE_REQUESTS);
        let s3 = MetricLayer::new(s3, "s3");

        let object_store_registry = DefaultObjectStoreRegistry::new();
        let url = ObjectStoreUrl::parse(format!("s3://{}", &self.bucket_name)).unwrap();
        object_store_registry.register_store(url.as_ref(), Arc::new(s3));

        RuntimeEnvBuilder::new().with_object_store_registry(Arc::new(object_store_registry))
    }

    fn construct_client(&self) -> Arc<dyn ObjectStorage> {
        let s3 = self.get_default_builder().build().unwrap();

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

#[derive(Debug)]
pub struct S3 {
    client: AmazonS3,
    bucket: String,
    root: StorePath,
}

impl S3 {
    async fn _get_object(&self, path: &RelativePath) -> Result<Bytes, ObjectStorageError> {
        let time = std::time::Instant::now();
        let resp = self.client.get(&to_object_store_path(path)).await;
        let elapsed = time.elapsed().as_secs_f64();
        STORAGE_FILES_SCANNED
            .with_label_values(&["s3", "GET"])
            .inc();
        match resp {
            Ok(resp) => {
                let body = resp.bytes().await?;
                STORAGE_REQUEST_RESPONSE_TIME
                    .with_label_values(&["s3", "GET", "200"])
                    .observe(elapsed);
                Ok(body)
            }
            Err(err) => {
                let status_code = error_to_status_code(&err);
                STORAGE_REQUEST_RESPONSE_TIME
                    .with_label_values(&["s3", "GET", status_code])
                    .observe(elapsed);
                Err(err.into())
            }
        }
    }

    async fn _put_object(
        &self,
        path: &RelativePath,
        resource: PutPayload,
    ) -> Result<(), ObjectStorageError> {
        let time = std::time::Instant::now();
        let resp = self.client.put(&to_object_store_path(path), resource).await;
        let elapsed = time.elapsed().as_secs_f64();
        STORAGE_FILES_SCANNED
            .with_label_values(&["s3", "PUT"])
            .inc();
        match resp {
            Ok(_) => {
                STORAGE_REQUEST_RESPONSE_TIME
                    .with_label_values(&["s3", "PUT", "200"])
                    .observe(elapsed);
                Ok(())
            }
            Err(err) => {
                let status_code = error_to_status_code(&err);
                STORAGE_REQUEST_RESPONSE_TIME
                    .with_label_values(&["s3", "PUT", status_code])
                    .observe(elapsed);
                Err(err.into())
            }
        }
    }

    async fn _delete_prefix(&self, key: &str) -> Result<(), ObjectStorageError> {
        let files_scanned = Arc::new(AtomicU64::new(0));
        let files_deleted = Arc::new(AtomicU64::new(0));
        // Track LIST operation
        let list_start = Instant::now();
        let object_stream = self.client.list(Some(&(key.into())));
        let list_elapsed = list_start.elapsed().as_secs_f64();
        STORAGE_REQUEST_RESPONSE_TIME
            .with_label_values(&["s3", "LIST", "200"])
            .observe(list_elapsed);

        object_stream
            .for_each_concurrent(None, |x| async {
                files_scanned.fetch_add(1, Ordering::Relaxed);
                match x {
                    Ok(obj) => {
                        files_deleted.fetch_add(1, Ordering::Relaxed);
                        let delete_start = Instant::now();
                        let delete_resp = self.client.delete(&obj.location).await;
                        let delete_elapsed = delete_start.elapsed().as_secs_f64();
                        match delete_resp {
                            Ok(_) => {
                                STORAGE_REQUEST_RESPONSE_TIME
                                    .with_label_values(&["s3", "DELETE", "200"])
                                    .observe(delete_elapsed);
                            }
                            Err(err) => {
                                let status_code = error_to_status_code(&err);
                                STORAGE_REQUEST_RESPONSE_TIME
                                    .with_label_values(&["s3", "DELETE", status_code])
                                    .observe(delete_elapsed);
                                error!("Failed to delete object during delete stream: {:?}", err);
                            }
                        }
                    }
                    Err(err) => {
                        error!("Failed to fetch object during delete stream: {:?}", err);
                    }
                };
            })
            .await;

        STORAGE_FILES_SCANNED
            .with_label_values(&["s3", "LIST"])
            .inc_by(files_scanned.load(Ordering::Relaxed) as f64);
        STORAGE_FILES_SCANNED
            .with_label_values(&["s3", "DELETE"])
            .inc_by(files_deleted.load(Ordering::Relaxed) as f64);
        Ok(())
    }

    async fn _list_dates(&self, stream: &str) -> Result<Vec<String>, ObjectStorageError> {
        let list_start = Instant::now();
        let resp: Result<object_store::ListResult, object_store::Error> = self
            .client
            .list_with_delimiter(Some(&(stream.into())))
            .await;
        let list_elapsed = list_start.elapsed().as_secs_f64();

        let resp = match resp {
            Ok(resp) => {
                STORAGE_REQUEST_RESPONSE_TIME
                    .with_label_values(&["s3", "LIST", "200"])
                    .observe(list_elapsed);
                resp
            }
            Err(err) => {
                let status_code = error_to_status_code(&err);
                STORAGE_REQUEST_RESPONSE_TIME
                    .with_label_values(&["s3", "LIST", status_code])
                    .observe(list_elapsed);
                return Err(err.into());
            }
        };

        let common_prefixes = resp.common_prefixes;

        STORAGE_FILES_SCANNED
            .with_label_values(&["s3", "LIST"])
            .inc_by(common_prefixes.len() as f64);

        // return prefixes at the root level
        let dates: Vec<_> = common_prefixes
            .iter()
            .filter_map(|path| path.as_ref().strip_prefix(&format!("{stream}/")))
            .map(String::from)
            .collect();

        Ok(dates)
    }

    async fn _upload_file(&self, key: &str, path: &Path) -> Result<(), ObjectStorageError> {
        let bytes = tokio::fs::read(path).await?;

        let put_start = Instant::now();
        let result = self.client.put(&key.into(), bytes.into()).await;
        let put_elapsed = put_start.elapsed().as_secs_f64();
        STORAGE_FILES_SCANNED
            .with_label_values(&["s3", "PUT"])
            .inc();
        match result {
            Ok(result) => {
                STORAGE_REQUEST_RESPONSE_TIME
                    .with_label_values(&["s3", "PUT", "200"])
                    .observe(put_elapsed);
                info!("Uploaded file to S3: {:?}", result);
                Ok(())
            }
            Err(err) => {
                let status_code = error_to_status_code(&err);
                STORAGE_REQUEST_RESPONSE_TIME
                    .with_label_values(&["s3", "PUT", status_code])
                    .observe(put_elapsed);
                Err(err.into())
            }
        }
    }

    async fn _upload_multipart(
        &self,
        key: &RelativePath,
        path: &Path,
    ) -> Result<(), ObjectStorageError> {
        let mut file = OpenOptions::new().read(true).open(path).await?;
        let location = &to_object_store_path(key);

        // Track multipart initiation
        let multipart_start = Instant::now();
        let async_writer = self.client.put_multipart(location).await;
        let multipart_elapsed = multipart_start.elapsed().as_secs_f64();

        let mut async_writer = match async_writer {
            Ok(writer) => {
                STORAGE_REQUEST_RESPONSE_TIME
                    .with_label_values(&["s3", "PUT_MULTIPART", "200"])
                    .observe(multipart_elapsed);
                writer
            }
            Err(err) => {
                let status_code = error_to_status_code(&err);
                STORAGE_REQUEST_RESPONSE_TIME
                    .with_label_values(&["s3", "PUT_MULTIPART", status_code])
                    .observe(multipart_elapsed);
                return Err(err.into());
            }
        };

        let meta = file.metadata().await?;
        let total_size = meta.len() as usize;
        if total_size < MIN_MULTIPART_UPLOAD_SIZE {
            let mut data = Vec::new();
            file.read_to_end(&mut data).await?;

            // Track single PUT operation for small files
            let put_start = Instant::now();
            let result = self.client.put(location, data.into()).await;
            let put_elapsed = put_start.elapsed().as_secs_f64();
            STORAGE_FILES_SCANNED
                .with_label_values(&["s3", "PUT"])
                .inc();
            match result {
                Ok(_) => {
                    STORAGE_REQUEST_RESPONSE_TIME
                        .with_label_values(&["s3", "PUT", "200"])
                        .observe(put_elapsed);
                }
                Err(err) => {
                    let status_code = error_to_status_code(&err);
                    STORAGE_REQUEST_RESPONSE_TIME
                        .with_label_values(&["s3", "PUT", status_code])
                        .observe(put_elapsed);
                    return Err(err.into());
                }
            }

            // async_writer.put_part(data.into()).await?;
            // async_writer.complete().await?;
            return Ok(());
        } else {
            let mut data = Vec::new();
            file.read_to_end(&mut data).await?;

            // let mut upload_parts = Vec::new();

            let has_final_partial_part = !total_size.is_multiple_of(MIN_MULTIPART_UPLOAD_SIZE);
            let num_full_parts = total_size / MIN_MULTIPART_UPLOAD_SIZE;
            let total_parts = num_full_parts + if has_final_partial_part { 1 } else { 0 };

            // Upload each part with metrics
            for part_number in 0..(total_parts) {
                let start_pos = part_number * MIN_MULTIPART_UPLOAD_SIZE;
                let end_pos = if part_number == num_full_parts && has_final_partial_part {
                    // Last part might be smaller than 5MB (which is allowed)
                    total_size
                } else {
                    // All other parts must be at least 5MB
                    start_pos + MIN_MULTIPART_UPLOAD_SIZE
                };

                // Extract this part's data
                let part_data = data[start_pos..end_pos].to_vec();

                // Track individual part upload
                let part_start = Instant::now();
                let result = async_writer.put_part(part_data.into()).await;
                let part_elapsed = part_start.elapsed().as_secs_f64();

                match result {
                    Ok(_) => {
                        STORAGE_REQUEST_RESPONSE_TIME
                            .with_label_values(&["s3", "PUT_MULTIPART_PART", "200"])
                            .observe(part_elapsed);
                    }
                    Err(err) => {
                        let status_code = error_to_status_code(&err);
                        STORAGE_REQUEST_RESPONSE_TIME
                            .with_label_values(&["s3", "PUT_MULTIPART_PART", status_code])
                            .observe(part_elapsed);
                        return Err(err.into());
                    }
                }

                // upload_parts.push(part_number as u64 + 1);
            }

            // Track multipart completion
            let complete_start = Instant::now();
            let complete_result = async_writer.complete().await;
            let complete_elapsed = complete_start.elapsed().as_secs_f64();

            if let Err(err) = complete_result {
                let status_code = error_to_status_code(&err);
                STORAGE_REQUEST_RESPONSE_TIME
                    .with_label_values(&["s3", "PUT_MULTIPART_COMPLETE", status_code])
                    .observe(complete_elapsed);
                error!("Failed to complete multipart upload. {:?}", err);
                async_writer.abort().await?;
                return Err(err.into());
            } else {
                STORAGE_REQUEST_RESPONSE_TIME
                    .with_label_values(&["s3", "PUT_MULTIPART_COMPLETE", "200"])
                    .observe(complete_elapsed);
            }
        }
        Ok(())
    }
}

#[async_trait]
impl ObjectStorage for S3 {
    async fn get_buffered_reader(
        &self,
        path: &RelativePath,
    ) -> Result<BufReader, ObjectStorageError> {
        let path = &to_object_store_path(path);

        let head_start = Instant::now();
        let meta = self.client.head(path).await;
        let head_elapsed = head_start.elapsed().as_secs_f64();
        STORAGE_FILES_SCANNED
            .with_label_values(&["s3", "HEAD"])
            .inc();
        let meta = match meta {
            Ok(meta) => {
                STORAGE_REQUEST_RESPONSE_TIME
                    .with_label_values(&["s3", "HEAD", "200"])
                    .observe(head_elapsed);
                meta
            }
            Err(err) => {
                let status_code = error_to_status_code(&err);
                STORAGE_REQUEST_RESPONSE_TIME
                    .with_label_values(&["s3", "HEAD", status_code])
                    .observe(head_elapsed);
                return Err(err.into());
            }
        };

        let store: Arc<dyn ObjectStore> = Arc::new(self.client.clone());
        let buf = object_store::buffered::BufReader::new(store, &meta);
        Ok(buf)
    }

    async fn upload_multipart(
        &self,
        key: &RelativePath,
        path: &Path,
    ) -> Result<(), ObjectStorageError> {
        self._upload_multipart(key, path).await
    }

    async fn head(&self, path: &RelativePath) -> Result<ObjectMeta, ObjectStorageError> {
        let head_start = Instant::now();
        let result = self.client.head(&to_object_store_path(path)).await;
        let head_elapsed = head_start.elapsed().as_secs_f64();
        STORAGE_FILES_SCANNED
            .with_label_values(&["s3", "HEAD"])
            .inc();
        match &result {
            Ok(_) => {
                STORAGE_REQUEST_RESPONSE_TIME
                    .with_label_values(&["s3", "HEAD", "200"])
                    .observe(head_elapsed);
                // Record single file accessed
                STORAGE_FILES_SCANNED
                    .with_label_values(&["s3", "HEAD"])
                    .inc();
            }
            Err(err) => {
                let status_code = error_to_status_code(err);
                STORAGE_REQUEST_RESPONSE_TIME
                    .with_label_values(&["s3", "HEAD", status_code])
                    .observe(head_elapsed);
            }
        }

        Ok(result?)
    }

    async fn get_object(&self, path: &RelativePath) -> Result<Bytes, ObjectStorageError> {
        Ok(self._get_object(path).await?)
    }

    async fn get_objects(
        &self,
        base_path: Option<&RelativePath>,
        filter_func: Box<dyn Fn(String) -> bool + Send>,
    ) -> Result<Vec<Bytes>, ObjectStorageError> {
        let prefix = if let Some(base_path) = base_path {
            to_object_store_path(base_path)
        } else {
            self.root.clone()
        };

        // Track list operation
        let list_start = Instant::now();
        let mut list_stream = self.client.list(Some(&prefix));

        let mut res = vec![];
        let mut files_scanned = 0;

        // Note: We track each streaming list item retrieval
        while let Some(meta_result) = list_stream.next().await {
            let meta = match meta_result {
                Ok(meta) => meta,
                Err(err) => {
                    return Err(err.into());
                }
            };

            files_scanned += 1;
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
            STORAGE_REQUEST_RESPONSE_TIME
                .with_label_values(&["s3", "GET", "200"])
                .observe(list_start.elapsed().as_secs_f64());
            STORAGE_FILES_SCANNED
                .with_label_values(&["s3", "GET"])
                .inc();

            res.push(byts);
        }
        let list_elapsed = list_start.elapsed().as_secs_f64();
        STORAGE_REQUEST_RESPONSE_TIME
            .with_label_values(&["s3", "LIST", "200"])
            .observe(list_elapsed);

        // Record total files scanned
        STORAGE_FILES_SCANNED
            .with_label_values(&["s3", "LIST"])
            .inc_by(files_scanned as f64);

        Ok(res)
    }

    async fn get_ingestor_meta_file_paths(
        &self,
    ) -> Result<Vec<RelativePathBuf>, ObjectStorageError> {
        let mut path_arr = vec![];
        let mut files_scanned = 0;

        // Track list operation
        let list_start = Instant::now();
        let mut object_stream = self.client.list(Some(&self.root));

        while let Some(meta_result) = object_stream.next().await {
            let meta = match meta_result {
                Ok(meta) => meta,
                Err(err) => {
                    return Err(err.into());
                }
            };

            files_scanned += 1;
            let flag = meta.location.filename().unwrap().starts_with("ingestor");

            if flag {
                path_arr.push(RelativePathBuf::from(meta.location.as_ref()));
            }
        }
        let list_elapsed = list_start.elapsed().as_secs_f64();
        STORAGE_REQUEST_RESPONSE_TIME
            .with_label_values(&["s3", "LIST", "200"])
            .observe(list_elapsed);
        // Record total files scanned
        STORAGE_FILES_SCANNED
            .with_label_values(&["s3", "LIST"])
            .inc_by(files_scanned as f64);

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
        let delete_start = Instant::now();
        let result = self.client.delete(&to_object_store_path(path)).await;
        let delete_elapsed = delete_start.elapsed().as_secs_f64();

        match &result {
            Ok(_) => {
                STORAGE_REQUEST_RESPONSE_TIME
                    .with_label_values(&["s3", "DELETE", "200"])
                    .observe(delete_elapsed);
                // Record single file deleted
                STORAGE_FILES_SCANNED
                    .with_label_values(&["s3", "DELETE"])
                    .inc();
            }
            Err(err) => {
                let status_code = error_to_status_code(err);
                STORAGE_REQUEST_RESPONSE_TIME
                    .with_label_values(&["s3", "DELETE", status_code])
                    .observe(delete_elapsed);
            }
        }

        Ok(result?)
    }

    async fn check(&self) -> Result<(), ObjectStorageError> {
        let head_start = Instant::now();
        let result = self
            .client
            .head(&to_object_store_path(&parseable_json_path()))
            .await;
        let head_elapsed = head_start.elapsed().as_secs_f64();

        match &result {
            Ok(_) => {
                STORAGE_REQUEST_RESPONSE_TIME
                    .with_label_values(&["s3", "HEAD", "200"])
                    .observe(head_elapsed);
            }
            Err(err) => {
                let status_code = error_to_status_code(err);
                STORAGE_REQUEST_RESPONSE_TIME
                    .with_label_values(&["s3", "HEAD", status_code])
                    .observe(head_elapsed);
            }
        }
        STORAGE_FILES_SCANNED
            .with_label_values(&["s3", "HEAD"])
            .inc();

        Ok(result.map(|_| ())?)
    }

    async fn delete_stream(&self, stream_name: &str) -> Result<(), ObjectStorageError> {
        self._delete_prefix(stream_name).await?;

        Ok(())
    }

    async fn try_delete_node_meta(&self, node_filename: String) -> Result<(), ObjectStorageError> {
        let file = RelativePathBuf::from(&node_filename);

        let delete_start = Instant::now();
        let result = self.client.delete(&to_object_store_path(&file)).await;
        let delete_elapsed = delete_start.elapsed().as_secs_f64();
        STORAGE_FILES_SCANNED
            .with_label_values(&["s3", "DELETE"])
            .inc();
        match result {
            Ok(_) => {
                STORAGE_REQUEST_RESPONSE_TIME
                    .with_label_values(&["s3", "DELETE", "200"])
                    .observe(delete_elapsed);
                Ok(())
            }
            Err(err) => {
                let status_code = error_to_status_code(&err);
                STORAGE_REQUEST_RESPONSE_TIME
                    .with_label_values(&["s3", "DELETE", status_code])
                    .observe(delete_elapsed);

                Err(err.into())
            }
        }
    }

    async fn list_streams(&self) -> Result<HashSet<LogStream>, ObjectStorageError> {
        // self._list_streams().await
        Err(ObjectStorageError::Custom(
            "S3 doesn't implement list_streams".into(),
        ))
    }

    async fn list_old_streams(&self) -> Result<HashSet<LogStream>, ObjectStorageError> {
        // Track LIST operation
        let list_start = Instant::now();
        let resp = self.client.list_with_delimiter(None).await?;
        let list_elapsed = list_start.elapsed().as_secs_f64();
        STORAGE_REQUEST_RESPONSE_TIME
            .with_label_values(&["s3", "LIST", "200"])
            .observe(list_elapsed);

        let common_prefixes = resp.common_prefixes; // get all dirs

        // return prefixes at the root level
        let dirs: HashSet<_> = common_prefixes
            .iter()
            .filter_map(|path| path.parts().next())
            .map(|name| name.as_ref().to_string())
            .filter(|x| x != PARSEABLE_ROOT_DIRECTORY)
            .collect();

        let stream_json_check = FuturesUnordered::new();

        for dir in &dirs {
            let key = format!("{dir}/{STREAM_METADATA_FILE_NAME}");
            let task = async move {
                let head_start = Instant::now();
                let result = self.client.head(&StorePath::from(key)).await;
                let head_elapsed = head_start.elapsed().as_secs_f64();

                match &result {
                    Ok(_) => {
                        STORAGE_REQUEST_RESPONSE_TIME
                            .with_label_values(&["s3", "HEAD", "200"])
                            .observe(head_elapsed);
                    }
                    Err(err) => {
                        let status_code = error_to_status_code(err);
                        STORAGE_REQUEST_RESPONSE_TIME
                            .with_label_values(&["s3", "HEAD", status_code])
                            .observe(head_elapsed);
                    }
                }

                result.map(|_| ())
            };
            stream_json_check.push(task);
        }

        stream_json_check.try_collect::<()>().await?;

        Ok(dirs)
    }

    async fn list_dates(&self, stream_name: &str) -> Result<Vec<String>, ObjectStorageError> {
        let streams = self._list_dates(stream_name).await?;

        Ok(streams)
    }

    async fn list_hours(
        &self,
        stream_name: &str,
        date: &str,
    ) -> Result<Vec<String>, ObjectStorageError> {
        let pre = object_store::path::Path::from(format!("{}/{}/", stream_name, date));
        let list_start = Instant::now();
        let resp = self.client.list_with_delimiter(Some(&pre)).await?;
        let list_elapsed = list_start.elapsed().as_secs_f64();
        STORAGE_REQUEST_RESPONSE_TIME
            .with_label_values(&["s3", "LIST", "200"])
            .observe(list_elapsed);

        let hours: Vec<String> = resp
            .common_prefixes
            .iter()
            .filter_map(|path| {
                let path_str = path.as_ref();
                if let Some(stripped) = path_str.strip_prefix(&format!("{}/{}/", stream_name, date))
                {
                    // Remove trailing slash if present, otherwise use as is
                    let clean_path = stripped.strip_suffix('/').unwrap_or(stripped);
                    Some(clean_path.to_string())
                } else {
                    None
                }
            })
            .filter(|dir| dir.starts_with("hour="))
            .collect();

        Ok(hours)
    }

    async fn list_minutes(
        &self,
        stream_name: &str,
        date: &str,
        hour: &str,
    ) -> Result<Vec<String>, ObjectStorageError> {
        let pre = object_store::path::Path::from(format!("{}/{}/{}/", stream_name, date, hour));
        let list_start = Instant::now();
        let resp = self.client.list_with_delimiter(Some(&pre)).await?;
        let list_elapsed = list_start.elapsed().as_secs_f64();
        STORAGE_REQUEST_RESPONSE_TIME
            .with_label_values(&["s3", "LIST", "200"])
            .observe(list_elapsed);

        let minutes: Vec<String> = resp
            .common_prefixes
            .iter()
            .filter_map(|path| {
                let path_str = path.as_ref();
                if let Some(stripped) =
                    path_str.strip_prefix(&format!("{}/{}/{}/", stream_name, date, hour))
                {
                    // Remove trailing slash if present, otherwise use as is
                    let clean_path = stripped.strip_suffix('/').unwrap_or(stripped);
                    Some(clean_path.to_string())
                } else {
                    None
                }
            })
            .filter(|dir| dir.starts_with("minute="))
            .collect();

        Ok(minutes)
    }

    // async fn list_manifest_files(
    //     &self,
    //     stream_name: &str,
    // ) -> Result<BTreeMap<String, Vec<String>>, ObjectStorageError> {
    //     let files = self._list_manifest_files(stream_name).await?;

    //     Ok(files)
    // }

    async fn upload_file(&self, key: &str, path: &Path) -> Result<(), ObjectStorageError> {
        Ok(self._upload_file(key, path).await?)
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

        let list_start = Instant::now();
        let resp = self.client.list_with_delimiter(Some(&pre)).await;
        let list_elapsed = list_start.elapsed().as_secs_f64();

        let resp = match resp {
            Ok(resp) => {
                STORAGE_REQUEST_RESPONSE_TIME
                    .with_label_values(&["s3", "LIST", "200"])
                    .observe(list_elapsed);
                resp
            }
            Err(err) => {
                let status_code = error_to_status_code(&err);
                STORAGE_REQUEST_RESPONSE_TIME
                    .with_label_values(&["s3", "LIST", status_code])
                    .observe(list_elapsed);
                return Err(err.into());
            }
        };

        Ok(resp
            .common_prefixes
            .iter()
            .flat_map(|path| path.parts())
            .map(|name| name.as_ref().to_string())
            .collect::<Vec<_>>())
    }

    async fn list_dirs_relative(
        &self,
        relative_path: &RelativePath,
    ) -> Result<Vec<String>, ObjectStorageError> {
        let prefix = object_store::path::Path::from(relative_path.as_str());

        let list_start = Instant::now();
        let resp = self.client.list_with_delimiter(Some(&prefix)).await;
        let list_elapsed = list_start.elapsed().as_secs_f64();

        let resp = match resp {
            Ok(resp) => {
                STORAGE_REQUEST_RESPONSE_TIME
                    .with_label_values(&["s3", "LIST", "200"])
                    .observe(list_elapsed);
                resp
            }
            Err(err) => {
                let status_code = error_to_status_code(&err);
                STORAGE_REQUEST_RESPONSE_TIME
                    .with_label_values(&["s3", "LIST", status_code])
                    .observe(list_elapsed);
                return Err(err.into());
            }
        };

        Ok(resp
            .common_prefixes
            .iter()
            .flat_map(|path| path.parts())
            .map(|name| name.as_ref().to_string())
            .collect::<Vec<_>>())
    }

    fn get_bucket_name(&self) -> String {
        self.bucket.clone()
    }

    async fn list_with_delimiter(
        &self,
        prefix: Option<object_store::path::Path>,
    ) -> Result<ListResult, ObjectStorageError> {
        Ok(self.client.list_with_delimiter(prefix.as_ref()).await?)
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
