/*
 * Parseable Server (C) 2022 - 2025 Parseable, Inc.
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
    num::NonZeroUsize,
    ops::Range,
    path::{Path, PathBuf},
    str::FromStr,
    sync::Arc,
    time::Duration,
};

use async_trait::async_trait;
use bytes::Bytes;
use chrono::Utc;
use datafusion::{
    datasource::listing::ListingTableUrl,
    execution::{
        object_store::{DefaultObjectStoreRegistry, ObjectStoreRegistry, ObjectStoreUrl},
        runtime_env::RuntimeEnvBuilder,
    },
};
use futures::{StreamExt, TryStreamExt, stream::FuturesUnordered};
use object_store::{
    BackoffConfig, ClientOptions, ListResult, MultipartUpload, ObjectMeta, ObjectStore,
    ObjectStoreExt, PutPayload, RetryConfig,
    aws::{AmazonS3, AmazonS3Builder, AmazonS3ConfigKey, Checksum},
    buffered::BufReader,
    limit::LimitStore,
    path::Path as StorePath,
};
use relative_path::{RelativePath, RelativePathBuf};
use tokio::{
    fs::OpenOptions,
    io::{AsyncRead, AsyncReadExt},
};
use tracing::{error, warn};

use crate::{
    metrics::{
        increment_bytes_scanned_in_object_store_calls_by_date,
        increment_files_scanned_in_object_store_calls_by_date,
        increment_object_store_calls_by_date,
        increment_partial_file_scans_in_object_store_calls_by_date,
    },
    parseable::{DEFAULT_TENANT, LogStream, PARSEABLE},
    storage::RETRY_TIMEOUT_SECS,
};

use super::{
    CONNECT_TIMEOUT_SECS, DEFAULT_MAX_OBJECT_STORE_REQUESTS, ObjectStorage, ObjectStorageError,
    ObjectStorageProvider, PARSEABLE_ROOT_DIRECTORY, REQUEST_TIMEOUT_SECS,
    STREAM_METADATA_FILE_NAME,
    metrics_layer::MetricLayer,
    object_storage::{parseable_json_path, stream_data_listing_prefix},
    partial_path, to_object_store_path,
};
use crate::storage::GET_OBJECTS_CONCURRENCY;

// in bytes
// const MULTIPART_UPLOAD_SIZE: usize = 1024 * 1024 * 100;
const AWS_CONTAINER_CREDENTIALS_RELATIVE_URI: &str = "AWS_CONTAINER_CREDENTIALS_RELATIVE_URI";
const AWS_WEB_IDENTITY_TOKEN_FILE: &str = "AWS_WEB_IDENTITY_TOKEN_FILE";
const AWS_ROLE_ARN: &str = "AWS_ROLE_ARN";
const AWS_ROLE_SESSION_NAME: &str = "AWS_ROLE_SESSION_NAME";

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

    /// Add `Cache-Control: no-store` to non-Parquet object uploads.
    #[arg(
        long,
        env = "P_S3_CACHE_CONTROL_NO_STORE",
        value_name = "bool",
        default_value = "false"
    )]
    pub cache_control_no_store: bool,

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

    /// Cap on concurrent in-flight requests to the object store.
    #[arg(
        long,
        env = "P_MAX_OBJECT_STORE_REQUESTS",
        value_name = "number",
        default_value_t = DEFAULT_MAX_OBJECT_STORE_REQUESTS
    )]
    pub max_object_store_requests: NonZeroUsize,
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
            .with_pool_idle_timeout(Duration::from_secs(15))
            .with_pool_max_idle_per_host(128)
            .with_timeout(Duration::from_secs(REQUEST_TIMEOUT_SECS));

        if self.skip_tls {
            client_options = client_options.with_allow_invalid_certificates(true)
        }
        let retry_config = RetryConfig {
            max_retries: 5,
            retry_timeout: Duration::from_secs(RETRY_TIMEOUT_SECS),
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

        assert!(
            self.access_key_id.is_some() == self.secret_key.is_some(),
            "P_S3_ACCESS_KEY and P_S3_SECRET_KEY must be set together"
        );

        if let Some((access_key, secret_key)) =
            self.access_key_id.as_ref().zip(self.secret_key.as_ref())
        {
            builder = builder
                .with_access_key_id(access_key)
                .with_secret_access_key(secret_key);
        } else {
            let token_file = std::env::var(AWS_WEB_IDENTITY_TOKEN_FILE).ok();
            let role_arn = std::env::var(AWS_ROLE_ARN).ok();

            assert!(
                token_file.is_some() == role_arn.is_some(),
                "{AWS_WEB_IDENTITY_TOKEN_FILE} and {AWS_ROLE_ARN} must be set together"
            );

            if let Some((token_file, role_arn)) = token_file.zip(role_arn) {
                builder = builder
                    .with_config(AmazonS3ConfigKey::WebIdentityTokenFile, token_file)
                    .with_config(AmazonS3ConfigKey::RoleArn, role_arn);

                if let Ok(session_name) = std::env::var(AWS_ROLE_SESSION_NAME) {
                    builder = builder.with_config(AmazonS3ConfigKey::RoleSessionName, session_name);
                }
            }
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
        let s3 = LimitStore::new(s3, self.max_object_store_requests.get());
        let s3 =
            MetricLayer::new(s3, "s3").with_cache_control_no_store(self.cache_control_no_store);

        let object_store_registry = DefaultObjectStoreRegistry::new();
        let url = ObjectStoreUrl::parse(format!("s3://{}", self.bucket_name)).unwrap();
        object_store_registry.register_store(url.as_ref(), Arc::new(s3));

        RuntimeEnvBuilder::new().with_object_store_registry(Arc::new(object_store_registry))
    }

    fn construct_client(&self) -> Arc<dyn ObjectStorage> {
        let s3 = self.get_default_builder().build().unwrap();
        // limit objectstore to a concurrent request limit
        let s3 = LimitStore::new(s3, self.max_object_store_requests.get());
        let s3 =
            MetricLayer::new(s3, "s3").with_cache_control_no_store(self.cache_control_no_store);
        Arc::new(S3 {
            client: Arc::new(s3),
            bucket: self.bucket_name.clone(),
            root: StorePath::from(""),
        })
    }

    fn get_endpoint(&self) -> String {
        format!("{}/{}", self.endpoint_url, self.bucket_name)
    }
}

#[derive(Debug)]
pub struct S3 {
    client: Arc<MetricLayer<LimitStore<AmazonS3>>>,
    bucket: String,
    root: StorePath,
}

impl S3 {
    async fn _parallel_download(
        &self,
        path: &RelativePath,
        tenant_id: &Option<String>,
        write_path: PathBuf,
    ) -> Result<(), ObjectStorageError> {
        let partial = partial_path(&write_path)?;
        match self
            ._parallel_download_inner(path, tenant_id, partial.clone())
            .await
        {
            Ok(()) => {
                if let Err(e) = tokio::fs::rename(&partial, &write_path).await {
                    let _ = tokio::fs::remove_file(&partial).await;
                    return Err(e.into());
                }
                Ok(())
            }
            Err(e) => {
                let _ = tokio::fs::remove_file(&partial).await;
                Err(e)
            }
        }
    }

    #[tracing::instrument(
        name = "s3.parallel_download",
        skip(self, partial_path),
        fields(path = %path, tenant = ?tenant_id, total_bytes = tracing::field::Empty, chunks = tracing::field::Empty),
        err
    )]
    async fn _parallel_download_inner(
        &self,
        path: &RelativePath,
        tenant_id: &Option<String>,
        partial_path: PathBuf,
    ) -> Result<(), ObjectStorageError> {
        let tenant_str = tenant_id.as_deref().unwrap_or(DEFAULT_TENANT);
        let date = Utc::now().date_naive().to_string();
        let src = to_object_store_path(path);

        let meta = self.client.head(&src).await?;
        increment_object_store_calls_by_date("HEAD", &date, tenant_str);
        let total = meta.size;

        if let Some(parent) = partial_path.parent() {
            tokio::fs::create_dir_all(parent).await?;
        }
        let file = tokio::fs::File::create(&partial_path).await?;
        file.set_len(total).await?;
        let std_file = Arc::new(file.into_std().await);

        let chunk = PARSEABLE.options.hot_tier_download_chunk_size;
        let concurrency = PARSEABLE.options.hot_tier_download_concurrency;
        let ranges: Vec<Range<u64>> = (0..total)
            .step_by(chunk as usize)
            .map(|s| s..(s + chunk).min(total))
            .collect();
        let chunk_count = ranges.len() as u64;
        tracing::Span::current()
            .record("total_bytes", total)
            .record("chunks", chunk_count);

        let semaphore = Arc::new(tokio::sync::Semaphore::new(concurrency as usize));

        futures::stream::iter(ranges)
            .map(|r| {
                let src = src.clone();
                let std_file = std_file.clone();
                let semaphore = semaphore.clone();
                async move {
                    let _permit = semaphore.acquire_owned().await.map_err(|e| {
                        ObjectStorageError::Custom(format!("semaphore closed: {e}"))
                    })?;
                    let bytes = self.client.get_range(&src, r.clone()).await?;
                    let offset = r.start;
                    tokio::task::spawn_blocking(move || -> std::io::Result<()> {
                        crate::storage::write_all_at(&std_file, &bytes, offset)
                    })
                    .await
                    .map_err(|e| ObjectStorageError::Custom(format!("join: {e}")))??;
                    Ok::<_, ObjectStorageError>(())
                }
            })
            .buffer_unordered(concurrency as usize)
            .try_collect::<Vec<_>>()
            .await?;

        increment_object_store_calls_by_date("GET", &date, tenant_str);
        increment_files_scanned_in_object_store_calls_by_date("GET", 1, &date, tenant_str);
        increment_bytes_scanned_in_object_store_calls_by_date("GET", total, &date, tenant_str);
        increment_partial_file_scans_in_object_store_calls_by_date(
            "GET",
            chunk_count,
            &date,
            tenant_str,
        );
        Ok(())
    }

    #[tracing::instrument(
        name = "s3.get_object",
        skip(self),
        fields(path = %path, tenant = ?tenant_id, bytes = tracing::field::Empty)
    )]
    async fn _get_object(
        &self,
        path: &RelativePath,
        tenant_id: &Option<String>,
    ) -> Result<Bytes, ObjectStorageError> {
        let tenant_str = tenant_id.as_deref().unwrap_or(DEFAULT_TENANT);
        let resp = self.client.get(&to_object_store_path(path)).await;
        increment_object_store_calls_by_date(
            "GET",
            &Utc::now().date_naive().to_string(),
            tenant_str,
        );

        match resp {
            Ok(resp) => {
                let body = resp.bytes().await?;
                tracing::Span::current().record("bytes", body.len());
                increment_files_scanned_in_object_store_calls_by_date(
                    "GET",
                    1,
                    &Utc::now().date_naive().to_string(),
                    tenant_str,
                );
                increment_bytes_scanned_in_object_store_calls_by_date(
                    "GET",
                    body.len() as u64,
                    &Utc::now().date_naive().to_string(),
                    tenant_str,
                );
                Ok(body)
            }
            Err(err) => Err(err.into()),
        }
    }

    #[tracing::instrument(
        name = "s3.put_object",
        skip(self, resource),
        fields(path = %path, tenant = ?tenant_id, bytes = resource.content_length())
    )]
    async fn _put_object(
        &self,
        path: &RelativePath,
        resource: PutPayload,
        tenant_id: &Option<String>,
    ) -> Result<(), ObjectStorageError> {
        let tenant_str = tenant_id.as_deref().unwrap_or(DEFAULT_TENANT);
        let resp = self.client.put(&to_object_store_path(path), resource).await;
        increment_object_store_calls_by_date(
            "PUT",
            &Utc::now().date_naive().to_string(),
            tenant_str,
        );
        match resp {
            Ok(_) => {
                increment_files_scanned_in_object_store_calls_by_date(
                    "PUT",
                    1,
                    &Utc::now().date_naive().to_string(),
                    tenant_str,
                );
                Ok(())
            }
            Err(err) => Err(err.into()),
        }
    }

    #[tracing::instrument(
        name = "s3.delete_prefix",
        skip(self),
        fields(prefix = %key, tenant = ?tenant_id, deleted = tracing::field::Empty, failed = tracing::field::Empty),
        err
    )]
    async fn _delete_prefix(
        &self,
        key: &str,
        tenant_id: &Option<String>,
    ) -> Result<(), ObjectStorageError> {
        let tenant_str = tenant_id.as_deref().unwrap_or(DEFAULT_TENANT);

        // List all objects under the prefix
        let object_stream = self.client.list(Some(&(key.into())));
        increment_object_store_calls_by_date(
            "LIST",
            &Utc::now().date_naive().to_string(),
            tenant_str,
        );

        // Map listed objects to their locations for delete_stream
        let locations_stream = object_stream.map(|result| result.map(|obj| obj.location));

        // Use delete_stream which batches deletes via the S3 DeleteObjects API
        let mut delete_results = self.client.delete_stream(Box::pin(locations_stream));

        let mut files_deleted: u64 = 0;
        let mut failed_deletes: u64 = 0;
        while let Some(result) = delete_results.next().await {
            match result {
                Ok(_) => files_deleted += 1,
                Err(err) => {
                    failed_deletes += 1;
                    error!("Failed to delete object during prefix deletion: {:?}", err);
                }
            }
        }

        let total_files = files_deleted + failed_deletes;
        tracing::Span::current()
            .record("deleted", files_deleted)
            .record("failed", failed_deletes);
        increment_files_scanned_in_object_store_calls_by_date(
            "LIST",
            total_files,
            &Utc::now().date_naive().to_string(),
            tenant_str,
        );
        increment_files_scanned_in_object_store_calls_by_date(
            "DELETE",
            files_deleted,
            &Utc::now().date_naive().to_string(),
            tenant_str,
        );

        if failed_deletes > 0 {
            return Err(ObjectStorageError::UnhandledError(
                anyhow::anyhow!("Failed to delete {failed_deletes} out of {total_files} objects under prefix '{key}'").into(),
            ));
        }

        Ok(())
    }

    #[tracing::instrument(
        name = "s3.list_dates",
        skip(self),
        fields(stream = %stream, tenant = ?tenant_id, dates = tracing::field::Empty)
    )]
    async fn _list_dates(
        &self,
        stream: &str,
        tenant_id: &Option<String>,
    ) -> Result<Vec<String>, ObjectStorageError> {
        let (pre, prefix) = stream_data_listing_prefix(stream, &[], tenant_id);
        let tenant_str = tenant_id.as_deref().unwrap_or(DEFAULT_TENANT);
        let resp: Result<object_store::ListResult, object_store::Error> =
            self.client.list_with_delimiter(Some(&pre)).await;
        increment_object_store_calls_by_date(
            "LIST",
            &Utc::now().date_naive().to_string(),
            tenant_str,
        );

        let resp = match resp {
            Ok(resp) => resp,
            Err(err) => {
                return Err(err.into());
            }
        };

        let common_prefixes = resp.common_prefixes;

        increment_files_scanned_in_object_store_calls_by_date(
            "LIST",
            common_prefixes.len() as u64,
            &Utc::now().date_naive().to_string(),
            tenant_str,
        );

        // return prefixes at the root level
        let dates: Vec<_> = common_prefixes
            .iter()
            .filter_map(|path| path.as_ref().strip_prefix(&prefix))
            .map(String::from)
            .collect();
        tracing::Span::current().record("dates", dates.len());

        Ok(dates)
    }

    #[tracing::instrument(
        name = "s3.upload_file",
        skip(self),
        fields(key = %key, path = %path.display(), tenant = ?tenant_id)
    )]
    async fn _upload_file(
        &self,
        key: &str,
        path: &Path,
        tenant_id: &Option<String>,
    ) -> Result<(), ObjectStorageError> {
        let tenant_str = tenant_id.as_deref().unwrap_or(DEFAULT_TENANT);
        let bytes = tokio::fs::read(path).await?;

        let result = self.client.put(&key.into(), bytes.into()).await;
        increment_object_store_calls_by_date(
            "PUT",
            &Utc::now().date_naive().to_string(),
            tenant_str,
        );
        match result {
            Ok(_) => {
                increment_files_scanned_in_object_store_calls_by_date(
                    "PUT",
                    1,
                    &Utc::now().date_naive().to_string(),
                    tenant_str,
                );
                Ok(())
            }
            Err(err) => Err(err.into()),
        }
    }

    #[tracing::instrument(
        name = "s3.upload_multipart",
        skip(self),
        fields(key = %key, path = %path.display(), tenant = ?tenant_id, total_bytes = tracing::field::Empty, parts = tracing::field::Empty)
    )]
    async fn _upload_multipart(
        &self,
        key: &RelativePath,
        path: &Path,
        tenant_id: &Option<String>,
    ) -> Result<(), ObjectStorageError> {
        let tenant_str = tenant_id.as_deref().unwrap_or(DEFAULT_TENANT);
        let mut file = OpenOptions::new().read(true).open(path).await?;
        let location = &to_object_store_path(key);

        let meta = file.metadata().await?;
        let total_size = meta.len() as usize;
        let min_multipart_size = PARSEABLE.options.min_multipart_size as usize;
        if use_single_put(
            total_size,
            min_multipart_size,
            PARSEABLE.options.enable_multipart,
        ) {
            // Read directly from the already-opened std::fs::File in one blocking call
            // rather than chunked tokio::fs::File::read_to_end (which copies every chunk
            // through an internal buffer across multiple blocking-pool round trips).
            // The reported length is only a capacity hint: the body is still whatever
            // read_to_end returns, as before.
            let mut std_file = file.into_std().await;
            let data = tokio::task::spawn_blocking(move || -> std::io::Result<Vec<u8>> {
                use std::io::Read;
                let mut data = Vec::with_capacity(total_size.saturating_add(1));
                std_file.read_to_end(&mut data)?;
                Ok(data)
            })
            .await
            .map_err(|err| ObjectStorageError::UnhandledError(Box::new(err)))??;

            // Track single PUT operation for small files (PutPayload::from(Vec<u8>) is zero-copy).
            let result = self.client.put(location, PutPayload::from(data)).await;
            increment_object_store_calls_by_date(
                "PUT",
                &Utc::now().date_naive().to_string(),
                tenant_str,
            );
            match result {
                Ok(_) => {
                    increment_files_scanned_in_object_store_calls_by_date(
                        "PUT",
                        1,
                        &Utc::now().date_naive().to_string(),
                        tenant_str,
                    );
                }
                Err(err) => {
                    return Err(err.into());
                }
            }

            return Ok(());
        } else {
            let async_writer = self.client.put_multipart(location).await;
            let async_writer = match async_writer {
                Ok(writer) => writer,
                Err(err) => {
                    return Err(err.into());
                }
            };

            let total_parts = total_size.div_ceil(min_multipart_size);
            tracing::Span::current()
                .record("total_bytes", total_size)
                .record("parts", total_parts);

            upload_file_parts(
                &mut file,
                async_writer,
                total_size,
                min_multipart_size,
                || {
                    increment_object_store_calls_by_date(
                        "PUT_MULTIPART",
                        &Utc::now().date_naive().to_string(),
                        tenant_str,
                    )
                },
            )
            .await?;
        }
        Ok(())
    }
}

#[async_trait]
impl ObjectStorage for S3 {
    async fn get_buffered_reader(
        &self,
        path: &RelativePath,
        tenant_id: &Option<String>,
    ) -> Result<BufReader, ObjectStorageError> {
        let tenant_str = tenant_id.as_deref().unwrap_or(DEFAULT_TENANT);
        let path = &to_object_store_path(path);
        let meta = self.client.head(path).await;
        increment_object_store_calls_by_date(
            "HEAD",
            &Utc::now().date_naive().to_string(),
            tenant_str,
        );
        let meta = match meta {
            Ok(meta) => {
                increment_files_scanned_in_object_store_calls_by_date(
                    "HEAD",
                    1,
                    &Utc::now().date_naive().to_string(),
                    tenant_str,
                );
                meta
            }
            Err(err) => {
                return Err(err.into());
            }
        };

        let buf = object_store::buffered::BufReader::new(self.client.clone(), &meta);
        Ok(buf)
    }

    async fn upload_multipart(
        &self,
        key: &RelativePath,
        path: &Path,
        tenant_id: &Option<String>,
    ) -> Result<(), ObjectStorageError> {
        self._upload_multipart(key, path, tenant_id).await
    }

    #[tracing::instrument(
        name = "s3.head",
        skip(self),
        fields(path = %path, tenant = ?tenant_id)
    )]
    async fn head(
        &self,
        path: &RelativePath,
        tenant_id: &Option<String>,
    ) -> Result<ObjectMeta, ObjectStorageError> {
        let tenant_str = tenant_id.as_deref().unwrap_or(DEFAULT_TENANT);
        let result = self.client.head(&to_object_store_path(path)).await;
        increment_object_store_calls_by_date(
            "HEAD",
            &Utc::now().date_naive().to_string(),
            tenant_str,
        );
        if result.is_ok() {
            increment_files_scanned_in_object_store_calls_by_date(
                "HEAD",
                1,
                &Utc::now().date_naive().to_string(),
                tenant_str,
            );
        }

        Ok(result?)
    }

    async fn parallel_chunked_download(
        &self,
        path: &RelativePath,
        tenant_id: &Option<String>,
        write_path: PathBuf,
    ) -> Result<(), ObjectStorageError> {
        self._parallel_download(path, tenant_id, write_path).await
    }

    async fn get_metadata_versioned(
        &self,
        path: &RelativePath,
        _tenant_id: &Option<String>,
    ) -> Result<
        Option<super::object_storage::VersionedMetadata>,
        super::object_storage::ConditionalMetadataError,
    > {
        super::object_storage::get_cloud_metadata(self.client.as_ref(), path).await
    }

    async fn put_metadata_conditional(
        &self,
        path: &RelativePath,
        bytes: Bytes,
        condition: super::object_storage::MetadataWriteCondition,
        _tenant_id: &Option<String>,
    ) -> Result<
        super::object_storage::MetadataVersion,
        super::object_storage::ConditionalMetadataError,
    > {
        super::object_storage::put_cloud_metadata(self.client.as_ref(), path, bytes, condition)
            .await
    }

    async fn get_object(
        &self,
        path: &RelativePath,
        tenant_id: &Option<String>,
    ) -> Result<Bytes, ObjectStorageError> {
        self._get_object(path, tenant_id).await
    }

    async fn get_object_ranged(
        &self,
        path: &RelativePath,
        tenant_id: &Option<String>,
    ) -> Result<Bytes, ObjectStorageError> {
        let tenant = tenant_id.as_deref().unwrap_or(DEFAULT_TENANT);
        let date = Utc::now().date_naive().to_string();
        let body = crate::storage::get_object_ranged(
            &self.client,
            &to_object_store_path(path),
            path,
            || increment_object_store_calls_by_date("GET", &date, tenant),
        )
        .await?;
        increment_files_scanned_in_object_store_calls_by_date("GET", 1, &date, tenant);
        increment_bytes_scanned_in_object_store_calls_by_date(
            "GET",
            body.len() as u64,
            &date,
            tenant,
        );
        Ok(body)
    }

    async fn get_objects(
        &self,
        base_path: Option<&RelativePath>,
        filter_func: Box<dyn Fn(String) -> bool + Send>,
        tenant_id: &Option<String>,
    ) -> Result<Vec<Bytes>, ObjectStorageError> {
        let tenant_str = tenant_id.as_deref().unwrap_or(DEFAULT_TENANT);
        let prefix = if let Some(base_path) = base_path {
            to_object_store_path(base_path)
        } else {
            self.root.clone()
        };

        let mut list_stream = self.client.list(Some(&prefix));

        let mut paths = vec![];
        let mut files_scanned = 0;

        // Drain the listing first, then fetch. Awaiting each GET inside the
        // listing loop serialises every download against the one before it.
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

            paths.push(
                RelativePath::from_path(meta.location.as_ref())
                    .map_err(ObjectStorageError::PathError)?
                    .to_relative_path_buf(),
            );
        }

        // `buffered` rather than `buffer_unordered`: callers of this generic
        // helper may rely on results following listing order.
        let res: Vec<Bytes> = futures::stream::iter(paths)
            .map(|path| async move { self.get_object(&path, tenant_id).await })
            .buffered(GET_OBJECTS_CONCURRENCY)
            .try_collect()
            .await?;

        // Record total files scanned
        increment_files_scanned_in_object_store_calls_by_date(
            "LIST",
            files_scanned as u64,
            &Utc::now().date_naive().to_string(),
            tenant_str,
        );
        increment_object_store_calls_by_date(
            "LIST",
            &Utc::now().date_naive().to_string(),
            tenant_str,
        );
        Ok(res)
    }

    async fn get_ingestor_meta_file_paths(
        &self,
        tenant_id: &Option<String>,
    ) -> Result<Vec<RelativePathBuf>, ObjectStorageError> {
        let tenant_str = tenant_id.as_deref().unwrap_or(DEFAULT_TENANT);
        let mut path_arr = vec![];
        let mut files_scanned = 0;

        let mut object_stream = self.client.list(Some(&self.root));
        increment_object_store_calls_by_date(
            "LIST",
            &Utc::now().date_naive().to_string(),
            tenant_str,
        );

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
        // Record total files scanned
        increment_files_scanned_in_object_store_calls_by_date(
            "LIST",
            files_scanned as u64,
            &Utc::now().date_naive().to_string(),
            tenant_str,
        );
        Ok(path_arr)
    }

    async fn put_object(
        &self,
        path: &RelativePath,
        resource: Bytes,
        tenant_id: &Option<String>,
    ) -> Result<(), ObjectStorageError> {
        self._put_object(path, resource.into(), tenant_id)
            .await
            .map_err(|err| ObjectStorageError::ConnectionError(Box::new(err)))?;

        Ok(())
    }

    async fn delete_prefix(
        &self,
        path: &RelativePath,
        tenant_id: &Option<String>,
    ) -> Result<(), ObjectStorageError> {
        self._delete_prefix(path.as_ref(), tenant_id).await?;

        Ok(())
    }

    #[tracing::instrument(
        name = "s3.delete_object",
        skip(self),
        fields(path = %path, tenant = ?tenant_id),
        err
    )]
    async fn delete_object(
        &self,
        path: &RelativePath,
        tenant_id: &Option<String>,
    ) -> Result<(), ObjectStorageError> {
        let tenant_str = tenant_id.as_deref().unwrap_or(DEFAULT_TENANT);
        let result = self.client.delete(&to_object_store_path(path)).await;
        increment_object_store_calls_by_date(
            "DELETE",
            &Utc::now().date_naive().to_string(),
            tenant_str,
        );
        if result.is_ok() {
            increment_files_scanned_in_object_store_calls_by_date(
                "DELETE",
                1,
                &Utc::now().date_naive().to_string(),
                tenant_str,
            );
        }

        Ok(result?)
    }

    #[tracing::instrument(
        name = "s3.check",
        skip(self),
        fields(tenant = ?tenant_id, ok = tracing::field::Empty)
    )]
    async fn check(&self, tenant_id: &Option<String>) -> Result<(), ObjectStorageError> {
        let tenant_str = tenant_id.as_deref().unwrap_or(DEFAULT_TENANT);
        let result = self
            .client
            .head(&to_object_store_path(&parseable_json_path()))
            .await;
        tracing::Span::current().record("ok", result.is_ok());
        increment_object_store_calls_by_date(
            "HEAD",
            &Utc::now().date_naive().to_string(),
            tenant_str,
        );

        if result.is_ok() {
            increment_files_scanned_in_object_store_calls_by_date(
                "HEAD",
                1,
                &Utc::now().date_naive().to_string(),
                tenant_str,
            );
        }

        Ok(result.map(|_| ())?)
    }

    async fn delete_stream(
        &self,
        stream_name: &str,
        tenant_id: &Option<String>,
    ) -> Result<(), ObjectStorageError> {
        let prefix = if let Some(tenant) = tenant_id.as_ref() {
            &format!("{tenant}/{stream_name}")
        } else {
            stream_name
        };
        self._delete_prefix(prefix, tenant_id).await?;

        Ok(())
    }

    async fn try_delete_node_meta(
        &self,
        node_filename: String,
        tenant_id: &Option<String>,
    ) -> Result<(), ObjectStorageError> {
        let tenant_str = tenant_id.as_deref().unwrap_or(DEFAULT_TENANT);
        let file = RelativePathBuf::from(&node_filename);

        let result = self.client.delete(&to_object_store_path(&file)).await;
        increment_object_store_calls_by_date(
            "DELETE",
            &Utc::now().date_naive().to_string(),
            tenant_str,
        );
        match result {
            Ok(_) => {
                increment_files_scanned_in_object_store_calls_by_date(
                    "DELETE",
                    1,
                    &Utc::now().date_naive().to_string(),
                    tenant_str,
                );
                Ok(())
            }
            Err(err) => Err(err.into()),
        }
    }

    async fn list_streams(&self) -> Result<HashSet<LogStream>, ObjectStorageError> {
        // self._list_streams().await
        Err(ObjectStorageError::Custom(
            "S3 doesn't implement list_streams".into(),
        ))
    }

    async fn list_old_streams(&self) -> Result<HashSet<LogStream>, ObjectStorageError> {
        let tenant_str = DEFAULT_TENANT;
        let resp = self.client.list_with_delimiter(None).await?;
        let common_prefixes = resp.common_prefixes; // get all dirs
        increment_files_scanned_in_object_store_calls_by_date(
            "LIST",
            common_prefixes.len() as u64,
            &Utc::now().date_naive().to_string(),
            tenant_str,
        );
        increment_object_store_calls_by_date(
            "LIST",
            &Utc::now().date_naive().to_string(),
            tenant_str,
        );
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
                let result = self.client.head(&StorePath::from(key)).await;
                increment_object_store_calls_by_date(
                    "HEAD",
                    &Utc::now().date_naive().to_string(),
                    "",
                );
                result.map(|_| ())
            };
            stream_json_check.push(task);
        }
        increment_files_scanned_in_object_store_calls_by_date(
            "HEAD",
            dirs.len() as u64,
            &Utc::now().date_naive().to_string(),
            tenant_str,
        );
        stream_json_check.try_collect::<()>().await?;

        Ok(dirs)
    }

    async fn list_dates(
        &self,
        stream_name: &str,
        tenant_id: &Option<String>,
    ) -> Result<Vec<String>, ObjectStorageError> {
        let streams = self._list_dates(stream_name, tenant_id).await?;

        Ok(streams)
    }

    async fn list_hours(
        &self,
        stream_name: &str,
        date: &str,
        tenant_id: &Option<String>,
    ) -> Result<Vec<String>, ObjectStorageError> {
        let (pre_path, pre) = stream_data_listing_prefix(stream_name, &[date], tenant_id);
        let tenant_str = tenant_id.as_deref().unwrap_or(DEFAULT_TENANT);
        let resp = self.client.list_with_delimiter(Some(&pre_path)).await?;
        increment_files_scanned_in_object_store_calls_by_date(
            "LIST",
            resp.common_prefixes.len() as u64,
            &Utc::now().date_naive().to_string(),
            tenant_str,
        );
        increment_object_store_calls_by_date(
            "LIST",
            &Utc::now().date_naive().to_string(),
            tenant_str,
        );

        let hours: Vec<String> = resp
            .common_prefixes
            .iter()
            .filter_map(|path| {
                let path_str = path.as_ref();
                if let Some(stripped) = path_str.strip_prefix(&pre) {
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
        tenant_id: &Option<String>,
    ) -> Result<Vec<String>, ObjectStorageError> {
        let (pre_path, pre) = stream_data_listing_prefix(stream_name, &[date, hour], tenant_id);
        let tenant_str = tenant_id.as_deref().unwrap_or(DEFAULT_TENANT);
        let resp = self.client.list_with_delimiter(Some(&pre_path)).await?;
        increment_files_scanned_in_object_store_calls_by_date(
            "LIST",
            resp.common_prefixes.len() as u64,
            &Utc::now().date_naive().to_string(),
            tenant_str,
        );
        increment_object_store_calls_by_date(
            "LIST",
            &Utc::now().date_naive().to_string(),
            tenant_str,
        );
        let minutes: Vec<String> = resp
            .common_prefixes
            .iter()
            .filter_map(|path| {
                let path_str = path.as_ref();
                if let Some(stripped) = path_str.strip_prefix(&pre) {
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

    async fn upload_file(
        &self,
        key: &str,
        path: &Path,
        tenant_id: &Option<String>,
    ) -> Result<(), ObjectStorageError> {
        Ok(self._upload_file(key, path, tenant_id).await?)
    }

    fn absolute_url(&self, prefix: &RelativePath) -> object_store::path::Path {
        object_store::path::Path::parse(prefix).unwrap()
    }

    fn query_prefixes(&self, prefixes: Vec<String>) -> Vec<ListingTableUrl> {
        prefixes
            .into_iter()
            .map(|prefix| {
                let path = format!("s3://{}/{}", self.bucket, prefix);
                ListingTableUrl::parse(path).unwrap()
            })
            .collect()
    }

    fn store_url(&self) -> url::Url {
        url::Url::parse(&format!("s3://{}", self.bucket)).unwrap()
    }

    async fn list_dirs(
        &self,
        tenant_id: &Option<String>,
    ) -> Result<Vec<String>, ObjectStorageError> {
        let tenant_str = tenant_id.as_deref().unwrap_or(DEFAULT_TENANT);
        let pre = object_store::path::Path::from("/");
        let resp = self.client.list_with_delimiter(Some(&pre)).await;
        increment_object_store_calls_by_date(
            "LIST",
            &Utc::now().date_naive().to_string(),
            tenant_str,
        );
        let resp = match resp {
            Ok(resp) => {
                increment_files_scanned_in_object_store_calls_by_date(
                    "LIST",
                    resp.common_prefixes.len() as u64,
                    &Utc::now().date_naive().to_string(),
                    tenant_str,
                );

                resp
            }
            Err(err) => {
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
        tenant_id: &Option<String>,
    ) -> Result<Vec<String>, ObjectStorageError> {
        let tenant_str = tenant_id.as_deref().unwrap_or(DEFAULT_TENANT);
        let prefix = object_store::path::Path::from(relative_path.as_str());

        let resp = self.client.list_with_delimiter(Some(&prefix)).await;
        increment_object_store_calls_by_date(
            "LIST",
            &Utc::now().date_naive().to_string(),
            tenant_str,
        );
        let resp = match resp {
            Ok(resp) => {
                increment_files_scanned_in_object_store_calls_by_date(
                    "LIST",
                    resp.common_prefixes.len() as u64,
                    &Utc::now().date_naive().to_string(),
                    tenant_str,
                );

                resp
            }
            Err(err) => {
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

    async fn metadata_hidden_prefix(
        &self,
        prefix: object_store::path::Path,
    ) -> Result<Vec<(String, chrono::DateTime<chrono::Utc>)>, ObjectStorageError> {
        use futures::TryStreamExt;
        let objects: Vec<_> = self.client.list(Some(&prefix)).try_collect().await?;
        objects
            .into_iter()
            .map(|o| {
                crate::storage::object_storage::decode_metadata_key(o.location.as_ref())
                    .map(|key| (key, o.last_modified))
            })
            .collect()
    }

    async fn list_with_delimiter(
        &self,
        prefix: Option<object_store::path::Path>,
    ) -> Result<ListResult, ObjectStorageError> {
        Ok(self.client.list_with_delimiter(prefix.as_ref()).await?)
    }
}

// BEGIN multipart-part-upload helper (extracted verbatim by the standalone upload harness)

/// Files smaller than the multipart threshold, or every file when multipart is
/// disabled, are sent with a single PUT.
fn use_single_put(total_size: usize, min_multipart_size: usize, enable_multipart: bool) -> bool {
    total_size < min_multipart_size || !enable_multipart
}

/// Aborts a multipart upload dropped before completion or an explicit abort
/// resolves, e.g. when the owning upload task is cancelled during either await.
///
/// Best effort only: the abort is spawned onto the current tokio runtime, so it is
/// skipped without a runtime and may itself be cancelled during runtime shutdown.
/// Stores without lifecycle cleanup can still retain parts in those cases.
struct AbortOnDrop(Option<Box<dyn MultipartUpload>>);

impl Drop for AbortOnDrop {
    fn drop(&mut self) {
        let Some(mut upload) = self.0.take() else {
            return;
        };
        if let Ok(handle) = tokio::runtime::Handle::try_current() {
            handle.spawn(async move {
                if let Err(err) = upload.abort().await {
                    warn!("Failed to abort cancelled multipart upload. {:?}", err);
                }
            });
        }
    }
}

/// Uploads exactly `total_size` bytes from `reader` as sequential, ordered parts of
/// `part_size` bytes (the last part may be smaller), then completes the upload.
///
/// Only one part buffer is held at a time and each part is awaited before the next is
/// read. `on_part` runs after each successful part. A short read, extra trailing
/// bytes, or a read/part/complete failure aborts the upload. Read/part failures
/// retain their original error. As before, an abort failure after a completion
/// failure is returned instead of the completion error.
async fn upload_file_parts<R>(
    reader: &mut R,
    upload: Box<dyn MultipartUpload>,
    total_size: usize,
    part_size: usize,
    on_part: impl FnMut(),
) -> Result<(), ObjectStorageError>
where
    R: AsyncRead + Unpin,
{
    assert!(part_size > 0, "multipart part size must be non-zero");
    let mut guard = AbortOnDrop(Some(upload));
    let upload = guard.0.as_mut().expect("upload is present until completed");
    let result = put_file_parts(reader, upload, total_size, part_size, on_part).await;
    let completing = result.is_ok();
    let result = match result {
        Ok(()) => upload.complete().await.map(|_| ()).map_err(|err| {
            error!("Failed to complete multipart upload. {:?}", err);
            ObjectStorageError::from(err)
        }),
        Err(err) => Err(err),
    };
    if let Err(err) = result {
        // Keep ownership in the guard throughout the await. If a sibling upload
        // failure cancels this task mid-abort, Drop can retry best-effort cleanup.
        let aborted = guard
            .0
            .as_mut()
            .expect("upload is present until cleanup resolves")
            .abort()
            .await;
        guard.0 = None;
        if let Err(abort_err) = aborted {
            error!("Failed to abort multipart upload. {:?}", abort_err);
            if completing {
                return Err(abort_err.into());
            }
        }
        return Err(err);
    }
    guard.0 = None;
    Ok(())
}

async fn put_file_parts<R>(
    reader: &mut R,
    upload: &mut Box<dyn MultipartUpload>,
    total_size: usize,
    part_size: usize,
    mut on_part: impl FnMut(),
) -> Result<(), ObjectStorageError>
where
    R: AsyncRead + Unpin,
{
    let mut remaining = total_size;
    while remaining > 0 {
        let len = remaining.min(part_size);
        let mut part = Vec::with_capacity(len);
        (&mut *reader)
            .take(len as u64)
            .read_to_end(&mut part)
            .await?;
        if part.len() != len {
            return Err(std::io::Error::new(
                std::io::ErrorKind::UnexpectedEof,
                format!(
                    "file ended after {} of {total_size} bytes during multipart upload",
                    total_size - remaining + part.len()
                ),
            )
            .into());
        }
        upload.put_part(part.into()).await?;
        on_part();
        remaining -= len;
    }

    let mut probe = [0u8; 1];
    if reader.read(&mut probe).await? != 0 {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            format!("file grew beyond {total_size} bytes during multipart upload"),
        )
        .into());
    }

    Ok(())
}

// END multipart-part-upload helper

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

#[cfg(test)]
mod multipart_upload_tests {
    // BEGIN multipart-part-upload tests (extracted verbatim by the standalone upload harness)
    use std::{
        io,
        pin::Pin,
        sync::{
            Arc, Mutex,
            atomic::{AtomicUsize, Ordering},
        },
        task::{Context, Poll},
        time::Duration,
    };

    use async_trait::async_trait;
    use bytes::Bytes;
    use object_store::{MultipartUpload, PutPayload, PutResult, UploadPart};
    use tokio::io::{AsyncRead, AsyncWriteExt, ReadBuf};

    use super::{ObjectStorageError, upload_file_parts, use_single_put};

    #[derive(Debug, Default)]
    struct FakeState {
        parts: Vec<Bytes>,
        fail_part_at: Option<usize>,
        hang_part_at: Option<usize>,
        fail_complete: bool,
        fail_abort: bool,
        completed: usize,
        aborted: usize,
    }

    #[derive(Debug, Clone, Default)]
    struct FakeUpload(Arc<Mutex<FakeState>>);

    fn fake_error(msg: &str) -> object_store::Error {
        object_store::Error::Generic {
            store: "fake",
            source: msg.to_string().into(),
        }
    }

    #[async_trait]
    impl MultipartUpload for FakeUpload {
        fn put_part(&mut self, data: PutPayload) -> UploadPart {
            let mut state = self.0.lock().unwrap();
            let index = state.parts.len();
            if state.hang_part_at == Some(index) {
                return Box::pin(futures::future::pending());
            }
            if state.fail_part_at == Some(index) {
                return Box::pin(async { Err(fake_error("part failed")) });
            }
            let mut bytes = Vec::new();
            for chunk in data.iter() {
                bytes.extend_from_slice(chunk);
            }
            state.parts.push(bytes.into());
            Box::pin(async { Ok(()) })
        }

        async fn complete(&mut self) -> object_store::Result<PutResult> {
            let mut state = self.0.lock().unwrap();
            state.completed += 1;
            if state.fail_complete {
                return Err(fake_error("complete failed"));
            }
            Ok(PutResult {
                e_tag: None,
                version: None,
            })
        }

        async fn abort(&mut self) -> object_store::Result<()> {
            let mut state = self.0.lock().unwrap();
            state.aborted += 1;
            if state.fail_abort {
                return Err(fake_error("abort failed"));
            }
            Ok(())
        }
    }

    /// Yields `data`, then optionally fails instead of reporting EOF.
    struct ScriptedReader {
        data: Vec<u8>,
        pos: usize,
        fail_at_end: bool,
    }

    impl AsyncRead for ScriptedReader {
        fn poll_read(
            mut self: Pin<&mut Self>,
            _cx: &mut Context<'_>,
            buf: &mut ReadBuf<'_>,
        ) -> Poll<io::Result<()>> {
            if self.pos == self.data.len() {
                if self.fail_at_end {
                    return Poll::Ready(Err(io::Error::other("read failed")));
                }
                return Poll::Ready(Ok(()));
            }
            // Short reads exercise the per-part read loop.
            let n = buf.remaining().min(self.data.len() - self.pos).min(7);
            let start = self.pos;
            buf.put_slice(&self.data[start..start + n]);
            self.pos += n;
            Poll::Ready(Ok(()))
        }
    }

    fn pattern(len: usize) -> Vec<u8> {
        (0..len).map(|i| (i % 251) as u8).collect()
    }

    fn reader(data: Vec<u8>, fail_at_end: bool) -> ScriptedReader {
        ScriptedReader {
            data,
            pos: 0,
            fail_at_end,
        }
    }

    async fn run(
        fake: &FakeUpload,
        data: Vec<u8>,
        fail_at_end: bool,
        total_size: usize,
        part_size: usize,
    ) -> (Result<(), ObjectStorageError>, usize) {
        let mut parts = 0;
        let result = upload_file_parts(
            &mut reader(data, fail_at_end),
            Box::new(fake.clone()),
            total_size,
            part_size,
            || parts += 1,
        )
        .await;
        (result, parts)
    }

    fn uploaded(fake: &FakeUpload) -> (Vec<u8>, Vec<usize>) {
        let state = fake.0.lock().unwrap();
        let sizes = state.parts.iter().map(Bytes::len).collect();
        (state.parts.concat(), sizes)
    }

    fn counts(fake: &FakeUpload) -> (usize, usize) {
        let state = fake.0.lock().unwrap();
        (state.completed, state.aborted)
    }

    #[test]
    fn single_put_threshold_routing() {
        assert!(use_single_put(99, 100, true));
        assert!(!use_single_put(100, 100, true));
        assert!(!use_single_put(101, 100, true));
        assert!(use_single_put(100, 100, false));
        assert!(use_single_put(1000, 100, false));
        assert!(use_single_put(0, 100, true));
    }

    #[tokio::test]
    async fn exact_multiple_uploads_ordered_full_parts() {
        let fake = FakeUpload::default();
        let data = pattern(300);
        let (result, parts) = run(&fake, data.clone(), false, 300, 100).await;
        result.unwrap();
        let (bytes, sizes) = uploaded(&fake);
        assert_eq!(bytes, data);
        assert_eq!(sizes, vec![100, 100, 100]);
        assert_eq!(parts, 3);
        assert_eq!(counts(&fake), (1, 0));
    }

    #[tokio::test]
    async fn final_partial_part_is_smaller() {
        let fake = FakeUpload::default();
        let data = pattern(301);
        let (result, parts) = run(&fake, data.clone(), false, 301, 100).await;
        result.unwrap();
        let (bytes, sizes) = uploaded(&fake);
        assert_eq!(bytes, data);
        assert_eq!(sizes, vec![100, 100, 100, 1]);
        assert_eq!(parts, 4);
        assert_eq!(counts(&fake), (1, 0));
    }

    #[tokio::test]
    async fn threshold_sized_file_is_one_part() {
        let fake = FakeUpload::default();
        let data = pattern(100);
        let (result, parts) = run(&fake, data.clone(), false, 100, 100).await;
        result.unwrap();
        assert_eq!(uploaded(&fake), (data, vec![100]));
        assert_eq!(parts, 1);
        assert_eq!(counts(&fake), (1, 0));
    }

    #[tokio::test]
    async fn real_file_reader_round_trips() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("data.parquet");
        let data = pattern(1024 * 1024 + 17);
        let mut file = tokio::fs::File::create(&path).await.unwrap();
        file.write_all(&data).await.unwrap();
        file.flush().await.unwrap();
        drop(file);

        let fake = FakeUpload::default();
        let mut file = tokio::fs::File::open(&path).await.unwrap();
        let total_size = file.metadata().await.unwrap().len() as usize;
        upload_file_parts(
            &mut file,
            Box::new(fake.clone()),
            total_size,
            256 * 1024,
            || {},
        )
        .await
        .unwrap();
        let (bytes, sizes) = uploaded(&fake);
        assert_eq!(bytes, data);
        assert_eq!(sizes, vec![262144, 262144, 262144, 262144, 17]);
        assert_eq!(counts(&fake), (1, 0));
    }

    #[tokio::test]
    async fn read_failure_aborts_without_completing() {
        let fake = FakeUpload::default();
        let (result, parts) = run(&fake, pattern(150), true, 300, 100).await;
        match result {
            Err(ObjectStorageError::IoError(err)) => assert_eq!(err.to_string(), "read failed"),
            other => panic!("unexpected result {other:?}"),
        }
        assert_eq!(parts, 1);
        assert_eq!(uploaded(&fake).1, vec![100]);
        assert_eq!(counts(&fake), (0, 1));
    }

    #[tokio::test]
    async fn short_file_aborts_with_unexpected_eof() {
        let fake = FakeUpload::default();
        let (result, parts) = run(&fake, pattern(250), false, 300, 100).await;
        match result {
            Err(ObjectStorageError::IoError(err)) => {
                assert_eq!(err.kind(), io::ErrorKind::UnexpectedEof)
            }
            other => panic!("unexpected result {other:?}"),
        }
        assert_eq!(parts, 2);
        assert_eq!(counts(&fake), (0, 1));
    }

    #[tokio::test]
    async fn grown_file_aborts_before_completing() {
        let fake = FakeUpload::default();
        let (result, parts) = run(&fake, pattern(301), false, 300, 100).await;
        match result {
            Err(ObjectStorageError::IoError(err)) => {
                assert_eq!(err.kind(), io::ErrorKind::InvalidData)
            }
            other => panic!("unexpected result {other:?}"),
        }
        assert_eq!(parts, 3);
        assert_eq!(counts(&fake), (0, 1));
    }

    #[tokio::test]
    async fn put_part_failure_aborts_and_stops() {
        let fake = FakeUpload::default();
        fake.0.lock().unwrap().fail_part_at = Some(1);
        let (result, parts) = run(&fake, pattern(300), false, 300, 100).await;
        assert_eq!(
            result.unwrap_err().to_string(),
            "Unhandled Error: part failed"
        );
        assert_eq!(parts, 1);
        assert_eq!(uploaded(&fake).1, vec![100]);
        assert_eq!(counts(&fake), (0, 1));
    }

    #[tokio::test]
    async fn put_part_failure_keeps_error_when_abort_fails() {
        let fake = FakeUpload::default();
        {
            let mut state = fake.0.lock().unwrap();
            state.fail_part_at = Some(0);
            state.fail_abort = true;
        }
        let (result, parts) = run(&fake, pattern(300), false, 300, 100).await;
        assert_eq!(
            result.unwrap_err().to_string(),
            "Unhandled Error: part failed"
        );
        assert_eq!(parts, 0);
        assert_eq!(counts(&fake), (0, 1));
    }

    #[tokio::test]
    async fn complete_failure_aborts() {
        let fake = FakeUpload::default();
        fake.0.lock().unwrap().fail_complete = true;
        let (result, parts) = run(&fake, pattern(300), false, 300, 100).await;
        assert_eq!(
            result.unwrap_err().to_string(),
            "Unhandled Error: complete failed"
        );
        assert_eq!(parts, 3);
        assert_eq!(counts(&fake), (1, 1));
    }

    #[tokio::test]
    async fn complete_failure_returns_abort_error_when_abort_fails() {
        let fake = FakeUpload::default();
        {
            let mut state = fake.0.lock().unwrap();
            state.fail_complete = true;
            state.fail_abort = true;
        }
        let (result, _) = run(&fake, pattern(300), false, 300, 100).await;
        assert_eq!(
            result.unwrap_err().to_string(),
            "Unhandled Error: abort failed"
        );
        assert_eq!(counts(&fake), (1, 1));
    }

    #[tokio::test]
    async fn cancellation_spawns_best_effort_abort() {
        let fake = FakeUpload::default();
        fake.0.lock().unwrap().hang_part_at = Some(1);
        let timed_out = tokio::time::timeout(
            Duration::from_millis(50),
            run(&fake, pattern(300), false, 300, 100),
        )
        .await;
        assert!(timed_out.is_err());
        for _ in 0..100 {
            if counts(&fake).1 == 1 {
                break;
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
        assert_eq!(counts(&fake), (0, 1));
        assert_eq!(uploaded(&fake).1, vec![100]);
    }

    #[derive(Debug)]
    struct GatedAbortUpload {
        inner: FakeUpload,
        attempts: Arc<AtomicUsize>,
        started: Arc<tokio::sync::Notify>,
        finished: Arc<tokio::sync::Notify>,
    }

    #[async_trait]
    impl MultipartUpload for GatedAbortUpload {
        fn put_part(&mut self, data: PutPayload) -> UploadPart {
            self.inner.put_part(data)
        }

        async fn complete(&mut self) -> object_store::Result<PutResult> {
            self.inner.complete().await
        }

        async fn abort(&mut self) -> object_store::Result<()> {
            if self.attempts.fetch_add(1, Ordering::SeqCst) == 0 {
                self.started.notify_one();
                futures::future::pending::<()>().await;
            }
            let result = self.inner.abort().await;
            self.finished.notify_one();
            result
        }
    }

    async fn cancel_during_abort_retries_cleanup(fail_complete: bool) {
        let inner = FakeUpload::default();
        {
            let mut state = inner.0.lock().unwrap();
            state.fail_complete = fail_complete;
            state.fail_part_at = (!fail_complete).then_some(0);
        }
        let started = Arc::new(tokio::sync::Notify::new());
        let finished = Arc::new(tokio::sync::Notify::new());
        let attempts = Arc::new(AtomicUsize::new(0));
        let upload = GatedAbortUpload {
            inner: inner.clone(),
            attempts: attempts.clone(),
            started: started.clone(),
            finished: finished.clone(),
        };
        let task = tokio::spawn(async move {
            upload_file_parts(
                &mut reader(pattern(300), false),
                Box::new(upload),
                300,
                100,
                || {},
            )
            .await
        });
        tokio::time::timeout(Duration::from_secs(5), started.notified())
            .await
            .expect("inline abort must start");
        task.abort();
        assert!(task.await.unwrap_err().is_cancelled());
        tokio::time::timeout(Duration::from_secs(5), finished.notified())
            .await
            .expect("drop guard must retry the interrupted abort");
        assert_eq!(attempts.load(Ordering::SeqCst), 2);
        assert_eq!(counts(&inner), (usize::from(fail_complete), 1));
    }

    #[tokio::test]
    async fn cancellation_during_part_error_abort_retries_cleanup() {
        cancel_during_abort_retries_cleanup(false).await;
    }

    #[tokio::test]
    async fn cancellation_during_complete_error_abort_retries_cleanup() {
        cancel_during_abort_retries_cleanup(true).await;
    }
    // END multipart-part-upload tests
}
