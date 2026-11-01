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

use object_store::{
    BackoffConfig, ClientOptions, ListResult, ObjectMeta, ObjectStore, ObjectStoreExt, PutPayload,
    RetryConfig,
    aws::{AmazonS3, AmazonS3Builder, AmazonS3ConfigKey, Checksum},
    buffered::BufReader,
    limit::LimitStore,
    path::Path as StorePath,
};
use relative_path::{RelativePath, RelativePathBuf};
use tokio::{fs::OpenOptions, io::AsyncReadExt};
use tracing::error;

use crate::{
    metrics::increment_object_store_calls_by_date,
    parseable::{DEFAULT_TENANT, LogStream, PARSEABLE},
    storage::RETRY_TIMEOUT_SECS,
};

use super::{
    CONNECT_TIMEOUT_SECS, DEFAULT_MAX_OBJECT_STORE_REQUESTS, ObjectStorage, ObjectStorageError,
    ObjectStorageProvider, REQUEST_TIMEOUT_SECS, metrics_layer::MetricLayer,
    object_storage::parseable_json_path, partial_path, to_object_store_path,
};

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
        super::parallel_download(
            self.client.as_ref(),
            path,
            tenant_id.as_deref().unwrap_or(DEFAULT_TENANT),
            partial_path,
            PARSEABLE.options.hot_tier_download_chunk_size,
            PARSEABLE.options.hot_tier_download_concurrency,
        )
        .await
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
        super::get_object(self.client.as_ref(), path, tenant_id).await
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
        let result = self.client.put(&to_object_store_path(path), resource).await;
        super::record_request(result, "PUT", tenant_id).map(|_| ())
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
        super::delete_prefix(
            self.client.as_ref(),
            key,
            tenant_id.as_deref().unwrap_or(DEFAULT_TENANT),
        )
        .await
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
        let prefix = if let Some(tenant) = tenant_id {
            format!("{tenant}/{stream}/")
        } else {
            format!("{stream}/")
        };
        super::list_dates(
            self.client.as_ref(),
            &StorePath::from(prefix.as_str()),
            &prefix,
            tenant_id,
        )
        .await
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
        let bytes = tokio::fs::read(path).await?;
        let result = self.client.put(&key.into(), bytes.into()).await;
        super::record_request(result, "PUT", tenant_id).map(|_| ())
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
        if total_size < min_multipart_size || !PARSEABLE.options.enable_multipart {
            let mut data = Vec::new();
            file.read_to_end(&mut data).await?;

            // Track single PUT operation for small files
            let result = self.client.put(location, data.into()).await;
            super::record_request(result, "PUT", tenant_id)?;

            return Ok(());
        } else {
            let mut async_writer = self.client.put_multipart(location).await?;

            let mut data = Vec::new();
            file.read_to_end(&mut data).await?;

            for part_data in data[..total_size].chunks(min_multipart_size) {
                async_writer.put_part(part_data.to_vec().into()).await?;
                increment_object_store_calls_by_date(
                    "PUT_MULTIPART",
                    &Utc::now().date_naive().to_string(),
                    tenant_str,
                );
            }

            // Track multipart completion
            let complete_result = async_writer.complete().await;
            if let Err(err) = complete_result {
                error!("Failed to complete multipart upload. {:?}", err);
                async_writer.abort().await?;
                return Err(err.into());
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
        tenant_id: &Option<String>,
    ) -> Result<BufReader, ObjectStorageError> {
        let result = self.client.head(&to_object_store_path(path)).await;
        let meta = super::record_request(result, "HEAD", tenant_id)?;
        Ok(BufReader::new(self.client.clone(), &meta))
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
        let result = self.client.head(&to_object_store_path(path)).await;
        super::record_request(result, "HEAD", tenant_id)
    }

    async fn parallel_chunked_download(
        &self,
        path: &RelativePath,
        tenant_id: &Option<String>,
        write_path: PathBuf,
    ) -> Result<(), ObjectStorageError> {
        self._parallel_download(path, tenant_id, write_path).await
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
        super::get_object_ranged(self.client.as_ref(), path, tenant_id).await
    }

    async fn get_objects(
        &self,
        base_path: Option<&RelativePath>,
        filter_func: Box<dyn Fn(String) -> bool + Send>,
        tenant_id: &Option<String>,
    ) -> Result<Vec<Bytes>, ObjectStorageError> {
        let prefix = if let Some(base_path) = base_path {
            to_object_store_path(base_path)
        } else {
            self.root.clone()
        };
        super::get_objects(self.client.as_ref(), self, &prefix, filter_func, tenant_id).await
    }

    async fn get_ingestor_meta_file_paths(
        &self,
        tenant_id: &Option<String>,
    ) -> Result<Vec<RelativePathBuf>, ObjectStorageError> {
        super::get_ingestor_meta_file_paths(self.client.as_ref(), &self.root, tenant_id).await
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
        self._delete_prefix(path.as_ref(), tenant_id).await
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
        let result = self.client.delete(&to_object_store_path(path)).await;
        super::record_request(result, "DELETE", tenant_id)
    }

    #[tracing::instrument(
        name = "s3.check",
        skip(self),
        fields(tenant = ?tenant_id, ok = tracing::field::Empty)
    )]
    async fn check(&self, tenant_id: &Option<String>) -> Result<(), ObjectStorageError> {
        let result = self
            .client
            .head(&to_object_store_path(&parseable_json_path()))
            .await;
        tracing::Span::current().record("ok", result.is_ok());
        super::record_request(result.map(|_| ()), "HEAD", tenant_id)
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
        self._delete_prefix(prefix, tenant_id).await
    }

    async fn try_delete_node_meta(
        &self,
        node_filename: String,
        tenant_id: &Option<String>,
    ) -> Result<(), ObjectStorageError> {
        let file = RelativePathBuf::from(&node_filename);
        let result = self.client.delete(&to_object_store_path(&file)).await;
        super::record_request(result, "DELETE", tenant_id)
    }

    async fn list_streams(&self) -> Result<HashSet<LogStream>, ObjectStorageError> {
        // self._list_streams().await
        Err(ObjectStorageError::Custom(
            "S3 doesn't implement list_streams".into(),
        ))
    }

    async fn list_old_streams(&self) -> Result<HashSet<LogStream>, ObjectStorageError> {
        super::list_old_streams(self.client.as_ref(), "").await
    }

    async fn list_dates(
        &self,
        stream_name: &str,
        tenant_id: &Option<String>,
    ) -> Result<Vec<String>, ObjectStorageError> {
        self._list_dates(stream_name, tenant_id).await
    }

    async fn list_hours(
        &self,
        stream_name: &str,
        date: &str,
        tenant_id: &Option<String>,
    ) -> Result<Vec<String>, ObjectStorageError> {
        let prefix = if let Some(tenant) = tenant_id {
            format!("{tenant}/{stream_name}/{date}/")
        } else {
            format!("{stream_name}/{date}/")
        };
        super::list_partitions(self.client.as_ref(), &prefix, "hour=", tenant_id).await
    }

    async fn list_minutes(
        &self,
        stream_name: &str,
        date: &str,
        hour: &str,
        tenant_id: &Option<String>,
    ) -> Result<Vec<String>, ObjectStorageError> {
        let prefix = if let Some(tenant) = tenant_id {
            format!("{tenant}/{stream_name}/{date}/{hour}/")
        } else {
            format!("{stream_name}/{date}/{hour}/")
        };
        super::list_partitions(self.client.as_ref(), &prefix, "minute=", tenant_id).await
    }

    async fn upload_file(
        &self,
        key: &str,
        path: &Path,
        tenant_id: &Option<String>,
    ) -> Result<(), ObjectStorageError> {
        self._upload_file(key, path, tenant_id).await
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
        super::list_dirs(self.client.as_ref(), &StorePath::from("/"), tenant_id).await
    }

    async fn list_dirs_relative(
        &self,
        relative_path: &RelativePath,
        tenant_id: &Option<String>,
    ) -> Result<Vec<String>, ObjectStorageError> {
        let prefix = StorePath::from(relative_path.as_str());
        super::list_dirs(self.client.as_ref(), &prefix, tenant_id).await
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
