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
    num::NonZeroUsize,
    path::{Path, PathBuf},
    sync::Arc,
    time::Duration,
};

use crate::{
    metrics::increment_object_store_calls_by_date,
    parseable::{DEFAULT_TENANT, LogStream, PARSEABLE},
    storage::RETRY_TIMEOUT_SECS,
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
    buffered::BufReader,
    gcp::{GoogleCloudStorage, GoogleCloudStorageBuilder},
    limit::LimitStore,
    path::Path as StorePath,
};
use relative_path::{RelativePath, RelativePathBuf};
use tokio::{fs::OpenOptions, io::AsyncReadExt};

use tracing::error;

use super::{
    CONNECT_TIMEOUT_SECS, DEFAULT_MAX_OBJECT_STORE_REQUESTS, ObjectStorage, ObjectStorageError,
    ObjectStorageProvider, REQUEST_TIMEOUT_SECS, metrics_layer::MetricLayer,
    object_storage::parseable_json_path, partial_path, to_object_store_path,
};

#[derive(Debug, Clone, clap::Args)]
#[command(
    name = "GCS config",
    about = "Start Parseable with GCS or compatible as storage",
    help_template = "\
{about-section}
{all-args}
"
)]
pub struct GcsConfig {
    /// The endpoint to GCS or compatible object storage platform
    #[arg(
        long,
        env = "P_GCS_URL",
        value_name = "url",
        default_value = "https://storage.googleapis.com",
        required = false
    )]
    pub endpoint_url: String,

    /// The GCS or compatible object storage bucket to be used for storage
    #[arg(
        long,
        env = "P_GCS_BUCKET",
        value_name = "bucket-name",
        required = true
    )]
    pub bucket_name: String,

    /// Set client to skip tls verification
    #[arg(
        long,
        env = "P_GCS_TLS_SKIP_VERIFY",
        value_name = "bool",
        default_value = "false"
    )]
    pub skip_tls: bool,

    /// Cap on concurrent in-flight requests to the object store.
    #[arg(
        long,
        env = "P_MAX_OBJECT_STORE_REQUESTS",
        value_name = "number",
        default_value_t = DEFAULT_MAX_OBJECT_STORE_REQUESTS
    )]
    pub max_object_store_requests: NonZeroUsize,
}

impl GcsConfig {
    fn get_default_builder(&self) -> GoogleCloudStorageBuilder {
        let mut client_options = ClientOptions::default()
            .with_allow_http(true)
            .with_connect_timeout(Duration::from_secs(CONNECT_TIMEOUT_SECS))
            .with_timeout(Duration::from_secs(REQUEST_TIMEOUT_SECS));

        if self.skip_tls {
            client_options = client_options.with_allow_invalid_certificates(true)
        }
        let retry_config = RetryConfig {
            max_retries: 5,
            retry_timeout: Duration::from_secs(RETRY_TIMEOUT_SECS),
            backoff: BackoffConfig::default(),
        };

        let builder = GoogleCloudStorageBuilder::from_env()
            .with_bucket_name(&self.bucket_name)
            .with_retry(retry_config);

        builder.with_client_options(client_options)
    }
}

impl ObjectStorageProvider for GcsConfig {
    fn name(&self) -> &'static str {
        "gcs"
    }

    fn get_datafusion_runtime(&self) -> RuntimeEnvBuilder {
        let gcs = self.get_default_builder().build().unwrap();

        // limit objectstore to a concurrent request limit
        let gcs = LimitStore::new(gcs, self.max_object_store_requests.get());
        let gcs = MetricLayer::new(gcs, "gcs");

        let object_store_registry = DefaultObjectStoreRegistry::new();
        // Register GCS client under the "gs://" scheme so DataFusion can route
        // object store calls to our GoogleCloudStorage implementation
        let url = ObjectStoreUrl::parse(format!("gs://{}", self.bucket_name)).unwrap();
        object_store_registry.register_store(url.as_ref(), Arc::new(gcs));

        RuntimeEnvBuilder::new().with_object_store_registry(Arc::new(object_store_registry))
    }

    fn construct_client(&self) -> Arc<dyn ObjectStorage> {
        let gcs = self.get_default_builder().build().unwrap();
        // limit objectstore to a concurrent request limit
        let gcs = LimitStore::new(gcs, self.max_object_store_requests.get());
        let gcs = MetricLayer::new(gcs, "gcs");
        Arc::new(Gcs {
            client: Arc::new(gcs),
            bucket: self.bucket_name.clone(),
            root: StorePath::from(""),
        })
    }

    fn get_endpoint(&self) -> String {
        format!("{}/{}", self.endpoint_url, self.bucket_name)
    }

    fn get_object_store(&self) -> Arc<dyn ObjectStorage> {
        static STORE: once_cell::sync::OnceCell<Arc<dyn ObjectStorage>> =
            once_cell::sync::OnceCell::new();

        STORE.get_or_init(|| self.construct_client()).clone()
    }
}

#[derive(Debug)]
pub struct Gcs {
    client: Arc<MetricLayer<LimitStore<GoogleCloudStorage>>>,
    bucket: String,
    root: StorePath,
}

impl Gcs {
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
        name = "gcs.parallel_download",
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
        name = "gcs.get_object",
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
        name = "gcs.put_object",
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
        name = "gcs.delete_prefix",
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
        name = "gcs.list_dates",
        skip(self),
        fields(stream = %stream, tenant = ?tenant_id, dates = tracing::field::Empty)
    )]
    async fn _list_dates(
        &self,
        stream: &str,
        tenant_id: &Option<String>,
    ) -> Result<Vec<String>, ObjectStorageError> {
        super::list_dates(
            self.client.as_ref(),
            &StorePath::from(stream),
            &format!("{stream}/"),
            tenant_id,
        )
        .await
    }

    #[tracing::instrument(
        name = "gcs.upload_file",
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
        name = "gcs.upload_multipart",
        skip(self),
        fields(key = %key, path = %path.display(), tenant = ?tenant_id, total_bytes = tracing::field::Empty)
    )]
    async fn _upload_multipart(
        &self,
        key: &RelativePath,
        path: &Path,
        tenant_id: &Option<String>,
    ) -> Result<(), ObjectStorageError> {
        let mut file = OpenOptions::new().read(true).open(path).await?;
        let location = &to_object_store_path(key);
        let tenant = tenant_id.as_deref().unwrap_or(DEFAULT_TENANT);
        let meta = file.metadata().await?;
        let total_size = meta.len() as usize;
        tracing::Span::current().record("total_bytes", total_size);
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
                    tenant,
                );
            }

            // Track multipart completion
            let complete_result = async_writer.complete().await;
            if let Err(err) = complete_result {
                if let Err(abort_err) = async_writer.abort().await {
                    error!(
                        "Failed to abort multipart upload after completion failure: {:?}",
                        abort_err
                    );
                }
                return Err(err.into());
            }
        }
        Ok(())
    }
}

#[async_trait]
impl ObjectStorage for Gcs {
    async fn parallel_chunked_download(
        &self,
        path: &RelativePath,
        tenant_id: &Option<String>,
        write_path: PathBuf,
    ) -> Result<(), ObjectStorageError> {
        self._parallel_download(path, tenant_id, write_path).await
    }
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
        name = "gcs.head",
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
        name = "gcs.delete_object",
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
        name = "gcs.check",
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
            "GCS doesn't implement list_streams".into(),
        ))
    }

    async fn list_old_streams(&self) -> Result<HashSet<LogStream>, ObjectStorageError> {
        super::list_old_streams(self.client.as_ref(), DEFAULT_TENANT).await
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
        let prefix = format!("{stream_name}/{date}/");
        super::list_partitions(self.client.as_ref(), &prefix, "hour=", tenant_id).await
    }

    async fn list_minutes(
        &self,
        stream_name: &str,
        date: &str,
        hour: &str,
        tenant_id: &Option<String>,
    ) -> Result<Vec<String>, ObjectStorageError> {
        let prefix = format!("{stream_name}/{date}/{hour}/");
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
                let path = format!("gs://{}/{}", self.bucket, prefix);
                ListingTableUrl::parse(path).unwrap()
            })
            .collect()
    }

    fn store_url(&self) -> url::Url {
        url::Url::parse(&format!("gs://{}", self.bucket)).unwrap()
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

    async fn list_with_delimiter(
        &self,
        prefix: Option<object_store::path::Path>,
    ) -> Result<ListResult, ObjectStorageError> {
        Ok(self.client.list_with_delimiter(prefix.as_ref()).await?)
    }

    fn get_bucket_name(&self) -> String {
        self.bucket.clone()
    }
}
