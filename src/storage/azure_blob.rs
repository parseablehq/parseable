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
    azure::{MicrosoftAzure, MicrosoftAzureBuilder},
    buffered::BufReader,
    limit::LimitStore,
    path::Path as StorePath,
};
use relative_path::{RelativePath, RelativePathBuf};
use tokio::{fs::OpenOptions, io::AsyncReadExt};
use tracing::error;
use url::Url;

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

#[derive(Debug, Clone, clap::Args)]
#[command(
    name = "Azure config",
    about = "Start Parseable with Azure Blob storage",
    help_template = "\
{about-section}
{all-args}
"
)]
pub struct AzureBlobConfig {
    /// The endpoint to Azure Blob Storage
    /// eg. `https://{account}.blob.core.windows.net`
    #[arg(long, env = "P_AZR_URL", value_name = "url", required = true)]
    pub endpoint_url: String,

    // The Azure Storage Account ID
    #[arg(long, env = "P_AZR_ACCOUNT", value_name = "account", required = true)]
    pub account: String,

    /// The Azure Storage Access key
    #[arg(
        long,
        env = "P_AZR_ACCESS_KEY",
        value_name = "access-key",
        required = false
    )]
    pub access_key: Option<String>,

    ///Client ID
    #[arg(
        long,
        env = "P_AZR_CLIENT_ID",
        value_name = "client-id",
        required = false
    )]
    pub client_id: Option<String>,

    ///Secret ID
    #[arg(
        long,
        env = "P_AZR_CLIENT_SECRET",
        value_name = "client-secret",
        required = false
    )]
    pub client_secret: Option<String>,

    ///Tenant ID
    #[arg(
        long,
        env = "P_AZR_TENANT_ID",
        value_name = "tenant-id",
        required = false
    )]
    pub tenant_id: Option<String>,

    /// The container name to be used for storage
    #[arg(
        long,
        env = "P_AZR_CONTAINER",
        value_name = "container",
        required = true
    )]
    pub container: String,

    /// Cap on concurrent in-flight requests to the object store.
    #[arg(
        long,
        env = "P_MAX_OBJECT_STORE_REQUESTS",
        value_name = "number",
        default_value_t = DEFAULT_MAX_OBJECT_STORE_REQUESTS
    )]
    pub max_object_store_requests: NonZeroUsize,
}

impl AzureBlobConfig {
    fn get_default_builder(&self) -> MicrosoftAzureBuilder {
        let client_options = ClientOptions::default()
            .with_allow_http(true)
            .with_connect_timeout(Duration::from_secs(CONNECT_TIMEOUT_SECS))
            .with_timeout(Duration::from_secs(REQUEST_TIMEOUT_SECS));

        let retry_config = RetryConfig {
            max_retries: 5,
            retry_timeout: Duration::from_secs(RETRY_TIMEOUT_SECS),
            backoff: BackoffConfig::default(),
        };

        let mut builder = MicrosoftAzureBuilder::new()
            .with_endpoint(self.endpoint_url.clone())
            .with_account(&self.account)
            .with_container_name(&self.container)
            .with_retry(retry_config);

        if let Some(access_key) = self.access_key.clone() {
            builder = builder.with_access_key(access_key)
        }

        if let (Some(client_id), Some(client_secret), Some(tenant_id)) = (
            self.client_id.clone(),
            self.client_secret.clone(),
            self.tenant_id.clone(),
        ) {
            builder = builder.with_client_secret_authorization(client_id, client_secret, tenant_id)
        }

        builder.with_client_options(client_options)
    }
}

impl ObjectStorageProvider for AzureBlobConfig {
    fn name(&self) -> &'static str {
        "blob-store"
    }

    fn get_datafusion_runtime(&self) -> RuntimeEnvBuilder {
        let azure = self.get_default_builder().build().unwrap();
        // limit objectstore to a concurrent request limit
        let azure = LimitStore::new(azure, self.max_object_store_requests.get());
        let azure = MetricLayer::new(azure, "azure_blob");

        let object_store_registry = DefaultObjectStoreRegistry::new();
        let url = ObjectStoreUrl::parse(format!("https://{}.blob.core.windows.net", self.account))
            .unwrap();
        object_store_registry.register_store(url.as_ref(), Arc::new(azure));

        RuntimeEnvBuilder::new().with_object_store_registry(Arc::new(object_store_registry))
    }

    fn construct_client(&self) -> Arc<dyn super::ObjectStorage> {
        let azure = self.get_default_builder().build().unwrap();
        // limit objectstore to a concurrent request limit
        let azure = LimitStore::new(azure, self.max_object_store_requests.get());
        let azure = MetricLayer::new(azure, "azure_blob");
        Arc::new(BlobStore {
            client: Arc::new(azure),
            account: self.account.clone(),
            container: self.container.clone(),
            root: StorePath::from(""),
        })
    }

    fn get_endpoint(&self) -> String {
        self.endpoint_url.clone()
    }
}

// ObjStoreClient is generic client to enable interactions with different cloudprovider's
// object store such as S3 and Azure Blob
#[derive(Debug)]
pub struct BlobStore {
    client: Arc<MetricLayer<LimitStore<MicrosoftAzure>>>,
    account: String,
    container: String,
    root: StorePath,
}

impl BlobStore {
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
        name = "azure.parallel_download",
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
        name = "azure.get_object",
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
        name = "azure.put_object",
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
        name = "azure.delete_prefix",
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
        name = "azure.list_dates",
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
        name = "azure.upload_file",
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
        name = "azure.upload_multipart",
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
                error!("Failed to complete multipart upload. {:?}", err);
                async_writer.abort().await?;
                return Err(err.into());
            }
        }
        Ok(())
    }
}

#[async_trait]
impl ObjectStorage for BlobStore {
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
        _path: &RelativePath,
        _tenant_id: &Option<String>,
    ) -> Result<BufReader, ObjectStorageError> {
        Err(ObjectStorageError::UnhandledError(Box::new(
            std::io::Error::new(
                std::io::ErrorKind::Unsupported,
                "Buffered reader not implemented for Blob Storage yet",
            ),
        )))
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
        name = "azure.head",
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
        name = "azure.delete_object",
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
        name = "azure.check",
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
            "Azure Blob Store doesn't implement list_streams".into(),
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
                let path = format!(
                    "https://{}.blob.core.windows.net/{}/{}",
                    self.account, self.container, prefix
                );
                ListingTableUrl::parse(path).unwrap()
            })
            .collect()
    }

    fn store_url(&self) -> Url {
        let url_string = format!("https://{}.blob.core.windows.net", self.account);
        Url::parse(&url_string).unwrap()
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
        self.container.clone()
    }
}
