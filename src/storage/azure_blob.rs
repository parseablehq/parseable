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
    path::Path,
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
    time::{Duration, Instant},
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
    BackoffConfig, ClientOptions, ListResult, ObjectMeta, ObjectStore, PutPayload, RetryConfig,
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
    metrics::{
        STORAGE_REQUEST_RESPONSE_TIME, increment_files_scanned_in_object_store_calls_by_date,
        increment_object_store_calls_by_date,
    },
    parseable::LogStream,
};

use super::{
    CONNECT_TIMEOUT_SECS, MIN_MULTIPART_UPLOAD_SIZE, ObjectStorage, ObjectStorageError,
    ObjectStorageProvider, PARSEABLE_ROOT_DIRECTORY, REQUEST_TIMEOUT_SECS,
    STREAM_METADATA_FILE_NAME, metrics_layer::MetricLayer, metrics_layer::error_to_status_code,
    object_storage::parseable_json_path, to_object_store_path,
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
}

impl AzureBlobConfig {
    fn get_default_builder(&self) -> MicrosoftAzureBuilder {
        let client_options = ClientOptions::default()
            .with_allow_http(true)
            .with_connect_timeout(Duration::from_secs(CONNECT_TIMEOUT_SECS))
            .with_timeout(Duration::from_secs(REQUEST_TIMEOUT_SECS));

        let retry_config = RetryConfig {
            max_retries: 5,
            retry_timeout: Duration::from_secs(120),
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
        let azure = LimitStore::new(azure, super::MAX_OBJECT_STORE_REQUESTS);
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
        let azure = LimitStore::new(azure, super::MAX_OBJECT_STORE_REQUESTS);
        Arc::new(BlobStore {
            client: azure,
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
    client: LimitStore<MicrosoftAzure>,
    account: String,
    container: String,
    root: StorePath,
}

impl BlobStore {
    async fn _get_object(&self, path: &RelativePath) -> Result<Bytes, ObjectStorageError> {
        let time = std::time::Instant::now();
        let resp = self.client.get(&to_object_store_path(path)).await;
        let elapsed = time.elapsed().as_secs_f64();

        increment_object_store_calls_by_date(
            "azure_blob",
            "GET",
            &Utc::now().date_naive().to_string(),
        );

        match resp {
            Ok(resp) => {
                let body: Bytes = resp.bytes().await.unwrap();
                STORAGE_REQUEST_RESPONSE_TIME
                    .with_label_values(&["azure_blob", "GET", "200"])
                    .observe(elapsed);
                increment_files_scanned_in_object_store_calls_by_date(
                    "azure_blob",
                    "GET",
                    1,
                    &Utc::now().date_naive().to_string(),
                );
                Ok(body)
            }
            Err(err) => {
                let status_code = error_to_status_code(&err);
                STORAGE_REQUEST_RESPONSE_TIME
                    .with_label_values(&["azure_blob", "GET", status_code])
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

        increment_object_store_calls_by_date(
            "azure_blob",
            "PUT",
            &Utc::now().date_naive().to_string(),
        );
        match resp {
            Ok(_) => {
                STORAGE_REQUEST_RESPONSE_TIME
                    .with_label_values(&["azure_blob", "PUT", "200"])
                    .observe(elapsed);
                increment_files_scanned_in_object_store_calls_by_date(
                    "azure_blob",
                    "PUT",
                    1,
                    &Utc::now().date_naive().to_string(),
                );
                Ok(())
            }
            Err(err) => {
                let status_code = error_to_status_code(&err);
                STORAGE_REQUEST_RESPONSE_TIME
                    .with_label_values(&["azure_blob", "PUT", status_code])
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
            .with_label_values(&["azure_blob", "LIST", "200"])
            .observe(list_elapsed);
        increment_object_store_calls_by_date(
            "azure_blob",
            "LIST",
            &Utc::now().date_naive().to_string(),
        );

        object_stream
            .for_each_concurrent(None, |x| async {
                files_scanned.fetch_add(1, Ordering::Relaxed);

                match x {
                    Ok(obj) => {
                        files_deleted.fetch_add(1, Ordering::Relaxed);
                        let delete_start = Instant::now();
                        let delete_resp = self.client.delete(&obj.location).await;
                        let delete_elapsed = delete_start.elapsed().as_secs_f64();
                        increment_object_store_calls_by_date(
                            "azure_blob",
                            "DELETE",
                            &Utc::now().date_naive().to_string(),
                        );
                        match delete_resp {
                            Ok(_) => {
                                STORAGE_REQUEST_RESPONSE_TIME
                                    .with_label_values(&["azure_blob", "DELETE", "200"])
                                    .observe(delete_elapsed);
                            }
                            Err(err) => {
                                let status_code = error_to_status_code(&err);
                                STORAGE_REQUEST_RESPONSE_TIME
                                    .with_label_values(&["azure_blob", "DELETE", status_code])
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

        increment_files_scanned_in_object_store_calls_by_date(
            "azure_blob",
            "LIST",
            files_scanned.load(Ordering::Relaxed),
            &Utc::now().date_naive().to_string(),
        );
        increment_files_scanned_in_object_store_calls_by_date(
            "azure_blob",
            "DELETE",
            files_deleted.load(Ordering::Relaxed),
            &Utc::now().date_naive().to_string(),
        );
        // Note: Individual DELETE calls are tracked inside the concurrent loop
        Ok(())
    }

    async fn _list_dates(&self, stream: &str) -> Result<Vec<String>, ObjectStorageError> {
        let list_start = Instant::now();
        let resp: Result<object_store::ListResult, object_store::Error> = self
            .client
            .list_with_delimiter(Some(&(stream.into())))
            .await;
        let list_elapsed = list_start.elapsed().as_secs_f64();
        increment_object_store_calls_by_date(
            "azure_blob",
            "LIST",
            &Utc::now().date_naive().to_string(),
        );

        let resp = match resp {
            Ok(resp) => {
                STORAGE_REQUEST_RESPONSE_TIME
                    .with_label_values(&["azure_blob", "LIST", "200"])
                    .observe(list_elapsed);
                resp
            }
            Err(err) => {
                let status_code = error_to_status_code(&err);
                STORAGE_REQUEST_RESPONSE_TIME
                    .with_label_values(&["azure_blob", "LIST", status_code])
                    .observe(list_elapsed);
                return Err(err.into());
            }
        };

        let common_prefixes = resp.common_prefixes;

        increment_files_scanned_in_object_store_calls_by_date(
            "azure_blob",
            "LIST",
            common_prefixes.len() as u64,
            &Utc::now().date_naive().to_string(),
        );

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

        increment_object_store_calls_by_date(
            "azure_blob",
            "PUT",
            &Utc::now().date_naive().to_string(),
        );
        match result {
            Ok(_) => {
                STORAGE_REQUEST_RESPONSE_TIME
                    .with_label_values(&["azure_blob", "PUT", "200"])
                    .observe(put_elapsed);
                increment_files_scanned_in_object_store_calls_by_date(
                    "azure_blob",
                    "PUT",
                    1,
                    &Utc::now().date_naive().to_string(),
                );
                Ok(())
            }
            Err(err) => {
                let status_code = error_to_status_code(&err);
                STORAGE_REQUEST_RESPONSE_TIME
                    .with_label_values(&["azure_blob", "PUT", status_code])
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
        increment_object_store_calls_by_date(
            "azure_blob",
            "PUT_MULTIPART",
            &Utc::now().date_naive().to_string(),
        );
        let mut async_writer = match async_writer {
            Ok(writer) => {
                STORAGE_REQUEST_RESPONSE_TIME
                    .with_label_values(&["azure_blob", "PUT_MULTIPART", "200"])
                    .observe(multipart_elapsed);
                writer
            }
            Err(err) => {
                let status_code = error_to_status_code(&err);
                STORAGE_REQUEST_RESPONSE_TIME
                    .with_label_values(&["azure_blob", "PUT_MULTIPART", status_code])
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
            increment_object_store_calls_by_date(
                "azure_blob",
                "PUT",
                &Utc::now().date_naive().to_string(),
            );

            match result {
                Ok(_) => {
                    STORAGE_REQUEST_RESPONSE_TIME
                        .with_label_values(&["azure_blob", "PUT", "200"])
                        .observe(put_elapsed);
                    increment_files_scanned_in_object_store_calls_by_date(
                        "azure_blob",
                        "PUT",
                        1,
                        &Utc::now().date_naive().to_string(),
                    );
                }
                Err(err) => {
                    let status_code = error_to_status_code(&err);
                    STORAGE_REQUEST_RESPONSE_TIME
                        .with_label_values(&["azure_blob", "PUT", status_code])
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
                increment_object_store_calls_by_date(
                    "azure_blob",
                    "PUT_MULTIPART",
                    &Utc::now().date_naive().to_string(),
                );
                match result {
                    Ok(_) => {
                        STORAGE_REQUEST_RESPONSE_TIME
                            .with_label_values(&["azure_blob", "PUT_MULTIPART", "200"])
                            .observe(part_elapsed);
                    }
                    Err(err) => {
                        let status_code = error_to_status_code(&err);
                        STORAGE_REQUEST_RESPONSE_TIME
                            .with_label_values(&["azure_blob", "PUT_MULTIPART", status_code])
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
                    .with_label_values(&["azure_blob", "PUT_MULTIPART_COMPLETE", status_code])
                    .observe(complete_elapsed);
                error!("Failed to complete multipart upload. {:?}", err);
                async_writer.abort().await?;
                return Err(err.into());
            } else {
                STORAGE_REQUEST_RESPONSE_TIME
                    .with_label_values(&["azure_blob", "PUT_MULTIPART_COMPLETE", "200"])
                    .observe(complete_elapsed);
            }
        }
        Ok(())
    }
}

#[async_trait]
impl ObjectStorage for BlobStore {
    async fn get_buffered_reader(
        &self,
        _path: &RelativePath,
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
    ) -> Result<(), ObjectStorageError> {
        self._upload_multipart(key, path).await
    }

    async fn head(&self, path: &RelativePath) -> Result<ObjectMeta, ObjectStorageError> {
        let head_start = Instant::now();
        let result = self.client.head(&to_object_store_path(path)).await;
        let head_elapsed = head_start.elapsed().as_secs_f64();

        increment_object_store_calls_by_date(
            "azure_blob",
            "HEAD",
            &Utc::now().date_naive().to_string(),
        );
        match &result {
            Ok(_) => {
                STORAGE_REQUEST_RESPONSE_TIME
                    .with_label_values(&["azure_blob", "HEAD", "200"])
                    .observe(head_elapsed);
                increment_files_scanned_in_object_store_calls_by_date(
                    "azure_blob",
                    "HEAD",
                    1,
                    &Utc::now().date_naive().to_string(),
                );
            }
            Err(err) => {
                let status_code = error_to_status_code(err);
                STORAGE_REQUEST_RESPONSE_TIME
                    .with_label_values(&["azure_blob", "HEAD", status_code])
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
                .with_label_values(&["azure_blob", "GET", "200"])
                .observe(list_start.elapsed().as_secs_f64());
            increment_files_scanned_in_object_store_calls_by_date(
                "azure_blob",
                "GET",
                1,
                &Utc::now().date_naive().to_string(),
            );
            increment_object_store_calls_by_date(
                "azure_blob",
                "GET",
                &Utc::now().date_naive().to_string(),
            );
            res.push(byts);
        }
        let list_elapsed = list_start.elapsed().as_secs_f64();
        STORAGE_REQUEST_RESPONSE_TIME
            .with_label_values(&["azure_blob", "LIST", "200"])
            .observe(list_elapsed);

        // Record total files scanned
        increment_files_scanned_in_object_store_calls_by_date(
            "azure_blob",
            "LIST",
            files_scanned as u64,
            &Utc::now().date_naive().to_string(),
        );
        increment_object_store_calls_by_date(
            "azure_blob",
            "LIST",
            &Utc::now().date_naive().to_string(),
        );
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
        increment_object_store_calls_by_date(
            "azure_blob",
            "LIST",
            &Utc::now().date_naive().to_string(),
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
        let list_elapsed = list_start.elapsed().as_secs_f64();
        STORAGE_REQUEST_RESPONSE_TIME
            .with_label_values(&["azure_blob", "LIST", "200"])
            .observe(list_elapsed);
        // Record total files scanned
        increment_files_scanned_in_object_store_calls_by_date(
            "azure_blob",
            "LIST",
            files_scanned as u64,
            &Utc::now().date_naive().to_string(),
        );
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
        increment_object_store_calls_by_date(
            "azure_blob",
            "DELETE",
            &Utc::now().date_naive().to_string(),
        );
        match &result {
            Ok(_) => {
                STORAGE_REQUEST_RESPONSE_TIME
                    .with_label_values(&["azure_blob", "DELETE", "200"])
                    .observe(delete_elapsed);
                // Record single file deleted
                increment_files_scanned_in_object_store_calls_by_date(
                    "azure_blob",
                    "DELETE",
                    1,
                    &Utc::now().date_naive().to_string(),
                );
            }
            Err(err) => {
                let status_code = error_to_status_code(err);
                STORAGE_REQUEST_RESPONSE_TIME
                    .with_label_values(&["azure_blob", "DELETE", status_code])
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
        increment_object_store_calls_by_date(
            "azure_blob",
            "HEAD",
            &Utc::now().date_naive().to_string(),
        );

        match &result {
            Ok(_) => {
                STORAGE_REQUEST_RESPONSE_TIME
                    .with_label_values(&["azure_blob", "HEAD", "200"])
                    .observe(head_elapsed);
                increment_files_scanned_in_object_store_calls_by_date(
                    "azure_blob",
                    "HEAD",
                    1,
                    &Utc::now().date_naive().to_string(),
                );
            }
            Err(err) => {
                let status_code = error_to_status_code(err);
                STORAGE_REQUEST_RESPONSE_TIME
                    .with_label_values(&["azure_blob", "HEAD", status_code])
                    .observe(head_elapsed);
            }
        }

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

        increment_object_store_calls_by_date(
            "azure_blob",
            "DELETE",
            &Utc::now().date_naive().to_string(),
        );
        match result {
            Ok(_) => {
                STORAGE_REQUEST_RESPONSE_TIME
                    .with_label_values(&["azure_blob", "DELETE", "200"])
                    .observe(delete_elapsed);
                increment_files_scanned_in_object_store_calls_by_date(
                    "azure_blob",
                    "DELETE",
                    1,
                    &Utc::now().date_naive().to_string(),
                );
                Ok(())
            }
            Err(err) => {
                let status_code = error_to_status_code(&err);
                STORAGE_REQUEST_RESPONSE_TIME
                    .with_label_values(&["azure_blob", "DELETE", status_code])
                    .observe(delete_elapsed);

                Err(err.into())
            }
        }
    }

    async fn list_streams(&self) -> Result<HashSet<LogStream>, ObjectStorageError> {
        // self._list_streams().await
        Err(ObjectStorageError::Custom(
            "Azure Blob Store doesn't implement list_streams".into(),
        ))
    }

    async fn list_old_streams(&self) -> Result<HashSet<LogStream>, ObjectStorageError> {
        // Track LIST operation
        let list_start = Instant::now();
        let resp = self.client.list_with_delimiter(None).await?;
        let list_elapsed = list_start.elapsed().as_secs_f64();
        STORAGE_REQUEST_RESPONSE_TIME
            .with_label_values(&["azure_blob", "LIST", "200"])
            .observe(list_elapsed);

        let common_prefixes = resp.common_prefixes; // get all dirs
        increment_files_scanned_in_object_store_calls_by_date(
            "azure_blob",
            "LIST",
            common_prefixes.len() as u64,
            &Utc::now().date_naive().to_string(),
        );
        increment_object_store_calls_by_date(
            "azure_blob",
            "LIST",
            &Utc::now().date_naive().to_string(),
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
                let head_start = Instant::now();
                let result = self.client.head(&StorePath::from(key)).await;
                let head_elapsed = head_start.elapsed().as_secs_f64();
                increment_object_store_calls_by_date(
                    "azure_blob",
                    "HEAD",
                    &Utc::now().date_naive().to_string(),
                );
                match &result {
                    Ok(_) => {
                        STORAGE_REQUEST_RESPONSE_TIME
                            .with_label_values(&["azure_blob", "HEAD", "200"])
                            .observe(head_elapsed);
                    }
                    Err(err) => {
                        let status_code = error_to_status_code(err);
                        STORAGE_REQUEST_RESPONSE_TIME
                            .with_label_values(&["azure_blob", "HEAD", status_code])
                            .observe(head_elapsed);
                    }
                }

                result.map(|_| ())
            };
            stream_json_check.push(task);
        }
        increment_files_scanned_in_object_store_calls_by_date(
            "azure_blob",
            "HEAD",
            dirs.len() as u64,
            &Utc::now().date_naive().to_string(),
        );
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
            .with_label_values(&["azure_blob", "LIST", "200"])
            .observe(list_elapsed);
        increment_files_scanned_in_object_store_calls_by_date(
            "azure_blob",
            "LIST",
            resp.common_prefixes.len() as u64,
            &Utc::now().date_naive().to_string(),
        );
        increment_object_store_calls_by_date(
            "azure_blob",
            "LIST",
            &Utc::now().date_naive().to_string(),
        );

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
            .with_label_values(&["azure_blob", "LIST", "200"])
            .observe(list_elapsed);
        increment_files_scanned_in_object_store_calls_by_date(
            "azure_blob",
            "LIST",
            resp.common_prefixes.len() as u64,
            &Utc::now().date_naive().to_string(),
        );
        increment_object_store_calls_by_date(
            "azure_blob",
            "LIST",
            &Utc::now().date_naive().to_string(),
        );
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

    async fn list_dirs(&self) -> Result<Vec<String>, ObjectStorageError> {
        let pre = object_store::path::Path::from("/");

        let list_start = Instant::now();
        let resp = self.client.list_with_delimiter(Some(&pre)).await;
        let list_elapsed = list_start.elapsed().as_secs_f64();
        increment_object_store_calls_by_date(
            "azure_blob",
            "LIST",
            &Utc::now().date_naive().to_string(),
        );
        let resp = match resp {
            Ok(resp) => {
                STORAGE_REQUEST_RESPONSE_TIME
                    .with_label_values(&["azure_blob", "LIST", "200"])
                    .observe(list_elapsed);
                increment_files_scanned_in_object_store_calls_by_date(
                    "azure_blob",
                    "LIST",
                    resp.common_prefixes.len() as u64,
                    &Utc::now().date_naive().to_string(),
                );

                resp
            }
            Err(err) => {
                let status_code = error_to_status_code(&err);
                STORAGE_REQUEST_RESPONSE_TIME
                    .with_label_values(&["azure_blob", "LIST", status_code])
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
        increment_object_store_calls_by_date(
            "azure_blob",
            "LIST",
            &Utc::now().date_naive().to_string(),
        );
        let resp = match resp {
            Ok(resp) => {
                STORAGE_REQUEST_RESPONSE_TIME
                    .with_label_values(&["azure_blob", "LIST", "200"])
                    .observe(list_elapsed);
                increment_files_scanned_in_object_store_calls_by_date(
                    "azure_blob",
                    "LIST",
                    resp.common_prefixes.len() as u64,
                    &Utc::now().date_naive().to_string(),
                );

                resp
            }
            Err(err) => {
                let status_code = error_to_status_code(&err);
                STORAGE_REQUEST_RESPONSE_TIME
                    .with_label_values(&["azure_blob", "LIST", status_code])
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
