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
    path::Path,
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
    time::Duration,
};

use crate::{
    metrics::{
        increment_bytes_scanned_in_object_store_calls_by_date,
        increment_files_scanned_in_object_store_calls_by_date,
        increment_object_store_calls_by_date,
    },
    parseable::{DEFAULT_TENANT, LogStream},
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
    buffered::BufReader,
    gcp::{GoogleCloudStorage, GoogleCloudStorageBuilder},
    limit::LimitStore,
    path::Path as StorePath,
};
use relative_path::{RelativePath, RelativePathBuf};
use tokio::{fs::OpenOptions, io::AsyncReadExt};
use tracing::error;

use super::{
    CONNECT_TIMEOUT_SECS, MIN_MULTIPART_UPLOAD_SIZE, ObjectStorage, ObjectStorageError,
    ObjectStorageProvider, PARSEABLE_ROOT_DIRECTORY, REQUEST_TIMEOUT_SECS,
    STREAM_METADATA_FILE_NAME, metrics_layer::MetricLayer, object_storage::parseable_json_path,
    to_object_store_path,
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
            retry_timeout: Duration::from_secs(30),
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
        let gcs = LimitStore::new(gcs, super::MAX_OBJECT_STORE_REQUESTS);
        let gcs = MetricLayer::new(gcs, "gcs");

        let object_store_registry = DefaultObjectStoreRegistry::new();
        // Register GCS client under the "gs://" scheme so DataFusion can route
        // object store calls to our GoogleCloudStorage implementation
        let url = ObjectStoreUrl::parse(format!("gs://{}", &self.bucket_name)).unwrap();
        object_store_registry.register_store(url.as_ref(), Arc::new(gcs));

        RuntimeEnvBuilder::new().with_object_store_registry(Arc::new(object_store_registry))
    }

    fn construct_client(&self) -> Arc<dyn ObjectStorage> {
        let gcs = self.get_default_builder().build().unwrap();

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
    client: Arc<GoogleCloudStorage>,
    bucket: String,
    root: StorePath,
}

impl Gcs {
    async fn _get_object(
        &self,
        path: &RelativePath,
        tenant_id: &Option<String>,
    ) -> Result<Bytes, ObjectStorageError> {
        let resp = self.client.get(&to_object_store_path(path)).await;
        let tenant = tenant_id.as_deref().unwrap_or(DEFAULT_TENANT);
        increment_object_store_calls_by_date("GET", &Utc::now().date_naive().to_string(), tenant);
        match resp {
            Ok(resp) => {
                let body: Bytes = resp.bytes().await?;
                increment_files_scanned_in_object_store_calls_by_date(
                    "GET",
                    1,
                    &Utc::now().date_naive().to_string(),
                    tenant,
                );
                increment_bytes_scanned_in_object_store_calls_by_date(
                    "GET",
                    body.len() as u64,
                    &Utc::now().date_naive().to_string(),
                    tenant,
                );
                Ok(body)
            }
            Err(err) => Err(err.into()),
        }
    }

    async fn _put_object(
        &self,
        path: &RelativePath,
        resource: PutPayload,
        tenant_id: &Option<String>,
    ) -> Result<(), ObjectStorageError> {
        let tenant = tenant_id.as_deref().unwrap_or(DEFAULT_TENANT);
        let resp = self.client.put(&to_object_store_path(path), resource).await;
        increment_object_store_calls_by_date("PUT", &Utc::now().date_naive().to_string(), tenant);
        match resp {
            Ok(_) => {
                increment_files_scanned_in_object_store_calls_by_date(
                    "PUT",
                    1,
                    &Utc::now().date_naive().to_string(),
                    tenant,
                );
                Ok(())
            }
            Err(err) => Err(err.into()),
        }
    }

    async fn _delete_prefix(
        &self,
        key: &str,
        tenant_id: &Option<String>,
    ) -> Result<(), ObjectStorageError> {
        let files_scanned = Arc::new(AtomicU64::new(0));
        let files_deleted = Arc::new(AtomicU64::new(0));
        let tenant = tenant_id.as_deref().unwrap_or(DEFAULT_TENANT);
        // Track LIST operation
        let object_stream = self.client.list(Some(&(key.into())));
        increment_object_store_calls_by_date("LIST", &Utc::now().date_naive().to_string(), tenant);
        object_stream
            .for_each_concurrent(None, |x| async {
                files_scanned.fetch_add(1, Ordering::Relaxed);

                match x {
                    Ok(obj) => {
                        files_deleted.fetch_add(1, Ordering::Relaxed);
                        let delete_resp = self.client.delete(&obj.location).await;
                        increment_object_store_calls_by_date(
                            "DELETE",
                            &Utc::now().date_naive().to_string(),
                            tenant,
                        );
                        if delete_resp.is_err() {
                            error!(
                                "Failed to delete object during delete stream: {:?}",
                                delete_resp
                            );
                        }
                    }
                    Err(err) => {
                        error!("Failed to fetch object during delete stream: {:?}", err);
                    }
                };
            })
            .await;

        increment_files_scanned_in_object_store_calls_by_date(
            "LIST",
            files_scanned.load(Ordering::Relaxed),
            &Utc::now().date_naive().to_string(),
            tenant,
        );
        increment_files_scanned_in_object_store_calls_by_date(
            "DELETE",
            files_deleted.load(Ordering::Relaxed),
            &Utc::now().date_naive().to_string(),
            tenant,
        );
        Ok(())
    }

    async fn _list_dates(
        &self,
        stream: &str,
        tenant_id: &Option<String>,
    ) -> Result<Vec<String>, ObjectStorageError> {
        let resp: Result<object_store::ListResult, object_store::Error> = self
            .client
            .list_with_delimiter(Some(&(stream.into())))
            .await;
        let tenant = tenant_id.as_deref().unwrap_or(DEFAULT_TENANT);
        increment_object_store_calls_by_date("LIST", &Utc::now().date_naive().to_string(), tenant);

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
            tenant,
        );

        // return prefixes at the root level
        let dates: Vec<_> = common_prefixes
            .iter()
            .filter_map(|path| path.as_ref().strip_prefix(&format!("{stream}/")))
            .map(String::from)
            .collect();

        Ok(dates)
    }

    async fn _upload_file(
        &self,
        key: &str,
        path: &Path,
        tenant_id: &Option<String>,
    ) -> Result<(), ObjectStorageError> {
        let bytes = tokio::fs::read(path).await?;
        let tenant = tenant_id.as_deref().unwrap_or(DEFAULT_TENANT);
        let result = self.client.put(&key.into(), bytes.into()).await;
        increment_object_store_calls_by_date("PUT", &Utc::now().date_naive().to_string(), tenant);
        match result {
            Ok(_) => {
                increment_files_scanned_in_object_store_calls_by_date(
                    "PUT",
                    1,
                    &Utc::now().date_naive().to_string(),
                    tenant,
                );
                Ok(())
            }
            Err(err) => Err(err.into()),
        }
    }

    async fn _upload_multipart(
        &self,
        key: &RelativePath,
        path: &Path,
        tenant_id: &Option<String>,
    ) -> Result<(), ObjectStorageError> {
        let mut file = OpenOptions::new().read(true).open(path).await?;
        let location = &to_object_store_path(key);
        let tenant = tenant_id.as_deref().unwrap_or(DEFAULT_TENANT);
        let async_writer = self.client.put_multipart(location).await;
        let mut async_writer = match async_writer {
            Ok(writer) => writer,
            Err(err) => {
                return Err(err.into());
            }
        };

        let meta = file.metadata().await?;
        let total_size = meta.len() as usize;
        if total_size < MIN_MULTIPART_UPLOAD_SIZE {
            let mut data = Vec::new();
            file.read_to_end(&mut data).await?;

            // Track single PUT operation for small files
            let result = self.client.put(location, data.into()).await;
            increment_object_store_calls_by_date(
                "PUT",
                &Utc::now().date_naive().to_string(),
                tenant,
            );
            match result {
                Ok(_) => {
                    increment_files_scanned_in_object_store_calls_by_date(
                        "PUT",
                        1,
                        &Utc::now().date_naive().to_string(),
                        tenant,
                    );
                }
                Err(err) => {
                    return Err(err.into());
                }
            }
            return Ok(());
        } else {
            let mut data = Vec::new();
            file.read_to_end(&mut data).await?;

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
                let result = async_writer.put_part(part_data.into()).await;
                if result.is_err() {
                    return Err(result.err().unwrap().into());
                }
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
    async fn get_buffered_reader(
        &self,
        path: &RelativePath,
        tenant_id: &Option<String>,
    ) -> Result<BufReader, ObjectStorageError> {
        let path = &to_object_store_path(path);
        let tenant = tenant_id.as_deref().unwrap_or(DEFAULT_TENANT);
        let meta = self.client.head(path).await;
        increment_object_store_calls_by_date("HEAD", &Utc::now().date_naive().to_string(), tenant);
        let meta = match meta {
            Ok(meta) => {
                increment_files_scanned_in_object_store_calls_by_date(
                    "HEAD",
                    1,
                    &Utc::now().date_naive().to_string(),
                    tenant,
                );
                meta
            }
            Err(err) => {
                return Err(err.into());
            }
        };

        let store: Arc<dyn ObjectStore> = self.client.clone();
        let buf = object_store::buffered::BufReader::new(store, &meta);
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

    async fn head(
        &self,
        path: &RelativePath,
        tenant_id: &Option<String>,
    ) -> Result<ObjectMeta, ObjectStorageError> {
        let result = self.client.head(&to_object_store_path(path)).await;
        let tenant = tenant_id.as_deref().unwrap_or(DEFAULT_TENANT);
        increment_object_store_calls_by_date("HEAD", &Utc::now().date_naive().to_string(), tenant);
        if result.is_ok() {
            increment_files_scanned_in_object_store_calls_by_date(
                "HEAD",
                1,
                &Utc::now().date_naive().to_string(),
                tenant,
            );
        }

        Ok(result?)
    }

    async fn get_object(
        &self,
        path: &RelativePath,
        tenant_id: &Option<String>,
    ) -> Result<Bytes, ObjectStorageError> {
        Ok(self._get_object(path, tenant_id).await?)
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
        let tenant = tenant_id.as_deref().unwrap_or(DEFAULT_TENANT);
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
                    tenant_id,
                )
                .await?;
            res.push(byts);
        }

        // Record total files scanned
        increment_files_scanned_in_object_store_calls_by_date(
            "LIST",
            files_scanned as u64,
            &Utc::now().date_naive().to_string(),
            tenant,
        );
        increment_object_store_calls_by_date("LIST", &Utc::now().date_naive().to_string(), tenant);
        Ok(res)
    }

    async fn get_ingestor_meta_file_paths(
        &self,
        tenant_id: &Option<String>,
    ) -> Result<Vec<RelativePathBuf>, ObjectStorageError> {
        let mut path_arr = vec![];
        let mut files_scanned = 0;
        let tenant = tenant_id.as_deref().unwrap_or(DEFAULT_TENANT);
        let mut object_stream = self.client.list(Some(&self.root));
        increment_object_store_calls_by_date("LIST", &Utc::now().date_naive().to_string(), tenant);

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
            tenant,
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

    async fn delete_object(
        &self,
        path: &RelativePath,
        tenant_id: &Option<String>,
    ) -> Result<(), ObjectStorageError> {
        let result = self.client.delete(&to_object_store_path(path)).await;
        let tenant = tenant_id.as_deref().unwrap_or(DEFAULT_TENANT);
        increment_object_store_calls_by_date(
            "DELETE",
            &Utc::now().date_naive().to_string(),
            tenant,
        );
        if result.is_ok() {
            increment_files_scanned_in_object_store_calls_by_date(
                "DELETE",
                1,
                &Utc::now().date_naive().to_string(),
                tenant,
            );
        }

        Ok(result?)
    }

    async fn check(&self, tenant_id: &Option<String>) -> Result<(), ObjectStorageError> {
        let result = self
            .client
            .head(&to_object_store_path(&parseable_json_path()))
            .await;
        let tenant = tenant_id.as_deref().unwrap_or(DEFAULT_TENANT);
        increment_object_store_calls_by_date("HEAD", &Utc::now().date_naive().to_string(), tenant);

        if result.is_ok() {
            increment_files_scanned_in_object_store_calls_by_date(
                "HEAD",
                1,
                &Utc::now().date_naive().to_string(),
                tenant,
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
        let file = RelativePathBuf::from(&node_filename);
        let tenant = tenant_id.as_deref().unwrap_or(DEFAULT_TENANT);
        let result = self.client.delete(&to_object_store_path(&file)).await;
        increment_object_store_calls_by_date(
            "DELETE",
            &Utc::now().date_naive().to_string(),
            tenant,
        );
        match result {
            Ok(_) => {
                increment_files_scanned_in_object_store_calls_by_date(
                    "DELETE",
                    1,
                    &Utc::now().date_naive().to_string(),
                    tenant,
                );
                Ok(())
            }
            Err(err) => Err(err.into()),
        }
    }

    async fn list_streams(&self) -> Result<HashSet<LogStream>, ObjectStorageError> {
        // self._list_streams().await
        Err(ObjectStorageError::Custom(
            "GCS doesn't implement list_streams".into(),
        ))
    }

    async fn list_old_streams(&self) -> Result<HashSet<LogStream>, ObjectStorageError> {
        let resp = self.client.list_with_delimiter(None).await?;
        let common_prefixes = resp.common_prefixes; // get all dirs
        increment_files_scanned_in_object_store_calls_by_date(
            "LIST",
            common_prefixes.len() as u64,
            &Utc::now().date_naive().to_string(),
            DEFAULT_TENANT,
        );
        increment_object_store_calls_by_date(
            "LIST",
            &Utc::now().date_naive().to_string(),
            DEFAULT_TENANT,
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
                    DEFAULT_TENANT,
                );
                result.map(|_| ())
            };
            stream_json_check.push(task);
        }
        increment_files_scanned_in_object_store_calls_by_date(
            "HEAD",
            dirs.len() as u64,
            &Utc::now().date_naive().to_string(),
            DEFAULT_TENANT,
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
        let pre = object_store::path::Path::from(format!("{}/{}/", stream_name, date));
        let tenant = tenant_id.as_deref().unwrap_or(DEFAULT_TENANT);
        let resp = self.client.list_with_delimiter(Some(&pre)).await?;
        increment_files_scanned_in_object_store_calls_by_date(
            "LIST",
            resp.common_prefixes.len() as u64,
            &Utc::now().date_naive().to_string(),
            tenant,
        );
        increment_object_store_calls_by_date("LIST", &Utc::now().date_naive().to_string(), tenant);

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
        tenant_id: &Option<String>,
    ) -> Result<Vec<String>, ObjectStorageError> {
        let pre = object_store::path::Path::from(format!("{}/{}/{}/", stream_name, date, hour));
        let resp = self.client.list_with_delimiter(Some(&pre)).await?;
        let tenant = tenant_id.as_deref().unwrap_or(DEFAULT_TENANT);
        increment_files_scanned_in_object_store_calls_by_date(
            "LIST",
            resp.common_prefixes.len() as u64,
            &Utc::now().date_naive().to_string(),
            tenant,
        );
        increment_object_store_calls_by_date("LIST", &Utc::now().date_naive().to_string(), tenant);
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
                let path = format!("gs://{}/{}", &self.bucket, prefix);
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
        let pre = object_store::path::Path::from("/");

        let resp = self.client.list_with_delimiter(Some(&pre)).await;
        let tenant = tenant_id.as_deref().unwrap_or(DEFAULT_TENANT);
        increment_object_store_calls_by_date("LIST", &Utc::now().date_naive().to_string(), tenant);
        let resp = match resp {
            Ok(resp) => {
                increment_files_scanned_in_object_store_calls_by_date(
                    "LIST",
                    resp.common_prefixes.len() as u64,
                    &Utc::now().date_naive().to_string(),
                    tenant,
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
        let prefix = object_store::path::Path::from(relative_path.as_str());
        let tenant = tenant_id.as_deref().unwrap_or(DEFAULT_TENANT);
        let resp = self.client.list_with_delimiter(Some(&prefix)).await;
        increment_object_store_calls_by_date("LIST", &Utc::now().date_naive().to_string(), tenant);
        let resp = match resp {
            Ok(resp) => {
                increment_files_scanned_in_object_store_calls_by_date(
                    "LIST",
                    resp.common_prefixes.len() as u64,
                    &Utc::now().date_naive().to_string(),
                    tenant,
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
