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
    collections::{BTreeMap, HashSet},
    path::Path,
    sync::Arc,
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
    BackoffConfig, ClientOptions, ObjectMeta, ObjectStore, PutPayload, RetryConfig,
    azure::{MicrosoftAzure, MicrosoftAzureBuilder},
    buffered::BufReader,
    limit::LimitStore,
    path::Path as StorePath,
};
use relative_path::{RelativePath, RelativePathBuf};
use tokio::{fs::OpenOptions, io::AsyncReadExt};
use tracing::{error, info};
use url::Url;

use crate::{
    handlers::http::users::USERS_ROOT_DIR,
    metrics::storage::{StorageMetrics, azureblob::REQUEST_RESPONSE_TIME},
    parseable::LogStream,
};

use super::{
    CONNECT_TIMEOUT_SECS, MIN_MULTIPART_UPLOAD_SIZE, ObjectStorage, ObjectStorageError,
    ObjectStorageProvider, PARSEABLE_ROOT_DIRECTORY, REQUEST_TIMEOUT_SECS, SCHEMA_FILE_NAME,
    STREAM_METADATA_FILE_NAME, STREAM_ROOT_DIRECTORY, metrics_layer::MetricLayer,
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
        let azure = MetricLayer::new(azure);

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

    fn register_store_metrics(&self, handler: &actix_web_prometheus::PrometheusMetrics) {
        self.register_metrics(handler)
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
            return Err(ObjectStorageError::Custom(
                format!("Failed to upload, error: {source:?}").to_string(),
            ));
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

    async fn _list_streams(&self) -> Result<HashSet<LogStream>, ObjectStorageError> {
        let mut result_file_list = HashSet::new();
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
                result_file_list.insert(stream);
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
    async fn _upload_file(&self, key: &str, path: &Path) -> Result<(), ObjectStorageError> {
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
            info!("Uploaded file to Azure Blob Storage: {:?}", result);
            Ok(())
        };

        let status = if res.is_ok() { "200" } else { "400" };
        let time = instant.elapsed().as_secs_f64();
        REQUEST_RESPONSE_TIME
            .with_label_values(&["UPLOAD_PARQUET", status])
            .observe(time);

        res
    }

    async fn _upload_multipart(
        &self,
        key: &RelativePath,
        path: &Path,
    ) -> Result<(), ObjectStorageError> {
        let mut file = OpenOptions::new().read(true).open(path).await?;
        let location = &to_object_store_path(key);

        let mut async_writer = self.client.put_multipart(location).await?;

        let meta = file.metadata().await?;
        let total_size = meta.len() as usize;
        if total_size < MIN_MULTIPART_UPLOAD_SIZE {
            let mut data = Vec::new();
            file.read_to_end(&mut data).await?;
            self.client.put(location, data.into()).await?;
            // async_writer.put_part(data.into()).await?;
            // async_writer.complete().await?;
            return Ok(());
        } else {
            let mut data = Vec::new();
            file.read_to_end(&mut data).await?;

            // let mut upload_parts = Vec::new();

            let has_final_partial_part = total_size % MIN_MULTIPART_UPLOAD_SIZE > 0;
            let num_full_parts = total_size / MIN_MULTIPART_UPLOAD_SIZE;
            let total_parts = num_full_parts + if has_final_partial_part { 1 } else { 0 };

            // Upload each part
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

                // Upload the part
                async_writer.put_part(part_data.into()).await?;

                // upload_parts.push(part_number as u64 + 1);
            }
            if let Err(err) = async_writer.complete().await {
                error!("Failed to complete multipart upload. {:?}", err);
                async_writer.abort().await?;
            };
        }
        Ok(())
    }

    // TODO: introduce parallel, multipart-uploads if required
    // async fn _upload_multipart(&self, key: &str, path: &Path) -> Result<(), ObjectStorageError> {
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
impl ObjectStorage for BlobStore {
    async fn upload_multipart(
        &self,
        key: &RelativePath,
        path: &Path,
    ) -> Result<(), ObjectStorageError> {
        self._upload_multipart(key, path).await
    }
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
    async fn head(&self, _path: &RelativePath) -> Result<ObjectMeta, ObjectStorageError> {
        Err(ObjectStorageError::UnhandledError(Box::new(
            std::io::Error::new(
                std::io::ErrorKind::Unsupported,
                "Head operation not implemented for Blob Storage yet",
            ),
        )))
    }

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

    async fn try_delete_node_meta(&self, node_filename: String) -> Result<(), ObjectStorageError> {
        let file = RelativePathBuf::from(&node_filename);
        match self.client.delete(&to_object_store_path(&file)).await {
            Ok(_) => Ok(()),
            Err(err) => {
                // if the object is not found, it is not an error
                // the given url path was incorrect
                if matches!(err, object_store::Error::NotFound { .. }) {
                    error!("Node does not exist");
                    Err(err.into())
                } else {
                    error!("Error deleting node meta file: {:?}", err);
                    Err(err.into())
                }
            }
        }
    }

    async fn list_streams(&self) -> Result<HashSet<LogStream>, ObjectStorageError> {
        self._list_streams().await
    }

    async fn list_old_streams(&self) -> Result<HashSet<LogStream>, ObjectStorageError> {
        let resp = self.client.list_with_delimiter(None).await?;

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
            let task = async move { self.client.head(&StorePath::from(key)).await.map(|_| ()) };
            stream_json_check.push(task);
        }

        stream_json_check.try_collect::<()>().await?;

        Ok(dirs.into_iter().collect())
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

    async fn upload_file(&self, key: &str, path: &Path) -> Result<(), ObjectStorageError> {
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
        let resp = self.client.list_with_delimiter(Some(&pre)).await?;

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
        let resp = self.client.list_with_delimiter(Some(&prefix)).await?;

        Ok(resp
            .common_prefixes
            .iter()
            .flat_map(|path| path.parts())
            .map(|name| name.as_ref().to_string())
            .collect::<Vec<_>>())
    }

    fn get_bucket_name(&self) -> String {
        self.container.clone()
    }
}
