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
use aws_sdk_s3::config::retry::RetryConfig;
use aws_sdk_s3::error::{HeadBucketError, HeadBucketErrorKind};
use aws_sdk_s3::model::{CommonPrefix, Delete, ObjectIdentifier};
use aws_sdk_s3::types::{ByteStream, SdkError};
use aws_sdk_s3::Error as AwsSdkError;
use aws_sdk_s3::{Client, Credentials, Region};
use aws_smithy_async::rt::sleep::default_async_sleep;
use base64::engine::general_purpose::STANDARD as BASE64;
use base64::engine::Engine as _;
use bytes::Bytes;
use datafusion::arrow::datatypes::Schema;

use datafusion::datasource::file_format::parquet::ParquetFormat;
use datafusion::datasource::listing::{
    ListingOptions, ListingTable, ListingTableConfig, ListingTableUrl,
};
use datafusion::datasource::object_store::ObjectStoreRegistry;
use datafusion::error::DataFusionError;
use datafusion::execution::runtime_env::{RuntimeConfig, RuntimeEnv};
use futures::StreamExt;
use lazy_static::lazy_static;
use md5::{Digest, Md5};
use object_store::aws::AmazonS3Builder;
use object_store::limit::LimitStore;
use prometheus::{HistogramOpts, HistogramVec};
use relative_path::RelativePath;

use std::iter::Iterator;
use std::path::Path;
use std::sync::Arc;
use std::time::Instant;

use crate::metrics::METRICS_NAMESPACE;
use crate::storage::{LogStream, ObjectStorage, ObjectStorageError};

use super::ObjectStorageProvider;

lazy_static! {
    pub static ref REQUEST_RESPONSE_TIME: HistogramVec = HistogramVec::new(
        HistogramOpts::new("s3_response_time", "S3 Request Latency").namespace(METRICS_NAMESPACE),
        &["method", "status"]
    )
    .expect("metric can be created");
}

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
    #[arg(
        long,
        env = "P_S3_ACCESS_KEY",
        value_name = "access-key",
        required = true
    )]
    pub access_key_id: String,

    /// The secret key for AWS S3 or compatible object storage platform
    #[arg(
        long,
        env = "P_S3_SECRET_KEY",
        value_name = "secret-key",
        required = true
    )]
    pub secret_key: String,

    /// The region for AWS S3 or compatible object storage platform
    #[arg(long, env = "P_S3_REGION", value_name = "region", required = true)]
    pub region: String,

    /// The AWS S3 or compatible object storage bucket to be used for storage
    #[arg(long, env = "P_S3_BUCKET", value_name = "bucket-name", required = true)]
    pub bucket_name: String,

    /// Set client to send content_md5 header on every put request
    #[arg(
        long,
        env = "P_S3_SET_CONTENT_MD5",
        value_name = "bool",
        default_value = "false"
    )]
    pub content_md5: bool,
}

impl ObjectStorageProvider for S3Config {
    fn get_datafusion_runtime(&self) -> Arc<RuntimeEnv> {
        let s3 = AmazonS3Builder::new()
            .with_region(&self.region)
            .with_endpoint(&self.endpoint_url)
            .with_bucket_name(&self.bucket_name)
            .with_access_key_id(&self.access_key_id)
            .with_secret_access_key(&self.secret_key)
            // allow http for local instances
            .with_allow_http(true)
            .build()
            .unwrap();

        // limit objectstore to a concurrent request limit
        let s3 = LimitStore::new(s3, super::MAX_OBJECT_STORE_REQUESTS);

        let object_store_registry = ObjectStoreRegistry::new();
        object_store_registry.register_store("s3", &self.bucket_name, Arc::new(s3));

        let config =
            RuntimeConfig::new().with_object_store_registry(Arc::new(object_store_registry));

        let runtime = RuntimeEnv::new(config).unwrap();

        Arc::new(runtime)
    }

    fn get_object_store(&self) -> Arc<dyn ObjectStorage + Send> {
        let uri: String = self.endpoint_url.parse().unwrap();
        let region = Region::new(self.region.clone());
        let creds = Credentials::new(&self.access_key_id, &self.secret_key, None, None, "");

        let config = aws_sdk_s3::Config::builder()
            .region(region)
            .endpoint_url(uri)
            .credentials_provider(creds)
            .retry_config(RetryConfig::standard().with_max_attempts(5))
            .sleep_impl(default_async_sleep().expect("sleep impl is provided for tokio rt"))
            .build();

        let client = Client::from_conf(config);

        Arc::new(S3 {
            client,
            bucket: self.bucket_name.clone(),
            set_content_md5: self.content_md5,
        })
    }

    fn get_endpoint(&self) -> String {
        format!("{}/{}", self.endpoint_url, self.bucket_name)
    }

    fn register_store_metrics(&self, handler: &actix_web_prometheus::PrometheusMetrics) {
        handler
            .registry
            .register(Box::new(REQUEST_RESPONSE_TIME.clone()))
            .expect("metric can be registered");
    }
}

pub struct S3 {
    client: aws_sdk_s3::Client,
    bucket: String,
    set_content_md5: bool,
}

impl S3 {
    async fn _get_object(&self, path: &RelativePath) -> Result<Bytes, AwsSdkError> {
        let instant = Instant::now();

        let resp = self
            .client
            .get_object()
            .bucket(&self.bucket)
            .key(path.as_str())
            .send()
            .await;

        match resp {
            Ok(resp) => {
                let time = instant.elapsed().as_secs_f64();
                REQUEST_RESPONSE_TIME
                    .with_label_values(&["GET", "200"])
                    .observe(time);
                let body = resp.body.collect().await.unwrap().into_bytes();
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
        md5: Option<String>,
    ) -> Result<(), AwsSdkError> {
        let time = Instant::now();
        let resp = self
            .client
            .put_object()
            .bucket(&self.bucket)
            .key(path.as_str())
            .body(resource.into())
            .set_content_md5(md5)
            .send()
            .await;
        let status = if resp.is_ok() { "200" } else { "400" };
        let time = time.elapsed().as_secs_f64();
        REQUEST_RESPONSE_TIME
            .with_label_values(&["PUT", status])
            .observe(time);

        resp.map(|_| ()).map_err(|err| err.into())
    }

    async fn _delete_stream(&self, stream_name: &str) -> Result<(), AwsSdkError> {
        let mut pages = self
            .client
            .list_objects_v2()
            .bucket(&self.bucket)
            .prefix(format!("{stream_name}/"))
            .into_paginator()
            .send();

        let mut delete_objects: Vec<ObjectIdentifier> = vec![];
        while let Some(page) = pages.next().await {
            let page = page?;
            for obj in page.contents.unwrap() {
                let obj_id = ObjectIdentifier::builder().set_key(obj.key).build();
                delete_objects.push(obj_id);
            }
        }

        let delete = Delete::builder().set_objects(Some(delete_objects)).build();

        self.client
            .delete_objects()
            .bucket(&self.bucket)
            .delete(delete)
            .send()
            .await?;

        Ok(())
    }

    async fn _list_streams(&self) -> Result<Vec<LogStream>, AwsSdkError> {
        let resp = self
            .client
            .list_objects_v2()
            .bucket(&self.bucket)
            .delimiter('/')
            .send()
            .await?;

        let common_prefixes = resp.common_prefixes().unwrap_or_default();

        // return prefixes at the root level
        let logstreams: Vec<_> = common_prefixes
            .iter()
            .filter_map(CommonPrefix::prefix)
            .filter_map(|name| name.strip_suffix('/'))
            .map(String::from)
            .map(|name| LogStream { name })
            .collect();

        Ok(logstreams)
    }

    async fn _upload_file(
        &self,
        key: &str,
        path: &Path,
        md5: Option<String>,
    ) -> Result<(), AwsSdkError> {
        let body = ByteStream::from_path(&path).await.unwrap();

        let instant = Instant::now();
        let resp = self
            .client
            .put_object()
            .bucket(&self.bucket)
            .key(key)
            .body(body)
            .set_content_md5(md5)
            .send()
            .await;

        let status = if resp.is_ok() { "200" } else { "400" };
        let time = instant.elapsed().as_secs_f64();
        REQUEST_RESPONSE_TIME
            .with_label_values(&["UPLOAD_PARQUET", status])
            .observe(time);

        log::trace!("{:?}", resp);
        resp.map(|_| ()).map_err(|err| err.into())
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
        let hash = self.set_content_md5.then(|| {
            let mut hash = Md5::new();
            hash.update(&resource);
            BASE64.encode(hash.finalize())
        });

        self._put_object(path, resource, hash)
            .await
            .map_err(|err| ObjectStorageError::ConnectionError(Box::new(err)))?;

        Ok(())
    }

    async fn check(&self) -> Result<(), ObjectStorageError> {
        self.client
            .head_bucket()
            .bucket(&self.bucket)
            .send()
            .await
            .map(|_| ())
            .map_err(|err| err.into())
    }

    async fn delete_stream(&self, stream_name: &str) -> Result<(), ObjectStorageError> {
        self._delete_stream(stream_name).await?;

        Ok(())
    }

    async fn list_streams(&self) -> Result<Vec<LogStream>, ObjectStorageError> {
        let streams = self._list_streams().await?;

        Ok(streams)
    }

    async fn upload_file(&self, key: &str, path: &Path) -> Result<(), ObjectStorageError> {
        let hash = if self.set_content_md5 {
            let mut file = std::fs::File::open(path)?;
            let mut digest = Md5::new();
            std::io::copy(&mut file, &mut digest)?;
            Some(BASE64.encode(digest.finalize()))
        } else {
            None
        };

        self._upload_file(key, path, hash).await?;

        Ok(())
    }

    fn query_table(
        &self,
        prefixes: Vec<String>,
        schema: Arc<Schema>,
    ) -> Result<Option<ListingTable>, DataFusionError> {
        // Get all prefix paths and convert them into futures which yeilds ListingTableUrl
        let prefixes: Vec<ListingTableUrl> = prefixes
            .into_iter()
            .map(|prefix| {
                let path = format!("s3://{}/{}", &self.bucket, prefix);
                ListingTableUrl::parse(path).unwrap()
            })
            .collect();

        if prefixes.is_empty() {
            return Ok(None);
        }

        let file_format = ParquetFormat::default().with_enable_pruning(Some(true));
        let listing_options = ListingOptions {
            file_extension: ".parquet".to_string(),
            file_sort_order: None,
            infinite_source: false,
            format: Arc::new(file_format),
            table_partition_cols: vec![],
            collect_stat: true,
            target_partitions: 1,
        };

        let config = ListingTableConfig::new_with_multi_paths(prefixes)
            .with_listing_options(listing_options)
            .with_schema(schema);

        Ok(Some(ListingTable::try_new(config)?))
    }
}

impl From<AwsSdkError> for ObjectStorageError {
    fn from(error: AwsSdkError) -> Self {
        match error {
            AwsSdkError::NotFound(_) | AwsSdkError::NoSuchKey(_) => {
                ObjectStorageError::NoSuchKey("".to_string())
            }
            e => ObjectStorageError::UnhandledError(Box::new(e)),
        }
    }
}

// TODO: Needs to be adjusted https://github.com/awslabs/aws-sdk-rust/issues/657#issue-1436568853
impl From<SdkError<HeadBucketError>> for ObjectStorageError {
    fn from(error: SdkError<HeadBucketError>) -> Self {
        match error {
            SdkError::ServiceError(err) => {
                let err = err.into_err();
                let err_kind = &err.kind;
                match err_kind {
                    HeadBucketErrorKind::Unhandled(_) => {
                        ObjectStorageError::AuthenticationError(err.into())
                    }
                    _ => ObjectStorageError::UnhandledError(err.into()),
                }
            }
            err @ SdkError::DispatchFailure(_) | err @ SdkError::TimeoutError(_) => {
                ObjectStorageError::ConnectionError(err.into())
            }
            err => ObjectStorageError::UnhandledError(Box::new(err)),
        }
    }
}

impl From<serde_json::Error> for ObjectStorageError {
    fn from(error: serde_json::Error) -> Self {
        ObjectStorageError::UnhandledError(Box::new(error))
    }
}
