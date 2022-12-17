/*
 * Parseable Server (C) 2022 Parseable, Inc.
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
use aws_sdk_s3::error::{HeadBucketError, HeadBucketErrorKind};
use aws_sdk_s3::model::{CommonPrefix, Delete, ObjectIdentifier};
use aws_sdk_s3::types::{ByteStream, SdkError};
use aws_sdk_s3::Error as AwsSdkError;
use aws_sdk_s3::RetryConfig;
use aws_sdk_s3::{Client, Credentials, Endpoint, Region};
use aws_smithy_async::rt::sleep::default_async_sleep;
use base64::encode;
use bytes::Bytes;
use clap::builder::ArgPredicate;

use datafusion::datasource::file_format::parquet::ParquetFormat;
use datafusion::datasource::listing::{
    ListingOptions, ListingTable, ListingTableConfig, ListingTableUrl,
};
use datafusion::datasource::object_store::ObjectStoreRegistry;
use datafusion::execution::runtime_env::{RuntimeConfig, RuntimeEnv};
use futures::StreamExt;
use http::Uri;
use md5::{Digest, Md5};
use object_store::aws::AmazonS3Builder;
use object_store::limit::LimitStore;
use relative_path::RelativePath;

use std::iter::Iterator;
use std::path::Path;
use std::sync::Arc;

use crate::query::Query;

use crate::storage::{LogStream, ObjectStorage, ObjectStorageError};

use super::ObjectStorageProvider;

// Default object storage currently is DO Spaces bucket
// Any user who starts the Parseable server with default configuration
// will point to this bucket and will see any data present on this bucket
const DEFAULT_S3_URL: &str = "https://minio.parseable.io:9000";
const DEFAULT_S3_REGION: &str = "us-east-1";
const DEFAULT_S3_BUCKET: &str = "parseable";
const DEFAULT_S3_ACCESS_KEY: &str = "minioadmin";
const DEFAULT_S3_SECRET_KEY: &str = "minioadmin";

#[derive(Debug, Clone, clap::Args)]
#[command(name = "S3 config", about = "configuration for AWS S3 SDK")]
pub struct S3Config {
    /// The endpoint to AWS S3 or compatible object storage platform
    #[arg(
        long,
        env = "P_S3_URL",
        value_name = "url",
        default_value_if("demo", ArgPredicate::IsPresent, DEFAULT_S3_URL)
    )]
    pub s3_endpoint_url: String,

    /// The access key for AWS S3 or compatible object storage platform
    #[arg(
        long,
        env = "P_S3_ACCESS_KEY",
        value_name = "access-key",
        default_value_if("demo", ArgPredicate::IsPresent, DEFAULT_S3_ACCESS_KEY)
    )]
    pub s3_access_key_id: String,

    /// The secret key for AWS S3 or compatible object storage platform
    #[arg(
        long,
        env = "P_S3_SECRET_KEY",
        value_name = "secret-key",
        default_value_if("demo", ArgPredicate::IsPresent, DEFAULT_S3_SECRET_KEY)
    )]
    pub s3_secret_key: String,

    /// The region for AWS S3 or compatible object storage platform
    #[arg(
        long,
        env = "P_S3_REGION",
        value_name = "region",
        default_value_if("demo", ArgPredicate::IsPresent, DEFAULT_S3_REGION)
    )]
    pub s3_region: String,

    /// The AWS S3 or compatible object storage bucket to be used for storage
    #[arg(
        long,
        env = "P_S3_BUCKET",
        value_name = "bucket-name",
        default_value_if("demo", ArgPredicate::IsPresent, DEFAULT_S3_BUCKET)
    )]
    pub s3_bucket_name: String,

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
            .with_region(&self.s3_region)
            .with_endpoint(&self.s3_endpoint_url)
            .with_bucket_name(&self.s3_bucket_name)
            .with_access_key_id(&self.s3_access_key_id)
            .with_secret_access_key(&self.s3_secret_key)
            // allow http for local instances
            .with_allow_http(true)
            .build()
            .unwrap();

        // limit objectstore to a concurrent request limit
        let s3 = LimitStore::new(s3, super::MAX_OBJECT_STORE_REQUESTS);

        let object_store_registry = ObjectStoreRegistry::new();
        object_store_registry.register_store("s3", &self.s3_bucket_name, Arc::new(s3));

        let config =
            RuntimeConfig::new().with_object_store_registry(Arc::new(object_store_registry));

        let runtime = RuntimeEnv::new(config).unwrap();

        Arc::new(runtime)
    }

    fn get_object_store(&self) -> Arc<dyn ObjectStorage + Send> {
        let uri = self.s3_endpoint_url.parse::<Uri>().unwrap();
        let endpoint = Endpoint::immutable(uri);
        let region = Region::new(self.s3_region.clone());
        let creds = Credentials::new(&self.s3_access_key_id, &self.s3_secret_key, None, None, "");

        let config = aws_sdk_s3::Config::builder()
            .region(region)
            .endpoint_resolver(endpoint)
            .credentials_provider(creds)
            .retry_config(RetryConfig::standard().with_max_attempts(5))
            .sleep_impl(default_async_sleep().expect("sleep impl is provided for tokio rt"))
            .build();

        let client = Client::from_conf(config);

        Arc::new(S3 {
            client,
            bucket: self.s3_bucket_name.clone(),
            set_content_md5: self.content_md5,
        })
    }

    fn get_endpoint(&self) -> String {
        format!("{}/{}", self.s3_endpoint_url, self.s3_bucket_name)
    }
}

pub struct S3 {
    client: aws_sdk_s3::Client,
    bucket: String,
    set_content_md5: bool,
}

impl S3 {
    async fn _get_object(&self, path: &RelativePath) -> Result<Bytes, AwsSdkError> {
        let resp = self
            .client
            .get_object()
            .bucket(&self.bucket)
            .key(path.as_str())
            .send()
            .await?;
        let body = resp.body.collect().await;
        let body_bytes = body.unwrap().into_bytes();
        Ok(body_bytes)
    }

    async fn _delete_stream(&self, stream_name: &str) -> Result<(), AwsSdkError> {
        let mut pages = self
            .client
            .list_objects_v2()
            .bucket(&self.bucket)
            .prefix(format!("{}/", stream_name))
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

        let resp = self
            .client
            .put_object()
            .bucket(&self.bucket)
            .key(key)
            .body(body)
            .set_content_md5(md5)
            .send()
            .await?;

        log::trace!("{:?}", resp);

        Ok(())
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
            encode(hash.finalize())
        });

        self.client
            .put_object()
            .bucket(&self.bucket)
            .key(path.as_str())
            .body(resource.into())
            .set_content_md5(hash)
            .send()
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
            Some(encode(digest.finalize()))
        } else {
            None
        };

        self._upload_file(key, path, hash).await?;

        Ok(())
    }

    fn query_table(&self, query: &Query) -> Result<Option<ListingTable>, ObjectStorageError> {
        // Get all prefix paths and convert them into futures which yeilds ListingTableUrl
        let prefixes: Vec<ListingTableUrl> = query
            .get_prefixes()
            .into_iter()
            .map(|prefix| {
                let path = format!("s3://{}/{}", &self.bucket, prefix);
                ListingTableUrl::parse(path).unwrap()
            })
            .collect();

        if prefixes.is_empty() {
            return Ok(None);
        }

        let file_format = ParquetFormat::default().with_enable_pruning(true);
        let listing_options = ListingOptions {
            file_extension: ".parquet".to_string(),
            format: Arc::new(file_format),
            table_partition_cols: vec![],
            collect_stat: true,
            target_partitions: 1,
        };

        let config = ListingTableConfig::new_with_multi_paths(prefixes)
            .with_listing_options(listing_options)
            .with_schema(Arc::clone(&query.schema));

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

impl From<SdkError<HeadBucketError>> for ObjectStorageError {
    fn from(error: SdkError<HeadBucketError>) -> Self {
        match error {
            SdkError::ServiceError {
                err:
                    HeadBucketError {
                        kind: HeadBucketErrorKind::Unhandled(err),
                        ..
                    },
                ..
            } => ObjectStorageError::AuthenticationError(err),
            SdkError::DispatchFailure(err) => ObjectStorageError::ConnectionError(Box::new(err)),
            SdkError::TimeoutError(err) => ObjectStorageError::ConnectionError(err),
            err => ObjectStorageError::UnhandledError(Box::new(err)),
        }
    }
}

impl From<serde_json::Error> for ObjectStorageError {
    fn from(error: serde_json::Error) -> Self {
        ObjectStorageError::UnhandledError(Box::new(error))
    }
}
