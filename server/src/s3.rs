use async_trait::async_trait;
use aws_sdk_s3::model::{Delete, ObjectIdentifier};
use aws_sdk_s3::types::ByteStream;
use aws_sdk_s3::Error as AwsSdkError;
use aws_sdk_s3::{Client, Credentials, Endpoint, Region};
use bytes::Bytes;
use crossterm::style::Stylize;
use http::Uri;
use std::collections::HashSet;
use std::fs;
use std::iter::Iterator;
use std::sync::Arc;
use structopt::StructOpt;
use tokio_stream::StreamExt;

use crate::error::Error;
use crate::option::{StorageOpt, CONFIG};
use crate::storage::{LogStream, ObjectStorage, ObjectStorageError};

pub const DEFAULT_S3_URL: &str = "http://127.0.0.1:9000";
pub const S3_URL_ENV_VAR: &str = "P_S3_URL";

lazy_static::lazy_static! {
    #[derive(Debug)]
    pub static ref S3_CONFIG: Arc<S3Config> = Arc::new(S3Config::from_args());
}

#[derive(Debug, Clone, StructOpt)]
#[structopt(name = "S3 config", about = "configuration for AWS S3 SDK")]
pub struct S3Config {
    /// The endpoint to AWS S3 or compatible object storage platform
    #[structopt(long, env = S3_URL_ENV_VAR, default_value = DEFAULT_S3_URL )]
    pub s3_endpoint_url: String,

    /// The access key for AWS S3 or compatible object storage platform
    #[structopt(long, env = "P_S3_ACCESS_KEY", default_value = "minioadmin")]
    pub s3_access_key_id: String,

    /// The secret key for AWS S3 or compatible object storage platform
    #[structopt(long, env = "P_S3_SECRET_KEY", default_value = "minioadmin")]
    pub s3_secret_key: String,

    /// The region for AWS S3 or compatible object storage platform
    #[structopt(long, env = "P_S3_REGION", default_value = "us-east-1")]
    pub s3_default_region: String,

    /// The AWS S3 or compatible object storage bucket to be used for storage
    #[structopt(long, env = "P_S3_BUCKET", default_value = "logstorage")]
    pub s3_bucket_name: String,
}

impl StorageOpt for S3Config {
    fn bucket_name(&self) -> &str {
        &self.s3_bucket_name
    }

    fn endpoint_url(&self) -> &str {
        &self.s3_endpoint_url
    }

    fn is_default_url(&self) -> bool {
        self.s3_endpoint_url == DEFAULT_S3_URL
    }

    fn warning(&self) {
        if self.is_default_url() {
            eprintln!(
                "
        {}
        {}",
                "Parseable server is using default object storage backend with public access."
                    .to_string()
                    .red(),
                format!(
                    "Setup your object storage backend with {} before storing production logs.",
                    S3_URL_ENV_VAR
                )
                .red()
            )
        }
    }
}

impl ObjectStorageError for AwsSdkError {}

impl From<AwsSdkError> for Error {
    fn from(e: AwsSdkError) -> Self {
        Self::Storage(Box::new(e))
    }
}

pub struct S3 {
    client: aws_sdk_s3::Client,
}

impl S3 {
    async fn _put_schema(&self, stream_name: String, body: String) -> Result<(), AwsSdkError> {
        let _resp = self
            .client
            .put_object()
            .bucket(&S3_CONFIG.s3_bucket_name)
            .key(format!("{}/.schema", stream_name))
            .body(body.into_bytes().into())
            .send()
            .await?;

        Ok(())
    }

    async fn _create_stream(&self, stream_name: &str) -> Result<(), AwsSdkError> {
        let _resp = self
            .client
            .put_object()
            .bucket(&S3_CONFIG.s3_bucket_name)
            .key(format!("{}/.schema", stream_name))
            .send()
            .await?;
        // Prefix created on S3, now create the directory in
        // the local storage as well
        let _res = fs::create_dir_all(CONFIG.parseable.local_stream_data_path(stream_name));
        Ok(())
    }

    async fn _delete_stream(&self, stream_name: &str) -> Result<(), AwsSdkError> {
        let mut pages = self
            .client
            .list_objects_v2()
            .bucket(&S3_CONFIG.s3_bucket_name)
            .prefix(stream_name)
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
            .bucket(&S3_CONFIG.s3_bucket_name)
            .delete(delete)
            .send()
            .await?;

        Ok(())
    }

    async fn _create_alert(&self, stream_name: &str, body: String) -> Result<(), AwsSdkError> {
        let _resp = self
            .client
            .put_object()
            .bucket(&S3_CONFIG.s3_bucket_name)
            .key(format!("{}/.alert.json", stream_name))
            .body(body.into_bytes().into())
            .send()
            .await?;

        Ok(())
    }

    async fn _stream_exists(&self, stream_name: &str) -> Result<Bytes, AwsSdkError> {
        let resp = self
            .client
            .get_object()
            .bucket(&S3_CONFIG.s3_bucket_name)
            .key(format!("{}/.schema", stream_name))
            .send()
            .await?;
        let body = resp.body.collect().await;
        let body_bytes = body.unwrap().into_bytes();

        Ok(body_bytes)
    }

    async fn _alert_exists(&self, stream_name: &str) -> Result<Bytes, AwsSdkError> {
        let resp = self
            .client
            .get_object()
            .bucket(&S3_CONFIG.s3_bucket_name)
            .key(format!("{}/.alert.json", stream_name))
            .send()
            .await?;
        let body = resp.body.collect().await;
        let body_bytes = body.unwrap().into_bytes();
        Ok(body_bytes)
    }

    async fn _list_streams(&self) -> Result<Vec<LogStream>, AwsSdkError> {
        let resp = self
            .client
            .list_objects_v2()
            .bucket(&S3_CONFIG.s3_bucket_name)
            .send()
            .await?;
        let body = resp.contents().unwrap_or_default();
        // make a set of unique prefixes at the root level
        let mut hs = HashSet::<String>::new();
        for logstream in body {
            let name = logstream.key().unwrap_or_default().to_string();
            let tokens = name.split('/').collect::<Vec<&str>>();
            hs.insert(tokens[0].to_string());
        }
        // transform that hashset to a vector before returning
        let mut streams = Vec::new();
        for v in hs {
            streams.push(LogStream { name: v });
        }

        Ok(streams)
    }

    async fn _put_parquet(&self, key: &str, path: &str) -> Result<(), AwsSdkError> {
        let body = ByteStream::from_path(path).await.unwrap();
        let resp = self
            .client
            .put_object()
            .bucket(&S3_CONFIG.s3_bucket_name)
            .key(key)
            .body(body)
            .send()
            .await?;
        println!("{:?}", resp);

        Ok(())
    }
}

#[async_trait]
impl ObjectStorage for S3 {
    fn new() -> Self {
        let uri = S3_CONFIG.s3_endpoint_url.parse::<Uri>().unwrap();
        let endpoint = Endpoint::immutable(uri);
        let region = Region::new(&S3_CONFIG.s3_default_region);
        let creds = Credentials::new(
            &S3_CONFIG.s3_access_key_id,
            &S3_CONFIG.s3_secret_key,
            None,
            None,
            "",
        );
        let config = aws_sdk_s3::Config::builder()
            .region(region)
            .endpoint_resolver(endpoint)
            .credentials_provider(creds)
            .build();

        let client = Client::from_conf(config);

        Self { client }
    }

    async fn is_available(&self) -> bool {
        self.client
            .head_bucket()
            .bucket(&S3_CONFIG.s3_bucket_name)
            .send()
            .await
            .is_ok()
    }

    async fn put_schema(&self, stream_name: String, body: String) -> Result<(), Error> {
        self._put_schema(stream_name, body).await?;

        Ok(())
    }

    async fn create_stream(&self, stream_name: &str) -> Result<(), Error> {
        self._create_stream(stream_name).await?;

        Ok(())
    }

    async fn delete_stream(&self, stream_name: &str) -> Result<(), Error> {
        self._delete_stream(stream_name).await?;

        Ok(())
    }

    async fn create_alert(&self, stream_name: &str, body: String) -> Result<(), Error> {
        self._create_alert(stream_name, body).await?;

        Ok(())
    }

    async fn stream_exists(&self, stream_name: &str) -> Result<Bytes, Error> {
        let body_bytes = self._stream_exists(stream_name).await?;

        Ok(body_bytes)
    }

    async fn alert_exists(&self, stream_name: &str) -> Result<Bytes, Error> {
        let body_bytes = self._alert_exists(stream_name).await?;

        Ok(body_bytes)
    }

    async fn list_streams(&self) -> Result<Vec<LogStream>, Error> {
        let streams = self._list_streams().await?;

        Ok(streams)
    }

    async fn put_parquet(&self, key: &str, path: &str) -> Result<(), Error> {
        self._put_parquet(key, path).await?;

        Ok(())
    }
}

// fn s3_env_variables() {
//     let (secret_key, access_key, region, endpoint_url, bucket_name) = (
//         "AWS_SECRET_ACCESS_KEY",
//         "AWS_ACCESS_KEY_ID",
//         "AWS_DEFAULT_REGION",
//         "AWS_ENDPOINT_URL",
//         "AWS_BUCKET_NAME",
//     );
//     let data = vec![secret_key, access_key, region, endpoint_url, bucket_name];

//     for data in data.iter() {
//         match *data {
//             "AWS_SECRET_ACCESS_KEY" => env::set_var(secret_key, &opt.s3_secret_key),
//             "AWS_ACCESS_KEY_ID" => env::set_var(access_key, &opt.s3_access_key_id),
//             "AWS_DEFAULT_REGION" => env::set_var(region, &opt.s3_default_region),
//             "AWS_ENDPOINT_URL" => env::set_var(endpoint_url, &opt.s3_endpoint_url),
//             "AWS_BUCKET_NAME" => env::set_var(bucket_name, &opt.s3_bucket_name),
//             _ => println!(),
//         }
//     }
// }
