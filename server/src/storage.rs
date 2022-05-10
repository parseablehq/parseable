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

use aws_sdk_s3::{Client, Credentials, Endpoint, Error, Region};
use bytes::Bytes;
use http::Uri;
use serde::Serialize;
use std::collections::HashSet;
use std::env;
use std::fs;
use std::io::prelude::*;

use crate::option;

pub struct S3 {
    pub client: aws_sdk_s3::Client,
}

pub trait ObjectStorage {
    fn new(opt: &option::Opt) -> Self;
}

impl ObjectStorage for S3 {
    fn new(opt: &option::Opt) -> S3 {
        S3 {
            client: s3_client(opt),
        }
    }
}

fn local_path_for_stream(opt: &option::Opt, stream_name: &str) -> String {
    format!("{}{}{}", opt.local_disk_path, "/", stream_name)
}

fn s3_client(opt: &option::Opt) -> aws_sdk_s3::Client {
    let (secret_key, access_key, region, endpoint_url, bucket_name) = (
        "AWS_SECRET_ACCESS_KEY",
        "AWS_ACCESS_KEY_ID",
        "AWS_DEFAULT_REGION",
        "AWS_ENDPOINT_URL",
        "AWS_BUCKET_NAME",
    );
    let data = vec![secret_key, access_key, region, endpoint_url, bucket_name];

    for data in data.iter() {
        match *data {
            "AWS_SECRET_ACCESS_KEY" => env::set_var(secret_key, &opt.s3_secret_key),
            "AWS_ACCESS_KEY_ID" => env::set_var(access_key, &opt.s3_access_key_id),
            "AWS_DEFAULT_REGION" => env::set_var(region, &opt.s3_default_region),
            "AWS_ENDPOINT_URL" => env::set_var(endpoint_url, &opt.s3_endpoint_url),
            "AWS_BUCKET_NAME" => env::set_var(bucket_name, &opt.s3_bucket_name),
            _ => println!(),
        }
    }
    let ep = env::var("AWS_ENDPOINT_URL").unwrap_or_else(|_| "none".to_string());
    let uri = ep.parse::<Uri>().unwrap();
    let endpoint = Endpoint::immutable(uri);
    let region =
        Region::new(env::var("AWS_DEFAULT_REGION").unwrap_or_else(|_| "us-east-1".to_string()));
    let creds = Credentials::new(
        env::var("AWS_ACCESS_KEY_ID").unwrap(),
        env::var("AWS_SECRET_ACCESS_KEY").unwrap(),
        None,
        None,
        "",
    );
    let config = aws_sdk_s3::Config::builder()
        .region(region)
        .endpoint_resolver(endpoint)
        .credentials_provider(creds)
        .build();
    Client::from_conf(config)
}

pub fn setup_storage(opt: &option::Opt) -> S3 {
    S3::new(opt)
}

#[tokio::main]
pub async fn put_schema(stream_name: &str, schema: String) -> Result<(), Error> {
    let opt = option::get_opts();
    let client = setup_storage(&opt).client;
    let s = schema.clone();
    let _resp = client
        .put_object()
        .bucket(env::var("AWS_BUCKET_NAME").unwrap().to_string())
        .key(format!("{}{}", stream_name, "/.schema"))
        .body(schema.into_bytes().into())
        .send()
        .await?;
    let mut schema_file = fs::File::create(format!(
        "{}{}{}",
        local_path_for_stream(&opt, stream_name),
        "/",
        ".schema"
    ))
    .unwrap();
    schema_file
        .write_all(s.as_bytes())
        .expect("Unable to write data");

    Ok(())
}

#[tokio::main]
pub async fn create_stream(stream_name: &str) -> Result<(), Error> {
    let opt = option::get_opts();
    let client = setup_storage(&opt).client;
    let _resp = client
        .put_object()
        .bucket(env::var("AWS_BUCKET_NAME").unwrap().to_string())
        .key(format!("{}{}", stream_name, "/.schema"))
        .send()
        .await?;
    // Prefix created on S3, now create the directory in
    // the local storage as well
    let dir_name = local_path_for_stream(&opt, stream_name);
    let _res = fs::create_dir_all(dir_name.clone());

    let file_name = format!("{}{}{}", dir_name, "/", ".schema");
    fs::File::create(file_name).unwrap();
    Ok(())
}

#[tokio::main]
pub async fn stream_exists(stream_name: &str) -> Result<Bytes, Error> {
    let opt = option::get_opts();
    let client = setup_storage(&opt).client;
    let resp = client
        .get_object()
        .bucket(env::var("AWS_BUCKET_NAME").unwrap().to_string())
        .key(format!("{}{}", stream_name, "/.schema"))
        .send()
        .await?;
    let body = resp.body.collect().await;
    let body_bytes = body.unwrap().into_bytes();
    Ok(body_bytes)
}

#[tokio::main]
pub async fn list_streams() -> Result<Vec<Stream>, Error> {
    let opt = option::get_opts();
    let client = setup_storage(&opt).client;
    let resp = client
        .list_objects_v2()
        .bucket(env::var("AWS_BUCKET_NAME").unwrap().to_string())
        .send()
        .await?;
    let body = resp.contents().unwrap_or_default();
    // make a set of unique prefixes at the root level
    let mut hs = HashSet::<String>::new();
    for stream in body {
        let name = stream.key().unwrap_or_default().to_string();
        let tokens = name.split('/').collect::<Vec<&str>>();
        hs.insert(tokens[0].to_string());
    }
    // transform that hashset to a vector before returning
    let mut streams = Vec::new();
    for v in hs {
        streams.push(Stream { name: v });
    }
    Ok(streams)
}

#[derive(Serialize)]
pub struct Stream {
    name: String,
}
