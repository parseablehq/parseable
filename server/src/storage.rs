use aws_sdk_s3::{Client,Endpoint};
use std::env;
use http::{Uri};

use crate::option;

pub struct S3 {
    pub client: aws_sdk_s3::Client,
}

pub trait ObjectStorage {
    fn new(opt: &option::Opt) -> Self;
}

impl ObjectStorage for S3 {
    fn new (opt: &option::Opt) -> S3 {
        S3 { client: s3_client(&opt) }
    }
}

fn s3_client(opt: &option::Opt) -> aws_sdk_s3::Client {
    let ( secret_key, access_key, region, endpoint_url, bucket_name ) = ("AWS_SECRET_ACCESS_KEY", "AWS_ACCESS_KEY_ID", "AWS_DEFAULT_REGION", "AWS_ENDPOINT_URL", "AWS_BUCKET_NAME");
    let  data = vec![secret_key, access_key, region, endpoint_url, bucket_name];

    for data in data.iter() {
        match data {
            &"AWS_SECRET_ACCESS_KEY" => env::set_var(secret_key, &opt.s3_secret_key),
            &"AWS_ACCESS_KEY_ID" => env::set_var(access_key, &opt.s3_access_key_id),
            &"AWS_DEFAULT_REGION" => env::set_var(region, &opt.s3_default_region),
            &"AWS_ENDPOINT_URL" => env::set_var(endpoint_url, &opt.s3_endpoint_url),
            &"AWS_BUCKET_NAME" => env::set_var(bucket_name, &opt.s3_bucket_name),
            _ => println!(""),
        }     
    }
    let ep = env::var("AWS_ENDPOINT_URL").unwrap_or("none".to_string());
    let uri = ep.parse::<Uri>().unwrap();
    let endpoint = Endpoint::immutable(uri);
    let config = aws_sdk_s3::Config::builder().endpoint_resolver(endpoint).build();
    Client::from_conf(config)
}

pub fn setup_storage(opt: &option::Opt) -> S3 {
    S3::new(&opt)
}