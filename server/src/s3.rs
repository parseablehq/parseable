use std::fs;
use serde::Deserialize;
use std::env;
use http::{Uri};
use aws_sdk_s3::{Client,Config,Endpoint,Error};

#[derive(Debug,Deserialize)]
pub struct ConfigToml {
    pub s3: S3,
}

#[derive(Debug,Deserialize)]
pub struct S3 {
    aws_access_key_id: String,
    aws_secret_key: String,
    aws_default_region: String,
    aws_endpoint_url: String,
    pub aws_bucket_name: String,
}

pub fn read_config(file_name: &str) -> ConfigToml {
    let contents = fs::read_to_string(file_name).expect("Error reading file");
    let config_info: ConfigToml = toml::from_str(&contents).unwrap();
    config_info
}

pub fn init_s3client(config_info: ConfigToml) -> aws_sdk_s3::Client {
    let ( secret_key, access_key, region, endpoint_url, bucket_name ) = ("AWS_SECRET_ACCESS_KEY", "AWS_ACCESS_KEY_ID", "AWS_DEFAULT_REGION", "AWS_ENDPOINT_URL", "AWS_BUCKET_NAME");
    let  data = vec![secret_key, access_key, region, endpoint_url];

    for data in data.iter() {
        match data {
            &"AWS_SECRET_ACCESS_KEY" => env::set_var(secret_key, &config_info.s3.aws_secret_key),
            &"AWS_ACCESS_KEY_ID" => env::set_var(access_key, &config_info.s3.aws_access_key_id),
            &"AWS_DEFAULT_REGION" => env::set_var(region, &config_info.s3.aws_default_region),
            &"AWS_ENDPOINT_URL" => env::set_var(endpoint_url, &config_info.s3.aws_endpoint_url),
            &"AWS_BUCKET_NAME" => env::set_var(bucket_name, &config_info.s3.aws_bucket_name),
            _ => println!("{:?}",config_info),
        }
    }
    let ep = env::var("AWS_ENDPOINT_URL").unwrap_or("none".to_string());
    let uri = ep.parse::<Uri>().unwrap();
    let endpoint = Endpoint::immutable(uri);
    let config = Config::builder().endpoint_resolver(endpoint).build();
    Client::from_conf(config)
}

#[tokio::main]
pub async fn create_stream(s3_client: Option<aws_sdk_s3::Client>,bucket_name: String, stream_name: String) -> Result<(), Error> {
    match s3_client {
        Some(client) => {    
            let resp = client
            .put_object()
            .bucket(bucket_name)
            .key(format!("{}{}", stream_name, "/.schema"))
            .send()
            .await?;
            println!("Upload success. Version: {:?}", resp.version_id);
            Ok(())     
        },
        _ => {
            Ok(())
        },
    }
}
