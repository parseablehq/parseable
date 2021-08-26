use std::fs;
use serde::Deserialize;
use std::env;
use std::path::Path;
use http::{Uri};
use aws_sdk_s3::{Client,ByteStream,Config,Endpoint,Error};


#[derive(Deserialize)]
#[derive(Debug)]
pub struct ConfigToml {
    s3: S3,
}

#[derive(Deserialize)]
#[derive(Debug)]
struct S3 {
    aws_access_key_id: String,
    aws_secret_key: String,
    aws_default_region: String,
    aws_endpoint_url: String,
    aws_bucket_name: String,
}

pub fn read_config(file_name: &str) -> ConfigToml {
    let contents = fs::read_to_string(file_name).expect("Error reading file");
    let config_info: ConfigToml = toml::from_str(&contents).unwrap();
    config_info
}

pub fn init_s3client(config_info: ConfigToml) -> (aws_sdk_s3::Client, String) {

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
    
    println!("{}", &config_info.s3.aws_bucket_name);

    let ep = env::var("AWS_ENDPOINT_URL").unwrap_or("none".to_string());
    let uri = ep.parse::<Uri>().unwrap();
    let endpoint = Endpoint::immutable(uri);
    let config = Config::builder().endpoint_resolver(endpoint).build();

    return (Client::from_conf(config), config_info.s3.aws_bucket_name)
}

#[tokio::main]
pub async fn create_stream(s3_client: aws_sdk_s3::Client,bucket_name: String, stream_name: &str) -> Result<(), Error> {

    let body = ByteStream::from_path(Path::new("Cargo.toml")).await;
    
    match body {
        Ok(b) => {
            let resp = s3_client
                .put_object()
                .bucket(bucket_name)
                .key(format!("{}{}", stream_name, "/.schema"))
                .body(b)
                .send()
                .await?;

            println!("Upload success. Version: {:?}", resp.version_id);
        }
        Err(e) => {
            println!("Got an error DOING SOMETHING:");
            println!("{}", e);
        }
    }
    Ok(())     
}
