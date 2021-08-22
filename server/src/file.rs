use std::fs;
use serde::Deserialize;
use std::env;


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
}

fn read_toml(file_name: &str) -> ConfigToml {
    let contents = fs::read_to_string(file_name)
        .expect("Error reading file");
    let package_info: ConfigToml = toml::from_str(&contents).unwrap();
    package_info
}

pub fn export_env() {
    let ( secret_key, access_key, region, endpoint_url ) = ("AWS_SECRET_ACCESS_KEY", "AWS_ACCESS_KEY_ID", "AWS_DEFAULT_REGION", "AWS_ENDPOINT_URL");
    let  data = vec![secret_key, access_key, region, endpoint_url];

    for data in data.iter() {
        let cf = read_toml("Config.toml");
        match data {
            &"AWS_SECRET_ACCESS_KEY" => env::set_var(secret_key, cf.s3.aws_secret_key),
            &"AWS_ACCESS_KEY_ID" => env::set_var(access_key, cf.s3.aws_access_key_id),
            &"AWS_DEFAULT_REGION" => env::set_var(region, cf.s3.aws_default_region),
            &"AWS_ENDPOINT_URL" => env::set_var(endpoint_url, cf.s3.aws_endpoint_url),
            _ => println!("{:?}",cf),
        }
    }
}
