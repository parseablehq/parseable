use std::fs;
use serde::Deserialize;

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
}

pub fn read_toml(file_name: &str) -> ConfigToml {
   
    let contents = fs::read_to_string(file_name)
        .expect("Error reading file");

    let package_info: ConfigToml = toml::from_str(&contents).unwrap();
   
    package_info
}
