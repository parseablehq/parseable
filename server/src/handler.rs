
use std::env;
use aws_sdk_s3::{Error};

#[tokio::main]
pub async fn create_stream(s3_client: Option<aws_sdk_s3::Client>,stream_name: String) -> Result<(), Error> {
    match s3_client {
        Some(client) => {    
            let resp = client
            .put_object()
            .bucket(env::var("AWS_BUCKET_NAME").unwrap().to_string())
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

/*
#[tokio::main]
pub async fn bucket_exists(s3_client: Option<aws_sdk_s3::Client>,bucket_name: String, stream_name: String) -> bool {
    match s3_client {
        Some(client) => {    
            let resp = client
            .get_object()
            .bucket(bucket_name)
            .key(format!("{}{}", stream_name, "/.schema"))
            .send();
            println!("Bucket {:?} Exists", bucket_name);
            return true;
        },
        None => {
         return false;
        },
    }
}
*/
