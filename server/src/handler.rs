
use std::env;
use aws_sdk_s3::{Error};
use actix_web::{HttpRequest, HttpResponse};
use crate::config;

pub async fn put_stream(req: HttpRequest) -> HttpResponse {
    let stream_name: String = req.match_info().get("stream").unwrap().parse().unwrap();
    let s3_client = config::ConfigToml::s3client();
    match stream_exists(&s3_client, &stream_name) {
        Ok(_) => HttpResponse::Ok().body(format!("Stream {} already exists, please create a Stream with unique name", stream_name)),
        Err(_) => {
            match create_stream(&s3_client, &stream_name) {
                Ok(_) => HttpResponse::Ok().body(format!("Created Stream {}", stream_name)),
                Err(_) => HttpResponse::Ok().body(format!("Failed to create Stream {}", stream_name))
            }
        }
    }
}

pub async fn post_event(req: HttpRequest) -> HttpResponse {
    let stream_name: String = req.match_info().get("stream").unwrap().parse().unwrap();
    let s3_client = config::ConfigToml::s3client();
    match stream_exists(&s3_client, &stream_name) {
        Ok(_) => HttpResponse::Ok().body(format!("Uploading event to Stream {} ", stream_name)),
        Err(_) => HttpResponse::Ok().body(format!("Stream {} doesn't exist", stream_name))
    }
    // TODO
    // 1. Check if this is the first event in the stream
    //  a. If yes, create a schema and upload the schema file to <bucket>/<stream_prefix>/.schema.
    //  b. If no, validate if the schema of new event matches existing schema. Fail with invalid schema, if no match.
    // 2. Add the event to existing Arrow RecordBatch. 
    // 3. Check if event count threshold is reached, convert record batch to parquet and push to S3.
    // 4. Init new RecordBatch if previos record batch was pushed to S3.
}

#[tokio::main]
pub async fn create_stream(s3_client: &aws_sdk_s3::Client, stream_name: &String) -> Result<(), Error> {
    let _resp = s3_client
        .put_object()
        .bucket(env::var("AWS_BUCKET_NAME").unwrap().to_string())
        .key(format!("{}{}", stream_name, "/.schema"))
        .send()
        .await?;
    Ok(())     
}

#[tokio::main]
pub async fn stream_exists(s3_client: &aws_sdk_s3::Client, stream_name: &String) -> Result<(), Error> {
    let _resp = s3_client
        .get_object()
        .bucket(env::var("AWS_BUCKET_NAME").unwrap().to_string())
        .key(format!("{}{}", stream_name, "/.schema"))
        .send()
        .await?;
    Ok(())
}
