use std::env;
use aws_sdk_s3::Error;
use actix_web::{web, HttpRequest, HttpResponse, Result};

use arrow::record_batch::RecordBatch;
use std::sync::Arc;

use bytes::Bytes;

use crate::storage;
use crate::option;
use crate::event;

pub async fn put_stream(req: HttpRequest) -> HttpResponse {
    let stream_name: String = req.match_info().get("stream").unwrap().parse().unwrap();
    match stream_exists(&stream_name) {
        Ok(_) => HttpResponse::Ok().body(format!("Stream {} already exists, please create a Stream with unique name", stream_name)),
        Err(e) => {
            match create_stream(&stream_name) {
                Ok(_) => HttpResponse::Ok().body(format!("Created Stream {}", stream_name)),
                Err(e) => HttpResponse::Ok().body(format!("Failed to create Stream {}", e))
            }
        }
    }
}

pub async fn post_event(req: HttpRequest, body: web::Json<serde_json::Value>) -> HttpResponse {
    let stream_name: String = req.match_info().get("stream").unwrap().parse().unwrap();
    match stream_exists(&stream_name) {
        Ok(schema) => {
            let e = event::Event{
                body: body, 
                path: option::get_opts().local_disk_path,
                stream_name: stream_name.clone(),
                schema: schema
            };

            // If the schema is empty, this is the first event in this stream. 
            // Parse the arrow schema, upload it to <bucket>/<stream_prefix>/.schema file
            if e.schema.is_empty() {
                 e.initial_event()
            } 
            else {
                let mut map = event::HASHMAP.lock().unwrap();
                let b2 = map.get(&stream_name).unwrap();
                let e2 = e.next_event();
                let vec = vec![e2.0,b2.clone()];
                let new_batch = RecordBatch::concat(&Arc::new(e2.1.clone()), &vec).unwrap();
                map.insert(stream_name.clone(), new_batch.clone());
                println!("{:?}", map);
                e.convert_arrow_parquet(new_batch);
                drop(map);
                HttpResponse::Ok().body(format!("Schema already present for Stream"))
            }
        },
        Err(_) => HttpResponse::Ok().body(format!("Stream {} doesn't exist", stream_name))
    }
}

#[tokio::main]
pub async fn put_schema(stream_name: &String, schema: String) -> Result<(), Error> {
    let opt = option::get_opts();
    let client = storage::setup_storage(&opt).client;
    let _resp = client
        .put_object()
        .bucket(env::var("AWS_BUCKET_NAME").unwrap().to_string())
        .key(format!("{}{}", stream_name, "/.schema"))
        .body(schema.into_bytes().into())
        .send()
        .await?;
    Ok(())
}

#[tokio::main]
pub async fn create_stream(stream_name: &String) -> Result<(), Error> {
    let opt = option::get_opts();
    let client = storage::setup_storage(&opt).client;
    let _resp = client
        .put_object()
        .bucket(env::var("AWS_BUCKET_NAME").unwrap().to_string())
        .key(format!("{}{}", stream_name, "/.schema"))
        .send()
        .await?;
    Ok(())
}

#[tokio::main]
pub async fn stream_exists(stream_name: &String) -> Result<Bytes, Error> {
    let opt = option::get_opts();
    let client = storage::setup_storage(&opt).client;
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
