use std::env;
use std::sync::Arc;
use aws_sdk_s3::Error;
use actix_web::{web, HttpRequest, HttpResponse,  Result};
use arrow::record_batch::RecordBatch;
use bytes::Bytes;
use std::fs;
use std::io::prelude::*;


use crate::storage;
use crate::option;
use crate::event;
use crate::response;

pub async fn put_stream(req: HttpRequest) -> HttpResponse {
    let stream_name: String = req.match_info().get("stream").unwrap().parse().unwrap();
    match stream_exists(&stream_name) {
        Ok(_) => {
            let r = response::ServerResponse{
                http_response: HttpResponse::Ok(),
                msg: format!("Stream {} already exists, please create a Stream with unique name", stream_name).to_string()
            };
            r.error_server_response()
        }
        Err(_) => {
            match create_stream(&stream_name) {
                Ok(_) => {
                    let r = response::ServerResponse{
                        http_response: HttpResponse::Ok(),
                        msg: format!("Created Stream {}", stream_name)
                    };
                    r.success_server_response()
                }
                Err(e) => {
                    let r = response::ServerResponse{
                        http_response: HttpResponse::Ok(),
                        msg: format!("Failed to create Stream due to err: {}", e)
                    };
                    r.error_server_response()
                }
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
            } else {
                let mut map = event::HASHMAP.lock().unwrap();
                let b2 = map.get(&stream_name).unwrap();
                let e2 = e.next_event();
                let vec = vec![e2.0,b2.clone()];
                let new_batch = RecordBatch::concat(&Arc::new(e2.1.clone()), &vec).unwrap();
                map.insert(stream_name.clone(), new_batch.clone());
                println!("{:?}", map);
                e.convert_arrow_parquet(new_batch);
                drop(map);
                let r = response::ServerResponse{
                    http_response: HttpResponse::Ok(),
                    msg: format!("Event appended to Record Batch successfully for stream {}", &stream_name)
                };
                r.success_server_response()
            }
        },
        Err(e) => {
            let r = response::ServerResponse{
                http_response: HttpResponse::Ok(),
                msg: format!("Stream {} Does not Exist, Error: {}", &stream_name, e)
            };
            r.error_server_response()
        }
    }
}

#[tokio::main]
pub async fn put_schema(stream_name: &String, schema: String) -> Result<(), Error> {
    let opt = option::get_opts();
    let client = storage::setup_storage(&opt).client;
    let s = schema.clone();
    let _resp = client
        .put_object()
        .bucket(env::var("AWS_BUCKET_NAME").unwrap().to_string())
        .key(format!("{}{}", stream_name, "/.schema"))
        .body(schema.into_bytes().into())
        .send()
        .await?;
    let dir_name = format!("{}{}{}", opt.local_disk_path, "/", stream_name);
    let _res = fs::create_dir_all(dir_name.clone());
    let file_name = format!("{}{}{}", dir_name, "/", "/.schema");
    let mut schema_file = fs::File::create(file_name).unwrap();
    schema_file.write_all(s.as_bytes()).expect("Unable to write data");
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
