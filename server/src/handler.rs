
use std::env;
use aws_sdk_s3::Error;
use actix_web::{web, HttpRequest, HttpResponse, Result};

use arrow::json::reader::infer_json_schema;
use std::io::BufReader;
use bytes::Bytes;
use arrow::json;

use lazy_static::lazy_static;
use std::collections::HashMap;
use arrow::record_batch::RecordBatch;
use std::sync::Arc;
use std::io::{Cursor, Seek, SeekFrom, Write};

use crate::storage;
use crate::option;
use std::sync::Mutex;

lazy_static! {
    pub static ref HASHMAP: Mutex<HashMap<String, RecordBatch>>= {
        let m = HashMap::new();
        Mutex::new(m)
    };    
}

pub async fn put_stream(req: HttpRequest) -> HttpResponse {
    let stream_name: String = req.match_info().get("stream").unwrap().parse().unwrap();
    match stream_exists(&stream_name) {
        Ok(_) => HttpResponse::Ok().body(format!("Stream {} already exists, please create a Stream with unique name", stream_name)),
        Err(_) => {
            match create_stream(&stream_name) {
                Ok(_) => HttpResponse::Ok().body(format!("Created Stream {}", stream_name)),
                Err(_) => HttpResponse::Ok().body(format!("Failed to create Stream {}", stream_name))
            }
        }
    }
}

// Event holds all values for server to process into record batch.
struct Event {
    body: web::Json<serde_json::Value>,
    stream_name: String,
    schema: Bytes
}

impl Event {
    fn initial_event(&self) -> HttpResponse {
        let mut map = HASHMAP.lock().unwrap();

        let mut c = Cursor::new(Vec::new());
    
        let str_body = format!("{}", &self.body);
        let reader = str_body.as_bytes();
        
        c.write_all(reader).unwrap();
        c.seek(SeekFrom::Start(0)).unwrap();

        let mut buf_reader = BufReader::new(c);
        let buf_reader1 = BufReader::new(reader);
        
        let inferred_schema = infer_json_schema(&mut buf_reader, None).unwrap(); 
        let str_inferred_schema = format!("{}", serde_json::to_string(&inferred_schema).unwrap());
        
        let mut event = json::Reader::new(buf_reader1, Arc::new(inferred_schema), 1024, None);
        let b1 = event.next().unwrap().unwrap();
        map.insert(self.stream_name.to_string(), b1);
        drop(map);

        match put_schema(&self.stream_name, str_inferred_schema) {
            Ok(_) => HttpResponse::Ok().body(format!("Uploading event to Stream {} ", self.stream_name)),
            Err(_) => HttpResponse::Ok().body(format!("Stream {} doesn't exist", self.stream_name))
        }
    }

    fn next_event(&self) -> (RecordBatch, arrow::datatypes::Schema, HttpResponse) {
        let str_inferred_schema = self.return_schema();
        if self.schema != str_inferred_schema.1 {
            // TODO: return nil, nil and invalid HTTP response
        }

        let mut c = Cursor::new(Vec::new());

        let str_body = format!("{}", self.body);
        let reader = str_body.as_bytes();
    
        c.write_all(reader).unwrap();
        c.seek(SeekFrom::Start(0)).unwrap();
        let buf_reader1 = BufReader::new(reader);
        
        let schema = self.return_schema();
        let schema_clone = schema.clone();

        let mut event = json::Reader::new(buf_reader1, Arc::new(schema.0), 1024, None);
        let b1 = event.next().unwrap().unwrap();
        return (b1, schema_clone.0,HttpResponse::Ok().body(format!("Schema already present for Stream"))    )
    }

    fn return_schema(&self) -> (arrow::datatypes::Schema ,std::string::String ){
        let str_body = format!("{}",self.body);
        let reader = str_body.as_bytes();
        let mut buf_reader = BufReader::new(reader);
        let inferred_schema = infer_json_schema(&mut buf_reader, None).unwrap();
        let  str_inferred_schema = format!("{}", serde_json::to_string(&inferred_schema).unwrap());
        return (inferred_schema, str_inferred_schema)
    }

}

pub async fn post_event(req: HttpRequest, body: web::Json<serde_json::Value>) -> HttpResponse {
    let stream_name: String = req.match_info().get("stream").unwrap().parse().unwrap();
    match stream_exists(&stream_name) {
        Ok(schema) => {
            let e = Event{
                body: body, 
                stream_name: stream_name.clone(),
                schema: schema
            };

            // If the schema is empty, this is the first event in this stream. 
            // Parse the arrow schema, upload it to <bucket>/<stream_prefix>/.schema file
            if e.schema.is_empty() {
                 e.initial_event()
            } 
            // The schema is not empty here, so this stream already has events. 
            // Proceed with validating against current schema and adding event to record batch. 
            else {
                let mut map = HASHMAP.lock().unwrap();
                let b2 = map.get(&stream_name).unwrap();
                let e2 = e.next_event();
                let vec = vec![e2.0,b2.clone()];
                let new_batch = RecordBatch::concat(&Arc::new(e2.1.clone()), &vec).unwrap();
                map.insert(stream_name, new_batch);
                println!("{:?}", map);
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
