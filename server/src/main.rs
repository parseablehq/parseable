/*
 * Parseable Server (C) 2022 Parseable, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

use actix_web::dev::ServiceRequest;
use actix_web::{middleware, web, App, Error, HttpServer};
use actix_web_httpauth::extractors::basic::BasicAuth;
use std::path::Path;
use std::{fs, io};

mod banner;
mod event;
mod handler;
mod mem_store;
mod option;
mod response;
mod storage;
mod utils;

// Init
// Read S3
// Fetch all schemas
// Local cache for schemas/stream
// config file validation

#[actix_web::main]
async fn main() -> anyhow::Result<()> {
    env_logger::init();
    banner::print();
    let opt = option::get_opts();
    // Check local data path and load streams and corresponding schema to 
    // internal in-memory store
    if Path::new(&opt.local_disk_path).exists() {
        let entries = fs::read_dir(&opt.local_disk_path)?
            .map(|res| res.map(|e| e.path()))
            .collect::<Result<Vec<_>, io::Error>>()?;
        for entry in entries {
            let path = format!("{:?}", entry);
            let new_path = utils::rem_first_and_last(&path);
            let new_parquet_path = format!("{}/{}", &new_path, "data.parquet");
            let new_schema_path = format!("{}/{}", &new_path, ".schema");
            if Path::new(&new_parquet_path).exists() {
                let parquet_file = fs::File::open(new_parquet_path).unwrap();
                let rb_reader = utils::convert_parquet_rb_reader(parquet_file);
                let tokens: Vec<&str> = new_path.split("/").collect();
                for rb in rb_reader {
                    let stream_name: String = tokens[2].to_string();
                    mem_store::MEM_STREAMS::put(
                        stream_name,
                        mem_store::Stream {
                            schema: Some(fs::read_to_string(&new_schema_path)?.parse()?),
                            rb: Some(rb.unwrap()),
                        },
                    );
                }
            }
        }
    }
    run_http(opt).await?;
    Ok(())
}

async fn run_http(opt: option::Opt) -> anyhow::Result<()> {
    let opt_clone = opt.clone();
    let http_server = HttpServer::new(move || create_app!(opt_clone)).disable_signals();
    http_server.bind(&opt.http_addr)?.run().await?;
    Ok(())
}

async fn validator(req: ServiceRequest, _credentials: BasicAuth) -> Result<ServiceRequest, Error> {
    // pass through for now
    Ok(req)
}

pub fn configure_routes(cfg: &mut web::ServiceConfig) {
    cfg.service(
        web::resource("/v1/{stream}")
            .route(web::put().to(handler::put_stream))
            .route(web::post().to(handler::post_event)),
    );
}

pub fn configure_auth(cfg: &mut web::ServiceConfig, opts: &option::Opt) {
    if opts.master_key.is_none() {
        cfg.app_data(validator);
    } else {
        cfg.app_data(validator);
    }
}

#[macro_export]
macro_rules! create_app {
    ($opt:expr) => {
        App::new()
            .configure(|cfg| configure_routes(cfg))
            .configure(|cfg| configure_auth(cfg, &$opt))
            .wrap(middleware::Logger::default())
            .wrap(middleware::Compress::default())
    };
}
