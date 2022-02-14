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
use actix_web::{middleware, web, App, HttpServer};
use actix_web_httpauth::extractors::basic::BasicAuth;
use actix_web_httpauth::middleware::HttpAuthentication;

use std::thread;
use std::time::Duration;
extern crate ticker;

mod banner;
mod event;
mod handler;
mod load_memstore;
mod mem_store;
mod option;
mod query;
mod response;
mod storage;
mod sync_s3;
mod utils;

// Global configurations
const MAX_EVENT_PAYLOAD_SIZE: usize = 102400;

#[actix_web::main]
async fn main() -> anyhow::Result<()> {
    env_logger::init();
    banner::print();
    let opt = option::get_opts();
    let _result = load_memstore::load_memstore(opt.clone());
    wrap(opt.clone()).await?;
    run_http(opt).await?;

    Ok(())
}

async fn wrap(opt: option::Opt) -> anyhow::Result<()> {
    thread::spawn(move || {
        let ticker = ticker::Ticker::new(0.., Duration::from_secs(1));
        for _ in ticker {
            match sync_s3::syncer(opt.clone()) {
                Ok(_) => {}
                Err(e) => println!("{}", e),
            }
        }
    });
    Ok(())
}

async fn validator(
    req: ServiceRequest,
    credentials: BasicAuth,
) -> Result<ServiceRequest, actix_web::Error> {
    let opt = option::get_opts();

    match req.headers().get("AUTHORIZATION") {
        Some(_auth) => {
            if credentials.user_id().trim() == opt.username.unwrap()
                && credentials.password().unwrap().trim() == opt.password.unwrap()
            {
                Ok(req)
            } else {
                Err(actix_web::error::ErrorUnauthorized("Unauthorized"))
            }
        }
        None => Ok(req),
    }
}

async fn run_http(opt: option::Opt) -> anyhow::Result<()> {
    match opt.username {
        Some(_username) => match opt.password {
            Some(_password) => {
                let http_server = HttpServer::new(move || {
                    create_app!().wrap(HttpAuthentication::basic(validator))
                });
                http_server.bind(&opt.http_addr)?.run().await?;
                Ok(())
            }
            None => Ok(()),
        },
        None => {
            let http_server = HttpServer::new(move || create_app!());
            http_server.bind(&opt.http_addr)?.run().await?;
            Ok(())
        }
    }
}

pub fn configure_routes(cfg: &mut web::ServiceConfig) {
    cfg.service(
        web::resource(utils::stream_path("/{stream}"))
            .route(web::put().to(handler::put_stream))
            .route(web::post().to(handler::post_event))
            .app_data(web::JsonConfig::default().limit(MAX_EVENT_PAYLOAD_SIZE)),
    )
    .service(web::resource(utils::stream_path("")).route(web::get().to(handler::list_streams)))
    .service(
        web::resource(utils::stream_path("/{stream}/schema"))
            .route(web::get().to(handler::get_schema)),
    )
    .service(web::resource(utils::query_path()).route(web::get().to(handler::cache_query)));
}

#[macro_export]
macro_rules! create_app {
    () => {
        App::new()
            .configure(|cfg| configure_routes(cfg))
            .wrap(middleware::Logger::default())
            .wrap(middleware::Compress::default())
    };
}
