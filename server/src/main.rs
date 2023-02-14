/*
 * Parseable Server (C) 2022 - 2023 Parseable, Inc.
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 *
 */

use actix_cors::Cors;
use actix_web::dev::ServiceRequest;
use actix_web::{middleware, web, App, HttpServer};
use actix_web_httpauth::extractors::basic::BasicAuth;
use actix_web_httpauth::middleware::HttpAuthentication;
use actix_web_prometheus::PrometheusMetrics;
use actix_web_static_files::ResourceFiles;
use clokwerk::{AsyncScheduler, Scheduler, TimeUnits};
use log::warn;
use rustls::{Certificate, PrivateKey, ServerConfig};
use rustls_pemfile::{certs, pkcs8_private_keys};
use thread_priority::{ThreadBuilder, ThreadPriority};
use tokio::sync::oneshot;
use tokio::sync::oneshot::error::TryRecvError;

include!(concat!(env!("OUT_DIR"), "/generated.rs"));

use std::env;
use std::fs::File;
use std::io::BufReader;
use std::panic::{catch_unwind, AssertUnwindSafe};
use std::thread::{self, JoinHandle};
use std::time::Duration;

mod alerts;
mod banner;
mod event;
mod handlers;
mod metadata;
mod metrics;
mod migration;
mod option;
mod query;
mod response;
mod stats;
mod storage;
mod utils;
mod validator;

use option::CONFIG;

const MAX_EVENT_PAYLOAD_SIZE: usize = 10485760;
const API_BASE_PATH: &str = "/api";
const API_VERSION: &str = "v1";

#[actix_web::main]
async fn main() -> anyhow::Result<()> {
    env_logger::init();
    CONFIG.validate();
    let storage = CONFIG.storage().get_object_store();
    CONFIG.validate_staging()?;
    CONFIG.validate_storage(&*storage).await;
    let metadata = storage::resolve_parseable_metadata().await?;
    banner::print(&CONFIG, metadata);
    let prometheus = metrics::build_metrics_handler();
    CONFIG.storage().register_store_metrics(&prometheus);

    migration::run_migration(&CONFIG).await?;

    if let Err(e) = metadata::STREAM_INFO.load(&*storage).await {
        warn!("could not populate local metadata. {:?}", e);
    }

    // track all parquet files already in the data directory
    storage::CACHED_FILES.track_parquet();

    let (localsync_handler, mut localsync_outbox, localsync_inbox) = run_local_sync();
    let (mut remote_sync_handler, mut remote_sync_outbox, mut remote_sync_inbox) =
        object_store_sync();

    let app = run_http(prometheus);
    tokio::pin!(app);
    loop {
        tokio::select! {
            e = &mut app => {
                // actix server finished .. stop other threads and stop the server
                remote_sync_inbox.send(()).unwrap_or(());
                localsync_inbox.send(()).unwrap_or(());
                localsync_handler.join().unwrap_or(());
                remote_sync_handler.join().unwrap_or(());
                return e
            },
            _ = &mut localsync_outbox => {
                // crash the server if localsync fails for any reason
                // panic!("Local Sync thread died. Server will fail now!")
                return Err(anyhow::Error::msg("Failed to sync local data to disc. This can happen due to critical error in disc or environment. Please restart the Parseable server.\n\nJoin us on Parseable Slack if the issue persists after restart : https://launchpass.com/parseable"))
            },
            _ = &mut remote_sync_outbox => {
                // remote_sync failed, this is recoverable by just starting remote_sync thread again
                remote_sync_handler.join().unwrap_or(());
                (remote_sync_handler, remote_sync_outbox, remote_sync_inbox) = object_store_sync();
            }
        };
    }
}

fn object_store_sync() -> (JoinHandle<()>, oneshot::Receiver<()>, oneshot::Sender<()>) {
    let (outbox_tx, outbox_rx) = oneshot::channel::<()>();
    let (inbox_tx, inbox_rx) = oneshot::channel::<()>();
    let mut inbox_rx = AssertUnwindSafe(inbox_rx);
    let handle = thread::spawn(move || {
        let res = catch_unwind(move || {
            let rt = actix_web::rt::System::new();
            rt.block_on(async {
                let mut scheduler = AsyncScheduler::new();
                scheduler
                    .every((CONFIG.parseable.upload_interval as u32).seconds())
                    .run(|| async {
                        if let Err(e) = CONFIG.storage().get_object_store().sync().await {
                            warn!("failed to sync local data with object store. {:?}", e);
                        }
                    });

                loop {
                    tokio::time::sleep(Duration::from_secs(1)).await;
                    scheduler.run_pending().await;
                    match AssertUnwindSafe(|| inbox_rx.try_recv())() {
                        Ok(_) => break,
                        Err(TryRecvError::Empty) => continue,
                        Err(TryRecvError::Closed) => {
                            // should be unreachable but breaking anyways
                            break;
                        }
                    }
                }
            })
        });

        if res.is_err() {
            outbox_tx.send(()).unwrap();
        }
    });

    (handle, outbox_rx, inbox_tx)
}

fn run_local_sync() -> (JoinHandle<()>, oneshot::Receiver<()>, oneshot::Sender<()>) {
    let (outbox_tx, outbox_rx) = oneshot::channel::<()>();
    let (inbox_tx, inbox_rx) = oneshot::channel::<()>();
    let mut inbox_rx = AssertUnwindSafe(inbox_rx);

    let handle = ThreadBuilder::default()
        .name("local-sync")
        .priority(ThreadPriority::Max)
        .spawn(move |priority_result| {
            if priority_result.is_err() {
                log::warn!("Max priority cannot be set for sync thread. Make sure that user/program is allowed to set thread priority.")
            }
            let res = catch_unwind(move || {
                let mut scheduler = Scheduler::new();
                scheduler
                    .every((storage::LOCAL_SYNC_INTERVAL as u32).seconds())
                    .run(move || {
                        if let Err(e) = crate::event::STREAM_WRITERS::unset_all() {
                            warn!("failed to sync local data. {:?}", e);
                        }
                    });

                loop {
                    thread::sleep(Duration::from_millis(50));
                    scheduler.run_pending();
                    match AssertUnwindSafe(|| inbox_rx.try_recv())() {
                        Ok(_) => break,
                        Err(TryRecvError::Empty) => continue,
                        Err(TryRecvError::Closed) => {
                            // should be unreachable but breaking anyways
                            break;
                        }
                    }
                }
            });

            if res.is_err() {
                outbox_tx.send(()).unwrap();
            }
        })
        .unwrap();

    (handle, outbox_rx, inbox_tx)
}

async fn validator(
    req: ServiceRequest,
    credentials: BasicAuth,
) -> Result<ServiceRequest, (actix_web::Error, ServiceRequest)> {
    if credentials.user_id().trim() == CONFIG.parseable.username
        && credentials.password().unwrap().trim() == CONFIG.parseable.password
    {
        return Ok(req);
    }

    Err((actix_web::error::ErrorUnauthorized("Unauthorized"), req))
}

async fn run_http(prometheus: PrometheusMetrics) -> anyhow::Result<()> {
    let ssl_acceptor = match (
        &CONFIG.parseable.tls_cert_path,
        &CONFIG.parseable.tls_key_path,
    ) {
        (Some(cert), Some(key)) => {
            // init server config builder with safe defaults
            let config = ServerConfig::builder()
                .with_safe_defaults()
                .with_no_client_auth();

            // load TLS key/cert files
            let cert_file = &mut BufReader::new(File::open(cert)?);
            let key_file = &mut BufReader::new(File::open(key)?);

            // convert files to key/cert objects
            let cert_chain = certs(cert_file)?.into_iter().map(Certificate).collect();

            let mut keys: Vec<PrivateKey> = pkcs8_private_keys(key_file)?
                .into_iter()
                .map(PrivateKey)
                .collect();

            // exit if no keys could be parsed
            if keys.is_empty() {
                anyhow::bail!("Could not locate PKCS 8 private keys.");
            }

            let server_config = config.with_single_cert(cert_chain, keys.remove(0))?;

            Some(server_config)
        }
        (_, _) => None,
    };

    // concurrent workers equal to number of cores on the cpu
    let http_server = HttpServer::new(move || create_app!(prometheus)).workers(num_cpus::get());
    if let Some(config) = ssl_acceptor {
        http_server
            .bind_rustls(&CONFIG.parseable.address, config)?
            .run()
            .await?;
    } else {
        http_server.bind(&CONFIG.parseable.address)?.run().await?;
    }

    Ok(())
}

pub fn configure_routes(cfg: &mut web::ServiceConfig) {
    let generated = generate();

    cfg.service(
        // Base path "{url}/api/v1"
        web::scope(&base_path())
            // POST "/query" ==> Get results of the SQL query passed in request body
            .service(web::resource(query_path()).route(web::post().to(handlers::event::query)))
            // POST "/ingest" ==> Post logs to given log stream based on header
            .service(
                web::resource(ingest_path())
                    .route(web::post().to(handlers::event::ingest))
                    .app_data(web::JsonConfig::default().limit(MAX_EVENT_PAYLOAD_SIZE)),
            )
            .service(
                // logstream API
                web::resource(logstream_path("{logstream}"))
                    // PUT "/logstream/{logstream}" ==> Create log stream
                    .route(web::put().to(handlers::logstream::put_stream))
                    // POST "/logstream/{logstream}" ==> Post logs to given log stream
                    .route(web::post().to(handlers::event::post_event))
                    // DELETE "/logstream/{logstream}" ==> Delete log stream
                    .route(web::delete().to(handlers::logstream::delete))
                    .app_data(web::JsonConfig::default().limit(MAX_EVENT_PAYLOAD_SIZE)),
            )
            .service(
                web::resource(alert_path("{logstream}"))
                    // PUT "/logstream/{logstream}/alert" ==> Set alert for given log stream
                    .route(web::put().to(handlers::logstream::put_alert))
                    // GET "/logstream/{logstream}/alert" ==> Get alert for given log stream
                    .route(web::get().to(handlers::logstream::get_alert)),
            )
            // GET "/logstream" ==> Get list of all Log Streams on the server
            .service(
                web::resource(logstream_path("")).route(web::get().to(handlers::logstream::list)),
            )
            .service(
                // GET "/logstream/{logstream}/schema" ==> Get schema for given log stream
                web::resource(schema_path("{logstream}"))
                    .route(web::get().to(handlers::logstream::schema)),
            )
            .service(
                // GET "/logstream/{logstream}/stats" ==> Get stats for given log stream
                web::resource(stats_path("{logstream}"))
                    .route(web::get().to(handlers::logstream::get_stats)),
            )
            // GET "/liveness" ==> Liveness check as per https://kubernetes.io/docs/tasks/configure-pod-container/configure-liveness-readiness-startup-probes/#define-a-liveness-command
            .service(web::resource(liveness_path()).route(web::get().to(handlers::liveness)))
            // GET "/readiness" ==> Readiness check as per https://kubernetes.io/docs/tasks/configure-pod-container/configure-liveness-readiness-startup-probes/#define-readiness-probes
            .service(web::resource(readiness_path()).route(web::get().to(handlers::readiness)))
            .wrap(HttpAuthentication::basic(validator)),
    )
    // GET "/" ==> Serve the static frontend directory
    .service(ResourceFiles::new("/", generated));
}

#[macro_export]
macro_rules! create_app {
    ($prometheus: expr) => {
        App::new()
            .wrap($prometheus.clone())
            .configure(|cfg| configure_routes(cfg))
            .wrap(middleware::Logger::default())
            .wrap(middleware::Compress::default())
            .wrap(
                Cors::default()
                    .allow_any_header()
                    .allow_any_method()
                    .allow_any_origin(),
            )
    };
}

fn base_path() -> String {
    format!("{API_BASE_PATH}/{API_VERSION}")
}

fn logstream_path(stream_name: &str) -> String {
    if stream_name.is_empty() {
        "/logstream".to_string()
    } else {
        format!("/logstream/{stream_name}")
    }
}

fn readiness_path() -> String {
    "/readiness".to_string()
}

fn liveness_path() -> String {
    "/liveness".to_string()
}

fn query_path() -> String {
    "/query".to_string()
}

fn ingest_path() -> String {
    "/ingest".to_string()
}

fn alert_path(stream_name: &str) -> String {
    format!("{}/alert", logstream_path(stream_name))
}

fn schema_path(stream_name: &str) -> String {
    format!("{}/schema", logstream_path(stream_name))
}

fn stats_path(stream_name: &str) -> String {
    format!("{}/stats", logstream_path(stream_name))
}
