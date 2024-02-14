/*
 * Parseable Server (C) 2022 - 2024 Parseable, Inc.
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

use crate::analytics;
use crate::banner;
use crate::handlers::http::logstream;
use crate::handlers::http::middleware::RouteExt;
use crate::localcache::LocalCacheManager;
use crate::metadata;
use crate::metrics;
use crate::rbac::role::Action;
use crate::storage;
use crate::storage::ObjectStorageError;
use crate::storage::PARSEABLE_METADATA_FILE_NAME;
use crate::sync;

use std::net::SocketAddr;

use super::server::Server;
use super::ssl_acceptor::get_ssl_acceptor;
use super::IngesterMetadata;
use super::OpenIdClient;
use super::ParseableServer;
use super::DEFAULT_VERSION;

use actix_web::body::MessageBody;
use actix_web::Scope;
use actix_web::{web, App, HttpServer};
use actix_web_prometheus::PrometheusMetrics;
use async_trait::async_trait;
use relative_path::RelativePathBuf;
use url::Url;

use crate::{
    handlers::http::{base_path, cross_origin_config},
    option::CONFIG,
};

#[derive(Default)]
pub struct IngestServer;

#[async_trait(?Send)]
impl ParseableServer for IngestServer {
    // we dont need oidc client here its just here to satisfy the trait
    async fn start(
        &mut self,
        prometheus: PrometheusMetrics,
        _oidc_client: Option<crate::oidc::OpenidConfig>,
    ) -> anyhow::Result<()> {
        // set the ingestor metadata
        self.set_ingestor_metadata().await?;

        // get the ssl stuff
        let ssl = get_ssl_acceptor(
            &CONFIG.parseable.tls_cert_path,
            &CONFIG.parseable.tls_key_path,
        )?;

        // fn that creates the app
        let create_app_fn = move || {
            App::new()
                .wrap(prometheus.clone())
                .configure(|config| IngestServer::configure_routes(config, None))
                .wrap(actix_web::middleware::Logger::default())
                .wrap(actix_web::middleware::Compress::default())
                .wrap(cross_origin_config())
        };

        // concurrent workers equal to number of logical cores
        let http_server = HttpServer::new(create_app_fn).workers(num_cpus::get());

        if let Some(config) = ssl {
            http_server
                .bind_rustls(&CONFIG.parseable.address, config)?
                .run()
                .await?;
        } else {
            http_server.bind(&CONFIG.parseable.address)?.run().await?;
        }

        Ok(())
    }

    /// implement the init method will just invoke the initialize method
    async fn init(&mut self) -> anyhow::Result<()> {
        // self.validate()?;
        self.initialize().await
    }

    #[allow(unused)]
    fn validate(&self) -> anyhow::Result<()> {
        if CONFIG.get_storage_mode_string() == "Local drive" {
            return Err(anyhow::Error::msg(
                // Error Message can be better
                "Ingest Server cannot be started in local storage mode. Please start the server in a supported storage mode.",
            ));
        }

        Ok(())
    }
}

impl IngestServer {
    // configure the api routes
    fn configure_routes(config: &mut web::ServiceConfig, _oidc_client: Option<OpenIdClient>) {
        config
            .service(
                // Base path "{url}/api/v1"
                web::scope(&base_path()).service(Server::get_ingest_factory()),
            )
            .service(Server::get_liveness_factory())
            .service(Server::get_readiness_factory())
            .service(Self::get_metrics_webscope());
    }

    fn get_metrics_webscope() -> Scope {
        web::scope("/logstream").service(
            web::scope("/{logstream}")
                .service(
                    // GET "/logstream/{logstream}/schema" ==> Get schema for given log stream
                    web::resource("/schema").route(
                        web::get()
                            .to(logstream::schema)
                            .authorize_for_stream(Action::GetSchema),
                    ),
                )
                .service(
                    web::resource("/stats").route(
                        web::get()
                            .to(logstream::get_stats)
                            .authorize_for_stream(Action::GetStats),
                    ),
                ),
        )
    }

    #[inline(always)]
    fn get_ingestor_address(&self) -> SocketAddr {
        // this might cause an issue down the line
        // best is to make the Cli Struct better, but thats a chore
        (CONFIG.parseable.address.clone())
            .parse::<SocketAddr>()
            .unwrap()
    }

    // create the ingestor metadata and put the .ingestor.json file in the object store
    async fn set_ingestor_metadata(&self) -> anyhow::Result<()> {
        let store = CONFIG.storage().get_object_store();

        // remove ip adn go with the domain name
        let sock = self.get_ingestor_address();
        let path = RelativePathBuf::from(format!(
            "ingestor.{}.{}.json",
            sock.ip(), // this might be wrong
            sock.port()
        ));

        if store.get_object(&path).await.is_ok() {
            println!("Ingestor metadata already exists");
            return Ok(());
        };

        let resource = IngesterMetadata::new(
            sock.port().to_string(),
            CONFIG
                .parseable
                .domain_address
                .clone()
                .unwrap_or_else(|| {
                    Url::parse(&format!("https://{}:{}", sock.ip(), sock.port())).unwrap()
                })
                .to_string(),
            DEFAULT_VERSION.to_string(),
            store.get_bucket_name(),
            CONFIG.parseable.username.clone(),
            CONFIG.parseable.password.clone(), // is this secure?
        );

        let resource = serde_json::to_string(&resource)
            .unwrap()
            .try_into_bytes()
            .unwrap();

        store.put_object(&path, resource).await?;

        Ok(())
    }

    // check for querier state. Is it there, or was it there in the past
    // this should happen before the set the ingestor metadata
    async fn check_querier_state(&self) -> anyhow::Result<(), ObjectStorageError> {
        // how do we check for querier state?
        // based on the work flow of the system, the querier will always need to start first
        // i.e the querier will create the `.parseable.json` file

        let store = CONFIG.storage().get_object_store();
        let path = RelativePathBuf::from(PARSEABLE_METADATA_FILE_NAME);

        match store.get_object(&path).await {
            Ok(_) => Ok(()),
            Err(_) => Err(ObjectStorageError::Custom(
                "Query Server has not been started yet. Please start the querier server first."
                    .to_string(),
            )),
        }
    }

    async fn initialize(&mut self) -> anyhow::Result<()> {
        // check for querier state. Is it there, or was it there in the past
        self.check_querier_state().await?;
        // to get the .parseable.json file in staging
        let meta = storage::resolve_parseable_metadata().await?;
        banner::print(&CONFIG, &meta).await;

        // set the info in the global metadata
        meta.set_global();

        if let Some(cache_manager) = LocalCacheManager::global() {
            cache_manager
                .validate(CONFIG.parseable.local_cache_size)
                .await?;
        };

        let prom = metrics::build_metrics_handler();
        CONFIG.storage().register_store_metrics(&prom);

        let storage = CONFIG.storage().get_object_store();
        if let Err(err) = metadata::STREAM_INFO.load(&*storage).await {
            log::warn!("could not populate local metadata. {:?}", err);
        }

        metrics::fetch_stats_from_storage().await;

        let (localsync_handler, mut localsync_outbox, localsync_inbox) = sync::run_local_sync();
        let (mut remote_sync_handler, mut remote_sync_outbox, mut remote_sync_inbox) =
            sync::object_store_sync();

        // all internal data structures populated now.
        // start the analytics scheduler if enabled
        if CONFIG.parseable.send_analytics {
            analytics::init_analytics_scheduler();
        }
        let app = self.start(prom, CONFIG.parseable.openid.clone());
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
                    return Err(anyhow::Error::msg("Failed to sync local data to drive. Please restart the Parseable server.\n\nJoin us on Parseable Slack if the issue persists after restart : https://launchpass.com/parseable"))
                },
                _ = &mut remote_sync_outbox => {
                    // remote_sync failed, this is recoverable by just starting remote_sync thread again
                    remote_sync_handler.join().unwrap_or(());
                    (remote_sync_handler, remote_sync_outbox, remote_sync_inbox) = sync::object_store_sync();
                }

            };
        }
    }
}
