use std::{collections::{HashMap, HashSet}, sync::Arc};

use actix_web::{web::{self, ServiceConfig}, App, HttpServer, Resource, Scope};
use chrono::{DateTime, Utc};
use once_cell::sync::Lazy;
use tokio::sync::{oneshot, Mutex};
use tonic::async_trait;
use crate::{analytics, banner, handlers::{airplane, http::{base_path, cluster::init_cluster_metrics_scheduler, cross_origin_config, health_check, logstream::{self, create_internal_stream_if_not_exists}, middleware::RouteExt, API_BASE_PATH, API_VERSION, MAX_EVENT_PAYLOAD_SIZE}}, hottier::HotTierManager, metrics, migration, option::CONFIG, rbac::{self, role::Action}, storage, sync, users::{dashboards::DASHBOARDS, filters::FILTERS}};
use super::{coordinator::coordinator_query, query::{querier_ingest, querier_logstream}, server::Server, ssl_acceptor::get_ssl_acceptor, OpenIdClient, ParseableServer};

pub static QUERY_COORDINATION: Lazy<QueryCoordination> = Lazy::new(|| QueryCoordination::default());

#[derive(Debug, Clone)]
pub struct QueryNodeStats {
    pub ticket: String,
    pub start_time: DateTime<Utc>,
    pub hottier_info: Option<Vec<String>>
}

#[derive(Debug, Clone, Default)]
pub struct QueryCoordination {
    pub available_nodes: HashSet<String>,
    pub query_map: HashMap<String, QueryNodeStats>
}

#[derive(Debug, Default)]
pub struct CoordinatorServer;

#[async_trait(?Send)]
impl ParseableServer for CoordinatorServer {
    async fn start(
        &self,
        prometheus: actix_web_prometheus::PrometheusMetrics,
        oidc_client: Option<crate::oidc::OpenidConfig>
    ) -> anyhow::Result<()> {
        let oidc_client = match oidc_client {
            Some(config) => {
                let client = config
                    .connect(&format!("{API_BASE_PATH}/{API_VERSION}/o/code"))
                    .await?;
                Some(Arc::new(client))
            }
            None => None
        };

        let ssl = get_ssl_acceptor(
            &CONFIG.parseable.tls_cert_path,
            &CONFIG.parseable.tls_key_path,
            &CONFIG.parseable.trusted_ca_certs_path
        )?;

        let create_app_fn = move || {
            App::new()
                .wrap(prometheus.clone())
                .configure(|config| CoordinatorServer::configure_routes(config, oidc_client.clone()))
                .wrap(actix_web::middleware::Logger::default())
                .wrap(actix_web::middleware::Compress::default())
                .wrap(cross_origin_config())
        };

        // Create a channel to trigger server shutdown
        let (shutdown_trigger, shutdown_rx) = oneshot::channel::<()>();
        let server_shutdown_signal = Arc::new(Mutex::new(Some(shutdown_trigger)));

        // Clone the shutdown signal for the signal handler
        let shutdown_signal = server_shutdown_signal.clone();

        // Spawn the signal handler task
        tokio::spawn(async move {
            health_check::handle_signals(shutdown_signal).await;
        });

        // Create the HTTP server
        let http_server = HttpServer::new(create_app_fn)
            .workers(num_cpus::get())
            .shutdown_timeout(120);

        // Start the server with or without TLS
        let srv = if let Some(config) = ssl {
            http_server
                .bind_rustls_0_22(&CONFIG.parseable.address, config)?
                .run()
        } else {
            http_server.bind(&CONFIG.parseable.address)?.run()
        };

        // Graceful shutdown handling
        let srv_handle = srv.handle();

        tokio::spawn(async move {
            // Wait for the shutdown signal
            shutdown_rx.await.ok();

            // Initate graceful shutdown
            log::info!("Graceful shutdown of HTTP server triggered");
            srv_handle.stop(true).await;
        });

        // Await the server to run and handle shutdown
        srv.await?;
        
        Ok(())
    }

    async fn init(&self) -> anyhow::Result<()>{
        self.validate()?;
        migration::run_file_migration(&CONFIG).await?;
        let parseable_json = CONFIG.validate_storage().await?;
        migration::run_metadata_migration(&CONFIG, &parseable_json).await?;
        let metadata = storage::resolve_parseable_metadata(&parseable_json).await?;
        banner::print(&CONFIG, &metadata).await;
        // Initialize the rbac map
        rbac::map::init(&metadata);
        // keep the metadata info in mem
        metadata.set_global();
        self.initialize().await
    }

    fn validate(&self) -> anyhow::Result<()> {
        if CONFIG.get_storage_mode_string() == "Local drive" {
            return Err(anyhow::anyhow!(
                 "Coordinator Server cannot be started in local storage mode. Please start the server in a supported storage mode.",
             ));
        }

        Ok(())
    }
}

// TODO: modify API access for all the servers
impl CoordinatorServer {
    fn configure_routes(config: &mut ServiceConfig, oidc_client: Option<OpenIdClient>) {
        config.service(
            web::scope(&base_path())
            .service(Self::get_query_factory()) // TODO
            .service(Server::get_trino_factory()) // No change
            .service(Server::get_cache_webscope()) // Remove?? 
            .service(Server::get_liveness_factory()) // No change
            .service(Server::get_readiness_factory()) // No change??
            .service(Server::get_about_factory()) // Handle hottier
            // .service(Self::get_logstream_webscope()) 
            // .service(Self::get_user_webscope())
            // .service(Server::get_dashboards_webscope())
            // .service(Server::get_filters_webscope())
            // .service(Server::get_llm_webscope())
            // .service(Server::get_oauth_webscope(oidc_client))
            // .service(Self::get_user_role_webscope())
            // .service(Server::get_metrics_webscope())
            // .service(Self::get_cluster_web_scope()),
        )
        .service(Server::get_generated());
    }

    fn get_query_factory() -> Resource {
        web::resource("/query")
            .route(web::post().to(coordinator_query::query).authorize(Action::Query))
    }

    // get the logstream web scope
    fn get_logstream_webscope() -> Scope {
        web::scope("/logstream")
            .service(
                // GET "/logstream" ==> Get list of all Log Streams on the server
                web::resource("")
                    .route(web::get().to(logstream::list).authorize(Action::ListStream)),
            )
            .service(
                web::scope("/{logstream}")
                    .service(
                        web::resource("")
                            // PUT "/logstream/{logstream}" ==> Create log stream
                            .route(
                                web::put()
                                    .to(querier_logstream::put_stream)
                                    .authorize_for_stream(Action::CreateStream),
                            )
                            // POST "/logstream/{logstream}" ==> Post logs to given log stream
                            .route(
                                web::post()
                                    .to(querier_ingest::post_event)
                                    .authorize_for_stream(Action::Ingest),
                            )
                            // DELETE "/logstream/{logstream}" ==> Delete log stream
                            .route(
                                web::delete()
                                    .to(querier_logstream::delete)
                                    .authorize_for_stream(Action::DeleteStream),
                            )
                            .app_data(web::PayloadConfig::default().limit(MAX_EVENT_PAYLOAD_SIZE)),
                    )
                    .service(
                        // GET "/logstream/{logstream}/info" ==> Get info for given log stream
                        web::resource("/info").route(
                            web::get()
                                .to(logstream::get_stream_info)
                                .authorize_for_stream(Action::GetStreamInfo),
                        ),
                    )
                    .service(
                        web::resource("/alert")
                            // PUT "/logstream/{logstream}/alert" ==> Set alert for given log stream
                            .route(
                                web::put()
                                    .to(logstream::put_alert)
                                    .authorize_for_stream(Action::PutAlert),
                            )
                            // GET "/logstream/{logstream}/alert" ==> Get alert for given log stream
                            .route(
                                web::get()
                                    .to(logstream::get_alert)
                                    .authorize_for_stream(Action::GetAlert),
                            ),
                    )
                    .service(
                        // GET "/logstream/{logstream}/schema" ==> Get schema for given log stream
                        web::resource("/schema").route(
                            web::get()
                                .to(logstream::schema)
                                .authorize_for_stream(Action::GetSchema),
                        ),
                    )
                    .service(
                        // GET "/logstream/{logstream}/stats" ==> Get stats for given log stream
                        web::resource("/stats").route(
                            web::get()
                                .to(querier_logstream::get_stats)
                                .authorize_for_stream(Action::GetStats),
                        ),
                    )
                    .service(
                        web::resource("/retention")
                            // PUT "/logstream/{logstream}/retention" ==> Set retention for given logstream
                            .route(
                                web::put()
                                    .to(logstream::put_retention)
                                    .authorize_for_stream(Action::PutRetention),
                            )
                            // GET "/logstream/{logstream}/retention" ==> Get retention for given logstream
                            .route(
                                web::get()
                                    .to(logstream::get_retention)
                                    .authorize_for_stream(Action::GetRetention),
                            ),
                    )
                    .service(
                        web::resource("/cache")
                            // PUT "/logstream/{logstream}/cache" ==> Set retention for given logstream
                            .route(
                                web::put()
                                    .to(querier_logstream::put_enable_cache)
                                    .authorize_for_stream(Action::PutCacheEnabled),
                            )
                            // GET "/logstream/{logstream}/cache" ==> Get retention for given logstream
                            .route(
                                web::get()
                                    .to(querier_logstream::get_cache_enabled)
                                    .authorize_for_stream(Action::GetCacheEnabled),
                            ),
                    )
                    .service(
                        web::resource("/hottier")
                            // PUT "/logstream/{logstream}/hottier" ==> Set hottier for given logstream
                            .route(
                                web::put()
                                    .to(logstream::put_stream_hot_tier)
                                    .authorize_for_stream(Action::PutHotTierEnabled),
                            )
                            .route(
                                web::get()
                                    .to(logstream::get_stream_hot_tier)
                                    .authorize_for_stream(Action::GetHotTierEnabled),
                            )
                            .route(
                                web::delete()
                                    .to(logstream::delete_stream_hot_tier)
                                    .authorize_for_stream(Action::DeleteHotTierEnabled),
                            ),
                    ),
            )
    }

    async fn initialize(&self) -> anyhow::Result<()> {
        let prometheus = metrics::build_metrics_handler();
        CONFIG.storage().register_store_metrics(&prometheus);

        migration::run_migration(&CONFIG).await?;

        // create internal stream at server start
        create_internal_stream_if_not_exists().await?;

        FILTERS.load().await?;
        DASHBOARDS.load().await?;

        // tack all parquet files already in the data directory
        storage::retention::load_retention_from_global();

        // all internal data structures populated now.
        // start the analytics scheduler if enabled
        if CONFIG.parseable.send_analytics {
            analytics::init_analytics_scheduler()?;
        }

        if matches!(init_cluster_metrics_scheduler(), Ok(())) {
            log::info!("Cluster metrics scheduler started successfully");
        }

        // // TODO: HotTier!!!!
        // if let Some(hot_tier_manager) = HotTierManager::global() {
        //     hot_tier_manager.put_internal_stream_hot_tier().await?;
        //     hot_tier_manager.download_from_s3()?;
        // };

        let (localsync_handler, mut localsync_outbox, localsync_inbox) = sync::object_store_sync().await;

        let (mut remote_sync_handler, mut remote_sync_outbox, mut remote_sync_inbox) = sync::object_store_sync().await;

        tokio::spawn(airplane::server());
        let app = self.start(prometheus, CONFIG.parseable.openid.clone());

        tokio::pin!(app);
        loop {
            tokio::select! {
                e = &mut app => {
                    // actix server finished, stop other threads and stop the server
                    remote_sync_inbox.send(()).unwrap_or(());
                    localsync_inbox.send(()).unwrap_or(());
                    if let Err(e) = localsync_handler.await {
                        log::error!("Error joining localsync_handler: {e:?}");
                    }
                    if let Err(e) = remote_sync_handler.await {
                        log::error!("Error joining remote_sync_handler: {e:?}");
                    }
                    return e
                },
                _ = &mut remote_sync_outbox => {
                    // remote_sync failed, this is recoverable by just starting remote_sync thread again
                    if let Err(e) = remote_sync_handler.await {
                        log::error!("Error joining remote_sync_handler: {e:?}");
                    }
                    (remote_sync_handler, remote_sync_outbox, remote_sync_inbox) = sync::object_store_sync().await;
                }
            };
        }
    }
}