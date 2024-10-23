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

use std::{collections::{HashMap, HashSet}, sync::Arc};

use actix_web::{web::{self, ServiceConfig}, App, HttpServer, Resource, Scope};
use bytes::Bytes;
use chrono::{DateTime, Utc};
use http::header;
use itertools::Itertools;
use once_cell::sync::Lazy;
use rand::seq::IteratorRandom;
use relative_path::RelativePathBuf;
use reqwest::Client;
use serde_json::Value;
use tokio::sync::{oneshot, Mutex, RwLock};
use tonic::async_trait;
use crate::{analytics, banner, handlers::{airplane, http::{base_path, cluster::{get_querier_info_storage, init_cluster_metrics_scheduler, utils::check_liveness}, cross_origin_config, health_check, logstream::{self, create_internal_stream_if_not_exists}, middleware::{DisAllowRootUser, RouteExt}, users::{dashboards, filters}, API_BASE_PATH, API_VERSION, MAX_EVENT_PAYLOAD_SIZE}}, hottier::HotTierManager, metrics, migration, option::CONFIG, rbac::{self, role::Action}, storage::{self, object_storage::parseable_json_path, ObjectStorageError, PARSEABLE_ROOT_DIRECTORY}, sync, users::{dashboards::DASHBOARDS, filters::FILTERS}};
use super::{coordinator::{coordinator_cluster, coordinator_dashboards, coordinator_filters, coordinator_llm, coordinator_logstream, coordinator_metrics, coordinator_query, coordinator_rbac, coordinator_role}, query::{querier_ingest, querier_logstream}, server::Server, ssl_acceptor::get_ssl_acceptor, OpenIdClient, ParseableServer, QuerierMetadata};

pub static QUERY_COORDINATION: Lazy<Mutex<QueryCoordination>> = Lazy::new(|| Mutex::new(QueryCoordination::default()));

#[derive(Debug, Clone, Default)]
pub struct QueryNodeStats {
    pub ticket: String,
    pub start_time: DateTime<Utc>,
    pub hottier_info: Option<Vec<String>>
}

#[derive(Debug, Clone, Default)]
pub struct QueryCoordination {
    pub available_nodes: HashSet<String>,
    pub stats: HashMap<String, QueryNodeStats>,
    pub info: HashMap<String, QuerierMetadata>,
    pub leader: Option<QuerierMetadata>
}

impl QueryCoordination {
    /// selects a new leader from the given list of nodes
    async fn select_leader(&mut self, mut qmetas: Vec<QuerierMetadata>) {
                
        qmetas.sort_by_key(|item| item.start_time);

        // iterate over querier_metas to see which node is alive
        // if alive, make leader and break
        for meta in qmetas {
            if check_liveness(&meta.domain_name).await {
                self.make_leader(meta.clone()).await;
                break;
            }
        }
    }

    async fn make_leader(&mut self, leader: QuerierMetadata) {
        self.leader = Some(leader.clone());

        // send request to this leader letting it know of its promotion
        let client = Client::new();
        let token = leader.token;
        
        let domain = if let Some(domain) = leader.domain_name.strip_suffix("/") {
            domain.to_string()
        } else {
            leader.domain_name.to_string()
        };

        let target_url = format!("{domain}{}/leader",base_path());
        client.post(target_url)
            .header(header::AUTHORIZATION, token)
            .header(header::CONTENT_TYPE, "application/json")
            .send()
            .await
            .unwrap();
    }

    /// checks if the leader is alive
    /// if not, then makes self.leader = None
    pub async fn leader_liveness(&mut self) -> bool {

        if let Some(leader) = &self.leader {
            if check_liveness(&leader.domain_name).await {
                true
            } else {
                self.leader = None;
                false
            }
        } else {
            false
        }
    }

    /// gets the leader if present
    /// otherwise selects a new leader
    pub async fn get_leader(&mut self) -> anyhow::Result<QuerierMetadata> {
        if self.leader.is_some() {
            Ok(self.leader.clone().unwrap())
        } else {
            self.update().await;
            match self.leader.clone() {
                Some(l) => Ok(l),
                None => Err(anyhow::Error::msg("Please start a Query server.")),
            }
        }
    }

    /// reads node metadata from storage
    /// and selects a leader if not present
    async fn update(&mut self) {
        
        let qmetas = get_querier_info_storage().await.unwrap();
        if !qmetas.is_empty() {

            if self.leader.is_none() {
                self.select_leader(qmetas.clone()).await;
            }

            // for qm in qmetas {
            //     self.append_querier_info(qm);
            // }
        }

    }
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
            // .service(Self::register_querier_factory())
            .service(Self::get_logstream_webscope()) 
            .service(Self::get_user_webscope())
            .service(Self::get_dashboards_webscope())
            .service(Self::get_filters_webscope())
            .service(Self::get_llm_webscope())
            .service(Server::get_oauth_webscope(oidc_client))
            .service(Self::get_user_role_webscope())
            .service(Self::get_metrics_webscope())
            .service(Self::get_cluster_web_scope()),
        )
        .service(Server::get_generated());
    }

    // fn register_querier_factory() -> Resource {
    //     web::resource("/register")
    //         .route(web::post().to(coordinator_register::register_querier))
    // }

    fn get_cluster_web_scope() -> actix_web::Scope {
        web::scope("/cluster")
            .service(
                // GET "/cluster/info" ==> Get info of the cluster
                web::resource("/info").route(
                    web::get()
                        .to(coordinator_cluster::get_cluster_info)
                        .authorize(Action::ListCluster),
                ),
            )
            // GET "/cluster/metrics" ==> Get metrics of the cluster
            .service(
                web::resource("/metrics").route(
                    web::get()
                        .to(coordinator_cluster::get_cluster_metrics)
                        .authorize(Action::ListClusterMetrics),
                ),
            )
            // DELETE "/cluster/{ingestor_domain:port}" ==> Delete an ingestor from the cluster
            .service(
                web::scope("/{ingestor}").service(
                    web::resource("").route(
                        web::delete()
                            .to(coordinator_cluster::remove_ingestor)
                            .authorize(Action::Deleteingestor),
                    ),
                ),
            )
    }

    fn get_user_role_webscope() -> Scope {
        web::scope("/role")
            // GET Role List
            .service(web::resource("").route(web::get().to(coordinator_role::list).authorize(Action::ListRole)))
            .service(
                // PUT and GET Default Role
                web::resource("/default")
                    .route(web::put().to(coordinator_role::put_default).authorize(Action::PutRole))
                    .route(web::get().to(coordinator_role::get_default).authorize(Action::GetRole)),
            )
            .service(
                // PUT, GET, DELETE Roles
                web::resource("/{name}")
                    .route(web::put().to(coordinator_role::put).authorize(Action::PutRole))
                    .route(web::delete().to(coordinator_role::delete).authorize(Action::DeleteRole))
                    .route(web::get().to(coordinator_role::get).authorize(Action::GetRole)),
            )
    }

    fn get_user_webscope() -> Scope {
        web::scope("/user")
            .service(
                web::resource("")
                    // GET /user => List all users
                    .route(
                        web::get()
                            .to(coordinator_rbac::list_users)
                            .authorize(Action::ListUser),
                    ),
            )
            .service(
                web::resource("/{username}")
                    // PUT /user/{username} => Create a new user
                    .route(
                        web::post()
                            .to(coordinator_rbac::post_user)
                            .authorize(Action::PutUser),
                    )
                    // DELETE /user/{username} => Delete a user
                    .route(
                        web::delete()
                            .to(coordinator_rbac::delete_user)
                            .authorize(Action::DeleteUser),
                    )
                    .wrap(DisAllowRootUser),
            )
            .service(
                web::resource("/{username}/role")
                    // PUT /user/{username}/roles => Put roles for user
                    .route(
                        web::put()
                            .to(coordinator_rbac::put_role)
                            .authorize(Action::PutUserRoles)
                            .wrap(DisAllowRootUser),
                    )
                    .route(
                        web::get()
                            .to(coordinator_rbac::get_role)
                            .authorize_for_user(Action::GetUserRoles),
                    ),
            )
            .service(
                web::resource("/{username}/generate-new-password")
                    // POST /user/{username}/generate-new-password => reset password for this user
                    .route(
                        web::post()
                            .to(coordinator_rbac::post_gen_password)
                            .authorize(Action::PutUser)
                            .wrap(DisAllowRootUser),
                    ),
            )
    }

    fn get_metrics_webscope() -> Scope {
        web::scope("/metrics").service(
            web::resource("").route(web::get().to(coordinator_metrics::get).authorize(Action::Metrics)),
        )
    }

    fn get_llm_webscope() -> Scope {
        web::scope("/llm").service(
            web::resource("").route(
                web::post()
                    .to(coordinator_llm::make_llm_request)
                    .authorize(Action::QueryLLM),
            ),
        )
    }

    fn get_filters_webscope() -> Scope {
        web::scope("/filters")
            .service(
                web::resource("")
                    .route(
                        web::post()
                            .to(coordinator_filters::post)
                            .authorize(Action::CreateFilter),
                    )
                    .route(web::get().to(coordinator_filters::list).authorize(Action::ListFilter)),
            )
            .service(
                web::resource("/{filter_id}")
                    .route(web::get().to(coordinator_filters::get).authorize(Action::GetFilter))
                    .route(
                        web::delete()
                            .to(coordinator_filters::delete)
                            .authorize(Action::DeleteFilter),
                    )
                    .route(
                        web::put()
                            .to(coordinator_filters::update)
                            .authorize(Action::CreateFilter),
                    ),
            )
    }

    fn get_dashboards_webscope() -> Scope {
        web::scope("/dashboards")
            .service(
                web::resource("")
                    .route(
                        web::post()
                            .to(coordinator_dashboards::post)
                            .authorize(Action::CreateDashboard),
                    )
                    .route(
                        web::get()
                            .to(coordinator_dashboards::list)
                            .authorize(Action::ListDashboard),
                    ),
            )
            .service(
                web::resource("/{dashboard_id}")
                    .route(
                        web::get()
                            .to(coordinator_dashboards::get)
                            .authorize(Action::GetDashboard),
                    )
                    .route(
                        web::delete()
                            .to(coordinator_dashboards::delete)
                            .authorize(Action::DeleteDashboard),
                    )
                    .route(
                        web::put()
                            .to(coordinator_dashboards::update)
                            .authorize(Action::CreateDashboard),
                    ),
            )
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
                    .route(web::get().to(coordinator_logstream::list).authorize(Action::ListStream)),
            )
            .service(
                web::scope("/{logstream}")
                    .service(
                        web::resource("")
                            // PUT "/logstream/{logstream}" ==> Create log stream
                            .route(
                                web::put()
                                    .to(coordinator_logstream::put_stream)
                                    .authorize_for_stream(Action::CreateStream),
                            )
                            // // This method is not allowed for coordinator/querier
                            // // POST "/logstream/{logstream}" ==> Post logs to given log stream
                            // .route(
                            //     web::post()
                            //         .to(coordinator_ingest::post_event)
                            //         .authorize_for_stream(Action::Ingest),
                            // )
                            // DELETE "/logstream/{logstream}" ==> Delete log stream
                            .route(
                                web::delete()
                                    .to(coordinator_logstream::delete)
                                    .authorize_for_stream(Action::DeleteStream),
                            )
                            .app_data(web::PayloadConfig::default().limit(MAX_EVENT_PAYLOAD_SIZE)),
                    )
                    .service(
                        // GET "/logstream/{logstream}/info" ==> Get info for given log stream
                        web::resource("/info").route(
                            web::get()
                                .to(coordinator_logstream::get_stream_info)
                                .authorize_for_stream(Action::GetStreamInfo),
                        ),
                    )
                    .service(
                        web::resource("/alert")
                            // PUT "/logstream/{logstream}/alert" ==> Set alert for given log stream
                            .route(
                                web::put()
                                    .to(coordinator_logstream::put_alert)
                                    .authorize_for_stream(Action::PutAlert),
                            )
                            // GET "/logstream/{logstream}/alert" ==> Get alert for given log stream
                            .route(
                                web::get()
                                    .to(coordinator_logstream::get_alert)
                                    .authorize_for_stream(Action::GetAlert),
                            ),
                    )
                    .service(
                        // GET "/logstream/{logstream}/schema" ==> Get schema for given log stream
                        web::resource("/schema").route(
                            web::get()
                                .to(coordinator_logstream::schema)
                                .authorize_for_stream(Action::GetSchema),
                        ),
                    )
                    .service(
                        // GET "/logstream/{logstream}/stats" ==> Get stats for given log stream
                        web::resource("/stats").route(
                            web::get()
                                .to(coordinator_logstream::get_stats)
                                .authorize_for_stream(Action::GetStats),
                        ),
                    )
                    .service(
                        web::resource("/retention")
                            // PUT "/logstream/{logstream}/retention" ==> Set retention for given logstream
                            .route(
                                web::put()
                                    .to(coordinator_logstream::put_retention)
                                    .authorize_for_stream(Action::PutRetention),
                            )
                            // GET "/logstream/{logstream}/retention" ==> Get retention for given logstream
                            .route(
                                web::get()
                                    .to(coordinator_logstream::get_retention)
                                    .authorize_for_stream(Action::GetRetention),
                            ),
                    )
                    // .service(
                    //     web::resource("/cache")
                    //         // PUT "/logstream/{logstream}/cache" ==> Set retention for given logstream
                    //         .route(
                    //             web::put()
                    //                 .to(querier_logstream::put_enable_cache)
                    //                 .authorize_for_stream(Action::PutCacheEnabled),
                    //         )
                    //         // GET "/logstream/{logstream}/cache" ==> Get retention for given logstream
                    //         .route(
                    //             web::get()
                    //                 .to(querier_logstream::get_cache_enabled)
                    //                 .authorize_for_stream(Action::GetCacheEnabled),
                    //         ),
                    // )
                    // .service(
                    //     web::resource("/hottier")
                    //         // PUT "/logstream/{logstream}/hottier" ==> Set hottier for given logstream
                    //         .route(
                    //             web::put()
                    //                 .to(logstream::put_stream_hot_tier)
                    //                 .authorize_for_stream(Action::PutHotTierEnabled),
                    //         )
                    //         .route(
                    //             web::get()
                    //                 .to(logstream::get_stream_hot_tier)
                    //                 .authorize_for_stream(Action::GetHotTierEnabled),
                    //         )
                    //         .route(
                    //             web::delete()
                    //                 .to(logstream::delete_stream_hot_tier)
                    //                 .authorize_for_stream(Action::DeleteHotTierEnabled),
                    //         ),
                    // ),
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

        match QUERY_COORDINATION.lock().await.get_leader().await {
            Ok(_) => {},
            Err(e) => {
                log::warn!("Please start a query server as well")
            },
        };

        loop {
            tokio::select! {
                e = &mut app => {
                    // actix server finished .. stop other threads and stop the server
                    remote_sync_inbox.send(()).unwrap_or(());
                    localsync_inbox.send(()).unwrap_or(());
                    if let Err(e) = localsync_handler.await {
                        log::error!("Error joining localsync_handler: {:?}", e);
                    }
                    if let Err(e) = remote_sync_handler.await {
                        log::error!("Error joining remote_sync_handler: {:?}", e);
                    }
                    return e
                },
                _ = &mut localsync_outbox => {
                    // crash the server if localsync fails for any reason
                    // panic!("Local Sync thread died. Server will fail now!")
                    return Err(anyhow::Error::msg("Failed to sync local data to drive. Please restart the Parseable server.\n\nJoin us on Parseable Slack if the issue persists after restart : https://launchpass.com/parseable"))
                },
                _ = &mut remote_sync_outbox => {
                    // remote_sync failed, this is recoverable by just starting remote_sync thread again
                    if let Err(e) = remote_sync_handler.await {
                        log::error!("Error joining remote_sync_handler: {:?}", e);
                    }
                    (remote_sync_handler, remote_sync_outbox, remote_sync_inbox) = sync::object_store_sync().await;
                }

            };
        }
    }
}