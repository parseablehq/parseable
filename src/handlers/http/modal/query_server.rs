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

use std::sync::Arc;
use std::thread;

use crate::handlers::airplane;
use crate::handlers::http::cluster::{self, init_cluster_metrics_schedular};
use crate::handlers::http::middleware::{DisAllowRootUser, RouteExt};
use crate::handlers::http::{MAX_EVENT_PAYLOAD_SIZE, logstream};
use crate::handlers::http::{base_path, prism_base_path, resource_check};
use crate::handlers::http::{rbac, role};
use crate::hottier::HotTierManager;
use crate::rbac::role::Action;
use crate::sync::sync_start;
use crate::{analytics, migration, storage, sync};
use actix_web::middleware::from_fn;
use actix_web::web::{ServiceConfig, resource};
use actix_web::{Scope, web};
use actix_web_prometheus::PrometheusMetrics;
use async_trait::async_trait;
use bytes::Bytes;
use tokio::sync::{OnceCell, oneshot};
use tracing::info;

use crate::Server;
use crate::parseable::PARSEABLE;

use super::query::{querier_ingest, querier_logstream, querier_rbac, querier_role};
use super::{NodeType, OpenIdClient, ParseableServer, QuerierMetadata, load_on_init};

pub struct QueryServer;
pub static QUERIER_META: OnceCell<Arc<QuerierMetadata>> = OnceCell::const_new();
#[async_trait]
impl ParseableServer for QueryServer {
    // configure the api routes
    fn configure_routes(config: &mut ServiceConfig, oidc_client: Option<OpenIdClient>) {
        config
            .service(
                web::scope(&base_path())
                    .service(Server::get_correlation_webscope())
                    .service(Server::get_query_factory().wrap(from_fn(
                        resource_check::check_resource_utilization_middleware,
                    )))
                    .service(Server::get_liveness_factory())
                    .service(Server::get_readiness_factory())
                    .service(Server::get_about_factory())
                    .service(Self::get_logstream_webscope())
                    .service(Self::get_user_webscope())
                    .service(Server::get_users_webscope())
                    .service(Server::get_dashboards_webscope())
                    .service(Server::get_filters_webscope())
                    .service(Server::get_llm_webscope())
                    .service(Server::get_oauth_webscope(oidc_client))
                    .service(Self::get_user_role_webscope())
                    .service(Server::get_roles_webscope())
                    .service(Server::get_counts_webscope().wrap(from_fn(
                        resource_check::check_resource_utilization_middleware,
                    )))
                    .service(Server::get_metrics_webscope())
                    .service(Server::get_alerts_webscope())
                    .service(Server::get_targets_webscope())
                    .service(Self::get_cluster_web_scope())
                    .service(Server::get_demo_data_webscope()),
            )
            .service(
                web::scope(&prism_base_path())
                    .service(Server::get_prism_home())
                    .service(Server::get_prism_logstream())
                    .service(Server::get_prism_datasets()),
            )
            .service(Server::get_generated());
    }

    async fn load_metadata(&self) -> anyhow::Result<Option<Bytes>> {
        // parseable can't use local storage for persistence when running a distributed setup
        if PARSEABLE.storage.name() == "drive" {
            return Err(anyhow::anyhow!(
                "This instance of the Parseable server has been configured to run in a distributed setup, it doesn't support local storage.",
            ));
        }

        let mut parseable_json = PARSEABLE.validate_storage().await?;
        migration::run_metadata_migration(&PARSEABLE, &mut parseable_json).await?;
        Ok(parseable_json)
    }

    /// initialize the server, run migrations as needed and start an instance
    async fn init(
        &self,
        prometheus: &PrometheusMetrics,
        shutdown_rx: oneshot::Receiver<()>,
    ) -> anyhow::Result<()> {
        PARSEABLE.storage.register_store_metrics(prometheus);
        // write the ingestor metadata to storage
        QUERIER_META
            .get_or_init(|| async {
                QuerierMetadata::load_node_metadata(NodeType::Querier)
                    .await
                    .expect("Querier Metadata should be set in ingestor mode")
            })
            .await;
        migration::run_migration(&PARSEABLE).await?;

        //create internal stream at server start
        PARSEABLE.create_internal_stream_if_not_exists().await?;
        // load on init
        load_on_init().await?;
        // track all parquet files already in the data directory
        storage::retention::load_retention_from_global();

        // all internal data structures populated now.
        // start the analytics scheduler if enabled
        if PARSEABLE.options.send_analytics {
            analytics::init_analytics_scheduler()?;
        }

        if init_cluster_metrics_schedular().is_ok() {
            info!("Cluster metrics scheduler started successfully");
        }

        // local sync on init
        let startup_sync_handle = tokio::spawn(async {
            if let Err(e) = sync_start().await {
                tracing::warn!("local sync on server start failed: {e}");
            }
        });
        if let Some(hot_tier_manager) = HotTierManager::global() {
            hot_tier_manager.put_internal_stream_hot_tier().await?;
            hot_tier_manager.download_from_s3()?;
        };

        // Run sync on a background thread
        let (cancel_tx, cancel_rx) = oneshot::channel();
        thread::spawn(|| sync::handler(cancel_rx));

        tokio::spawn(airplane::server());

        let result = self
            .start(shutdown_rx, prometheus.clone(), PARSEABLE.options.openid())
            .await?;
        // Cancel sync jobs
        cancel_tx.send(()).expect("Cancellation should not fail");
        if let Err(join_err) = startup_sync_handle.await {
            tracing::warn!("startup sync task panicked: {join_err}");
        }
        Ok(result)
    }
}

impl QueryServer {
    // get the role webscope
    pub fn get_user_role_webscope() -> Scope {
        web::scope("/role")
            // GET Role List
            .service(resource("").route(web::get().to(role::list).authorize(Action::ListRole)))
            .service(
                // PUT and GET Default Role
                resource("/default")
                    .route(web::put().to(role::put_default).authorize(Action::PutRole))
                    .route(web::get().to(role::get_default).authorize(Action::GetRole)),
            )
            .service(
                // PUT, GET, DELETE Roles
                resource("/{name}")
                    .route(web::put().to(querier_role::put).authorize(Action::PutRole))
                    .route(web::delete().to(role::delete).authorize(Action::DeleteRole))
                    .route(web::get().to(role::get).authorize(Action::GetRole)),
            )
    }

    // get the user webscope
    pub fn get_user_webscope() -> Scope {
        web::scope("/user")
            .service(
                web::resource("")
                    // GET /user => List all users
                    .route(web::get().to(rbac::list_users).authorize(Action::ListUser)),
            )
            .service(
                web::resource("/{username}")
                    // PUT /user/{username} => Create a new user
                    .route(
                        web::post()
                            .to(querier_rbac::post_user)
                            .authorize(Action::PutUser),
                    )
                    // DELETE /user/{username} => Delete a user
                    .route(
                        web::delete()
                            .to(querier_rbac::delete_user)
                            .authorize(Action::DeleteUser),
                    )
                    .wrap(DisAllowRootUser),
            )
            .service(
                web::resource("/{username}/role").route(
                    web::get()
                        .to(rbac::get_role)
                        .authorize_for_user(Action::GetUserRoles),
                ),
            )
            .service(
                web::resource("/{username}/role/add")
                    // PATCH /user/{username}/role/add => Add roles to a user
                    .route(
                        web::patch()
                            .to(rbac::add_roles_to_user)
                            .authorize(Action::PutUserRoles)
                            .wrap(DisAllowRootUser),
                    ),
            )
            .service(
                web::resource("/{username}/role/remove")
                    // PATCH /user/{username}/role/remove => Remove roles from a user
                    .route(
                        web::patch()
                            .to(rbac::remove_roles_from_user)
                            .authorize(Action::PutUserRoles)
                            .wrap(DisAllowRootUser),
                    ),
            )
            .service(
                web::resource("/{username}/generate-new-password")
                    // POST /user/{username}/generate-new-password => reset password for this user
                    .route(
                        web::post()
                            .to(querier_rbac::post_gen_password)
                            .authorize(Action::PutUser)
                            .wrap(DisAllowRootUser),
                    ),
            )
    }

    // get the logstream web scope
    pub fn get_logstream_webscope() -> Scope {
        web::scope("/logstream")
            .service(
                // GET "/logstream" ==> Get list of all Log Streams on the server
                web::resource("")
                    .route(web::get().to(logstream::list).authorize(Action::ListStream)),
            )
            .service(
                web::scope("/schema/detect").service(
                    web::resource("")
                        // PUT "/logstream/{logstream}" ==> Create log stream
                        .route(
                            web::post()
                                .to(logstream::detect_schema)
                                .authorize(Action::DetectSchema),
                        ),
                ),
            )
            .service(
                web::scope("/{logstream}")
                    .service(
                        web::resource("")
                            // PUT "/logstream/{logstream}" ==> Create log stream
                            .route(
                                web::put()
                                    .to(querier_logstream::put_stream)
                                    .authorize_for_resource(Action::CreateStream),
                            )
                            // POST "/logstream/{logstream}" ==> Post logs to given log stream
                            .route(
                                web::post()
                                    .to(querier_ingest::post_event)
                                    .authorize_for_resource(Action::Ingest),
                            )
                            // DELETE "/logstream/{logstream}" ==> Delete log stream
                            .route(
                                web::delete()
                                    .to(querier_logstream::delete)
                                    .authorize_for_resource(Action::DeleteStream),
                            )
                            .app_data(web::JsonConfig::default().limit(MAX_EVENT_PAYLOAD_SIZE)),
                    )
                    .service(
                        // GET "/logstream/{logstream}/info" ==> Get info for given log stream
                        web::resource("/info").route(
                            web::get()
                                .to(logstream::get_stream_info)
                                .authorize_for_resource(Action::GetStreamInfo),
                        ),
                    )
                    .service(
                        // GET "/logstream/{logstream}/schema" ==> Get schema for given log stream
                        web::resource("/schema").route(
                            web::get()
                                .to(logstream::get_schema)
                                .authorize_for_resource(Action::GetSchema),
                        ),
                    )
                    .service(
                        // GET "/logstream/{logstream}/stats" ==> Get stats for given log stream
                        web::resource("/stats").route(
                            web::get()
                                .to(querier_logstream::get_stats)
                                .authorize_for_resource(Action::GetStats),
                        ),
                    )
                    .service(
                        web::resource("/retention")
                            // PUT "/logstream/{logstream}/retention" ==> Set retention for given logstream
                            .route(
                                web::put()
                                    .to(logstream::put_retention)
                                    .authorize_for_resource(Action::PutRetention),
                            )
                            // GET "/logstream/{logstream}/retention" ==> Get retention for given logstream
                            .route(
                                web::get()
                                    .to(logstream::get_retention)
                                    .authorize_for_resource(Action::GetRetention),
                            ),
                    )
                    .service(
                        web::resource("/hottier")
                            // PUT "/logstream/{logstream}/hottier" ==> Set hottier for given logstream
                            .route(
                                web::put()
                                    .to(logstream::put_stream_hot_tier)
                                    .authorize_for_resource(Action::PutHotTierEnabled),
                            )
                            .route(
                                web::get()
                                    .to(logstream::get_stream_hot_tier)
                                    .authorize_for_resource(Action::GetHotTierEnabled),
                            )
                            .route(
                                web::delete()
                                    .to(logstream::delete_stream_hot_tier)
                                    .authorize_for_resource(Action::DeleteHotTierEnabled),
                            ),
                    ),
            )
    }

    pub fn get_cluster_web_scope() -> actix_web::Scope {
        web::scope("/cluster")
            .service(
                // GET "/cluster/info" ==> Get info of the cluster
                web::resource("/info").route(
                    web::get()
                        .to(cluster::get_cluster_info)
                        .authorize(Action::ListCluster),
                ),
            )
            // GET "/cluster/metrics" ==> Get metrics of the cluster
            .service(
                web::resource("/metrics").route(
                    web::get()
                        .to(cluster::get_cluster_metrics)
                        .authorize(Action::ListClusterMetrics),
                ),
            )
            // DELETE "/cluster/{node_domain:port}" ==> Delete a node from the cluster
            .service(
                web::scope("/{node_url}").service(
                    web::resource("").route(
                        web::delete()
                            .to(cluster::remove_node)
                            .authorize(Action::DeleteNode),
                    ),
                ),
            )
    }
}
