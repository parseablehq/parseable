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

use std::thread;

use crate::analytics;
use crate::handlers;
use crate::handlers::http::about;
use crate::handlers::http::alerts;
use crate::handlers::http::base_path;
use crate::handlers::http::demo_data::get_demo_data;
use crate::handlers::http::health_check;
use crate::handlers::http::prism_base_path;
use crate::handlers::http::query;
use crate::handlers::http::resource_check;
use crate::handlers::http::targets;
use crate::handlers::http::users::dashboards;
use crate::handlers::http::users::filters;
use crate::hottier::HotTierManager;
use crate::metrics;
use crate::migration;
use crate::storage;
use crate::sync;
use crate::sync::sync_start;

use actix_web::Resource;
use actix_web::Scope;
use actix_web::middleware::from_fn;
use actix_web::web;
use actix_web::web::resource;
use actix_web_prometheus::PrometheusMetrics;
use actix_web_static_files::ResourceFiles;
use async_trait::async_trait;
use bytes::Bytes;
use tokio::sync::oneshot;

use crate::{
    handlers::http::{
        self, MAX_EVENT_PAYLOAD_SIZE, ingest, llm, logstream,
        middleware::{DisAllowRootUser, RouteExt},
        oidc, role,
    },
    parseable::PARSEABLE,
    rbac::role::Action,
};

// use super::generate;
use super::OpenIdClient;
use super::ParseableServer;
use super::generate;
use super::load_on_init;

pub struct Server;

#[async_trait]
impl ParseableServer for Server {
    fn configure_routes(config: &mut web::ServiceConfig, oidc_client: Option<OpenIdClient>) {
        // there might be a bug in the configure routes method
        config
            .service(
                web::scope(&base_path())
                    .service(Self::get_correlation_webscope())
                    .service(Self::get_query_factory().wrap(from_fn(
                        resource_check::check_resource_utilization_middleware,
                    )))
                    .service(Self::get_ingest_factory().wrap(from_fn(
                        resource_check::check_resource_utilization_middleware,
                    )))
                    .service(Self::get_liveness_factory())
                    .service(Self::get_readiness_factory())
                    .service(Self::get_about_factory())
                    .service(Self::get_logstream_webscope())
                    .service(Self::get_user_webscope())
                    .service(Self::get_users_webscope())
                    .service(Self::get_dashboards_webscope())
                    .service(Self::get_filters_webscope())
                    .service(Self::get_llm_webscope())
                    .service(Self::get_oauth_webscope(oidc_client))
                    .service(Self::get_user_role_webscope())
                    .service(Self::get_roles_webscope())
                    .service(Self::get_counts_webscope().wrap(from_fn(
                        resource_check::check_resource_utilization_middleware,
                    )))
                    .service(Self::get_alerts_webscope())
                    .service(Self::get_targets_webscope())
                    .service(Self::get_metrics_webscope())
                    .service(Self::get_demo_data_webscope()),
            )
            .service(
                web::scope(&prism_base_path())
                    .service(Server::get_prism_home())
                    .service(Server::get_prism_logstream())
                    .service(Server::get_prism_datasets()),
            )
            .service(Self::get_ingest_otel_factory().wrap(from_fn(
                resource_check::check_resource_utilization_middleware,
            )))
            .service(Self::get_generated());
    }

    async fn load_metadata(&self) -> anyhow::Result<Option<Bytes>> {
        //TODO: removed file migration
        //deprecated support for deployments < v1.0.0
        let mut parseable_json = PARSEABLE.validate_storage().await?;
        migration::run_metadata_migration(&PARSEABLE, &mut parseable_json).await?;

        Ok(parseable_json)
    }

    // configure the server and start an instance of the single server setup
    async fn init(
        &self,
        prometheus: &PrometheusMetrics,
        shutdown_rx: oneshot::Receiver<()>,
    ) -> anyhow::Result<()> {
        PARSEABLE.storage.register_store_metrics(prometheus);

        migration::run_migration(&PARSEABLE).await?;

        // load on init
        load_on_init().await?;

        storage::retention::load_retention_from_global();

        // local sync on init
        let startup_sync_handle = tokio::spawn(async {
            if let Err(e) = sync_start().await {
                tracing::warn!("local sync on server start failed: {e}");
            }
        });

        if let Some(hot_tier_manager) = HotTierManager::global() {
            hot_tier_manager.download_from_s3()?;
        };

        // Run sync on a background thread
        let (cancel_tx, cancel_rx) = oneshot::channel();
        thread::spawn(|| sync::handler(cancel_rx));

        if PARSEABLE.options.send_analytics {
            analytics::init_analytics_scheduler()?;
        }

        tokio::spawn(handlers::livetail::server());
        tokio::spawn(handlers::airplane::server());

        let result = self
            .start(shutdown_rx, prometheus.clone(), PARSEABLE.options.openid())
            .await;
        // Cancel sync jobs
        cancel_tx.send(()).expect("Cancellation should not fail");
        if let Err(join_err) = startup_sync_handle.await {
            tracing::warn!("startup sync task panicked: {join_err}");
        }
        return result;
    }
}

impl Server {
    pub fn get_prism_home() -> Scope {
        web::scope("/home")
            .service(web::resource("").route(web::get().to(http::prism_home::home_api)))
            .service(web::resource("/search").route(web::get().to(http::prism_home::home_search)))
    }

    pub fn get_prism_logstream() -> Scope {
        web::scope("/logstream").service(
            web::scope("/{logstream}").service(
                web::resource("/info").route(
                    web::get()
                        .to(http::prism_logstream::get_info)
                        .authorize_for_resource(Action::GetStreamInfo)
                        .authorize_for_resource(Action::GetStats)
                        .authorize_for_resource(Action::GetRetention),
                ),
            ),
        )
    }

    pub fn get_prism_datasets() -> Scope {
        web::scope("/datasets").route(
            "",
            web::post()
                .to(http::prism_logstream::post_datasets)
                .authorize_for_resource(Action::GetStreamInfo)
                .authorize_for_resource(Action::GetStats)
                .authorize_for_resource(Action::GetRetention),
        )
    }

    pub fn get_demo_data_webscope() -> Scope {
        web::scope("/demodata").service(web::resource("").route(web::get().to(get_demo_data)))
    }

    pub fn get_metrics_webscope() -> Scope {
        web::scope("/metrics").service(
            web::resource("").route(web::get().to(metrics::get).authorize(Action::Metrics)),
        )
    }

    pub fn get_correlation_webscope() -> Scope {
        web::scope("/correlation")
            .service(
                web::resource("")
                    .route(
                        web::get()
                            .to(http::correlation::list)
                            .authorize(Action::GetCorrelation),
                    )
                    .route(
                        web::post()
                            .to(http::correlation::post)
                            .authorize(Action::CreateCorrelation),
                    ),
            )
            .service(
                web::resource("/{correlation_id}")
                    .route(
                        web::get()
                            .to(http::correlation::get)
                            .authorize(Action::GetCorrelation),
                    )
                    .route(
                        web::put()
                            .to(http::correlation::modify)
                            .authorize(Action::PutCorrelation),
                    )
                    .route(
                        web::delete()
                            .to(http::correlation::delete)
                            .authorize(Action::DeleteCorrelation),
                    ),
            )
    }

    pub fn get_alerts_webscope() -> Scope {
        web::scope("/alerts")
            .service(
                web::resource("")
                    .route(web::get().to(alerts::list).authorize(Action::GetAlert))
                    .route(web::post().to(alerts::post).authorize(Action::PutAlert)),
            )
            .service(
                web::resource("/{alert_id}")
                    .route(web::get().to(alerts::get).authorize(Action::GetAlert))
                    .route(
                        web::put()
                            .to(alerts::update_state)
                            .authorize(Action::PutAlert),
                    )
                    .route(
                        web::delete()
                            .to(alerts::delete)
                            .authorize(Action::DeleteAlert),
                    ),
            )
    }

    pub fn get_targets_webscope() -> Scope {
        web::scope("/targets")
            .service(
                web::resource("")
                    .route(web::get().to(targets::list).authorize(Action::GetAlert))
                    .route(web::post().to(targets::post).authorize(Action::PutAlert)),
            )
            .service(
                web::resource("/{target_id}")
                    .route(web::get().to(targets::get).authorize(Action::GetAlert))
                    .route(web::put().to(targets::update).authorize(Action::PutAlert))
                    .route(
                        web::delete()
                            .to(targets::delete)
                            .authorize(Action::DeleteAlert),
                    ),
            )
    }

    // get the dashboards web scope
    pub fn get_dashboards_webscope() -> Scope {
        web::scope("/dashboards")
            .service(
                web::resource("")
                    .route(
                        web::post()
                            .to(dashboards::create_dashboard)
                            .authorize(Action::CreateDashboard),
                    )
                    .route(
                        web::get()
                            .to(dashboards::list_dashboards)
                            .authorize(Action::ListDashboard),
                    ),
            )
            .service(
                web::resource("/list_tags").route(
                    web::get()
                        .to(dashboards::list_tags)
                        .authorize(Action::ListDashboard),
                ),
            )
            .service(
                web::scope("/{dashboard_id}")
                    .service(
                        web::resource("")
                            .route(
                                web::get()
                                    .to(dashboards::get_dashboard)
                                    .authorize(Action::GetDashboard),
                            )
                            .route(
                                web::delete()
                                    .to(dashboards::delete_dashboard)
                                    .authorize(Action::DeleteDashboard),
                            )
                            .route(
                                web::put()
                                    .to(dashboards::update_dashboard)
                                    .authorize(Action::CreateDashboard),
                            ),
                    )
                    .service(
                        web::resource("/add_tile").route(
                            web::put()
                                .to(dashboards::add_tile)
                                .authorize(Action::CreateDashboard),
                        ),
                    ),
            )
    }

    // get the filters web scope
    pub fn get_filters_webscope() -> Scope {
        web::scope("/filters")
            .service(
                web::resource("")
                    .route(
                        web::post()
                            .to(filters::post)
                            .authorize(Action::CreateFilter),
                    )
                    .route(web::get().to(filters::list).authorize(Action::ListFilter)),
            )
            .service(
                web::resource("/{filter_id}")
                    .route(web::get().to(filters::get).authorize(Action::GetFilter))
                    .route(
                        web::delete()
                            .to(filters::delete)
                            .authorize(Action::DeleteFilter),
                    )
                    .route(
                        web::put()
                            .to(filters::update)
                            .authorize(Action::CreateFilter),
                    ),
            )
    }
    pub fn get_counts_webscope() -> Resource {
        web::resource("/counts").route(web::post().to(query::get_counts).authorize(Action::Query))
    }

    // get the query factory
    // POST "/query" ==> Get results of the SQL query passed in request body
    pub fn get_query_factory() -> Resource {
        web::resource("/query").route(web::post().to(query::query).authorize(Action::Query))
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
                                    .to(logstream::put_stream)
                                    .authorize_for_resource(Action::CreateStream),
                            )
                            // POST "/logstream/{logstream}" ==> Post logs to given log stream
                            .route(
                                web::post()
                                    .to(ingest::post_event)
                                    .authorize_for_resource(Action::Ingest)
                                    .wrap(from_fn(
                                        resource_check::check_resource_utilization_middleware,
                                    )),
                            )
                            // DELETE "/logstream/{logstream}" ==> Delete log stream
                            .route(
                                web::delete()
                                    .to(logstream::delete)
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
                                .to(logstream::get_stats)
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

    // get the factory for the ingest route
    pub fn get_ingest_factory() -> Resource {
        web::resource("/ingest")
            .route(
                web::post()
                    .to(ingest::ingest)
                    .authorize_for_resource(Action::Ingest),
            )
            .app_data(web::JsonConfig::default().limit(MAX_EVENT_PAYLOAD_SIZE))
    }

    // /v1/logs endpoint to be used for OTEL log ingestion only
    pub fn get_ingest_otel_factory() -> Scope {
        web::scope("/v1")
            .service(
                web::resource("/logs")
                    .route(
                        web::post()
                            .to(ingest::handle_otel_logs_ingestion)
                            .authorize_for_resource(Action::Ingest),
                    )
                    .app_data(web::JsonConfig::default().limit(MAX_EVENT_PAYLOAD_SIZE)),
            )
            .service(
                web::resource("/metrics")
                    .route(
                        web::post()
                            .to(ingest::handle_otel_metrics_ingestion)
                            .authorize_for_resource(Action::Ingest),
                    )
                    .app_data(web::JsonConfig::default().limit(MAX_EVENT_PAYLOAD_SIZE)),
            )
            .service(
                web::resource("/traces")
                    .route(
                        web::post()
                            .to(ingest::handle_otel_traces_ingestion)
                            .authorize_for_resource(Action::Ingest),
                    )
                    .app_data(web::JsonConfig::default().limit(MAX_EVENT_PAYLOAD_SIZE)),
            )
    }

    // get the oauth webscope
    pub fn get_oauth_webscope(oidc_client: Option<OpenIdClient>) -> Scope {
        let oauth = web::scope("/o")
            .service(resource("/login").route(web::get().to(oidc::login)))
            .service(resource("/logout").route(web::get().to(oidc::logout)))
            .service(resource("/code").route(web::get().to(oidc::reply_login)));

        if let Some(client) = oidc_client {
            oauth.app_data(web::Data::from(client))
        } else {
            oauth
        }
    }

    // get list of roles
    pub fn get_roles_webscope() -> Scope {
        web::scope("/roles").service(
            web::resource("").route(web::get().to(role::list_roles).authorize(Action::ListRole)),
        )
    }

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
                    .route(web::put().to(role::put).authorize(Action::PutRole))
                    .route(web::delete().to(role::delete).authorize(Action::DeleteRole))
                    .route(web::get().to(role::get).authorize(Action::GetRole)),
            )
    }

    // get the users webscope (for Prism only)
    pub fn get_users_webscope() -> Scope {
        web::scope("/users")
            .service(
                web::resource("")
                    // GET /users => List all users
                    .route(
                        web::get()
                            .to(http::rbac::list_users_prism)
                            .authorize(Action::ListUser),
                    ),
            )
            .service(
                web::resource("/{username}").route(
                    web::get()
                        .to(http::rbac::get_prism_user)
                        .authorize_for_user(Action::GetUserRoles),
                ),
            )
    }

    // get the user webscope
    pub fn get_user_webscope() -> Scope {
        web::scope("/user")
            .service(
                web::resource("")
                    // GET /user => List all users
                    .route(
                        web::get()
                            .to(http::rbac::list_users)
                            .authorize(Action::ListUser),
                    ),
            )
            .service(
                web::resource("/{username}")
                    // PUT /user/{username} => Create a new user
                    .route(
                        web::post()
                            .to(http::rbac::post_user)
                            .authorize(Action::PutUser),
                    )
                    // DELETE /user/{username} => Delete a user
                    .route(
                        web::delete()
                            .to(http::rbac::delete_user)
                            .authorize(Action::DeleteUser),
                    )
                    .wrap(DisAllowRootUser),
            )
            .service(
                web::resource("/{username}/role").route(
                    web::get()
                        .to(http::rbac::get_role)
                        .authorize_for_user(Action::GetUserRoles),
                ),
            )
            .service(
                web::resource("/{username}/role/add")
                    // PATCH /user/{username}/role/add => Add roles to a user
                    .route(
                        web::patch()
                            .to(http::rbac::add_roles_to_user)
                            .authorize(Action::PutUserRoles)
                            .wrap(DisAllowRootUser),
                    ),
            )
            .service(
                web::resource("/{username}/role/remove")
                    // PATCH /user/{username}/role/remove => Remove roles from a user
                    .route(
                        web::patch()
                            .to(http::rbac::remove_roles_from_user)
                            .authorize(Action::PutUserRoles)
                            .wrap(DisAllowRootUser),
                    ),
            )
            .service(
                web::resource("/{username}/generate-new-password")
                    // POST /user/{username}/generate-new-password => reset password for this user
                    .route(
                        web::post()
                            .to(http::rbac::post_gen_password)
                            .authorize(Action::PutUser)
                            .wrap(DisAllowRootUser),
                    ),
            )
    }

    // get the llm webscope
    pub fn get_llm_webscope() -> Scope {
        web::scope("/llm").service(
            web::resource("").route(
                web::post()
                    .to(llm::make_llm_request)
                    .authorize(Action::QueryLLM),
            ),
        )
    }

    // get the live check
    // GET "/liveness" ==> Liveness check as per https://kubernetes.io/docs/tasks/configure-pod-container/configure-liveness-readiness-startup-probes/#define-a-liveness-command
    // HEAD "/liveness"
    pub fn get_liveness_factory() -> Resource {
        web::resource("/liveness")
            .route(web::get().to(health_check::liveness))
            .route(web::head().to(health_check::liveness))
    }

    // get the readiness check
    // GET "/readiness" ==> Readiness check as per https://kubernetes.io/docs/tasks/configure-pod-container/configure-liveness-readiness-startup-probes/#define-readiness-probes
    // HEAD "/readiness"
    pub fn get_readiness_factory() -> Resource {
        web::resource("/readiness")
            .route(web::get().to(health_check::readiness))
            .route(web::head().to(health_check::readiness))
    }

    // get the about factory
    pub fn get_about_factory() -> Resource {
        web::resource("/about").route(web::get().to(about::about).authorize(Action::GetAbout))
    }

    // GET "/" ==> Serve the static frontend directory
    pub fn get_generated() -> ResourceFiles {
        ResourceFiles::new("/", generate()).resolve_not_found_to_root()
    }
}
