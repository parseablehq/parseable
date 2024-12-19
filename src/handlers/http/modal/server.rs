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
use crate::handlers;
use crate::handlers::http::about;
use crate::handlers::http::base_path;
use crate::handlers::http::caching_removed;
use crate::handlers::http::health_check;
use crate::handlers::http::query;
use crate::handlers::http::trino;
use crate::handlers::http::users::dashboards;
use crate::handlers::http::users::filters;
use crate::hottier::HotTierManager;
use crate::metrics;
use crate::migration;
use crate::storage;
use crate::sync;
use crate::users::dashboards::DASHBOARDS;
use crate::users::filters::FILTERS;

use actix_web::web;
use actix_web::web::resource;
use actix_web::Resource;
use actix_web::Scope;
use actix_web_static_files::ResourceFiles;
use async_trait::async_trait;
use bytes::Bytes;
use tracing::error;

use crate::{
    handlers::http::{
        self, ingest, llm, logstream,
        middleware::{DisAllowRootUser, RouteExt},
        oidc, role, MAX_EVENT_PAYLOAD_SIZE,
    },
    option::CONFIG,
    rbac::role::Action,
};

// use super::generate;
use super::generate;
use super::OpenIdClient;
use super::ParseableServer;

pub struct Server;

#[async_trait]
impl ParseableServer for Server {
    fn configure_routes(config: &mut web::ServiceConfig, oidc_client: Option<OpenIdClient>) {
        // there might be a bug in the configure routes method
        config
            .service(
                web::scope(&base_path())
                    .service(Self::get_query_factory())
                    .service(Self::get_trino_factory())
                    .service(Self::get_cache_webscope())
                    .service(Self::get_ingest_factory())
                    .service(Self::get_liveness_factory())
                    .service(Self::get_readiness_factory())
                    .service(Self::get_about_factory())
                    .service(Self::get_logstream_webscope())
                    .service(Self::get_user_webscope())
                    .service(Self::get_dashboards_webscope())
                    .service(Self::get_filters_webscope())
                    .service(Self::get_llm_webscope())
                    .service(Self::get_oauth_webscope(oidc_client))
                    .service(Self::get_user_role_webscope())
                    .service(Self::get_metrics_webscope()),
            )
            .service(Self::get_ingest_otel_factory())
            .service(Self::get_generated());
    }

    async fn load_metadata(&self) -> anyhow::Result<Option<Bytes>> {
        migration::run_file_migration(&CONFIG).await?;
        let parseable_json = CONFIG.validate_storage().await?;
        migration::run_metadata_migration(&CONFIG, &parseable_json).await?;

        Ok(parseable_json)
    }

    // configure the server and start an instance of the single server setup
    async fn init(&self) -> anyhow::Result<()> {
        let prometheus = metrics::build_metrics_handler();
        CONFIG.storage().register_store_metrics(&prometheus);

        migration::run_migration(&CONFIG).await?;

        FILTERS.load().await?;
        DASHBOARDS.load().await?;

        storage::retention::load_retention_from_global();

        if let Some(hot_tier_manager) = HotTierManager::global() {
            hot_tier_manager.download_from_s3()?;
        };

        let (localsync_handler, mut localsync_outbox, localsync_inbox) =
            sync::run_local_sync().await;
        let (mut remote_sync_handler, mut remote_sync_outbox, mut remote_sync_inbox) =
            sync::object_store_sync().await;

        if CONFIG.parseable.send_analytics {
            analytics::init_analytics_scheduler()?;
        }

        tokio::spawn(handlers::livetail::server());
        tokio::spawn(handlers::airplane::server());

        let app = self.start(prometheus, CONFIG.parseable.openid.clone());

        tokio::pin!(app);

        loop {
            tokio::select! {
                e = &mut app => {
                    // actix server finished .. stop other threads and stop the server
                    remote_sync_inbox.send(()).unwrap_or(());
                    localsync_inbox.send(()).unwrap_or(());
                    if let Err(e) = localsync_handler.await {
                        error!("Error joining remote_sync_handler: {:?}", e);
                    }
                    if let Err(e) = remote_sync_handler.await {
                        error!("Error joining remote_sync_handler: {:?}", e);
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
                        error!("Error joining remote_sync_handler: {:?}", e);
                    }
                    (remote_sync_handler, remote_sync_outbox, remote_sync_inbox) = sync::object_store_sync().await;
                }

            };
        }
    }
}

impl Server {
    // get the trino factory
    pub fn get_trino_factory() -> Resource {
        web::resource("/trinoquery")
            .route(web::post().to(trino::trino_query).authorize(Action::Query))
    }

    pub fn get_metrics_webscope() -> Scope {
        web::scope("/metrics").service(
            web::resource("").route(web::get().to(metrics::get).authorize(Action::Metrics)),
        )
    }

    // get the dashboards web scope
    pub fn get_dashboards_webscope() -> Scope {
        web::scope("/dashboards")
            .service(
                web::resource("")
                    .route(
                        web::post()
                            .to(dashboards::post)
                            .authorize(Action::CreateDashboard),
                    )
                    .route(
                        web::get()
                            .to(dashboards::list)
                            .authorize(Action::ListDashboard),
                    ),
            )
            .service(
                web::resource("/{dashboard_id}")
                    .route(
                        web::get()
                            .to(dashboards::get)
                            .authorize(Action::GetDashboard),
                    )
                    .route(
                        web::delete()
                            .to(dashboards::delete)
                            .authorize(Action::DeleteDashboard),
                    )
                    .route(
                        web::put()
                            .to(dashboards::update)
                            .authorize(Action::CreateDashboard),
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

    // get the query factory
    // POST "/query" ==> Get results of the SQL query passed in request body
    pub fn get_query_factory() -> Resource {
        web::resource("/query").route(web::post().to(query::query).authorize(Action::Query))
    }

    pub fn get_cache_webscope() -> Scope {
        web::scope("/cache").service(
            web::scope("/{user_id}").service(
                web::scope("/{stream}").service(
                    web::resource("")
                        .route(web::get().to(caching_removed))
                        .route(web::post().to(caching_removed)),
                ),
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
                                    .to(logstream::put_stream)
                                    .authorize_for_stream(Action::CreateStream),
                            )
                            // POST "/logstream/{logstream}" ==> Post logs to given log stream
                            .route(
                                web::post()
                                    .to(ingest::post_event)
                                    .authorize_for_stream(Action::Ingest),
                            )
                            // DELETE "/logstream/{logstream}" ==> Delete log stream
                            .route(
                                web::delete()
                                    .to(logstream::delete)
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
                                .to(logstream::get_stats)
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
                            .route(web::put().to(caching_removed))
                            // GET "/logstream/{logstream}/cache" ==> Get retention for given logstream
                            .route(web::get().to(caching_removed)),
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

    // get the factory for the ingest route
    pub fn get_ingest_factory() -> Resource {
        web::resource("/ingest")
            .route(
                web::post()
                    .to(ingest::ingest)
                    .authorize_for_stream(Action::Ingest),
            )
            .app_data(web::PayloadConfig::default().limit(MAX_EVENT_PAYLOAD_SIZE))
    }

    // /v1/logs endpoint to be used for OTEL log ingestion only
    pub fn get_ingest_otel_factory() -> Scope {
        web::scope("/v1")
            .service(
                web::resource("/logs")
                    .route(
                        web::post()
                            .to(ingest::handle_otel_logs_ingestion)
                            .authorize_for_stream(Action::Ingest),
                    )
                    .app_data(web::PayloadConfig::default().limit(MAX_EVENT_PAYLOAD_SIZE)),
            )
            .service(
                web::resource("/metrics")
                    .route(
                        web::post()
                            .to(ingest::handle_otel_metrics_ingestion)
                            .authorize_for_stream(Action::Ingest),
                    )
                    .app_data(web::PayloadConfig::default().limit(MAX_EVENT_PAYLOAD_SIZE)),
            )
            .service(
                web::resource("/traces")
                    .route(
                        web::post()
                            .to(ingest::handle_otel_traces_ingestion)
                            .authorize_for_stream(Action::Ingest),
                    )
                    .app_data(web::PayloadConfig::default().limit(MAX_EVENT_PAYLOAD_SIZE)),
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

    // get the user webscope
    fn get_user_webscope() -> Scope {
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
                web::resource("/{username}/role")
                    // PUT /user/{username}/roles => Put roles for user
                    .route(
                        web::put()
                            .to(http::rbac::put_role)
                            .authorize(Action::PutUserRoles)
                            .wrap(DisAllowRootUser),
                    )
                    .route(
                        web::get()
                            .to(http::rbac::get_role)
                            .authorize_for_user(Action::GetUserRoles),
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
