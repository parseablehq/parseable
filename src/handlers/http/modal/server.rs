/*
 * Parseable Server (C) 2022 - 2025 Parseable, Inc.
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

use std::sync::Once;
use std::thread;

use crate::analytics;
use crate::handlers;
use crate::handlers::http::about;
use crate::handlers::http::alerts;
use crate::handlers::http::apikeys;
use crate::handlers::http::base_path;
use crate::handlers::http::demo_data::get_demo_data;
use crate::handlers::http::health_check;
use crate::handlers::http::max_event_payload_size;
use crate::handlers::http::middleware::IntraClusterRequest;
use crate::handlers::http::otel_generator::{
    get_otel_generator_status, start_otel_generator, stop_otel_generator,
};
use crate::handlers::http::prism_base_path;
use crate::handlers::http::query;
use crate::handlers::http::query_context;
use crate::handlers::http::resource_check;
use crate::handlers::http::targets;
use crate::handlers::http::users::dashboards;
use crate::handlers::http::users::filters;
use crate::hottier::GLOBAL_HOTTIER;
use crate::hottier::HotTierManager;
use crate::hottier::HotTierMessage;
use crate::hottier::hottier_runtime;
use crate::metrics;
use crate::migration;
use crate::storage;
use crate::storage::field_stats::get_dataset_stats;
use crate::sync;

use crate::handlers::http::alert_target_policy;
use actix_web::HttpRequest;
use actix_web::HttpResponse;
use actix_web::Resource;
use actix_web::Scope;
use actix_web::body::{EitherBody, MessageBody};
use actix_web::dev::{ServiceFactory, ServiceRequest, ServiceResponse};
use actix_web::http::header::{CACHE_CONTROL, CONTENT_TYPE, HeaderMap, HeaderValue, LOCATION};
use actix_web::http::{Method, StatusCode};
use actix_web::middleware::{Next, from_fn};
use actix_web::web;
use actix_web::web::resource;
use actix_web_prometheus::PrometheusMetrics;
use actix_web_static_files::ResourceFiles;
use async_trait::async_trait;
use bytes::Bytes;
use std::rc::Rc;
use tokio::sync::mpsc;
use tokio::sync::oneshot;

use crate::{
    handlers::http::{
        self, ingest, logstream,
        middleware::{DisAllowRootUser, RouteExt},
        oidc, role, traces,
    },
    parseable::PARSEABLE,
    rbac::role::Action,
};

use super::ParseableServer;
use super::generate;
use super::load_on_init;

pub struct Server;

#[async_trait]
impl ParseableServer for Server {
    fn configure_routes(config: &mut web::ServiceConfig) {
        // there might be a bug in the configure routes method
        config
            .service(crate::handlers::http::promql::scope())
            .service(
                web::scope(&base_path())
                    .service(Self::get_query_factory())
                    .service(Self::get_query_context_factory())
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
                    .service(Self::get_oauth_webscope())
                    .service(Self::get_user_role_webscope())
                    .service(Self::get_roles_webscope())
                    .service(Self::get_counts_webscope())
                    .service(Self::get_alerts_webscope())
                    .service(Self::get_targets_webscope())
                    .service(Self::get_alert_target_policy())
                    .service(Self::get_metrics_webscope())
                    .service(Self::get_demo_data_webscope()),
            )
            .service(
                web::scope(&prism_base_path())
                    .service(Server::get_prism_home())
                    .service(Server::get_llm_webscope())
                    .service(Server::get_prism_logstream())
                    .service(Server::get_prism_datasets())
                    .service(Server::get_apikeys_webscope())
                    .service(Self::get_otel_generator_webscope())
                    .service(Self::get_traces_webscope())
                    .service(Self::get_dataset_stats_webscope()),
            )
            .service(Self::get_ingest_otel_factory().wrap(from_fn(
                resource_check::check_resource_utilization_middleware,
            )))
            .configure(Self::configure_ui);
    }

    async fn load_metadata(&self) -> anyhow::Result<Option<Bytes>> {
        //TODO: removed file migration
        //deprecated support for deployments < v1.0.0
        let mut parseable_json = PARSEABLE.validate_storage().await?;
        migration::run_metadata_migration(&PARSEABLE, &mut parseable_json, &None).await?;

        Ok(parseable_json)
    }

    // configure the server and start an instance of the single server setup
    async fn init(
        &self,
        prometheus: &PrometheusMetrics,
        shutdown_rx: oneshot::Receiver<()>,
    ) -> anyhow::Result<()> {
        migration::run_migration(&PARSEABLE).await?;

        // load on init
        load_on_init().await?;

        storage::retention::load_retention_from_global();

        // Reserve the startup backlog before periodic sync can claim files.
        let (startup_snapshot_tx, startup_snapshot_rx) = std::sync::mpsc::channel();
        thread::spawn(move || sync::sync_start_and_signal(startup_snapshot_tx));

        if let Some(htm) = PARSEABLE
            .options
            .hot_tier_storage_path
            .as_ref()
            .map(|hot_tier_path| {
                // start hottier runtime
                let (sender, receiver): (
                    mpsc::UnboundedSender<HotTierMessage>,
                    mpsc::UnboundedReceiver<HotTierMessage>,
                ) = mpsc::unbounded_channel();
                std::thread::spawn(|| hottier_runtime(receiver));

                // set global hottier

                GLOBAL_HOTTIER.get_or_init(|| HotTierManager::new(hot_tier_path, sender))
            })
        {
            htm.start_all_tasks().await;
        }

        // Conversion may continue in startup sync; periodic sync only waits
        // for the short processing-file snapshot.
        let (cancel_tx, cancel_rx) = oneshot::channel();
        thread::spawn(move || {
            if startup_snapshot_rx.recv().is_err() {
                tracing::warn!(
                    "Startup sync exited before signaling snapshot completion; starting periodic sync"
                );
            }
            sync::handler(cancel_rx)
        });

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
        return result;
    }
}

impl Server {
    pub fn get_llm_webscope() -> Scope {
        web::scope("/llm")
            .app_data(web::JsonConfig::default().limit(2 * 1024 * 1024))
            .app_data(web::Data::new(
                crate::users::dashboard_tools::DashboardToolPolicy {
                    writes_enabled: PARSEABLE.options.dashboard_agent_writes,
                },
            ))
            .service(
                web::resource("/tools/list").route(
                    web::get()
                        .to(crate::tool_catalog::list_tool_registry)
                        .authorize(Action::Query),
                ),
            )
            .service(
                web::resource("/tools/call").route(
                    web::post()
                        .to(crate::tool_executor::call_tool_registry)
                        .authenticate(),
                ),
            )
    }

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

    pub fn get_apikeys_webscope() -> Scope {
        web::scope("/apikeys")
            .service(
                resource("")
                    .route(
                        web::post()
                            .to(apikeys::create_api_key)
                            .authorize(Action::All),
                    )
                    .route(web::get().to(apikeys::list_api_keys).authorize(Action::All)),
            )
            .service(
                resource("/validate").route(
                    web::post()
                        .to(apikeys::validate_api_key)
                        .authorize(Action::All),
                ),
            )
            .service(
                resource("/{key_id}")
                    .route(web::get().to(apikeys::get_api_key).authorize(Action::All))
                    .route(
                        web::delete()
                            .to(apikeys::delete_api_key)
                            .authorize(Action::All),
                    ),
            )
    }

    pub fn get_demo_data_webscope() -> Scope {
        web::scope("/demodata")
            .service(web::resource("").route(web::get().to(get_demo_data).authenticate()))
    }

    pub fn get_otel_generator_webscope() -> Scope {
        web::scope("/otel_generator").service(Self::get_otel_generator_resource())
    }

    pub fn get_otel_generator_ingest_webscope() -> Scope {
        web::scope("/otel_generator")
            .service(Self::get_otel_generator_resource().wrap(IntraClusterRequest))
    }

    fn get_otel_generator_resource() -> Resource {
        web::resource("")
            .route(
                web::get()
                    .to(get_otel_generator_status)
                    .authorize(Action::Ingest),
            )
            .route(
                web::post()
                    .to(start_otel_generator)
                    .authorize(Action::Ingest),
            )
            .route(
                web::delete()
                    .to(stop_otel_generator)
                    .authorize(Action::Ingest),
            )
    }

    pub fn get_traces_webscope() -> Scope {
        web::scope("/services/traces")
            .service(
                web::resource("/list")
                    .route(web::post().to(traces::list_traces).authorize(Action::Query)),
            )
            .service(
                web::resource("/detail").route(
                    web::post()
                        .to(traces::get_trace_detail)
                        .authorize(Action::Query),
                ),
            )
    }

    pub fn get_metrics_webscope() -> Scope {
        web::scope("/metrics").service(
            web::resource("").route(web::get().to(metrics::get).authorize(Action::Metrics)),
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
                web::resource("/list_tags").route(
                    web::get()
                        .to(alerts::list_tags)
                        .authorize(Action::ListDashboard),
                ),
            )
            .service(
                web::resource("/{alert_id}")
                    .route(web::get().to(alerts::get).authorize(Action::GetAlert))
                    .route(
                        web::put()
                            .to(alerts::modify_alert)
                            .authorize(Action::PutAlert),
                    )
                    .route(
                        web::delete()
                            .to(alerts::delete)
                            .authorize(Action::DeleteAlert),
                    ),
            )
            .service(
                web::resource("/{alert_id}/disable").route(
                    web::patch()
                        .to(alerts::disable_alert)
                        .authorize(Action::PutAlert),
                ),
            )
            .service(
                web::resource("/{alert_id}/enable").route(
                    web::patch()
                        .to(alerts::enable_alert)
                        .authorize(Action::PutAlert),
                ),
            )
            .service(
                web::resource("/{alert_id}/update_notification_state").route(
                    web::patch()
                        .to(alerts::update_notification_state)
                        .authorize(Action::PutAlert),
                ),
            )
            .service(
                web::resource("/{alert_id}/evaluate_alert").route(
                    web::put()
                        .to(alerts::evaluate_alert)
                        .authorize(Action::PutAlert),
                ),
            )
    }

    pub fn get_alert_target_policy() -> Scope {
        web::scope("/alert-target-policy").service(
            web::resource("")
                .route(
                    web::get()
                        .to(alert_target_policy::get)
                        .authorize(Action::All),
                )
                .route(
                    web::put()
                        .to(alert_target_policy::put)
                        .authorize(Action::All),
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
            .app_data(web::JsonConfig::default().limit(2 * 1024 * 1024))
            .app_data(web::Data::new(
                crate::users::dashboard_tools::DashboardToolPolicy {
                    writes_enabled: PARSEABLE.options.dashboard_agent_writes,
                },
            ))
            .service(
                web::resource("/preview").route(
                    web::post()
                        .to(crate::users::dashboard_runtime::preview)
                        .authorize(Action::Query),
                ),
            )
            .service(
                web::resource("/capabilities").route(
                    web::get()
                        .to(dashboards::dashboard_capabilities)
                        .authorize(Action::GetDashboard),
                ),
            )
            .service(
                web::resource("/validate").route(
                    web::post()
                        .to(dashboards::validate_dashboard)
                        .authorize(Action::CreateDashboard),
                ),
            )
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
                            )
                            .route(
                                web::patch()
                                    .to(dashboards::patch_dashboard)
                                    .authorize(Action::CreateDashboard),
                            ),
                    )
                    .service(
                        web::resource("/validate").route(
                            web::post()
                                .to(dashboards::validate_dashboard_changes)
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
                web::resource("/{filter_id}/convert-to-sql").route(
                    web::post()
                        .to(filters::convert_to_sql)
                        .authorize(Action::UpdateFilter),
                ),
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
                            .authorize(Action::UpdateFilter),
                    ),
            )
    }
    pub fn get_counts_webscope() -> Resource {
        web::resource("/counts").route(web::post().to(query::get_counts).authorize(Action::Query))
    }

    // get the query factory
    // POST "/query" ==> Get results of the SQL query passed in request body
    pub fn get_query_factory() -> Resource {
        web::resource("/query").route(
            web::post()
                .to(query::query)
                .authorize(Action::Query)
                .wrap(IntraClusterRequest),
        )
    }

    // POST "/query/context" ==> Get a page of logs around a selected log line
    pub fn get_query_context_factory() -> Resource {
        web::resource("/query/context").route(
            web::post()
                .to(query_context::query_context)
                .authorize(Action::Query)
                .wrap(IntraClusterRequest),
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
                            .app_data(web::JsonConfig::default().limit(max_event_payload_size())),
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
                                    .authorize_for_resource(Action::GetHotTierEnabled)
                                    .wrap(IntraClusterRequest),
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
            .app_data(web::JsonConfig::default().limit(max_event_payload_size()))
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
                    .app_data(web::PayloadConfig::default().limit(max_event_payload_size())),
            )
            .service(
                web::resource("/metrics")
                    .route(
                        web::post()
                            .to(ingest::handle_otel_metrics_ingestion)
                            .authorize_for_resource(Action::Ingest),
                    )
                    .app_data(web::PayloadConfig::default().limit(max_event_payload_size())),
            )
            .service(
                web::resource("/traces")
                    .route(
                        web::post()
                            .to(ingest::handle_otel_traces_ingestion)
                            .authorize_for_resource(Action::Ingest),
                    )
                    .app_data(web::PayloadConfig::default().limit(max_event_payload_size())),
            )
    }

    // get the oauth webscope
    pub fn get_oauth_webscope() -> Scope {
        web::scope("/o")
            .service(resource("/login").route(web::get().to(oidc::login)))
            .service(resource("/logout").route(web::get().to(oidc::logout)))
            .service(resource("/code").route(web::get().to(oidc::reply_login)))
    }
    // get list of roles
    pub fn get_roles_webscope() -> Scope {
        web::scope("/roles")
            .service(web::resource("").route(web::get().to(role::list).authorize(Action::ListRole)))
    }
    // get the role webscope
    pub fn get_user_role_webscope() -> Scope {
        web::scope("/role")
            .service(
                // PUT and GET Default Role
                resource("/default")
                    .route(web::put().to(role::put_default).authorize(Action::PutRole))
                    .route(
                        web::delete()
                            .to(role::delete_default)
                            .authorize(Action::PutRole),
                    )
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
                web::resource("/{userid}").route(
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
                web::resource("/{userid}")
                    // POST /user/{userid} => Create a new user
                    .route(
                        web::post()
                            .to(http::rbac::post_user)
                            .authorize(Action::PutUser),
                    )
                    // DELETE /user/{userid} => Delete a user
                    .route(
                        web::delete()
                            .to(http::rbac::delete_user)
                            .authorize(Action::DeleteUser),
                    )
                    .route(
                        web::patch()
                            .to(http::rbac::patch_user)
                            .authorize_for_user(Action::PatchUser),
                    )
                    .wrap(DisAllowRootUser),
            )
            .service(
                web::resource("/{userid}/role").route(
                    web::get()
                        .to(http::rbac::get_role)
                        .authorize_for_user(Action::GetUserRoles),
                ),
            )
            .service(
                web::resource("/{userid}/role/add")
                    // PATCH /user/{userid}/role/add => Add roles to a user
                    .route(
                        web::patch()
                            .to(http::rbac::add_roles_to_user)
                            .authorize(Action::PutUserRoles)
                            .wrap(DisAllowRootUser),
                    ),
            )
            .service(
                web::resource("/{userid}/role/remove")
                    // PATCH /user/{userid}/role/remove => Remove roles from a user
                    .route(
                        web::patch()
                            .to(http::rbac::remove_roles_from_user)
                            .authorize(Action::PutUserRoles)
                            .wrap(DisAllowRootUser),
                    ),
            )
            .service(
                web::resource("/{userid}/generate-new-password")
                    // POST /user/{userid}/generate-new-password => reset password for this user
                    .route(
                        web::post()
                            .to(http::rbac::post_gen_password)
                            .authorize(Action::PutUser)
                            .wrap(DisAllowRootUser),
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

    // get the dataset stats webscope
    pub fn get_dataset_stats_webscope() -> Resource {
        web::resource("/dataset_stats")
            .route(web::post().to(get_dataset_stats).authorize(Action::Query))
    }

    // Native UI at `/`; preserve old `/next` bookmarks and OIDC return URLs.
    pub fn configure_ui(config: &mut web::ServiceConfig) {
        Self::mount_ui(config, generate());
    }

    fn mount_ui(config: &mut web::ServiceConfig, native: UiFiles) {
        config.service(web::scope("/next").default_service(web::to(redirect_from_next)));
        if native.is_empty() {
            static LOGGED: Once = Once::new();
            LOGGED.call_once(|| tracing::warn!(
                "UI disabled by P_BUILD_NO_UI=1 at build time; rebuild with NEXT_ASSETS_PATH to enable it"
            ));
            config.service(web::scope("").default_service(web::to(|| async {
                HttpResponse::ServiceUnavailable()
                    .insert_header((CACHE_CONTROL, "no-store"))
                    .body("UI disabled in this backend development build. Rebuild with NEXT_ASSETS_PATH pointing to frontend/dist.")
            })));
        } else {
            config.service(Self::ui_scope("", native));
        }
    }

    fn ui_scope(
        path: &str,
        files: UiFiles,
    ) -> Scope<
        impl ServiceFactory<
            ServiceRequest,
            Config = (),
            Response = ServiceResponse<impl MessageBody>,
            Error = actix_web::Error,
            InitError = (),
        >,
    > {
        // A single-page app: any path that is not a file is a client-side route
        // and gets `index.html`, except a missing hashed asset (see `serve_ui`).
        let files = Rc::new(files);
        let lookup = Rc::clone(&files);
        web::scope(path)
            .wrap(from_fn(move |req: ServiceRequest, next: Next<_>| {
                serve_ui(Rc::clone(&lookup), req, next)
            }))
            .service(ResourceFiles::new("", files).resolve_not_found_to_root())
    }
}

type UiFiles = std::collections::HashMap<&'static str, static_files::Resource>;

/// The native frontend moved from `/next` to `/`. Old links, bookmarks and
/// logins started there keep their path and query. Do not cache the redirect.
///
/// Assets are not redirected. A tab still running the build it loaded from
/// `/next` would import a chunk from `/assets/`, and that chunk's own imports
/// would load a second copy of the app beside the first. A 404 fails the
/// import like any chunk a deploy removed.
async fn redirect_from_next(req: HttpRequest) -> HttpResponse {
    // The path the root UI would resolve after the redirect, which drops a
    // leading `\` as well.
    let target = req
        .match_info()
        .unprocessed()
        .trim_start_matches(['/', '\\']);
    if normalize(target).is_some_and(|path| is_hashed_asset(&path)) {
        return HttpResponse::NotFound()
            .insert_header((CACHE_CONTROL, "no-cache"))
            .body("Not found");
    }
    // The router matched the decoded path, so `/%6Eext` lands here too. It never
    // decodes `/`, so the raw path past its first segment is the rest of the URL,
    // still encoded as sent. The decoded rest could hold a `?` or a tab.
    let raw = req.path();
    let rest = raw
        .get(1..)
        .and_then(|path| path.find('/').map(|i| &path[i..]))
        .unwrap_or_default();
    // A leading `//` or `/\` would leave the origin.
    let path = format!("/{}", rest.trim_start_matches(['/', '\\']));
    let location = match req.query_string() {
        "" => path,
        query => format!("{path}?{query}"),
    };
    HttpResponse::TemporaryRedirect()
        .insert_header((LOCATION, location))
        .insert_header((CACHE_CONTROL, "no-store"))
        .finish()
}

/// Vite names every file it emits under `assets/` after a hash of its content,
/// in the native UI. Only the prefix is checked: legacy client routes such as
/// `/apm/<service>` may end in a segment containing a dot. `path` must be
/// normalized, so that `assets/` with nothing after it is not a file.
fn is_hashed_asset(path: &str) -> bool {
    path.strip_prefix("assets/")
        .is_some_and(|file| !file.is_empty())
}

/// The path `ResourceFiles` looks up once its exact lookup fails: empty segments
/// dropped and `..` resolved. `None` where the crate rejects the path with a 400.
/// This mirrors the crate's private `get_pathbuf`.
fn normalize(path: &str) -> Option<String> {
    let mut segments = Vec::new();
    for segment in path.split('/') {
        if segment == ".." {
            segments.pop();
        } else if segment.starts_with(['.', '*'])
            || segment.ends_with([':', '>', '<'])
            || (cfg!(windows) && segment.contains('\\'))
        {
            return None;
        } else if !segment.is_empty() {
            segments.push(segment);
        }
    }
    Some(segments.join("/"))
}

/// A missing hashed asset is a 404. A tab opened before a deploy asks for chunks
/// the new build no longer has, and HTML in their place fails to load as a
/// script without the browser ever seeing an error status. This is decided here,
/// before `ResourceFiles` falls back to `index.html`, so its own lookup,
/// normalization, directory indexes and 400s stay exactly as they were.
///
/// Hashed assets never change, so browsers may keep them for good. Everything
/// else, `index.html` above all, is revalidated on each use so a deploy is
/// picked up by the next navigation.
async fn serve_ui(
    files: Rc<UiFiles>,
    req: ServiceRequest,
    next: Next<impl MessageBody>,
) -> Result<ServiceResponse<EitherBody<impl MessageBody>>, actix_web::Error> {
    // Decide from the path `ResourceFiles` resolves, not the raw one: `/assets/`
    // and `/assets/%2e%2e/index.html` resolve outside `assets/`.
    let raw = req.match_info().unprocessed();
    let hashed = normalize(raw).filter(|path| is_hashed_asset(path));
    // Before normalizing, the crate tries `index.html` under a path ending in `/`.
    let exists = |path: &str| {
        files.contains_key(path)
            || (raw.ends_with('/')
                && files.contains_key(format!("{raw}index.html").trim_start_matches('/')))
    };
    if let Some(path) = &hashed
        && !exists(path)
        && matches!(*req.method(), Method::GET | Method::HEAD)
    {
        let response = HttpResponse::NotFound()
            .insert_header((CACHE_CONTROL, "no-cache"))
            .body("Not found");
        return Ok(req.into_response(response).map_into_right_body());
    }
    let mut response = next.call(req).await?;
    let status = response.status();
    let served = status.is_success() || status == StatusCode::NOT_MODIFIED;
    let value = if hashed.is_some() && served && !is_html(response.headers()) {
        "public, max-age=31536000, immutable"
    } else {
        "no-cache"
    };
    response
        .headers_mut()
        .insert(CACHE_CONTROL, HeaderValue::from_static(value));
    Ok(response.map_into_left_body())
}

/// Vite emits no HTML under `assets/`, so HTML there is the SPA fallback and is
/// never kept for good. `ResourceFiles` sets `Content-Type` on a 304 as well.
fn is_html(headers: &HeaderMap) -> bool {
    headers
        .get(CONTENT_TYPE)
        .and_then(|value| value.to_str().ok())
        .and_then(|value| value.split(';').next())
        .is_some_and(|mime| mime.trim().eq_ignore_ascii_case("text/html"))
}

#[cfg(test)]
mod tests {
    use actix_web::http::header::{ETAG, IF_NONE_MATCH};
    use actix_web::{App, body::to_bytes, dev::ServiceResponse, test};

    use super::*;

    const IMMUTABLE: &str = "public, max-age=31536000, immutable";

    fn file(data: &'static [u8], mime_type: &'static str) -> static_files::Resource {
        static_files::Resource {
            data,
            modified: 0,
            mime_type,
        }
    }

    fn ui(index: &'static [u8]) -> UiFiles {
        UiFiles::from([
            ("index.html", file(index, "text/html")),
            ("assets/index-abc123.js", file(b"chunk", "text/javascript")),
            ("favicon.ico", file(b"icon", "image/x-icon")),
            ("integrations-assets/k8s.svg", file(b"svg", "image/svg+xml")),
            ("docs/index.html", file(b"docs", "text/html")),
        ])
    }

    async fn call(request: test::TestRequest, path: &str, native: UiFiles) -> ServiceResponse {
        let app =
            test::init_service(App::new().configure(|config| Server::mount_ui(config, native)))
                .await;
        test::call_service(&app, request.uri(path).to_request()).await
    }

    async fn get(path: &str, native: UiFiles) -> ServiceResponse {
        call(test::TestRequest::get(), path, native).await
    }

    /// Revalidate an actual response using its ETag, as browsers do.
    async fn get_and_revalidate(path: &str, native: UiFiles) -> [ServiceResponse; 2] {
        let app =
            test::init_service(App::new().configure(|config| Server::mount_ui(config, native)))
                .await;
        let first = test::call_service(&app, test::TestRequest::get().uri(path).to_request()).await;
        let etag = first.headers().get(ETAG).expect("an ETag").clone();
        let request = test::TestRequest::get()
            .uri(path)
            .insert_header((IF_NONE_MATCH, etag))
            .to_request();
        let second = test::call_service(&app, request).await;
        assert_eq!(
            second.status(),
            StatusCode::NOT_MODIFIED,
            "revalidate {path}"
        );
        [first, second]
    }

    async fn body_at(path: &str, native: UiFiles) -> Bytes {
        let response = get(path, native).await;
        assert!(response.status().is_success(), "GET {path} failed");
        to_bytes(response.into_body()).await.unwrap()
    }

    fn location(response: &ServiceResponse) -> &str {
        response.headers().get(LOCATION).unwrap().to_str().unwrap()
    }

    fn cache_control(response: &ServiceResponse) -> &str {
        response
            .headers()
            .get(CACHE_CONTROL)
            .unwrap()
            .to_str()
            .unwrap()
    }

    #[actix_web::test]
    async fn client_routes_get_index_html_even_with_a_dot_in_them() {
        for path in [
            "/apm/checkout.api",
            "/errors/TypeError.x",
            "/assetsfoo/a.js",
        ] {
            assert_eq!(body_at(path, ui(b"native")).await, "native", "GET {path}");
        }
        assert_eq!(body_at("/apm/a.b", ui(b"native")).await, "native");
    }

    #[actix_web::test]
    async fn a_missing_hashed_asset_is_not_found() {
        for path in ["/assets/index-gone.js", "/next/assets/index-gone.js"] {
            let response = get(path, ui(b"next")).await;
            assert_eq!(response.status(), StatusCode::NOT_FOUND, "GET {path}");
            assert_eq!(cache_control(&response), "no-cache", "GET {path}");
        }
        // Not even with the ETag of the `index.html` it would otherwise get.
        let index = get("/", ui(b"next")).await;
        let etag = index.headers().get(ETAG).unwrap().clone();
        let request = test::TestRequest::get().insert_header((IF_NONE_MATCH, etag));
        let response = call(request, "/assets/index-gone.js", ui(b"next")).await;
        assert_eq!(response.status(), StatusCode::NOT_FOUND);
        let response = call(
            test::TestRequest::post(),
            "/assets/index-gone.js",
            ui(b"native"),
        )
        .await;
        assert_eq!(response.status(), StatusCode::METHOD_NOT_ALLOWED);
        let response = call(
            test::TestRequest::default().method(Method::HEAD),
            "/assets/index-gone.js",
            ui(b"native"),
        )
        .await;
        assert_eq!(response.status(), StatusCode::NOT_FOUND);
    }

    #[actix_web::test]
    async fn hashed_assets_are_immutable_and_everything_else_revalidates() {
        for path in ["/assets/index-abc123.js", "/assets//index-abc123.js"] {
            let [response, revalidated] = get_and_revalidate(path, ui(b"next")).await;
            assert_eq!(response.status(), StatusCode::OK, "GET {path}");
            assert_eq!(cache_control(&response), IMMUTABLE, "GET {path}");
            assert_eq!(cache_control(&revalidated), IMMUTABLE, "revalidate {path}");
        }
        for path in ["/", "/logs", "/favicon.ico"] {
            let [response, revalidated] = get_and_revalidate(path, ui(b"next")).await;
            assert_eq!(response.status(), StatusCode::OK, "GET {path}");
            assert_eq!(cache_control(&response), "no-cache", "GET {path}");
            assert_eq!(cache_control(&revalidated), "no-cache", "revalidate {path}");
        }
        let response = call(
            test::TestRequest::default().method(Method::HEAD),
            "/assets/index-abc123.js",
            ui(b"next"),
        )
        .await;
        assert_eq!(cache_control(&response), IMMUTABLE, "HEAD");
    }

    /// `ResourceFiles` drops a trailing slash and resolves `..`, so these name no
    /// file under `assets/` and get the SPA's `index.html`. Kept for a year, that
    /// `index.html` would point at chunks the next deploy removes.
    #[actix_web::test]
    async fn paths_resolving_outside_assets_are_not_immutable() {
        for path in [
            "/assets/",
            "/assets//",
            "/assets/%2e%2e/index.html",
            "/assets/x/%2e%2e/%2e%2e/logs",
        ] {
            let [response, revalidated] = get_and_revalidate(path, ui(b"next")).await;
            assert_eq!(response.status(), StatusCode::OK, "GET {path}");
            assert_eq!(cache_control(&response), "no-cache", "GET {path}");
            assert_eq!(cache_control(&revalidated), "no-cache", "revalidate {path}");
        }
        let response = call(
            test::TestRequest::default().method(Method::HEAD),
            "/assets/",
            ui(b"next"),
        )
        .await;
        assert_eq!(response.status(), StatusCode::OK, "HEAD");
        assert_eq!(cache_control(&response), "no-cache", "HEAD");
    }

    #[actix_web::test]
    async fn html_is_never_immutable() {
        let mut next = ui(b"next");
        next.insert("assets/page-abc123.html", file(b"page", "text/html"));
        let [response, revalidated] = get_and_revalidate("/assets/page-abc123.html", next).await;
        assert_eq!(response.status(), StatusCode::OK);
        assert_eq!(cache_control(&response), "no-cache");
        assert_eq!(cache_control(&revalidated), "no-cache");
    }

    #[actix_web::test]
    async fn existing_files_are_served_as_themselves() {
        assert_eq!(
            body_at("/assets/index-abc123.js", ui(b"native")).await,
            "chunk"
        );
        assert_eq!(body_at("/favicon.ico", ui(b"native")).await, "icon");
    }

    #[actix_web::test]
    async fn the_native_frontend_serves_the_root_by_default() {
        for path in ["/", "/logs/explore/app", "/nextfoo", "/oidc-not-configured"] {
            assert_eq!(body_at(path, ui(b"native")).await, "native", "GET {path}");
            let unset = get(path, ui(b"native")).await;
            assert_eq!(to_bytes(unset.into_body()).await.unwrap(), "native");
        }
        let unset = get("/next/logs", ui(b"native")).await;
        assert_eq!(unset.status(), StatusCode::TEMPORARY_REDIRECT);
    }

    #[actix_web::test]
    async fn next_assets_are_not_found_once_the_native_frontend_moves_to_the_root() {
        // A tab loaded from `/next` before the switch must not import chunks
        // from `/assets/`, whose imports would start a second copy of the app.
        for path in [
            "/next/assets/index-abc123.js",
            "/next/assets/LogsPage-gone.js",
            "/next//assets/index-abc123.js",
            "/%6Eext/assets/index-abc123.js",
            "/next/%61ssets/index-abc123.js",
            "/next/\\assets/index-abc123.js",
            "/next/%5Cassets/index-abc123.js",
            "/next/x/../assets/index-abc123.js",
            "/next/%2e%2e/assets/index-abc123.js",
            "/next/assets//index-abc123.js",
        ] {
            let response = get(path, ui(b"native")).await;
            assert_eq!(response.status(), StatusCode::NOT_FOUND, "GET {path}");
            assert_eq!(cache_control(&response), "no-cache", "GET {path}");
            assert!(response.headers().get(LOCATION).is_none(), "GET {path}");
        }
        // At the root the native build's assets are served and cached as usual.
        let response = get("/assets/index-abc123.js", ui(b"native")).await;
        assert_eq!(cache_control(&response), IMMUTABLE);
        assert_eq!(to_bytes(response.into_body()).await.unwrap(), "chunk");
        let response = get("/assets/index-gone.js", ui(b"native")).await;
        assert_eq!(response.status(), StatusCode::NOT_FOUND);
        assert_eq!(cache_control(&response), "no-cache");
        let response = get("/logs", ui(b"native")).await;
        assert_eq!(cache_control(&response), "no-cache");
    }

    #[actix_web::test]
    async fn next_links_redirect_to_the_same_page_at_the_root() {
        for (path, target) in [
            ("/next", "/"),
            ("/next/", "/"),
            (
                "/next/logs/explore/app?start=1h&end=now",
                "/logs/explore/app?start=1h&end=now",
            ),
            ("/next/oidc-not-configured", "/oidc-not-configured"),
            ("/next//evil.example/x", "/evil.example/x"),
            ("/next/\\evil.example", "/evil.example"),
            ("/next/\\/evil.example", "/evil.example"),
            ("/next/%5Cevil.example", "/%5Cevil.example"),
            ("/next/%2F%2Fevil.example", "/%2F%2Fevil.example"),
            ("/next/%09/evil.example", "/%09/evil.example"),
            ("/next/%3Fx/logs", "/%3Fx/logs"),
            ("/next/assets", "/assets"),
            ("/next/assets/", "/assets/"),
            // The router decodes `%6E`, the redirect keeps the raw rest.
            ("/%6Eext/logs?x=1", "/logs?x=1"),
            ("/%6eext", "/"),
            ("/%6Eext/%2F%2Fevil.example", "/%2F%2Fevil.example"),
            ("/%6Eext//evil.example", "/evil.example"),
            ("/%6Eext/\\evil.example", "/evil.example"),
        ] {
            let response = get(path, ui(b"native")).await;
            assert_eq!(
                response.status(),
                StatusCode::TEMPORARY_REDIRECT,
                "GET {path}"
            );
            assert_eq!(location(&response), target, "GET {path}");
        }
    }

    /// The SPA fallback applies only once `ResourceFiles` has normalized the path
    /// and tried a directory index, as with the plain `ResourceFiles` before.
    #[actix_web::test]
    async fn paths_are_normalized_before_the_fallback() {
        for (path, body) in [
            ("/foo/../assets/index-abc123.js", "chunk"),
            ("/integrations-assets//k8s.svg", "svg"),
            ("/docs/", "docs"),
            ("/logs/", "next"),
            ("/logs//x/..", "next"),
            ("/favicon.ico/", "icon"),
            ("/integrations-assets/k8s.svg/", "svg"),
            ("/favicon.ico/index.html", "next"),
            ("/index.html/index.html", "next"),
            ("/assets/index-abc123.js/", "chunk"),
        ] {
            assert_eq!(body_at(path, ui(b"next")).await, body, "GET {path}");
        }
        let response = get("/foo/../assets/index-abc123.js", ui(b"next")).await;
        assert_eq!(cache_control(&response), IMMUTABLE);
        for path in [
            "/foo/../assets/index-gone.js",
            "/assets//index-gone.js",
            "/assets/index-gone.js/",
        ] {
            let response = get(path, ui(b"next")).await;
            assert_eq!(response.status(), StatusCode::NOT_FOUND, "GET {path}");
            assert_eq!(cache_control(&response), "no-cache", "GET {path}");
        }
    }

    /// `ResourceFiles` rejects these segments with a 400 rather than serve them.
    #[actix_web::test]
    async fn paths_the_crate_rejects_stay_bad_requests() {
        for path in ["/.env", "/logs/.git/config", "/explore/x:", "/*x"] {
            let response = get(path, ui(b"next")).await;
            assert_eq!(response.status(), StatusCode::BAD_REQUEST, "GET {path}");
            assert_eq!(cache_control(&response), "no-cache", "GET {path}");
        }
        // Even a file of that name in the build is not served.
        let mut next = ui(b"next");
        next.insert(".env", file(b"secret", "text/plain"));
        let response = get("/.env", next).await;
        assert_eq!(response.status(), StatusCode::BAD_REQUEST);
    }

    #[actix_web::test]
    async fn a_directory_index_under_assets_is_served() {
        let mut next = ui(b"next");
        next.insert("assets/dir/index.html", file(b"dir", "text/html"));
        let [response, revalidated] = get_and_revalidate("/assets/dir/", next).await;
        assert_eq!(cache_control(&response), "no-cache");
        assert_eq!(to_bytes(response.into_body()).await.unwrap(), "dir");
        assert_eq!(cache_control(&revalidated), "no-cache");
        let response = get("/assets/dir/", ui(b"next")).await;
        assert_eq!(response.status(), StatusCode::NOT_FOUND);
    }
    #[actix_web::test]
    async fn backend_only_build_explains_missing_ui_and_preserves_api_routes() {
        let app = test::init_service(
            App::new()
                .route(
                    "/api/prism/v1/probe",
                    web::get().to(|| async { HttpResponse::Ok().body("api") }),
                )
                .configure(|config| Server::mount_ui(config, UiFiles::new())),
        )
        .await;
        for path in ["/", "/logs", "/oidc-not-configured"] {
            let response =
                test::call_service(&app, test::TestRequest::get().uri(path).to_request()).await;
            assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);
            assert_eq!(cache_control(&response), "no-store");
            assert!(
                String::from_utf8(to_bytes(response.into_body()).await.unwrap().to_vec())
                    .unwrap()
                    .contains("NEXT_ASSETS_PATH")
            );
        }
        let response = test::call_service(
            &app,
            test::TestRequest::get()
                .uri("/api/prism/v1/probe")
                .to_request(),
        )
        .await;
        assert_eq!(response.status(), StatusCode::OK);
        let response = get("/next/logs?x=1", UiFiles::new()).await;
        assert_eq!(location(&response), "/logs?x=1");
    }
}
