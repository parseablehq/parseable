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

use actix_web::Scope;
use actix_web::middleware::from_fn;
use actix_web::web;
use actix_web_prometheus::PrometheusMetrics;
use async_trait::async_trait;
use base64::Engine;
use bytes::Bytes;
use relative_path::RelativePathBuf;
use serde_json::Value;
use tokio::sync::OnceCell;
use tokio::sync::oneshot;

use crate::handlers::http::modal::NodeType;
use crate::sync::sync_start;
use crate::{
    Server, analytics,
    handlers::{
        airplane,
        http::{
            base_path, ingest, logstream,
            middleware::{DisAllowRootUser, RouteExt},
            resource_check, role,
        },
    },
    migration,
    parseable::PARSEABLE,
    rbac::role::Action,
    storage::{ObjectStorageError, PARSEABLE_ROOT_DIRECTORY, object_storage::parseable_json_path},
    sync,
};

use super::IngestorMetadata;
use super::{
    OpenIdClient, ParseableServer,
    ingest::{ingestor_logstream, ingestor_rbac, ingestor_role},
};

pub const INGESTOR_EXPECT: &str = "Ingestor Metadata should be set in ingestor mode";
pub static INGESTOR_META: OnceCell<Arc<IngestorMetadata>> = OnceCell::const_new();
pub struct IngestServer;

#[async_trait]
impl ParseableServer for IngestServer {
    // configure the api routes
    fn configure_routes(config: &mut web::ServiceConfig, _oidc_client: Option<OpenIdClient>) {
        config
            .service(
                // Base path "{url}/api/v1"
                web::scope(&base_path())
                    .service(Server::get_ingest_factory().wrap(from_fn(
                        resource_check::check_resource_utilization_middleware,
                    )))
                    .service(Self::logstream_api())
                    .service(Server::get_about_factory())
                    .service(Self::analytics_factory())
                    .service(Server::get_liveness_factory())
                    .service(Self::get_user_webscope())
                    .service(Self::get_user_role_webscope())
                    .service(Server::get_metrics_webscope())
                    .service(Server::get_readiness_factory())
                    .service(Server::get_demo_data_webscope()),
            )
            .service(Server::get_ingest_otel_factory().wrap(from_fn(
                resource_check::check_resource_utilization_middleware,
            )));
    }

    async fn load_metadata(&self) -> anyhow::Result<Option<Bytes>> {
        // parseable can't use local storage for persistence when running a distributed setup
        if PARSEABLE.storage.name() == "drive" {
            return Err(anyhow::Error::msg(
                "This instance of the Parseable server has been configured to run in a distributed setup, it doesn't support local storage.",
            ));
        }

        // check for querier state. Is it there, or was it there in the past
        let parseable_json = check_querier_state().await?;
        // to get the .parseable.json file in staging
        validate_credentials().await?;

        Ok(parseable_json)
    }

    /// configure the server and start an instance to ingest data
    async fn init(
        &self,
        prometheus: &PrometheusMetrics,
        shutdown_rx: oneshot::Receiver<()>,
    ) -> anyhow::Result<()> {
        // write the ingestor metadata to storage
        INGESTOR_META
            .get_or_init(|| async {
                IngestorMetadata::load_node_metadata(NodeType::Ingestor)
                    .await
                    .expect("Ingestor Metadata should be set in ingestor mode")
            })
            .await;

        PARSEABLE.storage.register_store_metrics(prometheus);

        migration::run_migration(&PARSEABLE).await?;

        // local sync on init
        let startup_sync_handle = tokio::spawn(async {
            if let Err(e) = sync_start().await {
                tracing::warn!("local sync on server start failed: {e}");
            }
        });

        // Run sync on a background thread
        let (cancel_tx, cancel_rx) = oneshot::channel();
        thread::spawn(|| sync::handler(cancel_rx));

        tokio::spawn(airplane::server());

        // Ingestors shouldn't have to deal with OpenId auth flow
        let result = self.start(shutdown_rx, prometheus.clone(), None).await;
        // Cancel sync jobs
        cancel_tx.send(()).expect("Cancellation should not fail");
        if let Err(join_err) = startup_sync_handle.await {
            tracing::warn!("startup sync task panicked: {join_err}");
        }
        result
    }
}

impl IngestServer {
    pub fn analytics_factory() -> Scope {
        web::scope("/analytics").service(
            // GET "/analytics" ==> Get analytics data
            web::resource("").route(
                web::get()
                    .to(analytics::get_analytics)
                    .authorize(Action::GetAnalytics),
            ),
        )
    }

    // get the role webscope
    pub fn get_user_role_webscope() -> Scope {
        web::scope("/role")
            // GET Role List
            .service(web::resource("").route(web::get().to(role::list).authorize(Action::ListRole)))
            .service(
                // PUT and GET Default Role
                web::resource("/default")
                    .route(web::put().to(role::put_default).authorize(Action::PutRole))
                    .route(web::get().to(role::get_default).authorize(Action::GetRole)),
            )
            .service(
                // PUT, GET, DELETE Roles
                web::resource("/{name}")
                    .route(web::delete().to(role::delete).authorize(Action::DeleteRole))
                    .route(web::get().to(role::get).authorize(Action::GetRole)),
            )
            .service(
                web::resource("/{name}/sync")
                    .route(web::put().to(ingestor_role::put).authorize(Action::PutRole)),
            )
    }
    // get the user webscope
    pub fn get_user_webscope() -> Scope {
        web::scope("/user")
            .service(
                web::resource("/{username}/sync")
                    // PUT /user/{username}/sync => Sync creation of a new user
                    .route(
                        web::post()
                            .to(ingestor_rbac::post_user)
                            .authorize(Action::PutUser),
                    )
                    // DELETE /user/{username} => Sync deletion of a user
                    .route(
                        web::delete()
                            .to(ingestor_rbac::delete_user)
                            .authorize(Action::DeleteUser),
                    )
                    .wrap(DisAllowRootUser),
            )
            .service(
                web::resource("/{username}/role/sync/add")
                    // PATCH /user/{username}/role/sync/add => Add roles to a user
                    .route(
                        web::patch()
                            .to(ingestor_rbac::add_roles_to_user)
                            .authorize(Action::PutUserRoles)
                            .wrap(DisAllowRootUser),
                    ),
            )
            .service(
                web::resource("/{username}/role/sync/remove")
                    // PATCH /user/{username}/role/sync/remove => Remove roles from a user
                    .route(
                        web::patch()
                            .to(ingestor_rbac::remove_roles_from_user)
                            .authorize(Action::PutUserRoles)
                            .wrap(DisAllowRootUser),
                    ),
            )
            .service(
                web::resource("/{username}/generate-new-password/sync")
                    // POST /user/{username}/generate-new-password => reset password for this user
                    .route(
                        web::post()
                            .to(ingestor_rbac::post_gen_password)
                            .authorize(Action::PutUser)
                            .wrap(DisAllowRootUser),
                    ),
            )
    }
    pub fn logstream_api() -> Scope {
        web::scope("/logstream").service(
            web::scope("/{logstream}")
                .service(
                    web::resource("")
                        // POST "/logstream/{logstream}" ==> Post logs to given log stream
                        .route(
                            web::post()
                                .to(ingest::post_event)
                                .authorize_for_resource(Action::Ingest),
                        )
                        .wrap(from_fn(
                            resource_check::check_resource_utilization_middleware,
                        )),
                )
                .service(
                    web::resource("/sync")
                        // DELETE "/logstream/{logstream}/sync" ==> Sync deletion of a log stream
                        .route(
                            web::delete()
                                .to(ingestor_logstream::delete)
                                .authorize(Action::DeleteStream),
                        )
                        // PUT "/logstream/{logstream}/sync" ==> Sync creation of a new log stream
                        .route(
                            web::put()
                                .to(ingestor_logstream::put_stream)
                                .authorize_for_resource(Action::CreateStream),
                        ),
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
                    // GET "/logstream/{logstream}/stats" ==> Get stats for given log stream
                    web::resource("/stats").route(
                        web::get()
                            .to(logstream::get_stats)
                            .authorize_for_resource(Action::GetStats),
                    ),
                )
                .service(
                    web::scope("/retention").service(
                        web::resource("/cleanup").route(
                            web::post()
                                .to(ingestor_logstream::retention_cleanup)
                                .authorize_for_resource(Action::PutRetention),
                        ),
                    ),
                ),
        )
    }
}

// check for querier state. Is it there, or was it there in the past
// this should happen before the set the ingestor metadata
pub async fn check_querier_state() -> anyhow::Result<Option<Bytes>, ObjectStorageError> {
    // how do we check for querier state?
    // based on the work flow of the system, the querier will always need to start first
    // i.e the querier will create the `.parseable.json` file
    let parseable_json = PARSEABLE
        .storage
        .get_object_store()
        .get_object(&parseable_json_path())
        .await
        .map_err(|_| {
            ObjectStorageError::Custom(
                "Query Server has not been started yet. Please start the querier server first."
                    .to_string(),
            )
        })?;

    Ok(Some(parseable_json))
}

async fn validate_credentials() -> anyhow::Result<()> {
    // check if your creds match with others
    let store = PARSEABLE.storage.get_object_store();
    let base_path = RelativePathBuf::from(PARSEABLE_ROOT_DIRECTORY);
    let ingestor_metadata = store
        .get_objects(
            Some(&base_path),
            Box::new(|file_name| file_name.starts_with("ingestor")),
        )
        .await?;
    if !ingestor_metadata.is_empty() {
        let ingestor_metadata_value: Value =
            serde_json::from_slice(&ingestor_metadata[0]).expect("ingestor.json is valid json");
        let check = ingestor_metadata_value
            .as_object()
            .and_then(|meta| meta.get("token"))
            .and_then(|token| token.as_str())
            .unwrap();

        let token = base64::prelude::BASE64_STANDARD.encode(format!(
            "{}:{}",
            PARSEABLE.options.username, PARSEABLE.options.password
        ));

        let token = format!("Basic {token}");

        if check != token {
            return Err(anyhow::anyhow!(
                "Credentials do not match with other ingestors. Please check your credentials and try again."
            ));
        }
    }

    Ok(())
}
