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

use super::ingest::ingestor_logstream;
use super::ingest::ingestor_rbac;
use super::ingest::ingestor_role;
use super::server::Server;
use super::IngestorMetadata;
use super::OpenIdClient;
use super::ParseableServer;
use super::DEFAULT_VERSION;
use crate::analytics;
use crate::handlers::airplane;
use crate::handlers::http::ingest;
use crate::handlers::http::logstream;
use crate::handlers::http::middleware::DisAllowRootUser;
use crate::handlers::http::middleware::RouteExt;
use crate::handlers::http::role;
use crate::migration;
use crate::migration::metadata_migration::migrate_ingester_metadata;
use crate::rbac::role::Action;
use crate::storage::object_storage::ingestor_metadata_path;
use crate::storage::object_storage::parseable_json_path;
use crate::storage::ObjectStorageError;
use crate::storage::PARSEABLE_ROOT_DIRECTORY;
use crate::sync;

use crate::utils::get_ingestor_id;
use crate::utils::get_url;
use crate::{handlers::http::base_path, option::CONFIG};

use actix_web::web;
use actix_web::Scope;
use actix_web_prometheus::PrometheusMetrics;
use async_trait::async_trait;
use base64::Engine;
use bytes::Bytes;
use once_cell::sync::Lazy;
use relative_path::RelativePathBuf;
use serde_json::Value;
use tokio::sync::oneshot;
use tracing::error;
use tracing::info;

/// Metadata associated with this ingestor server
pub static INGESTOR_META: Lazy<IngestorMetadata> = Lazy::new(|| {
    // all the files should be in the staging directory root
    let entries =
        std::fs::read_dir(&CONFIG.options.local_staging_path).expect("Couldn't read from file");
    let url = get_url();
    let port = url.port().unwrap_or(80).to_string();
    let url = url.to_string();

    for entry in entries {
        // cause the staging directory will have only one file with ingestor in the name
        // so the JSON Parse should not error unless the file is corrupted
        let path = entry.expect("Should be a directory entry").path();
        let flag = path
            .file_name()
            .unwrap_or_default()
            .to_str()
            .unwrap_or_default()
            .contains("ingestor");

        if flag {
            // get the ingestor metadata from staging
            let text = std::fs::read(path).expect("File should be present");
            let mut meta: Value = serde_json::from_slice(&text).expect("Valid JSON");

            // migrate the staging meta
            let obj = meta
                .as_object_mut()
                .expect("Could Not parse Ingestor Metadata Json");

            if obj.get("flight_port").is_none() {
                obj.insert(
                    "flight_port".to_owned(),
                    Value::String(CONFIG.options.flight_port.to_string()),
                );
            }

            let mut meta: IngestorMetadata =
                serde_json::from_value(meta).expect("Couldn't write to disk");

            // compare url endpoint and port
            if meta.domain_name != url {
                info!(
                    "Domain Name was Updated. Old: {} New: {}",
                    meta.domain_name, url
                );
                meta.domain_name = url;
            }

            if meta.port != port {
                info!("Port was Updated. Old: {} New: {}", meta.port, port);
                meta.port = port;
            }

            let token = base64::prelude::BASE64_STANDARD.encode(format!(
                "{}:{}",
                CONFIG.options.username, CONFIG.options.password
            ));

            let token = format!("Basic {}", token);

            if meta.token != token {
                // TODO: Update the message to be more informative with username and password
                info!(
                    "Credentials were Updated. Old: {} New: {}",
                    meta.token, token
                );
                meta.token = token;
            }

            meta.put_on_disk(CONFIG.staging_dir())
                .expect("Couldn't write to disk");
            return meta;
        }
    }

    let store = CONFIG.storage().get_object_store();
    let out = IngestorMetadata::new(
        port,
        url,
        DEFAULT_VERSION.to_string(),
        store.get_bucket_name(),
        &CONFIG.options.username,
        &CONFIG.options.password,
        get_ingestor_id(),
        CONFIG.options.flight_port.to_string(),
    );

    out.put_on_disk(CONFIG.staging_dir())
        .expect("Should Be valid Json");
    out
});

pub struct IngestServer;

#[async_trait]
impl ParseableServer for IngestServer {
    // configure the api routes
    fn configure_routes(config: &mut web::ServiceConfig, _oidc_client: Option<OpenIdClient>) {
        config
            .service(
                // Base path "{url}/api/v1"
                web::scope(&base_path())
                    .service(Server::get_ingest_factory())
                    .service(Self::logstream_api())
                    .service(Server::get_about_factory())
                    .service(Self::analytics_factory())
                    .service(Server::get_liveness_factory())
                    .service(Self::get_user_webscope())
                    .service(Self::get_user_role_webscope())
                    .service(Server::get_metrics_webscope())
                    .service(Server::get_readiness_factory()),
            )
            .service(Server::get_ingest_otel_factory());
    }

    async fn load_metadata(&self) -> anyhow::Result<Option<Bytes>> {
        // parseable can't use local storage for persistence when running a distributed setup
        if CONFIG.get_storage_mode_string() == "Local drive" {
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
        CONFIG.storage().register_store_metrics(prometheus);

        migration::run_migration(&CONFIG).await?;

        let (localsync_handler, mut localsync_outbox, localsync_inbox) =
            sync::run_local_sync().await;
        let (mut remote_sync_handler, mut remote_sync_outbox, mut remote_sync_inbox) =
            sync::object_store_sync().await;

        tokio::spawn(airplane::server());

        // set the ingestor metadata
        set_ingestor_metadata().await?;

        // Ingestors shouldn't have to deal with OpenId auth flow
        let app = self.start(shutdown_rx, prometheus.clone(), None);

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

            }
        }
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
                web::resource("/{username}/role/sync")
                    // PUT /user/{username}/roles => Put roles for user
                    .route(
                        web::put()
                            .to(ingestor_rbac::put_role)
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
                                .authorize_for_stream(Action::Ingest),
                        ),
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
                                .authorize_for_stream(Action::CreateStream),
                        ),
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
                    // GET "/logstream/{logstream}/stats" ==> Get stats for given log stream
                    web::resource("/stats").route(
                        web::get()
                            .to(logstream::get_stats)
                            .authorize_for_stream(Action::GetStats),
                    ),
                )
                .service(
                    web::scope("/retention").service(
                        web::resource("/cleanup").route(
                            web::post()
                                .to(ingestor_logstream::retention_cleanup)
                                .authorize_for_stream(Action::PutRetention),
                        ),
                    ),
                ),
        )
    }
}

// create the ingestor metadata and put the .ingestor.json file in the object store
pub async fn set_ingestor_metadata() -> anyhow::Result<()> {
    let storage_ingestor_metadata = migrate_ingester_metadata().await?;
    let store = CONFIG.storage().get_object_store();

    // find the meta file in staging if not generate new metadata
    let resource = INGESTOR_META.clone();
    // use the id that was generated/found in the staging and
    // generate the path for the object store
    let path = ingestor_metadata_path(None);

    // we are considering that we can always get from object store
    if let Some(mut store_data) = storage_ingestor_metadata {
        if store_data.domain_name != INGESTOR_META.domain_name {
            store_data
                .domain_name
                .clone_from(&INGESTOR_META.domain_name);
            store_data.port.clone_from(&INGESTOR_META.port);

            let resource = Bytes::from(serde_json::to_vec(&store_data)?);

            // if pushing to object store fails propagate the error
            store.put_object(&path, resource).await?;
        }
    } else {
        let resource = Bytes::from(serde_json::to_vec(&resource)?);

        store.put_object(&path, resource).await?;
    }

    Ok(())
}

// check for querier state. Is it there, or was it there in the past
// this should happen before the set the ingestor metadata
async fn check_querier_state() -> anyhow::Result<Option<Bytes>, ObjectStorageError> {
    // how do we check for querier state?
    // based on the work flow of the system, the querier will always need to start first
    // i.e the querier will create the `.parseable.json` file
    let parseable_json = CONFIG
        .storage()
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
    let store = CONFIG.storage().get_object_store();
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
            CONFIG.options.username, CONFIG.options.password
        ));

        let token = format!("Basic {}", token);

        if check != token {
            return Err(anyhow::anyhow!("Credentials do not match with other ingestors. Please check your credentials and try again."));
        }
    }

    Ok(())
}
