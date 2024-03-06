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
use crate::handlers;
use crate::handlers::http::about;
use crate::handlers::http::base_path;
use crate::handlers::http::health_check;
use crate::handlers::http::query;
use crate::handlers::http::API_BASE_PATH;
use crate::handlers::http::API_VERSION;
use crate::localcache::LocalCacheManager;
use crate::metadata;
use crate::metrics;
use crate::migration;
use crate::rbac;
use crate::storage;
use crate::sync;
use std::net::SocketAddr;
use std::{fs::File, io::BufReader, sync::Arc};

use actix_web::web::resource;
use actix_web::Resource;
use actix_web::Scope;
use actix_web::{web, App, HttpServer};
use actix_web_prometheus::PrometheusMetrics;
use actix_web_static_files::ResourceFiles;
use async_trait::async_trait;

use rustls::{Certificate, PrivateKey, ServerConfig};
use rustls_pemfile::{certs, pkcs8_private_keys};

use crate::{
    handlers::http::{
        self, cross_origin_config, ingest, llm, logstream,
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

#[derive(Default)]
pub struct Server;

#[async_trait(?Send)]
impl ParseableServer for Server {
    async fn start(
        &self,
        prometheus: PrometheusMetrics,
        oidc_client: Option<crate::oidc::OpenidConfig>,
    ) -> anyhow::Result<()> {
        let oidc_client = match oidc_client {
            Some(config) => {
                let client = config
                    .connect(&format!("{API_BASE_PATH}/{API_VERSION}/o/code"))
                    .await?;
                Some(Arc::new(client))
            }
            None => None,
        };

        let create_app_fn = move || {
            App::new()
                .wrap(prometheus.clone())
                .configure(|cfg| Server::configure_routes(cfg, oidc_client.clone()))
                .wrap(actix_web::middleware::Logger::default())
                .wrap(actix_web::middleware::Compress::default())
                .wrap(cross_origin_config())
        };

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
        let http_server = HttpServer::new(create_app_fn).workers(num_cpus::get());
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

    /// implementation of init should just invoke a call to initialize
    async fn init(&self) -> anyhow::Result<()> {
        self.initialize().await
    }

    fn validate(&self) -> anyhow::Result<()> {
        Ok(())
    }
}

impl Server {
    fn configure_routes(config: &mut web::ServiceConfig, oidc_client: Option<OpenIdClient>) {
        // there might be a bug in the configure routes method
        config
            .service(
                web::scope(&base_path())
                    // POST "/query" ==> Get results of the SQL query passed in request body
                    .service(Self::get_query_factory())
                    .service(Self::get_ingest_factory())
                    .service(Self::get_liveness_factory())
                    .service(Self::get_readiness_factory())
                    .service(Self::get_about_factory())
                    .service(Self::get_logstream_webscope())
                    .service(Self::get_user_webscope())
                    .service(Self::get_llm_webscope())
                    .service(Self::get_oauth_webscope(oidc_client))
                    .service(Self::get_user_role_webscope()),
            )
            .service(Self::get_generated());
    }

    // get the query factory
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
                            .route(
                                web::put()
                                    .to(logstream::put_enable_cache)
                                    .authorize_for_stream(Action::PutCacheEnabled),
                            )
                            // GET "/logstream/{logstream}/cache" ==> Get retention for given logstream
                            .route(
                                web::get()
                                    .to(logstream::get_cache_enabled)
                                    .authorize_for_stream(Action::GetCacheEnabled),
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
    pub fn get_liveness_factory() -> Resource {
        web::resource("/liveness").route(web::get().to(health_check::liveness))
    }

    // get the readiness check
    // GET "/readiness" ==> Readiness check as per https://kubernetes.io/docs/tasks/configure-pod-container/configure-liveness-readiness-startup-probes/#define-readiness-probes
    pub fn get_readiness_factory() -> Resource {
        web::resource("/readiness").route(web::get().to(health_check::readiness))
    }

    // get the about factory
    pub fn get_about_factory() -> Resource {
        web::resource("/about").route(web::get().to(about::about).authorize(Action::GetAbout))
    }

    // GET "/" ==> Serve the static frontend directory
    pub fn get_generated() -> ResourceFiles {
        ResourceFiles::new("/", generate()).resolve_not_found_to_root()
    }

    async fn initialize(&self) -> anyhow::Result<()> {
        migration::run_metadata_migration(&CONFIG).await?;
        let metadata = storage::resolve_parseable_metadata().await?;
        banner::print(&CONFIG, &metadata).await;
        rbac::map::init(&metadata);
        metadata.set_global();

        if let Some(cache_manager) = LocalCacheManager::global() {
            cache_manager
                .validate(CONFIG.parseable.local_cache_size)
                .await?;
        };

        let prometheus = metrics::build_metrics_handler();
        CONFIG.storage().register_store_metrics(&prometheus);

        migration::run_migration(&CONFIG).await?;

        let storage = CONFIG.storage().get_object_store();
        if let Err(err) = metadata::STREAM_INFO.load(&*storage).await {
            log::warn!("could not populate local metadata. {:?}", err);
        }

        storage::retention::load_retention_from_global();
        metrics::fetch_stats_from_storage().await;

        let (localsync_handler, mut localsync_outbox, localsync_inbox) = sync::run_local_sync();
        let (mut remote_sync_handler, mut remote_sync_outbox, mut remote_sync_inbox) =
            sync::object_store_sync();

        if CONFIG.parseable.send_analytics {
            analytics::init_analytics_scheduler();
        }

        tokio::spawn(handlers::livetail::server());

        let app = self.start(prometheus, CONFIG.parseable.openid.clone());

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

    #[inline(always)]
    pub fn get_server_address() -> SocketAddr {
        // this might cause an issue down the line
        // best is to make the Cli Struct better, but thats a chore
        (CONFIG.parseable.address.clone())
            .parse::<SocketAddr>()
            .unwrap()
    }
}
