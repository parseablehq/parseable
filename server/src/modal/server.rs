use std::{fs::File, io::BufReader, sync::Arc};

use actix_web::{web, App, HttpServer};
use actix_web_prometheus::PrometheusMetrics;
use async_trait::async_trait;
use openid::Discovered;
use rustls::{Certificate, PrivateKey, ServerConfig};
use rustls_pemfile::{certs, pkcs8_private_keys};

use crate::{handlers::http::{ingest, llm, logstream, middleware::{DisAllowRootUser, RouteExt}, rbac, role, MAX_EVENT_PAYLOAD_SIZE}, oidc, option::CONFIG, rbac::role::Action};

use super::parseable_server::{cross_origin_config, ParseableServer, API_BASE_PATH, API_VERSION};

include!(concat!(env!("OUT_DIR"), "/generated.rs"));
pub struct SuperServer;



#[async_trait]
impl ParseableServer for SuperServer {
    async fn start(
        &self,
        prometheus: PrometheusMetrics,
        oidc_client: Option<oidc::OpenidConfig>,
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

        // use app here
        // app.configure(|config| )
        let create_app = move || {
            App::new()
                .wrap(prometheus.clone())
                .configure(|cfg| configure_routes(cfg, oidc_client.clone()))
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
        let http_server = HttpServer::new(create_app).workers(num_cpus::get());
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
}

pub fn configure_routes(
    cfg: &mut web::ServiceConfig,
    oidc_client: Option<Arc<openid::Client<Discovered, oidc::Claims>>>,
) {
    let generated = generate();

    //log stream API
    let logstream_api = web::scope("/{logstream}")
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
        );

    // User API
    let user_api = web::scope("/user")
        .service(
            web::resource("")
                // GET /user => List all users
                .route(web::get().to(rbac::list_users).authorize(Action::ListUser)),
        )
        .service(
            web::resource("/{username}")
                // PUT /user/{username} => Create a new user
                .route(web::post().to(rbac::post_user).authorize(Action::PutUser))
                // DELETE /user/{username} => Delete a user
                .route(
                    web::delete()
                        .to(rbac::delete_user)
                        .authorize(Action::DeleteUser),
                )
                .wrap(DisAllowRootUser),
        )
        .service(
            web::resource("/{username}/role")
                // PUT /user/{username}/roles => Put roles for user
                .route(
                    web::put()
                        .to(rbac::put_role)
                        .authorize(Action::PutUserRoles)
                        .wrap(DisAllowRootUser),
                )
                .route(
                    web::get()
                        .to(rbac::get_role)
                        .authorize_for_user(Action::GetUserRoles),
                ),
        )
        .service(
            web::resource("/{username}/generate-new-password")
                // POST /user/{username}/generate-new-password => reset password for this user
                .route(
                    web::post()
                        .to(rbac::post_gen_password)
                        .authorize(Action::PutUser)
                        .wrap(DisAllowRootUser),
                ),
        );

    let llm_query_api = web::scope("/llm").service(
        web::resource("").route(
            web::post()
                .to(llm::make_llm_request)
                .authorize(Action::QueryLLM),
        ),
    );

    let role_api = web::scope("/role")
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
                .route(web::put().to(role::put).authorize(Action::PutRole))
                .route(web::delete().to(role::delete).authorize(Action::DeleteRole))
                .route(web::get().to(role::get).authorize(Action::GetRole)),
        );

    let mut oauth_api = web::scope("/o")
        .service(web::resource("/login").route(web::get().to(crate::handlers::http::oidc::login)))
        .service(web::resource("/logout").route(web::get().to(crate::handlers::http::oidc::logout)))
        .service(web::resource("/code").route(web::get().to(crate::handlers::http::oidc::reply_login)));

    if let Some(client) = oidc_client {
        info!("Registered oidc client");
        oauth_api = oauth_api.app_data(web::Data::from(client))
    }

    // Deny request if username is same as the env variable P_USERNAME.
    cfg.service(
        // Base path "{url}/api/v1"
        web::scope(&base_path())
            // .wrap(PathFilter)
            // POST "/query" ==> Get results of the SQL query passed in request body
            .service(
                web::resource("/query")
                    .route(web::post().to(query::query).authorize(Action::Query)),
            )
            // POST "/ingest" ==> Post logs to given log stream based on header
            .service(
                web::resource("/ingest")
                    .route(
                        web::post()
                            .to(ingest::ingest)
                            .authorize_for_stream(Action::Ingest),
                    )
                    .app_data(web::PayloadConfig::default().limit(MAX_EVENT_PAYLOAD_SIZE)),
            )
            // GET "/liveness" ==> Liveness check as per https://kubernetes.io/docs/tasks/configure-pod-container/configure-liveness-readiness-startup-probes/#define-a-liveness-command
            .service(web::resource("/liveness").route(web::get().to(health_check::liveness)))
            // GET "/readiness" ==> Readiness check as per https://kubernetes.io/docs/tasks/configure-pod-container/configure-liveness-readiness-startup-probes/#define-readiness-probes
            .service(web::resource("/readiness").route(web::get().to(health_check::readiness)))
            // GET "/about" ==> Returns information about instance
            .service(
                web::resource("/about")
                    .route(web::get().to(about::about).authorize(Action::GetAbout)),
            )
            .service(
                web::scope("/logstream")
                    .service(
                        // GET "/logstream" ==> Get list of all Log Streams on the server
                        web::resource("")
                            .route(web::get().to(logstream::list).authorize(Action::ListStream)),
                    )
                    .service(
                        // logstream API
                        logstream_api,
                    ),
            )
            .service(user_api)
            .service(llm_query_api)
            .service(oauth_api)
            .service(role_api),
    )
    // GET "/" ==> Serve the static frontend directory
    .service(ResourceFiles::new("/", generated).resolve_not_found_to_root());
}