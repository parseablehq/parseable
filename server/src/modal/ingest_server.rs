use std::{fs::File, io::BufReader, path::PathBuf};

use super::parseable_server::ParseableServer;
use actix_web::{web, App, HttpServer, Scope};
use actix_web_prometheus::PrometheusMetrics;
use async_trait::async_trait;
use itertools::Itertools;
use rustls::{Certificate, PrivateKey, ServerConfig};
use rustls_pemfile::{certs, pkcs8_private_keys};

use crate::{
    handlers::http::{
        base_path, cross_origin_config, health_check, ingest, logstream, middleware::RouteExt,
        MAX_EVENT_PAYLOAD_SIZE,
    },
    option::CONFIG,
    rbac::role::Action,
};

pub struct IngestServer;

impl IngestServer {
    fn get_logstream_factory() -> Scope {
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
            )
    }

    fn get_ssl_acceptor(
        tls_cert: &Option<PathBuf>,
        tls_key: &Option<PathBuf>,
    ) -> anyhow::Result<Option<ServerConfig>> {
        match (tls_cert, tls_key) {
            (Some(cert), Some(key)) => {
                let server_config = ServerConfig::builder()
                    .with_safe_defaults()
                    .with_no_client_auth();

                let cert_file = &mut BufReader::new(File::open(cert)?);
                let key_file = &mut BufReader::new(File::open(key)?);
                let cert_chain = certs(cert_file)?.into_iter().map(Certificate).collect_vec();

                let mut keys = pkcs8_private_keys(key_file)?
                    .into_iter()
                    .map(PrivateKey)
                    .collect_vec();

                Ok(Some(
                    server_config.with_single_cert(cert_chain, keys.remove(0))?,
                ))
            }
            (_, _) => Ok(None),
        }
    }
}

#[async_trait]
impl ParseableServer for IngestServer {
    async fn start(&self, prometheus: PrometheusMetrics) -> anyhow::Result<()> {
        let server = HttpServer::new(move || {
            App::new()
                .wrap(prometheus.clone())
                .configure(|config| {
                    config.service(
                        web::scope(&base_path())
                            // POST "/ingest" ==> Post logs to given log stream based on header
                            .service(
                                web::resource("/ingest")
                                    .route(
                                        web::post()
                                            .to(ingest::ingest)
                                            .authorize_for_stream(Action::Ingest),
                                    )
                                    .app_data(
                                        web::PayloadConfig::default().limit(MAX_EVENT_PAYLOAD_SIZE),
                                    ),
                            )
                            // GET "/liveness" ==> Liveness check as per https://kubernetes.io/docs/tasks/configure-pod-container/configure-liveness-readiness-startup-probes/#define-a-liveness-command
                            .service(
                                web::resource("/liveness")
                                    .route(web::get().to(health_check::liveness)),
                            )
                            // GET "/readiness" ==> Readiness check as per https://kubernetes.io/docs/tasks/configure-pod-container/configure-liveness-readiness-startup-probes/#define-readiness-probes
                            .service(
                                web::resource("/readiness")
                                    .route(web::get().to(health_check::readiness)),
                            )
                            .service(
                                web::scope("/logstream")
                                    .service(
                                        web::resource("").route(
                                            web::get()
                                                .to(logstream::list)
                                                .authorize(Action::ListStream),
                                        ),
                                    )
                                    .service(IngestServer::get_logstream_factory()),
                            ),
                    );
                })
                .wrap(actix_web::middleware::Logger::default())
                .wrap(actix_web::middleware::Compress::default())
                .wrap(cross_origin_config())
        })
        .workers(num_cpus::get());

        let (tls_cert, tls_key) = (
            &CONFIG.parseable.tls_cert_path,
            &CONFIG.parseable.tls_key_path,
        );
        if let Some(server_config) = IngestServer::get_ssl_acceptor(tls_cert, tls_key)? {
            server
                .bind_rustls(&CONFIG.parseable.address, server_config)?
                .run()
                .await?;
        } else {
            server.bind(&CONFIG.parseable.address)?.run().await?;
        }

        Ok(())
    }
}
