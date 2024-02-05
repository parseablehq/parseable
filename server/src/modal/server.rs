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

impl SuperServer {
    fn configure_routes(config: &mut web::ServiceConfig, oidc_client: Option<OpenIdClient>) {
        let generated = generate();


                    web::get()
                ),
                ),

    }

            .service(
            )
            .service(
                    .route(
                    )
            )
            .service(
            )
            .service(
                    ),
            )
