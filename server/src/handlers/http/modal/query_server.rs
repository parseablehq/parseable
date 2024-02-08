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

use crate::handlers::http::middleware::RouteExt;
use crate::handlers::http::{
    base_path, cross_origin_config, logstream, API_BASE_PATH, API_VERSION,
};
use crate::rbac::role::Action;
use actix_web::http::header;
use actix_web::web;
use actix_web::web::ServiceConfig;
use actix_web::{App, HttpServer};
use actix_web_static_files::ResourceFiles;
use async_trait::async_trait;
use itertools::Itertools;
use relative_path::RelativePathBuf;
use std::sync::Arc;
use url::Url;

use crate::option::CONFIG;

use super::server::Server;
use super::ssl_acceptor::get_ssl_acceptor;
use super::{IngesterMetadata, OpenIdClient, ParseableServer};

include!(concat!(env!("OUT_DIR"), "/generated.rs"));

type IngesterMetadataArr = Vec<IngesterMetadata>;
type IngesterMetaPtr = Arc<IngesterMetadataArr>;

#[derive(Default, Debug)]
pub struct QueryServer(IngesterMetaPtr);

#[async_trait(?Send)]
impl ParseableServer for QueryServer {
    async fn start(
        &mut self,
        prometheus: actix_web_prometheus::PrometheusMetrics,
        oidc_client: Option<crate::oidc::OpenidConfig>,
    ) -> anyhow::Result<()> {
        self.0 = self.get_ingestor_info().await?;

        // on subsequent runs, the qurier should check if the ingestor is up and running or not
        for ingester in self.0.iter() {
            // yes the format macro does not need the '/' ingester.origin already
            // has '/' because Url::Parse will add it if it is not present
            // uri should be something like `http://address/api/v1/liveness`
            let uri = Url::parse(&format!("{}{}/liveness", &ingester.origin, base_path()))?;

            if !Self::check_liveness(uri).await {
                eprintln!("Ingestor at {} is not reachable", &ingester.origin);
            } else {
                println!("Ingestor at {} is up and running", &ingester.origin);
            }
        }

        let oidc_client = match oidc_client {
            Some(config) => {
                let client = config
                    .connect(&format!("{API_BASE_PATH}/{API_VERSION}/o/code"))
                    .await?;
                Some(Arc::new(client))
            }

            None => None,
        };

        let ssl = get_ssl_acceptor(
            &CONFIG.parseable.tls_cert_path,
            &CONFIG.parseable.tls_key_path,
        )?;

        let create_app_fn = move || {
            App::new()
                .wrap(prometheus.clone())
                .configure(|config| QueryServer::configure_routes(config, oidc_client.clone()))
                .wrap(actix_web::middleware::Logger::default())
                .wrap(actix_web::middleware::Compress::default())
                .wrap(cross_origin_config())
        };

        // concurrent workers equal to number of cores on the cpu
        let http_server = HttpServer::new(create_app_fn).workers(num_cpus::get());
        if let Some(config) = ssl {
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

impl QueryServer {
    // configure the api routes
    pub fn configure_routes(config: &mut ServiceConfig, oidc_client: Option<OpenIdClient>) {
        let generated = generate();

        let user_scope = Server::get_user_webscope();
        let llm_scope = Server::get_llm_webscope();
        let role_scope = Server::get_user_role_webscope();
        let oauth_scope = Server::get_oauth_webscope(oidc_client);

        config
            .service(
                web::scope(&base_path())
                    // POST "/query" ==> Get results of the SQL query passed in request body
                    .service(Server::get_query_factory())
                    .service(Server::get_liveness_factory())
                    .service(Server::get_readiness_factory())
                    // GET "/about" ==> Returns information about instance
                    .service(Server::get_about_factory())
                    .service(
                        web::scope("/logstream").service(
                            // GET "/logstream" ==> Get list of all Log Streams on the server
                            web::resource("").route(
                                web::get().to(logstream::list).authorize(Action::ListStream),
                            ),
                        ),
                    )
                    .service(user_scope)
                    .service(llm_scope)
                    .service(oauth_scope)
                    .service(role_scope),
            )
            .service(ResourceFiles::new("/", generated).resolve_not_found_to_root());
    }

    async fn get_ingestor_info(&self) -> anyhow::Result<IngesterMetaPtr> {
        let store = CONFIG.storage().get_object_store();

        let root_path = RelativePathBuf::from("");
        let arr = store
            .get_objects(Some(&root_path))
            .await?
            .to_vec()
            .iter()
            // this unwrap will most definateley shoot me in the foot later
            .map(|x| serde_json::from_slice::<IngesterMetadata>(x).unwrap_or_default())
            .collect_vec();

        Ok(Arc::new(arr))
    }

    pub async fn check_liveness(uri: Url) -> bool {
        let reqw = reqwest::Client::new()
            .get(uri)
            .header(header::CONTENT_TYPE, "application/json")
            .send()
            .await;

        match reqw {
            Ok(_) => true,
            Err(_) => false,
        }
    }
}
