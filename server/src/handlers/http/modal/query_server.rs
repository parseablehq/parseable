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

use crate::handlers::http::logstream::error::StreamError;
use crate::handlers::http::{base_path, cross_origin_config, API_BASE_PATH, API_VERSION};
use crate::{analytics, banner, metadata, metrics, migration, rbac, storage};
use actix_web::http::header;
use actix_web::web;
use actix_web::web::ServiceConfig;
use actix_web::{App, HttpServer};
use async_trait::async_trait;
use chrono::{DateTime, Utc};
use http::StatusCode;
use itertools::Itertools;
use relative_path::RelativePathBuf;
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use tokio::io::AsyncWriteExt;
use url::Url;

use tokio::fs::File as TokioFile;

use crate::option::{Mode, CONFIG};

use super::server::Server;
use super::ssl_acceptor::get_ssl_acceptor;
use super::{IngesterMetadata, OpenIdClient, ParseableServer};

type IngesterMetadataArr = Vec<IngesterMetadata>;

#[derive(Default, Debug)]
pub struct QueryServer;

#[async_trait(?Send)]
impl ParseableServer for QueryServer {
    async fn start(
        &self,
        prometheus: actix_web_prometheus::PrometheusMetrics,
        oidc_client: Option<crate::oidc::OpenidConfig>,
    ) -> anyhow::Result<()> {
        let data = Self::get_ingestor_info().await?;

        // on subsequent runs, the qurier should check if the ingestor is up and running or not
        for ingester in data.iter() {
            // dbg!(&ingester);

            if !Self::check_liveness(&ingester.domain_name).await {
                eprintln!("Ingestor at {} is not reachable", &ingester.domain_name);
            } else {
                println!("Ingestor at {} is up and running", &ingester.domain_name);
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

    /// implementation of init should just invoke a call to initialize
    async fn init(&self) -> anyhow::Result<()> {
        // self.validate()?;
        self.initialize().await
    }

    #[allow(unused)]
    fn validate(&self) -> anyhow::Result<()> {
        if CONFIG.get_storage_mode_string() == "Local drive" {
            return Err(anyhow::anyhow!(
                "Query Server cannot be started in local storage mode. Please start the server in a supported storage mode.",
            ));
        }

        Ok(())
    }
}

impl QueryServer {
    // configure the api routes
    fn configure_routes(config: &mut ServiceConfig, oidc_client: Option<OpenIdClient>) {
        config
            .service(
                web::scope(&base_path())
                    // POST "/query" ==> Get results of the SQL query passed in request body
                    .service(Server::get_query_factory())
                    .service(Server::get_liveness_factory())
                    .service(Server::get_readiness_factory())
                    .service(Server::get_about_factory())
                    .service(Server::get_logstream_webscope())
                    .service(Server::get_user_webscope())
                    .service(Server::get_llm_webscope())
                    .service(Server::get_oauth_webscope(oidc_client))
                    .service(Server::get_user_role_webscope()),
            )
            .service(Server::get_generated());
    }

    // update the .query.json file and return the new IngesterMetadataArr
    pub async fn get_ingestor_info() -> anyhow::Result<IngesterMetadataArr> {
        let store = CONFIG.storage().get_object_store();

        let root_path = RelativePathBuf::from("");
        let arr = store
            .get_objects(Some(&root_path))
            .await?
            .iter()
            // this unwrap will most definateley shoot me in the foot later
            .map(|x| serde_json::from_slice::<IngesterMetadata>(x).unwrap_or_default())
            .collect_vec();

        // TODO: add validation logic here
        // validate the ingester metadata

        let mut f = Self::get_meta_file().await;
        // writer the arr in f
        let _ = f.write(serde_json::to_string(&arr)?.as_bytes()).await?;
        Ok(arr)
    }

    pub async fn check_liveness(domain_name: &str) -> bool {
        let uri = Url::parse(&format!("{}{}/liveness", domain_name, base_path())).unwrap();

        let reqw = reqwest::Client::new()
            .get(uri)
            .header(header::CONTENT_TYPE, "application/json")
            .send()
            .await;

        reqw.is_ok()
    }

    /// initialize the server, run migrations as needed and start the server
    async fn initialize(&self) -> anyhow::Result<()> {
        migration::run_metadata_migration(&CONFIG).await?;
        let metadata = storage::resolve_parseable_metadata().await?;
        tokio::fs::File::create(CONFIG.staging_dir().join(".query.json")).await?;
        banner::print(&CONFIG, &metadata).await;

        // initialize the rbac map
        rbac::map::init(&metadata);

        // keep metadata info in mem
        metadata.set_global();

        let prometheus = metrics::build_metrics_handler();
        CONFIG.storage().register_store_metrics(&prometheus);

        migration::run_migration(&CONFIG).await?;

        // when do we do this
        let storage = CONFIG.storage().get_object_store();
        if let Err(e) = metadata::STREAM_INFO.load(&*storage).await {
            log::warn!("could not populate local metadata. {:?}", e);
        }

        // track all parquet files already in the data directory
        storage::retention::load_retention_from_global();

        // load data from stats back to prometheus metrics
        metrics::fetch_stats_from_storage().await;

        // all internal data structures populated now.
        // start the analytics scheduler if enabled
        if CONFIG.parseable.send_analytics {
            analytics::init_analytics_scheduler();
        }

        // spawn the sync thread
        // tokio::spawn(Self::sync_ingestor_metadata());

        self.start(prometheus, CONFIG.parseable.openid.clone())
            .await?;

        Ok(())
    }

    #[allow(dead_code)]
    async fn sync_ingestor_metadata() {
        let mut interval = tokio::time::interval(tokio::time::Duration::from_secs(60 / 10));
        loop {
            interval.tick().await;
            // dbg!("Tick");
            Self::get_ingestor_info().await.unwrap();
        }
    }

    async fn get_meta_file() -> TokioFile {
        let meta_path = CONFIG.staging_dir().join(".query.json");

        tokio::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .open(meta_path)
            .await
            .unwrap()
    }

    // forward the request to all ingestors to keep them in sync
    // BUG: If any ingestor is offline this will error out the entire request
    pub async fn sync_streams_with_ingestors(stream_name: &str) -> Result<(), StreamError> {
        // TODO: implment a transactional sync to rollback if any of the ingestors fail
        // ? Lower cognitive load by moving the mode filter to the calling function
        if CONFIG.parseable.mode == Mode::Query {
            let ingestor_infos = Self::get_ingestor_info().await.map_err(|err| {
                log::error!("Fatal: failed to get ingestor info: {:?}", err);
                StreamError::Custom {
                    msg: format!("failed to get ingestor info\n{:?}", err),
                    status: StatusCode::INTERNAL_SERVER_ERROR,
                }
            })?;

            for ingester in ingestor_infos {
                let url = format!(
                    "{}{}/logstream/{}",
                    ingester.domain_name.to_string().trim_end_matches('/'),
                    base_path(),
                    stream_name
                );

                let client = reqwest::Client::new();
                let res = client
                        .put(&url)
                        .header("Content-Type", "application/json")
                        .header("Authorization", ingester.token)
                        .send()
                        .await
                        .map_err(|err| {
                            log::error!(
                                "Fatal: failed to forward create stream request to ingestor: {}\n Error: {:?}",
                                ingester.domain_name,
                                err
                            );
                            StreamError::Custom {
                                msg: format!(
                                    "failed to forward create stream request to ingestor: {}\n Error: {:?}",
                                    ingester.domain_name,
                                    err
                                ),
                                status: StatusCode::INTERNAL_SERVER_ERROR,
                            }
                        })?;

                if !res.status().is_success() {
                    log::error!(
                            "failed to forward create stream request to ingestor: {}\nResponse Returned: {:?}",
                            ingester.domain_name,res
                        );
                    return Err(StreamError::Custom {
                            msg: format!(
                                "failed to forward create stream request to ingestor: {}\nResponse Returned: {:?}",
                                ingester.domain_name,res.text().await.unwrap_or_default()
                            ),
                            status: StatusCode::INTERNAL_SERVER_ERROR,
                        });
                }
            }
        }
        Ok(())
    }

    // BUG: If any ingestor is offline this will error out the entire request
    pub async fn fetch_stats_from_ingestors(stream_name: &str) -> Result<QuriedStats, StreamError> {
        // ? Lower cognitive load by moving the mode filter to the calling function
        let mut stats = Vec::new();

        let ingestor_infos = Self::get_ingestor_info().await.map_err(|err| {
            log::error!("Fatal: failed to get ingestor info: {:?}", err);
            StreamError::Custom {
                msg: format!("failed to get ingestor info\n{:?}", err),
                status: StatusCode::INTERNAL_SERVER_ERROR,
            }
        })?;

        for ingester in ingestor_infos {
            let url = format!(
                "{}{}/logstream/{}/stats",
                ingester.domain_name.to_string().trim_end_matches('/'),
                base_path(),
                stream_name
            );

            let client = reqwest::Client::new();
            let res = client
                .get(&url)
                .header("Content-Type", "application/json")
                .header("Authorization", ingester.token)
                .send()
                .await
                .map_err(|err| {
                    log::error!(
                        "Fatal: failed to fetch stats from ingestor: {}\n Error: {:?}",
                        ingester.domain_name,
                        err
                    );

                    StreamError::Custom {
                        msg: format!(
                            "failed to fetch stats from ingestor: {}\n Error: {:?}",
                            ingester.domain_name, err
                        ),
                        status: StatusCode::INTERNAL_SERVER_ERROR,
                    }
                })?;

            if !res.status().is_success() {
                log::error!(
                    "failed to forward create stream request to ingestor: {}\nResponse Returned: {:?}",
                    ingester.domain_name,res
                );
                return Err(StreamError::Custom {
                    msg: format!(
                        "failed to forward create stream request to ingestor: {}\nResponse Returned: {:?}",
                        ingester.domain_name,res.text().await.unwrap_or_default()
                    ),
                    status: StatusCode::INTERNAL_SERVER_ERROR,
                });
            }

            match serde_json::from_str::<Vec<QuriedStats>>(&res.text().await.unwrap()) {
                Ok(mut stat) => stats.append(&mut stat),
                Err(err) => {
                    log::error!(
                        "Could not parse stats from ingestor: {}\n Error: {:?}",
                        ingester.domain_name,
                        err
                    );
                    continue;
                }
            }
        }
        let stats = Self::merge_quried_stats(stats);
        Ok(stats)
    }

    pub fn merge_quried_stats(stats: Vec<QuriedStats>) -> QuriedStats {
        let min_creation_time = stats
            .iter()
            .map(|x| x.creation_time.parse::<DateTime<Utc>>().unwrap())
            .min()
            .unwrap_or_default();
        let stream_name = stats[0].stream.clone();
        let min_first_event_at = stats
            .iter()
            .map(|x| match x.first_event_at.as_ref() {
                Some(fea) => fea.parse::<DateTime<Utc>>().unwrap_or_default(),
                None => Utc::now(),
            })
            .min()
            .unwrap_or(Utc::now());

        let min_time = stats.iter().map(|x| x.time).min().unwrap_or(Utc::now());

        let cumulative_ingestion =
            stats
                .iter()
                .map(|x| &x.ingestion)
                .fold(IngestionStats::default(), |acc, x| IngestionStats {
                    count: acc.count + x.count,
                    size: format!(
                        "{}",
                        acc.size.split(' ').collect_vec()[0]
                            .parse::<u64>()
                            .unwrap_or_default()
                            + x.size.split(' ').collect_vec()[0]
                                .parse::<u64>()
                                .unwrap_or_default()
                    ),
                    format: x.format.clone(),
                });

        let cumulative_storage =
            stats
                .iter()
                .map(|x| &x.storage)
                .fold(StorageStats::default(), |acc, x| StorageStats {
                    size: format!(
                        "{}",
                        acc.size.split(' ').collect_vec()[0]
                            .parse::<u64>()
                            .unwrap_or_default()
                            + x.size.split(' ').collect_vec()[0]
                                .parse::<u64>()
                                .unwrap_or_default()
                    ),
                    format: x.format.clone(),
                });

        QuriedStats::new(
            &stream_name,
            &min_creation_time.to_string(),
            Some(min_first_event_at.to_string()),
            min_time,
            cumulative_ingestion,
            cumulative_storage,
        )
    }
}

#[derive(Debug, Default, Serialize, Deserialize)]
pub struct QuriedStats {
    pub stream: String,
    pub creation_time: String,
    pub first_event_at: Option<String>,
    pub time: DateTime<Utc>,
    pub ingestion: IngestionStats,
    pub storage: StorageStats,
}

impl QuriedStats {
    pub fn new(
        stream: &str,
        creation_time: &str,
        first_event_at: Option<String>,
        time: DateTime<Utc>,
        ingestion: IngestionStats,
        storage: StorageStats,
    ) -> Self {
        Self {
            stream: stream.to_string(),
            creation_time: creation_time.to_string(),
            first_event_at,
            time,
            ingestion,
            storage,
        }
    }
}

#[derive(Debug, Default, Serialize, Deserialize)]
pub struct IngestionStats {
    pub count: u64,
    pub size: String,
    pub format: String,
}

impl IngestionStats {
    pub fn new(count: u64, size: String, format: &str) -> Self {
        Self {
            count,
            size,
            format: format.to_string(),
        }
    }
}

#[derive(Debug, Default, Serialize, Deserialize)]
pub struct StorageStats {
    size: String,
    format: String,
}

impl StorageStats {
    pub fn new(size: String, format: &str) -> Self {
        Self {
            size,
            format: format.to_string(),
        }
    }
}
