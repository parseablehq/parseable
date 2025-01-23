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
 *
 */

use crate::{
    handlers::http::modal::{IngestorMetadata, DEFAULT_VERSION},
    option::Config,
    utils::{get_ingestor_id, get_url},
};
use anyhow::anyhow;
use arrow_schema::ArrowError;
use base64::Engine;
use once_cell::sync::Lazy;
use parquet::errors::ParquetError;
use serde_json::Value as JsonValue;
pub use streams::convert_disk_files_to_parquet;
pub use streams::{Stream, Streams};
use tracing::{error, info};
pub use writer::StreamWriterError;

mod streams;
mod writer;

#[derive(Debug, thiserror::Error)]
pub enum MoveDataError {
    #[error("Unable to create recordbatch stream")]
    Arrow(#[from] ArrowError),
    #[error("Could not generate parquet file")]
    Parquet(#[from] ParquetError),
    #[error("IO Error {0}")]
    ObjectStorage(#[from] std::io::Error),
    #[error("Could not generate parquet file")]
    Create,
}

/// Staging is made up of multiple streams, each stream's context is housed in a single `Stream` object.
/// `STAGING` is a globally shared mapping of `Streams` that are in staging.
pub static STAGING: Lazy<Streams> = Lazy::new(Streams::default);

pub fn get_ingestor_info(config: &Config) -> anyhow::Result<IngestorMetadata> {
    // all the files should be in the staging directory root
    let entries = std::fs::read_dir(&config.options.local_staging_path)?;
    let url = get_url();
    let port = url.port().unwrap_or(80).to_string();
    let url = url.to_string();

    for entry in entries {
        // cause the staging directory will have only one file with ingestor in the name
        // so the JSON Parse should not error unless the file is corrupted
        let path = entry?.path();
        let flag = path
            .file_name()
            .unwrap_or_default()
            .to_str()
            .unwrap_or_default()
            .contains("ingestor");

        if flag {
            // get the ingestor metadata from staging
            let mut meta: JsonValue = serde_json::from_slice(&std::fs::read(path)?)?;

            // migrate the staging meta
            let obj = meta
                .as_object_mut()
                .ok_or_else(|| anyhow!("Could Not parse Ingestor Metadata Json"))?;

            if obj.get("flight_port").is_none() {
                obj.insert(
                    "flight_port".to_owned(),
                    JsonValue::String(config.options.flight_port.to_string()),
                );
            }

            let mut meta: IngestorMetadata = serde_json::from_value(meta)?;

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
                config.options.username, config.options.password
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

            put_ingestor_info(config, meta.clone())?;
            return Ok(meta);
        }
    }

    let store = config.storage().get_object_store();
    let out = IngestorMetadata::new(
        port,
        url,
        DEFAULT_VERSION.to_string(),
        store.get_bucket_name(),
        &config.options.username,
        &config.options.password,
        get_ingestor_id(),
        config.options.flight_port.to_string(),
    );

    put_ingestor_info(config, out.clone())?;
    Ok(out)
}

/// Puts the ingestor info into the staging.
///
/// This function takes the ingestor info as a parameter and stores it in staging.
/// # Parameters
///
/// * `ingestor_info`: The ingestor info to be stored.
pub fn put_ingestor_info(config: &Config, info: IngestorMetadata) -> anyhow::Result<()> {
    let file_name = format!("ingestor.{}.json", info.ingestor_id);
    let file_path = config.options.local_staging_path.join(file_name);

    std::fs::write(file_path, serde_json::to_vec(&info)?)?;

    Ok(())
}
