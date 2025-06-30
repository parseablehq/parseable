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

use clap::ValueEnum;
use rdkafka::error::{KafkaError, RDKafkaErrorCode};
use std::str::FromStr;
use thiserror::Error;
use tokio::runtime;
use tokio::runtime::Builder;

pub mod processor;
pub mod shutdown;

#[derive(Debug, Error)]
pub enum ConnectorError {
    #[error("Kafka error: {0}")]
    Kafka(KafkaError),

    #[error("Connection error: {0}")]
    Connection(String),

    #[error("Fatal error: {0}")]
    Fatal(String),

    #[error("Processing error: {0}")]
    Processing(#[from] anyhow::Error),

    #[error("State error: {0}")]
    State(String),

    #[error("Authentication error: {0}")]
    Auth(String),
}

impl From<KafkaError> for ConnectorError {
    fn from(error: KafkaError) -> Self {
        if let Some(code) = error.rdkafka_error_code() {
            match code {
                RDKafkaErrorCode::BrokerTransportFailure
                | RDKafkaErrorCode::NetworkException
                | RDKafkaErrorCode::AllBrokersDown => ConnectorError::Connection(error.to_string()),

                RDKafkaErrorCode::Fatal | RDKafkaErrorCode::CriticalSystemResource => {
                    ConnectorError::Fatal(error.to_string())
                }

                RDKafkaErrorCode::Authentication | RDKafkaErrorCode::SaslAuthenticationFailed => {
                    ConnectorError::Auth(error.to_string())
                }

                _ => ConnectorError::Kafka(error),
            }
        } else {
            ConnectorError::Kafka(error)
        }
    }
}
impl ConnectorError {
    pub fn is_fatal(&self) -> bool {
        matches!(
            self,
            ConnectorError::Fatal(_) | ConnectorError::Auth(_) | ConnectorError::State(_)
        )
    }
}

#[derive(ValueEnum, Default, Clone, Debug, PartialEq, Eq, Hash)]
pub enum BadData {
    #[default]
    Fail,
    Drop,
    Dlt, //TODO: Implement Dead Letter Topic support when needed
}

impl FromStr for BadData {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_lowercase().as_str() {
            "drop" => Ok(BadData::Drop),
            "fail" => Ok(BadData::Fail),
            "dlt" => Ok(BadData::Dlt),
            _ => Err(format!("Invalid bad data policy: {s}")),
        }
    }
}

pub fn build_runtime(worker_threads: usize, thread_name: &str) -> anyhow::Result<runtime::Runtime> {
    Builder::new_multi_thread()
        .enable_all()
        .thread_name(thread_name)
        .worker_threads(worker_threads)
        .max_blocking_threads(worker_threads)
        .build()
        .map_err(|e| anyhow::anyhow!(e))
}
