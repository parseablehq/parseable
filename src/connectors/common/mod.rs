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
use rdkafka::error::{KafkaError, RDKafkaErrorCode};
use thiserror::Error;

pub mod config;
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
