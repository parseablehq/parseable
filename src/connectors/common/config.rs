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

use crate::connectors::common::types::BadData;
use crate::connectors::common::types::ConnectorType;
use crate::connectors::common::ConnectorError;
use crate::connectors::kafka::config::{KafkaConfig, SourceOffset};
use serde::{Deserialize, Serialize};
use std::{time::Duration, vec};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ConnectorConfig {
    // Basic Configuration
    pub connector_type: ConnectorType,
    pub name: String,

    // Performance Configuration
    pub buffer_size: usize,
    pub buffer_timeout: Duration,

    // Topic/Stream Configuration
    pub topics: Vec<String>,
    pub offset_mode: SourceOffset,

    // Error Handling
    pub bad_data: Option<BadData>,
    pub max_retries: u32,
    pub retry_interval: Duration,

    // Kafka-specific Configuration
    #[serde(skip_serializing_if = "Option::is_none")]
    pub kafka_config: Option<KafkaConfig>,

    // Monitoring
    pub metrics_enabled: bool,
    pub instance_id: String,
}

impl Default for ConnectorConfig {
    fn default() -> Self {
        Self {
            connector_type: ConnectorType::KafkaSource,
            name: String::from("parseable-connectors"),
            buffer_size: 10000,
            buffer_timeout: Duration::from_millis(500),
            topics: vec![],
            offset_mode: SourceOffset::Earliest,
            bad_data: None,
            max_retries: 3,
            retry_interval: Duration::from_secs(5),
            kafka_config: Some(KafkaConfig::default()),
            metrics_enabled: true,
            instance_id: String::from("parseable-connectors"),
        }
    }
}

impl ConnectorConfig {
    pub fn builder() -> ConnectorConfigBuilder {
        ConnectorConfigBuilder::default()
    }

    pub fn validate(&self) -> anyhow::Result<(), ConnectorError> {
        if self.buffer_size == 0 {
            return Err(ConnectorError::Config("Buffer size must be > 0".into()));
        }

        if let Some(kafka_config) = &self.kafka_config {
            self.validate_kafka_config(kafka_config)?;
        }

        Ok(())
    }

    fn validate_kafka_config(&self, config: &KafkaConfig) -> Result<(), ConnectorError> {
        if config.bootstrap_servers.is_empty() {
            return Err(ConnectorError::Config("Bootstrap servers required".into()));
        }

        if config.topics().is_empty() {
            return Err(ConnectorError::Config("Topic name required".into()));
        }

        Ok(())
    }
}

#[derive(Default)]
pub struct ConnectorConfigBuilder {
    config: ConnectorConfig,
}

impl ConnectorConfigBuilder {
    pub fn connector_type(mut self, connector_type: ConnectorType) -> Self {
        self.config.connector_type = connector_type;
        self
    }

    pub fn name(mut self, name: impl Into<String>) -> Self {
        self.config.name = name.into();
        self
    }

    pub fn buffer_size(mut self, buffer_size: usize) -> Self {
        self.config.buffer_size = buffer_size;
        self
    }

    pub fn buffer_timeout(mut self, buffer_timeout: Duration) -> Self {
        self.config.buffer_timeout = buffer_timeout;
        self
    }

    pub fn max_retries(mut self, max_retries: u32) -> Self {
        self.config.max_retries = max_retries;
        self
    }

    pub fn instance_id(mut self, instance_id: String) -> Self {
        self.config.instance_id = instance_id;
        self
    }

    pub fn retry_interval(mut self, retry_interval: Duration) -> Self {
        self.config.retry_interval = retry_interval;
        self
    }

    pub fn metrics_enabled(mut self, metrics_enabled: bool) -> Self {
        self.config.metrics_enabled = metrics_enabled;
        self
    }

    pub fn topic(mut self, topics: Vec<String>) -> Self {
        self.config.topics = topics;
        self
    }

    pub fn offset_mode(mut self, offset_mode: SourceOffset) -> Self {
        self.config.offset_mode = offset_mode;
        self
    }

    pub fn kafka_config(mut self, kafka_config: KafkaConfig) -> Self {
        self.config.kafka_config = Some(kafka_config);
        self
    }

    pub fn build(self) -> anyhow::Result<ConnectorConfig> {
        let config = self.config;
        config.validate()?;
        Ok(config)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn test_config_validation() {
        let result = ConnectorConfig::builder()
            .connector_type(ConnectorType::KafkaSource)
            .buffer_size(0)
            .build();

        assert!(result.is_err());
    }
}
