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

use anyhow::bail;
use rdkafka::Offset;
use rustls::pki_types::{CertificateDer, PrivateKeyDer};
use rustls_pemfile::{self, Item};
use serde::{Deserialize, Serialize};
use std::fmt::Display;
use std::io::BufReader;
use std::sync::Arc;
use tracing::{debug, info};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct KafkaConfig {
    // Common configuration
    pub bootstrap_servers: String,
    topics: Vec<String>,
    pub client_id: Option<String>,

    // Component-specific configurations
    #[serde(skip_serializing_if = "Option::is_none")]
    pub consumer: Option<ConsumerConfig>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub producer: Option<ProducerConfig>,

    // Security and advanced settings
    #[serde(skip_serializing_if = "Option::is_none")]
    pub security: Option<SecurityConfig>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ConsumerConfig {
    // Consumer group configuration
    pub group_id: String,
    pub group_instance_id: Option<String>,
    pub partition_assignment_strategy: String,

    // Session handling
    pub session_timeout_ms: u32,
    pub heartbeat_interval_ms: u32,
    pub max_poll_interval_ms: u32,

    // Offset management
    pub enable_auto_commit: bool,
    pub auto_commit_interval_ms: u32,
    pub enable_auto_offset_store: bool,
    pub auto_offset_reset: String,

    // Fetch configuration
    pub fetch_min_bytes: u32,
    pub fetch_max_bytes: u32,
    pub fetch_max_wait_ms: u32,
    pub max_partition_fetch_bytes: u32,

    // Queue configuration
    pub queued_min_messages: u32,
    pub queued_max_messages_kbytes: u32,

    // Processing configuration
    pub enable_partition_eof: bool,
    pub check_crcs: bool,
    pub isolation_level: String,
    pub fetch_message_max_bytes: String,
    pub stats_interval_ms: Option<u64>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ProducerConfig {
    pub acks: String,
    pub compression_type: String,
    pub batch_size: u32,
    pub linger_ms: u32,
    pub delivery_timeout_ms: u32,
    pub max_in_flight_requests_per_connection: u32,
    pub max_request_size: u32,
    pub enable_idempotence: bool,
    pub transaction_timeout_ms: Option<u32>,
    pub queue_buffering_max_messages: u32,
    queue_buffering_max_ms: u32,
    retry_backoff_ms: u32,
    batch_num_messages: u32,
    retries: u32,
}

impl Default for ConsumerConfig {
    fn default() -> Self {
        Self {
            group_id: "default-cg".to_string(),
            group_instance_id: Some("default-cg-ii".to_string()),
            // NOTE: cooperative-sticky does not work well in rdkafka when using manual commit.
            // @see https://github.com/confluentinc/librdkafka/issues/4629
            // @see https://github.com/confluentinc/librdkafka/issues/4368
            partition_assignment_strategy: "roundrobin,range".to_string(),
            session_timeout_ms: 60000,
            heartbeat_interval_ms: 3000,
            max_poll_interval_ms: 300000,
            enable_auto_commit: false,
            auto_commit_interval_ms: 5000,
            enable_auto_offset_store: true,
            auto_offset_reset: "earliest".to_string(),
            fetch_min_bytes: 1,
            fetch_max_bytes: 52428800,
            fetch_max_wait_ms: 500,
            max_partition_fetch_bytes: 1048576,
            queued_min_messages: 100000,
            queued_max_messages_kbytes: 65536,
            enable_partition_eof: false,
            check_crcs: false,
            isolation_level: "read_committed".to_string(),
            fetch_message_max_bytes: "1048576".to_string(),
            stats_interval_ms: Some(10000),
        }
    }
}

impl Default for ProducerConfig {
    fn default() -> Self {
        Self {
            acks: "all".to_string(),
            compression_type: "lz4".to_string(),
            batch_size: 16384,           // 16KB default batch size
            linger_ms: 5,                // Small latency for better batching
            delivery_timeout_ms: 120000, // 2 minute delivery timeout
            max_in_flight_requests_per_connection: 5,
            max_request_size: 1048576,            // 1MB max request size
            enable_idempotence: true,             // Ensure exactly-once delivery
            transaction_timeout_ms: Some(60000),  // 1 minute transaction timeout
            queue_buffering_max_messages: 100000, // Producer queue size
            queue_buffering_max_ms: 100,          // Max time to wait before sending
            retry_backoff_ms: 100,                // Backoff time between retries
            batch_num_messages: 10000,            // Messages per batch
            retries: 3,                           // Number of retries
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[warn(non_camel_case_types)]
#[serde(rename_all = "UPPERCASE")]
#[allow(non_camel_case_types)]
pub enum SecurityProtocol {
    Plaintext,
    SSL,
    SASL_SSL,
    SASL_PLAINTEXT,
}

impl Display for SecurityProtocol {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let str = match self {
            SecurityProtocol::Plaintext => "PLAINTEXT",
            SecurityProtocol::SSL => "SSL",
            SecurityProtocol::SASL_SSL => "SASL_SSL",
            SecurityProtocol::SASL_PLAINTEXT => "SASL_PLAINTEXT",
        }
        .to_string();
        write!(f, "{}", str)
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SecurityConfig {
    pub protocol: SecurityProtocol,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub ssl_config: Option<SSLConfig>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub sasl_config: Option<SASLConfig>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SSLConfig {
    pub ca_certificate_pem: String,
    pub client_certificate_pem: String,
    pub client_key_pem: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "UPPERCASE")]
pub enum SASLMechanism {
    Plain,
    ScramSha256,
    ScramSha512,
    GssAPI,
}

impl Display for SASLMechanism {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let str = match self {
            SASLMechanism::Plain => "PLAIN",
            SASLMechanism::ScramSha256 => "SCRAM-SHA-256",
            SASLMechanism::ScramSha512 => "SCRAM-SHA-512",
            SASLMechanism::GssAPI => "GSSAPI",
        }
        .to_string();
        write!(f, "{}", str)
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SASLConfig {
    pub mechanism: SASLMechanism,
    pub username: String,
    pub password: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub kerberos_service_name: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub kerberos_principal: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub kerberos_keytab: Option<String>,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum SourceOffset {
    Earliest,
    Latest,
    Group,
}

impl SourceOffset {
    pub fn get_offset(&self) -> Offset {
        match self {
            SourceOffset::Earliest => Offset::Beginning,
            SourceOffset::Latest => Offset::End,
            SourceOffset::Group => Offset::Stored,
        }
    }
}

impl KafkaConfig {
    pub fn new(
        bootstrap_servers: String,
        topics: Vec<String>,
        consumer_config: Option<ConsumerConfig>,
    ) -> Self {
        Self {
            bootstrap_servers,
            topics,
            client_id: None,
            consumer: consumer_config,
            producer: None,
            security: None,
        }
    }

    pub fn consumer_config(&self) -> rdkafka::ClientConfig {
        let mut config = rdkafka::ClientConfig::new();
        config
            .set("bootstrap.servers", &self.bootstrap_servers)
            .set("reconnect.backoff.ms", "100")
            .set("reconnect.backoff.max.ms", "3600000");

        if let Some(client_id) = &self.client_id {
            config
                .set("client.id", format!("parseable-{}-ci", client_id))
                .set("client.rack", format!("parseable-{}-cr", client_id));
        }

        if let Some(consumer) = &self.consumer {
            let enable_auto_commit = consumer.enable_auto_commit.to_string();
            let group_id = format!("parseable-{}-gi", &consumer.group_id);
            info!("Setting group.id to {}", group_id);
            config
                .set("group.id", group_id)
                .set("log_level", "7")
                .set("enable.auto.commit", enable_auto_commit)
                .set(
                    "enable.auto.offset.store",
                    consumer.enable_auto_offset_store.to_string(),
                )
                .set("auto.offset.reset", &consumer.auto_offset_reset)
                .set(
                    "partition.assignment.strategy",
                    &consumer.partition_assignment_strategy,
                )
                .set(
                    "session.timeout.ms",
                    consumer.session_timeout_ms.to_string(),
                )
                .set(
                    "heartbeat.interval.ms",
                    consumer.heartbeat_interval_ms.to_string(),
                )
                .set(
                    "max.poll.interval.ms",
                    consumer.max_poll_interval_ms.to_string(),
                )
                .set("fetch.min.bytes", consumer.fetch_min_bytes.to_string())
                .set("fetch.max.bytes", consumer.fetch_max_bytes.to_string())
                .set(
                    "fetch.message.max.bytes",
                    consumer.fetch_message_max_bytes.to_string(),
                )
                .set(
                    "max.partition.fetch.bytes",
                    consumer.max_partition_fetch_bytes.to_string(),
                )
                .set(
                    "queued.min.messages",
                    consumer.queued_min_messages.to_string(),
                )
                .set(
                    "queued.max.messages.kbytes",
                    consumer.queued_max_messages_kbytes.to_string(),
                )
                .set(
                    "enable.partition.eof",
                    consumer.enable_partition_eof.to_string(),
                )
                .set("isolation.level", &consumer.isolation_level)
                .set(
                    "statistics.interval.ms",
                    consumer.stats_interval_ms.unwrap_or(10000).to_string(),
                );

            if let Some(instance_id) = &consumer.group_instance_id {
                config.set("group.instance.id", instance_id);
            }
        }

        self.apply_security_config(&mut config);

        info!("Consumer configuration: {:?}", config);
        config
    }

    pub fn producer_config(&self) -> rdkafka::config::ClientConfig {
        let mut config = rdkafka::config::ClientConfig::new();
        config
            .set("bootstrap.servers", &self.bootstrap_servers)
            .set("reconnect.backoff.ms", "100")
            .set("reconnect.backoff.max.ms", "3600000");

        if let Some(client_id) = &self.client_id {
            config
                .set("client.id", format!("parseable-{}-ci", client_id))
                .set("client.rack", format!("parseable-{}-cr", client_id));
        }

        if let Some(producer_config) = &self.producer {
            config
                .set("acks", &producer_config.acks)
                .set("compression.type", &producer_config.compression_type)
                .set("batch.size", producer_config.batch_size.to_string())
                .set("linger.ms", producer_config.linger_ms.to_string())
                .set(
                    "delivery.timeout.ms",
                    producer_config.delivery_timeout_ms.to_string(),
                )
                .set(
                    "max.in.flight.requests.per.connection",
                    producer_config
                        .max_in_flight_requests_per_connection
                        .to_string(),
                )
                .set(
                    "max.request.size",
                    producer_config.max_request_size.to_string(),
                )
                .set(
                    "enable.idempotence",
                    producer_config.enable_idempotence.to_string(),
                )
                .set(
                    "batch.num.messages",
                    producer_config.batch_num_messages.to_string(),
                )
                .set(
                    "queue.buffering.max.messages",
                    producer_config.queue_buffering_max_messages.to_string(),
                )
                .set(
                    "queue.buffering.max.ms",
                    producer_config.queue_buffering_max_ms.to_string(),
                )
                .set(
                    "retry.backoff.ms",
                    producer_config.retry_backoff_ms.to_string(),
                )
                .set("retries", producer_config.retries.to_string());

            if let Some(timeout) = producer_config.transaction_timeout_ms {
                config.set("transaction.timeout.ms", timeout.to_string());
            }
        }

        self.apply_security_config(&mut config);

        config
    }

    fn apply_security_config(&self, config: &mut rdkafka::ClientConfig) {
        let security = match &self.security {
            Some(sec) => sec,
            None => {
                debug!("No security configuration provided, using PLAINTEXT");
                config.set("security.protocol", "plaintext");
                return;
            }
        };

        config.set(
            "security.protocol",
            security.protocol.to_string().to_lowercase(),
        );

        if matches!(
            security.protocol,
            SecurityProtocol::SSL | SecurityProtocol::SASL_SSL
        ) {
            if let Some(ssl) = &security.ssl_config {
                debug!("Applying SSL configuration");
                config
                    .set("ssl.ca.pem", &ssl.ca_certificate_pem)
                    .set("ssl.certificate.pem", &ssl.client_certificate_pem)
                    .set("ssl.key.pem", &ssl.client_key_pem);
            } else {
                panic!(
                    "SSL configuration required for {:?} protocol",
                    security.protocol
                );
            }
        }

        if matches!(
            security.protocol,
            SecurityProtocol::SASL_SSL | SecurityProtocol::SASL_PLAINTEXT
        ) {
            if let Some(sasl) = &security.sasl_config {
                debug!(
                    "Applying SASL configuration with mechanism: {}",
                    sasl.mechanism.to_string()
                );
                config
                    .set("sasl.mechanism", sasl.mechanism.to_string())
                    .set("sasl.username", &sasl.username)
                    .set("sasl.password", &sasl.password);

                // Apply Kerberos-specific configuration if using GSSAPI
                if matches!(sasl.mechanism, SASLMechanism::GssAPI) {
                    if let Some(service_name) = &sasl.kerberos_service_name {
                        config.set("sasl.kerberos.service.name", service_name);
                    }
                    if let Some(principal) = &sasl.kerberos_principal {
                        config.set("sasl.kerberos.principal", principal);
                    }
                    if let Some(keytab) = &sasl.kerberos_keytab {
                        config.set("sasl.kerberos.keytab", keytab);
                    }
                }
            } else {
                panic!(
                    "SASL configuration required for {:?} protocol",
                    security.protocol
                );
            }
        }
    }
}
impl Default for KafkaConfig {
    fn default() -> Self {
        Self {
            // Common configuration with standard broker port
            bootstrap_servers: "localhost:9092".to_string(),
            topics: vec![],
            client_id: Some("parseable-connect".to_string()),

            // Component-specific configurations with production-ready defaults
            consumer: Some(ConsumerConfig::default()),
            producer: Some(ProducerConfig::default()),

            // Security defaults to plaintext for development
            // Production environments should explicitly configure security
            security: Some(SecurityConfig {
                protocol: SecurityProtocol::Plaintext,
                ssl_config: None,
                sasl_config: None,
            }),
        }
    }
}

impl KafkaConfig {
    pub fn builder() -> KafkaConfigBuilder {
        KafkaConfigBuilder::default()
    }

    pub fn topics(&self) -> Vec<&str> {
        self.topics.iter().map(|s| s.as_str()).collect()
    }
}

#[derive(Default, Debug)]
pub struct KafkaConfigBuilder {
    config: KafkaConfig,
}

impl KafkaConfigBuilder {
    pub fn bootstrap_servers(mut self, servers: impl Into<String>) -> Self {
        self.config.bootstrap_servers = servers.into();
        self
    }

    pub fn topic(mut self, topics: Vec<String>) -> Self {
        self.config.topics = topics;
        self
    }

    pub fn client_id(mut self, client_id: impl Into<String>) -> Self {
        self.config.client_id = Some(client_id.into());
        self
    }

    pub fn with_consumer(mut self, consumer: ConsumerConfig) -> Self {
        self.config.consumer = Some(consumer);
        self
    }

    pub fn with_producer(mut self, producer: ProducerConfig) -> Self {
        self.config.producer = Some(producer);
        self
    }

    pub fn with_security(mut self, security: SecurityConfig) -> Self {
        self.config.security = Some(security);
        self
    }

    pub fn build(self) -> anyhow::Result<KafkaConfig> {
        let config = self.config;

        if config.bootstrap_servers.is_empty() {
            anyhow::bail!("bootstrap_servers cannot be empty");
        }

        Ok(config)
    }
}

#[derive(Debug, Clone)]
#[allow(dead_code)]
pub struct KafkaCertificates {
    ca_certificate: Arc<CertificateDer<'static>>,
    client_certificate: Arc<CertificateDer<'static>>,
    client_key: Arc<PrivateKeyDer<'static>>,
}

#[allow(dead_code)]
fn parse_first_certificate(pem: &str) -> anyhow::Result<CertificateDer<'static>> {
    let mut reader = BufReader::new(pem.as_bytes());
    let items = rustls_pemfile::read_all(&mut reader);

    for item in items.flatten() {
        if let Item::X509Certificate(cert_data) = item {
            return Ok(cert_data);
        }
    }
    bail!("No certificate found in PEM")
}

#[allow(dead_code)]
fn parse_first_private_key(pem: &str) -> anyhow::Result<PrivateKeyDer<'static>> {
    let mut reader = BufReader::new(pem.as_bytes());
    let items = rustls_pemfile::read_all(&mut reader);

    for item in items {
        if let Ok(Item::Pkcs1Key(key_data)) = item {
            return Ok(key_data.into());
        }
        if let Ok(Item::Pkcs8Key(key_data)) = item {
            return Ok(key_data.into());
        }
        if let Ok(Item::Sec1Key(key_data)) = item {
            return Ok(key_data.into());
        }
    }

    bail!("No private key found in PEM")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_consumer_config() {
        let consumer_config = ConsumerConfig {
            group_id: "test-group".to_string(),
            partition_assignment_strategy: "cooperative-sticky".to_string(),
            ..ConsumerConfig::default()
        };

        let config = KafkaConfig::new(
            "localhost:9092".to_string(),
            vec!["test-topic".to_string()],
            Some(consumer_config),
        );

        let rdkafka_config = config.consumer_config();
        assert_eq!(
            rdkafka_config.get("group.id"),
            Some("parseable-test-group-gi")
        );
        assert_eq!(
            rdkafka_config.get("partition.assignment.strategy"),
            Some("cooperative-sticky")
        );
    }

    #[test]
    fn test_default_kafka_config() {
        let config = KafkaConfig::default();
        assert_eq!(config.bootstrap_servers, "localhost:9092");
        assert!(config.topics.is_empty());
        assert!(config.consumer.is_some());
        assert!(config.producer.is_some());

        if let Some(producer) = config.producer {
            assert_eq!(producer.acks, "all");
            assert!(producer.enable_idempotence);
            assert_eq!(producer.compression_type, "lz4");
        }
    }

    #[test]
    fn test_kafka_config_builder() {
        let config = KafkaConfig::builder()
            .bootstrap_servers("kafka1:9092,kafka2:9092")
            .topic(vec!["test-topic".to_string()])
            .client_id("test-client")
            .build()
            .unwrap();

        assert_eq!(config.bootstrap_servers, "kafka1:9092,kafka2:9092");
        assert_eq!(config.topics.first().unwrap(), "test-topic");
        assert_eq!(config.client_id, Some("test-client".to_string()));
    }
}
