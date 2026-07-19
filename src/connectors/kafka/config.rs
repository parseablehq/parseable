/*
 * Parseable Server (C) 2022 - 2025 Parseable, Inc.
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

use crate::connectors::common::BadData;
use clap::{Args, Parser, ValueEnum};
use rdkafka::{ClientConfig, Offset};
use serde::{Deserialize, Serialize};
use std::path::PathBuf;
use std::time::Duration;

/// Standard AWS environment variables consulted when resolving the region for
/// MSK IAM authentication. Kept as named constants to avoid typo-prone string
/// literals in `std::env` calls.
const AWS_REGION_ENV: &str = "AWS_REGION";
const AWS_DEFAULT_REGION_ENV: &str = "AWS_DEFAULT_REGION";

#[derive(Debug, Clone, Parser)]
pub struct KafkaConfig {
    #[arg(
        long = "bootstrap-servers",
        env = "P_KAFKA_BOOTSTRAP_SERVERS",
        value_name = "bootstrap-servers",
        required = false,
        help = "Comma-separated list of Kafka bootstrap servers"
    )]
    pub bootstrap_servers: Option<String>,

    #[arg(
        long = "client-id",
        env = "P_KAFKA_CLIENT_ID",
        required = false,
        default_value_t = String::from("parseable-connect"),
        value_name = "client_id",
        help = "Client ID for Kafka connection"
    )]
    pub client_id: String,

    #[arg(
        long = "partition-listener-concurrency",
        env = "P_KAFKA_PARTITION_LISTENER_CONCURRENCY",
        value_name = "concurrency",
        required = false,
        default_value_t = 2,
        help = "Number of parallel threads for Kafka partition listeners. Each partition gets processed on a dedicated thread."
    )]
    pub partition_listener_concurrency: usize,

    #[command(flatten)]
    pub consumer: Option<ConsumerConfig>,

    #[command(flatten)]
    pub producer: Option<ProducerConfig>,

    #[command(flatten)]
    pub security: Option<SecurityConfig>,

    #[arg(
        value_enum,
        long = "bad-data-policy",
        required = false,
        default_value_t = BadData::Fail,
        env = "P_CONNECTOR_BAD_DATA_POLICY",
        help = "Policy for handling bad data"
    )]
    pub bad_data: BadData,
}

#[derive(Debug, Clone, Args)]
pub struct ConsumerConfig {
    #[arg(
        long = "consumer-topics",
        env = "P_KAFKA_CONSUMER_TOPICS",
        value_name = "consumer-topics",
        required = false,
        value_delimiter = ',',
        help = "Comma-separated list of topics"
    )]
    pub topics: Vec<String>,

    #[arg(
        long = "consumer-group-id",
        env = "P_KAFKA_CONSUMER_GROUP_ID",
        value_name = "id",
        required = false,
        default_value_t = String::from("parseable-connect-cg"),
        help = "Consumer group ID"
    )]
    pub group_id: String,

    // uses per partition stream micro-batch buffer size
    #[arg(
        long = "buffer-size",
        env = "P_KAFKA_CONSUMER_BUFFER_SIZE",
        value_name = "size",
        required = false,
        default_value_t = 10000,
        help = "Size of the buffer for batching records"
    )]
    pub buffer_size: usize,

    // uses per partition stream micro-batch buffer timeout
    #[clap(
        value_parser = humantime::parse_duration,
        default_value= "10000ms",
        long = "buffer-timeout",
        env = "P_KAFKA_CONSUMER_BUFFER_TIMEOUT",
        value_name = "timeout_ms",
        required = false,
        help = "Timeout for buffer flush in milliseconds"
    )]
    pub buffer_timeout: Duration,

    #[arg(
        long = "consumer-group-instance-id",
        required = false,
        env = "P_KAFKA_CONSUMER_GROUP_INSTANCE_ID",
             default_value_t = format!("parseable-connect-cg-ii-{}", rand::random::<u8>()).to_string(),
        help = "Group instance ID for static membership"
    )]
    pub group_instance_id: String,

    #[arg(
        long = "consumer-partition-strategy",
        env = "P_KAFKA_CONSUMER_PARTITION_STRATEGY",
        required = false,
        default_value_t = String::from("roundrobin,range"),
        help = "Partition assignment strategy"
    )]
    pub partition_assignment_strategy: String,

    #[arg(
        long = "consumer-session-timeout",
        env = "P_KAFKA_CONSUMER_SESSION_TIMEOUT",
        required = false,
        default_value_t = 60000,
        help = "Session timeout in milliseconds"
    )]
    pub session_timeout_ms: u32,

    #[arg(
        long = "consumer-heartbeat-interval",
        env = "P_KAFKA_CONSUMER_HEARTBEAT_INTERVAL",
        required = false,
        default_value_t = 3000,
        help = "Heartbeat interval in milliseconds"
    )]
    pub heartbeat_interval_ms: u32,

    #[arg(
        long = "consumer-max-poll-interval",
        env = "P_KAFKA_CONSUMER_MAX_POLL_INTERVAL",
        required = false,
        default_value_t = 300000,
        help = "Maximum poll interval in milliseconds"
    )]
    pub max_poll_interval_ms: u32,

    #[arg(
        long = "consumer-enable-auto-offset-store",
        env = "P_KAFKA_CONSUMER_ENABLE_AUTO_OFFSET_STORE",
        required = false,
        default_value_t = true,
        help = "Enable auto offset store"
    )]
    pub enable_auto_offset_store: bool,

    #[clap(
        value_enum,
        long = "consumer-auto-offset-reset",
        required = false,
        env = "P_KAFKA_CONSUMER_AUTO_OFFSET_RESET",
        default_value_t = SourceOffset::Earliest,
        help = "Where to start consuming when no committed offset exists: earliest, latest, or group. group resumes from committed offsets and falls back to librdkafka's default (latest) for a brand-new consumer group"
    )]
    pub auto_offset_reset: SourceOffset,

    #[arg(
        long = "consumer-fetch-min-bytes",
        env = "P_KAFKA_CONSUMER_FETCH_MIN_BYTES",
        default_value_t = 1,
        required = false,
        help = "Minimum bytes to fetch"
    )]
    pub fetch_min_bytes: u32,

    #[arg(
        long = "consumer-fetch-max-bytes",
        env = "P_KAFKA_CONSUMER_FETCH_MAX_BYTES",
        default_value_t = 52428800,
        required = false,
        help = "Maximum bytes to fetch"
    )]
    pub fetch_max_bytes: u32,

    #[arg(
        long = "consumer-fetch-max-wait",
        env = "P_KAFKA_CONSUMER_FETCH_MAX_WAIT",
        default_value_t = 500,
        required = false,
        help = "Maximum wait time for fetch in milliseconds"
    )]
    pub fetch_max_wait_ms: u32,

    #[arg(
        long = "consumer-max-partition-fetch-bytes",
        env = "P_KAFKA_CONSUMER_MAX_PARTITION_FETCH_BYTES",
        required = false,
        default_value_t = 1048576,
        help = "Maximum bytes to fetch per partition"
    )]
    pub max_partition_fetch_bytes: u32,

    #[arg(
        long = "consumer-queued-min-messages",
        env = "P_KAFKA_CONSUMER_QUEUED_MIN_MESSAGES",
        required = false,
        default_value_t = 100000,
        help = "Minimum messages to queue"
    )]
    pub queued_min_messages: u32,

    #[arg(
        long = "consumer-queued-max-messages-kbytes",
        env = "P_KAFKA_CONSUMER_QUEUED_MAX_MESSAGES_KBYTES",
        required = false,
        default_value_t = 65536,
        help = "Maximum message queue size in KBytes"
    )]
    pub queued_max_messages_kbytes: u32,

    #[arg(
        long = "consumer-enable-partition-eof",
        env = "P_KAFKA_CONSUMER_ENABLE_PARTITION_EOF",
        required = false,
        default_value_t = false,
        help = "Enable partition EOF"
    )]
    pub enable_partition_eof: bool,

    #[arg(
        long = "consumer-check-crcs",
        env = "P_KAFKA_CONSUMER_CHECK_CRCS",
        required = false,
        default_value_t = false,
        help = "Check CRCs on messages"
    )]
    pub check_crcs: bool,

    #[arg(
        long = "consumer-isolation-level",
        env = "P_KAFKA_CONSUMER_ISOLATION_LEVEL",
        required = false,
        default_value_t = String::from("read_committed"),
        help = "Transaction isolation level"
    )]
    pub isolation_level: String,

    #[arg(
        long = "consumer-fetch-message-max-bytes",
        env = "P_KAFKA_CONSUMER_FETCH_MESSAGE_MAX_BYTES",
        required = false,
        default_value_t = 1048576,
        help = "Maximum bytes per message"
    )]
    pub fetch_message_max_bytes: u64,

    #[arg(
        long = "consumer-stats-interval",
        env = "P_KAFKA_CONSUMER_STATS_INTERVAL",
        required = false,
        default_value_t = 10000,
        help = "Statistics interval in milliseconds"
    )]
    pub stats_interval_ms: u64,
}

#[derive(Debug, Clone, Args)]
pub struct ProducerConfig {
    #[arg(
        long = "producer-acks",
        env = "P_KAFKA_PRODUCER_ACKS",
        required = false,
        default_value_t = String::from("all"),
        value_parser = ["0", "1", "all"],
        help = "Number of acknowledgments the producer requires"
    )]
    pub acks: String,

    #[arg(
        long = "producer-compression-type",
        env = "P_KAFKA_PRODUCER_COMPRESSION_TYPE",
        required = false,
        default_value_t= String::from("lz4"),
        value_parser = ["none", "gzip", "snappy", "lz4", "zstd"],
        help = "Compression type for messages"
    )]
    pub compression_type: String,

    #[arg(
        long = "producer-batch-size",
        env = "P_KAFKA_PRODUCER_BATCH_SIZE",
        required = false,
        default_value_t = 16384,
        help = "Maximum size of a request in bytes"
    )]
    pub batch_size: u32,

    #[arg(
        long = "producer-linger-ms",
        env = "P_KAFKA_PRODUCER_LINGER_MS",
        required = false,
        default_value_t = 5,
        help = "Delay to wait for more messages in the same batch"
    )]
    pub linger_ms: u32,

    #[arg(
        long = "producer-message-timeout-ms",
        env = "P_KAFKA_PRODUCER_MESSAGE_TIMEOUT_MS",
        required = false,
        default_value_t = 120000,
        help = "Local message timeout"
    )]
    pub message_timeout_ms: u32,

    #[arg(
        long = "producer-max-inflight",
        env = "P_KAFKA_PRODUCER_MAX_INFLIGHT",
        required = false,
        default_value_t = 5,
        help = "Maximum number of in-flight requests per connection"
    )]
    pub max_in_flight_requests_per_connection: u32,

    #[arg(
        long = "producer-message-max-bytes",
        env = "P_KAFKA_PRODUCER_MESSAGE_MAX_BYTES",
        required = false,
        default_value_t = 1048576,
        help = "Maximum size of a message in bytes"
    )]
    pub message_max_bytes: u32,

    #[arg(
        long = "producer-enable-idempotence",
        env = "P_KAFKA_PRODUCER_ENABLE_IDEMPOTENCE",
        required = false,
        default_value_t = true,
        help = "Enable idempotent producer"
    )]
    pub enable_idempotence: bool,

    #[arg(
        long = "producer-transaction-timeout-ms",
        env = "P_KAFKA_PRODUCER_TRANSACTION_TIMEOUT_MS",
        required = false,
        default_value_t = 60000,
        help = "Transaction timeout"
    )]
    pub transaction_timeout_ms: u64,

    #[arg(
        long = "producer-buffer-memory",
        env = "P_KAFKA_PRODUCER_BUFFER_MEMORY",
        required = false,
        default_value_t = 33554432,
        help = "Total bytes of memory the producer can use"
    )]
    pub buffer_memory: u32,

    #[arg(
        long = "producer-retry-backoff-ms",
        env = "P_KAFKA_PRODUCER_RETRY_BACKOFF_MS",
        required = false,
        default_value_t = 100,
        help = "Time to wait before retrying a failed request"
    )]
    pub retry_backoff_ms: u32,

    #[arg(
        long = "producer-request-timeout-ms",
        env = "P_KAFKA_PRODUCER_REQUEST_TIMEOUT_MS",
        required = false,
        default_value_t = 30000,
        help = "Time to wait for a response from brokers"
    )]
    pub request_timeout_ms: u32,

    #[arg(
        long = "producer-queue-buffering-max-messages",
        env = "P_KAFKA_PRODUCER_QUEUE_BUFFERING_MAX_MESSAGES",
        required = false,
        default_value_t = 100000,
        help = "Maximum number of messages allowed on the producer queue"
    )]
    pub queue_buffering_max_messages: u32,

    #[arg(
        long = "producer-queue-buffering-max-kbytes",
        env = "P_KAFKA_PRODUCER_QUEUE_BUFFERING_MAX_KBYTES",
        required = false,
        default_value_t = 1048576,
        help = "Maximum total message size sum allowed on the producer queue"
    )]
    pub queue_buffering_max_kbytes: u32,

    #[arg(
        long = "producer-delivery-timeout-ms",
        env = "P_KAFKA_PRODUCER_DELIVERY_TIMEOUT_MS",
        required = false,
        default_value_t = 120000,
        help = "Maximum time to report success or failure after send"
    )]
    pub delivery_timeout_ms: u32,

    #[arg(
        long = "producer-max-retries",
        env = "P_KAFKA_PRODUCER_MAX_RETRIES",
        required = false,
        default_value_t = 2147483647,
        help = "Maximum number of retries per message"
    )]
    pub max_retries: u32,

    #[arg(
        long = "producer-retry-backoff-max-ms",
        env = "P_KAFKA_PRODUCER_RETRY_BACKOFF_MAX_MS",
        required = false,
        default_value_t = 1000,
        help = "Maximum back-off time between retries"
    )]
    pub retry_backoff_max_ms: u32,
}

#[derive(Debug, Clone, Args)]
pub struct SecurityConfig {
    #[clap(
        long = "security-protocol",
        env = "P_KAFKA_SECURITY_PROTOCOL",
        required = false,
        default_value_t = SecurityProtocol::Plaintext,
        help = "Security protocol"
    )]
    pub protocol: SecurityProtocol,

    // SSL Configuration
    #[arg(
        long = "ssl-ca-location",
        env = "P_KAFKA_SSL_CA_LOCATION",
        required = false,
        help = "CA certificate file path"
    )]
    pub ssl_ca_location: Option<PathBuf>,

    #[arg(
        long = "ssl-certificate-location",
        env = "P_KAFKA_SSL_CERTIFICATE_LOCATION",
        required = false,
        help = "Client certificate file path"
    )]
    pub ssl_certificate_location: Option<PathBuf>,

    #[arg(
        long = "ssl-key-location",
        env = "P_KAFKA_SSL_KEY_LOCATION",
        required = false,
        help = "Client key file path"
    )]
    pub ssl_key_location: Option<PathBuf>,

    // SASL Configuration
    #[arg(
        long = "sasl-mechanism",
        env = "P_KAFKA_SASL_MECHANISM",
        required = false,
        help = "SASL mechanism"
    )]
    pub sasl_mechanism: Option<SaslMechanism>,

    #[arg(
        long = "sasl-username",
        env = "P_KAFKA_SASL_USERNAME",
        required = false,
        help = "SASL username"
    )]
    pub sasl_username: Option<String>,

    #[arg(
        long = "sasl-password",
        env = "P_KAFKA_SASL_PASSWORD",
        required = false,
        help = "SASL password"
    )]
    pub sasl_password: Option<String>,

    // SASL/OAUTHBEARER provider configuration
    #[arg(
        long = "oauth-provider",
        env = "P_KAFKA_OAUTH_PROVIDER",
        required = false,
        help = "OAuth provider: aws-msk for AWS IAM signing, or oidc for a standard token endpoint such as the Google Managed Kafka local auth server"
    )]
    pub oauth_provider: Option<OAuthProvider>,

    #[arg(
        long = "oauth-token-endpoint-url",
        env = "P_KAFKA_OAUTH_TOKEN_ENDPOINT_URL",
        required = false,
        help = "OAuth/OIDC token endpoint URL. Required for the oidc provider"
    )]
    pub oauth_token_endpoint_url: Option<String>,

    #[arg(
        long = "oauth-client-id",
        env = "P_KAFKA_OAUTH_CLIENT_ID",
        required = false,
        help = "OAuth/OIDC client ID. Google Managed Kafka's local auth server accepts 'unused'"
    )]
    pub oauth_client_id: Option<String>,

    #[arg(
        long = "oauth-client-secret",
        env = "P_KAFKA_OAUTH_CLIENT_SECRET",
        required = false,
        help = "OAuth/OIDC client secret. Google Managed Kafka's local auth server accepts 'unused'"
    )]
    pub oauth_client_secret: Option<String>,

    // AWS MSK IAM configuration (SASL/OAUTHBEARER)
    #[arg(
        long = "aws-region",
        env = "P_KAFKA_AWS_REGION",
        required = false,
        help = "AWS region for MSK IAM authentication (SASL/OAUTHBEARER). Falls back to AWS_REGION / AWS_DEFAULT_REGION, then the AWS SDK default region chain (profile files, IMDS) when unset."
    )]
    pub aws_region: Option<String>,

    #[arg(
        long = "ssl-key-password",
        env = "P_KAFKA_SSL_KEY_PASSWORD",
        required = false,
        help = "SSL key password"
    )]
    pub ssl_key_password: Option<String>,

    // Kerberos configuration fields
    #[arg(
        long = "kerberos-service-name",
        env = "P_KAFKA_KERBEROS_SERVICE_NAME",
        required = false,
        help = "Kerberos service name"
    )]
    pub kerberos_service_name: Option<String>,

    #[arg(
        long = "kerberos-principal",
        env = "P_KAFKA_KERBEROS_PRINCIPAL",
        required = false,
        help = "Kerberos principal"
    )]
    pub kerberos_principal: Option<String>,

    #[arg(
        long = "kerberos-keytab",
        env = "P_KAFKA_KERBEROS_KEYTAB",
        required = false,
        help = "Path to Kerberos keytab file"
    )]
    pub kerberos_keytab: Option<PathBuf>,
}

impl KafkaConfig {
    pub fn to_rdkafka_consumer_config(&self) -> ClientConfig {
        let mut config = ClientConfig::new();

        // Basic configuration
        config
            .set(
                "bootstrap.servers",
                self.bootstrap_servers
                    .as_ref()
                    .expect("Bootstrap servers must not be empty"),
            )
            .set("client.id", &self.client_id);

        // Consumer configuration
        if let Some(consumer) = &self.consumer {
            consumer.apply_to_config(&mut config);
        }

        // Security configuration
        if let Some(security) = &self.security {
            security.apply_to_config(&mut config);
        } else {
            config.set("security.protocol", "PLAINTEXT");
        }

        config
    }

    pub fn to_rdkafka_producer_config(&self) -> ClientConfig {
        let mut config = ClientConfig::new();

        // Basic configuration
        config
            .set(
                "bootstrap.servers",
                self.bootstrap_servers
                    .as_ref()
                    .expect("Bootstrap servers must not be empty"),
            )
            .set("client.id", &self.client_id);

        // Producer configuration
        if let Some(producer) = &self.producer {
            producer.apply_to_config(&mut config);
        }

        // Security configuration
        if let Some(security) = &self.security {
            security.apply_to_config(&mut config);
        } else {
            config.set("security.protocol", "PLAINTEXT");
        }

        config
    }

    pub fn consumer(&self) -> Option<&ConsumerConfig> {
        self.consumer.as_ref()
    }

    pub fn producer(&self) -> Option<&ProducerConfig> {
        self.producer.as_ref()
    }

    pub fn security(&self) -> Option<&SecurityConfig> {
        self.security.as_ref()
    }

    pub fn validate(&self) -> anyhow::Result<()> {
        if self
            .bootstrap_servers
            .as_deref()
            .is_none_or(|servers| servers.trim().is_empty())
        {
            anyhow::bail!("Bootstrap servers must not be empty");
        }

        if let Some(consumer) = &self.consumer {
            consumer.validate()?;
        }

        if let Some(producer) = &self.producer {
            producer.validate()?;
        }

        if let Some(security) = &self.security {
            security.validate()?;
        }

        Ok(())
    }
}

impl ConsumerConfig {
    pub fn validate(&self) -> anyhow::Result<()> {
        if self.group_id.is_empty() {
            anyhow::bail!("Consumer group ID must not be empty");
        }
        if self.topics.is_empty() {
            anyhow::bail!("At least one topic must be specified");
        }
        Ok(())
    }

    fn apply_to_config(&self, config: &mut ClientConfig) {
        config
            .set("group.id", &self.group_id)
            .set(
                "partition.assignment.strategy",
                &self.partition_assignment_strategy,
            )
            .set("session.timeout.ms", self.session_timeout_ms.to_string())
            .set(
                "heartbeat.interval.ms",
                self.heartbeat_interval_ms.to_string(),
            )
            .set(
                "max.poll.interval.ms",
                self.max_poll_interval_ms.to_string(),
            )
            .set("enable.auto.commit", "false")
            .set("fetch.min.bytes", self.fetch_min_bytes.to_string())
            .set("fetch.max.bytes", self.fetch_max_bytes.to_string())
            .set(
                "max.partition.fetch.bytes",
                self.max_partition_fetch_bytes.to_string(),
            )
            .set("isolation.level", self.isolation_level.to_string())
            .set("group.instance.id", self.group_instance_id.to_string())
            .set("statistics.interval.ms", self.stats_interval_ms.to_string());

        // `auto.offset.reset` only governs where consumption starts when no
        // committed offset exists. `Group` means "resume from committed
        // offsets", so it keeps librdkafka's default fallback.
        if let Some(reset) = self.auto_offset_reset.auto_offset_reset_value() {
            config.set("auto.offset.reset", reset);
        }
    }

    pub fn topics(&self) -> Vec<&str> {
        self.topics.iter().map(|t| t.as_str()).collect()
    }

    pub fn buffer_config(&self) -> BufferConfig {
        BufferConfig {
            buffer_size: self.buffer_size,
            buffer_timeout: self.buffer_timeout,
        }
    }
}

#[derive(Clone, Debug)]
pub struct BufferConfig {
    pub buffer_size: usize,
    pub buffer_timeout: Duration,
}

impl Default for BufferConfig {
    fn default() -> Self {
        Self {
            buffer_size: 10000,
            buffer_timeout: Duration::from_millis(10000),
        }
    }
}

impl ProducerConfig {
    fn apply_to_config(&self, config: &mut ClientConfig) {
        config
            .set("acks", self.acks.to_string())
            .set("compression.type", self.compression_type.to_string())
            .set("batch.size", self.batch_size.to_string())
            .set("linger.ms", self.linger_ms.to_string())
            .set("enable.idempotence", self.enable_idempotence.to_string())
            .set(
                "max.in.flight.requests.per.connection",
                self.max_in_flight_requests_per_connection.to_string(),
            )
            .set("delivery.timeout.ms", self.delivery_timeout_ms.to_string())
            .set("retry.backoff.ms", self.retry_backoff_ms.to_string())
            .set(
                "transaction.timeout.ms",
                self.transaction_timeout_ms.to_string(),
            )
            .set("request.timeout.ms", self.request_timeout_ms.to_string())
            .set("max.retries", self.max_retries.to_string())
            .set(
                "retry.backoff.max.ms",
                self.retry_backoff_max_ms.to_string(),
            )
            .set("buffer.memory", self.buffer_memory.to_string())
            .set("message.timeout.ms", self.message_timeout_ms.to_string())
            .set("message.max.bytes", self.message_max_bytes.to_string());
    }

    fn validate(&self) -> anyhow::Result<()> {
        if self.batch_size == 0 {
            anyhow::bail!("Batch size must be greater than 0");
        }

        if self.linger_ms > self.delivery_timeout_ms {
            anyhow::bail!("Linger time cannot be greater than delivery timeout");
        }

        Ok(())
    }
}

impl SecurityConfig {
    fn apply_to_config(&self, config: &mut ClientConfig) {
        // Set security protocol
        config.set("security.protocol", self.protocol.to_string());

        // Configure SSL if enabled
        if matches!(
            self.protocol,
            SecurityProtocol::Ssl | SecurityProtocol::SaslSsl
        ) {
            if let Some(ref path) = self.ssl_ca_location {
                config.set("ssl.ca.location", path.to_string_lossy().to_string());
            }
            if let Some(ref path) = self.ssl_certificate_location {
                config.set(
                    "ssl.certificate.location",
                    path.to_string_lossy().to_string(),
                );
            }
            if let Some(ref path) = self.ssl_key_location {
                config.set("ssl.key.location", path.to_string_lossy().to_string());
            }
            if let Some(ref password) = self.ssl_key_password {
                config.set("ssl.key.password", password);
            }
        }

        // Configure SASL if enabled
        if matches!(
            self.protocol,
            SecurityProtocol::SaslSsl | SecurityProtocol::SaslPlaintext
        ) {
            if let Some(ref mechanism) = self.sasl_mechanism {
                config.set("sasl.mechanism", mechanism.to_string());
            }
            if let Some(ref username) = self.sasl_username {
                config.set("sasl.username", username);
            }
            if let Some(ref password) = self.sasl_password {
                config.set("sasl.password", password);
            }

            // Configure Kerberos settings if using GSSAPI
            if matches!(self.sasl_mechanism, Some(SaslMechanism::Gssapi)) {
                if let Some(ref service) = self.kerberos_service_name {
                    config.set("sasl.kerberos.service.name", service);
                }
                if let Some(ref principal) = self.kerberos_principal {
                    config.set("sasl.kerberos.principal", principal);
                }
                if let Some(ref keytab) = self.kerberos_keytab {
                    config.set("sasl.kerberos.keytab", keytab.to_string_lossy().to_string());
                }
            }

            if matches!(self.sasl_mechanism, Some(SaslMechanism::OAuthBearer))
                && matches!(self.resolved_oauth_provider(), Some(OAuthProvider::Oidc))
            {
                // librdkafka's built-in OIDC handler fetches and refreshes tokens
                // from the configured endpoint. Google Managed Kafka's local auth
                // server implements this endpoint using Application Default
                // Credentials.
                config.set("sasl.oauthbearer.method", "oidc");
                if let Some(ref endpoint) = self.oauth_token_endpoint_url {
                    config.set("sasl.oauthbearer.token.endpoint.url", endpoint);
                }
                if let Some(ref client_id) = self.oauth_client_id {
                    config.set("sasl.oauthbearer.client.id", client_id);
                }
                if let Some(ref client_secret) = self.oauth_client_secret {
                    config.set("sasl.oauthbearer.client.secret", client_secret);
                }
            }
        }
    }

    /// Resolves the token provider while preserving the original AWS-only
    /// configuration. An explicit provider wins, followed by an OIDC endpoint,
    /// then an AWS region.
    pub fn resolved_oauth_provider(&self) -> Option<OAuthProvider> {
        let region = self.resolved_aws_region();
        self.resolved_oauth_provider_with_region(region.as_deref())
    }

    fn resolved_oauth_provider_with_region(
        &self,
        resolved_region: Option<&str>,
    ) -> Option<OAuthProvider> {
        self.oauth_provider.or_else(|| {
            if Self::has_value(&self.oauth_token_endpoint_url) {
                Some(OAuthProvider::Oidc)
            } else if resolved_region.is_some() {
                Some(OAuthProvider::AwsMsk)
            } else {
                None
            }
        })
    }

    /// Resolves the AWS region to use for MSK IAM authentication.
    ///
    /// Precedence: the explicit `--aws-region` flag, then the standard
    /// `AWS_REGION` / `AWS_DEFAULT_REGION` environment variables. Each source is
    /// normalized (trimmed and rejected if empty) before falling through, so an
    /// explicitly-empty flag does not shadow a valid environment variable.
    pub fn resolved_aws_region(&self) -> Option<String> {
        Self::normalize_region(self.aws_region.as_deref())
            .or_else(|| Self::normalize_region(std::env::var(AWS_REGION_ENV).ok().as_deref()))
            .or_else(|| {
                Self::normalize_region(std::env::var(AWS_DEFAULT_REGION_ENV).ok().as_deref())
            })
    }

    /// Trims a candidate region value and discards it if it is empty.
    fn normalize_region(region: Option<&str>) -> Option<String> {
        region
            .map(str::trim)
            .filter(|region| !region.is_empty())
            .map(str::to_owned)
    }

    fn has_value(value: &Option<String>) -> bool {
        value
            .as_deref()
            .is_some_and(|value| !value.trim().is_empty())
    }

    fn validate(&self) -> anyhow::Result<()> {
        // Resolve the region (including environment fallbacks) once, then defer
        // to the pure validation logic so it can be tested deterministically.
        self.validate_with_region(self.resolved_aws_region().as_deref())
    }

    /// Validates the security configuration against an already-resolved AWS
    /// region. Kept separate from region resolution (which reads process
    /// environment variables) so the validation rules can be exercised without
    /// depending on the ambient environment.
    fn validate_with_region(&self, resolved_region: Option<&str>) -> anyhow::Result<()> {
        // Pure TLS (mutual auth) requires the full client certificate material.
        // For SASL_SSL, TLS is only used for server authentication and channel
        // encryption, so client certificates are not required — the caller
        // authenticates through the SASL mechanism instead.
        if matches!(self.protocol, SecurityProtocol::Ssl) {
            if self.ssl_ca_location.is_none() {
                anyhow::bail!("CA certificate location is required for SSL");
            }
            if self.ssl_certificate_location.is_none() {
                anyhow::bail!("Client certificate location is required for SSL");
            }
            if self.ssl_key_location.is_none() {
                anyhow::bail!("Client key location is required for SSL");
            }
        }

        // SASL-bearing protocols must specify a mechanism and satisfy the
        // credential requirements specific to that mechanism.
        if matches!(
            self.protocol,
            SecurityProtocol::SaslSsl | SecurityProtocol::SaslPlaintext
        ) {
            match self.sasl_mechanism {
                None => anyhow::bail!("SASL mechanism is required when SASL is enabled"),
                Some(SaslMechanism::OAuthBearer) => {
                    if matches!(self.protocol, SecurityProtocol::SaslPlaintext) {
                        anyhow::bail!(
                            "SASL/OAUTHBEARER requires the SASL_SSL security protocol; SASL_PLAINTEXT would expose the bearer token"
                        );
                    }

                    match self.resolved_oauth_provider_with_region(resolved_region) {
                        Some(OAuthProvider::AwsMsk) => {
                            // No hard region requirement: when unset here, token
                            // generation falls back to the AWS SDK default
                            // region chain (profile files, IMDS) — the same
                            // sources that supply the credentials.
                        }
                        Some(OAuthProvider::Oidc) => {
                            if !Self::has_value(&self.oauth_token_endpoint_url) {
                                anyhow::bail!(
                                    "OAuth token endpoint URL is required for the oidc OAuth provider"
                                );
                            }
                            if !Self::has_value(&self.oauth_client_id) {
                                anyhow::bail!(
                                    "OAuth client ID is required for the oidc OAuth provider"
                                );
                            }
                            if !Self::has_value(&self.oauth_client_secret) {
                                anyhow::bail!(
                                    "OAuth client secret is required for the oidc OAuth provider"
                                );
                            }
                        }
                        None => anyhow::bail!(
                            "OAuth provider is required for SASL/OAUTHBEARER; set --oauth-provider to aws-msk or oidc"
                        ),
                    }
                }
                Some(SaslMechanism::Gssapi) => {
                    if self.kerberos_service_name.is_none() {
                        anyhow::bail!("Kerberos service name is required for GSSAPI");
                    }
                }
                Some(
                    SaslMechanism::Plain | SaslMechanism::ScramSha256 | SaslMechanism::ScramSha512,
                ) => {
                    if self.sasl_username.is_none() || self.sasl_password.is_none() {
                        anyhow::bail!("SASL username and password are required");
                    }
                }
            }
        }

        Ok(())
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum SecurityProtocol {
    Plaintext,
    Ssl,
    SaslSsl,
    SaslPlaintext,
}

impl std::str::FromStr for SecurityProtocol {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_uppercase().as_str() {
            "PLAINTEXT" => Ok(SecurityProtocol::Plaintext),
            "SSL" => Ok(SecurityProtocol::Ssl),
            "SASL_SSL" | "SASL-SSL" => Ok(SecurityProtocol::SaslSsl),
            "SASL_PLAINTEXT" | "SASL-PLAINTEXT" => Ok(SecurityProtocol::SaslPlaintext),
            _ => Err(format!("Invalid security protocol: {s}")),
        }
    }
}

impl std::fmt::Display for SecurityProtocol {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            SecurityProtocol::Plaintext => write!(f, "PLAINTEXT"),
            SecurityProtocol::Ssl => write!(f, "SSL"),
            SecurityProtocol::SaslSsl => write!(f, "SASL_SSL"),
            SecurityProtocol::SaslPlaintext => write!(f, "SASL_PLAINTEXT"),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum SaslMechanism {
    Plain,
    ScramSha256,
    ScramSha512,
    Gssapi,
    OAuthBearer,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum OAuthProvider {
    AwsMsk,
    Oidc,
}

impl std::str::FromStr for OAuthProvider {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_ascii_lowercase().as_str() {
            "aws-msk" | "aws_msk" | "aws" => Ok(Self::AwsMsk),
            "oidc" | "gcp" | "gcp-managed-kafka" => Ok(Self::Oidc),
            _ => Err(format!("Invalid OAuth provider: {s}")),
        }
    }
}

impl std::fmt::Display for OAuthProvider {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::AwsMsk => write!(f, "aws-msk"),
            Self::Oidc => write!(f, "oidc"),
        }
    }
}

impl Default for KafkaConfig {
    fn default() -> Self {
        Self {
            // Common configuration with standard broker port
            bootstrap_servers: Some("localhost:9092".to_string()),
            client_id: "parseable-connect".to_string(),
            // Listener for all assigned partitions
            partition_listener_concurrency: 2,
            // Component-specific configurations with production-ready defaults
            consumer: Some(ConsumerConfig::default()),
            producer: Some(ProducerConfig::default()),
            // Security configuration with plaintext protocol
            security: Some(SecurityConfig::default()),
            bad_data: BadData::default(),
        }
    }
}

impl std::fmt::Display for SaslMechanism {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            SaslMechanism::Plain => write!(f, "PLAIN"),
            SaslMechanism::ScramSha256 => write!(f, "SCRAM-SHA-256"),
            SaslMechanism::ScramSha512 => write!(f, "SCRAM-SHA-512"),
            SaslMechanism::Gssapi => write!(f, "GSSAPI"),
            SaslMechanism::OAuthBearer => write!(f, "OAUTHBEARER"),
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
            max_retries: 20,
            max_in_flight_requests_per_connection: 5,
            message_max_bytes: 1048576,    // 1MB maximum message size
            enable_idempotence: true,      // Ensure exactly-once delivery
            transaction_timeout_ms: 60000, // 1 minute transaction timeout
            queue_buffering_max_messages: 100000, // Producer queue size
            retry_backoff_ms: 100,         // Backoff time between retries
            message_timeout_ms: 120000,    // 2 minute message timeout
            buffer_memory: 33554432,       // 32MB buffer memory
            request_timeout_ms: 60000,     // 60 second request timeout
            queue_buffering_max_kbytes: 1048576, // 1MB maximum queue size
            retry_backoff_max_ms: 1000,    // Maximum backoff time between retries
        }
    }
}

impl Default for ConsumerConfig {
    fn default() -> Self {
        Self {
            topics: vec![],
            group_id: "parseable-connect-cg".to_string(),
            buffer_size: 10_000,
            buffer_timeout: Duration::from_millis(10000),
            group_instance_id: "parseable-cg-ii".to_string(),
            // NOTE: cooperative-sticky does not work well in rdkafka when using manual commit.
            // @see https://github.com/confluentinc/librdkafka/issues/4629
            // @see https://github.com/confluentinc/librdkafka/issues/4368
            partition_assignment_strategy: "roundrobin,range".to_string(),
            session_timeout_ms: 60000,
            heartbeat_interval_ms: 3000,
            max_poll_interval_ms: 300000,
            enable_auto_offset_store: true,
            auto_offset_reset: SourceOffset::Earliest,
            fetch_min_bytes: 1,
            fetch_max_bytes: 52428800,
            fetch_max_wait_ms: 500,
            max_partition_fetch_bytes: 1048576,
            queued_min_messages: 100000,
            queued_max_messages_kbytes: 65536,
            enable_partition_eof: false,
            check_crcs: false,
            isolation_level: "read_committed".to_string(),
            fetch_message_max_bytes: 1048576,
            stats_interval_ms: 10000,
        }
    }
}

impl Default for SecurityConfig {
    fn default() -> Self {
        Self {
            protocol: SecurityProtocol::Plaintext,
            ssl_ca_location: None,
            ssl_certificate_location: None,
            ssl_key_location: None,
            sasl_mechanism: None,
            sasl_username: None,
            sasl_password: None,
            oauth_provider: None,
            oauth_token_endpoint_url: None,
            oauth_client_id: None,
            oauth_client_secret: None,
            aws_region: None,
            ssl_key_password: None,
            kerberos_service_name: None,
            kerberos_principal: None,
            kerberos_keytab: None,
        }
    }
}

impl std::str::FromStr for SaslMechanism {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_uppercase().as_str() {
            "PLAIN" => Ok(SaslMechanism::Plain),
            "SCRAM-SHA-256" => Ok(SaslMechanism::ScramSha256),
            "SCRAM-SHA-512" => Ok(SaslMechanism::ScramSha512),
            "GSSAPI" => Ok(SaslMechanism::Gssapi),
            "OAUTHBEARER" | "OAUTH-BEARER" | "O-AUTH-BEARER" => Ok(SaslMechanism::OAuthBearer),
            _ => Err(format!("Invalid SASL mechanism: {s}")),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum Acks {
    None,
    Leader,
    All,
}

impl std::str::FromStr for Acks {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "0" => Ok(Acks::None),
            "1" => Ok(Acks::Leader),
            "all" => Ok(Acks::All),
            _ => Err(format!("Invalid acks value: {s}")),
        }
    }
}

#[derive(ValueEnum, Debug, Clone)]
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

    /// Value for librdkafka's `auto.offset.reset`, or `None` to keep the
    /// library default.
    ///
    /// `Group` delegates to committed offsets and deliberately sets nothing:
    /// for a brand-new consumer group (no committed offsets anywhere) it is
    /// therefore equivalent to `latest`, librdkafka's default fallback. This
    /// is documented in the CLI help; use `earliest` when a new group must
    /// consume the full topic history.
    pub fn auto_offset_reset_value(&self) -> Option<&'static str> {
        match self {
            SourceOffset::Earliest => Some("earliest"),
            SourceOffset::Latest => Some("latest"),
            SourceOffset::Group => None,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[derive(Parser)]
    struct SecurityCli {
        #[command(flatten)]
        security: SecurityConfig,
    }

    #[test]
    fn standard_kafka_security_values_parse() {
        let cli = SecurityCli::try_parse_from([
            "test",
            "--security-protocol",
            "SASL_SSL",
            "--sasl-mechanism",
            "OAUTHBEARER",
            "--oauth-provider",
            "oidc",
            "--oauth-token-endpoint-url",
            "http://localhost:14293",
            "--oauth-client-id",
            "unused",
            "--oauth-client-secret",
            "unused",
        ])
        .expect("standard Kafka values should parse");

        assert!(matches!(cli.security.protocol, SecurityProtocol::SaslSsl));
        assert!(matches!(
            cli.security.sasl_mechanism,
            Some(SaslMechanism::OAuthBearer)
        ));
        assert_eq!(cli.security.oauth_provider, Some(OAuthProvider::Oidc));
    }

    #[test]
    fn empty_bootstrap_servers_are_rejected() {
        for bootstrap in [None, Some(String::new()), Some("   ".to_string())] {
            let config = KafkaConfig {
                bootstrap_servers: bootstrap,
                ..Default::default()
            };
            assert!(config.validate().is_err());
        }
    }

    #[test]
    fn auto_offset_reset_is_applied_to_consumer_config() {
        assert_eq!(
            SourceOffset::Earliest.auto_offset_reset_value(),
            Some("earliest")
        );
        assert_eq!(
            SourceOffset::Latest.auto_offset_reset_value(),
            Some("latest")
        );
        assert_eq!(SourceOffset::Group.auto_offset_reset_value(), None);

        // Default consumer config uses Earliest and must propagate it.
        let config = KafkaConfig::default().to_rdkafka_consumer_config();
        assert_eq!(config.get("auto.offset.reset"), Some("earliest"));
    }

    #[test]
    fn aws_msk_without_region_defers_to_sdk_chain() {
        // An explicit aws-msk provider without a region is valid: token
        // generation resolves the region via the AWS SDK default chain
        // (profile files, IMDS) at runtime.
        let security = SecurityConfig {
            protocol: SecurityProtocol::SaslSsl,
            sasl_mechanism: Some(SaslMechanism::OAuthBearer),
            oauth_provider: Some(OAuthProvider::AwsMsk),
            aws_region: None,
            ..Default::default()
        };

        assert!(security.validate_with_region(None).is_ok());
    }

    #[test]
    fn oauthbearer_requires_a_provider() {
        // With no explicit provider, no OIDC endpoint, and no resolvable
        // region, the provider cannot be inferred and validation must fail.
        let security = SecurityConfig {
            protocol: SecurityProtocol::SaslSsl,
            sasl_mechanism: Some(SaslMechanism::OAuthBearer),
            ..Default::default()
        };

        assert!(security.validate_with_region(None).is_err());
    }

    #[test]
    fn oauthbearer_rejects_sasl_plaintext() {
        // The bearer token must never traverse an unencrypted connection.
        let security = SecurityConfig {
            protocol: SecurityProtocol::SaslPlaintext,
            sasl_mechanism: Some(SaslMechanism::OAuthBearer),
            aws_region: Some("us-east-1".to_string()),
            ..Default::default()
        };

        assert!(security.validate_with_region(Some("us-east-1")).is_err());
    }

    #[test]
    fn normalize_region_trims_and_rejects_empty() {
        assert_eq!(
            SecurityConfig::normalize_region(Some("  us-east-1  ")).as_deref(),
            Some("us-east-1")
        );
        assert_eq!(SecurityConfig::normalize_region(Some("   ")), None);
        assert_eq!(SecurityConfig::normalize_region(Some("")), None);
        assert_eq!(SecurityConfig::normalize_region(None), None);
    }

    #[test]
    fn oauthbearer_with_explicit_region_is_valid() {
        let security = SecurityConfig {
            protocol: SecurityProtocol::SaslSsl,
            sasl_mechanism: Some(SaslMechanism::OAuthBearer),
            aws_region: Some("us-east-1".to_string()),
            ..Default::default()
        };

        assert!(security.validate().is_ok());
        assert_eq!(security.resolved_aws_region().as_deref(), Some("us-east-1"));
        assert_eq!(
            security.resolved_oauth_provider(),
            Some(OAuthProvider::AwsMsk)
        );
    }

    #[test]
    fn oidc_oauthbearer_does_not_require_an_aws_region() {
        let security = SecurityConfig {
            protocol: SecurityProtocol::SaslSsl,
            sasl_mechanism: Some(SaslMechanism::OAuthBearer),
            oauth_provider: Some(OAuthProvider::Oidc),
            oauth_token_endpoint_url: Some("http://localhost:14293".to_string()),
            oauth_client_id: Some("unused".to_string()),
            oauth_client_secret: Some("unused".to_string()),
            aws_region: None,
            ..Default::default()
        };

        assert!(security.validate_with_region(None).is_ok());
    }

    #[test]
    fn oidc_oauthbearer_requires_complete_endpoint_configuration() {
        let security = SecurityConfig {
            protocol: SecurityProtocol::SaslSsl,
            sasl_mechanism: Some(SaslMechanism::OAuthBearer),
            oauth_provider: Some(OAuthProvider::Oidc),
            oauth_token_endpoint_url: Some("http://localhost:14293".to_string()),
            oauth_client_id: Some("unused".to_string()),
            oauth_client_secret: None,
            ..Default::default()
        };

        assert!(security.validate_with_region(None).is_err());
    }

    #[test]
    fn oauthbearer_does_not_require_username_or_password() {
        let security = SecurityConfig {
            protocol: SecurityProtocol::SaslSsl,
            sasl_mechanism: Some(SaslMechanism::OAuthBearer),
            aws_region: Some("eu-west-1".to_string()),
            sasl_username: None,
            sasl_password: None,
            ..Default::default()
        };

        assert!(security.validate().is_ok());
    }

    #[test]
    fn sasl_ssl_does_not_require_client_certificates() {
        // SASL_SSL uses TLS only for the server side; client certs are a
        // mutual-TLS concern and must not be required here.
        let security = SecurityConfig {
            protocol: SecurityProtocol::SaslSsl,
            sasl_mechanism: Some(SaslMechanism::ScramSha512),
            sasl_username: Some("user".to_string()),
            sasl_password: Some("pass".to_string()),
            ssl_ca_location: None,
            ssl_certificate_location: None,
            ssl_key_location: None,
            ..Default::default()
        };

        assert!(security.validate().is_ok());
    }

    #[test]
    fn scram_still_requires_username_and_password() {
        let security = SecurityConfig {
            protocol: SecurityProtocol::SaslSsl,
            sasl_mechanism: Some(SaslMechanism::ScramSha512),
            sasl_username: None,
            sasl_password: None,
            ..Default::default()
        };

        assert!(security.validate().is_err());
    }

    #[test]
    fn oauthbearer_applies_mechanism_without_credentials() {
        let security = SecurityConfig {
            protocol: SecurityProtocol::SaslSsl,
            sasl_mechanism: Some(SaslMechanism::OAuthBearer),
            aws_region: Some("us-east-2".to_string()),
            ..Default::default()
        };

        let mut config = ClientConfig::new();
        security.apply_to_config(&mut config);

        assert_eq!(config.get("security.protocol"), Some("SASL_SSL"));
        assert_eq!(config.get("sasl.mechanism"), Some("OAUTHBEARER"));
        // No static credentials should be set for OAUTHBEARER.
        assert_eq!(config.get("sasl.username"), None);
        assert_eq!(config.get("sasl.password"), None);
        assert_eq!(config.get("sasl.oauthbearer.method"), None);
    }

    #[test]
    fn oidc_oauthbearer_applies_librdkafka_configuration() {
        let security = SecurityConfig {
            protocol: SecurityProtocol::SaslSsl,
            sasl_mechanism: Some(SaslMechanism::OAuthBearer),
            oauth_provider: Some(OAuthProvider::Oidc),
            oauth_token_endpoint_url: Some("http://localhost:14293".to_string()),
            oauth_client_id: Some("unused".to_string()),
            oauth_client_secret: Some("unused".to_string()),
            ..Default::default()
        };

        let mut config = ClientConfig::new();
        security.apply_to_config(&mut config);

        assert_eq!(config.get("security.protocol"), Some("SASL_SSL"));
        assert_eq!(config.get("sasl.mechanism"), Some("OAUTHBEARER"));
        assert_eq!(config.get("sasl.oauthbearer.method"), Some("oidc"));
        assert_eq!(
            config.get("sasl.oauthbearer.token.endpoint.url"),
            Some("http://localhost:14293")
        );
        assert_eq!(config.get("sasl.oauthbearer.client.id"), Some("unused"));
        assert_eq!(config.get("sasl.oauthbearer.client.secret"), Some("unused"));
    }
}
