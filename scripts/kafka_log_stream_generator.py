# Parseable Server (C) 2022 - 2025 Parseable, Inc.
#
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU Affero General Public License as
# published by the Free Software Foundation, either version 3 of the
# License, or (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU Affero General Public License for more details.
#
# You should have received a copy of the GNU Affero General Public License
# along with this program.  If not, see <http://www.gnu.org/licenses/>.

import json
import logging
import os
import random
import sys
import time
import uuid
from datetime import datetime, timezone
from typing import Dict, Any

from confluent_kafka import Producer
from confluent_kafka.admin import AdminClient
from confluent_kafka.cimpl import NewTopic
from faker import Faker

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)]
)

logger = logging.getLogger(__name__)
fake = Faker()

# Kafka Configuration
KAFKA_BROKERS = os.getenv("KAFKA_BROKERS", "localhost:9092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "local-logs-stream")
NUM_PARTITIONS = int(os.getenv("NUM_PARTITIONS", "6"))  # Default partitions
REPLICATION_FACTOR = int(os.getenv("REPLICATION_FACTOR", "1"))  # Default RF
TOTAL_LOGS = int(os.getenv("TOTAL_LOGS", "100"))  # Total logs to produce
LOG_RATE = int(os.getenv("LOG_RATE", "50"))  # Logs per second
REPORT_EVERY = 5_000  # Progress report frequency

# Kubernetes Configuration
K8S_NAMESPACES = ["default", "kube-system", "monitoring", "logging", "app"]
CONTAINER_IMAGES = [
    "parseable/parseable:v1.8.1",
    "parseable/query-service:v1.8.1",
    "parseable/ingester:v1.8.1",
    "parseable/frontend:v1.8.1"
]
NODE_TYPES = ["compute", "storage", "ingestion"]
COMPONENTS = ["query", "storage", "ingestion", "frontend"]

producer_conf = {
    "bootstrap.servers": KAFKA_BROKERS,
    "queue.buffering.max.messages": 200_000,
    "queue.buffering.max.ms": 100,  # Up to 100ms linger
    "batch.num.messages": 10_000,
    "compression.type": "lz4",  # Compression (lz4, snappy, zstd, gzip)
    "message.send.max.retries": 3,
    "reconnect.backoff.ms": 100,
    "reconnect.backoff.max.ms": 3600000,
    # "acks": "all",  # Safer but can reduce throughput if replication is slow
}

admin_client = AdminClient({"bootstrap.servers": KAFKA_BROKERS})
producer = Producer(producer_conf)


def generate_kubernetes_metadata() -> Dict[str, str]:
    namespace = random.choice(K8S_NAMESPACES)
    sts_name = f"parseable-{random.choice(COMPONENTS)}"
    pod_index = str(random.randint(0, 5))
    pod_name = f"{sts_name}-{pod_index}"

    return {
        "kubernetes_namespace_name": namespace,
        "kubernetes_pod_name": pod_name,
        "kubernetes_pod_id": str(uuid.uuid4()),
        "kubernetes_pod_ip": f"10.{random.randint(0, 255)}.{random.randint(0, 255)}.{random.randint(0, 255)}",
        "kubernetes_host": f"ip-10-0-{random.randint(0, 255)}-{random.randint(0, 255)}.ec2.internal",
        "kubernetes_container_name": random.choice(COMPONENTS),
        "kubernetes_container_image": random.choice(CONTAINER_IMAGES),
        "kubernetes_container_hash": fake.sha256(),
        "kubernetes_docker_id": fake.sha256()[:12],
        "kubernetes_labels_app": "parseable",
        "kubernetes_labels_component": random.choice(COMPONENTS),
        "kubernetes_labels_pbc_nodetype": random.choice(NODE_TYPES),
        "kubernetes_labels_spot": random.choice(["true", "false"]),
        "kubernetes_labels_sts_name": sts_name,
        "kubernetes_labels_statefulset.kubernetes.io/pod-name": pod_name,
        "kubernetes_labels_apps.kubernetes.io/pod-index": pod_index,
        "kubernetes_labels_controller-revision-hash": fake.sha256()[:10],
        "kubernetes_labels_original_sts_name": sts_name,
        "kubernetes_labels_parseable_cr": "parseable-cluster"
    }


def generate_log_entry() -> Dict[str, Any]:
    now = datetime.now(timezone.utc)

    # Generate request-related data
    status_code = random.choice([200, 200, 200, 201, 400, 401, 403, 404, 500])
    response_time = random.randint(10, 2000)

    # Basic log structure
    log_entry = {
        "app_meta": json.dumps({"version": "v0.8.0", "component": random.choice(COMPONENTS)}),
        "device_id": random.randint(1000, 9999),
        "host": f"ip-{random.randint(0, 255)}-{random.randint(0, 255)}-{random.randint(0, 255)}-{random.randint(0, 255)}",
        "level": random.choice(["INFO", "INFO", "INFO", "WARN", "ERROR"]),
        "location": fake.city(),
        "message": fake.sentence(),
        "os": random.choice(["linux/amd64", "linux/arm64"]),
        "process_id": random.randint(1, 65535),
        "request_body": json.dumps({"query": "SELECT * FROM logs LIMIT 100"}),
        "response_time": response_time,
        "runtime": "python3.9",
        "session_id": str(uuid.uuid4()),
        "source": "application",
        "source_time": now.isoformat(),
        "status_code": status_code,
        "stream": "stdout",
        "time": int(now.timestamp() * 1000),
        "timezone": "UTC",
        "user_agent": fake.user_agent(),
        "user_id": random.randint(1000, 9999),
        "uuid": str(uuid.uuid4()),
        "version": "v0.8.0"
    }

    # Add Kubernetes metadata
    log_entry.update(generate_kubernetes_metadata())

    return log_entry


def create_topic(topic_name: str, num_partitions: int, replication_factor: int) -> None:
    new_topic = NewTopic(
        topic=topic_name,
        num_partitions=num_partitions,
        replication_factor=replication_factor
    )

    logger.info(f"Creating topic '{topic_name}' with {num_partitions} partitions and RF {replication_factor}...")
    fs = admin_client.create_topics([new_topic])

    for topic, f in fs.items():
        try:
            f.result()
            logger.info(f"Topic '{topic}' created successfully.")
        except Exception as e:
            if "TopicExistsError" in str(e):
                logger.warning(f"Topic '{topic}' already exists.")
            else:
                logger.error(f"Failed to create topic '{topic}': {e}")


def delivery_report(err, msg):
    if err:
        logger.error(f"Delivery failed for message {msg.key()}: {err}")
    else:
        logger.debug(f"Message delivered to {msg.topic()} [{msg.partition()}]")


def main():
    logger.info("Starting continuous log producer...")
    create_topic(KAFKA_TOPIC, NUM_PARTITIONS, REPLICATION_FACTOR)
    logger.info(f"Broker: {KAFKA_BROKERS}, Topic: {KAFKA_TOPIC}, Rate: {LOG_RATE} logs/sec")

    message_count = 0
    start_time = time.time()
    batch_start_time = time.time()
    limit_reached = False

    try:
        while True:
            current_time = time.time()

            if not limit_reached:
                if message_count < TOTAL_LOGS:
                    log_data = generate_log_entry()
                    log_str = json.dumps(log_data)

                    # Send to Kafka
                    producer.produce(
                        topic=KAFKA_TOPIC,
                        value=log_str,
                        callback=delivery_report
                    )

                    message_count += 1

                    if message_count % REPORT_EVERY == 0:
                        batch_elapsed = current_time - batch_start_time
                        total_elapsed = current_time - start_time

                        logger.info(f"Batch of {REPORT_EVERY} messages produced in {batch_elapsed:.2f}s")
                        logger.info(f"Total messages: {message_count}, Running time: {total_elapsed:.2f}s")
                        logger.info(f"Current rate: ~{REPORT_EVERY / batch_elapsed:,.0f} logs/sec")

                        producer.flush()
                        batch_start_time = current_time

                elif not limit_reached:
                    logger.info(
                        f"Reached TOTAL_LOGS limit of {TOTAL_LOGS}. Continuing to run without producing messages...")
                    producer.flush()
                    limit_reached = True

            if limit_reached:
                time.sleep(5)
            else:
                # Sleep to maintain the logs/second rate
                time.sleep(1 / LOG_RATE)

    except KeyboardInterrupt:
        logger.warning("Interrupted by user! Flushing remaining messages...")
        producer.flush()

    except Exception as e:
        logger.error(f"An error occurred: {e}")

    finally:
        logger.info("Flushing producer...")
        producer.flush()
        logger.info("Generator stopped.")


if __name__ == "__main__":
    main()
