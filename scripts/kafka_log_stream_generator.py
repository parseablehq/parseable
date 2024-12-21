import json
import time
from datetime import datetime, timezone
from random import choice, randint
from uuid import uuid4
from confluent_kafka import Producer

# Configuration
config = {
    "kafka_broker": "localhost:9092",  # Replace with your Kafka broker address
    "kafka_topic": "log-stream",  # Replace with your Kafka topic name
    "log_rate": 500,  # Logs per second
    "log_template": {
        "timestamp": "",  # Timestamp will be added dynamically
        "correlation_id": "",  # Unique identifier for tracing requests
        "level": "INFO",  # Log level (e.g., INFO, ERROR, DEBUG)
        "message": "",  # Main log message to be dynamically set
        "pod": {
            "name": "example-pod",  # Kubernetes pod name
            "namespace": "default",  # Kubernetes namespace
            "node": "node-01"  # Kubernetes node name
        },
        "request": {
            "method": "",  # HTTP method
            "path": "",  # HTTP request path
            "remote_address": ""  # IP address of the client
        },
        "response": {
            "status_code": 200,  # HTTP response status code
            "latency_ms": 0  # Latency in milliseconds
        },
        "metadata": {
            "container_id": "",  # Container ID
            "image": "example/image:1.0",  # Docker image
            "environment": "prod"  # Environment (e.g., dev, staging, prod)
        }
    }
}

producer = Producer({"bootstrap.servers": config["kafka_broker"]})


def delivery_report(err, msg):
    if err is not None:
        print(f"Delivery failed for message {msg.key()}: {err}")
    else:
        print(f"Message delivered to {msg.topic()} [{msg.partition()}]")


def generate_log():
    log = config["log_template"].copy()
    log["timestamp"] = datetime.now(timezone.utc).isoformat()
    log["correlation_id"] = str(uuid4())

    levels = ["INFO", "WARNING", "ERROR", "DEBUG"]
    messages = [
        "Received incoming HTTP request",
        "Processed request successfully",
        "Failed to process request",
        "Request timeout encountered",
        "Service unavailable"
    ]
    log["level"] = choice(levels)
    log["message"] = choice(messages)

    # Populate request fields
    methods = ["GET", "POST", "PUT", "DELETE"]
    paths = ["/api/resource", "/api/login", "/api/logout", "/api/data"]
    log["request"] = {
        "method": choice(methods),
        "path": choice(paths),
        "remote_address": f"192.168.1.{randint(1, 255)}"
    }

    # Populate response fields
    log["response"] = {
        "status_code": choice([200, 201, 400, 401, 403, 404, 500]),
        "latency_ms": randint(10, 1000)
    }

    # Populate pod and metadata fields
    log["pod"] = {
        "name": f"pod-{randint(1, 100)}",
        "namespace": choice(["default", "kube-system", "production", "staging"]),
        "node": f"node-{randint(1, 10)}"
    }

    log["metadata"] = {
        "container_id": f"container-{randint(1000, 9999)}",
        "image": f"example/image:{randint(1, 5)}.0",
        "environment": choice(["dev", "staging", "prod"])
    }

    return log


def main():
    try:
        while True:
            # Generate log message
            log_message = generate_log()
            log_json = json.dumps(log_message)

            # Send to Kafka
            producer.produce(
                config["kafka_topic"],
                value=log_json,
                callback=delivery_report
            )

            # Flush the producer to ensure delivery
            producer.flush()

            # Wait based on the log rate
            time.sleep(1 / config["log_rate"])
    except KeyboardInterrupt:
        print("Stopped log generation.")
    finally:
        producer.flush()


if __name__ == "__main__":
    main()
