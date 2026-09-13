"""
kafka_client.py
Shared helper for talking to Upstash Kafka (a serverless, free-tier Kafka
service that speaks the standard Kafka protocol over SASL_SSL).

Used by both the demo app (producer) and the pipeline worker (consumer).
All credentials come from environment variables -- never hardcode them.

Get these values from your Upstash Kafka console (console.upstash.com):
  UPSTASH_KAFKA_BOOTSTRAP_SERVER  e.g. exotic-fox-12345-us1-kafka.upstash.io:9092
  UPSTASH_KAFKA_USERNAME
  UPSTASH_KAFKA_PASSWORD
"""

import os

from kafka import KafkaProducer, KafkaConsumer

BOOTSTRAP = os.environ.get("UPSTASH_KAFKA_BOOTSTRAP_SERVER", "")
USERNAME = os.environ.get("UPSTASH_KAFKA_USERNAME", "")
PASSWORD = os.environ.get("UPSTASH_KAFKA_PASSWORD", "")
TOPIC = os.environ.get("KAFKA_TOPIC", "app-logs")

_common_kwargs = dict(
    bootstrap_servers=BOOTSTRAP,
    security_protocol="SASL_SSL",
    sasl_mechanism="SCRAM-SHA-256",
    sasl_plain_username=USERNAME,
    sasl_plain_password=PASSWORD,
)


def get_producer():
    import json
    return KafkaProducer(
        **_common_kwargs,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    )


def get_consumer(group_id="pipeline-worker"):
    return KafkaConsumer(
        TOPIC,
        **_common_kwargs,
        group_id=group_id,
        auto_offset_reset="latest",
        enable_auto_commit=True,
        value_deserializer=lambda v: v.decode("utf-8"),
    )
