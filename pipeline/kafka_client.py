"""
kafka_client.py
Shared helper for talking to Kafka. Defaults to Upstash Kafka (a
serverless, free-tier Kafka service that speaks the standard Kafka
protocol over SASL_SSL) -- used by both the demo app (producer) and the
pipeline worker (consumer). All credentials come from environment
variables -- never hardcode them.

Get these values from your Upstash Kafka console (console.upstash.com):
  UPSTASH_KAFKA_BOOTSTRAP_SERVER  e.g. exotic-fox-12345-us1-kafka.upstash.io:9092
  UPSTASH_KAFKA_USERNAME
  UPSTASH_KAFKA_PASSWORD

For local testing against a plain, unauthenticated Kafka broker (e.g. the
docker-compose.local.yml in this repo), set:
  KAFKA_SECURITY_PROTOCOL=PLAINTEXT
and UPSTASH_KAFKA_BOOTSTRAP_SERVER=localhost:9092 -- the username/password
are simply ignored in that mode.
"""

import os

BOOTSTRAP = os.environ.get("UPSTASH_KAFKA_BOOTSTRAP_SERVER", "")
USERNAME = os.environ.get("UPSTASH_KAFKA_USERNAME", "")
PASSWORD = os.environ.get("UPSTASH_KAFKA_PASSWORD", "")
TOPIC = os.environ.get("KAFKA_TOPIC", "app-logs")
SECURITY_PROTOCOL = os.environ.get("KAFKA_SECURITY_PROTOCOL", "SASL_SSL")

if SECURITY_PROTOCOL == "PLAINTEXT":
    _common_kwargs = dict(bootstrap_servers=BOOTSTRAP, security_protocol="PLAINTEXT")
else:
    _common_kwargs = dict(
        bootstrap_servers=BOOTSTRAP,
        security_protocol="SASL_SSL",
        sasl_mechanism="SCRAM-SHA-256",
        sasl_plain_username=USERNAME,
        sasl_plain_password=PASSWORD,
    )


def get_producer():
    # Imported lazily -- see es_client.get_es_client for why: it lets the
    # rest of this tiny module be imported without the real `kafka` package
    # installed (useful for offline testing of code that only needs TOPIC
    # or the config, not an actual connection).
    import json
    from kafka import KafkaProducer
    return KafkaProducer(
        **_common_kwargs,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    )


def get_consumer(group_id="pipeline-worker"):
    from kafka import KafkaConsumer
    return KafkaConsumer(
        TOPIC,
        **_common_kwargs,
        group_id=group_id,
        auto_offset_reset="latest",
        enable_auto_commit=True,
        value_deserializer=lambda v: v.decode("utf-8"),
    )
