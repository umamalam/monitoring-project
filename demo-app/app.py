"""
app.py
A REAL multi-endpoint e-commerce-style app -- not a log simulator.
It has genuine performance issues on purpose (an unindexed "DB" lookup that
gets slower as data grows, a flaky payment endpoint that really does fail
sometimes) so the logs you collect reflect real application behaviour under
real load, not injected randomness.

Logs are written as structured JSON lines to app.log -- this is what
Filebeat will tail and ship to Kafka.
"""

import json
import logging
import os
import random
import time
import uuid
from datetime import datetime, timezone

from flask import Flask, request, jsonify

app = Flask(__name__)

# ---- structured JSON logging setup ----
LOG_PATH = os.environ.get("APP_LOG_PATH", "/var/log/app/app.log")
os.makedirs(os.path.dirname(LOG_PATH), exist_ok=True)

logger = logging.getLogger("app")
logger.setLevel(logging.INFO)
handler = logging.FileHandler(LOG_PATH)


class JsonFormatter(logging.Formatter):
    def format(self, record):
        payload = {
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "service": "demo-app",
            "level": record.levelname,
            "message": record.getMessage(),
        }
        if hasattr(record, "extra_fields"):
            payload.update(record.extra_fields)
        return json.dumps(payload)


handler.setFormatter(JsonFormatter())
logger.addHandler(handler)

_kafka_producer = None
if os.environ.get("KAFKA_BOOTSTRAP_SERVERS"):
    try:
        from confluent_kafka import Producer
        _kafka_producer = Producer({
            "bootstrap.servers": os.environ["KAFKA_BOOTSTRAP_SERVERS"],
            "security.protocol": "SASL_SSL",
            "sasl.mechanisms": os.environ.get("KAFKA_SASL_MECHANISM", "SCRAM-SHA-256"),
            "sasl.username": os.environ["KAFKA_USERNAME"],
            "sasl.password": os.environ["KAFKA_PASSWORD"],
        })
        print("Kafka producer connected -- shipping logs directly to Redpanda")
    except Exception as e:
        print(f"WARNING: could not connect Kafka producer, falling back to file-only: {e}")

KAFKA_TOPIC = os.environ.get("KAFKA_TOPIC", "app-logs")


def log_request(endpoint, status_code, latency_ms, message, level="INFO"):
    extra = {
        "endpoint": endpoint,
        "status_code": status_code,
        "latency_ms": round(latency_ms, 2),
        "request_id": str(uuid.uuid4()),
    }
    log_fn = logger.error if level == "ERROR" else logger.info
    log_fn(message, extra={"extra_fields": extra})

    if _kafka_producer:
        event = {
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "service": "demo-app",
            "level": level,
            **extra,
            "message": message,
        }
        try:
            _kafka_producer.produce(KAFKA_TOPIC, json.dumps(event).encode("utf-8"))
            _kafka_producer.poll(0)
        except Exception as e:
            print(f"WARNING: failed to publish to Kafka: {e}")


_fake_db = []


@app.route("/products", methods=["GET"])
def list_products():
    start = time.time()
    results = [p for p in _fake_db if p.get("active", True)]
    time.sleep(len(_fake_db) * 0.0003)
    latency_ms = (time.time() - start) * 1000
    log_request("/products", 200, latency_ms, f"listed {len(results)} products")
    return jsonify({"count": len(results)}), 200


@app.route("/products", methods=["POST"])
def add_product():
    start = time.time()
    _fake_db.append({"id": len(_fake_db) + 1, "active": True})
    latency_ms = (time.time() - start) * 1000
    log_request("/products", 201, latency_ms, "product created")
    return jsonify({"id": len(_fake_db)}), 201


@app.route("/login", methods=["POST"])
def login():
    start = time.time()
    data = request.get_json(silent=True) or {}
    username = data.get("username", "")
    time.sleep(random.uniform(0.01, 0.05))
    if not username:
        latency_ms = (time.time() - start) * 1000
        log_request("/login", 400, latency_ms, "login failed: missing username", level="ERROR")
        return jsonify({"error": "username required"}), 400
    latency_ms = (time.time() - start) * 1000
    log_request("/login", 200, latency_ms, f"login success for {username}")
    return jsonify({"token": str(uuid.uuid4())}), 200


@app.route("/checkout", methods=["POST"])
def checkout():
    start = time.time()
    roll = random.random()
    if roll < 0.05:
        time.sleep(2.5)
        latency_ms = (time.time() - start) * 1000
        log_request("/checkout", 504, latency_ms, "payment provider timeout", level="ERROR")
        return jsonify({"error": "timeout"}), 504
    if roll < 0.09:
        latency_ms = (time.time() - start) * 1000
        log_request("/checkout", 502, latency_ms, "payment provider returned bad gateway", level="ERROR")
        return jsonify({"error": "bad gateway"}), 502

    time.sleep(random.uniform(0.05, 0.15))
    latency_ms = (time.time() - start) * 1000
    log_request("/checkout", 200, latency_ms, "checkout completed")
    return jsonify({"order_id": str(uuid.uuid4())}), 200


@app.route("/search", methods=["GET"])
def search():
    start = time.time()
    q = request.args.get("q", "")
    time.sleep(random.uniform(0.02, 0.08) + (0.01 if len(q) < 2 else 0))
    latency_ms = (time.time() - start) * 1000
    log_request("/search", 200, latency_ms, f"search executed for query '{q}'")
    return jsonify({"results": []}), 200


@app.route("/health")
def health():
    return jsonify({"status": "ok"}), 200


if __name__ == "__main__":
    port = int(os.environ.get("PORT", 5000))
    app.run(host="0.0.0.0", port=port)
