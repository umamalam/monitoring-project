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
import threading
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

# ---- optional direct Kafka producer (used in the Render/Upstash cloud deploy) ----
# When UPSTASH_KAFKA_BOOTSTRAP_SERVER is set, every log event is ALSO published
# directly to Kafka, in addition to the local file. Locally (no env vars set),
# it just logs to the file, which Filebeat can tail instead -- both are valid,
# real patterns; this app supports either depending on deployment.
_kafka_producer = None
if os.environ.get("UPSTASH_KAFKA_BOOTSTRAP_SERVER"):
    try:
        from kafka import KafkaProducer
        security_protocol = os.environ.get("KAFKA_SECURITY_PROTOCOL", "SASL_SSL")
        if security_protocol == "PLAINTEXT":
            producer_kwargs = dict(
                bootstrap_servers=os.environ["UPSTASH_KAFKA_BOOTSTRAP_SERVER"],
                security_protocol="PLAINTEXT",
                value_serializer=lambda v: json.dumps(v).encode("utf-8"),
            )
        else:
            producer_kwargs = dict(
                bootstrap_servers=os.environ["UPSTASH_KAFKA_BOOTSTRAP_SERVER"],
                security_protocol="SASL_SSL",
                sasl_mechanism="SCRAM-SHA-256",
                sasl_plain_username=os.environ["UPSTASH_KAFKA_USERNAME"],
                sasl_plain_password=os.environ["UPSTASH_KAFKA_PASSWORD"],
                value_serializer=lambda v: json.dumps(v).encode("utf-8"),
            )
        _kafka_producer = KafkaProducer(**producer_kwargs)
        print(f"Kafka producer connected ({security_protocol}) -- shipping logs to Kafka")
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
            "type": "request",
            **extra,
            "message": message,
        }
        try:
            _kafka_producer.send(KAFKA_TOPIC, event)
        except Exception as e:
            print(f"WARNING: failed to publish to Kafka: {e}")


def log_gauge(metric, value):
    """
    Emits a point-in-time measurement (as opposed to a per-request event) --
    e.g. "how many DB connections are currently open". The pipeline treats
    these as a separate event type ("gauge") from request logs, since they
    describe ongoing state rather than a single request outcome.
    """
    logger.info(f"gauge {metric}={value}", extra={"extra_fields": {"type": "gauge", "metric": metric, "value": value}})
    if _kafka_producer:
        event = {
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "service": "demo-app",
            "level": "INFO",
            "type": "gauge",
            "metric": metric,
            "value": value,
            "message": f"gauge {metric}={value}",
        }
        try:
            _kafka_producer.send(KAFKA_TOPIC, event)
        except Exception as e:
            print(f"WARNING: failed to publish gauge to Kafka: {e}")


# ---- genuine slow-leak connection pool (this is the "predict the DB will
# hit its connection limit" scenario) ----
# Every request acquires a connection and, almost always, releases it right
# away -- like a real pool. But a small fraction of requests (concentrated
# on the already-flaky /checkout path) leak: the connection is never
# returned. Under sustained load this produces a real, gradually-rising
# trend toward MAX_CONNECTIONS, not an injected sawtooth -- exactly the
# kind of slow leak a forecasting model should be able to catch hours
# before it becomes an outage.
#
# NOTE: _pool_state lives in this process's memory, so it only means
# something if there's exactly one process holding it -- that's why the
# Dockerfile runs gunicorn with --workers 1 (more threads instead, since
# threads share memory within a process). A real production connection
# pool would live in a shared place (the actual DB driver's pool, or a
# counter in Redis) precisely so it's correct across multiple app
# instances; this in-memory version is a deliberate simplification for a
# single-instance demo, not something to copy into a real multi-worker
# deployment.
MAX_CONNECTIONS = int(os.environ.get("MAX_CONNECTIONS", 120))
LEAK_RATE = float(os.environ.get("CONNECTION_LEAK_RATE", 0.006))  # ~0.6% of requests leak
_pool_lock = threading.Lock()
_pool_state = {"active": 0}


def acquire_connection(leak_prone=False):
    with _pool_lock:
        _pool_state["active"] = min(MAX_CONNECTIONS, _pool_state["active"] + 1)
    leaks = leak_prone and random.random() < LEAK_RATE
    if not leaks:
        # released almost immediately, like a real pooled connection
        def _release():
            time.sleep(random.uniform(0.05, 0.2))
            with _pool_lock:
                _pool_state["active"] = max(0, _pool_state["active"] - 1)
        threading.Thread(target=_release, daemon=True).start()
    # if it leaks, we simply never release it -- that's the bug


def _connection_pool_reporter():
    """Background thread: reports the current pool size as a gauge every
    CONNECTION_REPORT_INTERVAL_SECONDS (default 30s; lower this for a
    faster local demo -- see docker-compose.local.yml instructions)."""
    interval = int(os.environ.get("CONNECTION_REPORT_INTERVAL_SECONDS", 30))
    while True:
        time.sleep(interval)
        with _pool_lock:
            active = _pool_state["active"]
        log_gauge("active_connections", active)


threading.Thread(target=_connection_pool_reporter, daemon=True).start()


# ---- fake "database" that genuinely gets slower as it grows (real bug) ----
_fake_db = []


@app.route("/products", methods=["GET"])
def list_products():
    start = time.time()
    acquire_connection()
    # Genuine O(n) unindexed scan -- this really does get slower as _fake_db grows.
    # That's a real, honest anomaly source: not injected, just bad code (on purpose).
    results = [p for p in _fake_db if p.get("active", True)]
    time.sleep(len(_fake_db) * 0.0003)  # simulates a real unindexed table scan cost
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
    time.sleep(random.uniform(0.01, 0.05))  # real password-hash-check style delay
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
    acquire_connection(leak_prone=True)
    # A genuinely flaky downstream dependency, simulating a real payment
    # gateway that has real, non-deterministic failure modes: timeouts and
    # occasional 5xx from the "provider". Not injected on a timer -- just
    # inherent to how this endpoint is written, like real flaky code.
    roll = random.random()
    if roll < 0.05:
        time.sleep(2.5)  # real timeout-style hang
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
    with _pool_lock:
        active = _pool_state["active"]
    return jsonify({"status": "ok", "active_connections": active, "max_connections": MAX_CONNECTIONS}), 200


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000)
