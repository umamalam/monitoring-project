"""
worker.py
Runs on Render as a free "Web Service" -- internally runs two background
threads: a Kafka consumer (Redpanda -> OpenSearch) and an anomaly detector
(z-score on rolling metric windows -> alerts).
"""

import json
import os
import statistics
import threading
import time
from collections import defaultdict
from datetime import datetime, timezone

from flask import Flask, jsonify
from confluent_kafka import Consumer

from es_client import get_es_client, ensure_indices

app = Flask(__name__)

TOPIC = os.environ.get("KAFKA_TOPIC", "app-logs")
WINDOW_SECONDS = 60
CHECK_INTERVAL_SECONDS = 30
BASELINE_WINDOW_COUNT = 20
MIN_BASELINE_SAMPLES = 5
Z_WARNING = 2.5
Z_CRITICAL = 4.0

_status = {"consumer": "starting", "detector": "starting", "last_error": None}


def get_kafka_consumer():
    return Consumer({
        "bootstrap.servers": os.environ["KAFKA_BOOTSTRAP_SERVERS"],
        "security.protocol": "SASL_SSL",
        "sasl.mechanisms": os.environ.get("KAFKA_SASL_MECHANISM", "SCRAM-SHA-256"),
        "sasl.username": os.environ["KAFKA_USERNAME"],
        "sasl.password": os.environ["KAFKA_PASSWORD"],
        "group.id": "pipeline-worker",
        "auto.offset.reset": "latest",
    })


def consumer_loop():
    es = get_es_client()
    consumer = get_kafka_consumer()
    consumer.subscribe([TOPIC])

    window_start = datetime.now(timezone.utc)
    buckets = defaultdict(lambda: {"count": 0, "errors": 0, "latencies": []})
    last_flush = time.time()

    _status["consumer"] = "running"
    print("[consumer] connected to Kafka + OpenSearch, consuming...")

    while True:
        msg = consumer.poll(timeout=1.0)
        if msg is not None and not msg.error():
            try:
                event = json.loads(msg.value().decode("utf-8"))
                es.index(index="logs", body=event)

                service = event.get("service", "unknown")
                b = buckets[service]
                b["count"] += 1
                if event.get("level") == "ERROR":
                    b["errors"] += 1
                if "latency_ms" in event:
                    b["latencies"].append(event["latency_ms"])
            except Exception as e:
                _status["last_error"] = f"consumer: {e}"
                print(f"[consumer] error processing message: {e}")
        elif msg is not None and msg.error():
            print(f"[consumer] Kafka error: {msg.error()}")

        if time.time() - last_flush >= WINDOW_SECONDS:
            flush_window(es, window_start, buckets)
            buckets = defaultdict(lambda: {"count": 0, "errors": 0, "latencies": []})
            window_start = datetime.now(timezone.utc)
            last_flush = time.time()


def flush_window(es, window_start, buckets):
    for service, stats in buckets.items():
        count = stats["count"]
        if count == 0:
            continue
        errors = stats["errors"]
        latencies = sorted(stats["latencies"]) or [0]
        avg_latency = sum(latencies) / len(latencies)
        p95_idx = min(len(latencies) - 1, int(len(latencies) * 0.95))

        es.index(index="metric_windows", body={
            "window_start": window_start.isoformat(),
            "service": service,
            "request_count": count,
            "error_count": errors,
            "error_rate": errors / count,
            "avg_latency_ms": avg_latency,
            "p95_latency_ms": latencies[p95_idx],
        })
    print(f"[consumer] flushed window starting {window_start}, services: {list(buckets.keys())}")


def get_recent_windows(es, service, limit):
    resp = es.search(index="metric_windows", body={
        "query": {"term": {"service": service}},
        "sort": [{"window_start": "desc"}],
        "size": limit,
    })
    return [hit["_source"] for hit in resp["hits"]["hits"]]


def get_active_alert(es, service, metric):
    resp = es.search(index="alerts", body={
        "query": {"bool": {"must": [
            {"term": {"service": service}},
            {"term": {"metric": metric}},
            {"term": {"resolved": False}},
        ]}},
        "size": 1,
    })
    hits = resp["hits"]["hits"]
    return hits[0] if hits else None


def check_metric(es, service, metric_name, latest_value, history_values):
    if len(history_values) < MIN_BASELINE_SAMPLES:
        return
    mean = statistics.mean(history_values)
    stdev = statistics.pstdev(history_values) or 1e-6
    z = (latest_value - mean) / stdev

    active = get_active_alert(es, service, metric_name)

    if z >= Z_WARNING:
        severity = "critical" if z >= Z_CRITICAL else "warning"
        doc = {
            "service": service, "metric": metric_name, "value": latest_value,
            "baseline": mean, "z_score": z, "severity": severity,
            "resolved": False,
            "created_at": datetime.now(timezone.utc).isoformat(),
            "message": f"{service}: {metric_name} = {latest_value:.2f} (baseline {mean:.2f}, z={z:.1f})",
        }
        if active:
            es.update(index="alerts", id=active["_id"], body={"doc": doc})
        else:
            es.index(index="alerts", body=doc)
            print(f"[detector] ALERT {service} {metric_name} z={z:.1f}")
    elif active:
        es.update(index="alerts", id=active["_id"], body={"doc": {"resolved": True}})
        print(f"[detector] RESOLVED {service} {metric_name}")


def detector_loop():
    es = get_es_client()
    _status["detector"] = "running"
    print(f"[detector] running, checking every {CHECK_INTERVAL_SECONDS}s")

    while True:
        try:
            resp = es.search(index="metric_windows", body={
                "size": 0,
                "aggs": {"services": {"terms": {"field": "service", "size": 50}}},
            })
            services = [b["key"] for b in resp["aggregations"]["services"]["buckets"]]

            for service in services:
                windows = get_recent_windows(es, service, BASELINE_WINDOW_COUNT + 1)
                if len(windows) < 2:
                    continue
                latest, history = windows[0], windows[1:]
                check_metric(es, service, "error_rate",
                             latest["error_rate"], [w["error_rate"] for w in history])
                check_metric(es, service, "avg_latency_ms",
                             latest["avg_latency_ms"], [w["avg_latency_ms"] for w in history])
        except Exception as e:
            _status["last_error"] = f"detector: {e}"
            print(f"[detector] error: {e}")

        time.sleep(CHECK_INTERVAL_SECONDS)


@app.route("/health")
def health():
    return jsonify(_status), 200


@app.route("/alerts")
def alerts():
    es = get_es_client()
    resp = es.search(index="alerts", body={
        "query": {"term": {"resolved": False}},
        "sort": [{"created_at": "desc"}],
        "size": 50,
    })
    return jsonify([hit["_source"] for hit in resp["hits"]["hits"]]), 200


def start_background_threads():
    ensure_indices(get_es_client())
    threading.Thread(target=consumer_loop, daemon=True).start()
    threading.Thread(target=detector_loop, daemon=True).start()


start_background_threads()

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(os.environ.get("PORT", 5001)))
