"""
ingest.py
Thread 1 of the pipeline: consumes raw events from Kafka, indexes each one
into `logs` (full detail, for debugging/search), and rolls them up into the
unified `timeseries` index that both the reactive detector and the
predictor read from.

Two event shapes come through the same topic:
  - "request" events (from app.py's log_request): each one contributes to
    that window's request_count / error_count / latency stats for its
    service.
  - "gauge" events (from app.py's log_gauge, e.g. active_connections): each
    one is a direct point-in-time reading -- the window's value is just the
    last gauge reading seen in that window, not an aggregate.

Every WINDOW_SECONDS, whatever has accumulated in the current window is
flushed to `timeseries` as one document per (service, metric).
"""

import json
import os
import time
from collections import defaultdict
from datetime import datetime, timezone

import es_client
from kafka_client import get_consumer

WINDOW_SECONDS = int(os.environ.get("WINDOW_SECONDS", 60))

_status = {"consumer": "starting", "last_error": None}


def _new_bucket():
    return {"count": 0, "errors": 0, "latencies": [], "gauges": {}}


def _apply_event(buckets, event):
    service = event.get("service", "unknown")
    b = buckets[service]

    if event.get("type") == "gauge":
        # last-value-wins within the window -- a gauge describes current
        # state, not something you sum or count.
        b["gauges"][event["metric"]] = event["value"]
        return

    # default / "request" events
    b["count"] += 1
    if event.get("level") == "ERROR":
        b["errors"] += 1
    if "latency_ms" in event:
        b["latencies"].append(event["latency_ms"])


def flush_window(es, window_start, buckets):
    for service, stats in buckets.items():
        if stats["count"] > 0:
            count = stats["count"]
            errors = stats["errors"]
            latencies = sorted(stats["latencies"]) or [0]
            avg_latency = sum(latencies) / len(latencies)
            p95_idx = min(len(latencies) - 1, int(len(latencies) * 0.95))

            es_client.write_timeseries_point(es, service, "request_count", count, window_start)
            es_client.write_timeseries_point(es, service, "error_count", errors, window_start)
            es_client.write_timeseries_point(es, service, "error_rate", errors / count, window_start)
            es_client.write_timeseries_point(es, service, "avg_latency_ms", avg_latency, window_start)
            es_client.write_timeseries_point(es, service, "p95_latency_ms", latencies[p95_idx], window_start)

        for metric, value in stats["gauges"].items():
            es_client.write_timeseries_point(es, service, metric, value, window_start)

    print(f"[ingest] flushed window starting {window_start}")


def consumer_loop():
    es = es_client.get_es_client()
    consumer = get_consumer()

    window_start = datetime.now(timezone.utc)
    buckets = defaultdict(_new_bucket)
    last_flush = time.time()

    _status["consumer"] = "running"
    print("[ingest] connected to Kafka + Elasticsearch, consuming...")

    for message in consumer:
        try:
            event = json.loads(message.value)
            es.index(index="logs", document=event)
            _apply_event(buckets, event)
        except Exception as e:
            _status["last_error"] = f"ingest: {e}"
            print(f"[ingest] error processing message: {e}")

        if time.time() - last_flush >= WINDOW_SECONDS:
            flush_window(es, window_start, buckets)
            buckets = defaultdict(_new_bucket)
            window_start = datetime.now(timezone.utc)
            last_flush = time.time()
