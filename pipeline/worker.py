"""
worker.py
Runs on Render as a free "Web Service" (Render's free tier doesn't include
background workers, only web services) -- but internally it runs two real
background threads:
  1. Kafka consumer: reads app-logs from Redpanda, indexes into
     OpenSearch (Bonsai), and rolls up 1-minute metric windows.
  2. Anomaly detector: every 30s, queries OpenSearch aggregations for the
     latest window per service, compares against a rolling z-score baseline,
     and writes alerts back into OpenSearch.

The Flask app itself just exposes /health (so Render/UptimeRobot can keep it
alive) and /alerts (so Grafana or a quick check can see current alerts
without needing direct ES access).

This "single process, multiple background threads" pattern is a real,
legitimate design choice for fitting a pipeline into a constrained free-tier
environment -- worth explaining exactly like that in an interview.
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


# ---------------------------------------------------------------------------
# Thread 1: Kafka consumer -> OpenSearch (logs + rolled-up metric windows)
# ---------------------------------------------------------------------------
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


# ---------------------------------------------------------------------------
# Thread 2: Anomaly detector -> reads metric_windows, writes alerts
# ---------------------------------------------------------------------------
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
            "message": f"{service}: {metric_name} = {latest_value:.2f} "
                       f"(baseline {mean:.2f}, z={z:.1f})",
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
    print("[detector] running, checking every "
          f"{CHECK_INTERVAL_SECONDS}s against last {BASELINE_WINDOW_COUNT} windows")

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


# ---------------------------------------------------------------------------
# Flask routes (health check for Render/UptimeRobot, quick alerts view)
# ---------------------------------------------------------------------------
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


@app.route("/api/metrics")
def api_metrics():
    """Returns recent metric windows per service, for the dashboard's charts."""
    es = get_es_client()
    resp = es.search(index="metric_windows", body={
        "query": {"range": {"window_start": {"gte": "now-2h"}}},
        "sort": [{"window_start": "asc"}],
        "size": 500,
    })
    windows = [hit["_source"] for hit in resp["hits"]["hits"]]

    by_service = defaultdict(list)
    for w in windows:
        by_service[w["service"]].append({
            "t": w["window_start"],
            "error_rate": w["error_rate"],
            "avg_latency_ms": w["avg_latency_ms"],
            "request_count": w["request_count"],
        })
    return jsonify(by_service), 200


@app.route("/api/alerts")
def api_alerts():
    es = get_es_client()
    resp = es.search(index="alerts", body={
        "query": {"term": {"resolved": False}},
        "sort": [{"created_at": "desc"}],
        "size": 50,
    })
    return jsonify([hit["_source"] for hit in resp["hits"]["hits"]]), 200


@app.route("/dashboard")
def dashboard():
    return DASHBOARD_HTML


DASHBOARD_HTML = """
<!DOCTYPE html>
<html>
<head>
  <meta charset="utf-8">
  <title>Incident Monitor</title>
  <script src="https://cdnjs.cloudflare.com/ajax/libs/Chart.js/4.4.0/chart.umd.min.js"></script>
  <script src="https://cdnjs.cloudflare.com/ajax/libs/moment.js/2.29.4/moment.min.js"></script>
  <script src="https://cdnjs.cloudflare.com/ajax/libs/chartjs-adapter-moment/1.0.1/chartjs-adapter-moment.min.js"></script>
  <style>
    body { font-family: -apple-system, sans-serif; background: #0d1117; color: #e6edf3; margin: 0; padding: 24px; }
    h1 { font-size: 22px; margin-bottom: 4px; }
    .sub { color: #8b949e; font-size: 13px; margin-bottom: 24px; }
    .cards { display: flex; gap: 16px; margin-bottom: 24px; }
    .card { background: #161b22; border: 1px solid #30363d; border-radius: 8px; padding: 16px; flex: 1; }
    .card .num { font-size: 28px; font-weight: 700; }
    .card .label { color: #8b949e; font-size: 13px; }
    .critical { color: #f85149; }
    .ok { color: #3fb950; }
    .charts { display: grid; grid-template-columns: 1fr 1fr; gap: 16px; margin-bottom: 24px; }
    .chart-box { background: #161b22; border: 1px solid #30363d; border-radius: 8px; padding: 16px; }
    .chart-box h3 { margin: 0 0 12px 0; font-size: 14px; color: #c9d1d9; }
    canvas { max-height: 280px; }
    .alert-row { background: #2d1a1a; border-left: 3px solid #f85149; padding: 10px 14px; border-radius: 4px; margin-bottom: 8px; font-size: 13px; }
    .no-alerts { color: #3fb950; padding: 10px 14px; }
  </style>
</head>
<body>
  <h1>📡 Incident Monitor</h1>
  <div class="sub">Live view over OpenSearch — refreshes every 10s</div>

  <div class="cards">
    <div class="card"><div class="num" id="alert-count">-</div><div class="label">Active alerts</div></div>
    <div class="card"><div class="num" id="req-count">-</div><div class="label">Requests (last 2h)</div></div>
    <div class="card"><div class="num" id="service-count">-</div><div class="label">Services reporting</div></div>
  </div>

  <div id="alerts-section"></div>

  <div class="charts">
    <div class="chart-box"><h3>Error rate by service</h3><canvas id="errorChart"></canvas></div>
    <div class="chart-box"><h3>Average latency (ms) by service</h3><canvas id="latencyChart"></canvas></div>
  </div>

  <script>
    const colors = ['#58a6ff', '#3fb950', '#f0883e', '#a371f7', '#f85149', '#39c5cf'];
    let errorChart, latencyChart;

    function makeChart(ctx, label) {
      return new Chart(ctx, {
        type: 'line',
        data: { datasets: [] },
        options: {
          responsive: true,
          scales: {
            x: { type: 'time', time: { unit: 'minute' }, ticks: { color: '#8b949e' }, grid: { color: '#30363d' } },
            y: { ticks: { color: '#8b949e' }, grid: { color: '#30363d' }, beginAtZero: true }
          },
          plugins: { legend: { labels: { color: '#c9d1d9' } } }
        }
      });
    }

    async function refresh() {
      const [metricsRes, alertsRes] = await Promise.all([
        fetch('/api/metrics'), fetch('/api/alerts')
      ]);
      const metrics = await metricsRes.json();
      const alerts = await alertsRes.json();

      const services = Object.keys(metrics);
      document.getElementById('service-count').textContent = services.length;
      document.getElementById('alert-count').textContent = alerts.length;
      document.getElementById('alert-count').className = 'num ' + (alerts.length > 0 ? 'critical' : 'ok');

      let totalReq = 0;
      services.forEach(s => metrics[s].forEach(w => totalReq += w.request_count));
      document.getElementById('req-count').textContent = totalReq;

      const alertSection = document.getElementById('alerts-section');
      if (alerts.length === 0) {
        alertSection.innerHTML = '<div class="no-alerts">✅ No active anomalies</div>';
      } else {
        alertSection.innerHTML = alerts.map(a =>
          `<div class="alert-row"><b>${a.severity.toUpperCase()}</b> — ${a.message}</div>`
        ).join('');
      }

      if (!errorChart) {
        errorChart = makeChart(document.getElementById('errorChart'), 'error_rate');
        latencyChart = makeChart(document.getElementById('latencyChart'), 'latency');
      }

      errorChart.data.datasets = services.map((s, i) => ({
        label: s, borderColor: colors[i % colors.length], data: metrics[s].map(w => ({x: w.t, y: w.error_rate})), tension: 0.3
      }));
      latencyChart.data.datasets = services.map((s, i) => ({
        label: s, borderColor: colors[i % colors.length], data: metrics[s].map(w => ({x: w.t, y: w.avg_latency_ms})), tension: 0.3
      }));
      errorChart.update();
      latencyChart.update();
    }

    refresh();
    setInterval(refresh, 10000);
  </script>
</body>
</html>
"""


def start_background_threads():
    ensure_indices(get_es_client())
    threading.Thread(target=consumer_loop, daemon=True).start()
    threading.Thread(target=detector_loop, daemon=True).start()


start_background_threads()

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(os.environ.get("PORT", 5001)))
