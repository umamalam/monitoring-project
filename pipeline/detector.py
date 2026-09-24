"""
detector.py
The REACTIVE layer: "is this metric abnormal right now?" -- unchanged in
spirit from the original single-file worker, just extracted so it sits
alongside the new PROACTIVE layer (predictor.py) instead of being tangled
into worker.py. Reads from the same `timeseries` index the predictor uses,
so both layers see identical data -- this is deliberate: the reactive
alert and the earlier predictive warning for the same incident should
agree on what the metric actually did.

Every CHECK_INTERVAL_SECONDS: for each tracked (service, metric), compares
the latest value to a rolling baseline (mean/stdev of recent history) via a
z-score, and raises/resolves an alert in the `alerts` index.
"""

import os
import statistics
import time

import es_client

CHECK_INTERVAL_SECONDS = int(os.environ.get("DETECTOR_INTERVAL_SECONDS", 30))
BASELINE_WINDOW_COUNT = int(os.environ.get("DETECTOR_BASELINE_WINDOWS", 20))
MIN_BASELINE_SAMPLES = 5
Z_WARNING = 2.5
Z_CRITICAL = 4.0

# Metrics where a HIGH z-score deviation is only bad in one direction (e.g.
# request_count spiking isn't itself an incident the way error_rate spiking
# is). Kept simple and explicit rather than clever.
_DIRECTIONAL = {"request_count"}  # skip these for reactive alerting -- volume isn't a failure signal by itself


def get_active_alert(es, service, metric):
    resp = es.search(index="alerts", query={
        "bool": {"must": [
            {"term": {"service": service}},
            {"term": {"metric": metric}},
            {"term": {"resolved": False}},
        ]}
    }, size=1)
    hits = resp["hits"]["hits"]
    return hits[0] if hits else None


def check_metric(es, service, metric, latest_value, history_values):
    if len(history_values) < MIN_BASELINE_SAMPLES:
        return
    mean = statistics.mean(history_values)
    stdev = statistics.pstdev(history_values) or 1e-6
    z = (latest_value - mean) / stdev

    active = get_active_alert(es, service, metric)

    if z >= Z_WARNING:
        severity = "critical" if z >= Z_CRITICAL else "warning"
        doc = {
            "service": service, "metric": metric, "value": latest_value,
            "baseline": mean, "z_score": z, "severity": severity,
            "resolved": False, "source": "zscore",
            "message": f"{service}: {metric} = {latest_value:.2f} "
                       f"(baseline {mean:.2f}, z={z:.1f})",
        }
        import datetime as _dt
        doc["created_at"] = _dt.datetime.now(_dt.timezone.utc).isoformat()
        if active:
            es.update(index="alerts", id=active["_id"], doc=doc)
        else:
            es.index(index="alerts", document=doc)
            print(f"[detector] ALERT {service} {metric} z={z:.1f}")
    elif active:
        es.update(index="alerts", id=active["_id"], doc={"resolved": True})
        print(f"[detector] RESOLVED {service} {metric}")


def run_detection_cycle(es):
    tracked_pairs = es_client.list_tracked_metrics(es)
    for service, metric in tracked_pairs:
        if metric in _DIRECTIONAL:
            continue
        timestamps, values = es_client.get_timeseries(es, service, metric, BASELINE_WINDOW_COUNT + 1)
        if len(values) < 2:
            continue
        latest, history = values[-1], values[:-1]
        check_metric(es, service, metric, latest, history)


def detector_loop():
    es = es_client.get_es_client()
    print(f"[detector] running, checking every {CHECK_INTERVAL_SECONDS}s "
          f"against last {BASELINE_WINDOW_COUNT} windows")
    while True:
        try:
            run_detection_cycle(es)
        except Exception as e:
            print(f"[detector] error: {e}")
        time.sleep(CHECK_INTERVAL_SECONDS)
