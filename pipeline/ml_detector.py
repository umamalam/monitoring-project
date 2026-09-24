"""
ml_detector.py
The actual ML layer: multivariate anomaly detection via Isolation Forest,
running alongside (not instead of) detector.py's single-metric z-score.

Why this is a real gap the z-score can't cover: detector.py asks "is THIS
metric unusual on its own?" one metric at a time. It can't see that
error_rate, avg_latency_ms, and active_connections are each only mildly
elevated at the same moment -- individually not even 2 standard deviations
out, so no single z-score fires -- while the *combination* is a much
stronger signal that something is starting to go wrong. That's exactly
what Isolation Forest is for: it isolates points by repeatedly splitting
on random features/thresholds, and outliers -- points that don't look like
the bulk of the data in the combined feature space -- get isolated in
fewer splits than normal points. No labeled "this was an incident" data is
required, which matters because this project doesn't have any.

Per service, every CHECK_INTERVAL_SECONDS:
  1. Pull the last WINDOW_HISTORY aligned windows across every metric that
     service reports (es_client.get_service_metric_matrix).
  2. Standardize each metric column (z-score it) so metrics on very
     different scales -- latency in ms vs. error_rate in [0, 1] -- 
     contribute comparably instead of whichever metric has the biggest raw
     numbers dominating the split decisions.
  3. Fit a fresh IsolationForest on the history *excluding* the newest
     window, then score that newest window out-of-sample. Retraining every
     cycle is cheap at this data volume (a few hundred rows, a handful of
     features) and means "normal" always reflects how this service has
     actually behaved recently -- there's no stale model to go retrain by
     hand. The real cost of this choice: nothing here is a persisted,
     versioned model you could roll back to a known-good state; it's
     recomputed from scratch every cycle, which is fine for a demo and not
     how you'd run this against real production traffic.
  4. If the newest window is flagged, raise an alert in the shared
     `alerts` index (source="isolation_forest") naming the metrics whose
     standardized values deviate most -- so the alert is explainable
     ("active_connections, error_rate, avg_latency_ms all elevated
     together") rather than a bare "the model said so".
"""

import os
import time
from datetime import datetime, timezone

import numpy as np
from sklearn.ensemble import IsolationForest

import es_client

CHECK_INTERVAL_SECONDS = int(os.environ.get("ML_DETECTOR_INTERVAL_SECONDS", 45))
WINDOW_HISTORY = int(os.environ.get("ML_DETECTOR_HISTORY_WINDOWS", 200))
MIN_TRAINING_POINTS = int(os.environ.get("ML_DETECTOR_MIN_POINTS", 30))  # too little history to learn a shape from
MIN_METRICS = 2           # multivariate needs >=2 signals; a lone metric is what detector.py already covers
CONTAMINATION = 0.05      # assumed fraction of noisy-but-normal windows in the training history -- this IS the decision threshold
CRITICAL_SCORE = -0.1     # decision_function is negative on the outlier side; used only to grade severity of an already-flagged point, not to gate it


def _standardize(matrix):
    arr = np.asarray(matrix, dtype=float)
    mean = arr.mean(axis=0)
    std = arr.std(axis=0)
    std[std == 0] = 1e-9  # a metric that hasn't moved at all shouldn't blow up division
    return (arr - mean) / std


def _get_active_alert(es, service):
    resp = es.search(index="alerts", query={
        "bool": {"must": [
            {"term": {"service": service}},
            {"term": {"metric": "multivariate"}},
            {"term": {"source": "isolation_forest"}},
            {"term": {"resolved": False}},
        ]}
    }, size=1)
    hits = resp["hits"]["hits"]
    return hits[0] if hits else None


def check_service(es, service):
    _, metric_names, matrix = es_client.get_service_metric_matrix(es, service, WINDOW_HISTORY)
    if len(metric_names) < MIN_METRICS or len(matrix) < MIN_TRAINING_POINTS:
        return

    scaled = _standardize(matrix)
    train, latest = scaled[:-1], scaled[-1]  # score the newest window out-of-sample, never train on itself

    model = IsolationForest(contamination=CONTAMINATION, random_state=42, n_estimators=100)
    model.fit(train)
    score = float(model.decision_function([latest])[0])
    # predict() applies the contamination-calibrated cutoff for us -- that
    # IS the decision of whether this window is an outlier. `score` is kept
    # only to grade how severe an already-flagged point is, not as a second
    # independent gate (a point can legitimately have a barely-negative
    # score and still be the most isolated point in the batch).
    is_outlier = bool(model.predict([latest])[0] == -1)

    active = _get_active_alert(es, service)

    if is_outlier:
        deviations = sorted(zip(metric_names, latest), key=lambda pair: -abs(pair[1]))
        top = deviations[:3]
        readable = ", ".join(f"{name} ({val:+.1f}\u03c3)" for name, val in top)

        doc = {
            "service": service, "metric": "multivariate", "value": None,
            "baseline": None, "z_score": None,
            "severity": "critical" if score < CRITICAL_SCORE else "warning",
            "resolved": False, "source": "isolation_forest",
            "anomaly_score": score,
            "contributing_metrics": [name for name, _ in top],
            "message": f"{service}: unusual combination across signals -- {readable} "
                       f"(anomaly score {score:.3f})",
            "created_at": datetime.now(timezone.utc).isoformat(),
        }
        if active:
            es.update(index="alerts", id=active["_id"], doc=doc)
        else:
            es.index(index="alerts", document=doc)
            print(f"[ml_detector] ANOMALY {service} score={score:.3f} "
                  f"top={[name for name, _ in top]}")
    elif active:
        es.update(index="alerts", id=active["_id"], doc={"resolved": True})
        print(f"[ml_detector] RESOLVED {service} (multivariate)")


def run_detection_cycle(es):
    services = {service for service, _ in es_client.list_tracked_metrics(es)}
    for service in services:
        try:
            check_service(es, service)
        except Exception as e:
            print(f"[ml_detector] error checking {service}: {e}")


def ml_detector_loop():
    es = es_client.get_es_client()
    print(f"[ml_detector] running, checking every {CHECK_INTERVAL_SECONDS}s "
          f"(Isolation Forest over a {WINDOW_HISTORY}-window rolling history)")
    while True:
        try:
            run_detection_cycle(es)
        except Exception as e:
            print(f"[ml_detector] error: {e}")
        time.sleep(CHECK_INTERVAL_SECONDS)
