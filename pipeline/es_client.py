"""
es_client.py
Shared Elasticsearch connection helper, pointed at Bonsai (a free-tier
managed Elasticsearch host). Bonsai gives you a single URL that already
includes credentials, e.g.:
  https://<user>:<pass>@yourcluster-123456.us-east-1.bonsaisearch.net:443

Set this as the BONSAI_URL environment variable.
"""

import os

BONSAI_URL = os.environ.get("BONSAI_URL", "http://localhost:9200")


def get_es_client():
    # Imported lazily so the rest of this module (query/aggregation builders
    # used by predictor.py, detector.py, accuracy.py) can be imported and
    # unit-tested against a fake client without requiring the real
    # elasticsearch package to be installed.
    from elasticsearch import Elasticsearch
    return Elasticsearch(BONSAI_URL)


def ensure_indices(es):
    """Create indices with sane mappings if they don't already exist."""
    indices = {
        "logs": {
            "mappings": {"properties": {
                "timestamp": {"type": "date"},
                "service": {"type": "keyword"},
                "level": {"type": "keyword"},
                "endpoint": {"type": "keyword"},
                "status_code": {"type": "integer"},
                "latency_ms": {"type": "float"},
                "message": {"type": "text"},
            }}
        },
        # Unified time-series store: every tracked signal -- request-derived
        # metrics (error_rate, avg_latency_ms, ...) AND point-in-time gauges
        # (active_connections, ...) -- lands here as one document per
        # (service, metric, window). One shared schema means the forecaster
        # and correlator can treat every signal the same way, and Grafana
        # only needs a single Elasticsearch data source (filter by `metric`)
        # instead of three separate ones.
        "timeseries": {
            "mappings": {"properties": {
                "window_start": {"type": "date"},
                "service": {"type": "keyword"},
                "metric": {"type": "keyword"},
                "value": {"type": "float"},
            }}
        },
        "alerts": {
            "mappings": {"properties": {
                "created_at": {"type": "date"},
                "service": {"type": "keyword"},
                "metric": {"type": "keyword"},
                "value": {"type": "float"},
                "baseline": {"type": "float"},
                "z_score": {"type": "float"},
                "severity": {"type": "keyword"},
                "resolved": {"type": "boolean"},
                "message": {"type": "text"},
                # "zscore" (detector.py, single-metric) or "isolation_forest"
                # (ml_detector.py, multivariate) -- lets Grafana/consumers
                # tell the two detection layers apart in one shared index.
                "source": {"type": "keyword"},
                "anomaly_score": {"type": "float"},          # isolation_forest only
                "contributing_metrics": {"type": "keyword"},  # isolation_forest only
            }}
        },
        # Proactive predictions -- "this will cross a threshold in the
        # future", as opposed to `alerts`, which is "this is abnormal now".
        "predictions": {
            "mappings": {"properties": {
                "created_at": {"type": "date"},
                "updated_at": {"type": "date"},
                "service": {"type": "keyword"},
                "metric": {"type": "keyword"},
                "threshold": {"type": "float"},
                "current_value": {"type": "float"},
                "trend_per_hour": {"type": "float"},
                "predicted_at": {"type": "date"},        # best-estimate crossing time
                "predicted_at_earliest": {"type": "date"},
                "predicted_at_latest": {"type": "date"},
                "hours_until_threshold": {"type": "float"},
                "confidence": {"type": "keyword"},        # low / medium / high
                "confidence_score": {"type": "float"},    # 0-1, drives the label above
                "supporting_signals": {"type": "keyword"},  # other metrics trending the same way
                "method": {"type": "keyword"},            # linear / holt
                "resolved": {"type": "boolean"},          # threshold breach happened, or trend reversed
                "outcome": {"type": "keyword"},           # confirmed / false_positive / expired / pending
                "message": {"type": "text"},
            }}
        },
    }
    for name, body in indices.items():
        if not es.indices.exists(index=name):
            es.indices.create(index=name, **body)
            print(f"[es_client] created index '{name}'")


def write_timeseries_point(es, service, metric, value, window_start):
    es.index(index="timeseries", document={
        "window_start": window_start.isoformat(),
        "service": service,
        "metric": metric,
        "value": value,
    })


def get_timeseries(es, service, metric, limit=60):
    """Returns (timestamps, values) for a (service, metric) pair, oldest first."""
    resp = es.search(index="timeseries", query={
        "bool": {"must": [{"term": {"service": service}}, {"term": {"metric": metric}}]}
    }, sort=[{"window_start": "desc"}], size=limit)
    hits = list(reversed(resp["hits"]["hits"]))
    timestamps = [hit["_source"]["window_start"] for hit in hits]
    values = [hit["_source"]["value"] for hit in hits]
    return timestamps, values


def get_service_metric_matrix(es, service, limit=200):
    """
    Pulls recent timeseries history for every metric a service reports and
    pivots it into a matrix aligned by window_start: one row per window,
    one column per metric. Windows line up naturally because ingest.py
    flushes every metric for a service from the same window_start each
    cycle (see flush_window in ingest.py) -- this is what makes a
    multivariate view possible without a separate resampling step.

    A metric that had no data in a given window (e.g. a request-derived
    metric during a quiet window) is forward-filled from its last known
    value; any still-missing leading cells are back-filled from the first
    known value in that column. This keeps every row a complete feature
    vector, which ml_detector.py's IsolationForest requires -- the
    trade-off is that a genuinely missing reading looks identical to "flat,
    unchanged", which is fine for demo-scale gaps but worth knowing about.

    Returns (window_starts, metric_names, matrix) where matrix is a
    len(window_starts) x len(metric_names) list of lists, oldest first.
    """
    resp = es.search(index="timeseries", query={"term": {"service": service}},
                      sort=[{"window_start": "desc"}], size=limit * 10)
    hits = list(reversed(resp["hits"]["hits"]))

    by_window = {}
    order = []
    metrics_seen = set()
    for hit in hits:
        src = hit["_source"]
        ws = src["window_start"]
        if ws not in by_window:
            by_window[ws] = {}
            order.append(ws)
        by_window[ws][src["metric"]] = src["value"]
        metrics_seen.add(src["metric"])

    metric_names = sorted(metrics_seen)
    window_starts = order[-limit:]

    matrix = []
    last_known = {}
    for ws in window_starts:
        cell = by_window[ws]
        row = []
        for m in metric_names:
            if m in cell:
                last_known[m] = cell[m]
            row.append(last_known.get(m))
        matrix.append(row)

    for col in range(len(metric_names)):
        first_val = next((matrix[r][col] for r in range(len(matrix))
                           if matrix[r][col] is not None), 0.0)
        for r in range(len(matrix)):
            if matrix[r][col] is None:
                matrix[r][col] = first_val

    return window_starts, metric_names, matrix


def list_tracked_metrics(es, lookback_windows=500):
    """Returns the distinct (service, metric) pairs currently being written."""
    resp = es.search(index="timeseries", size=0, aggs={
        "services": {"terms": {"field": "service", "size": 50}, "aggs": {
            "metrics": {"terms": {"field": "metric", "size": 50}}
        }}
    })
    pairs = []
    for s_bucket in resp["aggregations"]["services"]["buckets"]:
        for m_bucket in s_bucket["metrics"]["buckets"]:
            pairs.append((s_bucket["key"], m_bucket["key"]))
    return pairs
