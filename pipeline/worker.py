"""
worker.py
Runs on Render as a free "Web Service" (Render's free tier doesn't include
background workers, only web services) -- but internally it runs FIVE real
background threads, each in its own module now that the pipeline covers
reactive, ML-based, and proactive monitoring:

  1. ingest.consumer_loop      Kafka -> Elasticsearch: indexes raw logs and
                                rolls request/gauge events into the unified
                                `timeseries` index.
  2. detector.detector_loop    REACTIVE: is a single metric abnormal right
                                now? (rolling z-score vs recent baseline)
  3. ml_detector.ml_detector_loop  REACTIVE, MULTIVARIATE: is this
                                service's *combination* of metrics unusual
                                right now, even if no single metric crosses
                                its own z-score threshold? (Isolation
                                Forest, retrained each cycle)
  4. predictor.predictor_loop  PROACTIVE: is a metric trending toward a
                                threshold, and when will it get there?
                                (trend forecast + multi-signal correlation)
  5. accuracy.accuracy_loop    Checks past predictions against what
                                actually happened, so prediction quality is
                                measurable, not just asserted.

The Flask app itself just exposes read endpoints -- /health (so
Render/UptimeRobot can keep it alive), /alerts (both detector layers,
distinguished by a `source` field), /predictions (proactive), and
/predictions/accuracy (how good the predictions have been) -- so Grafana
or a quick curl can see current state without needing direct Elasticsearch
access.

This "single process, multiple background threads" pattern is a real,
deliberate design choice for fitting a five-stage pipeline onto a
constrained free-tier environment without a paid background-worker plan.
"""

import os
import threading

from flask import Flask, jsonify

import es_client
import ingest
import detector
import ml_detector
import predictor
import accuracy

app = Flask(__name__)


@app.route("/health")
def health():
    return jsonify({
        "ingest": ingest._status,
        "detector": "running",
        "ml_detector": "running",
        "predictor": "running",
        "accuracy": "running",
    }), 200


@app.route("/alerts")
def alerts():
    """All active alerts from both detector layers. Optional ?source=zscore
    or ?source=isolation_forest to see just one layer -- useful for
    demoing that the multivariate detector is catching things the
    single-metric one wouldn't (and vice versa)."""
    from flask import request
    es = es_client.get_es_client()
    must = [{"term": {"resolved": False}}]
    source = request.args.get("source")
    if source:
        must.append({"term": {"source": source}})
    resp = es.search(index="alerts", query={"bool": {"must": must}},
                      sort=[{"created_at": "desc"}], size=50)
    return jsonify([hit["_source"] for hit in resp["hits"]["hits"]]), 200


@app.route("/predictions")
def predictions():
    """Active (unresolved) predictions -- the 'this will break in N hours' feed."""
    es = es_client.get_es_client()
    resp = es.search(index="predictions", query={"term": {"resolved": False}},
                      sort=[{"hours_until_threshold": "asc"}], size=50)
    return jsonify([hit["_source"] for hit in resp["hits"]["hits"]]), 200


@app.route("/predictions/accuracy")
def predictions_accuracy():
    """How good have the predictions actually been? See accuracy.py."""
    es = es_client.get_es_client()
    return jsonify(accuracy.summarize_accuracy(es)), 200


def start_background_threads():
    es_client.ensure_indices(es_client.get_es_client())
    threading.Thread(target=ingest.consumer_loop, daemon=True).start()
    threading.Thread(target=detector.detector_loop, daemon=True).start()
    threading.Thread(target=ml_detector.ml_detector_loop, daemon=True).start()
    threading.Thread(target=predictor.predictor_loop, daemon=True).start()
    threading.Thread(target=accuracy.accuracy_loop, daemon=True).start()


start_background_threads()

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(os.environ.get("PORT", 5001)))
