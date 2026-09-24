"""
accuracy.py
Runs as a fourth background thread. Predictions are only useful if you can
show they're trustworthy, so this periodically checks predictions whose
predicted crossing time has already passed and marks each one:

  - "confirmed"       the metric actually crossed the threshold at or
                       before predicted_at_latest
  - "false_positive"   predicted_at_latest has passed and it never crossed
  - "pending"          predicted_at_latest hasn't arrived yet -- too early
                       to judge

For confirmed predictions, it also records how many hours off the point
estimate was from the actual crossing time (`error_hours`) -- this is what
turns "predictions are accurate" into a measurable number: "predictions
were accurate to within X hours across N incidents", instead of a vague
accuracy claim.
"""

import os
import time
from datetime import datetime, timedelta, timezone

import es_client

CHECK_INTERVAL_SECONDS = int(os.environ.get("ACCURACY_INTERVAL_SECONDS", 300))


def _parse(ts):
    return datetime.fromisoformat(ts.replace("Z", "+00:00"))


def _find_actual_crossing(es, service, metric, threshold, direction, after_iso):
    """Looks through the metric's history after a given time for the first
    point that actually crossed the threshold. Returns that point's
    timestamp, or None if it never did (within what's been recorded)."""
    timestamps, values = es_client.get_timeseries(es, service, metric, limit=500)
    for ts, val in zip(timestamps, values):
        if ts <= after_iso:
            continue
        crossed = (val >= threshold) if direction == "above" else (val <= threshold)
        if crossed:
            return ts
    return None


def check_predictions(es, thresholds):
    resp = es.search(index="predictions", query={"term": {"outcome": "pending"}}, size=200)
    now = datetime.now(timezone.utc)

    for hit in resp["hits"]["hits"]:
        doc = hit["_source"]
        pred_id = hit["_id"]
        # Some low-confidence predictions have no "latest" bound (the slow
        # edge of the slope range never actually crosses the threshold).
        # Fall back to predicted_at + 24h so those don't sit as "pending"
        # forever with nothing ever able to judge them.
        latest = doc.get("predicted_at_latest") or None
        judge_after = _parse(latest) if latest else (_parse(doc["predicted_at"]) + timedelta(hours=24))
        if judge_after > now:
            continue  # too early to judge this one yet

        cfg = thresholds.get(doc["metric"])
        if cfg is None:
            continue

        actual_ts = _find_actual_crossing(
            es, doc["service"], doc["metric"], cfg["limit"], cfg["direction"],
            after_iso=doc["created_at"],
        )

        update = {"updated_at": now.isoformat()}
        if actual_ts:
            predicted_dt = _parse(doc["predicted_at"])
            actual_dt = _parse(actual_ts)
            error_hours = abs((actual_dt - predicted_dt).total_seconds()) / 3600.0
            update.update({
                "outcome": "confirmed",
                "actual_crossed_at": actual_ts,
                "error_hours": round(error_hours, 2),
            })
            print(f"[accuracy] CONFIRMED {pred_id}: predicted {doc['predicted_at']}, "
                  f"actual {actual_ts} (off by {error_hours:.1f}h)")
        else:
            update.update({"outcome": "false_positive"})
            print(f"[accuracy] FALSE POSITIVE {pred_id}: never crossed {cfg['limit']} "
                  f"by {latest}")

        es.update(index="predictions", id=pred_id, doc=update)


def summarize_accuracy(es):
    """Returns the numbers you'd actually want to quote: how many
    predictions have been judged, how many were right, and mean error in
    hours for the ones that were."""
    resp = es.search(index="predictions", query={
        "bool": {"must": [{"terms": {"outcome": ["confirmed", "false_positive"]}}]}
    }, size=500)
    docs = [hit["_source"] for hit in resp["hits"]["hits"]]
    confirmed = [d for d in docs if d["outcome"] == "confirmed"]
    total_judged = len(docs)
    if total_judged == 0:
        return {"judged": 0, "confirmed": 0, "false_positives": 0, "accuracy": None, "mean_error_hours": None}
    errors = [d["error_hours"] for d in confirmed if "error_hours" in d]
    return {
        "judged": total_judged,
        "confirmed": len(confirmed),
        "false_positives": total_judged - len(confirmed),
        "accuracy": round(len(confirmed) / total_judged, 3),
        "mean_error_hours": round(sum(errors) / len(errors), 2) if errors else None,
    }


def accuracy_loop():
    es = es_client.get_es_client()
    from predictor import load_thresholds
    thresholds = load_thresholds()
    print(f"[accuracy] running, checking every {CHECK_INTERVAL_SECONDS}s")
    while True:
        try:
            check_predictions(es, thresholds)
        except Exception as e:
            print(f"[accuracy] error: {e}")
        time.sleep(CHECK_INTERVAL_SECONDS)
