"""
predictor.py
The "predict failures before they happen" layer -- runs as a third
background thread inside worker.py, alongside the existing Kafka consumer
and the existing reactive z-score detector.

Every CHECK_INTERVAL_SECONDS, for every (service, metric) pair that has a
configured threshold in thresholds.json:

  1. Pull that metric's recent history from the `timeseries` index.
  2. Fit a trend forecast (forecaster.fit_trend / time_to_threshold).
  3. If the trend is heading toward the threshold within the configured
     horizon, check whether other metrics for the same service are
     trending the same way (correlate.py) and use that to set confidence.
  4. Write (or update) a document in the `predictions` index.

A prediction gets a STABLE id (service + metric), so repeated detections
update the same document -- exactly like the existing alert de-duplication
in the reactive detector -- instead of spamming a new row every cycle. When
a metric's trend reverses or the crossing time moves outside the horizon,
the existing prediction is marked resolved (trend_reversed) rather than
left dangling.
"""

import json
import os
import time
from datetime import datetime, timezone

import es_client
import forecaster
from correlate import find_corroborating_signals, confidence_boost

CHECK_INTERVAL_SECONDS = int(os.environ.get("PREDICTOR_INTERVAL_SECONDS", 60))
HISTORY_WINDOW = int(os.environ.get("PREDICTOR_HISTORY_WINDOW", 120))

_THRESHOLDS_PATH = os.path.join(os.path.dirname(__file__), "thresholds.json")


def load_thresholds():
    with open(_THRESHOLDS_PATH) as f:
        raw = json.load(f)
    return {k: v for k, v in raw.items() if not k.startswith("_")}


def _prediction_id(service, metric):
    return f"{service}:{metric}"


def _get_existing(es, pred_id):
    """Returns the existing prediction doc for this id, or None. Written as
    a try/except rather than the old ignore=[404] shorthand, which the
    elasticsearch-py 8.x client dropped -- a missing document now raises
    NotFoundError instead of returning a {'found': False} body."""
    try:
        resp = es.get(index="predictions", id=pred_id)
        return resp["_source"] if resp.get("found", True) else None
    except Exception:
        return None


def _make_timeseries_getter(es):
    def _get(service, metric, limit):
        return es_client.get_timeseries(es, service, metric, limit)
    return _get


def run_prediction_cycle(es, thresholds):
    tracked_pairs = es_client.list_tracked_metrics(es)
    services = sorted({service for service, _ in tracked_pairs})
    all_metrics = sorted({metric for _, metric in tracked_pairs})
    get_ts = _make_timeseries_getter(es)

    for service in services:
        for metric, cfg in thresholds.items():
            if (service, metric) not in tracked_pairs:
                continue  # this service doesn't emit this metric -- nothing to forecast

            timestamps, values = es_client.get_timeseries(es, service, metric, HISTORY_WINDOW)
            result = forecaster.time_to_threshold(
                timestamps, values,
                threshold=cfg["limit"],
                direction=cfg["direction"],
                max_horizon_hours=cfg["horizon_hours"],
            )

            pred_id = _prediction_id(service, metric)

            if result is None:
                _resolve_if_exists(es, pred_id, reason="trend_no_longer_heading_to_threshold")
                continue

            corroborating = find_corroborating_signals(
                get_ts, service, metric,
                candidate_metrics=[m for m in all_metrics if m != metric],
            )
            final_confidence_score = confidence_boost(result["confidence_score"], len(corroborating))
            final_confidence = (
                "high" if final_confidence_score >= 0.75 else
                "medium" if final_confidence_score >= 0.5 else
                "low"
            )

            now = datetime.now(timezone.utc)
            doc = {
                "updated_at": now.isoformat(),
                "service": service,
                "metric": metric,
                "threshold": cfg["limit"],
                "current_value": result["current_value"],
                "trend_per_hour": result["trend_per_hour"],
                "predicted_at": result["predicted_at"].isoformat(),
                "predicted_at_earliest": result["predicted_at_earliest"].isoformat(),
                "predicted_at_latest": (
                    result["predicted_at_latest"].isoformat() if result["predicted_at_latest"] else None
                ),
                "hours_until_threshold": round(result["hours_until_threshold"], 2),
                "confidence": final_confidence,
                "confidence_score": final_confidence_score,
                "supporting_signals": corroborating,
                "method": result["method"],
                "resolved": False,
                "outcome": "pending",
                "message": _human_message(service, metric, cfg, result, corroborating),
            }

            if _get_existing(es, pred_id) is not None:
                es.update(index="predictions", id=pred_id, doc=doc)
            else:
                doc["created_at"] = now.isoformat()
                es.index(index="predictions", id=pred_id, document=doc)
                print(f"[predictor] NEW PREDICTION {doc['message']}")


def _resolve_if_exists(es, pred_id, reason):
    existing = _get_existing(es, pred_id)
    if existing is not None and not existing.get("resolved"):
        es.update(index="predictions", id=pred_id, doc={
            "resolved": True,
            "outcome": "trend_reversed",
            "updated_at": datetime.now(timezone.utc).isoformat(),
            "message": existing.get("message", "") + f" (resolved: {reason})",
        })
        print(f"[predictor] RESOLVED {pred_id} ({reason})")


def _human_message(service, metric, cfg, result, corroborating):
    hrs = result["hours_until_threshold"]
    when = f"{hrs:.1f} hours" if hrs < 48 else f"{hrs / 24:.1f} days"
    base = (
        f"{service}: {cfg.get('description', metric)} -- "
        f"{metric} is at {result['current_value']:.2f}, trending "
        f"{result['trend_per_hour']:+.2f}/hr toward the limit of {cfg['limit']}. "
        f"Projected to cross in ~{when} "
        f"({result['confidence']} confidence)."
    )
    if corroborating:
        base += f" Corroborated by rising {', '.join(corroborating)}."
    return base


def predictor_loop():
    es = es_client.get_es_client()
    thresholds = load_thresholds()
    print(f"[predictor] running, checking every {CHECK_INTERVAL_SECONDS}s "
          f"against thresholds for: {', '.join(thresholds.keys())}")
    while True:
        try:
            run_prediction_cycle(es, thresholds)
        except Exception as e:
            print(f"[predictor] error: {e}")
        time.sleep(CHECK_INTERVAL_SECONDS)
