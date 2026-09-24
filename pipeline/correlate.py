"""
correlate.py
A real incident rarely shows up in one metric alone. This module checks
whether OTHER metrics for the same service are trending in a direction
consistent with trouble at the same time as the one a prediction was just
made for -- and uses that agreement to raise (or lower) confidence.

Deliberately simple: this is a rising/falling trend agreement check, not a
statistical correlation coefficient. That's an honest trade-off for a
demo-scale project -- a real production version would look at lagged cross-
correlation or a shared latent factor (e.g. PCA over all metrics), but a
same-direction-trend check is explainable in one sentence, cheap to compute,
and still catches the common case: "connections rising AND latency rising
AND error rate rising" is a much stronger signal than any one of those
alone.
"""

from forecaster import fit_trend

# Whether a rising trend in each metric is itself a bad sign. Used to decide
# whether two metrics trending together are corroborating (both getting
# worse) or just coincidental.
_BAD_WHEN_RISING = {
    "error_rate": True,
    "avg_latency_ms": True,
    "p95_latency_ms": True,
    "active_connections": True,
    "request_count": False,  # more traffic isn't inherently bad
}


def _trend_direction(fit, noise_floor):
    if fit is None:
        return None
    if abs(fit["slope_per_hour"]) < noise_floor:
        return "flat"
    return "rising" if fit["slope_per_hour"] > 0 else "falling"


def find_corroborating_signals(es_get_timeseries, service, target_metric, candidate_metrics, window=40):
    """
    es_get_timeseries: a callable(service, metric, limit) -> (timestamps, values),
    so this module doesn't import es_client directly and stays easy to unit test.

    Returns a list of metric names that are trending in the same "getting
    worse" direction as target_metric, for the given service.
    """
    target_is_bad_rising = _BAD_WHEN_RISING.get(target_metric, True)
    corroborating = []

    for metric in candidate_metrics:
        if metric == target_metric:
            continue
        ts, vals = es_get_timeseries(service, metric, window)
        if len(vals) < 5:
            continue
        fit = fit_trend(ts, vals)
        noise_floor = 0.02 * (max(vals) - min(vals) + 1e-6)
        direction = _trend_direction(fit, noise_floor)
        if direction == "flat" or direction is None:
            continue

        metric_is_bad_rising = _BAD_WHEN_RISING.get(metric, True)
        metric_getting_worse = (direction == "rising") == metric_is_bad_rising
        target_getting_worse_meaning = target_is_bad_rising  # target was already established as trending toward threshold

        if metric_getting_worse:
            corroborating.append(metric)

    return corroborating


def confidence_boost(base_score, corroborating_count):
    """Each corroborating signal nudges confidence up, with diminishing
    returns, capped so a single-metric prediction can never claim to be
    fully certain just because two noisy metrics happened to drift together."""
    boosted = base_score + (1 - base_score) * min(corroborating_count, 3) * 0.15
    return round(min(boosted, 0.97), 3)
