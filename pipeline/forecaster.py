"""
forecaster.py
Turns a metric's recent history (timestamps + values, pulled from the
`timeseries` Elasticsearch index) into a forward projection: at the current
trend, when will this metric cross a given threshold?

Two forecasting methods, chosen automatically based on how much history is
available -- both return the same result shape so callers don't need to
know which one ran:

  - "linear"  Ordinary least-squares trend line. Always available, always
              explainable ("the metric has been rising by X/hour"), and its
              own residual spread gives an honest, built-in confidence
              interval. This is the floor -- it always runs, even with as
              few as MIN_POINTS windows of history.

  - "holt"    Holt's linear trend exponential smoothing (statsmodels), which
              weights recent windows more heavily and adapts faster to a
              changing rate of change. Used once enough history exists
              (HOLT_MIN_POINTS windows). If statsmodels isn't installed,
              this method is skipped and we fall back to "linear" -- the
              system still works, it just won't adapt to trend changes as
              quickly. requirements.txt pins statsmodels for the real
              deployment; the fallback exists mainly so tests and local dev
              don't hard-depend on it.

Everything here operates in "hours since the first data point" internally,
so slopes are always expressed as a rate per hour -- the unit a human
actually thinks in ("rising 4 connections/hour"), not per-window.
"""

from datetime import datetime, timedelta, timezone

import numpy as np

try:
    from statsmodels.tsa.holtwinters import Holt
    _HAS_STATSMODELS = True
except ImportError:
    _HAS_STATSMODELS = False

MIN_POINTS = 5          # fewer than this and we refuse to forecast at all
HOLT_MIN_POINTS = 20    # fewer than this and we use plain linear regression


def _parse_ts(ts):
    if isinstance(ts, datetime):
        return ts
    # Elasticsearch date fields round-trip as ISO 8601 strings.
    return datetime.fromisoformat(ts.replace("Z", "+00:00"))


def _hours_since_start(timestamps):
    parsed = [_parse_ts(t) for t in timestamps]
    start = parsed[0]
    return np.array([(t - start).total_seconds() / 3600.0 for t in parsed]), start


def _linear_fit(hours, values):
    """OLS trend line + a 90% prediction interval on the slope, via the
    standard error of the regression. No scipy.stats dependency: for the
    sample sizes here (5-500 points) the normal approximation (z=1.645 for
    90%) is close enough to the t-distribution to be honest about
    uncertainty without adding another hard dependency."""
    n = len(hours)
    slope, intercept = np.polyfit(hours, values, 1)
    predicted = slope * hours + intercept
    residuals = values - predicted
    dof = max(n - 2, 1)
    residual_std = float(np.sqrt(np.sum(residuals ** 2) / dof))
    mean_h = float(np.mean(hours))
    ss_h = float(np.sum((hours - mean_h) ** 2)) or 1e-9
    slope_se = residual_std / np.sqrt(ss_h)
    z90 = 1.645
    return {
        "method": "linear",
        "slope_per_hour": float(slope),
        "slope_se": float(slope_se),
        "slope_low": float(slope - z90 * slope_se),
        "slope_high": float(slope + z90 * slope_se),
        "intercept": float(intercept),
        "residual_std": residual_std,
        "last_hour": float(hours[-1]),
        "last_value": float(values[-1]),
    }


def _holt_fit(hours, values):
    """Holt's linear trend smoothing. Gives a same-shaped result as
    _linear_fit by fitting a line through the model's own one-step-ahead
    forecasts near the end of the series -- this keeps the downstream
    "time to threshold" math identical regardless of which method ran,
    while still letting Holt's exponential weighting pick up a recent
    acceleration/deceleration that a plain OLS line over the whole window
    would average away."""
    model = Holt(np.asarray(values, dtype=float), initialization_method="estimated").fit()
    step_hours = float(np.median(np.diff(hours))) if len(hours) > 1 else 1.0
    horizon = 10
    forecast = model.forecast(horizon)
    future_hours = hours[-1] + step_hours * np.arange(1, horizon + 1)
    slope, intercept = np.polyfit(future_hours, forecast, 1)
    fitted = model.fittedvalues
    residuals = np.asarray(values[-len(fitted):]) - fitted
    residual_std = float(np.sqrt(np.mean(residuals ** 2))) if len(residuals) else 0.0
    # Holt doesn't give us a closed-form slope standard error, so we widen
    # the linear method's slope uncertainty a bit as a conservative stand-in
    # -- explainable trade-off, not a hidden guess.
    linear = _linear_fit(hours, values)
    slope_se = max(linear["slope_se"], residual_std / max(step_hours, 1e-6) * 0.5)
    z90 = 1.645
    return {
        "method": "holt",
        "slope_per_hour": float(slope),
        "slope_se": float(slope_se),
        "slope_low": float(slope - z90 * slope_se),
        "slope_high": float(slope + z90 * slope_se),
        "intercept": float(intercept),
        "residual_std": residual_std,
        "last_hour": float(hours[-1]),
        "last_value": float(values[-1]),
    }


def fit_trend(timestamps, values):
    """Fits the best available trend model to (timestamps, values).
    Returns None if there isn't enough history to say anything honest."""
    if len(values) < MIN_POINTS:
        return None
    hours, start = _hours_since_start(timestamps)
    values = np.asarray(values, dtype=float)

    if _HAS_STATSMODELS and len(values) >= HOLT_MIN_POINTS:
        try:
            fit = _holt_fit(hours, values)
        except Exception:
            fit = _linear_fit(hours, values)  # never let a modeling edge case kill the pipeline
    else:
        fit = _linear_fit(hours, values)

    fit["start_time"] = start
    return fit


def _hours_to_cross(current_value, slope_per_hour, threshold, direction):
    if direction == "above":
        if slope_per_hour <= 0:
            return None  # flat or falling -- never crosses going up
        if current_value >= threshold:
            return 0.0
        return (threshold - current_value) / slope_per_hour
    else:  # "below"
        if slope_per_hour >= 0:
            return None
        if current_value <= threshold:
            return 0.0
        return (threshold - current_value) / slope_per_hour


def time_to_threshold(timestamps, values, threshold, direction="above", max_horizon_hours=72):
    """
    The core "predict the failure" call. Given a metric's history and a
    threshold, returns None if no forecast can be made (not enough data, or
    the trend isn't headed toward the threshold within max_horizon_hours),
    otherwise a dict describing when it's expected to cross, with a
    confidence interval derived from the slope's own uncertainty:

      best-case slope  -> latest crossing time  (slower trend)
      point estimate    -> predicted crossing time
      worst-case slope  -> earliest crossing time (faster trend)
    """
    fit = fit_trend(timestamps, values)
    if fit is None:
        return None

    hours_point = _hours_to_cross(fit["last_value"], fit["slope_per_hour"], threshold, direction)
    if hours_point is None or hours_point > max_horizon_hours:
        return None

    # For the interval, the "fast" edge of the slope range gives the
    # earliest plausible crossing, and the "slow" edge gives the latest.
    if direction == "above":
        fast_slope, slow_slope = fit["slope_high"], fit["slope_low"]
    else:
        fast_slope, slow_slope = fit["slope_low"], fit["slope_high"]

    hours_earliest = _hours_to_cross(fit["last_value"], fast_slope, threshold, direction)
    hours_latest = _hours_to_cross(fit["last_value"], slow_slope, threshold, direction)

    now = fit["start_time"] + timedelta(hours=fit["last_hour"])
    predicted_at = now + timedelta(hours=hours_point)
    predicted_at_earliest = now + timedelta(hours=hours_earliest) if hours_earliest is not None else predicted_at
    predicted_at_latest = (now + timedelta(hours=hours_latest)) if hours_latest is not None else None

    # Confidence: how tight is the interval relative to the point estimate?
    # A prediction landing "in 6 hours, could be 5-7" is high confidence;
    # "in 6 hours, could be 2-40" is not, even though the point estimate is
    # identical -- the label should reflect that, not just the raw slope.
    if predicted_at_latest is not None and hours_point > 0:
        spread_ratio = (hours_latest - hours_earliest) / hours_point
    else:
        spread_ratio = 0.0
    if spread_ratio < 0.5:
        confidence_score, confidence = 0.85, "high"
    elif spread_ratio < 1.5:
        confidence_score, confidence = 0.6, "medium"
    else:
        confidence_score, confidence = 0.35, "low"

    return {
        "method": fit["method"],
        "current_value": fit["last_value"],
        "trend_per_hour": fit["slope_per_hour"],
        "hours_until_threshold": hours_point,
        "predicted_at": predicted_at,
        "predicted_at_earliest": predicted_at_earliest,
        "predicted_at_latest": predicted_at_latest,
        "confidence": confidence,
        "confidence_score": confidence_score,
    }
