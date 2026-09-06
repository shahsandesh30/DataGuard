"""Trailing baselines for Layer 2 event features."""

from __future__ import annotations

from datetime import date, timedelta

import numpy as np
import pandas as pd

from pipelines.config import LAYER2_BASELINE_DAYS


def _parse_day(date_local: str) -> date:
    return date.fromisoformat(str(date_local))


def trailing_daily_means(
    conformed: pd.DataFrame,
    location_id: int,
    parameter: str,
    before_day: str,
) -> pd.Series:
    """Daily means for trailing window strictly before ``before_day``."""
    cutoff = _parse_day(before_day)
    window_start = cutoff - timedelta(days=LAYER2_BASELINE_DAYS)
    subset = conformed[
        (conformed["location_id"] == location_id)
        & (conformed["parameter"] == parameter)
        & (conformed["date_local"] >= window_start.isoformat())
        & (conformed["date_local"] < before_day)
    ]
    if subset.empty:
        return pd.Series(dtype=float)
    return subset.groupby("date_local")["value"].mean()


def trailing_stats(
    conformed: pd.DataFrame,
    location_id: int,
    parameter: str,
    before_day: str,
) -> dict[str, float]:
    """Median, std, p90, Q3, IQR from trailing daily means."""
    daily = trailing_daily_means(conformed, location_id, parameter, before_day)
    if daily.empty:
        all_vals = conformed[
            (conformed["location_id"] == location_id) & (conformed["parameter"] == parameter)
        ]["value"]
        if all_vals.empty:
            return {
                "median": 0.0,
                "std": 1.0,
                "p90": 0.0,
                "q3": 0.0,
                "iqr": 1.0,
            }
        median = float(all_vals.median())
        std = float(all_vals.std()) if len(all_vals) > 1 else 1.0
        p90 = float(np.percentile(all_vals, 90))
        q3 = float(np.percentile(all_vals, 75))
        q1 = float(np.percentile(all_vals, 25))
        iqr = max(q3 - q1, 1e-6)
        return {"median": median, "std": max(std, 1e-6), "p90": p90, "q3": q3, "iqr": iqr}

    median = float(daily.median())
    std = float(daily.std()) if len(daily) > 1 else 1.0
    p90 = float(np.percentile(daily, 90))
    q3 = float(np.percentile(daily, 75))
    q1 = float(np.percentile(daily, 25))
    iqr = max(q3 - q1, 1e-6)
    return {"median": median, "std": max(std, 1e-6), "p90": p90, "q3": q3, "iqr": iqr}


def hourly_roc_max(values: pd.Series) -> float:
    if len(values) < 2:
        return 0.0
    diffs = values.diff().abs().dropna()
    return float(diffs.max()) if not diffs.empty else 0.0


def spike_count(values: pd.Series, threshold: float) -> int:
    if len(values) < 2 or threshold <= 0:
        return 0
    diffs = values.diff().abs().dropna()
    return int((diffs > threshold).sum())


def sustained_elevation_hours(values: pd.Series, p90: float) -> int:
    if values.empty or p90 <= 0:
        return 0
    above = (values > p90).astype(int)
    longest = current = 0
    for flag in above:
        if flag:
            current += 1
            longest = max(longest, current)
        else:
            current = 0
    return longest
