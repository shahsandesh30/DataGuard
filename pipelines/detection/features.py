"""Layer 2 feature engineering over silver measurements.

Genuine pollution events (bushfire smoke, dust storms, industrial incidents)
appear as large, spatially coherent changes: multiple nearby stations moving
together distinguishes a real event from one broken sensor.
"""

import pandas as pd
import numpy as np

BASELINE_MIN_SAMPLES = 8

def _add_time_parts(df: pd.DataFrame) -> pd.DataFrame:
    dt = df["datetime_utc"]
    df["hour"] = dt.dt.hour
    df["dow"] = dt.dt.dayofweek
    df["is_weekend"] = df["dow"].isin([5, 6]).astype(int)
    df["month"] = dt.dt.month
    # season keyed to Sydney (Southern Hemisphere)
    df["season"] = df["month"] % 12 // 3 + 1  # 1=summer(DJF)...4=spring(SON) approx
    df["hour_sin"] = np.sin(2 * np.pi * df["hour"] / 24)
    df["hour_cos"] = np.cos(2 * np.pi * df["hour"] / 24)
    return df

def _add_baseline_deviation(df: pd.DataFrame) -> pd.DataFrame:
    """Diurnal baseline per (location_id, parameter, hour, is_weekend, season)
    plus deviation of the observed value from that baseline.
    """
    group_keys = ["location_id", "parameter", "hour", "is_weekend", "season"]
    baseline = (
        df.groupby(group_keys)["value"]
        .agg(baseline_mean="mean", baseline_std="std", baseline_n="count")
        .reset_index()
    )
    # don't trust baselines built from too few historical points
    baseline.loc[baseline["baseline_n"] < BASELINE_MIN_SAMPLES, ["baseline_mean", "baseline_std"]] = np.nan
 
    df = df.merge(baseline, on=group_keys, how="left")
    df["baseline_std"] = df["baseline_std"].replace(0, np.nan)  # avoid div/0
    df["deviation"] = df["value"] - df["baseline_mean"]
    df["deviation_zscore"] = df["deviation"] / df["baseline_std"]
    return df


def build_event_features(silver: pd.DataFrame) -> pd.DataFrame:
    """Return station-window and region-window features for event detection.

    TODO(ML engineer): rolling deviation from each station's own baseline,
    rate of change, cross-station agreement within a region, multi-parameter
    co-movement (e.g. PM2.5 and PM10 rising together suggests smoke/dust).
    """

    df = silver.copy()

    # Sort the observations by datetime
    df = df.sort_values(
        ["sensor_id", "parameter", "datetime_utc"]
    )

    df["datetime_utc"] = pd.to_datetime(df["datetime_utc"], utc=True)
    df["parameter"] = df["parameter"].str.lower()
 
    df = _add_time_parts(df)
    df = _add_baseline_deviation(df)

    return df
    # raise NotImplementedError


def weak_labels(features: pd.DataFrame) -> pd.Series:
    """Deterministic rule-based weak labels for evaluation (no ground truth
    exists — see risk R4).

    TODO(ML engineer): e.g. sustained multi-station PM2.5 elevation above the
    station's seasonal norm. Used for Precision@K, never for training.
    """
    raise NotImplementedError
