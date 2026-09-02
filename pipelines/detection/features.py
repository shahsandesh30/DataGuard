"""Layer 2 feature engineering over conformed measurements."""

from __future__ import annotations

import json

import numpy as np
import pandas as pd

from pipelines.config import (
    DEFAULT_LOCATION_IDS,
    LAYER2_PM_PARAMETERS,
    LAYER2_REGION_ID,
    WEAK_LABEL_MIN_LOCATIONS,
    WEAK_LABEL_PM25_RATIO,
)
from pipelines.detection.baseline import (
    hourly_roc_max,
    spike_count,
    sustained_elevation_hours,
    trailing_daily_means,
    trailing_stats,
)

EVENT_FEATURE_COLUMNS = [
    "location_id",
    "date_local",
    "parameter",
    "region_id",
    "daily_mean",
    "daily_max",
    "z_score",
    "iqr_exceedance",
    "roc_max",
    "spike_count",
    "sustained_elevation_hours",
    "mean_shift_ratio",
    "peer_z_score",
    "spatial_isolation",
    "regional_agreement",
    "pm_co_movement",
]

FEATURE_MODEL_COLUMNS = [
    "daily_mean",
    "daily_max",
    "z_score",
    "iqr_exceedance",
    "roc_max",
    "spike_count",
    "sustained_elevation_hours",
    "mean_shift_ratio",
    "peer_z_score",
    "spatial_isolation",
    "regional_agreement",
    "pm_co_movement",
]


def _empty_features() -> pd.DataFrame:
    return pd.DataFrame(columns=EVENT_FEATURE_COLUMNS)


def _pm_co_movement(conformed: pd.DataFrame, location_id: int, date_local: str) -> float:
    pm25 = conformed[
        (conformed["location_id"] == location_id)
        & (conformed["date_local"] == date_local)
        & (conformed["parameter"] == "pm25")
    ].sort_values("datetime_utc")["value"]
    pm10 = conformed[
        (conformed["location_id"] == location_id)
        & (conformed["date_local"] == date_local)
        & (conformed["parameter"] == "pm10")
    ].sort_values("datetime_utc")["value"]
    if len(pm25) < 3 or len(pm10) < 3:
        return 0.0
    length = min(len(pm25), len(pm10))
    a = pm25.iloc[:length].to_numpy()
    b = pm10.iloc[:length].to_numpy()
    if np.std(a) == 0 or np.std(b) == 0:
        return 0.0
    return float(np.corrcoef(a, b)[0, 1])


def _regional_daily_means(
    conformed: pd.DataFrame,
    date_local: str,
    parameter: str,
    location_ids: list[int],
) -> dict[int, float]:
    means: dict[int, float] = {}
    for loc in location_ids:
        subset = conformed[
            (conformed["location_id"] == loc)
            & (conformed["date_local"] == date_local)
            & (conformed["parameter"] == parameter)
        ]
        if not subset.empty:
            means[loc] = float(subset["value"].mean())
    return means


def _regional_agreement(
    conformed: pd.DataFrame,
    date_local: str,
    parameter: str,
    location_ids: list[int],
    trailing_regional_median: float,
) -> float:
    if trailing_regional_median <= 0:
        return 0.0
    elevated = 0
    total = 0
    for loc in location_ids:
        subset = conformed[
            (conformed["location_id"] == loc)
            & (conformed["date_local"] == date_local)
            & (conformed["parameter"] == parameter)
        ]
        if subset.empty:
            continue
        total += 1
        if float(subset["value"].mean()) > trailing_regional_median:
            elevated += 1
    return elevated / total if total else 0.0


def build_event_features(conformed: pd.DataFrame) -> pd.DataFrame:
    """Return station-day-parameter features for event detection."""
    if conformed is None or conformed.empty:
        return _empty_features()

    region_locations = [loc for loc in DEFAULT_LOCATION_IDS if loc != 2178]

    pm = conformed[conformed["parameter"].isin(LAYER2_PM_PARAMETERS)].copy()
    if pm.empty:
        return _empty_features()

    rows: list[dict] = []
    keys = pm[["location_id", "date_local", "parameter"]].drop_duplicates()

    for _, key in keys.iterrows():
        location_id = int(key["location_id"])
        date_local = str(key["date_local"])
        parameter = str(key["parameter"])

        day = pm[
            (pm["location_id"] == location_id)
            & (pm["date_local"] == date_local)
            & (pm["parameter"] == parameter)
        ].sort_values("datetime_utc")
        if day.empty:
            continue

        stats = trailing_stats(conformed, location_id, parameter, date_local)
        values = day["value"]
        daily_mean = float(values.mean())
        daily_max = float(values.max())
        z_score = (daily_mean - stats["median"]) / stats["std"]
        iqr_exceedance = (daily_max - stats["q3"]) / stats["iqr"]
        roc = hourly_roc_max(values)
        spikes = spike_count(values, 2.0 * stats["std"])
        sustained = sustained_elevation_hours(values, stats["p90"])
        mean_shift = daily_mean / stats["median"] if stats["median"] > 0 else 0.0

        regional_means = _regional_daily_means(conformed, date_local, parameter, region_locations)
        if regional_means:
            regional_mean = float(np.mean(list(regional_means.values())))
            regional_std = float(np.std(list(regional_means.values()))) if len(regional_means) > 1 else 1.0
            peer_z = (daily_mean - regional_mean) / max(regional_std, 1e-6)
        else:
            regional_mean = daily_mean
            peer_z = 0.0

        trailing_regional = []
        for loc in region_locations:
            trail = trailing_daily_means(conformed, loc, parameter, date_local)
            if not trail.empty:
                trailing_regional.append(float(trail.median()))
        trailing_regional_median = float(np.median(trailing_regional)) if trailing_regional else regional_mean
        reg_agreement = _regional_agreement(
            conformed, date_local, parameter, region_locations, trailing_regional_median
        )
        spatial_isolation = max(0.0, abs(peer_z)) * (1.0 - reg_agreement)

        co_move = _pm_co_movement(conformed, location_id, date_local) if parameter in ("pm25", "pm10") else 0.0

        rows.append(
            {
                "location_id": location_id,
                "date_local": date_local,
                "parameter": parameter,
                "region_id": LAYER2_REGION_ID,
                "daily_mean": daily_mean,
                "daily_max": daily_max,
                "z_score": z_score,
                "iqr_exceedance": iqr_exceedance,
                "roc_max": roc,
                "spike_count": spikes,
                "sustained_elevation_hours": sustained,
                "mean_shift_ratio": mean_shift,
                "peer_z_score": peer_z,
                "spatial_isolation": spatial_isolation,
                "regional_agreement": reg_agreement,
                "pm_co_movement": co_move,
            }
        )

    return pd.DataFrame(rows, columns=EVENT_FEATURE_COLUMNS)


def _elevated_location_count(
    conformed: pd.DataFrame,
    date_local: str,
    parameter: str,
    location_ids: list[int],
    trailing_regional_median: float,
) -> int:
    elevated = 0
    for loc in location_ids:
        subset = conformed[
            (conformed["location_id"] == loc)
            & (conformed["date_local"] == date_local)
            & (conformed["parameter"] == parameter)
        ]
        if subset.empty:
            continue
        if float(subset["value"].mean()) > trailing_regional_median:
            elevated += 1
    return elevated


def weak_labels(features: pd.DataFrame, conformed: pd.DataFrame | None = None) -> pd.Series:
    """Deterministic weak labels for evaluation only (risk R4)."""
    if features is None or features.empty:
        return pd.Series(dtype=bool)

    region_locations = [loc for loc in DEFAULT_LOCATION_IDS if loc != 2178]
    pm25_daily = features[features["parameter"] == "pm25"].copy()
    regional_means: dict[str, float] = {}
    trailing_medians: dict[str, float] = {}
    elevated_counts: dict[str, int] = {}

    if conformed is not None and not conformed.empty:
        for date_local in pm25_daily["date_local"].unique():
            means = _regional_daily_means(conformed, str(date_local), "pm25", region_locations)
            regional_means[str(date_local)] = float(np.mean(list(means.values()))) if means else 0.0
            trailing = []
            for loc in region_locations:
                trail = trailing_daily_means(conformed, loc, "pm25", str(date_local))
                if not trail.empty:
                    trailing.append(float(trail.median()))
            trailing_medians[str(date_local)] = float(np.median(trailing)) if trailing else regional_means[str(date_local)]
            elevated_counts[str(date_local)] = _elevated_location_count(
                conformed,
                str(date_local),
                "pm25",
                region_locations,
                trailing_medians[str(date_local)],
            )

    labels = []
    for _, row in features.iterrows():
        date_local = str(row["date_local"])
        if row["parameter"] != "pm25" or date_local not in regional_means:
            labels.append(False)
            continue
        flagged = (
            regional_means[date_local] > WEAK_LABEL_PM25_RATIO * trailing_medians[date_local]
            and elevated_counts.get(date_local, 0) >= WEAK_LABEL_MIN_LOCATIONS
        )
        labels.append(bool(flagged))
    return pd.Series(labels, index=features.index, dtype=bool)


def feature_snapshot(row: pd.Series) -> str:
    payload = {col: row[col] for col in EVENT_FEATURE_COLUMNS if col in row.index}
    return json.dumps(payload, default=str)
