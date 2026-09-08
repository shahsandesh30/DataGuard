"""Layer 2 feature engineering over conformed measurements."""

from __future__ import annotations

import json

import numpy as np
import pandas as pd

from pipelines.config import (
    DEFAULT_locationidS,
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
    "locationid",
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

BASELINE_MIN_SAMPLES = 8
MIN_ROWS_PER_STATION_PARAMETER = 100
NEIGHBOR_K = 3
ROLLING_WINDOWS_HOURS = [3, 6, 24]
HIGH_HUMIDITY_PCT = 75
MAD_TO_STD = 1.4826  # scales MAD to be std-equivalent for a normal distribution


def _empty_features() -> pd.DataFrame:
    return pd.DataFrame(columns=EVENT_FEATURE_COLUMNS)


def _pm_co_movement(conformed: pd.DataFrame, locationid: int, date_local: str) -> float:
    pm25 = conformed[
        (conformed["locationid"] == locationid)
        & (conformed["date_local"] == date_local)
        & (conformed["parameter"] == "pm25")
    ].sort_values("datetime_utc")["value"]
    pm10 = conformed[
        (conformed["locationid"] == locationid)
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
    locationids: list[int],
) -> dict[int, float]:
    means: dict[int, float] = {}
    for loc in locationids:
        subset = conformed[
            (conformed["locationid"] == loc)
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
    locationids: list[int],
    trailing_regional_median: float,
) -> float:
    if trailing_regional_median <= 0:
        return 0.0
    elevated = 0
    total = 0
    for loc in locationids:
        subset = conformed[
            (conformed["locationid"] == loc)
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

    region_locations = [loc for loc in DEFAULT_locationidS if loc != 2178]

    pm = conformed[conformed["parameter"].isin(LAYER2_PM_PARAMETERS)].copy()
    if pm.empty:
        return _empty_features()

    rows: list[dict] = []
    keys = pm[["locationid", "date_local", "parameter"]].drop_duplicates()

    for _, key in keys.iterrows():
        locationid = int(key["locationid"])
        date_local = str(key["date_local"])
        parameter = str(key["parameter"])

        day = pm[
            (pm["locationid"] == locationid)
            & (pm["date_local"] == date_local)
            & (pm["parameter"] == parameter)
        ].sort_values("datetime_utc")
        if day.empty:
            continue

        stats = trailing_stats(conformed, locationid, parameter, date_local)
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

        co_move = _pm_co_movement(conformed, locationid, date_local) if parameter in ("pm25", "pm10") else 0.0

        rows.append(
            {
                "locationid": locationid,
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
    locationids: list[int],
    trailing_regional_median: float,
) -> int:
    elevated = 0
    for loc in locationids:
        subset = conformed[
            (conformed["locationid"] == loc)
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

    region_locations = [loc for loc in DEFAULT_locationidS if loc != 2178]
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
            trailing_medians[str(date_local)] = (
                float(np.median(trailing)) if trailing else regional_means[str(date_local)]
            )
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


# ---------------------------------------------------------------------------
# Hourly Athena silver features (Glue `locationid` / `datetime` schema).
# Used by `python -m pipelines.detection` to write silver/derived parquet.
# ---------------------------------------------------------------------------


def _add_time_parts(df: pd.DataFrame) -> pd.DataFrame:
    dt = df["datetime"]
    df["hour"] = dt.dt.hour
    df["dow"] = dt.dt.dayofweek
    df["is_weekend"] = df["dow"].isin([5, 6]).astype(int)
    df["month"] = dt.dt.month
    # season keyed to Sydney (Southern Hemisphere)
    df["season"] = df["month"] % 12 // 3 + 1  # 1=summer(DJF)...4=spring(SON) approx
    df["hour_sin"] = np.sin(2 * np.pi * df["hour"] / 24)
    df["hour_cos"] = np.cos(2 * np.pi * df["hour"] / 24)
    return df


def _drop_insufficient_stations(df: pd.DataFrame, min_rows: int) -> pd.DataFrame:
    counts = df.groupby(["locationid", "parameter"]).size()
    valid = counts[counts >= min_rows].index
    mask = df.set_index(["locationid", "parameter"]).index.isin(valid)
    dropped = df.loc[~mask, "locationid"].unique()
    if len(dropped):
        print(f"Dropping insufficient stations/parameters: {sorted(set(dropped))}")
    return df.loc[mask].reset_index(drop=True)


def _mad(series: pd.Series) -> float:
    med = series.median()
    return (series - med).abs().median()


def _add_baseline_deviation(df: pd.DataFrame) -> pd.DataFrame:
    """Diurnal baseline per (locationid, parameter, hour, is_weekend, season)
    plus robust-z deviation of the observed value from that baseline.
    """
    group_keys = ["locationid", "parameter", "hour", "is_weekend", "season"]

    baseline = (
        df.groupby(group_keys)["value"]
        .agg(
            baseline_median="median",
            baseline_n="count",
            baseline_mad=_mad,
        )
        .reset_index()
    )

    insufficient = baseline["baseline_n"] < BASELINE_MIN_SAMPLES
    baseline.loc[insufficient, ["baseline_median", "baseline_mad"]] = np.nan
    df = df.merge(baseline, on=group_keys, how="left")
    df["baseline_mad_scaled"] = (df["baseline_mad"] * MAD_TO_STD).replace(0, np.nan)
    df["deviation"] = df["value"] - df["baseline_median"]
    df["deviation_zscore"] = df["deviation"] / df["baseline_mad_scaled"]
    return df


def _run_length_above(s: pd.Series, thresh: float = 2.0) -> pd.Series:
    above = (s > thresh).astype(int)
    return above.groupby((above == 0).cumsum()).cumsum()


def _add_rolling_features(df: pd.DataFrame) -> pd.DataFrame:
    df = df.sort_values(["locationid", "parameter", "datetime"])
    grp = df.groupby(["locationid", "parameter"], group_keys=False)

    df["rate_of_change_1h"] = grp["value"].diff()

    for w in ROLLING_WINDOWS_HOURS:
        df[f"roll_mean_{w}h"] = grp["value"].transform(
            lambda s, window=w: s.rolling(window, min_periods=max(2, window // 2)).mean()
        )
        df[f"roll_std_{w}h"] = grp["value"].transform(
            lambda s, window=w: s.rolling(window, min_periods=max(2, window // 2)).std()
        )

    df["sustained_elevated_hours"] = grp["deviation_zscore"].transform(_run_length_above)
    return df


def _haversine_km(lat1, lon1, lat2, lon2) -> np.ndarray:
    r = 6371.0
    lat1, lon1, lat2, lon2 = map(np.radians, [lat1, lon1, lat2, lon2])
    dlat, dlon = lat2 - lat1, lon2 - lon1
    a = np.sin(dlat / 2) ** 2 + np.cos(lat1) * np.cos(lat2) * np.sin(dlon / 2) ** 2
    return 2 * r * np.arcsin(np.sqrt(a))


def _build_knn_table(df: pd.DataFrame, k: int) -> pd.DataFrame:
    stations = df[["locationid", "latitude", "longitude"]].drop_duplicates("locationid")
    pairs = stations.merge(stations, how="cross", suffixes=("", "_nbr"))
    pairs = pairs[pairs["locationid"] != pairs["locationid_nbr"]]
    pairs["distance_km"] = _haversine_km(
        pairs["latitude"], pairs["longitude"], pairs["latitude_nbr"], pairs["longitude_nbr"]
    )
    pairs = pairs.sort_values(["locationid", "distance_km"])
    knn = pairs.groupby("locationid").head(k)
    return knn[["locationid", "locationid_nbr", "distance_km"]].reset_index(drop=True)


def _add_spatial_features(df: pd.DataFrame, knn: pd.DataFrame) -> pd.DataFrame:
    small = df[["locationid", "parameter", "datetime", "value", "deviation_zscore"]]

    pairs = knn.merge(small, on="locationid", how="inner")
    pairs = pairs.rename(columns={"value": "value_self", "deviation_zscore": "zscore_self"})
    nbr_vals = small.rename(
        columns={"locationid": "locationid_nbr", "value": "value_nbr", "deviation_zscore": "zscore_nbr"}
    )
    pairs = pairs.merge(nbr_vals, on=["locationid_nbr", "parameter", "datetime"], how="inner")

    agg = (
        pairs.groupby(["locationid", "parameter", "datetime"])
        .agg(
            n_neighbors=("locationid_nbr", "nunique"),
            nearest_neighbor_km=("distance_km", "min"),
            neighbor_mean_value=("value_nbr", "mean"),
            neighbor_mean_zscore=("zscore_nbr", "mean"),
            neighbor_frac_elevated=("zscore_nbr", lambda s: (s > 2).mean()),
        )
        .reset_index()
    )

    df = df.merge(agg, on=["locationid", "parameter", "datetime"], how="left")
    df["spatial_deviation"] = df["value"] - df["neighbor_mean_value"]
    df["spatially_isolated"] = (
        (df["deviation_zscore"] > 2) & (df["neighbor_frac_elevated"].fillna(0) < 0.3)
    ).astype(int)
    df["regionally_coherent"] = (
        (df["deviation_zscore"] > 2) & (df["neighbor_frac_elevated"].fillna(0) >= 0.5)
    ).astype(int)
    return df


def _add_cross_parameter_features(df: pd.DataFrame) -> pd.DataFrame:
    wide_z = df.pivot_table(
        index=["locationid", "datetime"], columns="parameter", values="deviation_zscore"
    )
    wide_val = df.pivot_table(
        index=["locationid", "datetime"], columns="parameter", values="value"
    )

    cross = pd.DataFrame(index=wide_z.index)

    if {"pm1", "pm25"}.issubset(wide_z.columns):
        cross["pm1_pm25_comovement"] = wide_z[["pm1", "pm25"]].min(axis=1)

    if {"um003", "pm25"}.issubset(wide_z.columns):
        cross["particle_pm25_comovement"] = wide_z[["um003", "pm25"]].min(axis=1)

    if "relativehumidity" in wide_val.columns:
        cross["humidity_pct"] = wide_val["relativehumidity"]
        cross["high_humidity_flag"] = (wide_val["relativehumidity"] > HIGH_HUMIDITY_PCT).astype(int)

        if {"pm25", "um003"}.issubset(wide_z.columns):
            cross["possible_humidity_artifact"] = (
                (wide_z["pm25"] > 2)
                & (wide_val["relativehumidity"] > HIGH_HUMIDITY_PCT)
                & (wide_z["um003"] <= 1)
            ).astype(int)

    if "temperature" in wide_val.columns:
        cross["temperature_c"] = wide_val["temperature"]

    if cross.empty:
        return df

    cross = cross.reset_index()
    df = df.merge(cross, on=["locationid", "datetime"], how="left")
    return df


def build_hourly_event_features(silver: pd.DataFrame) -> pd.DataFrame:
    """Hourly features from Athena silver (`locationid`, `datetime`, lat/lon)."""
    df = silver.copy()
    df = df.sort_values(["locationid", "parameter", "datetime"])
    df["parameter"] = df["parameter"].str.lower()

    print("Building event features...")
    print(f"\nInitial silver data shape: {df.shape[0]} rows, {df.shape[1]} columns")

    print(f"\nDropping stations with < {MIN_ROWS_PER_STATION_PARAMETER} rows per parameter...")
    df = _drop_insufficient_stations(df, MIN_ROWS_PER_STATION_PARAMETER)

    print("\nAdding time parts...")
    df = _add_time_parts(df)

    print("\nAdding baseline deviation features...")
    df = _add_baseline_deviation(df)

    print("\nAdding rolling features...")
    df = _add_rolling_features(df)

    print("\nAdding spatial features...")
    knn = _build_knn_table(df, NEIGHBOR_K)
    df = _add_spatial_features(df, knn)

    print("\nAdding cross-parameter features...")
    df = _add_cross_parameter_features(df)

    return df
