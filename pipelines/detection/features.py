"""Layer 2 feature engineering over silver measurements.

Genuine pollution events (bushfire smoke, dust storms, industrial incidents)
appear as large, spatially coherent changes: multiple nearby stations moving
together distinguishes a real event from one broken sensor.
"""

import pandas as pd
import numpy as np

BASELINE_MIN_SAMPLES = 8
MIN_ROWS_PER_STATION_PARAMETER = 100 
NEIGHBOR_K = 3               
ROLLING_WINDOWS_HOURS = [3, 6, 24]
HIGH_HUMIDITY_PCT = 75       

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


# Baseline deviation features — robust z-score relative to diurnal baseline

MAD_TO_STD = 1.4826  # scales MAD to be std-equivalent for a normal distribution
 
 
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
    baseline.loc[
        insufficient,
        ["baseline_median", "baseline_mad"]
    ] = np.nan
    df = df.merge(baseline, on=group_keys, how="left")
    df["baseline_mad_scaled"] = (
        df["baseline_mad"] * MAD_TO_STD
    ).replace(0, np.nan)

    df["deviation"] = df["value"] - df["baseline_median"]

    df["deviation_zscore"] = (
        df["deviation"] / df["baseline_mad_scaled"]
    )
    return df

# Rolling / temporal-shape features
# ----------------------------------------------------------------------
 
def _run_length_above(s: pd.Series, thresh: float = 2.0) -> pd.Series:
    above = (s > thresh).astype(int)
    return above.groupby((above == 0).cumsum()).cumsum()
 
 
def _add_rolling_features(df: pd.DataFrame) -> pd.DataFrame:
    df = df.sort_values(["locationid", "parameter", "datetime"])
    grp = df.groupby(["locationid", "parameter"], group_keys=False)
 
    df["rate_of_change_1h"] = grp["value"].diff()
 
    for w in ROLLING_WINDOWS_HOURS:
        df[f"roll_mean_{w}h"] = grp["value"].transform(
            lambda s: s.rolling(w, min_periods=max(2, w // 2)).mean()
        )
        df[f"roll_std_{w}h"] = grp["value"].transform(
            lambda s: s.rolling(w, min_periods=max(2, w // 2)).std()
        )
 
    df["sustained_elevated_hours"] = grp["deviation_zscore"].transform(_run_length_above)
    return df


# ----------------------------------------------------------------------
# Spatial features — k nearest neighbors (not fixed radius)
#
# EDA showed a tight cluster of ~9 stations within ~7km of each other, and
# 6 isolated stations with zero neighbors within 15km (3 of those still
# isolated at 25km). A fixed radius leaves those stations with no spatial
# feature at all, so we take the K nearest stations regardless of distance
# and carry distance as context rather than a hard cutoff.
# ----------------------------------------------------------------------
 
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


# ----------------------------------------------------------------------
# Cross-parameter features — for the ACTUAL parameter set:
# pm1, pm25, um003 (particle count >0.3um), temperature, relativehumidity
#
# Two things this buys us that a single-parameter view can't:
#   1. PM/particle-count co-movement: a real particulate event (smoke, dust)
#      should raise both PM mass (pm1/pm25) AND particle count (um003)
#      together. If only the PM channels spike without particle count
#      corroborating it, that's a weaker signal of a genuine event.
#   2. Humidity artifact flag: low-cost optical PM sensors are well known to
#      over-read PM in high humidity/fog because water-swollen particles
#      scatter more light. A PM spike during high RH, NOT corroborated by
#      particle count, is a strong candidate for a humidity artifact rather
#      than a real pollution event — useful context for the model and for
#      whoever reviews flagged events later.
# ----------------------------------------------------------------------
 
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
                & (wide_z["um003"] <= 1)  # particle count NOT corroborating the PM spike
            ).astype(int)
 
    if "temperature" in wide_val.columns:
        cross["temperature_c"] = wide_val["temperature"]
 
    if cross.empty:
        return df
 
    cross = cross.reset_index()
    df = df.merge(cross, on=["locationid", "datetime"], how="left")
    return df

# Main feature engineering function for event detection (Layer 2)


def build_event_features(silver: pd.DataFrame) -> pd.DataFrame:
    """Return station-window and region-window features for event detection.

    TODO(ML engineer): rolling deviation from each station's own baseline,
    rate of change, cross-station agreement within a region, multi-parameter
    co-movement (e.g. PM2.5 and PM10 rising together suggests smoke/dust).
    """

    df = silver.copy()

    # Sort the observations by datetime
    df = df.sort_values(
        ["locationid", "parameter", "datetime"]
    )

    df["parameter"] = df["parameter"].str.lower()

    print("Building event features...")
    print(f"\nInitial silver data shape: {df.shape[0]} rows, {df.shape[1]} columns")

    print (f"\nDropping stations with < {MIN_ROWS_PER_STATION_PARAMETER} rows per parameter...")
    df = _drop_insufficient_stations(df, MIN_ROWS_PER_STATION_PARAMETER)

    print(f"\nAdding time parts...")
    df = _add_time_parts(df)

    print(f"\nAdding baseline deviation features...")
    df = _add_baseline_deviation(df)

    print(f"\nAdding rolling features...")
    df = _add_rolling_features(df)

    print(f"\nAdding spatial features...")
    knn = _build_knn_table(df, NEIGHBOR_K)
    df = _add_spatial_features(df, knn)

    print(f"\nAdding cross-parameter features...")
    df = _add_cross_parameter_features(df)
 
    return df


def weak_labels(features: pd.DataFrame) -> pd.Series:
    """Deterministic rule-based weak labels for evaluation (no ground truth
    exists — see risk R4).

    TODO(ML engineer): e.g. sustained multi-station PM2.5 elevation above the
    station's seasonal norm. Used for Precision@K, never for training.
    """
    raise NotImplementedError
