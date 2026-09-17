from __future__ import annotations
 
import numpy as np
import pandas as pd
from sklearn.ensemble import IsolationForest
 
# Only score actual pollutants — temperature/relativehumidity are covariates,
# not things we want flagged as "anomalous air quality" themselves.
SCORABLE_PARAMETERS = ["pm1", "pm25", "um003"]
 
MODEL_FEATURES = [
    "deviation_zscore",
    "rate_of_change_1h",
    "roll_std_3h",
    "roll_std_6h",
    "roll_std_24h",
    "sustained_elevated_hours",
    "spatial_deviation",
    "neighbor_mean_zscore",
    "neighbor_frac_elevated",
    "nearest_neighbor_km",
    "pm1_pm25_comovement",
    "particle_pm25_comovement",
    "humidity_pct",
    "high_humidity_flag",
    "possible_humidity_artifact",
    "hour_sin",
    "hour_cos",
    "is_weekend",
]
 
IF_PARAMS = dict(
    n_estimators=200,
    contamination="auto",
    random_state=42,
    n_jobs=-1,
)
 
 
def _select_model_frame(df: pd.DataFrame, parameter: str) -> pd.DataFrame:
    """Filter to one parameter's rows and the model feature columns,
    dropping rows with missing values in any required feature.
    """
    subset = df.loc[df["parameter"] == parameter].copy()
 
    available_features = [c for c in MODEL_FEATURES if c in subset.columns]
    missing_features = set(MODEL_FEATURES) - set(available_features)
    if missing_features:
        print(f"[{parameter}] skipping features not present in this table: {missing_features}")
 
    before = len(subset)
    subset = subset.dropna(subset=available_features)
    dropped = before - len(subset)
    if dropped:
        print(f"[{parameter}] dropped {dropped}/{before} rows with missing feature values")
 
    return subset, available_features
 
 
def fit_score_isolation_forest(
    df: pd.DataFrame,
    parameter: str,
) -> pd.DataFrame:
    """Fit an Isolation Forest for one parameter and return the input rows
    with two new columns appended: `anomaly_score` (higher = more anomalous,
    flipped from sklearn's raw convention for intuitive reading) and
    `is_anomaly` (bool, from IF's own contamination-based threshold).
    """
    subset, feature_cols = _select_model_frame(df, parameter)
    if subset.empty:
        print(f"[{parameter}] no rows left after dropping missing values — skipping")
        return subset
 
    X = subset[feature_cols].to_numpy()
 
    model = IsolationForest(**IF_PARAMS)
    model.fit(X)
 
    # sklearn's decision_function: LOWER (more negative) = more anomalous.
    # Flip sign so higher anomaly_score = more anomalous, which is the more
    # intuitive convention for anyone consuming this downstream (fusion, review).
    subset["anomaly_score"] = -model.decision_function(X)
    subset["is_anomaly"] = model.predict(X) == -1  # sklearn: -1 = outlier, 1 = inlier
    subset["model_name"] = "isolation_forest_baseline"
 
    return subset
 
 
def run_baseline_for_all_parameters(features: pd.DataFrame) -> pd.DataFrame:
    """Fit + score Isolation Forest separately for each scorable parameter,
    return the concatenated scored results.
    """
    scored_parts = []
    for parameter in SCORABLE_PARAMETERS:
        if parameter not in features["parameter"].unique():
            print(f"[{parameter}] not present in feature table — skipping")
            continue
        scored = fit_score_isolation_forest(features, parameter)
        if not scored.empty:
            scored_parts.append(scored)
 
    if not scored_parts:
        raise ValueError("No parameters produced scored output — check feature table.")
 
    return pd.concat(scored_parts, ignore_index=True)
 
 
def inspect_top_anomalies(scored: pd.DataFrame, parameter: str, k: int = 20) -> pd.DataFrame:
    """Quick qualitative check (no weak labels yet): the top-K highest-scored
    rows for one parameter, with the interpretive context columns included
    so you can eyeball whether flagged rows look like plausible events,
    isolated glitches, or humidity artifacts.
    """
    cols = [
        "locationid", "datetime", "parameter", "value", "is_anomaly",
        "anomaly_score", "deviation_zscore", "sustained_elevated_hours",
        "regionally_coherent", "spatially_isolated", "possible_humidity_artifact",
    ]
    cols = [c for c in cols if c in scored.columns]
    subset = scored.loc[scored["parameter"] == parameter]
    return subset.sort_values("anomaly_score", ascending=False).head(k)[cols]