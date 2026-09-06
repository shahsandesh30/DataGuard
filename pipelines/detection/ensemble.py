"""Layer 2 detector ensemble: Isolation Forest + LOF + DBSCAN."""

from __future__ import annotations

import numpy as np
import pandas as pd
from sklearn.cluster import DBSCAN
from sklearn.ensemble import IsolationForest
from sklearn.neighbors import LocalOutlierFactor
from sklearn.preprocessing import StandardScaler

from pipelines.detection.features import FEATURE_MODEL_COLUMNS

EVENT_ALERT_COLUMNS = [
    "location_id",
    "date_local",
    "parameter",
    "region_id",
    "alert_score",
    "rank",
    "if_flag",
    "lof_flag",
    "dbscan_flag",
    "agreement_count",
    "weak_label",
    "feature_snapshot",
]

TOP_K_ALERTS_PER_DAY = 10


def _feature_matrix(features: pd.DataFrame) -> pd.DataFrame:
    frame = features.copy()
    for col in FEATURE_MODEL_COLUMNS:
        if col not in frame.columns:
            frame[col] = 0.0
    return frame[FEATURE_MODEL_COLUMNS].fillna(0.0).replace([np.inf, -np.inf], 0.0)


def _normalize_scores(raw: np.ndarray) -> np.ndarray:
    """Map raw scores to [0, 1]; higher means more anomalous."""
    if len(raw) == 0:
        return raw
    inverted = -raw
    lo, hi = inverted.min(), inverted.max()
    if hi - lo < 1e-9:
        return np.zeros_like(inverted)
    return (inverted - lo) / (hi - lo)


def fit_ensemble(
    features: pd.DataFrame,
    *,
    contamination: float = 0.1,
    random_state: int = 42,
) -> dict | None:
    """Fit IF, LOF, and DBSCAN on scaled feature rows."""
    if features is None or features.empty:
        return None

    matrix = _feature_matrix(features)
    scaler = StandardScaler()
    scaled = scaler.fit_transform(matrix)

    iso = IsolationForest(
        contamination=contamination,
        random_state=random_state,
        n_estimators=100,
    )
    iso.fit(scaled)

    lof = LocalOutlierFactor(
        n_neighbors=min(5, len(scaled) - 1),
        contamination=contamination,
        novelty=False,
    )
    lof_labels = lof.fit_predict(scaled)

    dbscan = DBSCAN(eps=1.5, min_samples=max(2, len(scaled) // 10))
    dbscan_labels = dbscan.fit_predict(scaled)

    return {
        "scaler": scaler,
        "isolation_forest": iso,
        "lof_labels": lof_labels,
        "lof_scores": lof.negative_outlier_factor_,
        "dbscan_labels": dbscan_labels,
        "feature_columns": FEATURE_MODEL_COLUMNS,
    }


def score_events(
    models: dict | None,
    features: pd.DataFrame,
    *,
    weak_label: pd.Series | None = None,
    top_k: int = TOP_K_ALERTS_PER_DAY,
) -> pd.DataFrame:
    """Return ranked alert rows with per-detector flags."""
    from pipelines.detection.features import feature_snapshot

    if features is None or features.empty:
        return pd.DataFrame(columns=EVENT_ALERT_COLUMNS)

    if models is None:
        return pd.DataFrame(columns=EVENT_ALERT_COLUMNS)

    matrix = _feature_matrix(features)
    scaled = models["scaler"].transform(matrix)

    iso = models["isolation_forest"]
    if_preds = iso.predict(scaled)
    if_scores = _normalize_scores(iso.score_samples(scaled))

    lof_flags = models["lof_labels"] == -1
    lof_scores = _normalize_scores(models["lof_scores"])

    dbscan_flags = models["dbscan_labels"] == -1
    dbscan_scores = dbscan_flags.astype(float)

    agreement = if_preds.astype(int) + lof_flags.astype(int) + dbscan_flags.astype(int)
    combined = agreement / 3.0 + (if_scores + lof_scores + dbscan_scores) / 3.0

    scored = features.copy()
    scored["if_flag"] = if_preds == -1
    scored["lof_flag"] = lof_flags
    scored["dbscan_flag"] = dbscan_flags
    scored["agreement_count"] = agreement
    scored["alert_score"] = combined

    if weak_label is None:
        weak_label = pd.Series(False, index=features.index)
    scored["weak_label"] = weak_label.reindex(features.index, fill_value=False).values
    scored["feature_snapshot"] = scored.apply(feature_snapshot, axis=1)

    rows: list[pd.DataFrame] = []
    for (_region, date_local), group in scored.groupby(["region_id", "date_local"], sort=False):
        top = group.nlargest(min(top_k, len(group)), "alert_score")
        top = top.copy()
        top["rank"] = range(1, len(top) + 1)
        rows.append(top)

    if not rows:
        return pd.DataFrame(columns=EVENT_ALERT_COLUMNS)

    alerts = pd.concat(rows, ignore_index=True)
    return alerts[EVENT_ALERT_COLUMNS]
