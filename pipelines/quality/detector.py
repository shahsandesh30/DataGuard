"""Layer 1 anomaly model over the quality metric time-series.

Isolation Forest over station-day metric vectors. Activated only when a
location has enough history (MIN_STATION_DAYS); otherwise rules-only.
"""

from __future__ import annotations

from pathlib import Path

import joblib
import pandas as pd
from sklearn.ensemble import IsolationForest
from sklearn.preprocessing import StandardScaler

from pipelines.config import MIN_STATION_DAYS, load_settings
from pipelines.quality.metrics import METRIC_COLUMNS

MODEL_FILENAME = "layer1_isolation_forest.joblib"
FEATURE_COLUMNS = METRIC_COLUMNS


def _feature_matrix(metrics: pd.DataFrame) -> pd.DataFrame:
    frame = metrics.copy()
    for col in FEATURE_COLUMNS:
        if col not in frame.columns:
            frame[col] = 0.0
    return frame[FEATURE_COLUMNS].fillna(0.0)


def fit_quality_model(
    metrics: pd.DataFrame,
    *,
    contamination: float = 0.05,
    random_state: int = 42,
) -> dict | None:
    """Fit an Isolation Forest when any location has MIN_STATION_DAYS history."""
    if metrics is None or metrics.empty:
        return None

    eligible = metrics.groupby("location_id").size()
    if eligible.max() < MIN_STATION_DAYS:
        return None

    features = _feature_matrix(metrics)
    scaler = StandardScaler()
    scaled = scaler.fit_transform(features)
    model = IsolationForest(
        contamination=contamination,
        random_state=random_state,
        n_estimators=100,
    )
    model.fit(scaled)
    return {"model": model, "scaler": scaler, "feature_columns": FEATURE_COLUMNS}


def save_quality_model(artifact: dict, models_dir: Path | None = None) -> Path:
    settings = load_settings()
    root = Path(models_dir or Path("models"))
    root.mkdir(parents=True, exist_ok=True)
    path = root / MODEL_FILENAME
    joblib.dump(artifact, path)
    return path


def load_quality_model(models_dir: Path | None = None) -> dict | None:
    path = Path(models_dir or Path("models")) / MODEL_FILENAME
    if not path.exists():
        return None
    return joblib.load(path)


def score_quality(model_artifact: dict | None, metrics: pd.DataFrame) -> pd.DataFrame:
    """Return metrics with anomaly_score and model_is_incident columns."""
    if metrics is None or metrics.empty:
        return metrics

    scored = metrics.copy()
    scored["anomaly_score"] = 0.0
    scored["model_is_incident"] = False

    if model_artifact is None:
        return scored

    features = _feature_matrix(metrics)
    scaled = model_artifact["scaler"].transform(features)
    preds = model_artifact["model"].predict(scaled)
    scores = model_artifact["model"].score_samples(scaled)

    scored["anomaly_score"] = scores
    scored["model_is_incident"] = preds == -1
    return scored


def model_incidents(scored_metrics: pd.DataFrame) -> pd.DataFrame:
    """Build incident rows for model-flagged station-days (no rule overlap handled upstream)."""
    from pipelines.quality.rules import INCIDENT_COLUMNS

    if scored_metrics is None or scored_metrics.empty:
        return pd.DataFrame(columns=INCIDENT_COLUMNS)

    flagged = scored_metrics[scored_metrics["model_is_incident"]]
    rows = []
    for _, row in flagged.iterrows():
        rows.append(
            {
                "location_id": int(row["location_id"]),
                "date_local": str(row["date_local"]),
                "rule_id": "M1",
                "incident_type": "model_flagged",
                "severity": "medium",
                "event_code": "",
                "metric_snapshot": row[FEATURE_COLUMNS].to_json(),
                "is_incident": True,
                "source": "model",
            }
        )
    return pd.DataFrame(rows, columns=INCIDENT_COLUMNS)
