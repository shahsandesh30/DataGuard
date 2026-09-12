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
    """Select and clean the feature columns used by the model.

    Ensures every expected feature column exists (filling missing ones
    with 0.0) and fills any NaNs, since Isolation Forest can't handle them.
    """
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
    """Fit an Isolation Forest when any location has MIN_STATION_DAYS history.

    Returns None (skip modeling) if there isn't enough history yet for
    any single location, so callers fall back to rules-only detection.
    """
    if metrics is None or metrics.empty:
        return None

    # Only proceed if at least one location has enough station-days
    # to make a model fit meaningful.
    eligible = metrics.groupby("locationid").size()
    if eligible.max() < MIN_STATION_DAYS:
        return None

    # Standardize features before fitting, since Isolation Forest is
    # sensitive to differences in feature scale.
    features = _feature_matrix(metrics)
    scaler = StandardScaler()
    scaled = scaler.fit_transform(features)
    model = IsolationForest(
        contamination=contamination,
        random_state=random_state,
        n_estimators=100,
    )
    model.fit(scaled)
    # Bundle the model with its scaler and feature list so scoring
    # later uses the exact same preprocessing.
    return {"model": model, "scaler": scaler, "feature_columns": FEATURE_COLUMNS}


def save_quality_model(artifact: dict, models_dir: Path | None = None) -> Path:
    """Persist the fitted model artifact (model + scaler) to disk via joblib."""
    settings = load_settings()
    root = Path(models_dir or Path("models"))
    root.mkdir(parents=True, exist_ok=True)
    path = root / MODEL_FILENAME
    joblib.dump(artifact, path)
    return path


def load_quality_model(models_dir: Path | None = None) -> dict | None:
    """Load a previously saved model artifact, or None if it doesn't exist yet."""
    path = Path(models_dir or Path("models")) / MODEL_FILENAME
    if not path.exists():
        return None
    return joblib.load(path)


def score_quality(model_artifact: dict | None, metrics: pd.DataFrame) -> pd.DataFrame:
    """Return metrics with anomaly_score and model_is_incident columns.

    If no model artifact is available, returns the metrics with
    default (non-anomalous) values instead of failing.
    """
    if metrics is None or metrics.empty:
        return metrics

    scored = metrics.copy()
    scored["anomaly_score"] = 0.0
    scored["model_is_incident"] = False

    if model_artifact is None:
        return scored

    # Apply the same scaling used at fit time, then score each row.
    features = _feature_matrix(metrics)
    scaled = model_artifact["scaler"].transform(features)
    preds = model_artifact["model"].predict(scaled)  # -1 = anomaly, 1 = normal
    scores = model_artifact["model"].score_samples(scaled)  # lower = more anomalous

    scored["anomaly_score"] = scores
    scored["model_is_incident"] = preds == -1
    return scored


def model_incidents(scored_metrics: pd.DataFrame) -> pd.DataFrame:
    """Build incident rows for model-flagged station-days (no rule overlap handled upstream)."""
    from pipelines.quality.rules import INCIDENT_COLUMNS

    if scored_metrics is None or scored_metrics.empty:
        return pd.DataFrame(columns=INCIDENT_COLUMNS)

    # Only rows the model flagged as anomalous get turned into incidents.
    flagged = scored_metrics[scored_metrics["model_is_incident"]]
    rows = []
    for _, row in flagged.iterrows():
        rows.append(
            {
                "locationid": int(row["locationid"]),
                "date_local": str(row["date_local"]),
                "rule_id": "M1",
                "incident_type": "model_flagged",
                "severity": "medium",
                "event_code": "",
                # Store the raw feature values as JSON for later inspection/debugging.
                "metric_snapshot": row[FEATURE_COLUMNS].to_json(),
                "is_incident": True,
                "source": "model",
            }
        )
    return pd.DataFrame(rows, columns=INCIDENT_COLUMNS)