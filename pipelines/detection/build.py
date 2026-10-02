"""Build Layer 2 gold tables: event features and ranked alerts."""

from __future__ import annotations

import logging
from dataclasses import dataclass
from pathlib import Path

import pandas as pd

from pipelines import storage
from pipelines.build_metadata import build_summary_json
from pipelines.config import MIN_EVENT_ROWS, load_settings
from pipelines.conformance.conform import read_silver
from pipelines.detection.ensemble import EVENT_ALERT_COLUMNS, fit_ensemble, score_events
from pipelines.detection.features import build_event_features, weak_labels

logger = logging.getLogger(__name__)


@dataclass
class DetectionBuildResult:
    feature_rows: int
    alert_rows: int
    ensemble_trained: bool
    features_path: str
    alerts_path: str


def read_event_features(gold_root: str | Path | None = None) -> pd.DataFrame:
    """Read Layer 2 features from either a local or S3 gold zone."""
    settings = load_settings()
    return storage.read_parquet(gold_root or settings.gold_root, "layer2/event_features")


def read_event_alerts(gold_root: str | Path | None = None) -> pd.DataFrame:
    """Read Layer 2 alerts from either a local or S3 gold zone."""
    settings = load_settings()
    return storage.read_parquet(gold_root or settings.gold_root, "layer2/event_alerts")


def build_detection(
    silver_root: str | Path | None = None,
    gold_root: str | Path | None = None,
) -> DetectionBuildResult:
    """Compute Layer 2 features and ranked alerts, write to gold."""
    settings = load_settings()
    silver = silver_root or settings.silver_root
    gold = storage.join(gold_root or settings.gold_root, "layer2")

    silver = read_silver(silver)
    input_measurement_rows = int(len(silver))
    features = build_event_features(silver)
    labels = weak_labels(features, silver)

    ensemble_trained = len(features) >= MIN_EVENT_ROWS
    models = fit_ensemble(features) if ensemble_trained else None
    if not ensemble_trained:
        logger.info(
            "Skipping Layer 2 ensemble — need at least %s feature rows (have %s)",
            MIN_EVENT_ROWS,
            len(features),
        )
        alerts = pd.DataFrame(columns=EVENT_ALERT_COLUMNS)
    else:
        alerts = score_events(models, features, weak_label=labels)

    features_path = storage.write_parquet(features, gold, "event_features")
    alerts_path = storage.write_parquet(alerts, gold, "event_alerts")

    result = DetectionBuildResult(
        feature_rows=int(len(features)),
        alert_rows=int(len(alerts)),
        ensemble_trained=ensemble_trained,
        features_path=str(features_path),
        alerts_path=str(alerts_path),
    )

    storage.write_text(
        storage.join(gold, "_detection_build.json"),
        build_summary_json(result, input_measurement_rows=input_measurement_rows),
    )

    logger.info(
        "Layer 2 built: %s feature rows, %s alerts (trained=%s) -> %s",
        len(features),
        len(alerts),
        ensemble_trained,
        gold,
    )
    return result
