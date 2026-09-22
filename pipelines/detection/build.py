"""Build Layer 2 gold tables: event features and ranked alerts."""

from __future__ import annotations

import json
import logging
from dataclasses import dataclass
from pathlib import Path

import pandas as pd

from pipelines import storage
from pipelines.config import MIN_EVENT_ROWS, load_settings
from pipelines.conformance.conform import read_conformed, read_silver
from pipelines.detection.ensemble import EVENT_ALERT_COLUMNS, fit_ensemble, score_events
from pipelines.detection.features import build_event_features, weak_labels
from pipelines.quality.build import _write_partitioned

logger = logging.getLogger(__name__)

@dataclass
class DetectionBuildResult:
    feature_rows: int
    alert_rows: int
    ensemble_trained: bool
    features_path: str
    alerts_path: str


def read_event_features(gold_root: Path | None = None) -> pd.DataFrame:
    settings = load_settings()
    root = Path(gold_root or settings.gold_root) / "layer2" / "event_features"
    files = sorted(root.rglob("*.parquet"))
    if not files:
        return pd.DataFrame()
    return pd.concat((pd.read_parquet(f) for f in files), ignore_index=True)


def read_event_alerts(gold_root: Path | None = None) -> pd.DataFrame:
    settings = load_settings()
    root = Path(gold_root or settings.gold_root) / "layer2" / "event_alerts"
    files = sorted(root.rglob("*.parquet"))
    if not files:
        return pd.DataFrame()
    return pd.concat((pd.read_parquet(f) for f in files), ignore_index=True)


def build_detection(
    silver_root: Path | None = None,
    gold_root: Path | None = None,
) -> DetectionBuildResult:
    """Compute Layer 2 features and ranked alerts, write to gold."""
    settings = load_settings()
    silver = silver_root or settings.silver_root
    gold = storage.join(gold_root or settings.gold_root, "layer2")

    silver = read_silver(silver)
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
        alerts_path=str(alerts_path)
    )

    storage.write_text(
        storage.join(gold, "_detection_build.json"), json.dumps(result.__dict__, indent=2) + "\n"
    )

    logger.info(
        "Layer 2 built: %s feature rows, %s alerts (trained=%s) -> %s",
        len(features),
        len(alerts),
        ensemble_trained,
        gold,
    )
    return result
