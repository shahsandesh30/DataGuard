"""Build Layer 2 gold tables: event features and ranked alerts."""

from __future__ import annotations

import json
import logging
from dataclasses import dataclass
from pathlib import Path

import pandas as pd

from pipelines.config import MIN_EVENT_ROWS, load_settings
from pipelines.conformance.conform import read_conformed
from pipelines.detection.ensemble import EVENT_ALERT_COLUMNS, fit_ensemble, score_events
from pipelines.detection.features import build_event_features, weak_labels
from pipelines.quality.build import _write_partitioned

logger = logging.getLogger(__name__)


@dataclass
class DetectionBuildResult:
    feature_rows: int
    alert_rows: int
    ensemble_trained: bool
    output_path: str


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
    bronze_root: Path | None = None,
    gold_root: Path | None = None,
) -> DetectionBuildResult:
    """Compute Layer 2 features and ranked alerts, write to gold."""
    settings = load_settings()
    bronze = Path(bronze_root or settings.bronze_root)
    gold = Path(gold_root or settings.gold_root) / "layer2"

    conformed = read_conformed(bronze)
    features = build_event_features(conformed)
    labels = weak_labels(features, conformed)

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

    features_path = _write_partitioned(features, gold, "event_features", ["location_id", "date_local"])
    alerts_path = _write_partitioned(alerts, gold, "event_alerts", ["location_id", "date_local"])

    summary = {
        "feature_rows": int(len(features)),
        "alert_rows": int(len(alerts)),
        "ensemble_trained": ensemble_trained,
        "features_path": str(features_path),
        "alerts_path": str(alerts_path),
    }
    gold.mkdir(parents=True, exist_ok=True)
    (gold / "_detection_build.json").write_text(json.dumps(summary, indent=2) + "\n", encoding="utf-8")

    logger.info(
        "Layer 2 built: %s feature rows, %s alerts (trained=%s) -> %s",
        len(features),
        len(alerts),
        ensemble_trained,
        gold,
    )
    return DetectionBuildResult(
        feature_rows=int(len(features)),
        alert_rows=int(len(alerts)),
        ensemble_trained=ensemble_trained,
        output_path=str(gold),
    )
