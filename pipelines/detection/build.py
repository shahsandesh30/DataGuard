"""
Build Layer 2 gold tables: event features and ranked alerts.
Eachparameter's rows are grouped, fit, scored, and evaluated independently, 
thenconcatenated back into one output table.
"""

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
from pipelines.detection.evaluation import evaluate_and_save
from pipelines.detection.features import build_event_features, weak_labels

logger = logging.getLogger(__name__)


@dataclass
class DetectionBuildResult:
    feature_rows: int
    alert_rows: int
    ensemble_trained: bool  # True if at least one parameter had enough rows to train
    parameters_trained: list[str]
    parameters_skipped: list[str]
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


def _build_for_parameter(
    parameter: str,
    group: pd.DataFrame,
    labels: pd.Series,
    gold_root: str,
) -> tuple[pd.DataFrame, bool]:
    """Fit, score, and evaluate one parameter's rows. Returns (alerts, trained)."""
    trained = len(group) >= MIN_EVENT_ROWS
    group_labels = labels.reindex(group.index)

    if not trained:
        logger.info(
            "Skipping Layer 2 ensemble for %s — need at least %s feature rows (have %s)",
            parameter,
            MIN_EVENT_ROWS,
            len(group),
        )
        alerts = pd.DataFrame(columns=EVENT_ALERT_COLUMNS)
    else:
        models = fit_ensemble(group)
        alerts = score_events(models, group, weak_label=group_labels)

    evaluate_and_save(
        parameter,
        features=group,
        alerts=alerts,
        ensemble_trained=trained,
        gold_root=gold_root,
    )
    return alerts, trained


def build_detection(
    silver_root: str | Path | None = None,
    gold_root: str | Path | None = None,
) -> DetectionBuildResult:
    """
    Compute Layer 2 features and ranked alerts, write to gold
    per parameter, and return a summary of the build.
    """
    settings = load_settings()
    silver_path = silver_root or settings.silver_root
    gold = storage.join(gold_root or settings.gold_root, "layer2")

    silver = read_silver(silver_path)
    input_measurement_rows = int(len(silver))
    features = build_event_features(silver)
    labels = weak_labels(features, silver)

    if "parameter" not in features.columns:
        raise ValueError(
            "build_event_features output has no 'parameter' column — "
            "per-parameter modelling requires it."
        )

    all_alerts: list[pd.DataFrame] = []
    parameters_trained: list[str] = []
    parameters_skipped: list[str] = []

    for parameter, group in features.groupby("parameter", sort=False):
        alerts, trained = _build_for_parameter(parameter, group, labels, gold)
        if trained:
            parameters_trained.append(parameter)
        else:
            parameters_skipped.append(parameter)
        if not alerts.empty:
            all_alerts.append(alerts)

    alerts = (
        pd.concat(all_alerts, ignore_index=True)
        if all_alerts
        else pd.DataFrame(columns=EVENT_ALERT_COLUMNS)
    )

    features_path = storage.write_parquet(features, gold, "event_features")
    alerts_path = storage.write_parquet(alerts, gold, "event_alerts")

    result = DetectionBuildResult(
        feature_rows=int(len(features)),
        alert_rows=int(len(alerts)),
        ensemble_trained=bool(parameters_trained),
        parameters_trained=parameters_trained,
        parameters_skipped=parameters_skipped,
        features_path=str(features_path),
        alerts_path=str(alerts_path),
    )

    storage.write_text(
        storage.join(gold, "_detection_build.json"),
        build_summary_json(result, input_measurement_rows=input_measurement_rows),
    )

    logger.info(
        "Layer 2 built: %s feature rows, %s alerts. Trained: %s. Skipped (insufficient rows): %s -> %s",
        len(features),
        len(alerts),
        parameters_trained,
        parameters_skipped,
        gold,
    )
    return result
