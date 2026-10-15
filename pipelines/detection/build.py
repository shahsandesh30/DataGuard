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

from pipelines import model_store, storage
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
    model_paths: dict[str, str]  # parameter -> this run's saved ensemble


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
    *,
    run_id: str,
    models_root: str | Path | None,
) -> tuple[pd.DataFrame, str | None]:
    """Fit, score, and evaluate one parameter's rows.

    Returns the alerts and the saved model path, which is ``None`` when the
    parameter had too little history to train.
    """
    trained = len(group) >= MIN_EVENT_ROWS
    model_path = None
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
        if models is not None:
            model_path = model_store.save_model(
                models,
                "layer2",
                f"ensemble_{parameter}",
                run_id=run_id,
                models_root=models_root,
            )

    evaluate_and_save(
        parameter,
        features=group,
        alerts=alerts,
        ensemble_trained=trained,
        weak_label=group_labels,
        gold_root=gold_root,
    )
    return alerts, model_path


def build_detection(
    silver_root: str | Path | None = None,
    gold_root: str | Path | None = None,
    *,
    models_root: str | Path | None = None,
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
    model_paths: dict[str, str] = {}
    run_id = model_store.new_run_id()

    for parameter, group in features.groupby("parameter", sort=False):
        alerts, model_path = _build_for_parameter(
            parameter, group, labels, gold, run_id=run_id, models_root=models_root
        )
        if model_path is not None:
            parameters_trained.append(parameter)
            model_paths[parameter] = model_path
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
        model_paths=model_paths,
    )

    storage.write_text(
        storage.join(gold, "_detection_build.json"),
        build_summary_json(result, input_measurement_rows=input_measurement_rows),
    )

    logger.info(
        "Layer 2 built: %s feature rows, %s alerts. Trained: %s. Skipped: %s -> %s",
        len(features),
        len(alerts),
        parameters_trained,
        parameters_skipped,
        gold,
    )
    return result
