"""Layer 2 model evaluation: run-level metrics

Since Layer 2 has no labelled ground truth, this module computes 
the best available proxy: how often a model-flagged alert also matches 
an independent weak-label rule, how much the ensemble's detectors 
agree with each other, and how the flag rate compares to the configured 
contamination target. These are tracked per run, perparameter, and 
written as one small Parquet row per run so they accumulate
into a queryable history.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from datetime import UTC, datetime

import pandas as pd

from pipelines import storage
from pipelines.config import load_settings

logger = logging.getLogger(__name__)

MODEL_RUNS_DATASET = "model_runs"

RUN_METRIC_COLUMNS = [
    "parameter",
    "run_at",
    "ensemble_trained",
    "feature_rows",
    "alert_rows",
    "flag_rate",
    "weak_label_agreement_rate",
    "detector_agreement_mean",
    "avg_alert_score",
]


@dataclass
class RunMetrics:
    parameter: str
    run_at: str
    ensemble_trained: bool
    feature_rows: int
    alert_rows: int
    flag_rate: float
    weak_label_agreement_rate: float | None
    detector_agreement_mean: float | None
    avg_alert_score: float | None

    def to_row(self) -> dict:
        return {col: getattr(self, col) for col in RUN_METRIC_COLUMNS}


def compute_run_metrics(
    parameter: str,
    features: pd.DataFrame,
    alerts: pd.DataFrame,
    *,
    ensemble_trained: bool,
    run_at: str | None = None,
) -> RunMetrics:
    """Summarise one parameter's build into a single comparable record.

    ``features`` is that parameter's full scored population for this run;
    ``alerts`` is the subset score_events flagged and ranked. Both are
    expected already filtered to one parameter — this function does not
    group internally, so it stays usable whichever way the caller slices.
    """
    run_at = run_at or datetime.now(UTC).isoformat()
    feature_rows = int(len(features))
    alert_rows = int(len(alerts))
    flag_rate = (alert_rows / feature_rows) if feature_rows else 0.0

    # Not precision in the labelled-data sense — weak labels come from an
    # independent rule, not from this model, so agreement with them is
    # external corroboration rather than the model grading its own output.
    weak_label_agreement_rate = (
        float(alerts["weak_label"].mean()) if alert_rows and "weak_label" in alerts else None
    )
    detector_agreement_mean = (
        float(alerts["agreement_count"].mean()) if alert_rows and "agreement_count" in alerts else None
    )
    avg_alert_score = (
        float(alerts["alert_score"].mean()) if alert_rows and "alert_score" in alerts else None
    )

    return RunMetrics(
        parameter=parameter,
        run_at=run_at,
        ensemble_trained=bool(ensemble_trained),
        feature_rows=feature_rows,
        alert_rows=alert_rows,
        flag_rate=flag_rate,
        weak_label_agreement_rate=weak_label_agreement_rate,
        detector_agreement_mean=detector_agreement_mean,
        avg_alert_score=avg_alert_score,
    )


def save_run_metrics(metrics: RunMetrics, gold_root: str | None = None) -> str:
    """Append one run's metrics to the model_runs dataset (never overwritten)."""
    frame = pd.DataFrame([metrics.to_row()])
    path = storage.write_model_parquet(frame, gold_root, MODEL_RUNS_DATASET)
    logger.info("Saved Layer 2 run metrics for %s -> %s", metrics.parameter, path)
    return path


def evaluate_and_save(
    parameter: str,
    features: pd.DataFrame,
    alerts: pd.DataFrame,
    *,
    ensemble_trained: bool,
    gold_root: str | None = None,
) -> RunMetrics:
    """Compute this parameter's run metrics and persist them in one call."""
    metrics = compute_run_metrics(parameter, features, alerts, ensemble_trained=ensemble_trained)
    save_run_metrics(metrics, gold_root)
    return metrics


def read_run_history(gold_root: str | None = None, parameter: str | None = None) -> pd.DataFrame:
    """All recorded runs, optionally filtered to one parameter, newest first."""
    settings = load_settings()
    gold = storage.join(gold_root or settings.gold_root, "layer2")
    history = storage.read_parquet(gold, MODEL_RUNS_DATASET)
    if history.empty:
        return history
    if parameter is not None and "parameter" in history.columns:
        history = history[history["parameter"] == parameter]
    if "run_at" in history.columns:
        history = history.sort_values("run_at", ascending=False).reset_index(drop=True)
    return history


def read_latest_run(gold_root: str | None = None, parameter: str | None = None) -> pd.Series | None:
    """Most recent run record, for a dashboard 'last build time' panel.

    Returns None (not an exception) when no runs are recorded yet, since this
    is read on every dashboard load and an empty history is the normal state
    before the first build.
    """
    history = read_run_history(gold_root, parameter)
    return history.iloc[0] if not history.empty else None