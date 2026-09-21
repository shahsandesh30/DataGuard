"""Build Layer 1 gold tables: quality metrics and incidents.

Measurements come from the silver zone. File-level signals (schema drift,
file presence, delivery lateness) come from bronze, because they are
properties of the delivered file rather than of the readings inside it.
"""

from __future__ import annotations

import json
import logging
from dataclasses import dataclass
from pathlib import Path

import pandas as pd

from pipelines import storage
from pipelines.config import MIN_STATION_DAYS, load_settings
from pipelines.conformance.conform import read_silver
from pipelines.quality.detector import (
    fit_quality_model,
    model_incidents,
    save_quality_model,
    score_quality,
)
from pipelines.quality.metrics import compute_sensor_day_metrics, compute_station_day_metrics
from pipelines.quality.rules import apply_quality_rules

logger = logging.getLogger(__name__)


def _write_partitioned(
    frame: pd.DataFrame, root: str | Path, name: str, _key_cols: list[str] | None = None
) -> str:
    """Shim: ``pipelines.detection.build`` imports this name.
    """
    return storage.write_parquet(frame, root, name)


@dataclass
class QualityBuildResult:
    sensor_day_rows: int
    station_day_rows: int
    rule_incidents: int
    model_incidents: int
    model_trained: bool
    output_path: str


def read_quality_metrics(gold_root: str | Path | None = None) -> pd.DataFrame:
    settings = load_settings()
    return storage.read_parquet(gold_root or settings.gold_root, "layer1/quality_metrics")


def read_quality_incidents(gold_root: str | Path | None = None) -> pd.DataFrame:
    settings = load_settings()
    return storage.read_parquet(gold_root or settings.gold_root, "layer1/quality_incidents")


def _drop_duplicate_of_rules(
    ml_incidents: pd.DataFrame, rule_incidents: pd.DataFrame
) -> pd.DataFrame:
    """Keep model incidents only for station-days no rule already flagged."""
    if ml_incidents.empty or rule_incidents.empty:
        return ml_incidents
    flagged = set(
        zip(
            rule_incidents["locationid"].astype(int),
            rule_incidents["date_local"].astype(str),
            strict=True,
        )
    )
    keys = zip(
        ml_incidents["locationid"].astype(int),
        ml_incidents["date_local"].astype(str),
        strict=True,
    )
    return ml_incidents[[key not in flagged for key in keys]]


def build_quality(
    silver_root: str | Path | None = None,
    bronze_root: str | Path | None = None,
    gold_root: str | Path | None = None,
    *,
    models_dir: Path | None = None,
) -> QualityBuildResult:
    """Compute Layer 1 metrics and incidents, write them to gold."""
    settings = load_settings()
    bronze = bronze_root or settings.bronze_root
    gold = storage.join(gold_root or settings.gold_root, "layer1")

    measurements = read_silver(silver_root)
    sensor_metrics = compute_sensor_day_metrics(measurements)
    station_metrics = compute_station_day_metrics(measurements, bronze, sensor_metrics)
    rule_incidents = apply_quality_rules(station_metrics)

    artifact = fit_quality_model(station_metrics)
    if artifact is None:
        ml_incidents = pd.DataFrame(columns=rule_incidents.columns)
        logger.info(
            "Skipping Isolation Forest — no location has %s station-days yet",
            MIN_STATION_DAYS,
        )
    else:
        save_quality_model(artifact, models_dir)
        station_metrics = score_quality(artifact, station_metrics)
        ml_incidents = _drop_duplicate_of_rules(
            model_incidents(station_metrics), rule_incidents
        )

    storage.write_parquet(station_metrics, gold, "quality_metrics")
    storage.write_parquet(sensor_metrics, gold, "quality_sensor_metrics")
    storage.write_parquet(
        pd.concat([rule_incidents, ml_incidents], ignore_index=True),
        gold,
        "quality_incidents",
    )

    result = QualityBuildResult(
        sensor_day_rows=int(len(sensor_metrics)),
        station_day_rows=int(len(station_metrics)),
        rule_incidents=int(len(rule_incidents)),
        model_incidents=int(len(ml_incidents)),
        model_trained=artifact is not None,
        output_path=storage.normalize(gold),
    )
    storage.write_text(
        storage.join(gold, "_build.json"), json.dumps(result.__dict__, indent=2) + "\n"
    )
    logger.info(
        "Layer 1 built: %s station-days, %s rule incidents, %s model incidents -> %s",
        result.station_day_rows,
        result.rule_incidents,
        result.model_incidents,
        gold,
    )
    return result
