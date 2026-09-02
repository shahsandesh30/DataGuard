"""Build Layer 1 gold tables: quality metrics and incidents."""

from __future__ import annotations

import json
import logging
from dataclasses import dataclass
from pathlib import Path

import pandas as pd

from pipelines.config import MIN_STATION_DAYS, load_settings
from pipelines.conformance.conform import read_conformed
from pipelines.quality.detector import (
    fit_quality_model,
    model_incidents,
    save_quality_model,
    score_quality,
)
from pipelines.quality.metrics import compute_sensor_day_metrics, compute_station_day_metrics
from pipelines.quality.rules import apply_quality_rules

logger = logging.getLogger(__name__)


@dataclass
class QualityBuildResult:
    sensor_day_rows: int
    station_day_rows: int
    rule_incidents: int
    model_incidents: int
    model_trained: bool
    output_path: str


def _year_from_date_local(date_local: str) -> int:
    return int(str(date_local)[:4])


def _write_partitioned(
    frame: pd.DataFrame,
    gold_root: Path,
    table_name: str,
    key_cols: list[str],
) -> Path:
    output = gold_root / table_name
    if output.exists():
        for existing in output.rglob("*.parquet"):
            existing.unlink()

    output.mkdir(parents=True, exist_ok=True)
    if frame.empty:
        frame.to_parquet(output / "_empty.parquet", index=False, compression="snappy")
        return output

    working = frame.copy()
    working["_year"] = working["date_local"].map(_year_from_date_local)
    for (location_id, year), part in working.groupby(["location_id", "_year"], sort=False):
        partition_dir = output / f"locationid={int(location_id)}" / f"year={int(year)}"
        partition_dir.mkdir(parents=True, exist_ok=True)
        part.drop(columns=["_year"]).to_parquet(
            partition_dir / "part-0.parquet",
            index=False,
            compression="snappy",
        )
    return output


def read_quality_metrics(gold_root: Path | None = None) -> pd.DataFrame:
    settings = load_settings()
    root = Path(gold_root or settings.gold_root) / "layer1" / "quality_metrics"
    files = sorted(root.rglob("*.parquet"))
    if not files:
        return pd.DataFrame()
    return pd.concat((pd.read_parquet(f) for f in files), ignore_index=True)


def read_quality_incidents(gold_root: Path | None = None) -> pd.DataFrame:
    settings = load_settings()
    root = Path(gold_root or settings.gold_root) / "layer1" / "quality_incidents"
    files = sorted(root.rglob("*.parquet"))
    if not files:
        return pd.DataFrame()
    return pd.concat((pd.read_parquet(f) for f in files), ignore_index=True)


def build_quality(
    bronze_root: Path | None = None,
    gold_root: Path | None = None,
    *,
    models_dir: Path | None = None,
) -> QualityBuildResult:
    """Compute Layer 1 metrics and incidents, write to gold."""
    settings = load_settings()
    bronze = Path(bronze_root or settings.bronze_root)
    gold = Path(gold_root or settings.gold_root) / "layer1"

    conformed = read_conformed(bronze)
    sensor_metrics = compute_sensor_day_metrics(conformed)
    station_metrics = compute_station_day_metrics(conformed, bronze, sensor_metrics)

    rule_incidents = apply_quality_rules(station_metrics)

    artifact = fit_quality_model(station_metrics)
    model_trained = artifact is not None
    if model_trained:
        save_quality_model(artifact, models_dir)
        scored = score_quality(artifact, station_metrics)
        ml_incidents = model_incidents(scored)
        rule_keys: set[tuple[int, str]] = set()
        if not rule_incidents.empty:
            rule_keys = {
                (int(r["location_id"]), str(r["date_local"]))
                for _, r in rule_incidents.iterrows()
            }
        if not ml_incidents.empty:
            ml_incidents = ml_incidents[
                ~ml_incidents.apply(
                    lambda r: (int(r["location_id"]), str(r["date_local"])) in rule_keys,
                    axis=1,
                )
            ]
        station_metrics = scored
    else:
        ml_incidents = pd.DataFrame()
        logger.info(
            "Skipping Isolation Forest — no location has %s station-days yet",
            MIN_STATION_DAYS,
        )

    metrics_path = _write_partitioned(station_metrics, gold, "quality_metrics", ["location_id", "date_local"])
    sensor_path = _write_partitioned(sensor_metrics, gold, "quality_sensor_metrics", ["location_id", "date_local"])
    all_incidents = pd.concat([rule_incidents, ml_incidents], ignore_index=True)
    incidents_path = _write_partitioned(all_incidents, gold, "quality_incidents", ["location_id", "date_local"])

    summary = {
        "sensor_day_rows": int(len(sensor_metrics)),
        "station_day_rows": int(len(station_metrics)),
        "rule_incidents": int(len(rule_incidents)),
        "model_incidents": int(len(ml_incidents)),
        "model_trained": model_trained,
        "metrics_path": str(metrics_path),
        "sensor_metrics_path": str(sensor_path),
        "incidents_path": str(incidents_path),
    }
    gold.mkdir(parents=True, exist_ok=True)
    (gold / "_quality_build.json").write_text(json.dumps(summary, indent=2) + "\n", encoding="utf-8")

    logger.info(
        "Layer 1 built: %s station-days, %s rule incidents, %s model incidents -> %s",
        len(station_metrics),
        len(rule_incidents),
        len(ml_incidents),
        gold,
    )
    return QualityBuildResult(
        sensor_day_rows=int(len(sensor_metrics)),
        station_day_rows=int(len(station_metrics)),
        rule_incidents=int(len(rule_incidents)),
        model_incidents=int(len(ml_incidents)),
        model_trained=model_trained,
        output_path=str(gold),
    )
