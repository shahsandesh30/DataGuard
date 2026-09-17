"""Build the fusion gold table: Layer 2 alerts trust-scored by Layer 1 health."""

from __future__ import annotations

import json
import logging
from dataclasses import dataclass
from pathlib import Path

import pandas as pd

from pipelines import storage
from pipelines.config import (
    FUSION_STATUS_ESCALATED,
    FUSION_STATUS_QUARANTINED,
    load_settings,
)
from pipelines.fusion.trust_score import fuse
from pipelines.quality.build import read_quality_incidents

logger = logging.getLogger(__name__)


@dataclass
class FusionBuildResult:
    alert_rows: int
    escalated: int
    quarantined: int
    output_path: str


def read_trust_alerts(gold_root: str | Path | None = None) -> pd.DataFrame:
    settings = load_settings()
    return storage.read_parquet(gold_root or settings.gold_root, "fusion/trust_alerts")


def read_event_alerts(gold_root: str | Path | None = None) -> pd.DataFrame:
    """Read the Layer 2 alert table.

    Fusion depends on the gold table, not on pipelines.detection: that module's
    own reader globs the local filesystem and cannot see an S3 zone.
    """
    settings = load_settings()
    return storage.read_parquet(gold_root or settings.gold_root, "layer2/event_alerts")


def build_fusion(gold_root: str | Path | None = None) -> FusionBuildResult:
    """Join Layer 1 incidents with Layer 2 alerts and write fusion gold."""
    settings = load_settings()
    gold = storage.normalize(gold_root or settings.gold_root)

    fused = fuse(read_quality_incidents(gold), read_event_alerts(gold))
    fusion_root = storage.join(gold, "fusion")
    storage.write_parquet(fused, fusion_root, "trust_alerts")

    status = fused["status"] if not fused.empty else pd.Series(dtype=str)
    result = FusionBuildResult(
        alert_rows=int(len(fused)),
        escalated=int((status == FUSION_STATUS_ESCALATED).sum()),
        quarantined=int((status == FUSION_STATUS_QUARANTINED).sum()),
        output_path=fusion_root,
    )
    storage.write_text(
        storage.join(fusion_root, "_build.json"), json.dumps(result.__dict__, indent=2) + "\n"
    )
    logger.info(
        "Fusion built: %s alerts (%s escalated, %s quarantined) -> %s",
        result.alert_rows,
        result.escalated,
        result.quarantined,
        fusion_root,
    )
    return result
