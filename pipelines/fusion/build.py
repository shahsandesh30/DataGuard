"""Build fusion gold table: trust-scored Layer 2 alerts."""

from __future__ import annotations

import json
import logging
from dataclasses import dataclass
from pathlib import Path

import pandas as pd

from pipelines.config import load_settings
from pipelines.detection.build import read_event_alerts
from pipelines.fusion.trust_score import fuse
from pipelines.quality.build import _write_partitioned, read_quality_incidents

logger = logging.getLogger(__name__)


@dataclass
class FusionBuildResult:
    alert_rows: int
    escalated: int
    quarantined: int
    output_path: str


def read_trust_alerts(gold_root: Path | None = None) -> pd.DataFrame:
    settings = load_settings()
    root = Path(gold_root or settings.gold_root) / "fusion" / "trust_alerts"
    files = sorted(root.rglob("*.parquet"))
    if not files:
        return pd.DataFrame()
    return pd.concat((pd.read_parquet(f) for f in files), ignore_index=True)


def build_fusion(gold_root: Path | None = None) -> FusionBuildResult:
    """Join Layer 1 incidents with Layer 2 alerts and write fusion gold."""
    settings = load_settings()
    gold = Path(gold_root or settings.gold_root)

    incidents = read_quality_incidents(gold)
    alerts = read_event_alerts(gold)
    fused = fuse(incidents, alerts)

    fusion_root = gold / "fusion"
    alerts_path = _write_partitioned(fused, fusion_root, "trust_alerts", ["locationid", "date_local"])

    escalated = int((fused["status"] == "escalated").sum()) if not fused.empty else 0
    quarantined = int((fused["status"] == "quarantined").sum()) if not fused.empty else 0

    summary = {
        "alert_rows": int(len(fused)),
        "escalated": escalated,
        "quarantined": quarantined,
        "alerts_path": str(alerts_path),
    }
    fusion_root.mkdir(parents=True, exist_ok=True)
    (fusion_root / "_fusion_build.json").write_text(
        json.dumps(summary, indent=2) + "\n", encoding="utf-8"
    )

    logger.info(
        "Fusion built: %s alerts (%s escalated, %s quarantined) -> %s",
        len(fused),
        escalated,
        quarantined,
        fusion_root,
    )
    return FusionBuildResult(
        alert_rows=int(len(fused)),
        escalated=escalated,
        quarantined=quarantined,
        output_path=str(fusion_root),
    )
