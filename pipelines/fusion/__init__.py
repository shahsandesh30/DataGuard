"""Fusion: trust-score every Layer 2 alert using Layer 1 data health."""

from pipelines.fusion.build import FusionBuildResult, build_fusion, read_trust_alerts
from pipelines.fusion.trust_score import TRUST_ALERT_COLUMNS, aggregate_incidents, fuse

__all__ = [
    "FusionBuildResult",
    "TRUST_ALERT_COLUMNS",
    "aggregate_incidents",
    "build_fusion",
    "fuse",
    "read_trust_alerts",
]
