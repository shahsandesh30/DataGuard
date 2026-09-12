"""Layer 1: is the data itself healthy? Quality metrics + anomaly model.

This module re-exports the public API for the quality pipeline:
build results and readers from `build`, metric computation helpers
from `metrics`, and rule-based incident detection from `rules`.
"""

# Core build pipeline: assembles quality metrics/incidents and
# provides readers for previously computed results.
from pipelines.quality.build import (
    QualityBuildResult,
    build_quality,
    read_quality_incidents,
    read_quality_metrics,
)

# Metric computation: column schemas and functions for aggregating
# sensor-level and station-level daily quality metrics.
from pipelines.quality.metrics import (
    METRIC_COLUMNS,
    SENSOR_DAY_COLUMNS,
    STATION_DAY_COLUMNS,
    compute_sensor_day_metrics,
    compute_station_day_metrics,
)

# Rule-based incident detection: flags anomalies based on computed metrics.
from pipelines.quality.rules import INCIDENT_COLUMNS, apply_quality_rules

# Public API of this package, exposed for consumers importing
# `pipelines.quality` directly.
__all__ = [
    "METRIC_COLUMNS",
    "SENSOR_DAY_COLUMNS",
    "STATION_DAY_COLUMNS",
    "INCIDENT_COLUMNS",
    "QualityBuildResult",
    "apply_quality_rules",
    "build_quality",
    "compute_sensor_day_metrics",
    "compute_station_day_metrics",
    "read_quality_incidents",
    "read_quality_metrics",
]