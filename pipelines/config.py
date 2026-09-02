"""Central configuration, loaded from environment variables (see .env.example)."""

from __future__ import annotations

import os
from dataclasses import dataclass, field
from pathlib import Path

# OpenAQ public archive on AWS Open Data. Public bucket, no credentials needed.
OPENAQ_ARCHIVE_BUCKET = "openaq-data-archive"
OPENAQ_ARCHIVE_REGION = "us-east-1"

# OpenAQ commits to writing files ~72 hours after end of day (location timezone).
# Lateness beyond this is a Layer 1 freshness anomaly, measured against a
# published promise rather than a threshold we invented.
DELIVERY_COMMITMENT_HOURS = 72

# Layer 1 — minimum station-days before training Isolation Forest per location.
MIN_STATION_DAYS = 14

# Layer 1 rule thresholds (see pipelines/quality/rules.py).
STUCK_RUN_THRESHOLD = 6
MISSING_RATE_THRESHOLD = 0.25
VARIANCE_EPS = 1e-9
DEFAULT_HOURLY_READINGS = 24
TRAILING_CADENCE_DAYS = 7

# Layer 2 — pollution event detection.
LAYER2_REGION_ID = "sydney_metro"
LAYER2_PM_PARAMETERS = ("pm25", "pm10", "pm1")
LAYER2_BASELINE_DAYS = 7
MIN_EVENT_ROWS = 20
WEAK_LABEL_PM25_RATIO = 2.0
WEAK_LABEL_MIN_LOCATIONS = 2
DEFAULT_LOCATION_IDS = [1544061, 1601414, 2455394, 6430870, 2178]


def _env_path(name: str, default: str) -> Path:
    return Path(os.getenv(name, default))


@dataclass(frozen=True)
class Settings:
    aws_region: str = field(default_factory=lambda: os.getenv("AWS_REGION", "us-east-1"))
    bronze_bucket: str = field(default_factory=lambda: os.getenv("BRONZE_BUCKET", "dataguard-bronze"))
    silver_bucket: str = field(default_factory=lambda: os.getenv("SILVER_BUCKET", "dataguard-silver"))
    gold_bucket: str = field(default_factory=lambda: os.getenv("GOLD_BUCKET", "dataguard-gold"))
    athena_workgroup: str = field(default_factory=lambda: os.getenv("ATHENA_WORKGROUP", "dataguard"))
    athena_output: str = field(default_factory=lambda: os.getenv("ATHENA_OUTPUT", ""))
    glue_database: str = field(default_factory=lambda: os.getenv("GLUE_DATABASE", "dataguard"))
    data_root: Path = field(default_factory=lambda: _env_path("DATA_ROOT", "data"))
    bronze_root: Path = field(
        default_factory=lambda: _env_path(
            "BRONZE_ROOT", str(_env_path("DATA_ROOT", "data") / "bronze")
        )
    )
    silver_root: Path = field(
        default_factory=lambda: _env_path(
            "SILVER_ROOT", str(_env_path("DATA_ROOT", "data") / "silver")
        )
    )
    gold_root: Path = field(
        default_factory=lambda: _env_path(
            "GOLD_ROOT", str(_env_path("DATA_ROOT", "data") / "gold")
        )
    )


def load_settings() -> Settings:
    return Settings()
