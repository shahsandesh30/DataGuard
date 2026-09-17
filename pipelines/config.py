"""Every tunable in one place, loaded from .env (see .env.example).

Two kinds of setting live here:

* ``Settings`` — where data lives. Read per run, so a zone can be pointed at
  S3 without touching code.
* Module constants — thresholds the pipeline reasons with. Changing one
  changes what counts as an incident, so they are deliberately not per-run.
"""

from __future__ import annotations

import os
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from dotenv import load_dotenv

load_dotenv()

# --------------------------------------------------------------------------- #
# Source
# --------------------------------------------------------------------------- #

# OpenAQ's public archive on AWS Open Data. Read with unsigned requests, so no
# credentials are needed to ingest.
OPENAQ_ARCHIVE_BUCKET = "openaq-data-archive"
OPENAQ_ARCHIVE_REGION = "us-east-1"

# Stations to ingest by default: four Sydney metro sites plus one spare.
# Spelling is load-bearing — pipelines/detection imports this name.
DEFAULT_locationidS = [1544061, 1601414, 2455394, 6430870, 2178]

# --------------------------------------------------------------------------- #
# Layer 1 — data health
# --------------------------------------------------------------------------- #

# OpenAQ commits to publishing a day's file ~72 hours after that day ends. This
# is a published promise, not a threshold we invented, so lateness beyond it is
# measurable rather than arbitrary.
DELIVERY_COMMITMENT_HOURS = 72

MIN_STATION_DAYS = 14  # history before the Isolation Forest may train
STUCK_RUN_THRESHOLD = 6  # identical consecutive readings that mean "stuck"
MISSING_RATE_THRESHOLD = 0.25  # share of expected readings that may go missing
DEFAULT_HOURLY_READINGS = 24  # assumed cadence before a station has history
TRAILING_CADENCE_DAYS = 7  # window for learning a sensor's real cadence
VARIANCE_EPS = 1e-9  # below this, a day's readings never moved

# --------------------------------------------------------------------------- #
# Layer 2 — pollution events (owned by pipelines/detection)
# --------------------------------------------------------------------------- #

LAYER2_REGION_ID = "sydney_metro"
LAYER2_PM_PARAMETERS = ("pm25", "pm10", "pm1")
LAYER2_BASELINE_DAYS = 7
MIN_EVENT_ROWS = 20  # history before the ensemble may train
WEAK_LABEL_PM25_RATIO = 2.0
WEAK_LABEL_MIN_LOCATIONS = 2

# --------------------------------------------------------------------------- #
# Fusion — Layer 1 x Layer 2
# --------------------------------------------------------------------------- #

# How much a Layer 1 incident discounts a Layer 2 alert's score.
SEVERITY_PENALTY = {"low": 0.2, "medium": 0.4, "high": 0.7}
FUSION_STATUS_ESCALATED = "escalated"
FUSION_STATUS_QUARANTINED = "quarantined"

# --------------------------------------------------------------------------- #
# Where the data lives
# --------------------------------------------------------------------------- #

# Glue table name for the silver zone. pipelines/detection runs
# "SELECT * FROM silver_data", so conform registers silver under that name.
SILVER_GLUE_TABLE = "silver_data"


def _env(name: str, default: str) -> Any:
    return field(default_factory=lambda: os.getenv(name, default))


def _zone(name: str, subdirectory: str) -> Any:
    """Zone root: an explicit override, else ``<DATA_ROOT>/<subdirectory>``."""

    def resolve() -> str:
        override = os.getenv(name)
        if override:
            return override.strip()
        return str(Path(os.getenv("DATA_ROOT", "data")) / subdirectory)

    return field(default_factory=resolve)


@dataclass(frozen=True)
class Settings:
    """Where each zone lives, and the AWS names Layer 2 queries through.

    A zone root is a plain string: ``data/silver`` is a local directory,
    ``s3://dataguard-openaq-silver`` is an S3 prefix. pipelines.storage
    dispatches on the ``s3://`` scheme, so the two are interchangeable.
    """

    bronze_root: str = _zone("BRONZE_ROOT", "bronze")
    silver_root: str = _zone("SILVER_ROOT", "silver")
    gold_root: str = _zone("GOLD_ROOT", "gold")

    # Used when a zone is on S3, and by pipelines/detection's Athena reads.
    glue_database: str = _env("GLUE_DATABASE", "dataguard")
    athena_output: str = _env("ATHENA_OUTPUT", "")
    silver_bucket: str = _env("SILVER_BUCKET", "dataguard-silver")
    gold_bucket: str = _env("GOLD_BUCKET", "dataguard-gold")


def load_settings() -> Settings:
    return Settings()
