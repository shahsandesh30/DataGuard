"""Central configuration, loaded from environment variables (see .env.example)."""

from __future__ import annotations

import os
from dataclasses import dataclass

from dotenv import load_dotenv

load_dotenv()

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
DEFAULT_locationidS = [1544061, 1601414, 2455394, 6430870, 2178]

# Fusion — trust scoring (Layer 1 × Layer 2).
SEVERITY_PENALTY = {"low": 0.2, "medium": 0.4, "high": 0.7}
FUSION_STATUS_ESCALATED = "escalated"
FUSION_STATUS_QUARANTINED = "quarantined"

@dataclass(frozen=True)
class Settings:
    region: str # Sydney, Melbourne, etc.
    region_code: str  # Country code for the region (e.g., "AU" for Australia)
    region_bbox: str # Bounding box for the region (min_lon, min_lat, max_lon, max_lat)
    s3_bronze_bucket: str
    s3_silver_bucket: str
    s3_gold_bucket: str
    athena_output: str
    aws_region: str
    openaq_api_key: str
    openaq_archive_bucket: str
    openaq_archive_region: str
    glue_database: str

def get_settings() -> Settings:
    """
    Builds a Settings object for pipeline run.
    """

    return Settings(
        region=os.getenv("DATAGUARD_CITY", "Sydney").lower(),
        region_code=os.getenv("DATAGUARD_COUNTRY_ISO", "AU").upper(),
        region_bbox=os.getenv(
            "DATAGUARD_REGION_BBOX", "150.5209,-34.1183,151.3430,-33.5781"
        ),
        s3_bronze_bucket=os.getenv("BRONZE_BUCKET", "dataguard-bronze"),
        s3_silver_bucket=os.getenv("SILVER_BUCKET", "dataguard-silver"),
        s3_gold_bucket=os.getenv("GOLD_BUCKET", "dataguard-gold"),
        aws_region=os.getenv("AWS_REGION", "ap-southeast-2"),
        athena_output=os.getenv("ATHENA_OUTPUT"),
        openaq_api_key=os.getenv("OPENAQ_API_KEY"),
        openaq_archive_bucket=os.getenv("OPENAQ_ARCHIVE_BUCKET", "openaq-data-archive"),
        openaq_archive_region=os.getenv("OPENAQ_ARCHIVE_REGION", "us-east-1"),
        glue_database=os.getenv("GLUE_DATABASE", "dataguard_db")
    )
