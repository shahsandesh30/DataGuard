"""Central configuration, loaded from environment variables (see .env.example)."""

import os
from dataclasses import dataclass, field

# OpenAQ public archive on AWS Open Data. Public bucket, no credentials needed.
OPENAQ_ARCHIVE_BUCKET = "openaq-data-archive"
OPENAQ_ARCHIVE_REGION = "us-east-1"

# OpenAQ commits to writing files ~72 hours after end of day (location timezone).
# Lateness beyond this is a Layer 1 freshness anomaly, measured against a
# published promise rather than a threshold we invented.
DELIVERY_COMMITMENT_HOURS = 72


@dataclass(frozen=True)
class Settings:
    aws_region: str = field(default_factory=lambda: os.getenv("AWS_REGION", "us-east-1"))
    bronze_bucket: str = field(default_factory=lambda: os.getenv("BRONZE_BUCKET", "dataguard-bronze"))
    silver_bucket: str = field(default_factory=lambda: os.getenv("SILVER_BUCKET", "dataguard-silver"))
    gold_bucket: str = field(default_factory=lambda: os.getenv("GOLD_BUCKET", "dataguard-gold"))
    athena_workgroup: str = field(default_factory=lambda: os.getenv("ATHENA_WORKGROUP", "dataguard"))
    athena_output: str = field(default_factory=lambda: os.getenv("ATHENA_OUTPUT", ""))
    glue_database: str = field(default_factory=lambda: os.getenv("GLUE_DATABASE", "dataguard"))


def load_settings() -> Settings:
    return Settings()
