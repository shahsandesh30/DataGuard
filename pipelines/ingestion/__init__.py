"""Ingestion: copy raw OpenAQ archive files into the bronze zone, unchanged."""

from pipelines.ingestion.fetch import (
    archive_key,
    adopt_flat_bronze,
    bronze_key,
    bronze_path,
    fetch_location_day,
    fetch_range,
)

__all__ = [
    "archive_key",
    "bronze_key",
    "bronze_path",
    "adopt_flat_bronze",
    "fetch_location_day",
    "fetch_range",
]
