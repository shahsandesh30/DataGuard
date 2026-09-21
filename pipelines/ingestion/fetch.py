"""Fetch daily OpenAQ archive files for a set of locations into the bronze zone.

OpenAQ S3 archive keys use the full path::

    records/csv.gz/locationid=<ID>/year=<YYYY>/month=<MM>/location-<ID>-<YYYYMMDD>.csv.gz

Bronze uses a simplified Hive layout (no ``records/csv.gz`` or ``month=``)::

    locationid=<ID>/year=<YYYY>/location-<ID>-<YYYYMMDD>.csv.gz

The bronze root may be a local directory or an ``s3://`` prefix. Archive objects
are always downloaded to a local staging file first — the OpenAQ bucket is read
with unsigned requests, and the destination may need different credentials — and
then handed to :mod:`pipelines.storage` to land in the zone.
"""

from __future__ import annotations

import json
import logging
import re
import tempfile
from collections.abc import Callable, Iterator
from dataclasses import asdict, dataclass
from datetime import UTC, date, datetime, timedelta
from pathlib import Path

import boto3
from botocore import UNSIGNED
from botocore.config import Config
from botocore.exceptions import ClientError

from pipelines import storage
from pipelines.config import OPENAQ_ARCHIVE_BUCKET, OPENAQ_ARCHIVE_REGION, load_settings

logger = logging.getLogger(__name__)

FILENAME_RE = re.compile(r"location-(\d+)-(\d{8})\.csv\.gz$")
MANIFEST_FILENAME = "_manifest.jsonl"

# (bucket, key, local staging path) -> bytes written. Injected in tests.
Downloader = Callable[[str, str, Path], int]


@dataclass
class FetchResult:
    locationid: int
    day: date
    archive_key: str
    status: str
    local_path: str | None = None
    bytes: int | None = None
    arrived_at: str | None = None
    error: str | None = None


def archive_key(locationid: int, day: date) -> str:
    """Return the OpenAQ S3 archive object key for one location-day."""
    return (
        f"records/csv.gz/locationid={locationid}"
        f"/year={day.year}/month={day.month:02d}"
        f"/location-{locationid}-{day.strftime('%Y%m%d')}.csv.gz"
    )


def bronze_key(locationid: int, day: date) -> str:
    """Return the bronze-relative path for one location-day."""
    return (
        f"locationid={locationid}/year={day.year}"
        f"/location-{locationid}-{day.strftime('%Y%m%d')}.csv.gz"
    )


def bronze_path(bronze_root: str | Path, locationid: int, day: date) -> str:
    """Full location of one bronze file: a local path or an ``s3://`` URI."""
    return storage.join(bronze_root, bronze_key(locationid, day))


def parse_bronze_filename(name: str) -> tuple[int, date] | None:
    match = FILENAME_RE.search(name)
    if not match:
        return None
    return int(match.group(1)), datetime.strptime(match.group(2), "%Y%m%d").replace(
        tzinfo=UTC
    ).date()


def unsigned_s3_client(region: str = OPENAQ_ARCHIVE_REGION):
    """Client for the public OpenAQ bucket — no credentials, no signing."""
    return boto3.client("s3", region_name=region, config=Config(signature_version=UNSIGNED))


def _utc_now() -> str:
    return datetime.now(UTC).isoformat()


def download_archive_object(bucket: str, key: str, dest: Path, client=None) -> int:
    """Download one public archive object. Raises FileNotFoundError if missing."""
    client = client or unsigned_s3_client()
    dest.parent.mkdir(parents=True, exist_ok=True)
    try:
        client.download_file(bucket, key, str(dest))
    except ClientError as exc:
        dest.unlink(missing_ok=True)
        code = str(exc.response.get("Error", {}).get("Code", ""))
        if code in {"404", "NoSuchKey", "404 Not Found"}:
            raise FileNotFoundError(key) from exc
        raise
    return dest.stat().st_size


def _append_manifest(bronze_root: str | Path, result: FetchResult) -> None:
    """Record one arrival. Layer 1 measures delivery freshness against this."""
    payload = asdict(result)
    payload["day"] = result.day.isoformat()
    storage.append_text(
        storage.join(bronze_root, MANIFEST_FILENAME),
        json.dumps(payload, ensure_ascii=True) + "\n",
    )


def fetch_location_day(
    locationid: int,
    day: date,
    *,
    bronze_root: str | Path | None = None,
    bucket: str = OPENAQ_ARCHIVE_BUCKET,
    downloader: Downloader | None = None,
    force: bool = False,
) -> FetchResult:
    """Copy one location-day file from the OpenAQ archive into bronze."""
    settings = load_settings()
    root = bronze_root or settings.bronze_root
    key = archive_key(locationid, day)
    destination = bronze_path(root, locationid, day)

    existing = None if force else storage.file_info(destination)
    if existing is not None:
        size, modified = existing
        return FetchResult(
            locationid=locationid,
            day=day,
            archive_key=key,
            status="skipped",
            local_path=destination,
            bytes=size,
            arrived_at=modified,
        )

    arrived_at = _utc_now()
    get_object = downloader or download_archive_object
    with tempfile.TemporaryDirectory() as staging_dir:
        staging = Path(staging_dir) / f"location-{locationid}-{day:%Y%m%d}.csv.gz"
        try:
            get_object(bucket, key, staging)
        except FileNotFoundError:
            logger.info("Missing archive object (completeness gap): %s", key)
            result = FetchResult(locationid, day, key, "missing", arrived_at=arrived_at)
            _append_manifest(root, result)
            return result
        except Exception as exc:  # noqa: BLE001 — record and continue the range
            logger.warning("Failed to fetch %s: %s", key, exc)
            result = FetchResult(
                locationid, day, key, "error", arrived_at=arrived_at, error=str(exc)
            )
            _append_manifest(root, result)
            return result

        size = storage.put_file(staging, destination)

    result = FetchResult(
        locationid=locationid,
        day=day,
        archive_key=key,
        status="copied",
        local_path=destination,
        bytes=size,
        arrived_at=arrived_at,
    )
    _append_manifest(root, result)
    logger.info("Copied %s (%s bytes)", key, size)
    return result


def iter_days(start: date, end: date) -> Iterator[date]:
    if end < start:
        raise ValueError("end date must be on or after start date")
    current = start
    while current <= end:
        yield current
        current += timedelta(days=1)


def fetch_range(
    locationids: list[int],
    start: date,
    end: date,
    *,
    bronze_root: str | Path | None = None,
    force: bool = False,
    downloader: Downloader | None = None,
) -> list[FetchResult]:
    """Fetch every location-day in [start, end] for the given locations."""
    settings = load_settings()
    root = bronze_root or settings.bronze_root
    return [
        fetch_location_day(
            locationid, day, bronze_root=root, force=force, downloader=downloader
        )
        for locationid in locationids
        for day in iter_days(start, end)
    ]
