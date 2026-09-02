"""Fetch daily OpenAQ archive files for a set of locations into the bronze zone.

OpenAQ S3 archive keys use the full path:

    records/csv.gz/locationid=<ID>/year=<YYYY>/month=<MM>/location-<ID>-<YYYYMMDD>.csv.gz

Local bronze uses a simplified Hive layout (no ``records/csv.gz`` or ``month=``):

    locationid=<ID>/year=<YYYY>/location-<ID>-<YYYYMMDD>.csv.gz
"""

from __future__ import annotations

import json
import logging
import re
import shutil
from collections.abc import Callable, Iterator
from dataclasses import asdict, dataclass
from datetime import date, datetime, timedelta, timezone
from pathlib import Path

import boto3
from botocore import UNSIGNED
from botocore.config import Config
from botocore.exceptions import ClientError

from pipelines.config import OPENAQ_ARCHIVE_BUCKET, OPENAQ_ARCHIVE_REGION, load_settings

logger = logging.getLogger(__name__)

FILENAME_RE = re.compile(r"location-(\d+)-(\d{8})\.csv\.gz$")
Downloader = Callable[[str, str, Path], int]


@dataclass
class FetchResult:
    location_id: int
    day: date
    archive_key: str
    status: str
    local_path: str | None = None
    bytes: int | None = None
    arrived_at: str | None = None
    error: str | None = None


def archive_key(location_id: int, day: date) -> str:
    """Return the OpenAQ S3 archive object key for one location-day."""
    return (
        f"records/csv.gz/locationid={location_id}"
        f"/year={day.year}/month={day.month:02d}"
        f"/location-{location_id}-{day.strftime('%Y%m%d')}.csv.gz"
    )


def bronze_key(location_id: int, day: date) -> str:
    """Return the relative local bronze path for one location-day."""
    return (
        f"locationid={location_id}/year={day.year}"
        f"/location-{location_id}-{day.strftime('%Y%m%d')}.csv.gz"
    )


def bronze_path(bronze_root: Path, location_id: int, day: date) -> Path:
    """Local bronze path under ``bronze_root``."""
    return bronze_root / bronze_key(location_id, day)


def parse_bronze_filename(name: str) -> tuple[int, date] | None:
    match = FILENAME_RE.search(name)
    if not match:
        return None
    location_id = int(match.group(1))
    day = datetime.strptime(match.group(2), "%Y%m%d").date()
    return location_id, day


def unsigned_s3_client(region: str = OPENAQ_ARCHIVE_REGION):
    return boto3.client(
        "s3",
        region_name=region,
        config=Config(signature_version=UNSIGNED),
    )


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def download_archive_object(
    bucket: str,
    key: str,
    dest: Path,
    client=None,
) -> int:
    """Download one public archive object. Raises FileNotFoundError if missing."""
    client = client or unsigned_s3_client()
    dest.parent.mkdir(parents=True, exist_ok=True)
    tmp = dest.with_name(dest.name + ".tmp")
    try:
        client.download_file(bucket, key, str(tmp))
    except ClientError as exc:
        tmp.unlink(missing_ok=True)
        code = str(exc.response.get("Error", {}).get("Code", ""))
        if code in {"404", "NoSuchKey", "404 Not Found"}:
            raise FileNotFoundError(key) from exc
        raise
    tmp.replace(dest)
    return dest.stat().st_size


def _append_manifest(bronze_root: Path, result: FetchResult) -> None:
    bronze_root.mkdir(parents=True, exist_ok=True)
    payload = asdict(result)
    payload["day"] = result.day.isoformat()
    with (bronze_root / "_manifest.jsonl").open("a", encoding="utf-8") as handle:
        handle.write(json.dumps(payload, ensure_ascii=True) + "\n")


def fetch_location_day(
    location_id: int,
    day: date,
    *,
    bronze_root: Path | None = None,
    bucket: str = OPENAQ_ARCHIVE_BUCKET,
    downloader: Downloader | None = None,
    force: bool = False,
) -> FetchResult:
    """Copy one location-day file from the OpenAQ archive to local bronze."""
    settings = load_settings()
    root = Path(bronze_root or settings.bronze_root)
    key = archive_key(location_id, day)
    dest = bronze_path(root, location_id, day)
    arrived_at = _utc_now()

    if dest.exists() and not force:
        return FetchResult(
            location_id=location_id,
            day=day,
            archive_key=key,
            status="skipped",
            local_path=str(dest),
            bytes=dest.stat().st_size,
            arrived_at=datetime.fromtimestamp(
                dest.stat().st_mtime, tz=timezone.utc
            ).isoformat(),
        )

    get_object = downloader or (
        lambda bkt, obj_key, path: download_archive_object(bkt, obj_key, path)
    )
    try:
        size = get_object(bucket, key, dest)
    except FileNotFoundError:
        logger.info("Missing archive object (completeness gap): %s", key)
        result = FetchResult(
            location_id=location_id,
            day=day,
            archive_key=key,
            status="missing",
            arrived_at=arrived_at,
        )
        _append_manifest(root, result)
        return result
    except Exception as exc:  # noqa: BLE001 — record and continue the range
        logger.warning("Failed to fetch %s: %s", key, exc)
        result = FetchResult(
            location_id=location_id,
            day=day,
            archive_key=key,
            status="error",
            arrived_at=arrived_at,
            error=str(exc),
        )
        _append_manifest(root, result)
        return result

    result = FetchResult(
        location_id=location_id,
        day=day,
        archive_key=key,
        status="copied",
        local_path=str(dest),
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
    location_ids: list[int],
    start: date,
    end: date,
    *,
    bronze_root: Path | None = None,
    force: bool = False,
    downloader: Downloader | None = None,
) -> list[FetchResult]:
    """Fetch every location-day in [start, end] for the given locations."""
    settings = load_settings()
    root = Path(bronze_root or settings.bronze_root)
    results: list[FetchResult] = []
    for location_id in location_ids:
        for day in iter_days(start, end):
            results.append(
                fetch_location_day(
                    location_id,
                    day,
                    bronze_root=root,
                    force=force,
                    downloader=downloader,
                )
            )
    return results


def adopt_flat_bronze(bronze_root: Path | None = None) -> list[FetchResult]:
    """Move flat ``location-ID-YYYYMMDD.csv.gz`` files into bronze Hive layout.

    Also relocates files still under the legacy ``records/csv.gz/.../month=...``
    tree into ``locationid=<ID>/year=<YYYY>/``.
    """
    settings = load_settings()
    root = Path(bronze_root or settings.bronze_root)
    if not root.exists():
        return []

    results: list[FetchResult] = []

    legacy = sorted(root.glob("records/csv.gz/locationid=*/year=*/month=*/*.csv.gz"))
    for path in legacy:
        parsed = parse_bronze_filename(path.name)
        if parsed is None:
            continue
        location_id, day = parsed
        dest = bronze_path(root, location_id, day)
        dest.parent.mkdir(parents=True, exist_ok=True)
        if dest.exists():
            if dest.resolve() != path.resolve():
                path.unlink()
            continue
        shutil.move(str(path), str(dest))
        results.append(
            FetchResult(
                location_id=location_id,
                day=day,
                archive_key=archive_key(location_id, day),
                status="adopted",
                local_path=str(dest),
                bytes=dest.stat().st_size,
                arrived_at=datetime.fromtimestamp(
                    dest.stat().st_mtime, tz=timezone.utc
                ).isoformat(),
            )
        )
        logger.info("Adopted legacy %s -> %s", path, dest)

    for path in sorted(root.glob("location-*.csv.gz")):
        parsed = parse_bronze_filename(path.name)
        if parsed is None:
            continue
        location_id, day = parsed
        dest = bronze_path(root, location_id, day)
        dest.parent.mkdir(parents=True, exist_ok=True)
        if dest.exists():
            if dest.resolve() != path.resolve():
                path.unlink()
            continue
        shutil.move(str(path), str(dest))
        result = FetchResult(
            location_id=location_id,
            day=day,
            archive_key=archive_key(location_id, day),
            status="adopted",
            local_path=str(dest),
            bytes=dest.stat().st_size,
            arrived_at=datetime.fromtimestamp(
                dest.stat().st_mtime, tz=timezone.utc
            ).isoformat(),
        )
        _append_manifest(root, result)
        results.append(result)
        logger.info("Adopted %s -> %s", path.name, dest)
    return results
