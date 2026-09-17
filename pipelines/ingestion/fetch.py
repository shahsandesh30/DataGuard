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

from pipelines.config import OPENAQ_ARCHIVE_BUCKET, OPENAQ_ARCHIVE_REGION, get_settings

logger = logging.getLogger(__name__)

FILENAME_RE = re.compile(r"location-(\d+)-(\d{8})\.csv\.gz$")
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
    """Return the relative local bronze path for one location-day."""
    return (
        f"locationid={locationid}/year={day.year}"
        f"/location-{locationid}-{day.strftime('%Y%m%d')}.csv.gz"
    )


def bronze_path(bronze_root: Path, locationid: int, day: date) -> Path:
    """Local bronze path under ``bronze_root``."""
    return bronze_root / bronze_key(locationid, day)


def parse_bronze_filename(name: str) -> tuple[int, date] | None:
    match = FILENAME_RE.search(name)
    if not match:
        return None
    locationid = int(match.group(1))
    day = datetime.strptime(match.group(2), "%Y%m%d").date()
    return locationid, day


def unsigned_s3_client(region: str = OPENAQ_ARCHIVE_REGION):
    return boto3.client(
        "s3",
        region_name=region,
        config=Config(signature_version=UNSIGNED),
    )


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()

# def fetch_locations_for_region(settings: Settings) -> list[dict]:
#     """Returns every monitoring location OpenAQ has within settings.bbox."""
#     headers = _headers(settings)
#     locations: list[dict] = []
#     page = 1
#     while True:
#         payload = _get_with_retry(
#             f"{OPENAQ_BASE_URL}/locations",
#             headers=headers,
#             params={"bbox": settings.bbox, "limit": LOCATIONS_PAGE_LIMIT, "page": page},
#         )
#         results = payload.get("results", [])
#         locations.extend(results)
#         found = payload.get("meta", {}).get("found")
#         if not results or (isinstance(found, int) and len(locations) >= found):
#             break
#         page += 1
#     return locations

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
    locationid: int,
    day: date,
    *,
    bronze_root: Path | None = None,
    bucket: str = OPENAQ_ARCHIVE_BUCKET,
    downloader: Downloader | None = None,
    force: bool = False,
) -> FetchResult:
    """Copy one location-day file from the OpenAQ archive to local bronze."""
    settings = get_settings()
    root = Path(bronze_root or settings.bronze_root)
    key = archive_key(locationid, day)
    dest = bronze_path(root, locationid, day)
    arrived_at = _utc_now()

    if dest.exists() and not force:
        return FetchResult(
            locationid=locationid,
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
            locationid=locationid,
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
            locationid=locationid,
            day=day,
            archive_key=key,
            status="error",
            arrived_at=arrived_at,
            error=str(exc),
        )
        _append_manifest(root, result)
        return result

    result = FetchResult(
        locationid=locationid,
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
    locationids: list[int],
    start: date,
    end: date,
    *,
    bronze_root: Path | None = None,
    force: bool = False,
    downloader: Downloader | None = None,
) -> list[FetchResult]:
    """Fetch every location-day in [start, end] for the given locations."""
    settings = get_settings()
    root = Path(bronze_root or settings.bronze_root)
    results: list[FetchResult] = []
    for locationid in locationids:
        for day in iter_days(start, end):
            results.append(
                fetch_location_day(
                    locationid,
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
    settings = get_settings()
    root = Path(bronze_root or settings.bronze_root)
    if not root.exists():
        return []

    results: list[FetchResult] = []

    legacy = sorted(root.glob("records/csv.gz/locationid=*/year=*/month=*/*.csv.gz"))
    for path in legacy:
        parsed = parse_bronze_filename(path.name)
        if parsed is None:
            continue
        locationid, day = parsed
        dest = bronze_path(root, locationid, day)
        dest.parent.mkdir(parents=True, exist_ok=True)
        if dest.exists():
            if dest.resolve() != path.resolve():
                path.unlink()
            continue
        shutil.move(str(path), str(dest))
        results.append(
            FetchResult(
                locationid=locationid,
                day=day,
                archive_key=archive_key(locationid, day),
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
        locationid, day = parsed
        dest = bronze_path(root, locationid, day)
        dest.parent.mkdir(parents=True, exist_ok=True)
        if dest.exists():
            if dest.resolve() != path.resolve():
                path.unlink()
            continue
        shutil.move(str(path), str(dest))
        result = FetchResult(
            locationid=locationid,
            day=day,
            archive_key=archive_key(locationid, day),
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

from pipelines.config import Settings, get_settings

@dataclass
class IngestionResult:
    has_new_data: bool
    output_path: Optional[str] = None
    record_count: int = 0
    location_count: int = 0
    run_timestamp: str = ""

def run(settings: Settings) -> IngestionResult:
    """Entry point called by __main__.py's `ingest` stage and by run-inference."""
    run_timestamp = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H-%M-%S")
    records, location_count = fetch_openaq(settings)
    if not records:
        return IngestionResult(has_new_data=False, location_count=location_count, run_timestamp=run_timestamp)
    output_path = write_bronze(settings, records, run_timestamp)
    return IngestionResult(
        has_new_data=True,
        output_path=output_path,
        record_count=len(records),
        location_count=location_count,
        run_timestamp=run_timestamp,
    )


# if not records:
#         return ""

#     bronze_client = _bronze_client(settings)

#     # Group records by location and reading date
#     grouped_records = {}

#     for record in records:
#         location_id = record["location_id"]
#         datetime_utc = record["datetime_utc"]

#         # Parse OpenAQ timestamp
#         if isinstance(datetime_utc, str):
#             reading_dt = datetime.fromisoformat(
#                 datetime_utc.replace("Z", "+00:00")
#             )
#         else:
#             reading_dt = datetime_utc

#         year = reading_dt.strftime("%Y")
#         date = reading_dt.strftime("%Y%m%d")

#         group_key = (location_id, year, date)

#         grouped_records.setdefault(group_key, []).append(record)

#     output_keys = []

#     for (location_id, year, date), location_records in grouped_records.items():

#         filename = f"location-{location_id}-{date}.parquet"

#         key = (
#             f"locationid={location_id}/"
#             f"year={year}/"
#             f"{filename}"
#         )

#         # Convert records to an Arrow table
#         table = pa.Table.from_pylist(location_records)

#         # Write Parquet to memory
#         buffer = io.BytesIO()

#         pq.write_table(
#             table,
#             buffer,
#             compression="snappy",
#         )

#         # Upload Parquet to S3
#         bronze_client.put_object(
#             Bucket=settings.s3_bronze_bucket,
#             Key=key,
#             Body=buffer.getvalue(),
#             ContentType="application/octet-stream",
#         )

#         output_keys.append(key)

#     # Keep compatibility with the existing IngestionResult
#     return output_keys[0]