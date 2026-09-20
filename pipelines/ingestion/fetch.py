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

import csv
import gzip
import json
import logging
import re
import shutil
import tempfile
from collections import defaultdict
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
ARCHIVE_MANIFEST_FILENAME = "_archive_manifest.jsonl"
LIVE_MANIFEST_FILENAME = "_live_manifest.jsonl"

# (bucket, key, local staging path) -> bytes written. Injected in tests.
Downloader = Callable[[str, str, Path], int]

ARCHIVE_PREFIX = "archive"
LIVE_PREFIX = "live"

LIVE_COLUMNS = [
    "location_id", "sensors_id", "location", "datetime",
    "lat", "lon", "parameter", "units", "value",
]


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

@dataclass
class FetchLiveResult:
    """Outcome of one live day-file (or a per-location failure/empty response)."""
    locationid: int
    status: str  # "created" | "updated" | "unchanged" | "empty" | "error"
    day: date | None = None
    rows: int = 0  # rows received from the API for this day
    new_rows: int = 0  # rows that were new or changed vs. the existing file
    stale_skipped: int = 0  # sensors dropped by the optional max_age filter
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
    return storage.join(bronze_root, f"{ARCHIVE_PREFIX}/{bronze_key(locationid, day)}")

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


def _append_archive_manifest(bronze_root: str | Path, result: FetchResult) -> None:
    """Record one arrival. Layer 1 measures delivery freshness against this."""
    payload = asdict(result)
    payload["day"] = result.day.isoformat()
    storage.append_text(
        storage.join(bronze_root, ARCHIVE_MANIFEST_FILENAME),
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
            _append_archive_manifest(root, result)
            return result
        except Exception as exc:  # noqa: BLE001 — record and continue the range
            logger.warning("Failed to fetch %s: %s", key, exc)
            result = FetchResult(
                locationid, day, key, "error", arrived_at=arrived_at, error=str(exc)
            )
            _append_archive_manifest(root, result)
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
    _append_archive_manifest(root, result)
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

def live_path(bronze_root: str | Path, locationid: int, day: date) -> str:
    """Full location of one live day-file: archive's layout, under ``live/``."""
    return storage.join(bronze_root, f"{LIVE_PREFIX}/{bronze_key(locationid, day)}")
 
 
def _field(obj, *names):
    """Read a field from a dict or an SDK object, trying camelCase/snake_case names."""
    for name in names:
        value = obj.get(name) if isinstance(obj, dict) else getattr(obj, name, None)
        if value is not None:
            return value
    return None
 
 
def _sensor_meta(location) -> dict[int, tuple[str, str]]:
    """sensor id -> (parameter name, units), from the location record."""
    meta = {}
    for sensor in _field(location, "sensors") or []:
        parameter = _field(sensor, "parameter")
        meta[_field(sensor, "id")] = (
            _field(parameter, "name") or "",
            _field(parameter, "units") or "",
        )
    return meta
 
 
def _live_rows(
    locationid: int, location, latest, *, cutoff: datetime | None
) -> tuple[list[dict], list[tuple[int, str]]]:
    """Reshape live 'latest' results into archive-format rows.
 
    Returns (rows, stale). With a ``cutoff``, sensors whose latest reading is older
    are returned in ``stale`` as (sensor id, local datetime) instead of as rows.
    """
    name = _field(location, "name") or ""
    meta = _sensor_meta(location)
    rows: list[dict] = []
    stale: list[tuple[int, str]] = []
    for item in latest:
        local_dt = _field(_field(item, "datetime"), "local")
        if local_dt is None:
            continue
        sensor_id = _field(item, "sensors_id", "sensorsId")
        if cutoff is not None and datetime.fromisoformat(local_dt) < cutoff:
            stale.append((sensor_id, local_dt))
            continue
        coords = _field(item, "coordinates")
        parameter, units = meta.get(sensor_id, ("", ""))
        rows.append(
            {
                "location_id": locationid,
                "sensors_id": sensor_id,
                "location": name,
                "datetime": local_dt,  # local time with offset, like the archive
                "lat": _field(coords, "latitude"),
                "lon": _field(coords, "longitude"),
                "parameter": parameter,
                "units": units,
                "value": _field(item, "value"),
            }
        )
    return rows, stale
 
 
def _norm(row: dict) -> dict[str, str]:
    """Everything as text, so API rows compare equal to rows read back from CSV."""
    return {col: "" if row.get(col) is None else str(row[col]) for col in LIVE_COLUMNS}
 
 
def _upsert_live_day(destination: str, new_rows: list[dict]) -> tuple[str, int, int | None]:
    """Merge rows into one live day-file: read, dedupe on (sensor, datetime), rewrite.
 
    Returns (status, rows new or changed, bytes written). If nothing changed the
    file is not re-uploaded. A repeated reading is dropped; a revised value for an
    existing (sensor, datetime) replaces the old one.
    """
    with tempfile.TemporaryDirectory() as staging_dir:
        existing_file = Path(staging_dir) / "existing.csv.gz"
        merged: dict[tuple[str, str], dict[str, str]] = {}
        existed = storage.exists(destination)
        if existed:
            shutil.copyfile(destination, existing_file)
            with gzip.open(existing_file, "rt", newline="", encoding="utf-8") as fh:
                for row in csv.DictReader(fh):
                    merged[(row["sensors_id"], row["datetime"])] = _norm(row)
 
        changed = 0
        for row in map(_norm, new_rows):
            key = (row["sensors_id"], row["datetime"])
            if merged.get(key) != row:
                merged[key] = row
                changed += 1
        if existed and not changed:
            return "unchanged", 0, None
 
        out_file = Path(staging_dir) / Path(destination).name
        ordered = sorted(
            merged.values(),
            key=lambda r: (datetime.fromisoformat(r["datetime"]), r["sensors_id"]),
        )
        with gzip.open(out_file, "wt", newline="", encoding="utf-8") as fh:
            writer = csv.DictWriter(fh, fieldnames=LIVE_COLUMNS)
            writer.writeheader()
            writer.writerows(ordered)
        size = storage.put_file(out_file, destination)
    return ("updated" if existed else "created"), changed, size
 
 
def _append_live_manifest(bronze_root: str | Path, result: FetchLiveResult) -> None:
    payload = asdict(result)
    payload["day"] = result.day.isoformat() if result.day else None
    storage.append_text(
        storage.join(bronze_root, LIVE_MANIFEST_FILENAME),
        json.dumps(payload, ensure_ascii=True) + "\n",
    )
 
 
def _fetch_live_location(
    openaq_client,
    locationid: int,
    root: str | Path,
    now: datetime,
    max_age: timedelta | None,
) -> list[FetchLiveResult]:
    arrived_at = now.isoformat()
    location = openaq_client.locations.get(locations_id=locationid).results[0]
    latest = openaq_client.locations.latest(locations_id=locationid).results
    cutoff = now - max_age if max_age is not None else None
    rows, stale = _live_rows(locationid, location, latest, cutoff=cutoff)
    if stale:
        logger.info(
            "Location %s: skipping %d sensor(s) older than %s: %s",
            locationid, len(stale), max_age, stale,
        )
    if not rows:
        return [
            FetchLiveResult(locationid, "empty", arrived_at=arrived_at, stale_skipped=len(stale))
        ]
 
    # Sensors report independently, so one response can span several days:
    # each row goes to the day-file of its own local date.
    by_day: dict[str, list[dict]] = defaultdict(list)
    for row in rows:
        by_day[row["datetime"][:10]].append(row)
 
    results = []
    for day_str, day_rows in sorted(by_day.items()):
        day = date.fromisoformat(day_str)
        destination = live_path(root, locationid, day)
        status, changed, size = _upsert_live_day(destination, day_rows)
        results.append(
            FetchLiveResult(
                locationid, status, day=day, rows=len(day_rows), new_rows=changed,
                stale_skipped=len(stale), local_path=destination, bytes=size,
                arrived_at=arrived_at,
            )
        )
    return results
 
 
def fetch_live_api(
    openaq_client,
    locationids: list[int],
    *,
    bronze_root: str | Path | None = None,
    max_age: timedelta | None = None,
) -> list[FetchLiveResult]:
    """Append the latest reading of every sensor at each location into bronze/live.
 
    Every row goes into the day-file of its own observation date, merged with what
    is already there and deduplicated on (sensors_id, datetime). Re-polling the
    same data (including dead sensors that never change) is therefore harmless and
    rewrites nothing. Pass ``max_age`` to skip sensors whose latest reading is
    older, which also avoids re-reading their old day-files on every poll.
 
    Not safe to run concurrently against the same bronze root: the merge is
    read-modify-write.
    """
    settings = load_settings()
    root = bronze_root or settings.bronze_root
    now = datetime.now(UTC)
    results: list[FetchLiveResult] = []
    for locationid in locationids:
        try:
            outcomes = _fetch_live_location(openaq_client, locationid, root, now, max_age)
        except Exception as exc:  # noqa: BLE001 — record and continue with next location
            logger.warning("Failed to fetch live data for location %s: %s", locationid, exc)
            outcomes = [
                FetchLiveResult(locationid, "error", arrived_at=now.isoformat(), error=str(exc))
            ]
        for outcome in outcomes:
            if outcome.status != "unchanged":  # unchanged is not an arrival
                _append_live_manifest(root, outcome)
        results.extend(outcomes)
    return results