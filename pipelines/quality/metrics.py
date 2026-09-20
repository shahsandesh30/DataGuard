"""Layer 1 quality metrics, at two grains.

``compute_sensor_day_metrics``  one row per (location, sensor, parameter, day)
``compute_station_day_metrics`` one row per (location, day)

The station-day table is what the rules and the Isolation Forest both read, so
every column here is either a rule input, a model feature, or both.

Most metrics come from the readings themselves. Three do not — whether the file
arrived, when it arrived, and whether its columns changed — because those are
properties of the delivered file, not of the data inside it. They are read from
bronze and the arrival manifest.
"""

from __future__ import annotations

import json
import statistics
from datetime import UTC, date, datetime, timedelta
from pathlib import Path

import pandas as pd

from pipelines import storage
from pipelines.config import (
    DEFAULT_HOURLY_READINGS,
    DELIVERY_COMMITMENT_HOURS,
    TRAILING_CADENCE_DAYS,
    VARIANCE_EPS,
    load_settings,
)
from pipelines.conformance.units import CANONICAL_UNITS, canonical_parameter, normalize_unit
from pipelines.ingestion.fetch import ARCHIVE_MANIFEST_FILENAME, bronze_key, parse_bronze_filename

SENSOR_DAY_COLUMNS = [
    "locationid",
    "sensor_id",
    "parameter",
    "date_local",
    "readings_received",
    "readings_expected",
    "missing_rate",
    "null_value_count",
    "negative_count",
    "duplicate_count",
    "max_stuck_run",
    "value_variance",
    "unit_mismatch",
]

STATION_DAY_COLUMNS = [
    "locationid",
    "date_local",
    "total_readings",
    "missing_rate_mean",
    "sensors_expected",
    "sensors_received",
    "sensor_dropout_count",
    "negative_count_total",
    "max_stuck_run_max",
    "zero_variance_params",
    "duplicate_rate",
    "schema_changed",
    "file_present",
    "file_lateness_hours",
    "unit_mismatch_count",
    "cross_sensor_pm25_spread",
]

# The numeric subset the Isolation Forest trains on: booleans and keys dropped.
METRIC_COLUMNS = [
    "total_readings",
    "missing_rate_mean",
    "sensors_expected",
    "sensors_received",
    "sensor_dropout_count",
    "negative_count_total",
    "max_stuck_run_max",
    "zero_variance_params",
    "duplicate_rate",
    "file_lateness_hours",
    "unit_mismatch_count",
    "cross_sensor_pm25_spread",
]

SENSOR_DAY_KEY = ["locationid", "sensor_id", "parameter", "date_local"]


# --------------------------------------------------------------------------- #
# Sensor-day metrics
# --------------------------------------------------------------------------- #


def max_stuck_run(values: pd.Series) -> int:
    """Longest run of identical consecutive values (ordered by the caller).

    A working sensor's readings jitter. A run of identical values is the
    signature of a frozen sensor still reporting its last good number.
    """
    longest = current = 0
    prev = None
    for value in values:
        if pd.isna(value):
            prev, current = None, 0
            continue
        current = current + 1 if value == prev else 1
        longest = max(longest, current)
        prev = value
    return longest


def _unit_is_mismatch(parameter: str, original_unit: str) -> bool:
    canonical = CANONICAL_UNITS.get(canonical_parameter(parameter))
    if canonical is None:
        return False
    return normalize_unit(original_unit) != normalize_unit(canonical)


def _expected_readings(conformed: pd.DataFrame) -> dict[tuple, int]:
    """How many readings each sensor-day *should* have delivered.

    Learned from the sensor's own trailing cadence rather than assumed: a
    station that reports four times a day is not 83% incomplete. Falls back to
    hourly until there is history to learn from.
    """
    counts = conformed.groupby(SENSOR_DAY_KEY, sort=False, dropna=False).size()
    expected: dict[tuple, int] = {}

    for sensor_key, group in counts.groupby(level=[0, 1, 2], sort=False):
        history = sorted(
            (date.fromisoformat(str(key[3])), int(count)) for key, count in group.items()
        )
        for position, (day, _) in enumerate(history):
            window = [
                count
                for earlier, count in history[:position]
                if (day - earlier).days <= TRAILING_CADENCE_DAYS
            ]
            expected[(*sensor_key, day.isoformat())] = (
                max(1, round(statistics.median(window))) if window else DEFAULT_HOURLY_READINGS
            )
    return expected


def compute_sensor_day_metrics(conformed: pd.DataFrame) -> pd.DataFrame:
    """Return one metric row per (locationid, sensor_id, parameter, date_local)."""
    if conformed is None or conformed.empty:
        return pd.DataFrame(columns=SENSOR_DAY_COLUMNS)

    expected_by_key = _expected_readings(conformed)
    mismatched = {
        (str(parameter), str(unit))
        for parameter, unit in conformed[["parameter", "original_unit"]]
        .drop_duplicates()
        .itertuples(index=False)
        if _unit_is_mismatch(parameter, unit)
    }

    rows: list[dict] = []
    for keys, part in conformed.groupby(SENSOR_DAY_KEY, sort=False, dropna=False):
        locationid, sensor_id, parameter, date_local = keys
        ordered = part.sort_values("datetime")
        values = ordered["value"]
        received = len(ordered)
        expected = expected_by_key.get(keys, DEFAULT_HOURLY_READINGS)

        rows.append(
            {
                "locationid": int(locationid),
                "sensor_id": int(sensor_id),
                "parameter": str(parameter),
                "date_local": str(date_local),
                "readings_received": received,
                "readings_expected": expected,
                "missing_rate": min(1.0, max(0.0, 1.0 - received / expected)),
                "null_value_count": int(values.isna().sum()),
                "negative_count": int((values < 0).sum()),
                "duplicate_count": int(
                    ordered.duplicated(subset=["sensor_id", "datetime", "parameter"]).sum()
                ),
                "max_stuck_run": max_stuck_run(values),
                "value_variance": float(values.var()) if received > 1 else 0.0,
                "unit_mismatch": int(
                    sum((str(parameter), str(u)) in mismatched for u in ordered["original_unit"])
                ),
            }
        )
    return pd.DataFrame(rows, columns=SENSOR_DAY_COLUMNS)


# --------------------------------------------------------------------------- #
# File-level signals, read from bronze rather than from the readings
# --------------------------------------------------------------------------- #

MANIFEST_COLUMNS = [
    "locationid",
    "day",
    "archive_key",
    "status",
    "local_path",
    "bytes",
    "arrived_at",
    "error",
]


def load_bronze_manifest(bronze_root: str | Path) -> pd.DataFrame:
    """Load the bronze arrival log, one JSON line per ingest attempt."""
    text = storage.read_text(storage.join(bronze_root, ARCHIVE_MANIFEST_FILENAME))
    rows = [json.loads(line) for line in (text or "").splitlines() if line.strip()]
    if not rows:
        return pd.DataFrame(columns=MANIFEST_COLUMNS)
    frame = pd.DataFrame(rows)
    if "locationid" in frame.columns:
        frame["locationid"] = pd.to_numeric(frame["locationid"], errors="coerce").astype("Int64")
    return frame


def read_bronze_schema(uri: str | Path) -> set[str]:
    """Bronze column names from the header row alone — no rows are read."""
    return {str(c).strip().lower() for c in storage.read_csv(uri, nrows=0).columns}


def _schema_drift(bronze_root: str | Path) -> dict[tuple[int, str], bool]:
    """``{(locationid, date_local): columns differ from the previous day}``.

    Only archive-layout filenames carry a location and a day, so only those can
    be compared day over day.
    """
    by_location: dict[int, list[tuple[date, set[str]]]] = {}
    for uri in storage.list_files(bronze_root, (".csv.gz",)):
        parsed = parse_bronze_filename(uri.replace("\\", "/").rsplit("/", 1)[-1])
        if parsed is None:
            continue
        locationid, day = parsed
        by_location.setdefault(locationid, []).append((day, read_bronze_schema(uri)))

    flags: dict[tuple[int, str], bool] = {}
    for locationid, entries in by_location.items():
        previous: set[str] | None = None
        for day, schema in sorted(entries, key=lambda item: item[0]):
            flags[(locationid, day.isoformat())] = previous is not None and schema != previous
            previous = schema
    return flags


def _file_lateness_hours(locationid: int, date_local: str, manifest: pd.DataFrame) -> float:
    """Hours past OpenAQ's 72h publication commitment for one location-day.

    Measured from when the file reached bronze, so this only means what it says
    for a pipeline running near the present — on a backfill every file looks
    months late. No rule reads it for that reason; it is a model feature only.
    """
    if manifest.empty:
        return 0.0
    subset = manifest[(manifest["locationid"] == locationid) & (manifest["day"] == date_local)]
    if subset.empty:
        return 0.0

    row = subset.iloc[-1]
    if row.get("status") == "missing" or not row.get("arrived_at"):
        return float(DELIVERY_COMMITMENT_HOURS)
    arrived = pd.to_datetime(row["arrived_at"], utc=True, errors="coerce")
    if pd.isna(arrived):
        return 0.0

    day = date.fromisoformat(date_local)
    deadline = datetime.combine(
        day + timedelta(days=1), datetime.min.time(), tzinfo=UTC
    ) + timedelta(hours=DELIVERY_COMMITMENT_HOURS)
    return max(0.0, (arrived.to_pydatetime() - deadline).total_seconds() / 3600.0)


def _pm25_spread(conformed: pd.DataFrame) -> dict[tuple[int, str], float]:
    """Widest same-hour disagreement between co-located PM2.5 sensors, per day.

    Two sensors at one station in the same hour should roughly agree. A large
    spread means at least one of them is wrong.
    """
    pm25 = conformed[conformed["parameter"] == "pm25"]
    if pm25.empty:
        return {}

    hourly = pm25.assign(hour=pd.to_datetime(pm25["datetime"], utc=True).dt.floor("h"))
    per_hour = hourly.groupby(["locationid", "date_local", "hour"], sort=False).agg(
        sensors=("sensor_id", "nunique"),
        spread=("value", lambda values: values.max() - values.min()),
    )
    multi_sensor = per_hour[per_hour["sensors"] > 1]
    if multi_sensor.empty:
        return {}

    widest = multi_sensor.groupby(level=[0, 1], sort=False)["spread"].max()
    return {(int(loc), str(day)): float(value) for (loc, day), value in widest.items()}


# --------------------------------------------------------------------------- #
# Station-day metrics
# --------------------------------------------------------------------------- #


def compute_station_day_metrics(
    conformed: pd.DataFrame,
    bronze_root: str | Path | None = None,
    sensor_metrics: pd.DataFrame | None = None,
) -> pd.DataFrame:
    """Return one metric row per (locationid, date_local)."""
    if conformed is None or conformed.empty:
        return pd.DataFrame(columns=STATION_DAY_COLUMNS)

    bronze = bronze_root or load_settings().bronze_root
    detail = sensor_metrics if sensor_metrics is not None else compute_sensor_day_metrics(conformed)
    manifest = load_bronze_manifest(bronze)
    schema_flags = _schema_drift(bronze)
    spreads = _pm25_spread(conformed)

    sensors_by_day = {
        (int(locationid), str(day)): {int(s) for s in sensors.dropna().unique()}
        for (locationid, day), sensors in conformed.groupby(
            ["locationid", "date_local"], sort=False
        )["sensor_id"]
    }

    rows: list[dict] = []
    previous_sensors: dict[int, set[int]] = {}
    station_days = (
        conformed[["locationid", "date_local"]]
        .drop_duplicates()
        .sort_values(["locationid", "date_local"])
    )

    for _, station in station_days.iterrows():
        locationid = int(station["locationid"])
        date_local = str(station["date_local"])
        key = (locationid, date_local)

        # Empty only if a caller passed in sensor_metrics computed from a
        # different frame; every aggregate below has to survive it.
        day_detail = detail[
            (detail["locationid"] == locationid) & (detail["date_local"] == date_local)
        ]
        total_readings = int(day_detail["readings_received"].sum())
        duplicates = int(day_detail["duplicate_count"].sum())
        missing_rate = float(day_detail["missing_rate"].mean()) if not day_detail.empty else 0.0
        stuck_run = int(day_detail["max_stuck_run"].max()) if not day_detail.empty else 0

        # A sensor that reported yesterday and not today has dropped out. Read
        # before the update, or every station looks like it lost nothing.
        sensors_today = sensors_by_day.get(key, set())
        sensors_yesterday = previous_sensors.get(locationid, set())
        previous_sensors[locationid] = sensors_today

        rows.append(
            {
                "locationid": locationid,
                "date_local": date_local,
                "total_readings": total_readings,
                "missing_rate_mean": missing_rate,
                "sensors_expected": len(sensors_yesterday | sensors_today),
                "sensors_received": len(sensors_today),
                "sensor_dropout_count": len(sensors_yesterday - sensors_today),
                "negative_count_total": int(day_detail["negative_count"].sum()),
                "max_stuck_run_max": stuck_run,
                "zero_variance_params": int((day_detail["value_variance"] <= VARIANCE_EPS).sum()),
                "duplicate_rate": duplicates / total_readings if total_readings else 0.0,
                "schema_changed": bool(schema_flags.get(key, False)),
                "file_present": storage.exists(
                    storage.join(bronze, bronze_key(locationid, date.fromisoformat(date_local)))
                ),
                "file_lateness_hours": _file_lateness_hours(locationid, date_local, manifest),
                "unit_mismatch_count": int(day_detail["unit_mismatch"].sum()),
                "cross_sensor_pm25_spread": spreads.get(key, 0.0),
            }
        )

    return pd.DataFrame(rows, columns=STATION_DAY_COLUMNS)
