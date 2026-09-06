"""Layer 1 quality metrics at sensor-day and station-day grains.

Each metric maps to a documented degradation mode (docs/data-source.md E2–E8).
"""

from __future__ import annotations

import json
from datetime import date, datetime, timedelta, timezone
from pathlib import Path

import pandas as pd

from pipelines.config import (
    DEFAULT_HOURLY_READINGS,
    DELIVERY_COMMITMENT_HOURS,
    TRAILING_CADENCE_DAYS,
    VARIANCE_EPS,
    load_settings,
)
from pipelines.conformance.units import CANONICAL_UNITS, canonical_parameter, normalize_unit
from pipelines.ingestion.fetch import bronze_path, parse_bronze_filename

SENSOR_DAY_COLUMNS = [
    "location_id",
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
    "lat_lon_unique",
]

STATION_DAY_COLUMNS = [
    "location_id",
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

# Backward-compatible alias for station-day numeric features used by the detector.
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


def _empty_sensor_day() -> pd.DataFrame:
    return pd.DataFrame(columns=SENSOR_DAY_COLUMNS)


def _empty_station_day() -> pd.DataFrame:
    return pd.DataFrame(columns=STATION_DAY_COLUMNS)


def max_stuck_run(values: pd.Series) -> int:
    """Longest run of identical consecutive values (ordered by caller)."""
    longest = current = 0
    prev = None
    for value in values:
        if pd.isna(value):
            prev = None
            current = 0
            continue
        if value == prev:
            current += 1
            longest = max(longest, current)
        else:
            current = 1
            longest = max(longest, current)
        prev = value
    return longest


def _unit_is_mismatch(parameter: str, original_unit: str) -> bool:
    param = canonical_parameter(parameter)
    canonical = CANONICAL_UNITS.get(param)
    if canonical is None:
        return False
    return normalize_unit(original_unit) != normalize_unit(canonical)


def _expected_readings(
    conformed: pd.DataFrame,
    location_id: int,
    sensor_id: int,
    parameter: str,
    date_local: str,
) -> int:
    """Trailing median daily cadence for one sensor-parameter, else hourly default."""
    day = date.fromisoformat(date_local)
    window_start = (day - timedelta(days=TRAILING_CADENCE_DAYS)).isoformat()
    history = conformed[
        (conformed["location_id"] == location_id)
        & (conformed["sensor_id"] == sensor_id)
        & (conformed["parameter"] == parameter)
        & (conformed["date_local"] >= window_start)
        & (conformed["date_local"] < date_local)
    ]
    if history.empty:
        return DEFAULT_HOURLY_READINGS
    daily_counts = history.groupby("date_local").size()
    if daily_counts.empty:
        return DEFAULT_HOURLY_READINGS
    return max(1, int(round(daily_counts.median())))


def compute_sensor_day_metrics(conformed: pd.DataFrame) -> pd.DataFrame:
    """Return one metric row per (location_id, sensor_id, parameter, date_local)."""
    if conformed is None or conformed.empty:
        return _empty_sensor_day()

    rows: list[dict] = []
    group_cols = ["location_id", "sensor_id", "parameter", "date_local"]
    for keys, part in conformed.groupby(group_cols, sort=False, dropna=False):
        location_id, sensor_id, parameter, date_local = keys
        ordered = part.sort_values("datetime_utc")
        received = len(ordered)
        expected = _expected_readings(
            conformed, int(location_id), int(sensor_id), str(parameter), str(date_local)
        )
        missing_rate = min(1.0, max(0.0, 1.0 - received / expected))

        dup_mask = ordered.duplicated(subset=["sensor_id", "datetime_utc", "parameter"], keep="first")
        duplicate_count = int(dup_mask.sum())

        values = ordered["value"]
        variance = float(values.var()) if received > 1 else 0.0
        stuck = max_stuck_run(values)

        unit_mismatch = int(
            ordered.apply(
                lambda r: _unit_is_mismatch(r["parameter"], r["original_unit"]),
                axis=1,
            ).sum()
        )
        lat_lon_unique = int(ordered[["lat", "lon"]].drop_duplicates().shape[0])

        rows.append(
            {
                "location_id": int(location_id),
                "sensor_id": int(sensor_id),
                "parameter": str(parameter),
                "date_local": str(date_local),
                "readings_received": received,
                "readings_expected": expected,
                "missing_rate": missing_rate,
                "null_value_count": int(values.isna().sum()),
                "negative_count": int((values < 0).sum()),
                "duplicate_count": duplicate_count,
                "max_stuck_run": stuck,
                "value_variance": variance,
                "unit_mismatch": unit_mismatch,
                "lat_lon_unique": lat_lon_unique,
            }
        )
    return pd.DataFrame(rows, columns=SENSOR_DAY_COLUMNS)


def load_bronze_manifest(bronze_root: Path) -> pd.DataFrame:
    """Load bronze arrival manifest as a DataFrame."""
    path = bronze_root / "_manifest.jsonl"
    if not path.exists():
        return pd.DataFrame(
            columns=[
                "location_id",
                "day",
                "archive_key",
                "status",
                "local_path",
                "bytes",
                "arrived_at",
                "error",
            ]
        )
    rows = []
    for line in path.read_text(encoding="utf-8").splitlines():
        if line.strip():
            rows.append(json.loads(line))
    if not rows:
        return pd.DataFrame()
    frame = pd.DataFrame(rows)
    if "location_id" in frame.columns:
        frame["location_id"] = pd.to_numeric(frame["location_id"], errors="coerce").astype("Int64")
    return frame


def read_bronze_schema(path: Path) -> set[str]:
    """Return normalized bronze CSV column names from the gzip header row."""
    compression = "gzip" if path.suffix == ".gz" else None
    frame = pd.read_csv(path, compression=compression, nrows=0, encoding="utf-8")
    return {str(c).strip().lower() for c in frame.columns}


def _schema_drift_by_location(bronze_root: Path) -> dict[tuple[int, str], bool]:
    """Return {(location_id, date_local): schema_changed} vs previous day."""
    flags: dict[tuple[int, str], bool] = {}
    by_location: dict[int, list[tuple[date, Path, set[str]]]] = {}
    for path in sorted(bronze_root.rglob("*.csv.gz")):
        parsed = parse_bronze_filename(path.name)
        if parsed is None:
            continue
        location_id, day = parsed
        by_location.setdefault(location_id, []).append((day, path, read_bronze_schema(path)))

    for location_id, entries in by_location.items():
        entries.sort(key=lambda item: item[0])
        prev_schema: set[str] | None = None
        for day, _path, schema in entries:
            date_local = day.isoformat()
            changed = prev_schema is not None and schema != prev_schema
            flags[(location_id, date_local)] = changed
            prev_schema = schema
    return flags


def _file_lateness_hours(location_id: int, date_local: str, manifest: pd.DataFrame) -> float:
    """Hours past the 72h delivery commitment for one location-day."""
    if manifest.empty:
        return 0.0
    day = date.fromisoformat(date_local)
    subset = manifest[
        (manifest["location_id"] == location_id) & (manifest["day"] == date_local)
    ]
    if subset.empty:
        return 0.0
    row = subset.iloc[-1]
    if row.get("status") == "missing" or not row.get("arrived_at"):
        return float(DELIVERY_COMMITMENT_HOURS)
    arrived = pd.to_datetime(row["arrived_at"], utc=True, errors="coerce")
    if pd.isna(arrived):
        return 0.0
    # Commitment: 72 hours after end of local day (approximate using UTC midnight + 1 day).
    deadline = datetime.combine(day + timedelta(days=1), datetime.min.time(), tzinfo=timezone.utc)
    deadline += timedelta(hours=DELIVERY_COMMITMENT_HOURS)
    lateness = (arrived.to_pydatetime() - deadline).total_seconds() / 3600.0
    return max(0.0, lateness)


def _cross_sensor_pm25_spread(conformed: pd.DataFrame, location_id: int, date_local: str) -> float:
    pm25 = conformed[
        (conformed["location_id"] == location_id)
        & (conformed["date_local"] == date_local)
        & (conformed["parameter"] == "pm25")
    ].copy()
    if pm25.empty:
        return 0.0
    pm25["hour"] = pd.to_datetime(pm25["datetime_utc"], utc=True).dt.floor("h")
    spreads = []
    for _, group in pm25.groupby("hour"):
        if group["sensor_id"].nunique() > 1:
            spreads.append(group["value"].max() - group["value"].min())
    return float(max(spreads)) if spreads else 0.0


def compute_station_day_metrics(
    conformed: pd.DataFrame,
    bronze_root: Path | None = None,
    sensor_metrics: pd.DataFrame | None = None,
) -> pd.DataFrame:
    """Return one metric row per (location_id, date_local)."""
    if conformed is None or conformed.empty:
        return _empty_station_day()

    settings = load_settings()
    bronze = Path(bronze_root or settings.bronze_root)
    detail = sensor_metrics if sensor_metrics is not None else compute_sensor_day_metrics(conformed)
    manifest = load_bronze_manifest(bronze)
    schema_flags = _schema_drift_by_location(bronze)

    station_days = conformed[["location_id", "date_local"]].drop_duplicates()
    rows: list[dict] = []

    sensors_by_loc_day: dict[tuple[int, str], set[int]] = {}
    for _, row in conformed.drop_duplicates(subset=["location_id", "date_local", "sensor_id"]).iterrows():
        key = (int(row["location_id"]), str(row["date_local"]))
        sensors_by_loc_day.setdefault(key, set()).add(int(row["sensor_id"]))

    sorted_days = station_days.sort_values(["location_id", "date_local"])
    prev_sensors: dict[int, set[int]] = {}

    for _, station in sorted_days.iterrows():
        location_id = int(station["location_id"])
        date_local = str(station["date_local"])
        key = (location_id, date_local)

        day_detail = detail[
            (detail["location_id"] == location_id) & (detail["date_local"] == date_local)
        ]
        total_readings = int(day_detail["readings_received"].sum()) if not day_detail.empty else 0
        duplicates = int(day_detail["duplicate_count"].sum()) if not day_detail.empty else 0
        duplicate_rate = duplicates / total_readings if total_readings else 0.0

        sensors_today = sensors_by_loc_day.get(key, set())
        dropout = len(prev_sensors.get(location_id, set()) - sensors_today)
        prev_sensors[location_id] = sensors_today

        day_obj = date.fromisoformat(date_local)
        file_present = bronze_path(bronze, location_id, day_obj).exists()

        rows.append(
            {
                "location_id": location_id,
                "date_local": date_local,
                "total_readings": total_readings,
                "missing_rate_mean": float(day_detail["missing_rate"].mean())
                if not day_detail.empty
                else 0.0,
                "sensors_expected": len(prev_sensors.get(location_id, set()) | sensors_today),
                "sensors_received": len(sensors_today),
                "sensor_dropout_count": dropout,
                "negative_count_total": int(day_detail["negative_count"].sum())
                if not day_detail.empty
                else 0,
                "max_stuck_run_max": int(day_detail["max_stuck_run"].max())
                if not day_detail.empty
                else 0,
                "zero_variance_params": int((day_detail["value_variance"] <= VARIANCE_EPS).sum())
                if not day_detail.empty
                else 0,
                "duplicate_rate": duplicate_rate,
                "schema_changed": bool(schema_flags.get(key, False)),
                "file_present": file_present,
                "file_lateness_hours": _file_lateness_hours(location_id, date_local, manifest),
                "unit_mismatch_count": int(day_detail["unit_mismatch"].sum())
                if not day_detail.empty
                else 0,
                "cross_sensor_pm25_spread": _cross_sensor_pm25_spread(
                    conformed, location_id, date_local
                ),
            }
        )

    return pd.DataFrame(rows, columns=STATION_DAY_COLUMNS)
