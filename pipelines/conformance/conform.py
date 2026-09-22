"""Conform raw OpenAQ measurements from bronze into the silver zone.

Bronze layout::

    locationid=<ID>/year=<YYYY>/location-<ID>-<YYYYMMDD>.csv.gz

Silver layout (one Parquet part per location-year)::

    locationid=<ID>/year=<YYYY>/part-0.parquet

Silver is the single harmonised measurement table: canonical parameter names,
canonical units, parsed timestamps, one row per reading. Every downstream
stage reads it. Values are conformed but never dropped or clipped — Layer 1
has to be able to see broken readings (negative concentrations, stuck values),
so ``original_unit`` is carried alongside the converted value.
"""

from __future__ import annotations

import json
import logging
import zlib
from dataclasses import dataclass
from pathlib import Path

import pandas as pd

from pipelines import storage
from pipelines.config import SILVER_GLUE_TABLE, load_settings
from pipelines.conformance.units import canonical_parameter, convert_series

logger = logging.getLogger(__name__)

SILVER_COLUMNS = [
    "locationid",
    "sensor_id",
    "location_name",
    "datetime",  # UTC, timezone-aware
    "datetime_local",  # local wall clock, timezone-naive
    "date_local",  # YYYY-MM-DD in station local time — the station-day key
    "latitude",
    "longitude",
    "parameter",  # canonical name (pm2.5 -> pm25)
    "unit",  # canonical unit
    "value",  # value expressed in the canonical unit
    "original_unit",  # unit as delivered, for the Layer 1 conformance check
]

# Row identity: one sensor cannot report the same parameter twice per instant.
ROW_KEY = ["locationid", "sensor_id", "datetime", "parameter"]

# Keyed by the provider column name lowercased with underscores stripped, so
# one entry covers location_id / locationId / LOCATIONID. Providers disagree on
# nearly every one of these, which is what the silver zone exists to settle.
COLUMN_ALIASES = {
    "locationid": "locationid",
    "sensorsid": "sensor_id",
    "sensorid": "sensor_id",
    "location": "location_name",
    "locationname": "location_name",
    "units": "unit",
    "datetime": "datetime_raw",
    "datetimeutc": "datetime_raw",
    "datetimelocal": "datetime_local_raw",
    "lat": "latitude",
    "lon": "longitude",
}

REQUIRED_AFTER_RENAME = {
    "locationid",
    "location_name",
    "datetime_raw",
    "latitude",
    "longitude",
    "parameter",
    "unit",
    "value",
}


@dataclass
class SilverBuildResult:
    files_read: int
    files_failed: int
    rows: int
    parameters: list[str]
    units: list[str]
    locations: list[int]
    date_local_min: str | None
    date_local_max: str | None
    output_path: str
    failed: list[str]


def empty_silver() -> pd.DataFrame:
    return pd.DataFrame(columns=SILVER_COLUMNS)


def _rename_raw_columns(raw: pd.DataFrame) -> pd.DataFrame:
    mapping = {}
    for column in raw.columns:
        key = str(column).strip()
        mapping[column] = COLUMN_ALIASES.get(key.lower().replace("_", ""), key)
    return raw.rename(columns=mapping)


def _naive_local(series: pd.Series) -> pd.Series:
    parsed = pd.to_datetime(series, utc=False, errors="coerce")
    if getattr(parsed.dtype, "tz", None) is not None:
        return parsed.dt.tz_localize(None)
    return parsed


def _synthetic_sensor_id(locationid: pd.Series, parameter: pd.Series) -> pd.Series:
    """Stable placeholder sensor_id when a bronze export omits sensors_id.

    Archive files always carry one; flat API exports do not. Hashing the pair
    keeps the id stable across runs, so sensor-day metrics still line up.
    """
    keys = locationid.astype("string") + ":" + parameter.astype("string")
    return pd.array(
        [pd.NA if pd.isna(k) else zlib.crc32(k.encode()) & 0x7FFFFFFF for k in keys],
        dtype="Int64",
    )


def conform_measurements(raw: pd.DataFrame) -> pd.DataFrame:
    """Return raw bronze rows conformed to the silver schema."""
    if raw is None or raw.empty:
        return empty_silver()

    frame = _rename_raw_columns(raw.copy())
    missing = REQUIRED_AFTER_RENAME.difference(frame.columns)
    if missing:
        raise ValueError(f"Bronze file missing required columns: {sorted(missing)}")

    # Prefer an explicit local timestamp; fall back to the only one supplied.
    local_source = (
        frame["datetime_local_raw"]
        if "datetime_local_raw" in frame.columns
        else frame["datetime_raw"]
    )
    datetime_local = _naive_local(local_source)

    parameter = frame["parameter"].map(canonical_parameter).astype("string")
    locationid = pd.to_numeric(frame["locationid"], errors="coerce").astype("Int64")
    if "sensor_id" in frame.columns:
        sensor_id = pd.to_numeric(frame["sensor_id"], errors="coerce").astype("Int64")
    else:
        sensor_id = _synthetic_sensor_id(locationid, parameter)

    original_unit = frame["unit"].astype("string")
    value, unit = convert_series(
        parameter, original_unit, pd.to_numeric(frame["value"], errors="coerce")
    )

    conformed = pd.DataFrame(
        {
            "locationid": locationid,
            "sensor_id": sensor_id,
            "location_name": frame["location_name"].astype("string"),
            "datetime": pd.to_datetime(frame["datetime_raw"], utc=True, errors="coerce"),
            "datetime_local": datetime_local,
            "date_local": datetime_local.dt.strftime("%Y-%m-%d").astype("string"),
            "latitude": pd.to_numeric(frame["latitude"], errors="coerce"),
            "longitude": pd.to_numeric(frame["longitude"], errors="coerce"),
            "parameter": parameter,
            "unit": unit.astype("string"),
            "value": pd.to_numeric(value, errors="coerce"),
            "original_unit": original_unit,
        }
    )
    return conformed.loc[:, SILVER_COLUMNS]


BRONZE_SUFFIXES = (".csv.gz", ".csv")


def discover_bronze_files(bronze_root: str | Path) -> list[str]:
    """Return every readable bronze file: archive ``*.csv.gz`` and export CSVs."""
    files = storage.list_files(bronze_root, BRONZE_SUFFIXES)
    return [f for f in files if not f.endswith(".tmp")]


def read_bronze_file(uri: str | Path) -> pd.DataFrame:
    """Read one bronze file from either backend.

    Rows are attributed to the location named in their own ``location_id``
    column rather than to the directory they were filed under, so a misfiled
    copy still conforms correctly and is then de-duplicated on ``ROW_KEY``.
    """
    return storage.read_csv(uri)


def _conform_all(bronze_root: str | Path) -> tuple[pd.DataFrame, list[str], list[str]]:
    """Conform every bronze file. Returns (rows, files seen, failures)."""
    files = discover_bronze_files(bronze_root)
    frames: list[pd.DataFrame] = []
    failed: list[str] = []
    for uri in files:
        try:
            frames.append(conform_measurements(read_bronze_file(uri)))
        except Exception as exc:  # noqa: BLE001 — one bad file must not abort the zone
            logger.warning("Failed to conform %s: %s", uri, exc)
            failed.append(f"{uri}: {exc}")
    if not frames:
        return empty_silver(), files, failed
    combined = pd.concat(frames, ignore_index=True).drop_duplicates(ROW_KEY, keep="last")
    return combined, files, failed


def read_conformed(bronze_root: str | Path | None = None) -> pd.DataFrame:
    """Read and conform every bronze file in memory.

    This is the only place bronze CSVs are parsed. ``build_silver``
    materialises the result; downstream stages read the silver zone instead.
    """
    settings = load_settings()
    rows, _, _ = _conform_all(bronze_root or settings.bronze_root)
    return rows


def read_silver(silver_root: str | Path | None = None) -> pd.DataFrame:
    """Read the silver zone back as one frame."""
    settings = load_settings()
    frame = storage.read_parquet(silver_root or settings.silver_root, "")
    if frame.empty:
        return empty_silver()
    return frame.loc[:, SILVER_COLUMNS]


def write_silver(conformed: pd.DataFrame, silver_root: str | Path) -> str:
    """Overwrite the silver zone, partitioned by location and year."""
    ordered = conformed.sort_values(ROW_KEY, kind="mergesort") if not conformed.empty else conformed
    return storage.write_parquet(
        ordered,
        silver_root,
        "",
        year_from="datetime_local",
        glue_table=SILVER_GLUE_TABLE,
    )


def build_silver(
    bronze_root: str | Path | None = None,
    silver_root: str | Path | None = None,
) -> SilverBuildResult:
    """Conform every bronze file and write the silver zone."""
    settings = load_settings()
    bronze = bronze_root or settings.bronze_root
    s_root = silver_root or settings.silver_root
    silver = storage.join(silver_root or settings.silver_root, "silver-data")

    combined, files, failed = _conform_all(bronze)
    write_silver(combined, silver)

    result = SilverBuildResult(
        files_read=len(files) - len(failed),
        files_failed=len(failed),
        rows=int(len(combined)),
        parameters=sorted(combined["parameter"].dropna().unique().tolist()),
        units=sorted(combined["unit"].dropna().unique().tolist()),
        locations=sorted(int(v) for v in combined["locationid"].dropna().unique()),
        date_local_min=None if combined.empty else str(combined["date_local"].min()),
        date_local_max=None if combined.empty else str(combined["date_local"].max()),
        output_path=storage.normalize(silver),
        failed=failed,
    )

    storage.write_text(
        storage.join(s_root, "_silver_build.json"), json.dumps(result.__dict__, indent=2) + "\n"
    )

    logger.info("Silver built: %s rows from %s files -> %s", len(combined), len(files), s_root)
    return result
