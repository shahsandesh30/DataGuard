"""Conform raw OpenAQ measurements into the silver zone.

Bronze layout::

    locationid=<ID>/year=<YYYY>/location-<ID>-<YYYYMMDD>.csv.gz

Silver layout (one Parquet export per location-year)::

    locationid=<ID>/year=<YYYY>/<timestamp>_qnxhe_<uuid>

Silver export schema matches the OpenAQ parquet export: ``sensor_id``,
``location``, ``datetime``, ``latitude``, ``longitude``, ``parameter``,
``unit``, ``value``. Values are conformed but never dropped — Layer 1 needs
to see broken readings (negative concentrations, stuck values).
"""

from __future__ import annotations

import json
import logging
import uuid
import zlib
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd

from pipelines.config import load_settings
from pipelines.conformance.units import canonical_parameter, convert_series
from pipelines.ingestion.fetch import bronze_key, parse_bronze_filename

logger = logging.getLogger(__name__)

# On-disk silver schema (matches sample export parquet).
SILVER_COLUMNS = [
    "sensor_id",
    "location",
    "datetime",
    "latitude",
    "longitude",
    "parameter",
    "unit",
    "value",
]

# Internal working columns used during conform (not written to silver).
CONFORMED_COLUMNS = [
    "location_id",
    "sensor_id",
    "location_name",
    "datetime_utc",
    "datetime_local",
    "date_local",
    "lat",
    "lon",
    "parameter",
    "value",
    "unit",
    "original_value",
    "original_unit",
    "source_file",
]

COLUMN_ALIASES = {
    "sensorsid": "sensor_id",
    "sensorid": "sensor_id",
    "location": "location_name",
    "locationname": "location_name",
    "units": "unit",
    "datetime": "datetime_raw",
    "datetimeutc": "datetime_raw",
    "datetimelocal": "datetime_local_raw",
    "latitude": "lat",
    "longitude": "lon",
}

REQUIRED_AFTER_RENAME = {
    "location_id",
    "location_name",
    "datetime_raw",
    "lat",
    "lon",
    "parameter",
    "unit",
    "value",
}


@dataclass
class SilverBuildResult:
    files_read: int
    files_failed: int
    rows: int
    output_path: str
    failed: list[str]


def _empty_conformed() -> pd.DataFrame:
    return pd.DataFrame(columns=CONFORMED_COLUMNS)


def _empty_silver() -> pd.DataFrame:
    return pd.DataFrame(columns=SILVER_COLUMNS)


def _rename_raw_columns(raw: pd.DataFrame) -> pd.DataFrame:
    mapping: dict[str, str] = {}
    for column in raw.columns:
        key = str(column).strip()
        folded = key.lower().replace("_", "")
        mapping[column] = COLUMN_ALIASES.get(folded, key)
    return raw.rename(columns=mapping)


def _local_datetime(series: pd.Series) -> pd.Series:
    parsed = pd.to_datetime(series, utc=False, errors="coerce")
    if getattr(parsed.dtype, "tz", None) is not None:
        parsed = parsed.dt.tz_localize(None)
    return parsed


def _date_local_from_series(series: pd.Series) -> pd.Series:
    parsed = pd.to_datetime(series, utc=False, errors="coerce")
    if getattr(parsed.dtype, "tz", None) is not None:
        return parsed.dt.strftime("%Y-%m-%d").astype("string")
    return parsed.map(
        lambda stamp: stamp.date().isoformat() if pd.notna(stamp) else pd.NA
    ).astype("string")


def _parse_datetimes(
    utc_series: pd.Series,
    local_series: pd.Series | None = None,
) -> tuple[pd.Series, pd.Series, pd.Series]:
    datetime_utc = pd.to_datetime(utc_series, utc=True, errors="coerce")
    local_source = local_series if local_series is not None else utc_series
    datetime_local = _local_datetime(local_source)
    date_local = _date_local_from_series(local_source)
    return datetime_utc, datetime_local, date_local


def _synthetic_sensor_id(location_id: pd.Series, parameter: pd.Series) -> pd.Series:
    """Stable placeholder sensor_id when bronze exports omit sensors_id."""
    ids = []
    for loc, param in zip(location_id, parameter, strict=True):
        if pd.isna(loc) or pd.isna(param):
            ids.append(pd.NA)
            continue
        key = f"{int(loc)}:{param}".encode()
        ids.append(zlib.crc32(key) & 0x7FFFFFFF)
    return pd.array(ids, dtype="Int64")


def _source_file_name(raw: pd.DataFrame) -> pd.Series:
    if "source_file" in raw.columns:
        return raw["source_file"].astype("string")
    return pd.Series(["unknown"] * len(raw), index=raw.index, dtype="string")


def _file_location_id(path: Path) -> int | None:
    parsed = parse_bronze_filename(path.name)
    if parsed is not None:
        return parsed[0]
    for part in path.parts:
        if part.startswith("locationid="):
            return int(part.split("=", 1)[1])
    return None


def conform_measurements(raw: pd.DataFrame) -> pd.DataFrame:
    """Return raw bronze rows conformed to the internal working schema.

    Rows are never dropped or clipped. Invalid values stay in the table for
    Layer 1.
    """
    if raw is None or raw.empty:
        return _empty_conformed()

    frame = _rename_raw_columns(raw.copy())
    missing = REQUIRED_AFTER_RENAME.difference(frame.columns)
    if missing:
        raise ValueError(f"Bronze file missing required columns: {sorted(missing)}")

    local_raw = frame["datetime_local_raw"] if "datetime_local_raw" in frame.columns else None
    datetime_utc, datetime_local, date_local = _parse_datetimes(frame["datetime_raw"], local_raw)
    parameter = frame["parameter"].map(canonical_parameter).astype("string")
    location_id = pd.to_numeric(frame["location_id"], errors="coerce").astype("Int64")
    if "sensor_id" in frame.columns:
        sensor_id = pd.to_numeric(frame["sensor_id"], errors="coerce").astype("Int64")
    else:
        sensor_id = _synthetic_sensor_id(location_id, parameter)
    original_unit = frame["unit"].astype("string")
    original_value = pd.to_numeric(frame["value"], errors="coerce")
    value, unit = convert_series(parameter, original_unit, original_value)

    conformed = pd.DataFrame(
        {
            "location_id": location_id,
            "sensor_id": sensor_id,
            "location_name": frame["location_name"].astype("string"),
            "datetime_utc": datetime_utc,
            "datetime_local": datetime_local,
            "date_local": date_local,
            "lat": pd.to_numeric(frame["lat"], errors="coerce"),
            "lon": pd.to_numeric(frame["lon"], errors="coerce"),
            "parameter": parameter,
            "value": pd.to_numeric(value, errors="coerce"),
            "unit": unit.astype("string"),
            "original_value": original_value,
            "original_unit": original_unit,
            "source_file": _source_file_name(frame),
        }
    )
    return conformed.loc[:, CONFORMED_COLUMNS]


def to_export_frame(conformed: pd.DataFrame) -> pd.DataFrame:
    """Map the internal conformed frame to the on-disk silver export schema."""
    if conformed.empty:
        return _empty_silver()
    export = pd.DataFrame(
        {
            "sensor_id": pd.to_numeric(conformed["sensor_id"], errors="coerce").astype("Int64"),
            "location": conformed["location_name"].astype("string"),
            "datetime": conformed["datetime_local"],
            "latitude": pd.to_numeric(conformed["lat"], errors="coerce"),
            "longitude": pd.to_numeric(conformed["lon"], errors="coerce"),
            "parameter": conformed["parameter"].astype("string"),
            "unit": conformed["unit"].astype("string"),
            "value": pd.to_numeric(conformed["value"], errors="coerce"),
        }
    )
    return export.loc[:, SILVER_COLUMNS]


def discover_bronze_files(bronze_root: Path) -> list[Path]:
    """Return csv.gz files under bronze Hive layout and flat API export CSVs."""
    if not bronze_root.exists():
        return []
    files = {
        path
        for path in bronze_root.rglob("*.csv.gz")
        if path.is_file() and not path.name.endswith(".tmp")
    }
    for path in bronze_root.glob("openaq_location_*.csv"):
        if path.is_file() and not path.name.endswith(".tmp"):
            files.add(path)
    return sorted(files)


def _source_key_for(path: Path, bronze_root: Path) -> str:
    try:
        rel = path.relative_to(bronze_root)
        return rel.as_posix()
    except ValueError:
        pass
    parsed = parse_bronze_filename(path.name)
    if parsed is None:
        return path.name
    location_id, day = parsed
    return bronze_key(location_id, day)


def read_bronze_file(path: Path, bronze_root: Path | None = None) -> pd.DataFrame:
    root = bronze_root or _find_bronze_root(path)
    compression = "gzip" if path.suffix == ".gz" else None
    frame = pd.read_csv(path, compression=compression, encoding="utf-8")
    frame["source_file"] = _source_key_for(path, root)

    file_loc = _file_location_id(path)
    if file_loc is not None and "location_id" in frame.columns:
        frame = frame[frame["location_id"] == file_loc]
    return frame


def _find_bronze_root(path: Path) -> Path:
    for parent in path.parents:
        if parent.name == "bronze":
            return parent
    return path.parent


def _silver_export_filename() -> str:
    stamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    return f"{stamp}_00088_qnxhe_{uuid.uuid4()}"


def write_silver_dataset(conformed: pd.DataFrame, silver_root: Path) -> Path:
    """Write one Parquet export per location-year under ``silver_root``."""
    if silver_root.exists():
        for existing in silver_root.rglob("*"):
            if existing.is_file() and not existing.name.startswith("_"):
                existing.unlink()

    export = to_export_frame(conformed)
    if export.empty:
        silver_root.mkdir(parents=True, exist_ok=True)
        export.to_parquet(silver_root / "_empty", index=False, compression="snappy")
        return silver_root

    export = export.copy()
    export["_location_id"] = conformed["location_id"].values
    export["_year"] = pd.to_datetime(export["datetime"]).dt.year.astype("Int64")

    ordered = export.sort_values(
        ["_location_id", "_year", "datetime", "parameter", "sensor_id"],
        kind="mergesort",
    )
    for (location_id, year), part in ordered.groupby(["_location_id", "_year"], sort=False):
        if pd.isna(location_id) or pd.isna(year):
            continue
        partition_dir = silver_root / f"locationid={int(location_id)}" / f"year={int(year)}"
        partition_dir.mkdir(parents=True, exist_ok=True)
        out = part.drop(columns=["_location_id", "_year"]).drop_duplicates(
            subset=["sensor_id", "datetime", "parameter"], keep="last"
        )
        out.to_parquet(
            partition_dir / _silver_export_filename(),
            index=False,
            compression="snappy",
        )
    return silver_root


def read_silver(silver_root: Path | None = None) -> pd.DataFrame:
    settings = load_settings()
    root = Path(silver_root or settings.silver_root)
    files = [
        path
        for path in sorted(root.rglob("*"))
        if path.is_file() and not path.name.startswith("_")
    ]
    if not files:
        return _empty_silver()
    frames = [pd.read_parquet(path) for path in files]
    combined = pd.concat(frames, ignore_index=True)
    return combined.loc[:, SILVER_COLUMNS]


def build_silver(
    bronze_root: Path | None = None,
    silver_root: Path | None = None,
) -> SilverBuildResult:
    """Read every bronze file, conform it, and write the silver dataset."""
    settings = load_settings()
    bronze = Path(bronze_root or settings.bronze_root)
    silver_dir = Path(silver_root or settings.silver_root)

    files = discover_bronze_files(bronze)
    frames: list[pd.DataFrame] = []
    failed: list[str] = []
    for path in files:
        try:
            frames.append(conform_measurements(read_bronze_file(path, bronze_root=bronze)))
        except Exception as exc:  # noqa: BLE001 — one bad file must not abort the zone
            logger.warning("Failed to conform %s: %s", path, exc)
            failed.append(f"{path}: {exc}")

    combined = pd.concat(frames, ignore_index=True) if frames else _empty_conformed()
    if not combined.empty:
        combined = combined.drop_duplicates(
            subset=["location_id", "sensor_id", "datetime_utc", "parameter"],
            keep="last",
        )
    output_path = write_silver_dataset(combined, silver_dir)

    result = SilverBuildResult(
        files_read=len(files) - len(failed),
        files_failed=len(failed),
        rows=int(len(combined)),
        output_path=str(output_path),
        failed=failed,
    )
    silver_dir.mkdir(parents=True, exist_ok=True)
    export = to_export_frame(combined)
    summary = {
        "files_read": result.files_read,
        "files_failed": result.files_failed,
        "rows": result.rows,
        "export_rows": int(len(export)),
        "parameters": sorted(combined["parameter"].dropna().unique().tolist())
        if not combined.empty
        else [],
        "units": sorted(combined["unit"].dropna().unique().tolist()) if not combined.empty else [],
        "locations": sorted(int(v) for v in combined["location_id"].dropna().unique().tolist())
        if not combined.empty
        else [],
        "years": sorted(int(v) for v in pd.to_datetime(export["datetime"]).dt.year.unique().tolist())
        if not export.empty
        else [],
        "date_local_min": None if combined.empty else str(combined["date_local"].min()),
        "date_local_max": None if combined.empty else str(combined["date_local"].max()),
        "failed": failed,
    }
    (silver_dir / "_build.json").write_text(
        json.dumps(summary, indent=2) + "\n", encoding="utf-8"
    )
    logger.info(
        "Silver built: %s rows from %s files -> %s",
        result.rows,
        result.files_read,
        output_path,
    )
    return result


def read_conformed(bronze_root: Path | None = None) -> pd.DataFrame:
    """Conform all bronze files in memory for Layer 1 (internal schema)."""
    settings = load_settings()
    bronze = Path(bronze_root or settings.bronze_root)

    files = discover_bronze_files(bronze)
    frames: list[pd.DataFrame] = []
    for path in files:
        try:
            frames.append(conform_measurements(read_bronze_file(path, bronze_root=bronze)))
        except Exception as exc:  # noqa: BLE001
            logger.warning("Failed to conform %s: %s", path, exc)

    combined = pd.concat(frames, ignore_index=True) if frames else _empty_conformed()
    if combined.empty:
        return combined
    return combined.drop_duplicates(
        subset=["location_id", "sensor_id", "datetime_utc", "parameter"],
        keep="last",
    )
