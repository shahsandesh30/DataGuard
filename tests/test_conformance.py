import gzip
from datetime import date
from pathlib import Path

import pandas as pd

from pipelines.conformance.conform import (
    CONFORMED_COLUMNS,
    SILVER_COLUMNS,
    build_silver,
    conform_measurements,
    discover_bronze_files,
    read_silver,
    to_export_frame,
)
from pipelines.conformance.units import convert_value, normalize_unit
from pipelines.ingestion.fetch import archive_key, bronze_key, bronze_path


def _raw_row(**overrides) -> dict:
    row = {
        "location_id": 2178,
        "sensors_id": 3919,
        "location": "Del Norte-2178",
        "datetime": "2023-01-01T01:00:00-07:00",
        "lat": 35.1353,
        "lon": -106.584702,
        "parameter": "pm10",
        "units": "µg/m³",
        "value": 45.0,
    }
    row.update(overrides)
    return row


def test_normalize_unit_collapses_micro_variants():
    assert normalize_unit("µg/m³") == "ug/m3"
    assert normalize_unit("ug/m3") == "ug/m3"
    assert normalize_unit("ppm") == "ppm"


def test_convert_ppm_co_to_ugm3():
    value, unit = convert_value("co", "ppm", 1.0)
    assert unit == "µg/m³"
    assert abs(value - 1145.61) < 0.1


def test_conform_measurements_renames_parses_and_keeps_negatives():
    raw = pd.DataFrame(
        [
            _raw_row(),
            _raw_row(parameter="o3", units="ppm", value=0.04, sensors_id=3917),
            _raw_row(parameter="so2", units="ppm", value=-0.001, sensors_id=3916),
        ]
    )
    conformed = conform_measurements(raw)
    assert list(conformed.columns) == CONFORMED_COLUMNS
    assert conformed["datetime_utc"].dt.tz is not None
    assert set(conformed["date_local"]) == {"2023-01-01"}
    assert set(conformed["unit"]) == {"µg/m³"}

    pm10 = conformed.loc[conformed["parameter"] == "pm10"].iloc[0]
    assert pm10["value"] == 45.0
    assert pm10["original_unit"] == "µg/m³"

    export = to_export_frame(conformed)
    assert list(export.columns) == SILVER_COLUMNS
    assert export["latitude"].iloc[0] == 35.1353


def test_conform_measurements_empty_returns_schema():
    conformed = conform_measurements(pd.DataFrame())
    assert list(conformed.columns) == CONFORMED_COLUMNS
    assert conformed.empty


def test_build_silver_from_gzipped_bronze(tmp_path: Path):
    day = date(2023, 1, 1)
    bronze_file = tmp_path / "bronze" / bronze_key(2178, day)
    bronze_file.parent.mkdir(parents=True)
    csv = (
        "location_id,sensors_id,location,datetime,lat,lon,parameter,units,value\n"
        "2178,3919,Del Norte-2178,2023-01-01T01:00:00-07:00,35.1353,-106.584702,pm10,µg/m³,45.0\n"
        "2178,3917,Del Norte-2178,2023-01-01T01:00:00-07:00,35.1353,-106.584702,o3,ppm,0.04\n"
    )
    bronze_file.write_bytes(gzip.compress(csv.encode("utf-8")))

    silver_root = tmp_path / "silver"
    result = build_silver(bronze_root=tmp_path / "bronze", silver_root=silver_root)
    assert result.files_read == 1
    assert result.rows == 2
    assert result.files_failed == 0

    silver = read_silver(silver_root)
    assert len(silver) == 2
    partition_dir = silver_root / "locationid=2178" / "year=2023"
    assert partition_dir.exists()
    assert len(list(partition_dir.iterdir())) == 1
    assert (silver_root / "_build.json").exists()


def _sydney_row(**overrides) -> dict:
    row = {
        "location_id": 2392564,
        "location_name": "Sydney, Australia",
        "parameter": "pm25",
        "value": 12.5,
        "unit": "µg/m³",
        "datetimeUtc": "2026-08-01T14:00:00Z",
        "datetimeLocal": "2026-08-02T00:00:00+10:00",
        "timezone": "Australia/Sydney",
        "latitude": -33.8877,
        "longitude": 151.2150,
        "provider": "AirGradient",
    }
    row.update(overrides)
    return row


def test_conform_api_export_schema():
    raw = pd.DataFrame([_sydney_row(), _sydney_row(parameter="pm1", value=5.4)])
    conformed = conform_measurements(raw)
    assert list(conformed.columns) == CONFORMED_COLUMNS
    assert conformed["lat"].iloc[0] == -33.8877
    assert conformed["lon"].iloc[0] == 151.2150
    assert set(conformed["date_local"]) == {"2026-08-02"}
    assert conformed["sensor_id"].notna().all()

    export = to_export_frame(conformed)
    assert list(export.columns) == SILVER_COLUMNS
    assert export["latitude"].iloc[0] == -33.8877
    assert export["location"].iloc[0] == "Sydney, Australia"


def test_discover_bronze_includes_flat_csv(tmp_path: Path):
    day = date(2023, 1, 1)
    gz_path = tmp_path / "bronze" / bronze_key(2178, day)
    gz_path.parent.mkdir(parents=True)
    gz_path.write_bytes(gzip.compress(b"location_id,sensors_id\n"))
    csv_path = tmp_path / "bronze" / "openaq_location_2392564_sydney.csv"
    csv_path.write_text("location_id,parameter\n", encoding="utf-8")

    discovered = discover_bronze_files(tmp_path / "bronze")
    assert gz_path in discovered
    assert csv_path in discovered
    assert len(discovered) == 2


def test_build_silver_from_flat_export_csv(tmp_path: Path):
    bronze_root = tmp_path / "bronze"
    bronze_root.mkdir()
    csv_path = bronze_root / "openaq_location_2392564_sydney.csv"
    csv_path.write_text(
        "location_id,location_name,parameter,value,unit,datetimeUtc,datetimeLocal,"
        "latitude,longitude\n"
        "2392564,\"Sydney, Australia\",pm25,12.5,µg/m³,"
        "2026-08-01T14:00:00Z,2026-08-02T00:00:00+10:00,-33.8877,151.2150\n",
        encoding="utf-8",
    )

    silver_root = tmp_path / "silver"
    result = build_silver(bronze_root=bronze_root, silver_root=silver_root)
    assert result.files_read == 1
    assert result.rows == 1
    assert result.files_failed == 0

    silver = read_silver(silver_root)
    assert len(silver) == 1
    assert silver["location"].iloc[0] == "Sydney, Australia"
    partition_dir = silver_root / "locationid=2392564" / "year=2026"
    assert partition_dir.exists()
    assert (silver_root / "_build.json").exists()


def test_bronze_key_local_layout():
    assert bronze_key(1544061, date(2026, 1, 1)) == (
        "locationid=1544061/year=2026/location-1544061-20260101.csv.gz"
    )
    assert archive_key(1544061, date(2026, 1, 1)).startswith("records/csv.gz/")
