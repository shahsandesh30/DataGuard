import json

import pandas as pd
import pytest

from pipelines import storage
from pipelines.conformance.conform import SILVER_COLUMNS, empty_silver
from pipelines.ingestion.fetch import MANIFEST_FILENAME
from pipelines.quality.metrics import (
    compute_sensor_day_metrics,
    compute_station_day_metrics,
    max_stuck_run,
)
from pipelines.quality.rules import apply_quality_rules


def _silver_frame(rows: list[dict]) -> pd.DataFrame:
    return pd.DataFrame(rows, columns=SILVER_COLUMNS)


def _base_row(**overrides) -> dict:
    row = {
        "locationid": 100,
        "sensor_id": 1,
        "location_name": "Test-100",
        "datetime": pd.Timestamp("2026-01-01 01:00:00", tz="UTC"),
        "datetime_local": pd.Timestamp("2026-01-01 12:00:00"),
        "date_local": "2026-01-01",
        "latitude": -33.0,
        "longitude": 151.0,
        "parameter": "pm25",
        "value": 10.0,
        "unit": "µg/m³",
        "original_unit": "µg/m³",
    }
    row.update(overrides)
    return row


def test_max_stuck_run_detects_consecutive_identical_values():
    assert max_stuck_run(pd.Series([1.0, 1.0, 1.0, 2.0, 2.0])) == 3


def test_sensor_day_metrics_flags_negative_and_stuck():
    stuck_rows = [
        _base_row(
            datetime=pd.Timestamp(f"2026-01-01 {h:02d}:00:00", tz="UTC"),
            datetime_local=pd.Timestamp(f"2026-01-01 {h+11:02d}:00:00"),
            value=5.0,
        )
        for h in range(8)
    ]
    neg_row = _base_row(
        datetime=pd.Timestamp("2026-01-01 09:00:00", tz="UTC"),
        datetime_local=pd.Timestamp("2026-01-01 20:00:00"),
        value=-0.5,
    )
    conformed = _silver_frame(stuck_rows + [neg_row])
    metrics = compute_sensor_day_metrics(conformed)
    row = metrics.iloc[0]
    assert row["readings_received"] == 9
    assert row["negative_count"] == 1
    assert row["max_stuck_run"] == 8


def test_station_day_rules_fire_for_negative_and_stuck():
    stuck_rows = [
        _base_row(
            datetime=pd.Timestamp(f"2026-01-01 {h:02d}:00:00", tz="UTC"),
            datetime_local=pd.Timestamp(f"2026-01-01 {h+11:02d}:00:00"),
            value=5.0 if h < 7 else -1.0,
        )
        for h in range(8)
    ]
    conformed = _silver_frame(stuck_rows)
    sensor = compute_sensor_day_metrics(conformed)
    station = compute_station_day_metrics(conformed, sensor_metrics=sensor)
    incidents = apply_quality_rules(station)
    rule_ids = set(incidents["rule_id"])
    assert "R1" in rule_ids
    assert "R2" in rule_ids


def test_duplicate_readings_trigger_uniqueness_rule():
    rows = [
        _base_row(),
        _base_row(),
    ]
    conformed = _silver_frame(rows)
    sensor = compute_sensor_day_metrics(conformed)
    station = compute_station_day_metrics(conformed, sensor_metrics=sensor)
    incidents = apply_quality_rules(station)
    assert "R9" in set(incidents["rule_id"])


def test_missing_hours_raise_completeness_rule():
    rows = [
        _base_row(
            datetime=pd.Timestamp(f"2026-01-01 {h:02d}:00:00", tz="UTC"),
            datetime_local=pd.Timestamp(f"2026-01-01 {h+11:02d}:00:00"),
        )
        for h in range(4)
    ]
    conformed = _silver_frame(rows)
    sensor = compute_sensor_day_metrics(conformed)
    station = compute_station_day_metrics(conformed, sensor_metrics=sensor)
    assert station.iloc[0]["missing_rate_mean"] > 0.25
    incidents = apply_quality_rules(station)
    assert "R4" in set(incidents["rule_id"])


@pytest.mark.parametrize("as_string", [False, True])
def test_manifest_days_survive_consecutive_outages_without_inventing_days(tmp_path, as_string):
    bronze = str(tmp_path) if as_string else tmp_path
    manifest = [
        {"locationid": 100, "day": day, "status": "missing", "arrived_at": None}
        for day in ("2026-01-02", "2026-01-02", "2026-01-03")
    ]
    manifest.extend([
        {"locationid": None, "day": "2026-01-05", "status": "missing"},
        {"locationid": 100, "day": "invalid", "status": "missing"},
    ])
    storage.write_text(storage.join(bronze, MANIFEST_FILENAME),
                       "\n".join(json.dumps(row) for row in manifest))
    conformed = _silver_frame([
        _base_row(date_local=day) for day in ("2026-01-01", "2026-01-06")
    ])

    station = compute_station_day_metrics(conformed, bronze)
    assert station["date_local"].tolist() == [
        "2026-01-01", "2026-01-02", "2026-01-03", "2026-01-06",
    ]
    missing = station[station["date_local"].isin(["2026-01-02", "2026-01-03"])]
    assert missing["total_readings"].tolist() == [0, 0]
    assert missing["missing_rate_mean"].tolist() == [1.0, 1.0]
    assert missing["sensors_expected"].tolist() == [1, 1]
    assert missing["sensors_received"].tolist() == [0, 0]
    assert missing["sensor_dropout_count"].tolist() == [1, 1]
    assert missing["zero_variance_params"].tolist() == [0, 0]
    for _, incidents in apply_quality_rules(missing).groupby("date_local"):
        assert set(incidents["rule_id"]) == {"R4", "R5", "R7"}


def test_manifest_only_day_is_checked_on_s3_without_credentials(monkeypatch):
    """Missing silver must still reach backend-independent file-presence checks."""
    class FakeS3:
        def list_objects(self, prefix):
            assert prefix == "s3://test/bronze/"
            return []

        def does_object_exist(self, uri):
            assert uri == "s3://test/bronze/locationid=100/year=2026/location-100-20260102.csv.gz"
            return False

    class FakeWrangler:
        s3 = FakeS3()

    def read_text(uri):
        assert uri == "s3://test/bronze/_manifest.jsonl"
        return json.dumps({"locationid": 100, "day": "2026-01-02", "status": "missing"})

    monkeypatch.setattr(storage, "_wr", lambda: FakeWrangler())
    monkeypatch.setattr(storage, "read_text", read_text)
    station = compute_station_day_metrics(empty_silver(), "s3://test/bronze")
    assert len(station) == 1
    assert station.iloc[0]["sensors_expected"] == 0
    assert set(apply_quality_rules(station)["rule_id"]) == {"R4", "R7"}
