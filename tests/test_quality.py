import pandas as pd

from pipelines.conformance.conform import CONFORMED_COLUMNS
from pipelines.quality.metrics import (
    compute_sensor_day_metrics,
    compute_station_day_metrics,
    max_stuck_run,
)
from pipelines.quality.rules import apply_quality_rules


def _conformed_frame(rows: list[dict]) -> pd.DataFrame:
    return pd.DataFrame(rows, columns=CONFORMED_COLUMNS)


def _base_row(**overrides) -> dict:
    row = {
        "location_id": 100,
        "sensor_id": 1,
        "location_name": "Test-100",
        "datetime_utc": pd.Timestamp("2026-01-01 01:00:00", tz="UTC"),
        "datetime_local": pd.Timestamp("2026-01-01 12:00:00"),
        "date_local": "2026-01-01",
        "lat": -33.0,
        "lon": 151.0,
        "parameter": "pm25",
        "value": 10.0,
        "unit": "µg/m³",
        "original_value": 10.0,
        "original_unit": "µg/m³",
        "source_file": "test.csv.gz",
    }
    row.update(overrides)
    return row


def test_max_stuck_run_detects_consecutive_identical_values():
    assert max_stuck_run(pd.Series([1.0, 1.0, 1.0, 2.0, 2.0])) == 3


def test_sensor_day_metrics_flags_negative_and_stuck():
    stuck_rows = [
        _base_row(
            datetime_utc=pd.Timestamp(f"2026-01-01 {h:02d}:00:00", tz="UTC"),
            datetime_local=pd.Timestamp(f"2026-01-01 {h+11:02d}:00:00"),
            value=5.0,
        )
        for h in range(8)
    ]
    neg_row = _base_row(
        datetime_utc=pd.Timestamp("2026-01-01 09:00:00", tz="UTC"),
        datetime_local=pd.Timestamp("2026-01-01 20:00:00"),
        value=-0.5,
    )
    conformed = _conformed_frame(stuck_rows + [neg_row])
    metrics = compute_sensor_day_metrics(conformed)
    row = metrics.iloc[0]
    assert row["readings_received"] == 9
    assert row["negative_count"] == 1
    assert row["max_stuck_run"] == 8


def test_station_day_rules_fire_for_negative_and_stuck():
    stuck_rows = [
        _base_row(
            datetime_utc=pd.Timestamp(f"2026-01-01 {h:02d}:00:00", tz="UTC"),
            datetime_local=pd.Timestamp(f"2026-01-01 {h+11:02d}:00:00"),
            value=5.0 if h < 7 else -1.0,
        )
        for h in range(8)
    ]
    conformed = _conformed_frame(stuck_rows)
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
    conformed = _conformed_frame(rows)
    sensor = compute_sensor_day_metrics(conformed)
    station = compute_station_day_metrics(conformed, sensor_metrics=sensor)
    incidents = apply_quality_rules(station)
    assert "R9" in set(incidents["rule_id"])


def test_missing_hours_raise_completeness_rule():
    rows = [
        _base_row(
            datetime_utc=pd.Timestamp(f"2026-01-01 {h:02d}:00:00", tz="UTC"),
            datetime_local=pd.Timestamp(f"2026-01-01 {h+11:02d}:00:00"),
        )
        for h in range(4)
    ]
    conformed = _conformed_frame(rows)
    sensor = compute_sensor_day_metrics(conformed)
    station = compute_station_day_metrics(conformed, sensor_metrics=sensor)
    assert station.iloc[0]["missing_rate_mean"] > 0.25
    incidents = apply_quality_rules(station)
    assert "R4" in set(incidents["rule_id"])
