import json

import pandas as pd

from dashboard.data import (
    build_station_status,
    filter_by_date,
    latest_build_time,
    load_build_summaries,
    load_stations,
)
from dashboard.runtime import apply_runtime_secrets, is_public_dashboard
from pipelines import storage


def _stations() -> pd.DataFrame:
    return pd.DataFrame(
        [
            {"locationid": 1, "location_name": "A", "latitude": -33.8, "longitude": 151.2},
            {"locationid": 2, "location_name": "B", "latitude": -33.9, "longitude": 151.1},
            {"locationid": 3, "location_name": "C", "latitude": -33.7, "longitude": 151.0},
            {"locationid": 4, "location_name": "D", "latitude": -33.6, "longitude": 151.3},
        ]
    )


def _incidents() -> pd.DataFrame:
    return pd.DataFrame(
        [
            {"locationid": 2, "date_local": "2026-01-08", "rule_id": "R2", "severity": "high"},
            {"locationid": 3, "date_local": "2026-01-08", "rule_id": "R4", "severity": "medium"},
        ]
    )


def _trust_alerts() -> pd.DataFrame:
    return pd.DataFrame(
        [
            {
                "locationid": 1,
                "date_local": "2026-01-08",
                "status": "escalated",
                "trust_score": 0.9,
            },
            {
                "locationid": 2,
                "date_local": "2026-01-08",
                "status": "quarantined",
                "trust_score": 0.3,
            },
        ]
    )


def test_build_station_status_priority():
    result = build_station_status(
        _stations(), _incidents(), _trust_alerts(), as_of_date="2026-01-08"
    )
    by_id = result.set_index("locationid")["status"].to_dict()
    assert by_id[1] == "escalated"
    assert by_id[2] == "quarantined"
    assert by_id[3] == "quality_only"
    assert by_id[4] == "monitored"
    # Worst news sorts to the top of the map table.
    assert list(result["status"]) == ["escalated", "quarantined", "quality_only", "monitored"]


def test_build_station_status_other_day_is_all_monitored():
    result = build_station_status(
        _stations(), _incidents(), _trust_alerts(), as_of_date="2026-01-09"
    )
    assert set(result["status"]) == {"monitored"}
    assert set(result["incident_rule_ids"]) == {""}


def test_filter_by_date_without_date_returns_frame_unchanged():
    incidents = _incidents()
    assert len(filter_by_date(incidents, None)) == len(incidents)
    assert len(filter_by_date(incidents, "2026-01-08")) == 2
    assert filter_by_date(incidents, "2026-01-09").empty


def test_load_stations_empty_without_silver(tmp_path):
    assert load_stations(tmp_path / "silver").empty


def test_build_summaries_report_latest_success(tmp_path):
    silver, gold = tmp_path / "silver", tmp_path / "gold"
    storage.write_text(
        storage.join(silver, "_build.json"),
        json.dumps({"built_at_utc": "2026-01-08T01:00:00+00:00"}),
    )
    storage.write_text(
        storage.join(gold, "fusion/_build.json"),
        json.dumps({"built_at_utc": "2026-01-08T02:00:00+00:00"}),
    )

    summaries = load_build_summaries(silver, gold)
    assert set(summaries) == {"silver", "fusion"}
    assert latest_build_time(summaries).isoformat() == "2026-01-08T02:00:00+00:00"


def test_partial_build_is_not_reported_as_pipeline_success():
    summaries = {"silver": {"built_at_utc": "2026-01-08T03:00:00+00:00"}}
    assert latest_build_time(summaries) is None


def test_runtime_secrets_are_explicit_and_public_mode_is_boolean():
    environ = {"UNRELATED": "keep"}
    apply_runtime_secrets(
        {
            "SILVER_ROOT": "s3://bucket/silver",
            "DATAGUARD_PUBLIC_DASHBOARD": "true",
            "UNRELATED": "discard",
        },
        environ,
    )

    assert environ["SILVER_ROOT"] == "s3://bucket/silver"
    assert environ["UNRELATED"] == "keep"
    assert is_public_dashboard(environ)
