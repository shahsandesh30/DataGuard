import pandas as pd

from dashboard.data import build_station_status, filter_by_date, load_stations


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
