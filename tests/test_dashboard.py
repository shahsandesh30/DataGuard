import pandas as pd

from dashboard.data import build_station_status, load_stations


def test_build_station_status_priority():
    stations = pd.DataFrame(
        [
            {"location_id": 1, "location_name": "A", "lat": -33.8, "lon": 151.2},
            {"location_id": 2, "location_name": "B", "lat": -33.9, "lon": 151.1},
            {"location_id": 3, "location_name": "C", "lat": -33.7, "lon": 151.0},
            {"location_id": 4, "location_name": "D", "lat": -33.6, "lon": 151.3},
        ]
    )
    incidents = pd.DataFrame(
        [
            {
                "location_id": 2,
                "date_local": "2026-01-08",
                "rule_id": "R2",
                "severity": "high",
            },
            {
                "location_id": 3,
                "date_local": "2026-01-08",
                "rule_id": "R4",
                "severity": "medium",
            },
        ]
    )
    trust = pd.DataFrame(
        [
            {
                "location_id": 1,
                "date_local": "2026-01-08",
                "status": "escalated",
                "trust_score": 0.9,
            },
            {
                "location_id": 2,
                "date_local": "2026-01-08",
                "status": "quarantined",
                "trust_score": 0.3,
            },
        ]
    )
    result = build_station_status(stations, incidents, trust, as_of_date="2026-01-08")
    by_id = result.set_index("location_id")["status"].to_dict()
    assert by_id[1] == "escalated"
    assert by_id[2] == "quarantined"
    assert by_id[3] == "quality_only"
    assert by_id[4] == "monitored"


def test_load_stations_empty_without_bronze(tmp_path, monkeypatch):
    monkeypatch.setattr(
        "dashboard.data.read_conformed",
        lambda _bronze: pd.DataFrame(),
    )
    stations = load_stations(tmp_path / "bronze")
    assert stations.empty
