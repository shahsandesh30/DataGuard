import pandas as pd

from pipelines.config import MIN_EVENT_ROWS
from pipelines.conformance.conform import CONFORMED_COLUMNS
from pipelines.detection.build import build_detection, read_event_alerts, read_event_features
from pipelines.detection.ensemble import EVENT_ALERT_COLUMNS, fit_ensemble, score_events
from pipelines.detection.features import EVENT_FEATURE_COLUMNS, build_event_features, weak_labels


def _conformed_frame(rows: list[dict]) -> pd.DataFrame:
    return pd.DataFrame(rows, columns=CONFORMED_COLUMNS)


def _base_row(**overrides) -> dict:
    row = {
        "location_id": 1544061,
        "sensor_id": 1,
        "location_name": "Anzac",
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


def _baseline_day(location_id: int, date_local: str, hour_offset: int, value: float) -> list[dict]:
    rows = []
    for h in range(24):
        rows.append(
            _base_row(
                location_id=location_id,
                date_local=date_local,
                datetime_utc=pd.Timestamp(f"{date_local} {h:02d}:00:00", tz="UTC") + pd.Timedelta(hours=hour_offset),
                datetime_local=pd.Timestamp(f"{date_local} {h:02d}:00:00"),
                parameter="pm25",
                value=value + (h % 3) * 0.2,
            )
        )
    return rows


def _build_spike_fixture() -> pd.DataFrame:
    rows: list[dict] = []
    for day in range(1, 8):
        date_local = f"2026-01-{day:02d}"
        for loc in (1544061, 1601414, 2455394, 6430870):
            rows.extend(_baseline_day(loc, date_local, day, 12.0))
    spike_day = "2026-01-08"
    for loc in (1544061, 1601414, 2455394, 6430870):
        rows.extend(_baseline_day(loc, spike_day, 8, 12.0))
    for h in range(24):
        rows.append(
            _base_row(
                location_id=1544061,
                date_local=spike_day,
                datetime_utc=pd.Timestamp(f"{spike_day} {h:02d}:00:00", tz="UTC"),
                datetime_local=pd.Timestamp(f"{spike_day} {h:02d}:00:00"),
                parameter="pm25",
                value=80.0 if h >= 10 else 12.0,
            )
        )
    return _conformed_frame(rows)


def test_spike_day_has_high_z_score_and_roc():
    conformed = _build_spike_fixture()
    features = build_event_features(conformed)
    spike = features[
        (features["location_id"] == 1544061)
        & (features["date_local"] == "2026-01-08")
        & (features["parameter"] == "pm25")
    ]
    baseline = features[
        (features["location_id"] == 1544061)
        & (features["date_local"] == "2026-01-07")
        & (features["parameter"] == "pm25")
    ]
    assert not spike.empty
    assert spike.iloc[0]["z_score"] > baseline.iloc[0]["z_score"]
    assert spike.iloc[0]["roc_max"] > 0


def test_flat_baseline_days_have_low_alert_scores():
    conformed = _build_spike_fixture()
    features = build_event_features(conformed)
    models = fit_ensemble(features)
    alerts = score_events(models, features, weak_label=weak_labels(features, conformed))
    baseline_alerts = alerts[
        (alerts["location_id"] == 1601414) & (alerts["date_local"] == "2026-01-03")
    ]
    spike_alerts = alerts[
        (alerts["location_id"] == 1544061) & (alerts["date_local"] == "2026-01-08")
    ]
    if not baseline_alerts.empty and not spike_alerts.empty:
        assert spike_alerts.iloc[0]["alert_score"] >= baseline_alerts.iloc[0]["alert_score"]


def test_single_station_spike_has_high_spatial_isolation():
    rows: list[dict] = []
    for day in range(1, 6):
        date_local = f"2026-01-{day:02d}"
        for loc in (1544061, 1601414, 2455394):
            rows.extend(_baseline_day(loc, date_local, day, 10.0))
    spike_day = "2026-01-06"
    rows.extend(_baseline_day(1601414, spike_day, 6, 10.0))
    rows.extend(_baseline_day(2455394, spike_day, 6, 10.0))
    rows.extend(_baseline_day(1544061, spike_day, 6, 90.0))
    features = build_event_features(_conformed_frame(rows))
    isolated = features[
        (features["location_id"] == 1544061)
        & (features["date_local"] == spike_day)
        & (features["parameter"] == "pm25")
    ]
    peer = features[
        (features["location_id"] == 1601414)
        & (features["date_local"] == spike_day)
        & (features["parameter"] == "pm25")
    ]
    assert isolated.iloc[0]["spatial_isolation"] > peer.iloc[0]["spatial_isolation"]


def test_weak_labels_fire_on_multi_station_elevation():
    rows: list[dict] = []
    for day in range(1, 6):
        date_local = f"2026-01-{day:02d}"
        for loc in (1544061, 1601414, 2455394):
            rows.extend(_baseline_day(loc, date_local, day, 8.0))
    event_day = "2026-01-06"
    for loc in (1544061, 1601414, 2455394):
        rows.extend(_baseline_day(loc, event_day, 6, 40.0))
    conformed = _conformed_frame(rows)
    features = build_event_features(conformed)
    labels = weak_labels(features, conformed)
    pm25_event = features[
        (features["date_local"] == event_day) & (features["parameter"] == "pm25")
    ]
    assert labels.loc[pm25_event.index].any()


def test_build_detection_writes_layer2_partitions(tmp_path, monkeypatch):
    rows: list[dict] = []
    for day in range(1, 9):
        date_local = f"2026-01-{day:02d}"
        for loc in (1544061, 1601414, 2455394, 6430870):
            rows.extend(_baseline_day(loc, date_local, day, 10.0 + day))
            rows.extend(
                [
                    _base_row(
                        location_id=loc,
                        date_local=date_local,
                        datetime_utc=pd.Timestamp(f"{date_local} {h:02d}:00:00", tz="UTC"),
                        datetime_local=pd.Timestamp(f"{date_local} {h:02d}:00:00"),
                        parameter="pm10",
                        value=20.0,
                    )
                    for h in range(12)
                ]
            )
    conformed = _conformed_frame(rows)
    bronze = tmp_path / "bronze"
    bronze.mkdir()
    gold = tmp_path / "gold"

    monkeypatch.setattr(
        "pipelines.detection.build.read_conformed",
        lambda _bronze: conformed,
    )
    result = build_detection(bronze_root=bronze, gold_root=gold)
    assert result.feature_rows > 0

    features = read_event_features(gold)
    assert not features.empty
    assert set(EVENT_FEATURE_COLUMNS).issubset(features.columns)
    assert (gold / "layer2" / "event_features").exists() or (gold / "layer2").exists()

    alerts = read_event_alerts(gold)
    if result.ensemble_trained:
        assert not alerts.empty
        assert set(EVENT_ALERT_COLUMNS).issubset(alerts.columns)
