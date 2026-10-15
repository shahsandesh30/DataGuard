"""Explanations must preserve the saved decision and scope evidence to one alert."""

import copy
import json
from pathlib import Path

import pandas as pd
import pytest
import streamlit as st
from streamlit.testing.v1 import AppTest

from dashboard.data import load_stations
from dashboard.explanation import explain_alert
from pipelines import storage
from pipelines.conformance.conform import SILVER_COLUMNS, write_silver
from pipelines.fusion.trust_score import fuse


@pytest.fixture
def evidence_frames():
    alerts = pd.DataFrame(
        [
            {
                "locationid": location,
                "date_local": day,
                "parameter": parameter,
                "alert_score": 0.8,
                "if_flag": True,
                "lof_flag": True,
                "dbscan_flag": False,
                "agreement_count": 2,
                "weak_label": True,
                "rank": 1,
                "feature_snapshot": json.dumps(
                    {
                        "daily_mean": mean,
                        "daily_max": mean + 2,
                        "z_score": 3.1,
                        "regional_agreement": 0.5,
                    }
                ),
            }
            for location, day, parameter, mean in [
                (100, "2026-01-01", "pm25", 10),
                (100, "2026-01-01", "pm1", 50),
                (100, "2026-01-02", "pm25", 12),
                (200, "2026-01-01", "pm25", 14),
            ]
        ]
    )
    incidents = pd.DataFrame(
        [
            {
                "locationid": 100,
                "date_local": "2026-01-01",
                "rule_id": "R2",
                "severity": "high",
                "source": "rule",
                "metric_snapshot": json.dumps({"max_stuck_run_max": 6}),
            },
            {
                "locationid": 100,
                "date_local": "2026-01-01",
                "rule_id": "R9",
                "severity": "low",
                "source": "rule",
                "metric_snapshot": json.dumps({"duplicate_rate": 0.1}),
            },
            {
                "locationid": 200,
                "date_local": "2026-01-03",
                "rule_id": "R7",
                "severity": "high",
                "source": "rule",
                "metric_snapshot": json.dumps({"file_present": False}),
            },
        ]
    )
    rows = []
    for alert in alerts.itertuples():
        for hour in range(6):
            local = pd.Timestamp(f"{alert.date_local}T{hour:02d}:00:00")
            rows.append(
                {
                    "locationid": alert.locationid,
                    "sensor_id": alert.locationid * 10,
                    "location_name": f"Station-{alert.locationid}",
                    "datetime": local.tz_localize("Australia/Sydney").tz_convert("UTC"),
                    "datetime_local": local,
                    "date_local": alert.date_local,
                    "latitude": -33.8,
                    "longitude": 151.2,
                    "parameter": alert.parameter,
                    "value": 50 if alert.parameter == "pm1" else (-2 if hour == 0 else 10),
                    "unit": "µg/m³",
                    "original_unit": "mg/m³",
                }
            )
    measurements = pd.DataFrame(rows, columns=SILVER_COLUMNS)
    return fuse(incidents, alerts), incidents, measurements, alerts


def _explain(frames, location=100, day="2026-01-01", parameter="pm25", **kwargs):
    fused, incidents, measurements, _ = frames
    return explain_alert(
        fused,
        incidents,
        measurements,
        locationid=location,
        date_local=day,
        parameter=parameter,
        **kwargs,
    )


def test_explainer_scopes_keys_and_shows_saved_highest_severity_without_stacking(evidence_frames):
    originals = [frame.copy(deep=True) for frame in evidence_frames]
    report = _explain(evidence_frames)
    decision = report["decision"]
    assert decision["status"] == "quarantined"
    assert decision["policy_penalty"] == 0.7
    assert decision["trust_score"] == pytest.approx(0.24)
    assert decision["calculation_verified"]
    assert decision["human_review"] == "Not recorded in this dataset"
    assert [vote["flagged"] for vote in report["detector_votes"]] == [True, True, False]
    assert report["features"]["daily_mean"] == 10
    assert {row["rule_id"] for row in report["quality_incidents"]} == {"R2", "R9"}
    assert "threshold: 6" in report["quality_incidents"][0]["evidence"]
    assert min(row["value"] for row in report["readings"]) == -2
    assert {row["original_unit"] for row in report["readings"]} == {"mg/m³"}
    assert len(report["readings"]) == 6
    assert not report["warnings"]
    assert "weak_label" not in json.dumps(report, allow_nan=False)
    # Quality is station-day scoped even when the selected event is another pollutant.
    pm1 = _explain(evidence_frames, parameter="pm1")
    assert pm1["features"]["daily_mean"] == 50
    assert all(row["value"] == 50 for row in pm1["readings"])
    assert pm1["decision"]["status"] == "quarantined"
    for original, current in zip(originals, evidence_frames, strict=True):
        pd.testing.assert_frame_equal(original, current)


def test_escalated_alert_does_not_pick_up_other_station_or_date_incidents(evidence_frames):
    for location, day in ((100, "2026-01-02"), (200, "2026-01-01")):
        report = _explain(evidence_frames, location, day)
        assert report["decision"]["status"] == "escalated"
        assert report["decision"]["policy_penalty"] == 0
        assert report["decision"]["trust_score"] == 0.8
        assert not report["quality_incidents"]
        assert not report["warnings"]


def test_missing_incident_rows_do_not_turn_quarantine_into_healthy(evidence_frames):
    fused, incidents, silver, alerts = evidence_frames
    report = _explain((fused, incidents.iloc[:0], silver, alerts))
    assert report["decision"]["status"] == "quarantined"
    assert report["decision"]["saved_rule_ids"] == ["R2", "R9"]
    assert not report["quality_incidents"]
    assert any("do not match" in warning for warning in report["warnings"])


def test_history_uses_seven_prior_local_dates_and_the_selected_station_and_pollutant(
    evidence_frames,
):
    fused, incidents, silver, alerts = evidence_frames
    template = silver[
        (silver["locationid"] == 100)
        & (silver["date_local"] == "2026-01-01")
        & (silver["parameter"] == "pm25")
    ]
    additions = []
    # The first is outside the window; the last is after the selected alert day.
    for day in ("2025-12-25", "2025-12-26", "2025-12-31", "2026-01-03"):
        rows = template.copy()
        rows["date_local"] = day
        rows["datetime_local"] = pd.date_range(day, periods=len(rows), freq="h")
        rows["datetime"] = (
            rows["datetime_local"].dt.tz_localize("Australia/Sydney").dt.tz_convert("UTC")
        )
        additions.append(rows)
    measurements = pd.concat([silver, *additions], ignore_index=True)
    original = measurements.copy(deep=True)
    report = _explain((fused, incidents, measurements, alerts), day="2026-01-02")
    assert report["history_window"] == {
        "start_local_date": "2025-12-26",
        "end_local_date": "2026-01-01",
        "requested_days": 7,
        "available_local_dates": ["2025-12-26", "2025-12-31", "2026-01-01"],
    }
    assert len(report["context_readings"]) == 18
    assert {row["sensor_id"] for row in report["context_readings"]} == {1000}
    assert {row["value"] for row in report["context_readings"]} == {-2, 10}
    assert {row["date_local"] for row in report["readings"]} == {"2026-01-02"}
    # Sydney's first local hour belongs to the preceding UTC date.
    assert report["context_readings"][0]["datetime"].startswith("2025-12-25T13:00")
    assert report["decision"]["status"] == "escalated"
    json.dumps(report, allow_nan=False)
    pd.testing.assert_frame_equal(measurements, original)


@pytest.mark.parametrize("bad_score", [0.5, None, float("nan"), -1])
def test_bad_saved_scores_never_receive_a_verified_calculation(evidence_frames, bad_score):
    frames = copy.deepcopy(evidence_frames)
    frames[0].loc[0, "trust_score"] = bad_score
    report = _explain(frames)
    assert not report["decision"]["calculation_verified"]
    assert any("cannot be reconciled" in warning for warning in report["warnings"])
    json.dumps(report, allow_nan=False)


def test_zero_score_and_unknown_legacy_fields_are_not_invented(evidence_frames):
    frames = copy.deepcopy(evidence_frames)
    frames[0].loc[0, ["alert_score", "trust_score"]] = 0
    assert _explain(frames)["decision"]["calculation_verified"]
    frames = (
        frames[0].drop(
            columns=[
                "if_flag",
                "lof_flag",
                "dbscan_flag",
                "feature_snapshot",
                "has_quality_incident",
                "max_severity",
            ]
        ),
        frames[1],
        frames[2].iloc[:0],
        frames[3],
    )
    report = _explain(frames)
    assert all(vote["flagged"] is None for vote in report["detector_votes"])
    assert report["features"] == {}
    assert report["decision"]["policy_penalty"] is None
    assert not report["decision"]["calculation_verified"]
    assert report["readings"] == []
    assert report["context_readings"] == []
    assert report["history_window"]["requested_days"] == 7
    assert any("snapshot is unavailable" in warning for warning in report["warnings"])


def test_malformed_snapshots_and_nonfinite_values_remain_unavailable(evidence_frames):
    frames = copy.deepcopy(evidence_frames)
    frames[0].loc[0, "feature_snapshot"] = '{"daily_mean":NaN,"daily_max":Infinity}'
    frames[1].loc[0, "metric_snapshot"] = "[broken"
    report = _explain(frames)
    assert report["features"] == {"daily_mean": None, "daily_max": None}
    assert report["quality_incidents"][0]["metric_snapshot"] == {}
    assert report["quality_incidents"][0]["evidence"] == "Saved metric evidence is unavailable."
    json.dumps(report, allow_nan=False)


def test_mixed_builds_and_detector_vote_mismatch_are_visible(evidence_frames):
    frames = copy.deepcopy(evidence_frames)
    frames[0].loc[0, "agreement_count"] = 1
    report = _explain(
        frames,
        summaries={
            "layer1": {"built_at_utc": "2026-01-04T01:00:00Z", "model_path": "private"},
            "fusion": {"built_at_utc": "2026-01-03T01:00:00Z"},
        },
    )
    assert any("newer than fusion" in warning for warning in report["warnings"])
    assert any("agreement count" in warning for warning in report["warnings"])
    assert "model_path" not in json.dumps(report)


def test_ambiguous_or_absent_alert_is_not_arbitrarily_selected(evidence_frames):
    with pytest.raises(ValueError, match="exactly one"):
        _explain(evidence_frames, parameter="o3")
    fused, incidents, silver, alerts = evidence_frames
    with pytest.raises(ValueError, match="exactly one"):
        _explain((pd.concat([fused, fused]), incidents, silver, alerts))


def test_station_metadata_reuses_loaded_measurements(evidence_frames, monkeypatch):
    monkeypatch.setattr(
        "dashboard.data.read_silver", lambda _: pytest.fail("Duplicate silver read")
    )
    stations = load_stations("unused", measurements=evidence_frames[2])
    assert set(stations["locationid"]) == {100, 200}


@pytest.fixture
def dashboard_app(tmp_path, evidence_frames, monkeypatch):
    fused, incidents, silver, alerts = evidence_frames
    silver_root, gold = tmp_path / "silver", tmp_path / "gold"
    write_silver(silver, silver_root)
    storage.write_parquet(fused, gold, "fusion/trust_alerts")
    storage.write_parquet(alerts, gold, "layer2/event_alerts")
    storage.write_parquet(incidents, gold, "layer1/quality_incidents")
    metrics = incidents[["locationid", "date_local"]].drop_duplicates().copy()
    storage.write_parquet(metrics, gold, "layer1/quality_metrics")
    for name, stamp in (
        ("layer1/_build.json", "2026-01-04T01:00:00Z"),
        ("layer2/_detection_build.json", "2026-01-04T02:00:00Z"),
        ("fusion/_build.json", "2026-01-04T03:00:00Z"),
    ):
        storage.write_text(storage.join(gold, name), json.dumps({"built_at_utc": stamp}))
    for name, value in {
        "SILVER_ROOT": silver_root,
        "GOLD_ROOT": gold,
        "BRONZE_ROOT": tmp_path / "bronze",
        "MODELS_ROOT": tmp_path / "models",
        "DATAGUARD_PUBLIC_DASHBOARD": "true",
    }.items():
        monkeypatch.setenv(name, str(value))
    monkeypatch.setattr(storage, "_wr", lambda: pytest.fail("Unexpected AWS access"))
    monkeypatch.setattr(
        storage, "write_parquet", lambda *a, **k: pytest.fail("Dashboard wrote gold")
    )
    monkeypatch.setattr(storage, "write_text", lambda *a, **k: pytest.fail("Dashboard wrote text"))
    st.cache_data.clear()
    app = AppTest.from_file(
        str(Path(__file__).resolve().parents[1] / "dashboard/app.py"), default_timeout=25
    )
    app.secrets = {}
    app.run()
    assert not app.exception
    yield app
    st.cache_data.clear()


def test_streamlit_explainer_respects_filters_and_resets_dependent_selections(dashboard_app):
    app = dashboard_app
    assert any("No alerts match this local date" in info.value for info in app.info)
    app.sidebar.selectbox[0].select("2026-01-01").run()
    assert not app.exception
    assert app.selectbox(key="explain_station").value == 100
    assert any("Held for review" in warning.value for warning in app.warning)
    assert next(metric for metric in app.metric if metric.label == "Policy penalty").value == "70%"
    app.selectbox(key="explain_parameter").select("pm25").run()
    assert not app.exception
    assert "0.240000" in app.code[0].value
    assert app.get("download_button")[0].label == "Download this alert's evidence"
    app.selectbox(key="explain_parameter").select("pm1").run()
    app.selectbox(key="explain_station").select(200).run()
    assert not app.exception
    assert app.selectbox(key="explain_parameter").value == "pm25"
    assert any(expander.label == "Previous seven days of readings" for expander in app.expander)
    assert next(metric for metric in app.metric if metric.label == "Policy penalty").value == "0%"
    assert any("Saved status: escalated" in info.value for info in app.info)
    # Expand the date scope, then constrain it again while a different station was selected.
    app.checkbox(key="show_all_dates").check().run()
    app.sidebar.selectbox[0].select("2026-01-02").run()
    app.checkbox(key="show_all_dates").uncheck().run()
    assert not app.exception
    assert app.selectbox(key="explain_station").value == 100
    assert app.selectbox(key="explain_day").value == "2026-01-02"
    assert app.selectbox(key="explain_parameter").value == "pm25"
    assert any("1/7 dates have readings" in caption.value for caption in app.caption)
    # Public mode never offers arbitrary data-root editing.
    assert len(app.text_input) == 0


def test_streamlit_missing_supporting_evidence_is_a_warning_not_a_crash(evidence_frames):
    fused, incidents, silver, _ = evidence_frames
    one = fused.iloc[:1].drop(columns=["feature_snapshot", "if_flag", "lof_flag", "dbscan_flag"])
    script = """
import pandas as pd
from dashboard.alert_view import render_alert_explorer
render_alert_explorer(pd.DataFrame(ALERTS), pd.DataFrame(), pd.DataFrame(), {})
"""
    records = json.loads(one.to_json(orient="records"))
    app = AppTest.from_string("ALERTS = " + repr(records) + "\n" + script, default_timeout=15).run()
    assert not app.exception
    assert any("do not match" in warning.value for warning in app.warning)
    assert any("snapshot is unavailable" in warning.value for warning in app.warning)
    votes = app.dataframe[0].value
    assert set(votes["Vote"]) == {"Unavailable"}
    assert any("preceding seven local dates" in info.value for info in app.info)
