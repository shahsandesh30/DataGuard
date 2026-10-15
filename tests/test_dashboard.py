import json

import pandas as pd

from dashboard.data import (
    build_station_status,
    filter_by_date,
    last_run_models,
    latest_build_time,
    latest_model_runs,
    load_benchmark,
    load_build_summaries,
    load_model_runs,
    load_stations,
)
from dashboard.runtime import apply_runtime_secrets, is_public_dashboard
from pipelines import storage
from pipelines.detection.evaluation import compute_run_metrics, save_run_metrics


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


def test_last_run_models_lists_trained_and_skipped_models():
    summaries = {
        "layer1": {
            "built_at_utc": "2026-01-08T01:00:00+00:00",
            "model_trained": True,
            "model_path": "s3://b/models/layer1/history/run=20260108T010000Z/isolation_forest.joblib",
        },
        "layer2": {
            "built_at_utc": "2026-01-08T01:05:00+00:00",
            "parameters_trained": ["pm25"],
            "parameters_skipped": ["pm1"],
            "model_paths": {
                "pm25": "models\\layer2\\history\\run=20260108T010500Z\\ensemble_pm25.joblib"
            },
        },
    }

    models = last_run_models(summaries).set_index("model")

    assert list(models.index) == ["isolation_forest", "ensemble_pm25", "ensemble_pm1"]
    assert models.loc["isolation_forest", "run_id"] == "20260108T010000Z"
    # Windows separators from a local run must not leak into the run id.
    assert models.loc["ensemble_pm25", "run_id"] == "20260108T010500Z"
    assert not models.loc["ensemble_pm1", "trained"]
    assert models.loc["ensemble_pm1", "run_id"] == ""


def test_last_run_models_empty_before_any_build():
    assert last_run_models({}).empty


def _features(n: int) -> pd.DataFrame:
    return pd.DataFrame({"x": range(n)})


def test_model_runs_history_and_latest_per_parameter(tmp_path):
    layer2 = storage.join(tmp_path / "gold", "layer2")
    alerts = pd.DataFrame(
        {"weak_label": [True, False], "rank": [1, 2], "agreement_count": [2, 1],
         "alert_score": [0.9, 0.4]}
    )
    for run_at, parameter in [
        ("2026-01-01T00:00:00+00:00", "pm25"),
        ("2026-01-02T00:00:00+00:00", "pm25"),
        ("2026-01-02T00:00:00+00:00", "pm1"),
    ]:
        metrics = compute_run_metrics(
            parameter, _features(10), alerts, ensemble_trained=True, run_at=run_at
        )
        save_run_metrics(metrics, layer2)

    history = load_model_runs(tmp_path / "gold")
    assert len(history) == 3
    assert history["run_at"].is_monotonic_increasing
    # No weak labels were given, so lift is undefined but still numeric.
    assert pd.api.types.is_float_dtype(history["weak_label_lift"])

    latest = latest_model_runs(history)
    assert list(latest["parameter"]) == ["pm1", "pm25"]
    assert latest.set_index("parameter").loc["pm25", "run_at"].day == 2


def test_model_runs_empty_without_history(tmp_path):
    assert load_model_runs(tmp_path / "gold").empty
    assert latest_model_runs(load_model_runs(tmp_path / "gold")).empty


def test_load_benchmark_reads_the_evaluate_report(tmp_path):
    gold = tmp_path / "gold"
    assert load_benchmark(gold) is None

    storage.write_text(
        storage.join(gold, "layer2/_detection_benchmark.json"),
        json.dumps({"built_at_utc": "2026-01-08T00:00:00+00:00", "holdout": []}),
    )
    assert load_benchmark(gold)["built_at_utc"] == "2026-01-08T00:00:00+00:00"


def test_load_benchmark_maps_legacy_untouched_rate_without_rewriting(tmp_path):
    path = storage.join(tmp_path, "layer2/_detection_benchmark.json")
    original = json.dumps({"injection": {"false_alarm_rate": 0.2}})
    storage.write_text(path, original)

    report = load_benchmark(tmp_path)
    assert report["injection"]["untouched_alert_rate"] == 0.2
    assert storage.read_text(path) == original


def test_load_benchmark_prefers_corrected_metric_when_both_keys_exist(tmp_path):
    path = storage.join(tmp_path, "layer2/_detection_benchmark.json")
    storage.write_text(path, json.dumps({
        "methodology_version": 2,
        "injection": {"untouched_alert_rate": 0.15, "false_alarm_rate": 0.9},
    }))
    assert load_benchmark(tmp_path)["injection"]["untouched_alert_rate"] == 0.15

