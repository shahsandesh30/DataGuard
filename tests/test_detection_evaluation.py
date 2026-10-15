import json

import numpy as np
import pandas as pd
import pytest

from pipelines import storage
from pipelines.__main__ import main
from pipelines.detection.benchmark import (
    BENCHMARK_FILE,
    _dataset_summary,
    inject_spikes,
    injection_recall,
    out_of_sample_votes,
    temporal_feature_split,
    temporal_holdout,
)
from pipelines.detection.ensemble import _feature_matrix, fit_ensemble, tune_dbscan_eps
from pipelines.detection.evaluation import (
    RUN_METRIC_COLUMNS,
    compute_run_metrics,
    read_latest_run,
    read_run_history,
    save_run_metrics,
)
from pipelines.detection.features import (
    FEATURE_MODEL_COLUMNS,
    _add_baseline_deviation,
    build_event_features,
    build_hourly_event_features,
)
from tests.test_detection import _base_row, _silver_frame

STATIONS = (1544061, 1601414, 2455394, 6430870)


def _noisy_silver(days: int = 14, seed: int = 0) -> pd.DataFrame:
    """Hourly pm25 for four stations with realistic noise, so baselines are not flat."""
    rng = np.random.default_rng(seed)
    rows: list[dict] = []
    for day in range(1, days + 1):
        date_local = f"2026-01-{day:02d}"
        for loc in STATIONS:
            for h in range(24):
                local = pd.Timestamp(f"{date_local} {h:02d}:00:00")
                rows.append(
                    _base_row(
                        locationid=loc,
                        date_local=date_local,
                        datetime=local.tz_localize("UTC") - pd.Timedelta(hours=11),
                        datetime_local=local,
                        value=float(10.0 + rng.normal(0, 1.5)),
                    )
                )
    return _silver_frame(rows)


def _features(n: int, parameter: str = "pm25") -> pd.DataFrame:
    return pd.DataFrame(
        {
            "locationid": range(n),
            "date_local": "2026-01-01",
            "parameter": parameter,
        }
    )


def _alerts(ranks: list[int], weak: list[bool]) -> pd.DataFrame:
    return pd.DataFrame(
        {
            "rank": ranks,
            "weak_label": weak,
            "agreement_count": [1] * len(ranks),
            "alert_score": [0.5] * len(ranks),
        }
    )


def test_run_metrics_report_base_rate_lift_and_precision_at_k():
    features = _features(10)
    labels = pd.Series([True] * 2 + [False] * 8, index=features.index)
    alerts = _alerts(ranks=[1, 2, 3, 4], weak=[True, True, False, False])

    metrics = compute_run_metrics(
        "pm25", features, alerts, ensemble_trained=True, weak_label=labels, k=2
    )

    assert metrics.flag_rate == pytest.approx(0.4)
    assert metrics.weak_label_agreement_rate == pytest.approx(0.5)
    assert metrics.weak_label_base_rate == pytest.approx(0.2)
    assert metrics.weak_label_lift == pytest.approx(2.5)
    assert metrics.precision_at_k == pytest.approx(1.0)


def test_run_metrics_leave_lift_unknown_without_a_base_rate():
    features = _features(5, parameter="pm1")
    no_labels = pd.Series(False, index=features.index)
    alerts = _alerts(ranks=[1], weak=[False])

    metrics = compute_run_metrics(
        "pm1", features, alerts, ensemble_trained=True, weak_label=no_labels
    )
    assert metrics.weak_label_base_rate is None
    assert metrics.weak_label_lift is None
    assert metrics.weak_label_agreement_rate is None
    assert metrics.precision_at_k is None

    unlabelled = compute_run_metrics("pm1", features, alerts, ensemble_trained=True)
    assert unlabelled.weak_label_base_rate is None
    assert unlabelled.weak_label_lift is None


def test_run_metrics_with_no_alerts_are_none_not_zero():
    metrics = compute_run_metrics(
        "pm25", _features(5), pd.DataFrame(), ensemble_trained=False
    )
    assert metrics.alert_rows == 0
    assert metrics.flag_rate == 0.0
    assert metrics.weak_label_agreement_rate is None
    assert metrics.precision_at_k is None
    assert metrics.avg_alert_score is None


@pytest.mark.parametrize("as_str", [False, True])
def test_run_history_appends_and_reads_newest_first(tmp_path, as_str):
    layer2 = tmp_path / "gold" / "layer2"
    gold = str(tmp_path / "gold") if as_str else tmp_path / "gold"
    target = str(layer2) if as_str else layer2
    features = _features(4)
    for run_at in ("2026-01-01T00:00:00+00:00", "2026-01-02T00:00:00+00:00"):
        save_run_metrics(
            compute_run_metrics(
                "pm25", features, _alerts([1], [True]), ensemble_trained=True, run_at=run_at
            ),
            target,
        )

    history = read_run_history(gold, parameter="pm25")
    assert len(history) == 2
    assert set(RUN_METRIC_COLUMNS).issubset(history.columns)
    assert history.iloc[0]["run_at"] == "2026-01-02T00:00:00+00:00"
    assert read_latest_run(gold)["run_at"] == "2026-01-02T00:00:00+00:00"
    assert read_latest_run(tmp_path / "empty") is None


def test_inject_spikes_only_raises_target_hours_after_the_history_window():
    silver = _noisy_silver()
    original = silver.copy()
    injected, truth = inject_spikes(silver, n_local=3, n_regional=1, min_history_days=7)

    pd.testing.assert_frame_equal(silver, original)  # input untouched
    assert (truth["kind"] == "regional").sum() == len(STATIONS)
    assert (truth["kind"] == "local").sum() == 3
    assert (truth["date_local"] >= "2026-01-08").all()

    changed = injected["value"] != original["value"]
    assert changed.any()
    assert (injected.loc[changed, "value"] > original.loc[changed, "value"]).all()
    hours = injected.loc[changed, "datetime_local"].dt.hour
    assert hours.between(10, 17).all()
    spiked = injected.loc[changed]
    changed_days = set(zip(spiked["locationid"], spiked["date_local"], strict=True))
    assert changed_days == set(zip(truth["locationid"], truth["date_local"], strict=True))


def test_out_of_sample_votes_flag_far_rows_and_accept_training_rows():
    features = build_event_features(_noisy_silver())
    models = fit_ensemble(features)
    models["train_scaled"] = models["scaler"].transform(_feature_matrix(features))

    far = features.head(1).copy()
    far[FEATURE_MODEL_COLUMNS] = far[FEATURE_MODEL_COLUMNS].fillna(0.0) + 1_000.0
    votes = out_of_sample_votes(models, far)
    assert votes.loc[far.index[0], "dbscan_flag"]
    assert votes.loc[far.index[0], "agreement_count"] >= 2

    core_index = models["dbscan"].core_sample_indices_
    if len(core_index):
        core_row = features.iloc[[core_index[0]]]
        assert not out_of_sample_votes(models, core_row)["dbscan_flag"].iloc[0]


@pytest.mark.parametrize("parameter", ["pm25", "pm1"])
def test_temporal_holdout_scores_only_later_dates(parameter):
    silver = _noisy_silver(days=16)
    silver["parameter"] = parameter
    results = temporal_holdout(silver, holdout_fraction=0.25)

    assert [r.parameter for r in results] == [parameter]
    result = results[0]
    assert result.holdout_dates == 4
    assert result.train_rows + result.holdout_rows == 16 * len(STATIONS)
    assert result.train_date_max == "2026-01-12"
    assert result.holdout_date_min == "2026-01-13"
    for rate in (result.train_flag_rate, result.holdout_flag_rate, result.holdout_dbscan_rate):
        assert 0.0 <= rate <= 1.0
    assert result.holdout_weak_label_lift is None  # no weak labels supplied
    if parameter != "pm25":
        assert result.holdout_weak_label_precision is None
        assert result.holdout_weak_label_base_rate is None


def test_temporal_holdout_skips_parameters_without_enough_history():
    skipped = []
    assert temporal_holdout(_noisy_silver(days=5), skipped=skipped) == []
    assert skipped[0]["parameter"] == "pm25"
    assert skipped[0]["train_rows"] < 20


def test_injection_recall_catches_large_local_spikes():
    result = injection_recall(
        _noisy_silver(days=16), n_local=4, n_regional=0, magnitude=5.0, min_history_days=7
    )
    assert result.injected_scored == 4
    assert result.recall_regional is None
    assert result.recall is not None and result.recall >= 0.75
    assert result.recall > result.untouched_alert_rate
    assert result.scored_by_kind == {"local": 4, "regional": 0}
    assert result.skipped["unscored"] == 0


def test_evaluate_command_writes_benchmark_report(tmp_path, monkeypatch):
    silver = _noisy_silver(days=16)
    monkeypatch.setattr("pipelines.detection.benchmark.read_silver", lambda _root: silver)

    gold = tmp_path / "gold"
    code = main(["evaluate", "--silver-root", str(tmp_path / "silver"), "--gold-root", str(gold)])

    assert code == 0
    report = json.loads((gold / "layer2" / BENCHMARK_FILE).read_text(encoding="utf-8"))
    assert report["input_measurement_rows"] == len(silver)
    assert report["holdout"][0]["parameter"] == "pm25"
    assert report["injection"]["parameter"] == "pm25"
    assert report["methodology_version"] == 2
    assert report["configuration"]["minimum_prior_observed_days"] == 7
    assert len(report["dataset"]["sha256"]) == 64
    assert report["dataset"]["rows"] == len(silver)
    assert "untouched_alert_rate" in report["injection"]
    assert "false_alarm_rate" not in report["injection"]
    assert "holdout_skipped" in report


def test_tuned_dbscan_noise_stays_within_contamination():
    features = build_event_features(_noisy_silver(days=16))
    models = fit_ensemble(features, contamination=0.1)

    noise_rate = float(np.mean(models["dbscan_labels"] == -1))
    assert models["dbscan"].eps > 0
    assert noise_rate <= 0.1 + 1e-9


def test_tune_dbscan_eps_handles_identical_rows():
    assert tune_dbscan_eps(np.zeros((10, 3)), min_samples=3, contamination=0.1) > 0


def test_hourly_reference_statistics_use_only_training_readings():
    readings = pd.DataFrame({
        "locationid": [1544061] * 20,
        "parameter": ["pm25"] * 20,
        "hour": [12] * 20,
        "is_weekend": [False] * 20,
        "season": [1] * 20,
        "value": np.arange(20, dtype=float),
    })
    frozen = _add_baseline_deviation(readings, reference=readings.iloc[:15])
    assert frozen["baseline_median"].eq(7.0).all()
    assert frozen["baseline_n"].eq(15).all()


def test_future_readings_cannot_change_temporal_training_features():
    silver = _noisy_silver(days=16)
    cutoff = "2026-01-13"
    train, holdout = temporal_feature_split(silver, cutoff)
    changed = silver.copy()
    changed.loc[changed["date_local"] >= cutoff, "value"] += 1_000
    changed_train, changed_holdout = temporal_feature_split(changed, cutoff)

    pd.testing.assert_frame_equal(train, changed_train)
    assert (train["date_local"] < cutoff).all()
    assert (holdout["date_local"] >= cutoff).all()
    assert changed_holdout["hourly_deviation_zscore_max"].max() > (
        holdout["hourly_deviation_zscore_max"].max() + 100
    )


@pytest.mark.parametrize("case", ["short_history", "missing_hours", "nonfinite_values"])
def test_spike_truth_excludes_cases_without_real_eligible_changes(case):
    silver = _noisy_silver(days=5 if case == "short_history" else 14)
    in_window = silver["datetime_local"].dt.hour.between(10, 17)
    if case == "missing_hours":
        silver = silver[~in_window].copy()
    elif case == "nonfinite_values":
        silver.loc[in_window, "value"] = np.nan
    original = silver.copy()
    injected, truth = inject_spikes(silver)

    pd.testing.assert_frame_equal(injected, original)
    assert truth.empty
    reason = "insufficient_history" if case == "short_history" else "no_window_readings"
    assert truth.attrs["skipped"][reason] > 0


def test_regional_injection_requires_multiple_eligible_stations():
    silver = _noisy_silver()
    silver = silver[silver["locationid"] == STATIONS[0]]
    injected, truth = inject_spikes(silver, n_local=0, n_regional=1)
    assert truth.empty
    assert truth.attrs["skipped"]["regional_date_shortfall"] == 1
    pd.testing.assert_frame_equal(injected, silver)


@pytest.mark.parametrize("parameter", ["pm1", "um003"])
def test_unsupported_pollutant_weak_metrics_are_unavailable(parameter):
    metrics = compute_run_metrics(
        parameter, _features(4, parameter), _alerts([1], [True]),
        ensemble_trained=True, weak_label=pd.Series([True] * 4),
    )
    assert metrics.weak_label_agreement_rate is None
    assert metrics.weak_label_base_rate is None
    assert metrics.weak_label_lift is None
    assert metrics.precision_at_k is None


def test_pm25_zero_weak_label_base_rate_is_still_a_measured_zero():
    metrics = compute_run_metrics(
        "pm25", _features(4), _alerts([1], [False]),
        ensemble_trained=True, weak_label=pd.Series([False] * 4),
    )
    assert metrics.weak_label_base_rate == 0.0
    assert metrics.precision_at_k == 0.0
    assert metrics.weak_label_lift is None


def test_legacy_unsupported_run_metrics_are_masked_without_rewriting(tmp_path):
    legacy = pd.DataFrame([{
        "parameter": "pm1", "run_at": "2026-01-01T00:00:00+00:00",
        "weak_label_agreement_rate": 0.0, "weak_label_base_rate": 0.0,
        "weak_label_lift": 0.0, "precision_at_k": 0.0,
    }])
    layer2 = tmp_path / "gold" / "layer2"
    storage.write_model_parquet(legacy, layer2)
    loaded = read_run_history(tmp_path / "gold")
    for column in ("weak_label_agreement_rate", "weak_label_base_rate",
                   "weak_label_lift", "precision_at_k"):
        assert pd.isna(loaded.iloc[0][column])
    assert storage.read_parquet(layer2, "model_runs").iloc[0]["precision_at_k"] == 0.0


def test_dataset_fingerprint_is_order_independent_and_sensitive_to_readings():
    silver = _noisy_silver(days=1)
    fingerprint = _dataset_summary(silver)["sha256"]
    shuffled = silver.sample(frac=1, random_state=42).reset_index(drop=True)
    assert _dataset_summary(shuffled)["sha256"] == fingerprint
    shuffled.loc[0, "value"] += 1.0
    assert _dataset_summary(shuffled)["sha256"] != fingerprint


def test_hourly_reference_excludes_stations_without_training_history():
    silver = _noisy_silver(days=5)
    reference = silver[silver["locationid"] == STATIONS[0]]
    hourly = build_hourly_event_features(silver, baseline_reference=reference)
    assert set(hourly["locationid"]) == {STATIONS[0]}
    assert len(hourly) == len(reference)


def test_saved_unavailable_metrics_keep_numeric_parquet_columns(tmp_path):
    metrics = compute_run_metrics(
        "um003", _features(4, "um003"), _alerts([1], [False]), ensemble_trained=True,
    )
    layer2 = tmp_path / "layer2"
    save_run_metrics(metrics, layer2)
    stored = storage.read_parquet(layer2, "model_runs")
    for column in ("weak_label_agreement_rate", "weak_label_base_rate",
                   "weak_label_lift", "precision_at_k"):
        assert pd.api.types.is_float_dtype(stored[column])
        assert stored[column].isna().all()
