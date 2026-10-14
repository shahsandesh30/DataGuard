import json

import numpy as np
import pandas as pd
import pytest

from pipelines.__main__ import main
from pipelines.detection.benchmark import (
    BENCHMARK_FILE,
    inject_spikes,
    injection_recall,
    out_of_sample_votes,
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
from pipelines.detection.features import FEATURE_MODEL_COLUMNS, build_event_features
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
    assert metrics.weak_label_base_rate == 0.0
    assert metrics.weak_label_lift is None

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


def test_temporal_holdout_scores_only_later_dates():
    features = build_event_features(_noisy_silver(days=16))
    results = temporal_holdout(features, holdout_fraction=0.25)

    assert [r.parameter for r in results] == ["pm25"]
    result = results[0]
    assert result.holdout_dates == 4
    assert result.train_rows + result.holdout_rows == len(features)
    for rate in (result.train_flag_rate, result.holdout_flag_rate, result.holdout_dbscan_rate):
        assert 0.0 <= rate <= 1.0
    assert result.holdout_weak_label_lift is None  # no weak labels supplied


def test_temporal_holdout_skips_parameters_without_enough_history():
    features = build_event_features(_noisy_silver(days=5))
    assert temporal_holdout(features) == []


def test_injection_recall_catches_large_local_spikes():
    result = injection_recall(
        _noisy_silver(days=16), n_local=4, n_regional=0, magnitude=5.0, min_history_days=7
    )
    assert result.injected_scored == 4
    assert result.recall_regional is None
    assert result.recall is not None and result.recall >= 0.75
    assert result.recall > result.false_alarm_rate


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


def test_tuned_dbscan_noise_stays_within_contamination():
    features = build_event_features(_noisy_silver(days=16))
    models = fit_ensemble(features, contamination=0.1)

    noise_rate = float(np.mean(models["dbscan_labels"] == -1))
    assert models["dbscan"].eps > 0
    assert noise_rate <= 0.1 + 1e-9


def test_tune_dbscan_eps_handles_identical_rows():
    assert tune_dbscan_eps(np.zeros((10, 3)), min_samples=3, contamination=0.1) > 0
