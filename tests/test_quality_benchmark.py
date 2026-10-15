"""Controlled Layer 1 faults must exercise the actual bronze/silver/gold wiring."""

import json

import pandas as pd
import pytest

from pipelines import storage
from pipelines.conformance.conform import read_silver
from pipelines.quality.benchmark import (
    FAULT_RULES,
    TARGETS,
    run_quality_benchmark,
    summarize_run,
)
from pipelines.quality.rules import INCIDENT_COLUMNS


def test_quality_benchmark_detects_faults_and_counts_schema_reversion(tmp_path, monkeypatch):
    # Explicit isolated roots must win even if operational settings point at S3.
    for name in ("BRONZE_ROOT", "SILVER_ROOT", "GOLD_ROOT", "MODELS_ROOT"):
        monkeypatch.setenv(name, "s3://must-not-be-used/" + name.lower())
    monkeypatch.setattr(storage, "_wr", lambda: pytest.fail("Benchmark attempted AWS access"))
    output = tmp_path / "benchmark"
    report = run_quality_benchmark(str(output))
    assert json.loads((output / "quality-benchmark.json").read_text()) == report
    baseline = report["healthy_baseline"]
    assert baseline["input_reading_rows"] == 3024
    assert baseline["station_day_rows"] == 63
    assert baseline["model_trained"]
    assert baseline["rule_incident_rows"] == 0
    totals = report["totals"]
    assert totals["fault_cases"] == totals["scored_fault_cases"] == 18
    assert totals["all_expected_rules_detected_cases"] == 18
    assert totals["missed_expected_rule_cases"] == 0
    assert totals["control_station_day_occurrences"] == 360
    assert {item["scenario"] for item in report["scenarios"]} == set(FAULT_RULES)

    for scenario in report["scenarios"]:
        assert scenario["station_day_rows"] == 63
        assert len(scenario["targets"]) == 3
        assert all(target["scored"] for target in scenario["targets"])
        assert all(not target["missing_expected_rule_ids"] for target in scenario["targets"])
        assert all("R6" not in target["observed_rule_ids"] for target in scenario["targets"])
        # A regular day was copied exactly, rather than accumulated mutations.
        relative = "bronze/locationid=900001/year=2026/location-900001-20260101.csv.gz"
        assert (output / scenario["scenario"] / relative).read_bytes() == (
            output / "healthy_baseline" / relative
        ).read_bytes()
    schema = next(item for item in report["scenarios"] if item["scenario"] == "schema_drift")
    reversion_incidents = [item for item in schema["additional_control_incidents"]
                          if item["rule_id"] == "R8"]
    assert {(item["locationid"], item["date_local"]) for item in reversion_incidents} == {
        (900001, "2026-01-11"), (900002, "2026-01-15"), (900003, "2026-01-19"),
    }
    negatives = read_silver(output / "negative_readings" / "silver")
    assert (negatives["value"] < 0).sum() == 9
    units = read_silver(output / "incorrect_unit_label" / "silver")
    assert (units["original_unit"] == "ppm").sum() == 72


def test_unscored_faults_remain_misses_and_rules_are_distinct_from_model():
    location, day = TARGETS[0]
    metrics = pd.DataFrame([{"locationid": location, "date_local": day,
                             "model_is_incident": True}])
    incidents = pd.DataFrame([{"locationid": location, "date_local": day, "rule_id": "M1"}],
                             columns=INCIDENT_COLUMNS)
    summary = summarize_run(metrics, incidents, set(), ("R7",))
    assert len(summary["targets"]) == 3
    target = summary["targets"][0]
    assert target["scored"] and target["raw_model_flagged"]
    assert target["reported_model_incident"]
    assert not target["rule_detected"]
    assert target["missing_expected_rule_ids"] == ["R7"]
    assert not summary["targets"][1]["scored"]
    assert not summary["targets"][1]["rule_detected"]


@pytest.mark.parametrize("root", ["s3://bucket/quality-benchmark", "s3:/bucket/benchmark"])
def test_quality_benchmark_refuses_s3(root):
    with pytest.raises(ValueError, match="local"):
        run_quality_benchmark(root)


def test_quality_benchmark_refuses_to_overwrite_existing_files(tmp_path):
    evidence = tmp_path / "existing.txt"
    evidence.write_text("keep")
    with pytest.raises(ValueError, match="empty"):
        run_quality_benchmark(tmp_path)
    assert evidence.read_text() == "keep"
