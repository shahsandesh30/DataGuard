"""Fusion counts and human evidence must not be mistaken for verified accuracy."""

import copy
import gzip
import json
from datetime import date, timedelta
from pathlib import Path
from types import SimpleNamespace

import pandas as pd
import pytest

from pipelines import storage
from pipelines.conformance.conform import SILVER_COLUMNS, write_silver
from pipelines.fusion.evaluation import controlled_policy_checks, describe_fusion, run_evaluation
from pipelines.fusion.review import main as summarize_main
from pipelines.fusion.review import prepare_review, review_html, summarize_reviews
from pipelines.fusion.trust_score import fuse
from pipelines.ingestion.fetch import bronze_key
from pipelines.quality.benchmark import FAULT_RULES, LOCATIONS, TARGETS
from pipelines.quality.rules import INCIDENT_COLUMNS


def _alert(location=100, day="2026-01-10", parameter="pm25"):
    return {"locationid": location, "date_local": day, "parameter": parameter,
            "alert_score": .8, "weak_label": True, "rank": 1}


def _incident(location=100, day="2026-01-10", rule="R1", severity="high"):
    return {"locationid": location, "date_local": day, "rule_id": rule,
            "severity": severity, "is_incident": True, "source": "rule"}


@pytest.fixture
def silver():
    rows = []
    for location in (100, 101, 102, 103):
        for index in range(11):
            day = date(2026, 1, 1) + timedelta(days=index)
            for hour in range(24):
                stamp = pd.Timestamp(f"{day}T{hour:02d}:00:00+11:00")
                rows.append({"locationid": location, "sensor_id": location * 10,
                             "location_name": f"Test-{location}",
                             "datetime": stamp.tz_convert("UTC"),
                             "datetime_local": stamp.tz_localize(None), "date_local": str(day),
                             "latitude": -33.8 if location != 103 else 0,
                             "longitude": 151.2, "parameter": "pm25", "unit": "µg/m³",
                             "original_unit": "µg/m³", "value": float(10 + hour + index)})
    frame = pd.DataFrame(rows, columns=SILVER_COLUMNS)
    frame.loc[frame["date_local"] == "2026-01-11", "value"] = 99999
    frame.loc[(frame["date_local"] == "2026-01-10") & (frame["locationid"] == 100)
              & (frame["datetime_local"].dt.hour == 12), "value"] = -4
    return frame


@pytest.fixture
def quality_benchmark(tmp_path):
    root = tmp_path / "quality-benchmark"
    scenarios = []
    for name, expected in FAULT_RULES.items():
        rows = [_incident(location, day, rule) for location, day in TARGETS for rule in expected]
        extras = []
        if name == "schema_drift":
            for location, day in TARGETS:
                recovery = str(date.fromisoformat(day) + timedelta(days=1))
                rows.append(_incident(location, recovery, "R8"))
                extras.append({"locationid": location, "date_local": recovery, "rule_id": "R8"})
        storage.write_parquet(pd.DataFrame(rows, columns=INCIDENT_COLUMNS),
                              storage.join(root, name, "gold/layer1"), "quality_incidents")
        scenarios.append({"scenario": name, "targets": [
            {"locationid": loc, "date_local": day, "all_expected_rules_detected": True,
             "expected_rule_ids": list(expected)} for loc, day in TARGETS
        ], "additional_control_incidents": extras})
    storage.write_text(storage.join(root, "quality-benchmark.json"), json.dumps({
        "methodology_version": 1, "dataset": {"locations": list(LOCATIONS)}, "scenarios": scenarios,
    }))
    return root


def test_descriptive_summary_preserves_alerts_and_overlapping_rule_denominators():
    alerts = pd.DataFrame([_alert(), _alert(parameter="pm1"), _alert(101)])
    incidents = pd.DataFrame([_incident(), _incident(rule="R2")], columns=INCIDENT_COLUMNS)
    fused = fuse(incidents, alerts)
    report = describe_fusion(alerts, incidents, fused)
    assert report["candidate_alerts"] == 3
    assert report["quarantined"] == 2
    assert report["escalated"] == 1
    assert report["all_alerts_preserved"]
    assert report["human_verified_accuracy"] is None
    assert report["by_parameter"]["pm1"]["quarantined"] == 1
    assert [row["overlapping_alerts"] for row in report["quality_rules"]] == [2, 2]
    assert not report["quality_rule_overlap_is_additive"]
    assert set(report["pm25_weak_label_context_only"]) == {"escalated", "quarantined"}
    with pytest.raises(ValueError, match="lost or duplicated"):
        describe_fusion(alerts, incidents, fused.iloc[:1])
    with pytest.raises(ValueError, match="unique"):
        describe_fusion(pd.concat([alerts, alerts]), incidents, fused)


def test_empty_alert_summary_is_not_perfect_accuracy():
    alerts = pd.DataFrame(columns=["locationid", "date_local", "parameter"])
    incidents = pd.DataFrame([_incident()], columns=INCIDENT_COLUMNS)
    report = describe_fusion(alerts, incidents, fuse(incidents, alerts))
    assert report["candidate_alerts"] == 0
    assert report["quarantine_rate"] is None
    assert report["human_verified_accuracy"] is None
    assert report["quality_rules"][0]["overlapping_alerts"] == 0


def test_controlled_policy_exposes_coincident_event_and_schema_recovery(quality_benchmark):
    report = controlled_policy_checks(str(quality_benchmark))
    groups = report["groups"]
    assert groups["data_fault_candidate"]["quarantined"] == 18
    assert groups["healthy_data_candidate"]["escalated"] == 18
    assert groups["schema_recovery_candidate"]["quarantined"] == 3
    assert groups["assumed_regional_event"] == {
        "candidates": 3, "escalated": 2, "quarantined": 1, "policy_checks_passed": 3,
    }
    assert len(report["cases"]) == 42
    assert report["end_to_end_event_recall"] is None


def test_controlled_checks_report_policy_failures(quality_benchmark, monkeypatch):
    def incorrect_policy(incidents, alerts):
        result = fuse(incidents, alerts)
        result["status"] = "escalated"
        return result

    monkeypatch.setattr("pipelines.fusion.evaluation.fuse", incorrect_policy)
    report = controlled_policy_checks(str(quality_benchmark))
    assert report["groups"]["data_fault_candidate"]["policy_checks_passed"] == 0
    assert report["groups"]["healthy_data_candidate"]["policy_checks_passed"] == 18


def test_review_sampling_is_reproducible_blind_and_has_only_past_context(silver):
    alerts = pd.DataFrame([_alert(), _alert(101), _alert(102), _alert(103, parameter="pm1")])
    incidents = pd.DataFrame([_incident()], columns=INCIDENT_COLUMNS)
    fused = fuse(incidents, alerts)
    packet, key = prepare_review(fused, silver, per_status=2)
    shuffled, shuffled_key = prepare_review(fused.sample(frac=1, random_state=7), silver,
                                           per_status=2)
    assert shuffled == packet and shuffled_key == key
    assert len(packet["cases"]) == 3
    assert key["strata"][1]["shortfall"] == 1
    target = next(row for row in packet["cases"] if row["locationid"] == 100)
    assert target["history_observed_days"] == 7
    assert max(row["median"] for row in target["prior_seven_day_hourly_profile"]) < 100
    assert min(row["value"] for row in target["current_readings"]) == -4
    assert {row["locationid"] for row in target["nearby_stations"]} == {101, 102}
    page = review_html(packet)
    for forbidden in ("incident_rule_ids", "weak_label", "alert_score", '"status":'):
        assert forbidden not in page
    assert "Download review JSON" in page
    malicious = copy.deepcopy(packet)
    malicious["cases"][0]["parameter"] = "</script><script>alert(1)</script>"
    assert "</script><script>alert(1)" not in review_html(malicious)


def _review(key, name, judgments):
    return {"pack_id": key["pack_id"], "reviewer_id": name, "responses": [
        {"case_id": case["case_id"], "pollution_evidence": event, "data_quality": quality,
         "notes": "Independent measurement pattern judgment"}
        for case, (event, quality) in zip(key["cases"], judgments, strict=True)
    ]}


def test_review_summary_requires_two_people_and_keeps_unknown_denominators(silver):
    alerts = pd.DataFrame([_alert(), _alert(101), _alert(102)])
    incidents = pd.DataFrame([_incident()], columns=INCIDENT_COLUMNS)
    _, key = prepare_review(fuse(incidents, alerts), silver)
    pending = summarize_reviews(key)
    assert pending["status"] == "pending"
    assert pending["paired_complete_cases"] == 0
    assert pending["event_agreement"] is None
    assert pending["sample_only"]["supported_event_escalation_rate"] is None
    a = _review(key, "Reviewer A", [("supported", "issue"), ("not_supported", "issue"),
                                    ("uncertain", "uncertain")])
    b = copy.deepcopy(a)
    b["reviewer_id"] = "Reviewer B"
    assert summarize_reviews(key, a)["paired_complete_cases"] == 0
    complete = summarize_reviews(key, a, b)
    assert complete["status"] == "reviewed"
    assert complete["paired_complete_cases"] == 3
    assert complete["event_agreement"] == 1
    assert complete["sample_only"]["consensus_supported_events"] == 1
    assert complete["sample_only"]["consensus_non_events"] == 1
    assert len(complete["uncertain_case_ids"]) == 1
    b["responses"][0]["pollution_evidence"] = "not_supported"
    disputed = summarize_reviews(key, a, b)
    assert len(disputed["disagreement_case_ids"]) == 1
    assert disputed["sample_only"]["consensus_supported_events"] == 0
    assert disputed["sample_only"]["supported_event_escalation_rate"] is None
    b["responses"].pop()
    assert summarize_reviews(key, a, b)["status"] == "pending"


@pytest.mark.parametrize("problem", ["same_reviewer", "wrong_pack", "unknown_case", "duplicate",
                                   "bad_label", "missing_note"])
def test_review_rejects_invalid_or_non_independent_submissions(silver, problem):
    _, key = prepare_review(fuse(pd.DataFrame(columns=INCIDENT_COLUMNS),
                                pd.DataFrame([_alert()])), silver)
    a = _review(key, "Reviewer A", [("supported", "no_issue")])
    b = copy.deepcopy(a)
    b["reviewer_id"] = "Reviewer B"
    if problem == "same_reviewer":
        b["reviewer_id"] = "  REVIEWER   a "
    elif problem == "wrong_pack":
        b["pack_id"] = "other"
    elif problem == "unknown_case":
        b["responses"][0]["case_id"] = "missing"
    elif problem == "duplicate":
        b["responses"].append(b["responses"][0])
    elif problem == "bad_label":
        b["responses"][0]["pollution_evidence"] = "confirmed"
    else:
        b["responses"][0]["notes"] = ""
    with pytest.raises(ValueError):
        summarize_reviews(key, a, b)


@pytest.mark.parametrize("as_string", [False, True])
def test_offline_evaluation_freezes_inputs_and_wires_independent_layers(
    tmp_path, silver, quality_benchmark, monkeypatch, as_string,
):
    bronze, silver_root, output = [tmp_path / zone for zone in ("bronze", "silver", "evaluation")]
    for (location, day), _ in silver.groupby(["locationid", "date_local"]):
        uri = storage.join(bronze, bronze_key(int(location), date.fromisoformat(day)))
        storage.write_bytes(uri, gzip.compress(b"location_id,datetime\n"))
    write_silver(silver, silver_root)
    for name in ("BRONZE_ROOT", "SILVER_ROOT", "GOLD_ROOT", "MODELS_ROOT"):
        monkeypatch.setenv(name, "s3://must-not-be-used/" + name)
    monkeypatch.setattr(storage, "_wr", lambda: pytest.fail("Unexpected AWS access"))
    input_bytes = {str(path): path.read_bytes() for path in silver_root.rglob("*.parquet")}
    calls = []

    def detect(*, silver_root, gold_root, models_root):
        calls.append((silver_root, models_root))
        assert "inputs" in silver_root and str(output) in models_root
        storage.write_parquet(pd.DataFrame([_alert(), _alert(101)]),
                              storage.join(gold_root, "layer2"), "event_alerts")
        return SimpleNamespace(feature_rows=44)

    monkeypatch.setattr("pipelines.fusion.evaluation.build_detection", detect)
    convert = str if as_string else lambda path: path
    report = run_evaluation(bronze_root=convert(bronze), silver_root=convert(silver_root),
                            quality_benchmark_root=convert(quality_benchmark),
                            output_root=convert(output), per_status=1)
    assert len(calls) == 1
    assert report["real_snapshot"]["candidate_alerts"] == 2
    assert report["real_snapshot"]["quarantined"] == 1
    assert report["real_snapshot"]["all_alerts_preserved"]
    assert report["independent_review"]["status"] == "pending"
    assert report["review_sampling"]["strata"][0]["selected"] == 1
    assert (output / "review/review.html").is_file()
    assert all(Path(path).read_bytes() == payload for path, payload in input_bytes.items())
    assert json.loads((output / "fusion-evaluation.json").read_text()) == report
    with pytest.raises(ValueError, match="new or empty"):
        run_evaluation(bronze_root=bronze, silver_root=silver_root,
                       quality_benchmark_root=quality_benchmark, output_root=output)


def test_evaluation_rejects_remote_inputs_before_io():
    with pytest.raises(ValueError, match="local"):
        run_evaluation(bronze_root="s3://bucket/bronze", silver_root="unused",
                       quality_benchmark_root="unused", output_root="unused")


def test_review_cli_scores_download_format_without_overwriting(tmp_path, silver):
    _, key = prepare_review(fuse(pd.DataFrame(columns=INCIDENT_COLUMNS),
                                pd.DataFrame([_alert()])), silver)
    a = _review(key, "Reviewer A", [("supported", "no_issue")])
    b = _review(key, "Reviewer B", [("supported", "no_issue")])
    args = []
    for flag, data in (("key", key), ("review-a", a), ("review-b", b)):
        path = tmp_path / (flag + ".json")
        path.write_text(json.dumps(data))
        args.extend(["--" + flag, str(path)])
    output = tmp_path / "result.json"
    args.extend(["--output", str(output)])
    assert summarize_main(args) == 0
    report = json.loads(output.read_text())
    assert report["paired_complete_cases"] == 1
    assert report["sample_only"]["supported_events_escalated"] == 1
    before = output.read_bytes()
    with pytest.raises(SystemExit):
        summarize_main(args)
    assert output.read_bytes() == before
