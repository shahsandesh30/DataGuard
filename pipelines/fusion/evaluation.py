"""Offline fusion evaluation and independent-review pack, using frozen local inputs.

Both detectors rebuild independently on the same silver snapshot. Fusion policy
is unchanged. Synthetic checks supply candidate alerts, so they test the policy
conditional on an alert, not end-to-end pollution detection or true accuracy.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import platform
from datetime import UTC, datetime
from importlib.metadata import version
from pathlib import Path

import pandas as pd

from pipelines import storage
from pipelines.conformance.conform import read_silver
from pipelines.detection.build import build_detection, read_event_alerts
from pipelines.fusion.build import build_fusion, read_trust_alerts
from pipelines.fusion.review import ALERT_KEY, prepare_review, review_html, summarize_reviews
from pipelines.fusion.trust_score import fuse
from pipelines.quality.build import build_quality, read_quality_incidents


def _local_path(root: str | Path) -> str:
    if storage.is_s3(root):
        raise ValueError("Fusion evaluation requires explicit local roots")
    return str(Path(root).resolve())


def _snapshot(source: str, target: str, suffixes: tuple[str, ...]) -> list[dict]:
    """Copy original bytes through storage, recording a portable input manifest."""
    prefix = source.replace("\\", "/").rstrip("/") + "/"
    files = []
    for uri in storage.list_files(source, suffixes):
        normalized = uri.replace("\\", "/")
        if not normalized.startswith(prefix):
            raise ValueError("Snapshot source escaped its requested root")
        relative = normalized[len(prefix):]
        payload = storage.read_bytes(uri)
        if payload is None:
            raise ValueError("An input disappeared during snapshot creation")
        storage.write_bytes(storage.join(target, relative), payload)
        files.append({"path": relative, "bytes": len(payload),
                      "sha256": hashlib.sha256(payload).hexdigest()})
    return files


def _status_counts(frame: pd.DataFrame) -> dict:
    if not frame["status"].isin(["escalated", "quarantined"]).all():
        raise ValueError("Unexpected fusion status")
    total = len(frame)
    held = int((frame["status"] == "quarantined").sum())
    return {"candidate_alerts": total, "escalated": total - held, "quarantined": held,
            "quarantine_rate": held / total if total else None,
            "mean_alert_score": float(frame["alert_score"].mean()) if total else None,
            "mean_trust_score": float(frame["trust_score"].mean()) if total else None}


def describe_fusion(alerts: pd.DataFrame, incidents: pd.DataFrame, fused: pd.DataFrame) -> dict:
    """Describe decisions without treating weak labels or quarantine as correctness."""
    for frame in (alerts, fused):
        if frame.duplicated(ALERT_KEY).any():
            raise ValueError("Fusion evaluation requires unique station-day-parameter alert keys")
    before = set(alerts[ALERT_KEY].itertuples(index=False, name=None))
    after = set(fused[ALERT_KEY].itertuples(index=False, name=None))
    if before != after or len(alerts) != len(fused):
        raise ValueError("Fusion lost or duplicated candidate alerts")
    rules = []
    for rule, group in incidents.groupby("rule_id"):
        days = set(zip(group["locationid"].astype(int), group["date_local"].astype(str),
                       strict=True))
        mask = pd.Series([tuple(row) in days for row in fused[["locationid", "date_local"]]
                          .itertuples(index=False, name=None)], index=fused.index, dtype=bool)
        overlap = fused.loc[mask]
        rules.append({"rule_id": str(rule), "incident_rows": len(group),
                      "incident_station_days": len(days), "overlapping_alerts": len(overlap),
                      "overlapping_quarantined_alerts": int((overlap["status"] == "quarantined")
                                                            .sum())})
    pm25 = fused[fused["parameter"] == "pm25"]
    weak_label_context = {}
    for status, group in pm25.groupby("status"):
        known = group["weak_label"].dropna()
        weak_label_context[status] = {"alerts": len(group), "available_weak_labels": len(known),
                                      "weak_label_positive": int(known.astype(bool).sum())}
    return {
        **_status_counts(fused), "all_alerts_preserved": True,
        "by_parameter": {str(parameter): _status_counts(group)
                         for parameter, group in fused.groupby("parameter")},
        "quality_rules": rules,
        "quality_rule_overlap_is_additive": False,
        "pm25_weak_label_context_only": weak_label_context,
        "human_verified_accuracy": None,
    }


def controlled_policy_checks(root: str) -> dict:
    """Feed actual Step 3 quality outputs to fusion with explicitly constructed alerts."""
    report_text = storage.read_text(storage.join(root, "quality-benchmark.json"))
    if report_text is None:
        raise ValueError("Step 3 quality-benchmark.json is required")
    quality_report = json.loads(report_text)
    if quality_report.get("methodology_version") != 1:
        raise ValueError("Unsupported Step 3 benchmark methodology")
    scenarios = quality_report["scenarios"]
    expected = {"frozen_sensor", "negative_readings", "sensor_dropout", "missing_daily_file",
                "schema_drift", "incorrect_unit_label"}
    if {item["scenario"] for item in scenarios} != expected or len(scenarios) != 6:
        raise ValueError("The controlled check requires all six Step 3 scenarios")
    cases = []

    def check(incidents, requests, scenario):
        alerts = pd.DataFrame([
            {"locationid": loc, "date_local": day, "parameter": "pm25", "alert_score": .8,
             "agreement_count": 2, "region_id": "synthetic", "rank": index + 1}
            for index, (loc, day, _, _) in enumerate(requests)
        ])
        result = fuse(incidents, alerts)
        if len(result) != len(alerts):
            raise ValueError("Controlled fusion lost candidate rows")
        for row, (loc, day, group, wanted) in zip(result.itertuples(), requests, strict=True):
            cases.append({"scenario": scenario, "locationid": loc, "date_local": day,
                          "case_group": group,
                          "expected_status": wanted, "observed_status": row.status,
                          "passed": row.status == wanted, "alert_score": row.alert_score,
                          "trust_score": row.trust_score,
                          "incident_rule_ids": row.incident_rule_ids})

    for scenario in scenarios:
        name = scenario["scenario"]
        incidents = read_quality_incidents(storage.join(root, name, "gold"))
        targets = scenario["targets"]
        if len(targets) != 3 or any(not row["all_expected_rules_detected"] for row in targets):
            raise ValueError("Step 3 must contain three successfully checked targets per fault")
        requests = [(int(row["locationid"]), row["date_local"], "data_fault_candidate",
                     "quarantined") for row in targets]
        # The supplied alert scores assume a detector raised a candidate. They
        # do not assert that the healthy concentration profile contains an event.
        requests += [(int(loc), "2026-01-05", "healthy_data_candidate", "escalated")
                     for loc in quality_report["dataset"]["locations"]]
        for extra in scenario["additional_control_incidents"]:
            if extra["rule_id"] == "R8":
                requests.append((int(extra["locationid"]), extra["date_local"],
                                 "schema_recovery_candidate", "quarantined"))
        check(incidents, requests, name)
        if name == "frozen_sensor":
            target = targets[0]
            # Assume the same genuine regional event at all three stations.
            # A coincident data issue holds one candidate despite that assumption.
            mixed = [(int(loc), target["date_local"], "assumed_regional_event",
                      "quarantined" if int(loc) == int(target["locationid"]) else "escalated")
                     for loc in quality_report["dataset"]["locations"]]
            check(incidents, mixed, name)
    groups = {}
    for group in sorted({case["case_group"] for case in cases}):
        selected = [case for case in cases if case["case_group"] == group]
        groups[group] = {"candidates": len(selected),
                         "escalated": sum(case["observed_status"] == "escalated"
                                          for case in selected),
                         "quarantined": sum(case["observed_status"] == "quarantined"
                                            for case in selected),
                         "policy_checks_passed": sum(case["passed"] for case in selected)}
    return {"conditional_on_constructed_candidates": True, "groups": groups, "cases": cases,
            "end_to_end_event_recall": None,
            "warning": "Policy consistency using Step 3 quality output; not detector accuracy"}


def run_evaluation(
    *, bronze_root: str | Path, silver_root: str | Path, quality_benchmark_root: str | Path,
    output_root: str | Path, per_status: int = 10,
) -> dict:
    """Snapshot inputs, rebuild both layers, evaluate fusion and prepare an unsigned review."""
    bronze, silver, quality_source, output = map(
        _local_path, (bronze_root, silver_root, quality_benchmark_root, output_root)
    )
    destination = Path(output)
    if destination.exists() and (not destination.is_dir() or any(destination.iterdir())):
        raise ValueError("Fusion evaluation output directory must be new or empty")
    if per_status < 1:
        raise ValueError("per_status must be positive")
    # Validate sources before creating a destination; prevent recursive snapshots.
    for source in (bronze, silver, quality_source):
        if not Path(source).is_dir():
            raise ValueError("A required local input directory does not exist")
        if destination == Path(source) or destination.is_relative_to(Path(source)):
            raise ValueError("Output must be outside input directories")
    inputs = storage.join(output, "inputs")
    frozen_bronze, frozen_silver = storage.join(inputs, "bronze"), storage.join(inputs, "silver")
    frozen_quality = storage.join(inputs, "quality-benchmark")
    snapshots = {
        "bronze": _snapshot(bronze, frozen_bronze, (".csv", ".csv.gz", ".jsonl")),
        "silver": _snapshot(silver, frozen_silver, (".parquet", ".json")),
        "quality_benchmark": _snapshot(quality_source, frozen_quality, (".parquet", ".json")),
    }
    if not snapshots["silver"] or not read_silver(frozen_silver).shape[0]:
        raise ValueError("Fusion evaluation requires non-empty silver")
    storage.write_text(storage.join(inputs, "snapshot.json"),
                       json.dumps(snapshots, indent=2) + "\n")
    controlled = controlled_policy_checks(frozen_quality)
    gold, models = storage.join(output, "gold"), storage.join(output, "models")
    quality = build_quality(silver_root=frozen_silver, bronze_root=frozen_bronze,
                            gold_root=gold, models_root=models)
    detection = build_detection(silver_root=frozen_silver, gold_root=gold, models_root=models)
    build_fusion(gold_root=gold)
    measurements = read_silver(frozen_silver)
    fused, incidents = read_trust_alerts(gold), read_quality_incidents(gold)
    packet, key = prepare_review(fused, measurements, per_status=per_status)
    review_root = storage.join(output, "review")
    storage.write_text(storage.join(review_root, "review.html"), review_html(packet))
    storage.write_text(storage.join(review_root, "review-key.json"),
                       json.dumps(key, indent=2) + "\n")
    for name in ("a", "b"):
        template = {"pack_id": packet["pack_id"], "reviewer_id": "", "responses": [
            {"case_id": case["case_id"], "pollution_evidence": "", "data_quality": "", "notes": ""}
            for case in key["cases"]
        ]}
        storage.write_text(storage.join(review_root, f"reviewer-{name}.json"),
                           json.dumps(template, indent=2) + "\n")
    package = Path(__file__).resolve().parents[1]
    report = {
        "methodology_version": 1, "built_at_utc": datetime.now(UTC).isoformat(),
        "source_hash_normalization": "UTF-8 source text with LF line endings",
        "snapshot_manifest_sha256": hashlib.sha256(
            json.dumps(snapshots, sort_keys=True).encode()).hexdigest(),
        "source_sha256": {source.relative_to(package).as_posix(): hashlib.sha256(
            source.read_text(encoding="utf-8").encode()).hexdigest()
                          for source in sorted(package.rglob("*.py"))},
        "runtime_versions": {"python": platform.python_version(),
                             **{name: version(name)
                                for name in ("pandas", "numpy", "scikit-learn")}},
        "dataset": {"reading_rows": len(measurements),
                    "locations": int(measurements["locationid"].nunique()),
                    "local_date_start": str(measurements["date_local"].min()),
                    "local_date_end": str(measurements["date_local"].max()),
                    "quality_station_days": quality.station_day_rows,
                    "quality_rule_incidents": quality.rule_incidents,
                    "quality_model_incidents": quality.model_incidents,
                    "event_feature_rows": detection.feature_rows},
        "real_snapshot": describe_fusion(read_event_alerts(gold), incidents, fused),
        "controlled_policy": controlled,
        "review_sampling": {name: value for name, value in key.items() if name != "cases"},
        "independent_review": summarize_reviews(key),
        "limitations": [
            "Detectors refit on the snapshot; these decisions are not held-out accuracy.",
            "Step 2's corrected temporal and injection results remain separate evidence.",
            "Quarantined alerts remain visible; automatic escalation is reduced without deletion.",
            "Any station-day quality incident applies to every alerted parameter on that day.",
            "R7 archive/local-day boundary assumptions and backfill lateness can affect decisions.",
            "Real event retention and false-alert reduction need two independent human judgments.",
        ],
    }
    storage.write_text(storage.join(output, "fusion-evaluation.json"),
                       json.dumps(report, indent=2, allow_nan=False) + "\n")
    return report


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    for name in ("bronze-root", "silver-root", "quality-benchmark-root", "output-root"):
        parser.add_argument("--" + name, required=True)
    parser.add_argument("--per-status", type=int, default=10)
    args = parser.parse_args(argv)
    try:
        report = run_evaluation(bronze_root=args.bronze_root, silver_root=args.silver_root,
                                quality_benchmark_root=args.quality_benchmark_root,
                                output_root=args.output_root, per_status=args.per_status)
    except ValueError as exc:
        parser.error(str(exc))
    print(json.dumps(report["real_snapshot"], indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
