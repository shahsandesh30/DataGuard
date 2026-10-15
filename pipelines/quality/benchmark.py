"""Offline Layer 1 fault benchmark using isolated, synthetic archive files.

Run with ``python -m pipelines.quality.benchmark --output-root <new-directory>``.
The output root is required, must be local and empty, and never comes from .env.
Each scenario starts from the same healthy baseline; production thresholds and
the production conform/quality stages are used unchanged.
"""

from __future__ import annotations

import argparse
import gzip
import hashlib
import json
import platform
from datetime import UTC, date, datetime, timedelta
from importlib.metadata import version
from pathlib import Path

import pandas as pd

from pipelines import storage
from pipelines.config import MIN_STATION_DAYS
from pipelines.conformance.conform import build_silver
from pipelines.ingestion.fetch import MANIFEST_FILENAME, archive_key, bronze_key
from pipelines.quality.build import build_quality, read_quality_incidents, read_quality_metrics

LOCATIONS = (900001, 900002, 900003)
START = date(2026, 1, 1)
DAYS = 21
FAULT_RULES = {
    "frozen_sensor": ("R2", "R3"),
    "negative_readings": ("R1",),
    "sensor_dropout": ("R5",),
    "missing_daily_file": ("R4", "R5", "R7"),
    "schema_drift": ("R8",),
    "incorrect_unit_label": ("R10",),
}
# Different stations and interior days, each with observed prior sensor history.
TARGETS = tuple(
    (location, (START + timedelta(days=9 + index * 4)).isoformat())
    for index, location in enumerate(LOCATIONS)
)


def _archive_frame(location: int, day: date) -> pd.DataFrame:
    rows = []
    day_index = (day - START).days
    for sensor in range(2):
        for hour in range(24):
            local = pd.Timestamp(f"{day.isoformat()}T{hour:02d}:00:00+11:00")
            rows.append({
                "location_id": location,
                "sensors_id": location * 10 + sensor,
                "location": f"Synthetic-{location}",
                "datetime": local.tz_convert("UTC").isoformat(),
                "datetime_local": local.isoformat(),
                "lat": -33.87,
                "lon": 151.21,
                "parameter": "pm25",
                "units": "µg/m³",
                # Changing concentrations, fixed healthy cadence and sensor spread.
                "value": 10 + ((hour * 7 + day_index * 3 + location) % 19) + sensor * 2,
            })
    return pd.DataFrame(rows)


def _inject(frame: pd.DataFrame, fault: str) -> pd.DataFrame:
    changed = frame.copy()
    first_sensor = changed["sensors_id"] == changed["sensors_id"].min()
    if fault == "frozen_sensor":
        changed.loc[first_sensor, "value"] = 12
    elif fault == "negative_readings":
        changed.loc[first_sensor & (changed.index % 24).isin([12, 13, 14]), "value"] = -5
    elif fault == "sensor_dropout":
        changed = changed.loc[~first_sensor].copy()
    elif fault == "schema_drift":
        changed["unexpected_provider_field"] = "changed"
    elif fault == "incorrect_unit_label":
        # ppm cannot describe particulate mass. Conformance preserves this evidence.
        changed.loc[first_sensor, "units"] = "ppm"
    return changed


def _seed_bronze(root: str, fault: str | None) -> str:
    """Write deterministic gzip bytes and a manifest; return the input fingerprint."""
    manifest = []
    fingerprint = hashlib.sha256()
    for location in LOCATIONS:
        for index in range(DAYS):
            day = START + timedelta(days=index)
            targeted = fault is not None and (location, day.isoformat()) in TARGETS
            relative = bronze_key(location, day)
            missing = targeted and fault == "missing_daily_file"
            payload = b""
            if not missing:
                frame = _archive_frame(location, day)
                if targeted:
                    frame = _inject(frame, fault)
                payload = gzip.compress(frame.to_csv(index=False, lineterminator="\n").encode(),
                                        mtime=0)
                storage.write_bytes(storage.join(root, relative), payload)
            record = {
                "locationid": location,
                "day": day.isoformat(),
                "archive_key": archive_key(location, day),
                "status": "missing" if missing else "copied",
                "local_path": relative,
                "bytes": len(payload),
                # Simulated near-present arrival, before the 72h publication deadline.
                "arrived_at": f"{(day + timedelta(days=2)).isoformat()}T00:00:00+00:00",
                "error": None,
            }
            line = json.dumps(record, sort_keys=True)
            manifest.append(line)
            fingerprint.update((relative + "\n" + line + "\n").encode())
            fingerprint.update(payload)
    storage.write_text(storage.join(root, MANIFEST_FILENAME), "\n".join(manifest) + "\n")
    return fingerprint.hexdigest()


def _incident_keys(incidents: pd.DataFrame) -> set[tuple[int, str, str]]:
    return {
        (int(row.locationid), str(row.date_local), str(row.rule_id))
        for row in incidents.itertuples()
    }


def summarize_run(
    metrics: pd.DataFrame,
    incidents: pd.DataFrame,
    baseline_keys: set[tuple[int, str, str]],
    expected: tuple[str, ...] = (),
) -> dict:
    """Keep missing/unscored targets in the denominator and separate rule/model evidence."""
    keys = _incident_keys(incidents)
    metric_keys = {(int(row.locationid), str(row.date_local)) for row in metrics.itertuples()}
    targets = set(TARGETS) if expected else set()
    raw_model_keys = {
        (int(row.locationid), str(row.date_local)) for row in metrics.itertuples()
        if getattr(row, "model_is_incident", False)
    }
    target_results = []
    for location, day in sorted(targets):
        observed = {rule for loc, dt, rule in keys if (loc, dt) == (location, day)}
        rules = observed - {"M1"}
        target_results.append({
            "locationid": location,
            "date_local": day,
            "scored": (location, day) in metric_keys,
            "expected_rule_ids": list(expected),
            "observed_rule_ids": sorted(rules),
            "missing_expected_rule_ids": sorted(set(expected) - rules),
            "rule_detected": bool(set(expected) & rules),
            "all_expected_rules_detected": set(expected) <= rules,
            "raw_model_flagged": (location, day) in raw_model_keys,
            "reported_model_incident": "M1" in observed,
        })
    controls = metric_keys - targets
    control_incidents = {key for key in keys if key[:2] in controls}
    additional = control_incidents - baseline_keys
    flagged_controls = {key[:2] for key in control_incidents}
    return {
        "station_day_rows": len(metric_keys),
        "rule_incident_rows": sum(key[2] != "M1" for key in keys),
        "reported_model_incident_rows": sum(key[2] == "M1" for key in keys),
        "raw_model_flagged_station_days": len(raw_model_keys),
        "targets": target_results,
        "control_station_days": len(controls),
        "control_rule_incident_rows": sum(key[2] != "M1" for key in control_incidents),
        "control_model_incident_rows": sum(key[2] == "M1" for key in control_incidents),
        "control_flagged_station_days": len(flagged_controls),
        "untouched_control_flag_rate": len(flagged_controls) / len(controls) if controls else None,
        "additional_control_incidents": [
            {"locationid": loc, "date_local": day, "rule_id": rule}
            for loc, day, rule in sorted(additional)
        ],
    }


def _source_hashes() -> dict[str, str]:
    package = Path(__file__).resolve().parents[1]
    return {
        source.relative_to(package).as_posix(): hashlib.sha256(
            source.read_text(encoding="utf-8").encode("utf-8")
        ).hexdigest()
        for source in sorted(package.rglob("*.py"))
    }


def run_quality_benchmark(output_root: str | Path) -> dict:
    """Exercise actual bronze -> silver -> Layer 1 builds without operational roots."""
    if storage.is_s3(output_root):
        raise ValueError("The quality benchmark requires a new, local output directory")
    output = Path(output_root).resolve()
    if output.exists() and (not output.is_dir() or any(output.iterdir())):
        raise ValueError("The quality benchmark output directory must be empty")
    output.mkdir(parents=True, exist_ok=True)

    results = []
    baseline_keys: set[tuple[int, str, str]] = set()
    for fault in (None, *FAULT_RULES):
        name = fault or "healthy_baseline"
        scenario = storage.join(output, name)
        bronze = storage.join(scenario, "bronze")
        silver = storage.join(scenario, "silver")
        gold = storage.join(scenario, "gold")
        fingerprint = _seed_bronze(bronze, fault)
        conformed = build_silver(bronze_root=bronze, silver_root=silver)
        if conformed.files_failed:
            raise RuntimeError(f"Benchmark conformance failed: {conformed.failed}")
        quality = build_quality(silver_root=silver, bronze_root=bronze, gold_root=gold,
                                models_root=storage.join(scenario, "models"))
        metrics = read_quality_metrics(gold)
        incidents = read_quality_incidents(gold)
        summary = summarize_run(metrics, incidents, baseline_keys, FAULT_RULES.get(fault, ()))
        summary.update({
            "scenario": name,
            "input_sha256": fingerprint,
            "input_reading_rows": conformed.rows,
            "sensor_day_rows": quality.sensor_day_rows,
            "model_trained": quality.model_trained,
        })
        results.append(summary)
        if fault is None:
            baseline_keys = _incident_keys(incidents)

    cases = [target for item in results[1:] for target in item["targets"]]
    report = {
        "methodology_version": 1,
        "built_at_utc": datetime.now(UTC).isoformat(),
        "dataset": {
            "synthetic": True, "locations": list(LOCATIONS), "days_per_location": DAYS,
            "start": START.isoformat(), "end": (START + timedelta(days=DAYS - 1)).isoformat(),
            "sensors_per_station": 2, "hourly_readings_per_sensor": 24,
            "local_offset": "+11:00", "baseline_input_sha256": results[0]["input_sha256"],
        },
        "methodology": {
            "independent_scenarios": True, "repetitions_per_fault": len(TARGETS),
            "model_min_station_days": MIN_STATION_DAYS,
            "model_fit": "Production refit on each scenario, not held-out model accuracy",
            "rule_R6_enabled": False,
            "source_hash_normalization": "UTF-8 source text with LF line endings",
            "control_denominator": "Station-day occurrences across six separate scenario copies",
            "limitations": [
                "Constructed regular hourly controls do not measure real-world false positives.",
                "Three repetitions per fault; one region, parameter, cadence and fault strength.",
                "Schema reversion is another header transition and can flag the following day.",
                "No reference-monitor labels or independent Isolation Forest accuracy estimate.",
            ],
        },
        "runtime_versions": {"python": platform.python_version(), **{
            name: version(name) for name in ("pandas", "numpy", "scikit-learn")
        }},
        "source_sha256": _source_hashes(),
        "healthy_baseline": results[0],
        "scenarios": results[1:],
        "totals": {
            "fault_cases": len(cases),
            "scored_fault_cases": sum(case["scored"] for case in cases),
            "rule_detected_cases": sum(case["rule_detected"] for case in cases),
            "all_expected_rules_detected_cases": sum(
                case["all_expected_rules_detected"] for case in cases
            ),
            "missed_expected_rule_cases": sum(not case["rule_detected"] for case in cases),
            "raw_model_flagged_cases": sum(case["raw_model_flagged"] for case in cases),
            "reported_model_incident_cases": sum(case["reported_model_incident"] for case in cases),
            "control_station_day_occurrences": sum(
                item["control_station_days"] for item in results[1:]
            ),
            "control_flagged_station_day_occurrences": sum(
                item["control_flagged_station_days"] for item in results[1:]
            ),
            "additional_control_incident_rows": sum(
                len(item["additional_control_incidents"]) for item in results[1:]
            ),
        },
    }
    storage.write_text(storage.join(output, "quality-benchmark.json"),
                       json.dumps(report, indent=2) + "\n")
    return report


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output-root", required=True, help="New, empty local output directory")
    args = parser.parse_args(argv)
    try:
        report = run_quality_benchmark(args.output_root)
    except ValueError as exc:
        parser.error(str(exc))
    print(json.dumps(report["totals"], indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
