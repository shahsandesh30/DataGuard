"""Describe saved alerts without fitting models, changing policy or accessing AWS.

Run with the repository venv from the repository root. Raw frozen artifacts
are intentionally not committed; the aggregate outputs and hashes are.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import platform
from pathlib import Path

import numpy as np
import pandas as pd

POLICIES = {
    "none": {"low": 0.0, "medium": 0.0, "high": 0.0},
    "mild": {"low": 0.10, "medium": 0.30, "high": 0.50},
    "current": {"low": 0.20, "medium": 0.40, "high": 0.70},
    "stronger": {"low": 0.30, "medium": 0.50, "high": 0.90},
}
KEYS = ["locationid", "date_local", "parameter"]
FLAGS = ["if_flag", "lof_flag", "dbscan_flag"]


def digest(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


def fingerprints(root):
    return {
        p.relative_to(root).as_posix(): digest(p) for p in sorted(root.rglob("*")) if p.is_file()
    }


def read_table(root):
    files = sorted(root.rglob("*.parquet"))
    if not files:
        raise ValueError(f"No Parquet inputs: {root}")
    return pd.concat([pd.read_parquet(p) for p in files], ignore_index=True)


def json_value(value):
    if isinstance(value, dict):
        return {str(k): json_value(v) for k, v in value.items()}
    if isinstance(value, (list, tuple)):
        return [json_value(v) for v in value]
    if isinstance(value, (np.integer, np.bool_)):
        return value.item()
    if isinstance(value, (pd.Timestamp, Path)):
        return str(value)
    if pd.isna(value):
        return None
    return value


def ranking(frame, column):
    ordered = frame.sort_values([column, *KEYS], ascending=[False, True, True, True], kind="stable")
    return ordered.index.tolist()


def analyse(snapshot, output):
    snapshot, output = snapshot.resolve(), output.resolve()
    if snapshot == output or snapshot in output.parents or output in snapshot.parents:
        raise ValueError("Output must be separate from frozen inputs")
    before = fingerprints(snapshot)
    alerts = read_table(snapshot / "gold/fusion/trust_alerts")
    candidates = read_table(snapshot / "gold/layer2/event_alerts")
    quality = read_table(snapshot / "gold/layer1/quality_metrics")
    incidents = read_table(snapshot / "gold/layer1/quality_incidents")
    silver = read_table(snapshot / "inputs/silver")
    alerts["date_local"] = alerts["date_local"].astype(str)
    alerts["max_severity"] = alerts["max_severity"].fillna("")
    assert len(alerts) == len(candidates) == 175
    assert not alerts.duplicated(KEYS).any()
    assert set(map(tuple, alerts[KEYS].to_numpy())) == set(map(tuple, candidates[KEYS].to_numpy()))
    assert set(alerts.max_severity) <= {"", "low", "medium", "high"}
    assert alerts[FLAGS].notna().all().all()
    assert (alerts[FLAGS].astype(int).sum(axis=1) == alerts.agreement_count).all()
    status = np.where(alerts.has_quality_incident, "quarantined", "escalated")
    assert (status == alerts.status).all()
    assert alerts.status.value_counts().to_dict() == {"escalated": 110, "quarantined": 65}

    rows, movements = [], []
    for parameter, original in alerts.groupby("parameter", sort=True):
        baseline_order = ranking(original, "trust_score")
        baseline_ranks = {idx: i + 1 for i, idx in enumerate(baseline_order)}
        baseline_top = set(baseline_order[:10])
        anomaly_top = set(ranking(original, "alert_score")[:10])
        for name, penalties in POLICIES.items():
            frame = original.copy()
            penalty = frame.max_severity.map(penalties).fillna(0.0)
            frame["scenario_trust"] = (frame.alert_score * (1 - penalty)).clip(0, 1)
            if name == "current":
                assert np.allclose(frame.scenario_trust, frame.trust_score, atol=1e-12, rtol=0)
            order = ranking(frame, "scenario_trust")
            ranks = {idx: i + 1 for i, idx in enumerate(order)}
            shifts = [abs(ranks[idx] - baseline_ranks[idx]) for idx in order]
            top = set(order[:10])
            rows.append(
                {
                    "parameter": parameter,
                    "scenario": name,
                    "alerts": len(frame),
                    "mean_trust": float(frame.scenario_trust.mean()),
                    "mean_absolute_rank_movement_vs_current": float(np.mean(shifts)),
                    "alerts_moving_rank_vs_current": sum(s > 0 for s in shifts),
                    "top10_overlap_vs_current": len(top & baseline_top),
                    "top10_overlap_vs_anomaly_only": len(top & anomaly_top),
                    "escalated": int((frame.status == "escalated").sum()),
                    "quarantined": int((frame.status == "quarantined").sum()),
                }
            )
            for idx, row in frame.iterrows():
                movements.append(
                    {
                        **{key: row[key] for key in KEYS},
                        "scenario": name,
                        "status": row.status,
                        "max_severity": row.max_severity,
                        "alert_score": float(row.alert_score),
                        "scenario_trust": float(row.scenario_trust),
                        "rank": ranks[idx],
                        "current_rank": baseline_ranks[idx],
                    }
                )
    votes = []
    for parameter, frame in alerts.groupby("parameter", sort=True):
        votes.append(
            {
                "parameter": parameter,
                "retained_candidates": len(frame),
                **{flag: int(frame[flag].sum()) for flag in FLAGS},
                **{f"votes_{n}": int((frame.agreement_count == n).sum()) for n in (1, 2, 3)},
            }
        )

    def context(location, day):
        subset = incidents[
            (incidents.locationid == location) & (incidents.date_local.astype(str) == day)
        ]
        return sorted(set(subset.rule_id.astype(str)))

    cases = []
    for desired in ("escalated", "quarantined"):
        subset = alerts[(alerts.parameter == "pm25") & (alerts.status == desired)]
        row = subset.sort_values(["alert_score", *KEYS], ascending=[False, True, True, True]).iloc[
            0
        ]
        readings = silver[
            (silver.locationid == row.locationid)
            & (silver.date_local.astype(str) == row.date_local)
            & (silver.parameter == "pm25")
        ]
        assert len(readings) > 0
        cases.append(
            {
                "kind": desired,
                **{k: row[k] for k in KEYS},
                "alert_score": float(row.alert_score),
                "trust_score": float(row.trust_score),
                "max_severity": row.max_severity,
                "penalty": POLICIES["current"].get(row.max_severity, 0.0),
                "rules": context(row.locationid, row.date_local),
                "votes": {flag: bool(row[flag]) for flag in FLAGS},
                "agreement_count": int(row.agreement_count),
                "measurements": len(readings),
                "sensors": int(readings.sensor_id.nunique()),
                "value_min": float(readings.value.min()),
                "value_max": float(readings.value.max()),
                "units": sorted(set(readings.unit.astype(str))),
                "selection": (
                    "Highest saved PM2.5 anomaly score within this status; "
                    "illustration, not a random sample"
                ),
            }
        )
    manifest = [
        json.loads(line)
        for line in (snapshot / "inputs/bronze/_manifest.jsonl")
        .read_text(encoding="utf-8")
        .splitlines()
        if line.strip()
    ]
    missing = quality[quality.total_readings == 0].sort_values(["date_local", "locationid"])
    for _, row in missing.iterrows():
        day = str(row.date_local)
        attempts = [
            r for r in manifest if r.get("locationid") == row.locationid and r.get("day") == day
        ]
        if not attempts or not any(r.get("status") == "missing" for r in attempts):
            continue
        present = alerts[(alerts.locationid == row.locationid) & (alerts.date_local == day)]
        assert present.empty
        cases.append(
            {
                "kind": "manifest_only_missing_day",
                "locationid": row.locationid,
                "date_local": day,
                "total_readings": int(row.total_readings),
                "missing_rate_mean": float(row.missing_rate_mean),
                "file_present": bool(row.file_present),
                "sensors_expected": int(row.sensors_expected),
                "sensors_received": int(row.sensors_received),
                "rules": context(row.locationid, day),
                "event_candidates": 0,
                "manifest_statuses": [r.get("status") for r in attempts],
                "archive_key": attempts[-1].get("archive_key"),
                "selection": (
                    "Earliest local missing date, then lowest location ID "
                    "with a recorded missing attempt"
                ),
            }
        )
        break
    assert len(cases) == 3
    assert before == fingerprints(snapshot), "Frozen source bytes changed"
    report = {
        "analysis_version": 1,
        "analysis_source_sha256": digest(Path(__file__)),
        "runtime": {
            "python": platform.python_version(),
            "pandas": pd.__version__,
            "numpy": np.__version__,
        },
        "source_snapshot_manifest_sha256": json.loads(
            (snapshot / "fusion-evaluation.json").read_text()
        )["snapshot_manifest_sha256"],
        "snapshot_files_checked_unchanged": len(before),
        "source_input_hashes": before,
        "candidate_alerts": 175,
        "status_counts": alerts.status.value_counts().to_dict(),
        "severity_counts": alerts.max_severity.value_counts().to_dict(),
        "policies_low_medium_high": POLICIES,
        "sensitivity": rows,
        "detector_votes": votes,
        "worked_examples": cases,
        "ranking_definition": (
            "Descending scenario trust within each parameter over all saved dates; "
            "ties: location, local date, parameter. Absolute movement and top ten "
            "compare with current saved trust. This is not the operational region-day cap."
        ),
        "limitations": [
            "Post-selection analysis of 175 retained, capped candidates; "
            "no refit, new events or ground truth.",
            "Status rule fixed to incident presence in every scenario; "
            "no threshold or production policy change.",
            "No low-severity alerts; low penalty effect not empirically evaluated.",
            "Alternative penalties are illustrative choices, not expert-calibrated settings.",
            "Detector coverage describes these candidates only; "
            "not ablation, sensitivity/recall or model accuracy.",
        ],
    }
    output.mkdir(parents=True, exist_ok=True)
    (output / "analysis.json").write_text(
        json.dumps(json_value(report), indent=2, allow_nan=False) + "\n", encoding="utf-8"
    )
    pd.DataFrame(rows).to_csv(output / "penalty-sensitivity.csv", index=False, lineterminator="\n")
    pd.DataFrame(votes).to_csv(output / "detector-votes.csv", index=False, lineterminator="\n")
    pd.DataFrame(movements).to_csv(output / "rankings.csv", index=False, lineterminator="\n")
    print(
        json.dumps(
            json_value(
                {
                    k: report[k]
                    for k in [
                        "snapshot_files_checked_unchanged",
                        "severity_counts",
                        "sensitivity",
                        "detector_votes",
                        "worked_examples",
                    ]
                }
            ),
            indent=2,
        )
    )


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--snapshot", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    arguments = parser.parse_args()
    analyse(arguments.snapshot, arguments.output)
