"""Verify an existing local evaluation and record technical evidence without refitting.

Run with ``python -m scripts.verify_snapshot --evaluation-root ... --output-root ...``.
The output must be a new local directory outside the evaluation being checked.
"""

from __future__ import annotations

import argparse
import hashlib
import json
from collections import Counter
from datetime import UTC, datetime
from pathlib import Path

from dashboard.data import load_build_summaries, load_dashboard_frames
from dashboard.explanation import explain_alert
from pipelines import storage
from pipelines.conformance.conform import read_silver
from pipelines.fusion.evaluation import controlled_policy_checks, describe_fusion

ROOT = Path(__file__).resolve().parents[1]


def _checked_file(root: Path, relative: str, expected_sha256: str | None = None) -> dict:
    candidate = (root / relative).resolve()
    if not candidate.is_relative_to(root.resolve()):
        raise ValueError("An evidence path escapes its input root")
    data = storage.read_bytes(str(candidate))
    if data is None:
        raise ValueError(f"Missing evidence file: {relative}")
    digest = hashlib.sha256(data).hexdigest()
    if expected_sha256 is not None and digest != expected_sha256:
        raise ValueError(f"Evidence fingerprint differs: {relative}")
    return {
        "path": candidate.relative_to(root.resolve()).as_posix(),
        "bytes": len(data),
        "sha256": digest,
    }


def _model_record(root: Path, recorded_path: str) -> dict:
    # Build summaries retain their original absolute path. Resolve its models
    # suffix against this snapshot so a copied evaluation remains portable.
    suffix = recorded_path.replace("\\", "/").rsplit("/models/", 1)
    if len(suffix) != 2:
        raise ValueError("Build summary does not reference a snapshot model")
    relative = "models/" + suffix[1]
    if "/history/run=" not in relative or not relative.endswith(".joblib"):
        raise ValueError("Build summary must bind a versioned model, not latest")
    return _checked_file(root / "models", suffix[1])


def verify_snapshot(
    evaluation_root: str | Path,
    output_root: str | Path,
    *,
    code_revision: str | None = None,
) -> dict:
    """Check frozen inputs, producing code, model bindings, policy, and explanations."""
    if storage.is_s3(evaluation_root) or storage.is_s3(output_root):
        raise ValueError("Snapshot verification requires explicit local roots")
    evaluation, output = Path(evaluation_root).resolve(), Path(output_root).resolve()
    if output.is_relative_to(evaluation) or evaluation.is_relative_to(output):
        raise ValueError("Verification output must not overlap its evaluation inputs")
    if output.exists() and any(output.iterdir()):
        raise ValueError("Verification output must be empty")

    def read_json(relative):
        text = storage.read_text(storage.join(str(evaluation), relative))
        if text is None:
            raise ValueError(f"Missing evaluation metadata: {relative}")
        return json.loads(text)

    original = read_json("fusion-evaluation.json")
    if original.get("methodology_version") != 1:
        raise ValueError("Unsupported fusion evaluation methodology")
    pipeline_sources = {
        path.relative_to(ROOT / "pipelines").as_posix(): hashlib.sha256(
            path.read_text(encoding="utf-8").encode()
        ).hexdigest()
        for path in sorted((ROOT / "pipelines").rglob("*.py"))
    }
    if pipeline_sources != original["source_sha256"]:
        raise ValueError("Pipeline source differs from the evaluated build; rebuild its evaluation")
    snapshot = read_json("inputs/snapshot.json")
    manifest_hash = hashlib.sha256(json.dumps(snapshot, sort_keys=True).encode()).hexdigest()
    if manifest_hash != original["snapshot_manifest_sha256"]:
        raise ValueError("The frozen input manifest differs from the evaluation report")
    sections = {"bronze": "bronze", "silver": "silver", "quality_benchmark": "quality-benchmark"}
    if set(snapshot) != set(sections):
        raise ValueError("Frozen input manifest must include all three input zones")
    input_files = 0
    for name, folder in sections.items():
        for record in snapshot[name]:
            checked = _checked_file(
                evaluation / "inputs" / folder, record["path"], record["sha256"]
            )
            if checked["bytes"] != record["bytes"]:
                raise ValueError("Frozen input byte count differs")
            input_files += 1

    gold, silver = str(evaluation / "gold"), str(evaluation / "inputs/silver")
    frames = load_dashboard_frames(gold)
    summaries = load_build_summaries(silver, gold)
    measurements = read_silver(silver)
    result = describe_fusion(frames["event_alerts"], frames["incidents"], frames["trust_alerts"])
    if result != original["real_snapshot"]:
        raise ValueError("Saved gold decisions differ from the recorded fusion evaluation")
    controlled = controlled_policy_checks(str(evaluation / "inputs/quality-benchmark"))
    if controlled != original["controlled_policy"]:
        raise ValueError("Controlled policy checks differ from the recorded evaluation")

    models = {}
    if summaries["layer1"].get("model_trained"):
        models["layer1"] = _model_record(evaluation, summaries["layer1"]["model_path"])
    for parameter, model_path in summaries["layer2"].get("model_paths", {}).items():
        models[f"layer2/{parameter}"] = _model_record(evaluation, model_path)
    history_dates = Counter()
    for row in frames["trust_alerts"].to_dict("records"):
        evidence = explain_alert(
            frames["trust_alerts"],
            frames["incidents"],
            measurements,
            locationid=int(row["locationid"]),
            date_local=str(row["date_local"]),
            parameter=row["parameter"],
            summaries=summaries,
        )
        if not evidence["decision"]["calculation_verified"] or evidence["warnings"]:
            raise ValueError(f"Alert evidence is inconsistent: {evidence['alert']}")
        if (
            not evidence["readings"]
            or not evidence["features"]
            or any(vote["flagged"] is None for vote in evidence["detector_votes"])
        ):
            raise ValueError("An alert is missing measurements, features, or votes")
        json.dumps(evidence, allow_nan=False)
        history_dates[len(evidence["history_window"]["available_local_dates"])] += 1
    report = {
        "schema_version": 1,
        "checked_at_utc": datetime.now(UTC).isoformat(),
        "code_revision": code_revision,
        "source_sha256_normalized_lf": {
            path.relative_to(ROOT).as_posix(): hashlib.sha256(
                path.read_text(encoding="utf-8").encode()
            ).hexdigest()
            for folder in ("pipelines", "dashboard", "glue", "scripts")
            for path in sorted((ROOT / folder).rglob("*.py"))
        },
        "dataset": original["dataset"],
        "snapshot_manifest_sha256": manifest_hash,
        "frozen_input_files_verified": input_files,
        "model_history_artifacts": models,
        "build_times_utc": {
            name: summaries[name]["built_at_utc"] for name in ("layer1", "layer2", "fusion")
        },
        "alerts_checked": result["candidate_alerts"],
        "escalated": result["escalated"],
        "quarantined": result["quarantined"],
        "all_alerts_preserved": result["all_alerts_preserved"],
        "controlled_checks_passed": sum(row["passed"] for row in controlled["cases"]),
        "controlled_checks_total": len(controlled["cases"]),
        "alerts_by_available_prior_dates": dict(sorted(history_dates.items())),
        "scope": "Saved technical evidence and conditional policy checks, not event accuracy",
    }
    output.mkdir(parents=True, exist_ok=True)
    storage.write_text(
        str(output / "technical-check.json"), json.dumps(report, indent=2, allow_nan=False) + "\n"
    )
    return report


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--evaluation-root", required=True)
    parser.add_argument("--output-root", required=True)
    parser.add_argument("--code-revision")
    args = parser.parse_args(argv)
    try:
        report = verify_snapshot(
            args.evaluation_root, args.output_root, code_revision=args.code_revision
        )
    except ValueError as exc:
        parser.error(str(exc))
    print(
        json.dumps(
            {
                key: report[key]
                for key in (
                    "alerts_checked",
                    "escalated",
                    "quarantined",
                    "controlled_checks_passed",
                )
            }
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
