"""Read-only explanation of one persisted fusion decision, with bounded evidence."""

from __future__ import annotations

import json
import math

import numpy as np
import pandas as pd

from pipelines.config import MISSING_RATE_THRESHOLD, SEVERITY_PENALTY, STUCK_RUN_THRESHOLD
from pipelines.detection.features import FEATURE_MODEL_COLUMNS
from pipelines.quality.metrics import STATION_DAY_COLUMNS

ALERT_KEY = ["locationid", "date_local", "parameter"]
VOTERS = {
    "if_flag": "Isolation Forest",
    "lof_flag": "Local Outlier Factor",
    "dbscan_flag": "DBSCAN",
}
FEATURE_LABELS = {
    "daily_mean": "Daily mean",
    "daily_max": "Daily maximum",
    "z_score": "Daily baseline deviation (z-score)",
    "sustained_elevation_hours": "Longest run above trailing baseline (readings)",
    "mean_shift_ratio": "Daily mean / trailing median",
    "regional_agreement": "Regional agreement feature",
    "hourly_deviation_zscore_max": "Peak hourly baseline deviation",
    "hourly_sustained_elevated_hours_max": "Longest hourly elevation feature",
}
RULE_NAMES = {
    "R1": "Negative readings",
    "R2": "Frozen readings",
    "R3": "Zero-variance readings",
    "R4": "Incomplete readings",
    "R5": "Sensor dropout",
    "R7": "Missing daily archive",
    "R8": "Changed archive columns",
    "R9": "Duplicate readings",
    "R10": "Original-unit mismatch",
    "M1": "Unusual combination of data-health metrics",
}
RULE_METRICS = {
    "R1": [("negative_count_total", "Negative readings")],
    "R2": [("max_stuck_run_max", "Longest unchanged run")],
    "R3": [("zero_variance_params", "Zero-variance sensor/parameter groups")],
    "R4": [("missing_rate_mean", "Mean missing rate")],
    "R5": [
        ("sensors_expected", "Expected sensors"),
        ("sensors_received", "Reporting sensors"),
        ("sensor_dropout_count", "Missing sensors"),
    ],
    "R7": [("file_present", "Expected archive present")],
    "R8": [("schema_changed", "Header changed")],
    "R9": [("duplicate_rate", "Duplicate rate")],
    "R10": [("unit_mismatch_count", "Readings with an original-unit mismatch")],
    "M1": [("total_readings", "Readings"), ("missing_rate_mean", "Mean missing rate")],
}


def _number(value) -> float | None:
    if isinstance(value, (bool, np.bool_)):
        return None
    try:
        number = float(value)
    except (TypeError, ValueError):
        return None
    return number if math.isfinite(number) else None


def _flag(value) -> bool | None:
    if isinstance(value, (bool, np.bool_)):
        return bool(value)
    if isinstance(value, str) and value.lower() in ("true", "false"):
        return value.lower() == "true"
    if _number(value) in (0, 1):
        return bool(_number(value))
    return None


def _text(value) -> str:
    return value if isinstance(value, str) else ""


def _snapshot(value) -> dict:
    if isinstance(value, str):
        try:
            value = json.loads(value, parse_constant=lambda _: None)
        except (ValueError, TypeError):
            return {}
    return value if isinstance(value, dict) else {}


def station_day(frame: pd.DataFrame, locationid: int, day: str) -> pd.DataFrame:
    """Match the full station-day key without modifying cached frames."""
    if frame.empty or not {"locationid", "date_local"} <= set(frame.columns):
        return frame.iloc[:0].copy()
    return frame.loc[
        (pd.to_numeric(frame["locationid"], errors="coerce") == locationid)
        & (frame["date_local"].astype(str) == day)
    ].copy()


def _incident_details(row: dict) -> dict:
    rule = _text(row.get("rule_id"))
    raw = _snapshot(row.get("metric_snapshot"))
    snapshot = {
        key: (_flag(value) if key in ("file_present", "schema_changed") else _number(value))
        for key, value in raw.items()
        if key in STATION_DAY_COLUMNS and key not in ("locationid", "date_local")
    }
    evidence = []
    for key, label in RULE_METRICS.get(rule, []):
        value = snapshot.get(key)
        if value is None:
            continue
        if isinstance(value, bool):
            formatted = "yes" if value else "no"
        elif key in ("missing_rate_mean", "duplicate_rate"):
            formatted = f"{value:.1%}"
        else:
            formatted = f"{value:g}"
        evidence.append(f"{label}: {formatted}")
    if rule == "R2" and evidence:
        evidence.append(f"rule threshold: {STUCK_RUN_THRESHOLD} readings")
    if rule == "R4" and evidence:
        evidence.append(f"rule threshold: {MISSING_RATE_THRESHOLD:.0%}")
    return {
        "rule_id": rule,
        "finding": RULE_NAMES.get(rule, "Recorded quality incident"),
        "severity": _text(row.get("severity")),
        "source": _text(row.get("source")),
        "evidence": "; ".join(evidence) or "Saved metric evidence is unavailable.",
        "metric_snapshot": snapshot,
    }


def explain_alert(
    alerts: pd.DataFrame,
    incidents: pd.DataFrame,
    measurements: pd.DataFrame,
    *,
    locationid: int,
    date_local: str,
    parameter: str,
    summaries: dict | None = None,
) -> dict:
    """Explain a unique saved alert; never reclassify it or invent human review outcomes."""
    selected = station_day(alerts, locationid, date_local)
    if "parameter" not in selected.columns:
        raise ValueError("No saved alert matches this selection")
    selected = selected[selected["parameter"] == parameter]
    if len(selected) != 1:
        raise ValueError("Select exactly one saved station-day-pollutant alert")
    alert = selected.iloc[0]
    warnings = []
    score, trust = _number(alert.get("alert_score")), _number(alert.get("trust_score"))
    has_incident = _flag(alert.get("has_quality_incident"))
    severity = _text(alert.get("max_severity")).lower()
    penalty = SEVERITY_PENALTY.get(severity)
    if severity == "" and has_incident is False:
        penalty = 0.0
    expected = (
        min(1.0, max(0.0, score * (1 - penalty)))
        if score is not None and penalty is not None
        else None
    )
    consistent = (
        expected is not None
        and trust is not None
        and score is not None
        and 0 <= score <= 1
        and 0 <= trust <= 1
        and math.isclose(expected, trust, rel_tol=1e-7, abs_tol=1e-9)
    )
    if not consistent:
        warnings.append(
            "The saved trust score cannot be reconciled with the current severity "
            "policy. The recorded score is shown without a verified calculation."
        )
    status = _text(alert.get("status"))
    expected_status = {True: "quarantined", False: "escalated"}.get(has_incident)
    if expected_status is None or status != expected_status:
        warnings.append("The saved status and quality-incident flag are missing or inconsistent.")

    matching = station_day(incidents, locationid, date_local)
    details = [_incident_details(row) for row in matching.to_dict("records")]
    saved_rules = {
        part.strip() for part in _text(alert.get("incident_rule_ids")).split(",") if part.strip()
    }
    current_rules = {row["rule_id"] for row in details if row["rule_id"]}
    count = _number(alert.get("incident_count"))
    current_severity = max(
        (row["severity"].lower() for row in details),
        key=lambda value: SEVERITY_PENALTY.get(value, -1),
        default="",
    )
    if (
        saved_rules != current_rules
        or (count is not None and count != len(details))
        or (has_incident is not None and has_incident != bool(details))
        or (details and current_severity != severity)
    ):
        warnings.append(
            "Loaded quality incidents do not match the saved fusion decision. "
            "They are current context, not a verified explanation of that build; "
            "refresh data after rebuilding coherent outputs."
        )
    feature_data = _snapshot(alert.get("feature_snapshot"))
    features = {
        key: _number(value) for key, value in feature_data.items() if key in FEATURE_MODEL_COLUMNS
    }
    if not any(value is not None for value in features.values()):
        warnings.append("The saved event-feature snapshot is unavailable.")
    votes = [{"detector": name, "flagged": _flag(alert.get(key))} for key, name in VOTERS.items()]
    agreement = _number(alert.get("agreement_count"))
    known = all(vote["flagged"] is not None for vote in votes)
    if known and agreement != sum(vote["flagged"] for vote in votes):
        warnings.append("Saved detector votes do not match the recorded agreement count.")

    readings = station_day(measurements, locationid, date_local)
    if "parameter" in readings.columns:
        readings = readings[readings["parameter"] == parameter]
    else:
        readings = readings.iloc[:0]
    columns = [
        key
        for key in ("sensor_id", "datetime", "datetime_local", "value", "unit", "original_unit")
        if key in readings.columns
    ]
    readings = (
        readings[columns]
        .reindex(
            columns=[
                "sensor_id",
                "datetime",
                "datetime_local",
                "value",
                "unit",
                "original_unit",
            ]
        )
        .copy()
    )
    if "datetime" in readings.columns:
        readings = readings.sort_values("datetime")
    if readings.empty:
        warnings.append("No matching silver measurements are available for this alert.")
    provenance = {
        stage: _text((summaries or {}).get(stage, {}).get("built_at_utc"))
        for stage in ("layer1", "layer2", "fusion")
    }
    times = {
        stage: pd.to_datetime(value, utc=True, errors="coerce")
        for stage, value in provenance.items()
    }
    if pd.notna(times["fusion"]) and any(
        pd.notna(times[stage]) and times[stage] > times["fusion"] for stage in ("layer1", "layer2")
    ):
        warnings.append("A detector build is newer than fusion; the saved decision may be stale.")
    return {
        "schema_version": 1,
        "alert": {"locationid": int(locationid), "date_local": date_local, "parameter": parameter},
        "decision": {
            "status": status,
            "has_quality_incident": has_incident,
            "alert_score": score,
            "trust_score": trust,
            "max_severity": severity,
            "policy_penalty": penalty,
            "calculation_verified": consistent,
            "current_policy_expected_score": expected,
            "saved_rule_ids": sorted(saved_rules),
            "saved_incident_count": count,
            "human_review": "Not recorded in this dataset",
        },
        "detector_votes": votes,
        "recorded_agreement_count": agreement,
        "features": features,
        "quality_incidents": details,
        "readings": json.loads(readings.to_json(orient="records", date_format="iso")),
        "build_times_utc": provenance,
        "warnings": warnings,
        "interpretation": "Scores rank anomalies and apply a quality policy; they are not "
        "probabilities or verified event labels. Feature values describe "
        "the readings, not per-feature causal model attribution.",
    }
