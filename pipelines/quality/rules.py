"""Deterministic Layer 1 quality rules (R1–R10).

Explicit, auditable incident flags mapped to documented degradation events.
"""

from __future__ import annotations

import json

import pandas as pd

from pipelines.config import MISSING_RATE_THRESHOLD, STUCK_RUN_THRESHOLD
from pipelines.quality.metrics import STATION_DAY_COLUMNS

INCIDENT_COLUMNS = [
    "locationid",
    "date_local",
    "rule_id",
    "incident_type",
    "severity",
    "event_code",
    "metric_snapshot",
    "is_incident",
    "source",
]

# Each rule is a deterministic check over one station-day metric row.
# "check" returns True when that station-day should be flagged as an
# incident. Rules are independent — a single row can trigger several.
_RULES: list[dict] = [
    {
        "rule_id": "R1",
        "incident_type": "validity_violation",
        "severity": "high",
        "event_code": "E4",
        "source": "rule",
        # Any negative reading is physically invalid for these sensors.
        "check": lambda m: m["negative_count_total"] > 0,
    },
    {
        "rule_id": "R2",
        "incident_type": "stuck_sensor",
        "severity": "high",
        "event_code": "E3",
        "source": "rule",
        # A long run of identical consecutive values suggests a frozen sensor.
        "check": lambda m: m["max_stuck_run_max"] >= STUCK_RUN_THRESHOLD,
    },
    {
        "rule_id": "R3",
        "incident_type": "stuck_sensor",
        "severity": "medium",
        "event_code": "E3",
        "source": "rule",
        # Zero variance across enough readings also indicates a stuck sensor,
        # even if the stuck run itself is short.
        "check": lambda m: m["zero_variance_params"] >= 1 and m["total_readings"] >= 6,
    },
    {
        "rule_id": "R4",
        "incident_type": "completeness_gap",
        "severity": "medium",
        "event_code": "E5",
        "source": "rule",
        # Too many missing readings relative to what was expected.
        "check": lambda m: m["missing_rate_mean"] > MISSING_RATE_THRESHOLD,
    },
    {
        "rule_id": "R5",
        "incident_type": "partial_outage",
        "severity": "medium",
        "event_code": "E5/E6",
        "source": "rule",
        # One or more sensors that were reporting previously stopped reporting.
        "check": lambda m: m["sensor_dropout_count"] >= 1,
    },
    {
        "rule_id": "R6",
        "incident_type": "freshness_anomaly",
        "severity": "medium",
        "event_code": "E2",
        "source": "rule",
        # File arrived later than the delivery commitment window.
        "check": lambda m: m["file_lateness_hours"] > 0,
    },
    {
        "rule_id": "R7",
        "incident_type": "missing_file",
        "severity": "high",
        "event_code": "E5",
        "source": "rule",
        # No source file exists at all for this location/day.
        "check": lambda m: not m["file_present"],
    },
    {
        "rule_id": "R8",
        "incident_type": "schema_drift",
        "severity": "medium",
        "event_code": "E7",
        "source": "rule",
        # The source file's column schema changed from the previous day.
        "check": lambda m: bool(m["schema_changed"]),
    },
    {
        "rule_id": "R9",
        "incident_type": "uniqueness_violation",
        "severity": "low",
        "event_code": "",
        "source": "rule",
        # Duplicate readings were found for the same sensor/timestamp/parameter.
        "check": lambda m: m["duplicate_rate"] > 0,
    },
    {
        "rule_id": "R10",
        "incident_type": "conformance_violation",
        "severity": "medium",
        "event_code": "E7",
        "source": "rule",
        # Reported unit didn't match the canonical unit for its parameter.
        "check": lambda m: m["unit_mismatch_count"] > 0,
    },
]


def _snapshot(row: pd.Series) -> str:
    """Serialize the station-day metrics for this row into a JSON string,
    stored alongside each incident for later auditing/debugging."""
    payload = {col: row[col] for col in STATION_DAY_COLUMNS if col in row.index}
    return json.dumps(payload, default=str)


def apply_quality_rules(station_metrics: pd.DataFrame) -> pd.DataFrame:
    """Return one incident row per (locationid, date_local, rule_id) that fires.

    Every rule is checked independently against every station-day row,
    so a single station-day can produce zero, one, or several incidents.
    """
    if station_metrics is None or station_metrics.empty:
        return pd.DataFrame(columns=INCIDENT_COLUMNS)

    incidents: list[dict] = []
    for _, row in station_metrics.iterrows():
        for rule in _RULES:
            if rule["check"](row):
                incidents.append(
                    {
                        "locationid": int(row["locationid"]),
                        "date_local": str(row["date_local"]),
                        "rule_id": rule["rule_id"],
                        "incident_type": rule["incident_type"],
                        "severity": rule["severity"],
                        "event_code": rule["event_code"],
                        "metric_snapshot": _snapshot(row),
                        "is_incident": True,
                        "source": rule["source"],
                    }
                )
    return pd.DataFrame(incidents, columns=INCIDENT_COLUMNS)