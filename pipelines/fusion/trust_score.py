"""Join Layer 1 incidents with Layer 2 alerts and assign trust scores.

An alert raised while the underlying data was unhealthy is quarantined —
held back for human review, never deleted or hidden (see docs: public
safety). The headline evaluation metric is how much this reduces false
alerts while retaining genuine events.
"""

from __future__ import annotations

import numpy as np
import pandas as pd

from pipelines.config import (
    FUSION_STATUS_ESCALATED,
    FUSION_STATUS_QUARANTINED,
    SEVERITY_PENALTY,
)

_SEVERITY_RANK = {"low": 1, "medium": 2, "high": 3}

TRUST_ALERT_COLUMNS = [
    "locationid",
    "date_local",
    "parameter",
    "region_id",
    "alert_score",
    "rank",
    "agreement_count",
    "if_flag",
    "lof_flag",
    "dbscan_flag",
    "weak_label",
    "has_quality_incident",
    "max_severity",
    "incident_count",
    "incident_rule_ids",
    "trust_score",
    "status",
    "feature_snapshot",
]

_ALERT_PASS_THROUGH = [
    "locationid",
    "date_local",
    "parameter",
    "region_id",
    "alert_score",
    "rank",
    "agreement_count",
    "if_flag",
    "lof_flag",
    "dbscan_flag",
    "weak_label",
    "feature_snapshot",
]


def _empty_trust_alerts() -> pd.DataFrame:
    return pd.DataFrame(columns=TRUST_ALERT_COLUMNS)


def _max_severity(severities: pd.Series) -> str:
    best = ""
    best_rank = 0
    for value in severities.dropna().astype(str):
        rank = _SEVERITY_RANK.get(value.lower(), 0)
        if rank > best_rank:
            best_rank = rank
            best = value.lower()
    return best


def aggregate_incidents(quality_incidents: pd.DataFrame) -> pd.DataFrame:
    """Collapse Layer 1 incidents to one row per (locationid, date_local)."""
    columns = [
        "locationid",
        "date_local",
        "has_quality_incident",
        "max_severity",
        "incident_count",
        "incident_rule_ids",
    ]
    if quality_incidents is None or quality_incidents.empty:
        return pd.DataFrame(columns=columns)

    frame = quality_incidents.copy()
    frame["locationid"] = frame["locationid"].astype(int)
    frame["date_local"] = frame["date_local"].astype(str)

    rows: list[dict] = []
    for (locationid, date_local), group in frame.groupby(["locationid", "date_local"], sort=False):
        rule_ids = sorted({str(r) for r in group["rule_id"].dropna().unique()})
        rows.append(
            {
                "locationid": int(locationid),
                "date_local": str(date_local),
                "has_quality_incident": True,
                "max_severity": _max_severity(group["severity"]),
                "incident_count": int(len(group)),
                "incident_rule_ids": ",".join(rule_ids),
            }
        )
    return pd.DataFrame(rows, columns=columns)


def fuse(quality_incidents: pd.DataFrame, event_alerts: pd.DataFrame) -> pd.DataFrame:
    """Return alerts with trust_score and status in {escalated, quarantined}."""
    if event_alerts is None or event_alerts.empty:
        return _empty_trust_alerts()

    alerts = event_alerts.copy()
    for col in _ALERT_PASS_THROUGH:
        if col not in alerts.columns:
            alerts[col] = None if col != "alert_score" else 0.0

    alerts["locationid"] = alerts["locationid"].astype(int)
    alerts["date_local"] = alerts["date_local"].astype(str)
    alerts["alert_score"] = pd.to_numeric(alerts["alert_score"], errors="coerce").fillna(0.0)

    summary = aggregate_incidents(quality_incidents)
    merged = alerts.merge(summary, on=["locationid", "date_local"], how="left")

    if "has_quality_incident" not in merged.columns:
        merged["has_quality_incident"] = False
    else:
        merged["has_quality_incident"] = merged["has_quality_incident"].where(
            merged["has_quality_incident"].notna(), False
        )
    merged["has_quality_incident"] = merged["has_quality_incident"].astype(bool)
    if "max_severity" not in merged.columns:
        merged["max_severity"] = ""
    else:
        merged["max_severity"] = merged["max_severity"].where(merged["max_severity"].notna(), "")
    merged["max_severity"] = merged["max_severity"].astype(str)
    if "incident_count" not in merged.columns:
        merged["incident_count"] = 0
    else:
        merged["incident_count"] = pd.to_numeric(merged["incident_count"], errors="coerce")
        merged["incident_count"] = merged["incident_count"].where(merged["incident_count"].notna(), 0)
    merged["incident_count"] = merged["incident_count"].astype(int)
    if "incident_rule_ids" not in merged.columns:
        merged["incident_rule_ids"] = ""
    else:
        merged["incident_rule_ids"] = merged["incident_rule_ids"].where(
            merged["incident_rule_ids"].notna(), ""
        )
    merged["incident_rule_ids"] = merged["incident_rule_ids"].astype(str)

    penalties = merged["max_severity"].map(
        lambda s: SEVERITY_PENALTY.get(str(s).lower(), 0.0) if s else 0.0
    )
    merged["trust_score"] = np.clip(merged["alert_score"] * (1.0 - penalties), 0.0, 1.0)
    merged["status"] = np.where(
        merged["has_quality_incident"],
        FUSION_STATUS_QUARANTINED,
        FUSION_STATUS_ESCALATED,
    )

    return merged[TRUST_ALERT_COLUMNS]
