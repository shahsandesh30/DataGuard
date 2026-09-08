"""Dashboard data loaders: station geo + Layer 1 / fusion status joins."""

from __future__ import annotations

from pathlib import Path

import pandas as pd

from pipelines.conformance.conform import read_conformed
from pipelines.detection.build import read_event_alerts
from pipelines.fusion.build import read_trust_alerts
from pipelines.quality.build import read_quality_incidents, read_quality_metrics

STATUS_PRIORITY = {
    "escalated": 0,
    "quarantined": 1,
    "quality_only": 2,
    "monitored": 3,
}

STATUS_COLORS = {
    "escalated": [230, 57, 70],
    "quarantined": [244, 162, 97],
    "quality_only": [123, 97, 255],
    "monitored": [42, 157, 143],
}


def load_stations(bronze_root: Path | None = None) -> pd.DataFrame:
    """One row per locationid with median lat/lon and a display name."""
    conformed = read_conformed(bronze_root)
    if conformed is None or conformed.empty:
        return pd.DataFrame(columns=["locationid", "location_name", "lat", "lon"])

    frame = conformed.copy()
    frame["locationid"] = frame["locationid"].astype(int)
    frame["lat"] = pd.to_numeric(frame["lat"], errors="coerce")
    frame["lon"] = pd.to_numeric(frame["lon"], errors="coerce")
    frame = frame.dropna(subset=["lat", "lon"])
    if frame.empty:
        return pd.DataFrame(columns=["locationid", "location_name", "lat", "lon"])

    rows: list[dict] = []
    for locationid, group in frame.groupby("locationid", sort=False):
        names = group["location_name"].dropna().astype(str)
        name = names.mode().iloc[0] if not names.empty else str(locationid)
        rows.append(
            {
                "locationid": int(locationid),
                "location_name": name,
                "lat": float(group["lat"].median()),
                "lon": float(group["lon"].median()),
            }
        )
    return pd.DataFrame(rows)


def load_dashboard_frames(gold_root: Path | None = None) -> dict[str, pd.DataFrame]:
    return {
        "metrics": read_quality_metrics(gold_root),
        "incidents": read_quality_incidents(gold_root),
        "trust_alerts": read_trust_alerts(gold_root),
        "event_alerts": read_event_alerts(gold_root),
    }


def available_dates(*frames: pd.DataFrame) -> list[str]:
    dates: set[str] = set()
    for frame in frames:
        if frame is None or frame.empty or "date_local" not in frame.columns:
            continue
        dates.update(frame["date_local"].astype(str).unique())
    return sorted(dates)


def _filter_by_date(frame: pd.DataFrame, as_of_date: str | None) -> pd.DataFrame:
    if frame is None or frame.empty or not as_of_date or "date_local" not in frame.columns:
        return frame if frame is not None else pd.DataFrame()
    return frame[frame["date_local"].astype(str) == str(as_of_date)].copy()


def _severity_rank(value: str) -> int:
    order = {"high": 3, "medium": 2, "low": 1}
    return order.get(str(value).lower(), 0)


def build_station_status(
    stations: pd.DataFrame,
    incidents: pd.DataFrame,
    trust_alerts: pd.DataFrame,
    *,
    as_of_date: str | None = None,
) -> pd.DataFrame:
    """Join stations to L1/fusion signals and assign map status."""
    if stations is None or stations.empty:
        return pd.DataFrame(
            columns=[
                "locationid",
                "location_name",
                "lat",
                "lon",
                "status",
                "date_local",
                "trust_score",
                "max_severity",
                "incident_rule_ids",
                "color",
            ]
        )

    if as_of_date is None:
        dates = available_dates(incidents, trust_alerts)
        as_of_date = dates[-1] if dates else None

    day_incidents = _filter_by_date(incidents, as_of_date)
    day_alerts = _filter_by_date(trust_alerts, as_of_date)

    incident_rows: list[dict] = []
    if not day_incidents.empty:
        for locationid, group in day_incidents.groupby("locationid", sort=False):
            rule_ids = sorted({str(r) for r in group["rule_id"].dropna().unique()})
            severities = group["severity"].dropna().astype(str)
            max_sev = ""
            if not severities.empty:
                max_sev = max(severities, key=_severity_rank)
            incident_rows.append(
                {
                    "locationid": int(locationid),
                    "has_quality_incident": True,
                    "max_severity": max_sev.lower() if max_sev else "",
                    "incident_rule_ids": ",".join(rule_ids),
                }
            )
    incident_summary = pd.DataFrame(incident_rows)

    alert_rows: list[dict] = []
    if not day_alerts.empty:
        for locationid, group in day_alerts.groupby("locationid", sort=False):
            # Prefer escalated over quarantined when both exist for the station-day.
            statuses = set(group["status"].astype(str))
            if "escalated" in statuses:
                fusion_status = "escalated"
                subset = group[group["status"] == "escalated"]
            elif "quarantined" in statuses:
                fusion_status = "quarantined"
                subset = group[group["status"] == "quarantined"]
            else:
                fusion_status = str(group.iloc[0]["status"])
                subset = group
            alert_rows.append(
                {
                    "locationid": int(locationid),
                    "fusion_status": fusion_status,
                    "trust_score": float(subset["trust_score"].max()),
                }
            )
    alert_summary = pd.DataFrame(alert_rows)

    result = stations.copy()
    result["locationid"] = result["locationid"].astype(int)
    if not incident_summary.empty:
        result = result.merge(incident_summary, on="locationid", how="left")
    else:
        result["has_quality_incident"] = False
        result["max_severity"] = ""
        result["incident_rule_ids"] = ""

    if not alert_summary.empty:
        result = result.merge(alert_summary, on="locationid", how="left")
    else:
        result["fusion_status"] = pd.NA
        result["trust_score"] = pd.NA

    result["has_quality_incident"] = result.get(
        "has_quality_incident", pd.Series(False, index=result.index)
    )
    result["has_quality_incident"] = result["has_quality_incident"].where(
        result["has_quality_incident"].notna(), False
    ).astype(bool)
    result["max_severity"] = result.get("max_severity", pd.Series("", index=result.index))
    result["max_severity"] = result["max_severity"].where(result["max_severity"].notna(), "").astype(str)
    result["incident_rule_ids"] = result.get(
        "incident_rule_ids", pd.Series("", index=result.index)
    )
    result["incident_rule_ids"] = result["incident_rule_ids"].where(
        result["incident_rule_ids"].notna(), ""
    ).astype(str)

    statuses: list[str] = []
    for _, row in result.iterrows():
        fusion = row.get("fusion_status")
        if pd.notna(fusion) and str(fusion) == "escalated":
            statuses.append("escalated")
        elif pd.notna(fusion) and str(fusion) == "quarantined":
            statuses.append("quarantined")
        elif bool(row.get("has_quality_incident")):
            statuses.append("quality_only")
        else:
            statuses.append("monitored")
    result["status"] = statuses
    result["date_local"] = as_of_date or ""
    result["color"] = result["status"].map(STATUS_COLORS)
    result["priority"] = result["status"].map(STATUS_PRIORITY).fillna(99)
    return result.sort_values(["priority", "locationid"]).drop(columns=["priority"])
