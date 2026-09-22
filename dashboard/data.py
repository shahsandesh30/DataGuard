"""Dashboard data loaders: station geography joined to Layer 1 / fusion status."""

from __future__ import annotations

from pathlib import Path

import pandas as pd

from pipelines.conformance.conform import read_silver
from pipelines.fusion.build import read_event_alerts, read_trust_alerts
from pipelines.quality.build import read_quality_incidents, read_quality_metrics

STATION_COLUMNS = ["locationid", "location_name", "latitude", "longitude"]

STATUS_COLORS = {
    "escalated": [230, 57, 70],
    "quarantined": [244, 162, 97],
    "quality_only": [123, 97, 255],
    "monitored": [42, 157, 143],
}

# Lower sorts first on the map table — worst news at the top.
STATUS_PRIORITY = {"escalated": 0, "quarantined": 1, "quality_only": 2, "monitored": 3}

_SEVERITY_RANK = {"high": 3, "medium": 2, "low": 1}


def load_stations(silver_root: str | Path | None = None) -> pd.DataFrame:
    """One row per locationid with median coordinates and a display name."""
    silver = read_silver(silver_root)
    if silver.empty:
        return pd.DataFrame(columns=STATION_COLUMNS)

    frame = silver.astype({"locationid": int}).dropna(subset=["latitude", "longitude"])
    if frame.empty:
        return pd.DataFrame(columns=STATION_COLUMNS)

    grouped = frame.groupby("locationid", sort=False)
    stations = pd.DataFrame(
        {
            "location_name": grouped["location_name"].agg(
                lambda names: names.mode().iloc[0] if not names.mode().empty else ""
            ),
            "latitude": grouped["latitude"].median(),
            "longitude": grouped["longitude"].median(),
        }
    ).reset_index()
    stations["location_name"] = stations["location_name"].astype(str)
    return stations.loc[:, STATION_COLUMNS]


def load_dashboard_frames(gold_root: str | Path | None = None) -> dict[str, pd.DataFrame]:
    return {
        "metrics": read_quality_metrics(gold_root),
        "incidents": read_quality_incidents(gold_root),
        "trust_alerts": read_trust_alerts(gold_root),
        "event_alerts": read_event_alerts(gold_root),
    }


def available_dates(*frames: pd.DataFrame) -> list[str]:
    dates: set[str] = set()
    for frame in frames:
        if frame is not None and not frame.empty and "date_local" in frame.columns:
            dates.update(frame["date_local"].astype(str).unique())
    return sorted(dates)


def filter_by_date(frame: pd.DataFrame, as_of_date: str | None) -> pd.DataFrame:
    """Restrict a frame to one station-day. ``None`` means no filtering."""
    if frame is None:
        return pd.DataFrame()
    if frame.empty or not as_of_date or "date_local" not in frame.columns:
        return frame
    return frame[frame["date_local"].astype(str) == str(as_of_date)].copy()


def _incident_summary(day_incidents: pd.DataFrame) -> pd.DataFrame:
    """One row per station: worst severity and the rules that fired."""
    columns = ["locationid", "has_quality_incident", "max_severity", "incident_rule_ids"]
    if day_incidents.empty:
        return pd.DataFrame(columns=columns)

    rows = []
    for locationid, group in day_incidents.groupby("locationid", sort=False):
        severities = group["severity"].dropna().astype(str).str.lower()
        rows.append(
            {
                "locationid": int(locationid),
                "has_quality_incident": True,
                "max_severity": max(severities, key=lambda s: _SEVERITY_RANK.get(s, 0))
                if not severities.empty
                else "",
                "incident_rule_ids": ",".join(
                    sorted({str(r) for r in group["rule_id"].dropna().unique()})
                ),
            }
        )
    return pd.DataFrame(rows, columns=columns)


def _alert_summary(day_alerts: pd.DataFrame) -> pd.DataFrame:
    """One row per station: fusion status, escalated winning over quarantined."""
    columns = ["locationid", "fusion_status", "trust_score"]
    if day_alerts.empty:
        return pd.DataFrame(columns=columns)

    rows = []
    for locationid, group in day_alerts.groupby("locationid", sort=False):
        statuses = set(group["status"].astype(str))
        status = next(
            (s for s in ("escalated", "quarantined") if s in statuses),
            str(group.iloc[0]["status"]),
        )
        subset = group[group["status"].astype(str) == status]
        rows.append(
            {
                "locationid": int(locationid),
                "fusion_status": status,
                "trust_score": float(subset["trust_score"].max()),
            }
        )
    return pd.DataFrame(rows, columns=columns)


def build_station_status(
    stations: pd.DataFrame,
    incidents: pd.DataFrame,
    trust_alerts: pd.DataFrame,
    *,
    as_of_date: str | None = None,
) -> pd.DataFrame:
    """Join stations to Layer 1 and fusion signals and assign a map status."""
    columns = [
        *STATION_COLUMNS,
        "status",
        "date_local",
        "trust_score",
        "max_severity",
        "incident_rule_ids",
        "color",
    ]
    if stations is None or stations.empty:
        return pd.DataFrame(columns=columns)

    if as_of_date is None:
        dates = available_dates(incidents, trust_alerts)
        as_of_date = dates[-1] if dates else None

    result = stations.copy()
    result["locationid"] = result["locationid"].astype(int)
    result = result.merge(
        _incident_summary(filter_by_date(incidents, as_of_date)), on="locationid", how="left"
    ).merge(_alert_summary(filter_by_date(trust_alerts, as_of_date)), on="locationid", how="left")

    result["has_quality_incident"] = result["has_quality_incident"].fillna(False).astype(bool)
    result["max_severity"] = result["max_severity"].fillna("").astype(str)
    result["incident_rule_ids"] = result["incident_rule_ids"].fillna("").astype(str)

    # A fusion alert always wins; a Layer 1 incident on its own is quality_only.
    result["status"] = result["fusion_status"].where(
        result["fusion_status"].notna(),
        result["has_quality_incident"].map({True: "quality_only", False: "monitored"}),
    )
    result["date_local"] = as_of_date or ""
    result["color"] = result["status"].map(STATUS_COLORS)

    order = result["status"].map(STATUS_PRIORITY).fillna(99)
    return result.assign(_order=order).sort_values(["_order", "locationid"]).drop(columns="_order")
