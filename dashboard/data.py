"""Dashboard data loaders: station geography, Layer 1 / fusion status, model runs."""

from __future__ import annotations

import json
import re
from datetime import datetime
from pathlib import Path

import pandas as pd

from pipelines import storage
from pipelines.config import load_settings
from pipelines.conformance.conform import read_silver
from pipelines.detection.benchmark import BENCHMARK_FILE
from pipelines.detection.build import read_event_alerts
from pipelines.detection.evaluation import RUN_METRIC_COLUMNS, read_run_history
from pipelines.fusion.build import read_trust_alerts
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

BUILD_SUMMARIES = {
    "silver": "_build.json",
    "layer1": "layer1/_build.json",
    "layer2": "layer2/_detection_build.json",
    "fusion": "fusion/_build.json",
}

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


def load_build_summaries(
    silver_root: str | Path | None = None,
    gold_root: str | Path | None = None,
) -> dict[str, dict]:
    """Load successful stage summaries used for dashboard freshness checks."""
    settings = load_settings()
    silver = silver_root or settings.silver_root
    gold = gold_root or settings.gold_root
    roots = {"silver": silver, "layer1": gold, "layer2": gold, "fusion": gold}
    summaries: dict[str, dict] = {}
    for stage, relative_path in BUILD_SUMMARIES.items():
        text = storage.read_text(storage.join(roots[stage], relative_path))
        if text:
            try:
                summaries[stage] = json.loads(text)
            except json.JSONDecodeError:
                summaries[stage] = {}
    return summaries


def latest_build_time(summaries: dict[str, dict]) -> datetime | None:
    """Return the final fusion timestamp, which proves the whole run succeeded."""
    value = summaries.get("fusion", {}).get("built_at_utc")
    if not value:
        return None
    try:
        return datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    except ValueError:
        return None


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


# --------------------------------------------------------------------------- #
# Models
# --------------------------------------------------------------------------- #

MODEL_COLUMNS = ["layer", "model", "trained", "run_id", "built_at_utc", "model_path"]

# The run id is the history folder model_store writes each fit into.
_RUN_ID = re.compile(r"run=([^/\\]+)")


def _run_id(model_path: str | None) -> str:
    match = _RUN_ID.search(model_path or "")
    return match.group(1) if match else ""


def last_run_models(summaries: dict[str, dict]) -> pd.DataFrame:
    """One row per model the latest Layer 1 and Layer 2 builds trained or skipped.

    Read from the build summaries rather than the model store: the summaries
    live in gold, which the dashboard may read, and they say which models a
    build *skipped* for lack of history, which a model listing cannot.
    """
    rows = []
    layer1 = summaries.get("layer1")
    if layer1:
        rows.append(
            {
                "layer": "Layer 1",
                "model": "isolation_forest",
                "trained": bool(layer1.get("model_trained")),
                "run_id": _run_id(layer1.get("model_path")),
                "built_at_utc": layer1.get("built_at_utc", ""),
                "model_path": layer1.get("model_path") or "",
            }
        )
    layer2 = summaries.get("layer2")
    if layer2:
        paths = layer2.get("model_paths") or {}
        for parameter in [
            *layer2.get("parameters_trained", []),
            *layer2.get("parameters_skipped", []),
        ]:
            path = paths.get(parameter)
            rows.append(
                {
                    "layer": "Layer 2",
                    "model": f"ensemble_{parameter}",
                    "trained": parameter in layer2.get("parameters_trained", []),
                    "run_id": _run_id(path),
                    "built_at_utc": layer2.get("built_at_utc", ""),
                    "model_path": path or "",
                }
            )
    return pd.DataFrame(rows, columns=MODEL_COLUMNS)


def load_model_runs(gold_root: str | Path | None = None) -> pd.DataFrame:
    """Layer 2 per-run evaluation history, oldest first, with a parsed timestamp."""
    history = read_run_history(gold_root)
    if history.empty or "run_at" not in history.columns:
        return history
    history = history.copy()
    history["parameter"] = history["parameter"].astype(str)
    history["run_at"] = pd.to_datetime(history["run_at"], utc=True, errors="coerce")
    # A metric that was undefined for a run (no alerts, no weak labels) is stored
    # as null; coerce so object columns of Nones still chart as numbers.
    for column in set(RUN_METRIC_COLUMNS) - {"parameter", "run_at", "ensemble_trained"}:
        if column in history.columns:
            history[column] = pd.to_numeric(history[column], errors="coerce")
    return history.dropna(subset=["run_at"]).sort_values("run_at").reset_index(drop=True)


def latest_model_runs(history: pd.DataFrame) -> pd.DataFrame:
    """The most recent evaluation row for each parameter."""
    if history.empty or "parameter" not in history.columns:
        return history
    return (
        history.sort_values("run_at")
        .groupby("parameter", sort=True)
        .tail(1)
        .sort_values("parameter")
        .reset_index(drop=True)
    )


def load_benchmark(gold_root: str | Path | None = None) -> dict | None:
    """The offline Layer 2 benchmark report, or ``None`` if never run."""
    gold = gold_root or load_settings().gold_root
    text = storage.read_text(storage.join(gold, "layer2", BENCHMARK_FILE))
    if not text:
        return None
    try:
        return json.loads(text)
    except json.JSONDecodeError:
        return None
