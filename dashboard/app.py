"""DataGuard monitoring dashboard.

Serves the gold zone: quality incidents (Layer 1), ranked pollution alerts
(Layer 2), and fused trust-scored alerts. Quarantined alerts are always shown
— the system reduces noise, it never withholds information.

Run locally:  streamlit run dashboard/app.py
"""

from __future__ import annotations

import sys
from pathlib import Path

import pandas as pd
import pydeck as pdk
import streamlit as st

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from dashboard.data import (
    STATUS_COLORS,
    available_dates,
    build_station_status,
    load_dashboard_frames,
    load_stations,
)

st.set_page_config(page_title="DataGuard", page_icon="🛡️", layout="wide")

DISPLAY_COLS = [
    "status",
    "trust_score",
    "rank",
    "location_id",
    "date_local",
    "parameter",
    "alert_score",
    "agreement_count",
    "has_quality_incident",
    "max_severity",
    "incident_rule_ids",
    "if_flag",
    "lof_flag",
    "dbscan_flag",
    "weak_label",
]

STATUS_HEX = {
    "escalated": "#e63946",
    "quarantined": "#f4a261",
    "quality_only": "#7b61ff",
    "monitored": "#2a9d8f",
}


def _filter_frame(frame: pd.DataFrame, as_of_date: str | None, all_dates: bool) -> pd.DataFrame:
    if frame is None or frame.empty or all_dates or not as_of_date:
        return frame if frame is not None else pd.DataFrame()
    if "date_local" not in frame.columns:
        return frame
    return frame[frame["date_local"].astype(str) == str(as_of_date)].copy()


def _rgb_list(color) -> list[int]:
    if isinstance(color, list):
        return [int(c) for c in color]
    return list(STATUS_COLORS["monitored"])


st.title("DataGuard")
st.caption("Trust-aware anomaly detection for global air quality data (OpenAQ)")

with st.sidebar:
    st.header("Data roots")
    bronze_root = Path(
        st.text_input("Bronze root", value=str(ROOT / "data" / "bronze"))
    )
    gold_root = Path(st.text_input("Gold root", value=str(ROOT / "data" / "gold")))

    frames = load_dashboard_frames(gold_root)
    metrics = frames["metrics"]
    incidents = frames["incidents"]
    fused = frames["trust_alerts"]
    stations = load_stations(bronze_root)

    dates = available_dates(metrics, incidents, fused)
    st.header("As-of date")
    if dates:
        as_of_date = st.selectbox("Station-day", options=dates, index=len(dates) - 1)
        all_dates = st.checkbox("Show all dates in tables", value=False)
    else:
        as_of_date = None
        all_dates = True
        st.caption("No gold dates yet — run quality / detect / fuse.")

    station_status = build_station_status(
        stations, incidents, fused, as_of_date=as_of_date
    )

    st.header("KPIs")
    n_stations = len(stations)
    n_escalated = (
        int((station_status["status"] == "escalated").sum()) if not station_status.empty else 0
    )
    n_quarantined = (
        int((station_status["status"] == "quarantined").sum()) if not station_status.empty else 0
    )
    day_incidents = _filter_frame(incidents, as_of_date, all_dates=False)
    n_quality = len(day_incidents) if not day_incidents.empty else 0

    st.metric("Stations", n_stations)
    st.metric("Escalated (map)", n_escalated)
    st.metric("Quarantined (map)", n_quarantined)
    st.metric("L1 incidents (day)", n_quality)

tab_alerts, tab_quality, tab_map = st.tabs(
    ["Trust-scored alerts", "Data health (Layer 1)", "Station map"]
)

fused_view = _filter_frame(fused, as_of_date, all_dates)
metrics_view = _filter_frame(metrics, as_of_date, all_dates)
incidents_view = _filter_frame(incidents, as_of_date, all_dates)

with tab_alerts:
    if fused_view.empty:
        st.info(
            "No fused alerts yet. Run: `python -m pipelines quality` → "
            "`python -m pipelines detect` → `python -m pipelines fuse` "
            "(Layer 2 ensemble needs ≥20 feature rows before alerts exist)."
        )
    else:
        escalated = fused_view[fused_view["status"] == "escalated"]
        quarantined = fused_view[fused_view["status"] == "quarantined"]

        c1, c2, c3, c4 = st.columns(4)
        c1.metric("Total alerts", len(fused_view))
        c2.metric("Escalated", len(escalated))
        c3.metric("Quarantined", len(quarantined))
        c4.metric("Mean trust score", f"{fused_view['trust_score'].mean():.2f}")

        cols = [c for c in DISPLAY_COLS if c in fused_view.columns]

        st.subheader("Escalated")
        if escalated.empty:
            st.caption("No escalated alerts.")
        else:
            st.dataframe(
                escalated[cols].sort_values("trust_score", ascending=False),
                use_container_width=True,
            )

        st.subheader("Quarantined (for review — never deleted)")
        if quarantined.empty:
            st.caption("No quarantined alerts.")
        else:
            st.dataframe(
                quarantined[cols].sort_values("trust_score", ascending=False),
                use_container_width=True,
            )
        st.caption("Station colors for these alerts appear on the Station map tab.")

with tab_quality:
    if metrics_view.empty:
        st.warning("No Layer 1 metrics found. Run: `python -m pipelines quality`")
    else:
        c1, c2, c3 = st.columns(3)
        c1.metric("Station-days", len(metrics_view))
        c2.metric("Incidents", len(incidents_view))
        c3.metric("Locations", metrics_view["location_id"].nunique())

        st.subheader("Station-day metrics")
        st.dataframe(metrics_view, use_container_width=True)

        if not incidents_view.empty:
            st.subheader("Quality incidents")
            st.dataframe(
                incidents_view[
                    [
                        "location_id",
                        "date_local",
                        "rule_id",
                        "incident_type",
                        "severity",
                        "event_code",
                        "source",
                    ]
                ],
                use_container_width=True,
            )
            st.bar_chart(incidents_view["rule_id"].value_counts())

with tab_map:
    st.markdown(
        """
**Legend**
- <span style="color:#e63946">●</span> **escalated** — trusted pollution alert
- <span style="color:#f4a261">●</span> **quarantined** — alert + data-health incident (always shown)
- <span style="color:#7b61ff">●</span> **quality_only** — Layer 1 incident, no fusion alert
- <span style="color:#2a9d8f">●</span> **monitored** — healthy / no active signal
""",
        unsafe_allow_html=True,
    )

    if stations.empty:
        st.warning(
            "No station coordinates found. Ingest bronze and ensure "
            "`python -m pipelines` can read conformed lat/lon "
            f"(bronze root: `{bronze_root}`)."
        )
    elif station_status.empty:
        st.warning("Stations loaded but no mappable rows after status join.")
    else:
        map_df = station_status.copy()
        map_df["color"] = map_df["color"].apply(_rgb_list)
        map_df["lat"] = pd.to_numeric(map_df["lat"], errors="coerce")
        map_df["lon"] = pd.to_numeric(map_df["lon"], errors="coerce")
        map_df = map_df.dropna(subset=["lat", "lon"])

        if map_df.empty:
            st.warning("Stations have no valid lat/lon values.")
        else:
            mid_lat = float(map_df["lat"].mean())
            mid_lon = float(map_df["lon"].mean())
            layer = pdk.Layer(
                "ScatterplotLayer",
                data=map_df,
                get_position="[lon, lat]",
                get_fill_color="color",
                get_radius=2500,
                radius_min_pixels=8,
                radius_max_pixels=24,
                pickable=True,
            )
            view = pdk.ViewState(latitude=mid_lat, longitude=mid_lon, zoom=9)
            tooltip = {
                "html": (
                    "<b>{location_name}</b> ({location_id})<br/>"
                    "status: {status}<br/>"
                    "date: {date_local}<br/>"
                    "trust: {trust_score}<br/>"
                    "rules: {incident_rule_ids}"
                ),
                "style": {"backgroundColor": "#111", "color": "white"},
            }
            st.pydeck_chart(
                pdk.Deck(layers=[layer], initial_view_state=view, tooltip=tooltip),
                use_container_width=True,
            )

            if fused.empty:
                st.caption(
                    "Fusion alerts empty — map shows monitored stations and "
                    "Layer 1 quality signals only."
                )

            table_cols = [
                "location_id",
                "location_name",
                "lat",
                "lon",
                "status",
                "date_local",
                "trust_score",
                "max_severity",
                "incident_rule_ids",
            ]
            st.subheader("Stations")
            display = map_df[[c for c in table_cols if c in map_df.columns]].copy()
            st.dataframe(display, use_container_width=True)
