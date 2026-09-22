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

from dashboard.data import (  # noqa: E402 — needs ROOT on sys.path first
    STATUS_COLORS,
    available_dates,
    build_station_status,
    filter_by_date,
    load_dashboard_frames,
    load_stations,
)
from pipelines.config import load_settings  # noqa: E402

st.set_page_config(page_title="DataGuard", page_icon="🛡️", layout="wide")

ALERT_COLUMNS = [
    "status",
    "trust_score",
    "rank",
    "locationid",
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

INCIDENT_COLUMNS = [
    "locationid",
    "date_local",
    "rule_id",
    "incident_type",
    "severity",
    "event_code",
    "source",
]

MAP_COLUMNS = [
    "locationid",
    "location_name",
    "latitude",
    "longitude",
    "status",
    "date_local",
    "trust_score",
    "max_severity",
    "incident_rule_ids",
]

LEGEND = """
**Legend**
- <span style="color:#e63946">●</span> **escalated** — trusted pollution alert
- <span style="color:#f4a261">●</span> **quarantined** — alert + data-health incident (always shown)
- <span style="color:#7b61ff">●</span> **quality_only** — Layer 1 incident, no fusion alert
- <span style="color:#2a9d8f">●</span> **monitored** — healthy / no active signal
"""


def _show(frame: pd.DataFrame, columns: list[str], sort_by: str | None = None) -> None:
    view = frame[[c for c in columns if c in frame.columns]]
    if sort_by and sort_by in view.columns:
        view = view.sort_values(sort_by, ascending=False)
    st.dataframe(view, use_container_width=True)


st.title("DataGuard")
st.caption("Trust-aware anomaly detection for global air quality data (OpenAQ)")

with st.sidebar:
    st.header("Data roots")
    st.caption("A local directory or an s3:// prefix.")
    settings = load_settings()
    silver_root = st.text_input("Silver root", value=settings.silver_root)
    gold_root = st.text_input("Gold root", value=settings.gold_root)

    frames = load_dashboard_frames(gold_root)
    metrics, incidents, fused = frames["metrics"], frames["incidents"], frames["trust_alerts"]
    stations = load_stations(silver_root)

    st.header("As-of date")
    dates = available_dates(metrics, incidents, fused)
    if dates:
        as_of_date = st.selectbox("Station-day", options=dates, index=len(dates) - 1)
        show_all_dates = st.checkbox("Show all dates in tables", value=False)
    else:
        as_of_date, show_all_dates = None, True
        st.caption("No gold dates yet — run quality / detect / fuse.")

    station_status = build_station_status(stations, incidents, fused, as_of_date=as_of_date)

    st.header("KPIs")
    statuses = station_status["status"] if not station_status.empty else pd.Series(dtype=str)
    st.metric("Stations", len(stations))
    st.metric("Escalated (map)", int((statuses == "escalated").sum()))
    st.metric("Quarantined (map)", int((statuses == "quarantined").sum()))
    st.metric("L1 incidents (day)", len(filter_by_date(incidents, as_of_date)))

# The sidebar filters to one day; the tables optionally show the whole history.
table_date = None if show_all_dates else as_of_date
fused_view = filter_by_date(fused, table_date)
metrics_view = filter_by_date(metrics, table_date)
incidents_view = filter_by_date(incidents, table_date)


tab_alerts, tab_quality, tab_map = st.tabs(
    [
        "Trust-scored alerts",
        "Data health (Layer 1)",
        "Station map",
    ]
)

with tab_alerts:
    if fused_view.empty:
        st.info(
            "No fused alerts yet. Run: `python -m pipelines quality` → "
            "`python -m pipelines detect` → `python -m pipelines fuse` "
            "(the Layer 2 ensemble needs ≥20 feature rows before alerts exist)."
        )

    else:
        escalated = fused_view[
            fused_view["status"] == "escalated"
        ]

        quarantined = fused_view[
            fused_view["status"] == "quarantined"
        ]

        c1, c2, c3, c4 = st.columns(4)

        c1.metric(
            "Total alerts",
            len(fused_view),
        )

        c2.metric(
            "Escalated",
            len(escalated),
        )

        c3.metric(
            "Quarantined",
            len(quarantined),
        )

        c4.metric(
            "Mean trust score",
            f"{fused_view['trust_score'].mean():.2f}",
        )

        # ---------------------------------------------------------
        # Trust Score Trend
        # ---------------------------------------------------------
        # Shows how the average trust score changes over time.
        # The original dashboard only displayed the mean trust score
        # for the selected day. This visualisation provides historical
        # context and makes changes in alert reliability easier to see.

        st.subheader("Trust score trend")

        if (
            "date_local" in fused.columns
            and "trust_score" in fused.columns
        ):
            trust_trend = fused.copy()

            # Convert the date field into a datetime format so that
            # Streamlit can correctly display it on a time-based axis.
            trust_trend["date_local"] = pd.to_datetime(
                trust_trend["date_local"],
                errors="coerce",
            )

            # Ensure trust scores are numeric before calculating
            # averages and plotting the values.
            trust_trend["trust_score"] = pd.to_numeric(
                trust_trend["trust_score"],
                errors="coerce",
            )

            # Remove invalid values, calculate the average trust score
            # for each date, and arrange the results chronologically.
            trust_trend = (
                trust_trend
                .dropna(
                    subset=[
                        "date_local",
                        "trust_score",
                    ]
                )
                .groupby(
                    "date_local",
                    as_index=False,
                )["trust_score"]
                .mean()
                .sort_values("date_local")
            )

            # Display the historical trend only when valid data exists.
            if trust_trend.empty:
                st.caption(
                    "No trust-score history available."
                )
            else:
                st.line_chart(
                    trust_trend,
                    x="date_local",
                    y="trust_score",
                    use_container_width=True,
                )
        # ---------------------------------------------------------
        # Alert Decision Summary
        # ---------------------------------------------------------
        # Summarises how fusion classified alerts so users can quickly
        # compare how many were escalated versus quarantined.

        st.subheader("Alert decision summary")

        if "status" in fused.columns:
            status_counts = (
                fused["status"]
                .value_counts()
                .rename_axis("Decision")
                .reset_index(name="Alerts")
            )

            if status_counts.empty:
                st.caption("No alert decisions available.")
            else:
                st.bar_chart(
                    status_counts,
                    x="Decision",
                    y="Alerts",
                    use_container_width=True,
                )

        # ---------------------------------------------------------
        # Station Drill-down
        # ---------------------------------------------------------
        # Allows users to inspect the alert history of an individual
        # monitoring station instead of only viewing aggregate results.

        st.subheader("Station drill-down")

        if "locationid" in fused.columns:

            # Get all stations that have fusion alerts.
            station_options = sorted(
                fused["locationid"].dropna().unique().tolist()
            )

            if station_options:

                # Let the dashboard user choose a station to investigate.
                selected_station = st.selectbox(
                    "Select station",
                    options=station_options,
                    key="alert_station_selector",
                )

                # Keep only alerts belonging to the selected station.
                station_history = fused[
                    fused["locationid"] == selected_station
                ].copy()

                # Sort station history chronologically.
                station_history["date_local"] = pd.to_datetime(
                    station_history["date_local"],
                    errors="coerce",
                )

                station_history = station_history.sort_values(
                    "date_local"
                )

                # Display a quick summary of the selected station.
                s1, s2, s3 = st.columns(3)

                s1.metric(
                    "Alerts",
                    len(station_history),
                )

                s2.metric(
                    "Average trust score",
                    f"{station_history['trust_score'].mean():.2f}",
                )

                s3.metric(
                    "Quarantined",
                    int(
                        (
                            station_history["status"]
                            == "quarantined"
                        ).sum()
                    ),
                )

                # Plot the station's trust-score history.
                st.caption(
                    "Trust-score history for the selected station"
                )

                st.line_chart(
                    station_history,
                    x="date_local",
                    y="trust_score",
                    use_container_width=True,
                )

                # Provide the underlying alert records for investigation.
                station_cols = [
                    "date_local",
                    "parameter",
                    "status",
                    "trust_score",
                    "alert_score",
                    "has_quality_incident",
                    "max_severity",
                    "incident_rule_ids",
                ]

                _show(station_history, station_cols)

            else:
                st.caption(
                    "No stations with fusion alerts are available."
                )

        st.subheader("Escalated")

        if escalated.empty:
            st.caption("No escalated alerts.")
        else:
            _show(escalated, ALERT_COLUMNS, sort_by="trust_score")

        st.subheader(
            "Quarantined (for review — never deleted)"
        )

        if quarantined.empty:
            st.caption("No quarantined alerts.")
        else:
            _show(quarantined, ALERT_COLUMNS, sort_by="trust_score")

        st.caption(
            "Station colors for these alerts appear on the Station map tab."
        )

with tab_quality:
    if metrics_view.empty:
        st.warning(
            "No Layer 1 metrics found. Run: `python -m pipelines quality`"
        )

    else:
        c1, c2, c3 = st.columns(3)
        c1.metric("Station-days", len(metrics_view))
        c2.metric("Incidents", len(incidents_view))
        c3.metric("Locations", metrics_view["locationid"].nunique())

        st.subheader("Station-day metrics")

        st.dataframe(
            metrics_view,
            use_container_width=True,
        )

        if not incidents_view.empty:
            st.subheader("Quality incidents")
            _show(incidents_view, INCIDENT_COLUMNS)
            st.bar_chart(incidents_view["rule_id"].value_counts())

with tab_map:
    st.markdown(LEGEND, unsafe_allow_html=True)

    map_df = station_status.copy()
    if not map_df.empty:
        map_df["latitude"] = pd.to_numeric(map_df["latitude"], errors="coerce")
        map_df["longitude"] = pd.to_numeric(map_df["longitude"], errors="coerce")
        map_df = map_df.dropna(subset=["latitude", "longitude"])

    if map_df.empty:
        st.warning(
            "No mappable stations. Build the silver zone first: "
            f"`python -m pipelines conform` (silver root: `{silver_root}`)."
        )
    else:
        fallback = STATUS_COLORS["monitored"]
        map_df["color"] = map_df["color"].apply(
            lambda c: [int(v) for v in c] if isinstance(c, (list, tuple)) else fallback
        )
        layer = pdk.Layer(
            "ScatterplotLayer",
            data=map_df,
            get_position="[longitude, latitude]",
            get_fill_color="color",
            get_radius=2500,
            radius_min_pixels=8,
            radius_max_pixels=24,
            pickable=True,
        )
        view = pdk.ViewState(
            latitude=float(map_df["latitude"].mean()),
            longitude=float(map_df["longitude"].mean()),
            zoom=9,
        )
        st.pydeck_chart(
            pdk.Deck(
                layers=[layer],
                initial_view_state=view,
                tooltip={
                    "html": (
                        "<b>{location_name}</b> ({locationid})<br/>"
                        "status: {status}<br/>"
                        "date: {date_local}<br/>"
                        "trust: {trust_score}<br/>"
                        "rules: {incident_rule_ids}"
                    ),
                    "style": {"backgroundColor": "#111", "color": "white"},
                },
            ),
            use_container_width=True,
        )

        if fused.empty:
            st.caption(
                "Fusion alerts empty — the map shows monitored stations and "
                "Layer 1 quality signals only."
            )

        st.subheader("Stations")
        _show(map_df, MAP_COLUMNS)
