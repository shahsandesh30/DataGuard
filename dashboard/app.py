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
import streamlit as st

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from pipelines.quality.build import read_quality_incidents, read_quality_metrics

st.set_page_config(page_title="DataGuard", page_icon="🛡️", layout="wide")

st.title("DataGuard")
st.caption("Trust-aware anomaly detection for global air quality data (OpenAQ)")

tab_alerts, tab_quality, tab_map = st.tabs(
    ["Trust-scored alerts", "Data health (Layer 1)", "Station map"]
)

with tab_alerts:
    gold_root = ROOT / "data" / "gold"
    from pipelines.detection.build import read_event_alerts

    alerts = read_event_alerts(gold_root)
    if alerts.empty:
        st.info(
            "No Layer 2 alerts yet. Run: `python -m pipelines detect` "
            "(ensemble requires ≥20 feature rows)."
        )
    else:
        c1, c2, c3 = st.columns(3)
        c1.metric("Alerts", len(alerts))
        c2.metric("Locations", alerts["location_id"].nunique())
        c3.metric("Detector agreement (avg)", f"{alerts['agreement_count'].mean():.1f}")
        st.subheader("Ranked pollution event alerts (Layer 2)")
        st.dataframe(
            alerts[
                [
                    "rank",
                    "location_id",
                    "date_local",
                    "parameter",
                    "alert_score",
                    "agreement_count",
                    "if_flag",
                    "lof_flag",
                    "dbscan_flag",
                    "weak_label",
                ]
            ],
            use_container_width=True,
        )
        st.caption(
            "Fusion trust scoring is deferred — alerts shown here are raw Layer 2 output."
        )

with tab_quality:
    gold_root = ROOT / "data" / "gold"
    metrics = read_quality_metrics(gold_root)
    incidents = read_quality_incidents(gold_root)

    if metrics.empty:
        st.warning("No Layer 1 metrics found. Run: `python -m pipelines quality`")
    else:
        c1, c2, c3 = st.columns(3)
        c1.metric("Station-days", len(metrics))
        c2.metric("Incidents", len(incidents))
        c3.metric("Locations", metrics["location_id"].nunique())

        st.subheader("Station-day metrics")
        st.dataframe(metrics, use_container_width=True)

        if not incidents.empty:
            st.subheader("Quality incidents")
            st.dataframe(
                incidents[
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
            st.bar_chart(incidents["rule_id"].value_counts())

with tab_map:
    st.info("Boilerplate — will map monitored stations and active alerts.")
