"""DataGuard monitoring dashboard.

Serves the gold zone: quality incidents (Layer 1), ranked pollution alerts
(Layer 2), and fused trust-scored alerts. Quarantined alerts are always shown
— the system reduces noise, it never withholds information.

Run locally:  streamlit run dashboard/app.py
"""

import streamlit as st

st.set_page_config(page_title="DataGuard", page_icon="🛡️", layout="wide")

st.title("DataGuard")
st.caption("Trust-aware anomaly detection for global air quality data (OpenAQ)")

tab_alerts, tab_quality, tab_map = st.tabs(
    ["Trust-scored alerts", "Data health (Layer 1)", "Station map"]
)

with tab_alerts:
    st.info(
        "Boilerplate — will show fused Layer 2 alerts with trust scores, "
        "split into escalated and quarantined, queried from gold via Athena "
        "(DuckDB fallback)."
    )

with tab_quality:
    st.info(
        "Boilerplate — will show station-day quality metrics and Layer 1 "
        "incidents: missing readings, stuck sensors, negative values, late files."
    )

with tab_map:
    st.info("Boilerplate — will map monitored stations and active alerts.")
