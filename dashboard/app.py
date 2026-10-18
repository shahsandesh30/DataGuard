"""DataGuard monitoring dashboard.

Serves the gold zone: quality incidents (Layer 1), ranked pollution alerts
(Layer 2), and fused trust-scored alerts. Quarantined alerts are always shown
— the system reduces noise, it never withholds information.

Run locally:  streamlit run dashboard/app.py
"""

from __future__ import annotations

import logging
import sys
from contextlib import suppress
from datetime import UTC, datetime, timedelta
from pathlib import Path

import pandas as pd
import pydeck as pdk
import streamlit as st

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from dashboard.alert_view import render_alert_explorer
from dashboard.data import (  # noqa: E402 — needs ROOT on sys.path first
    STATUS_COLORS,
    available_dates,
    build_station_status,
    filter_by_date,
    last_run_models,
    latest_build_time,
    latest_model_runs,
    load_benchmark,
    load_build_summaries,
    load_dashboard_frames,
    load_model_runs,
    load_stations,
)
from dashboard.runtime import apply_runtime_secrets, is_public_dashboard  # noqa: E402
from pipelines.config import load_settings  # noqa: E402
from pipelines.conformance.conform import read_silver

st.set_page_config(page_title="DataGuard", page_icon="🛡️", layout="wide")

logger = logging.getLogger(__name__)

with suppress(Exception):  # No secrets file is normal for a local dashboard.
    apply_runtime_secrets(dict(st.secrets))


@st.cache_data(ttl=300, show_spinner="Loading DataGuard lake data…")
def _load_dashboard_data(silver_root: str, gold_root: str):
    measurements = read_silver(silver_root)
    return (
        load_dashboard_frames(gold_root),
        load_stations(silver_root, measurements=measurements),
        load_build_summaries(silver_root, gold_root),
        load_model_runs(gold_root),
        load_benchmark(gold_root),
        measurements,
    )

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

EVALUATION_COLUMNS = [
    "parameter",
    "run_at",
    "ensemble_trained",
    "feature_rows",
    "alert_rows",
    "flag_rate",
    "weak_label_lift",
    "precision_at_k",
    "weak_label_agreement_rate",
    "weak_label_base_rate",
    "detector_agreement_mean",
    "avg_alert_score",
]

# Metrics worth plotting across runs, with the label shown in the picker.
TREND_METRICS = {
    "weak_label_lift": "Weak-label lift",
    "precision_at_k": "Weak-label Precision@3",
    "flag_rate": "Flag rate",
    "detector_agreement_mean": "Mean detector agreement",
    "avg_alert_score": "Mean alert score",
}

LEGEND = """
**Legend**
- <span style="color:#e63946">●</span> **escalated** — alert with no coincident quality incident
- <span style="color:#f4a261">●</span> **quarantined** — alert + data-health incident (always shown)
- <span style="color:#7b61ff">●</span> **quality_only** — Layer 1 incident, no fusion alert
- <span style="color:#2a9d8f">●</span> **monitored** — no active signal recorded
"""


def _show(frame: pd.DataFrame, columns: list[str], sort_by: str | None = None) -> None:
    view = frame[[c for c in columns if c in frame.columns]]
    if sort_by and sort_by in view.columns:
        view = view.sort_values(sort_by, ascending=False)
    st.dataframe(view, width="stretch")


st.title("DataGuard")
st.caption("Trust-aware anomaly detection for global air quality data (OpenAQ)")

with st.sidebar:
    st.header("Data roots")
    settings = load_settings()
    if is_public_dashboard():
        silver_root, gold_root = settings.silver_root, settings.gold_root
        st.caption("Connected to the managed DataGuard lake.")
    else:
        st.caption("A local directory or an s3:// prefix.")
        silver_root = st.text_input("Silver root", value=settings.silver_root)
        gold_root = st.text_input("Gold root", value=settings.gold_root)

    if st.button("Refresh data", width="stretch"):
        _load_dashboard_data.clear()
        st.rerun()

    try:
        loaded = _load_dashboard_data(silver_root, gold_root)
        frames, stations, build_summaries, model_runs, benchmark, measurements = loaded
    except Exception as exc:  # noqa: BLE001 — the public UI must fail closed and clearly
        logger.exception("Dashboard data load failed")
        st.error("DataGuard could not read its configured data lake.")
        st.caption(
            "Check the S3 roots and read-only AWS credentials, then use Refresh data. "
            f"Error type: {type(exc).__name__}."
        )
        st.stop()

    metrics, incidents, fused = frames["metrics"], frames["incidents"], frames["trust_alerts"]

    built_at = latest_build_time(build_summaries)
    if built_at is not None:
        if built_at.tzinfo is None:
            built_at = built_at.replace(tzinfo=UTC)
        st.caption(f"Last successful build: {built_at.astimezone(UTC):%Y-%m-%d %H:%M UTC}")
        if datetime.now(UTC) - built_at.astimezone(UTC) > timedelta(hours=36):
            st.warning("Pipeline data is stale: the last successful build was over 36 hours ago.")
    else:
        st.warning("No timestamped build summary was found.")

    st.header("As-of date")
    dates = available_dates(metrics, incidents, fused)
    if dates:
        as_of_date = st.selectbox("Station-day", options=dates, index=len(dates) - 1)
        st.caption(f"Latest available station-day: {dates[-1]}")
        show_all_dates = st.checkbox("Show all dates in tables", value=False, key="show_all_dates")
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


tab_alerts, tab_quality, tab_map, tab_models = st.tabs(
    [
        "Trust-scored alerts",
        "Data health (Layer 1)",
        "Station map",
        "Models",
    ]
)

with tab_alerts:
    if fused_view.empty:
        if fused.empty:
            st.info("No saved fusion alerts are available in this dataset.")
        else:
            st.info("No alerts match this local date. Choose another date or show all dates.")

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

        # Explain the metrics without changing the fusion decision policy.
        with st.expander("ℹ️ Understanding trust scores and alert decisions"):
            st.markdown(
                """
                **Trust scores** summarise confidence in the data supporting an alert.
                Higher scores indicate stronger supporting data quality; lower scores
                indicate that the evidence should be treated with greater caution.

                - **Escalated:** an anomaly alert without a coincident quality incident.
                - **Quarantined:** an anomaly alert with a data-health incident.
                It remains visible for review and is never deleted from this view.

                Review the detector evidence and Layer 1 quality incidents before
                interpreting an individual alert. A trust score is not a probability
                that an anomaly is genuine.
                """
            )

        # Interactive filtering and export for review; does not change fusion decisions.
        st.subheader("Filter and export alerts")
        filter_status_col, filter_pollutant_col = st.columns(2)
        with filter_status_col:
            status_options = sorted(fused_view["status"].dropna().astype(str).unique().tolist())
            selected_status = st.selectbox(
                "Alert status", ["All statuses", *status_options], key="export_status_filter"
            )
        with filter_pollutant_col:
            pollutant_options = (
                sorted(fused_view["parameter"].dropna().astype(str).unique().tolist())
                if "parameter" in fused_view.columns else []
            )
            selected_pollutant = st.selectbox(
                "Pollutant", ["All pollutants", *pollutant_options],
                key="export_pollutant_filter",
            )

        export_view = fused_view.copy()
        if selected_status != "All statuses":
            export_view = export_view.loc[export_view["status"] == selected_status]
        if selected_pollutant != "All pollutants":
            export_view = export_view.loc[export_view["parameter"] == selected_pollutant]

        st.caption(
            f"Showing {len(export_view)} of {len(fused_view)} alerts for export. "
            "Filters apply only to the preview and CSV below; "
            "the alert explorer, charts, and metrics remain unchanged."
        )
        if export_view.empty:
            st.info("No alerts match these filters. Try a different status or pollutant.")
        else:
            st.dataframe(
                export_view[[c for c in ALERT_COLUMNS if c in export_view.columns]],
                width="stretch",
            )
        st.download_button(
            "Download filtered alerts (CSV)",
            data=export_view.to_csv(index=False).encode("utf-8-sig"),
            file_name="dataguard_filtered_alerts.csv",
            mime="text/csv",
            disabled=export_view.empty,
            key="download_filtered_alerts",
        )

        render_alert_explorer(fused_view, incidents, measurements, build_summaries)

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
                    width="stretch",
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
                    width="stretch",
                )

        with st.expander("History for the selected alert station"):
            selected_station = st.session_state.get("explain_station")
            history = fused[fused["locationid"] == selected_station].copy()
            if not history.empty:
                history["date_local"] = pd.to_datetime(history["date_local"], errors="coerce")
                st.line_chart(history, x="date_local", y="trust_score", width="stretch")
                _show(history, ALERT_COLUMNS)

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
            width="stretch",
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
            width="stretch",
        )

        if fused.empty:
            st.caption(
                "Fusion alerts empty — the map shows monitored stations and "
                "Layer 1 quality signals only."
            )

        st.subheader("Stations")
        _show(map_df, MAP_COLUMNS)

with tab_models:
    st.subheader("Last run")
    models = last_run_models(build_summaries)
    if models.empty:
        st.info("No Layer 1 or Layer 2 build summary yet. Run: `python -m pipelines run`.")
    else:
        trained = models[models["trained"]]
        c1, c2, c3 = st.columns(3)
        c1.metric("Models trained", f"{len(trained)} of {len(models)}")
        c2.metric("Layer 1 model incidents", build_summaries.get("layer1", {}).get(
            "model_incidents", 0
        ))
        c3.metric("Layer 2 alerts", build_summaries.get("layer2", {}).get("alert_rows", 0))
        # Paths name the bucket, which public mode keeps out of view.
        model_columns = [
            c for c in models.columns if not (is_public_dashboard() and c == "model_path")
        ]
        _show(models, model_columns)
        st.caption(
            "A skipped model had too little history: Layer 1 needs 14 station-days for a "
            "location, Layer 2 needs 20 feature rows per parameter. Skipped layers run "
            "rules-only."
        )

    st.subheader("Layer 2 evaluation — latest run")
    latest = latest_model_runs(model_runs)
    if latest.empty:
        st.info("No Layer 2 run metrics yet. They are recorded by `python -m pipelines detect`.")
    else:
        _show(latest, EVALUATION_COLUMNS)
        st.caption(
            "There is no labelled ground truth, so these are proxies. **Lift** compares how "
            "often alerts match the independent weak-label rule with how often any row does: "
            "above 1.0 the alerts beat random selection. **Precision@3** is the weak-label "
            "share of each region-day's top three alerts. Weak-label metrics apply only "
            "to PM2.5; they are unavailable for other pollutants."
        )

        st.subheader("Evaluation trend across runs")
        metric = st.selectbox(
            "Metric",
            options=[m for m in TREND_METRICS if m in model_runs.columns],
            format_func=TREND_METRICS.get,
            key="model_trend_metric",
        )
        trend = model_runs.pivot_table(
            index="run_at", columns="parameter", values=metric, aggfunc="last"
        )
        if trend.dropna(how="all").empty:
            st.caption(f"No {TREND_METRICS[metric].lower()} values recorded yet.")
        else:
            st.line_chart(trend, width="stretch")
        with st.expander(f"All recorded runs ({len(model_runs)})"):
            _show(model_runs, EVALUATION_COLUMNS, sort_by="run_at")

    st.subheader("Layer 2 offline benchmark")
    if benchmark is None:
        st.info(
            "No benchmark report yet. Run `python -m pipelines evaluate` to measure "
            "held-out behaviour and recall on injected spikes."
        )
    else:
        if benchmark.get("methodology_version", 1) < 2:
            st.warning(
                "This benchmark predates the corrected temporal split and spike eligibility "
                "checks. Rerun the offline evaluation before quoting its results."
            )
        st.caption(
            f"Built {benchmark.get('built_at_utc', 'unknown')} over "
            f"{benchmark.get('feature_rows', 0)} feature rows."
        )
        holdout = pd.DataFrame(benchmark.get("holdout", []))
        if not holdout.empty:
            st.markdown("**Temporal holdout** — trained on earlier days, scored on later ones")
            _show(
                holdout,
                [
                    "parameter",
                    "train_dates",
                    "holdout_dates",
                    "train_flag_rate",
                    "holdout_flag_rate",
                    "holdout_weak_label_lift",
                    "holdout_if_rate",
                    "holdout_lof_rate",
                    "holdout_dbscan_rate",
                ],
            )
        injection = benchmark.get("injection") or {}
        if injection:
            st.markdown(
                f"**Injected-spike recall** — {injection.get('injected_scored', 0)} synthetic "
                f"{injection.get('parameter', '')} spikes"
            )

            def _pct(value) -> str:
                return "—" if value is None else f"{value:.0%}"

            i1, i2, i3, i4 = st.columns(4)
            i1.metric("Recall", _pct(injection.get("recall")))
            i2.metric("Local spikes", _pct(injection.get("recall_local")))
            i3.metric("Regional spikes", _pct(injection.get("recall_regional")))
            i4.metric("Untouched-data alert rate", _pct(injection.get("untouched_alert_rate")))
            st.caption(
                "Untouched real readings have no verified normal labels. This alert rate "
                "does not measure false positives. Spike recall includes the daily top-K cap."
            )

    st.caption(
        "Layer 1 has controlled synthetic-fault coverage. Independent review of real "
        "alerts remains pending; see the assessment progress and model cards."
    )
