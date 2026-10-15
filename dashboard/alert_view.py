"""Native Streamlit view of the evidence behind one saved alert."""

from __future__ import annotations

import json

import pandas as pd
import streamlit as st

from dashboard.explanation import ALERT_KEY, FEATURE_LABELS, explain_alert


def _selection(label: str, options: list, key: str):
    if not options:
        return None
    # Dependent options can change with the date filter, roots, or a refresh.
    if st.session_state.get(key) not in options:
        st.session_state[key] = options[0]
    return st.selectbox(label, options, key=key)


def _show_readings(records: list[dict], exact_label: str) -> None:
    readings = pd.DataFrame(records)
    readings["datetime"] = pd.to_datetime(readings["datetime"], utc=True, errors="coerce")
    readings["Sensor"] = readings["sensor_id"].astype(str)
    for unit, group in readings.groupby("unit", dropna=False):
        plotted = group.dropna(subset=["datetime"])
        if not plotted.empty:
            st.line_chart(
                plotted,
                x="datetime",
                y="value",
                color="Sensor",
                height=240,
                x_label="Time (UTC)",
                y_label=f"Conformed value ({unit})",
                width="stretch",
            )
    st.caption(
        "Each sensor is a separate series; different units use separate charts. "
        "Lines connect observed readings, so inspect timestamps for gaps. Negative "
        "and constant readings are retained."
    )
    with st.expander(exact_label):
        st.dataframe(readings.drop(columns="Sensor"), hide_index=True, width="stretch")


def render_alert_explorer(
    alerts: pd.DataFrame,
    incidents: pd.DataFrame,
    measurements: pd.DataFrame,
    summaries: dict,
) -> None:
    st.subheader("Explain one alert")
    st.caption(
        "Choose an alert within the current date filter. Quarantined alerts remain available."
    )
    if alerts.empty:
        st.info("No saved alerts match the current date filter.")
        return
    if not set(ALERT_KEY) <= set(alerts.columns):
        st.warning("Saved alerts lack the identifiers needed for a unique explanation.")
        return
    station_options = sorted(alerts["locationid"].dropna().astype(int).unique())
    station = _selection("Alert station", station_options, "explain_station")
    station_alerts = alerts[alerts["locationid"] == station]
    day = _selection(
        "Alert local date", sorted(station_alerts["date_local"].astype(str).unique()), "explain_day"
    )
    day_alerts = station_alerts[station_alerts["date_local"].astype(str) == day]
    parameter = _selection(
        "Alert pollutant", sorted(day_alerts["parameter"].dropna().unique()), "explain_parameter"
    )
    if station is None or day is None or parameter is None:
        st.info("No identifiable alert matches this selection.")
        return
    try:
        evidence = explain_alert(
            alerts,
            incidents,
            measurements,
            locationid=station,
            date_local=day,
            parameter=parameter,
            summaries=summaries,
        )
    except ValueError as exc:
        st.warning(str(exc))
        return
    decision = evidence["decision"]
    status = decision["status"]
    with st.container(border=True):
        if status == "quarantined":
            st.warning("Held for review — this alert remains visible.")
            if decision["has_quality_incident"] is True:
                st.write(
                    "The saved decision attaches a data-health incident to this station-day. "
                    "Fusion holds all alerted pollutants on that day for review."
                )
        elif status == "escalated":
            st.info("Saved status: escalated.")
            if decision["has_quality_incident"] is False:
                st.write(
                    "The saved decision attaches no quality incident to this station-day. "
                    "This does not confirm a pollution event or prove that the data is healthy."
                )
        else:
            st.warning("The saved alert has no recognized decision status.")
        st.caption("Human review: " + decision["human_review"] + ".")
        with st.container(horizontal=True):
            for label, value, fmt in (
                ("Original anomaly score", decision["alert_score"], ".3f"),
                ("Policy penalty", decision["policy_penalty"], ".0%"),
                ("Stored trust score", decision["trust_score"], ".3f"),
            ):
                st.metric(
                    label, "Unavailable" if value is None else format(value, fmt), border=True
                )
        if decision["calculation_verified"]:
            st.code(
                f"{decision['alert_score']:.6f} × (1 − {decision['policy_penalty']:.0%}) "
                f"= {decision['trust_score']:.6f}",
                language=None,
            )
        st.caption(
            "The highest station-day severity sets the penalty; penalties do not stack. "
            "Scores are ranking signals, not probabilities."
        )
    for warning in evidence["warnings"]:
        st.warning(warning)

    st.markdown("**Recorded event evidence**")
    st.caption(
        "Detector votes explain which models flagged this row. The recorded measurements "
        "below provide context; they are not feature-importance or causal explanations."
    )
    voters = pd.DataFrame(
        [
            {
                "Detector": row["detector"],
                "Vote": {
                    True: "Flagged",
                    False: "Not flagged",
                    None: "Unavailable",
                }[row["flagged"]],
            }
            for row in evidence["detector_votes"]
        ]
    )
    st.dataframe(voters, hide_index=True, width="stretch")
    features = [
        {"Recorded measure": label, "Value": evidence["features"].get(key)}
        for key, label in FEATURE_LABELS.items()
        if key in evidence["features"]
    ]
    if features:
        st.dataframe(pd.DataFrame(features), hide_index=True, width="stretch")

    st.markdown("**Station-day data health**")
    st.caption("Quality incidents apply to the station-day across pollutants.")
    if evidence["quality_incidents"]:
        findings = pd.DataFrame(evidence["quality_incidents"])
        st.dataframe(
            findings[["rule_id", "finding", "severity", "source", "evidence"]],
            hide_index=True,
            width="stretch",
        )
        if "R7" in set(findings["rule_id"]):
            st.caption(
                "Missing-file checks assume archive filenames. Partial local-day windows "
                "can produce boundary findings; inspect the evidence before judging."
            )
    else:
        st.caption("No supporting incident rows are loaded for this station-day.")

    st.markdown("**Readings for this alert**")
    if evidence["readings"]:
        _show_readings(evidence["readings"], "Exact readings and original units")

    with st.expander("Previous seven days of readings"):
        window = evidence["history_window"]
        available = len(window["available_local_dates"])
        st.caption(
            f"{window['start_local_date']} to {window['end_local_date']} (local dates). "
            f"{available}/7 dates have readings for this station and pollutant. "
            "A represented date can still contain gaps."
        )
        if evidence["context_readings"]:
            _show_readings(evidence["context_readings"], "Exact historical readings")
        else:
            st.info("No readings are available in the preceding seven local dates.")

    with st.expander("Saved snapshots and build times"):
        st.json(
            {key: evidence[key] for key in ("features", "quality_incidents", "build_times_utc")}
        )
    st.download_button(
        "Download this alert's evidence",
        json.dumps(evidence, indent=2, allow_nan=False),
        file_name=f"alert-{station}-{day}-{parameter}.json",
        mime="application/json",
        key="download_alert_evidence",
        on_click="ignore",
    )
