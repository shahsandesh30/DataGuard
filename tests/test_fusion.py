import pandas as pd

from pipelines.config import FUSION_STATUS_ESCALATED, FUSION_STATUS_QUARANTINED, SEVERITY_PENALTY
from pipelines.detection.ensemble import EVENT_ALERT_COLUMNS
from pipelines.fusion.build import build_fusion, read_trust_alerts
from pipelines.fusion.trust_score import TRUST_ALERT_COLUMNS, aggregate_incidents, fuse
from pipelines.quality.rules import INCIDENT_COLUMNS


def _alert(**overrides) -> dict:
    row = {
        "location_id": 1544061,
        "date_local": "2026-01-08",
        "parameter": "pm25",
        "region_id": "sydney_metro",
        "alert_score": 0.8,
        "rank": 1,
        "if_flag": True,
        "lof_flag": True,
        "dbscan_flag": False,
        "agreement_count": 2,
        "weak_label": False,
        "feature_snapshot": "{}",
    }
    row.update(overrides)
    return row


def _incident(**overrides) -> dict:
    row = {
        "location_id": 1544061,
        "date_local": "2026-01-08",
        "rule_id": "R2",
        "incident_type": "stuck_sensor",
        "severity": "high",
        "event_code": "E3",
        "metric_snapshot": "{}",
        "is_incident": True,
        "source": "rule",
    }
    row.update(overrides)
    return row


def test_coincident_high_severity_quarantines_and_penalizes():
    alerts = pd.DataFrame([_alert()], columns=EVENT_ALERT_COLUMNS)
    incidents = pd.DataFrame([_incident()], columns=INCIDENT_COLUMNS)
    fused = fuse(incidents, alerts)
    assert fused.iloc[0]["status"] == FUSION_STATUS_QUARANTINED
    expected = 0.8 * (1.0 - SEVERITY_PENALTY["high"])
    assert abs(fused.iloc[0]["trust_score"] - expected) < 1e-9
    assert fused.iloc[0]["trust_score"] < fused.iloc[0]["alert_score"]


def test_clean_station_day_escalates_with_full_score():
    alerts = pd.DataFrame([_alert()], columns=EVENT_ALERT_COLUMNS)
    incidents = pd.DataFrame(columns=INCIDENT_COLUMNS)
    fused = fuse(incidents, alerts)
    assert fused.iloc[0]["status"] == FUSION_STATUS_ESCALATED
    assert fused.iloc[0]["trust_score"] == fused.iloc[0]["alert_score"]
    assert not bool(fused.iloc[0]["has_quality_incident"])


def test_multiple_rules_use_max_severity_and_aggregate_ids():
    alerts = pd.DataFrame([_alert()], columns=EVENT_ALERT_COLUMNS)
    incidents = pd.DataFrame(
        [
            _incident(rule_id="R4", severity="medium"),
            _incident(rule_id="R1", severity="high"),
            _incident(rule_id="R9", severity="low"),
        ],
        columns=INCIDENT_COLUMNS,
    )
    summary = aggregate_incidents(incidents)
    assert summary.iloc[0]["max_severity"] == "high"
    assert summary.iloc[0]["incident_count"] == 3
    assert summary.iloc[0]["incident_rule_ids"] == "R1,R4,R9"

    fused = fuse(incidents, alerts)
    expected = 0.8 * (1.0 - SEVERITY_PENALTY["high"])
    assert abs(fused.iloc[0]["trust_score"] - expected) < 1e-9
    assert fused.iloc[0]["status"] == FUSION_STATUS_QUARANTINED


def test_empty_alerts_returns_schema():
    fused = fuse(pd.DataFrame(columns=INCIDENT_COLUMNS), pd.DataFrame(columns=EVENT_ALERT_COLUMNS))
    assert fused.empty
    assert list(fused.columns) == TRUST_ALERT_COLUMNS


def test_build_fusion_writes_gold_partition(tmp_path, monkeypatch):
    alerts = pd.DataFrame(
        [
            _alert(),
            _alert(location_id=1601414, date_local="2026-01-08", alert_score=0.5, rank=2),
        ],
        columns=EVENT_ALERT_COLUMNS,
    )
    incidents = pd.DataFrame([_incident()], columns=INCIDENT_COLUMNS)
    gold = tmp_path / "gold"

    monkeypatch.setattr("pipelines.fusion.build.read_event_alerts", lambda _gold: alerts)
    monkeypatch.setattr("pipelines.fusion.build.read_quality_incidents", lambda _gold: incidents)

    result = build_fusion(gold_root=gold)
    assert result.alert_rows == 2
    assert result.quarantined == 1
    assert result.escalated == 1
    assert (gold / "fusion" / "trust_alerts").exists()

    loaded = read_trust_alerts(gold)
    assert not loaded.empty
    assert set(TRUST_ALERT_COLUMNS).issubset(loaded.columns)
    statuses = set(loaded["status"])
    assert FUSION_STATUS_QUARANTINED in statuses
    assert FUSION_STATUS_ESCALATED in statuses
