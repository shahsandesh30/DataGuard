# Fusion Spec — Trust Scoring Layer 1 × Layer 2

## Purpose

Every Layer 2 pollution alert receives a trust score derived from Layer 1
data-health incidents on the same station-day. Alerts raised while the
underlying data was unhealthy are **quarantined for human review** — never
deleted or hidden.

## Status

| Component | Status |
|---|---|
| Join on `(locationid, date_local)` | **Active** |
| Continuous `trust_score` | **Active** |
| Escalated / quarantined status | **Active** |
| Gold publish `data/gold/fusion/trust_alerts/` | **Active** |

Last run (January 2026, four Sydney stations): **164 alerts — 147 escalated,
17 quarantined**, trust scores spanning 0.105 to 1.000. The quarantines break
down as R2 x9, R4+R7 x7, M1 x1.

```bash
python -m pipelines quality
python -m pipelines detect
python -m pipelines fuse
```

## Join

Left-join Layer 2 `event_alerts` → Layer 1 incidents aggregated to station-day:

| Field | Meaning |
|---|---|
| `has_quality_incident` | Any L1 incident that day |
| `max_severity` | Highest of `{high, medium, low}` |
| `incident_count` | Number of incident rows |
| `incident_rule_ids` | Sorted unique rule ids (comma-separated) |

## Severity penalty

| max_severity | penalty |
|---|---|
| none | 0.0 |
| low | 0.2 |
| medium | 0.4 |
| high | 0.7 |

Constants: `SEVERITY_PENALTY` in `pipelines/config.py`.

## Trust score

```
trust_score = clip(alert_score * (1 - severity_penalty), 0, 1)
```

When multiple incidents fire the same day, the **maximum** severity penalty
is used (most conservative).

## Status rule

| Condition | status |
|---|---|
| `has_quality_incident` | `quarantined` |
| otherwise | `escalated` |

No continuous threshold gate — coincident quality incident is the quarantine
rule (matches `docs/architecture.md`). `trust_score` still ranks within each
bucket for review prioritisation.

## Output schema

`TRUST_ALERT_COLUMNS` in `pipelines/fusion/trust_score.py`:

- Pass-through: `locationid`, `date_local`, `parameter`, `region_id`,
  `alert_score`, `rank`, `agreement_count`, detector flags, `weak_label`,
  `feature_snapshot`
- Fusion: `has_quality_incident`, `max_severity`, `incident_count`,
  `incident_rule_ids`, `trust_score`, `status`

## Evaluation narrative

Headline metric: how much quarantine reduces false alerts (sensor failures
misread as pollution) while retaining genuine multi-station events that
occur on healthy data days.

Weak labels from Layer 2 remain evaluation-only; fusion does not retrain
detectors.

## Limitations

- **Fusion is alert-driven.** No Layer 2 alerts means no fusion rows, even when
  Layer 1 found incidents. Those Layer-1-only findings are not lost — the
  dashboard renders them as `quality_only` — but they never receive a trust
  score.
- **Binary quarantine on any coincident incident.** One low-severity R9
  (duplicate readings) quarantines an alert exactly as firmly as a high-severity
  R2 (stuck sensor). The severity penalty grades the *score* but not the
  *status*; fine-grained per-rule policy is deferred.
- **The headline metric has not been computed.** How much quarantine actually
  reduces false alerts, while retaining genuine events, is the whole claim of
  the project and is still unmeasured. It depends on Layer 2 evaluation, which
  depends on a seeded-failure test for Layer 1.
- A rule that fires on nearly every station-day will quarantine nearly every
  alert. Always check the `rule_id` histogram before quoting a quarantine rate
  as a result — see the R6 episode in `docs/data-source.md`.
- Public dashboard deploy still out of scope for this phase.
