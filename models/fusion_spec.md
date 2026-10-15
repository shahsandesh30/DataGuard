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

Historical documented local snapshot: **253 alerts — 181 escalated, 72 quarantined**,
with trust scores spanning 0.055 to 0.905.

The fresh, isolated Step 4 run (15 October 2026) produces **175 alerts — 110
escalated, 65 quarantined (37.1%)**. Every Layer 2 candidate is preserved.
Mean alert score is 0.332; mean trust score is 0.267. These are descriptive
decisions, not verified accuracy. See the
[measured report](../docs/evaluation/step4-fusion-evaluation.json) and
[reproduction/review guide](../docs/fusion-review.md). Older cached outputs
and documented counts are not a paired before/after baseline for this run.

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

### Step 4 automated evidence

The evaluation copies local bronze, frozen Step 2 silver, and Step 3 benchmark
outputs into a new directory and records input/source hashes and runtime
versions. The detectors rebuild independently before production fusion runs;
production policy and model settings are unchanged. This production-style
refit is separate from Step 2's held-out evaluation.

Layer 1 now covers **505 station-days** across 16 requested stations, including
**152 days with zero readings** and 353 observed days. Silver contains 38,449
readings at 14 locations, yielding 995 Layer 2 feature rows. File absence can
therefore produce quality-only evidence without a Layer 2 candidate.

The controlled checks feed actual Step 3 incidents into fusion with supplied
candidate alerts. All **42 policy checks pass**:

- All 18 faulty-data candidates are quarantined.
- All 18 candidates on healthy control data are escalated.
- Three schema-recovery candidates are quarantined because R8 also marks
  restoration of the original header.
- An assumed regional event at three stations retains two escalated candidates
  and holds one with a coincident quality issue. This exposes a possible cost
  of the policy, rather than claiming perfect event retention.

The candidate scores and event assumption are constructed. These checks do
not measure detector recall or verified false positives. Actual Step 3 faults
such as an entirely missing file may produce no Layer 2 candidate at all.

### Independent human evidence: pending

A fixed sample contains ten escalated and ten quarantined PM2.5 cases, drawn
randomly within status with seed 42. Both teammates review the same shuffled
cases independently. The shared HTML page withholds fusion decisions, scores,
rule IDs, detector votes, and weak labels. It supplies exact current-day
readings, prior-seven-day hourly context, original units, and available peers
within 50 km. The lead retains the separate answer key until review finishes.

The scorer requires two distinct reviewer IDs, matching evidence-pack IDs,
valid labels, and notes. It counts paired complete responses, disagreement,
uncertainty, and missing coverage; it never fills in human labels. Rates remain
unavailable when no eligible consensus judgments exist. The status-balanced
sample's pooled results are not population accuracy or false-positive rates.

## Limitations

- **Fusion is alert-driven.** No Layer 2 alerts means no fusion rows, even when
  Layer 1 found incidents. Those Layer-1-only findings are not lost — the
  dashboard renders them as `quality_only` — but they never receive a trust
  score.
- **Binary quarantine on any coincident incident.** One low-severity R9
  (duplicate readings) quarantines an alert exactly as firmly as a high-severity
  R2 (stuck sensor). The severity penalty grades the *score* but not the
  *status*; fine-grained per-rule policy is deferred.
- **Real false-alert reduction and event retention remain unverified.** The
  automated counts and controlled policy checks are measured, and a 20-case
  review pack is ready. Two independent human judgments are still required;
  human consensus will itself remain weaker evidence than reference monitors.
- A rule that fires on nearly every station-day will quarantine nearly every
  alert. Always check the `rule_id` histogram before quoting a quarantine rate
  as a result — see the R6 episode in `docs/data-source.md`.
- Public dashboard deploy still out of scope for this phase.
