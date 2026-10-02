# Layer 1 Model Card — Isolation Forest over Station-Day Metrics

## Purpose

Detect multivariate data-health anomalies at `(locationid, date_local)` that
single-metric rules may miss. Secondary to the deterministic rules, which
remain the primary detection path.

## Status

| Component | Status |
|---|---|
| Deterministic rules | **Active** — primary detection path |
| Isolation Forest | **Active** — trains once a location has ≥14 station-days |

Last run (January 2026, four Sydney stations): **98 station-days → 13 rule
incidents + 1 model incident**, `model_trained: true`.

The model is gated on history by design. Below `MIN_STATION_DAYS` (14) for every
location, `fit_quality_model` returns `None` and the stage runs rules-only and
says so in its log line, rather than fitting on noise. **Empty model output on a
small sample is expected, not a bug.**

```bash
python -m pipelines ingest --locations 1544061 1601414 2455394 6430870 --start 2026-01-01 --end 2026-01-31
python -m pipelines run --locations 1544061 1601414 2455394 6430870 --start 2026-01-01 --end 2026-01-31
```

## Features (station-day vector)

- `total_readings`, `missing_rate_mean`
- `sensors_expected`, `sensors_received`, `sensor_dropout_count`
- `negative_count_total`, `max_stuck_run_max`, `zero_variance_params`
- `duplicate_rate`, `file_lateness_hours`, `unit_mismatch_count`
- `cross_sensor_pm25_spread`

## Model

- **Algorithm:** `sklearn.ensemble.IsolationForest`
- **Preprocessing:** `StandardScaler` on feature matrix
- **Hyperparameters:** `contamination=0.05`, `n_estimators=100`, `random_state=42`
- **Artifact:** `models/layer1_isolation_forest.joblib` (gitignored)

## Validation (documented degradation events)

Model incidents are de-duplicated against the rules by
`_drop_duplicate_of_rules` — the Isolation Forest only reports station-days no
rule already caught, so it has to earn its place.

| Event | Primary detector | Notes |
|---|---|---|
| E2 Freshness | ~~R6~~ — **rule removed** | `file_lateness_hours` still feeds the model. Meaningless on backfilled data; see `docs/data-source.md`. |
| E3 Stuck sensor | R2, R3 | Go/no-go gate — self-evident in time series |
| E4 Negatives | R1 | Physically impossible concentrations |
| E5 Dropout | R4, R5, R7 | Needs multi-day history for dropout |
| E6 Partial outage | R5, cross_sensor_pm25_spread | Needs multiple sensors |
| E7 Schema/units | R8, R10 | Bronze header drift + unit mismatch |
| E8 Metadata drift | Deferred | Needs longer metadata history |

Synthetic injection (per `docs/data-source.md`) is for precision/recall curves
only — not primary evidence.

## Limitations

- **No seeded-failure test yet.** The rules have never been checked against
  deliberately corrupted data, so "Layer 1 detects known failures" is currently
  an assertion. This is the project's go/no-go gate (`docs/risk-register.md`)
  and the top open task.
- `contamination=0.05` is an assumption, not a measurement — it asserts 5% of
  station-days are anomalous.
- No accuracy metric without reference monitors.
- E1 (API retirement) is external to the measurement pipeline.
- E8 (metadata drift) needs longer history than we have ingested.
