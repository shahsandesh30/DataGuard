# Layer 1 Model Card — Isolation Forest over Station-Day Metrics

## Purpose

Detect multivariate data-health anomalies at `(location_id, date_local)` that
single-metric rules may miss. Secondary to deterministic rules R1–R10.

## Status

| Component | Status |
|---|---|
| Deterministic rules R1–R10 | **Active** — primary detection path |
| Isolation Forest | **Gated** — trains only when a location has ≥14 station-days |

With the current local sample (~1 day per location), only rules fire. Fetch
more history before expecting model incidents:

```bash
python -m pipelines ingest --locations 1544061 --start 2026-01-01 --end 2026-01-31
python -m pipelines run --locations 1544061 --start 2026-01-01 --end 2026-01-31
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

| Event | Primary detector | Notes |
|---|---|---|
| E2 Freshness | R6 | Requires ingest manifest `_manifest.jsonl` |
| E3 Stuck sensor | R2, R3 | Go/no-go gate — self-evident in time series |
| E4 Negatives | R1 | Physically impossible concentrations |
| E5 Dropout | R4, R5, R7 | Needs multi-day history for dropout |
| E6 Partial outage | R5, cross_sensor_pm25_spread | Needs multiple sensors |
| E7 Schema/units | R8, R10 | Bronze header drift + unit mismatch |
| E8 Metadata drift | Deferred | Needs longer metadata history |

Synthetic injection (per `docs/data-source.md`) is for precision/recall curves
only — not primary evidence.

## Limitations

- Sparse sample: IF cannot train until MIN_STATION_DAYS (14) per location
- No accuracy metric without reference monitors
- E1 (API retirement) is external to the measurement pipeline
