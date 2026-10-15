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

Historical operational run (January 2026, four Sydney stations): **98 station-days → 13 rule
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
- **Artifact:** `<MODELS_ROOT>/layer1/isolation_forest.joblib`, plus a copy per run under
  `layer1/history/run=<UTC timestamp>/` (default root `models/`, gitignored)

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

Synthetic injection checks controlled fault coverage; real-world accuracy
still requires independent evidence.

## Controlled fault benchmark (15 October 2026)

Run the actual conformance and quality stages using synthetic gzipped archive
files and their arrival manifest. The required output root must be local and
empty; each run creates isolated bronze, silver, gold, and model roots without
using operational `.env` roots:

```powershell
venv\Scripts\python.exe -m pipelines.quality.benchmark --output-root data/evaluation/quality-check
```

The deterministic baseline contains three synthetic stations, 21 local days,
two hourly PM2.5 sensors per station, and **3,024 readings / 63 station-days**.
Concentrations vary while cadence and sensor spread remain regular. Timestamps
have an explicit +11:00 offset. Manifest arrival times represent timely
deliveries; R6 stays disabled. Each fault starts from the same baseline in a
separate directory and affects three interior station-days with prior history.

- Frozen sensor: **3/3**, R2 and R3.
- Negative readings: **3/3**, R1; all nine negative values survive into silver.
- Sensor dropout: **3/3**, R5.
- Missing daily file: **3/3**, R4, R5, and R7; zero-reading days remain visible.
- Schema drift: **3/3**, R8.
- Incorrect particulate unit label (`ppm`): **3/3**, R10; original labels survive.

All **18/18 cases** and **27/27 expected rule checks** were detected, with no
unscored targets or misses. The healthy baseline produced zero rule/model
incidents. Across the six fault runs, **3/360 untouched control station-day
occurrences (0.83%)** were flagged: R8 on the day after each schema injection,
because restoring the original header is another schema transition. This
denominator counts separate copies, not 360 distinct real station-days.

Isolation Forest trained in each run and flagged 15/18 target cases before
rule de-duplication; it flagged none of the schema cases or controls. It added
zero published incidents because rules already caught its flagged days.
The production model refits on each scenario, so these flags are **in-sample
behaviour, not held-out model accuracy**.

The report records per-case expected/observed rules, misses, raw and reported
model flags, additional control incidents, dataset/configuration, runtime
versions, and source/input hashes. Recorded results:
[`docs/evaluation/step3-quality-benchmark.json`](../docs/evaluation/step3-quality-benchmark.json).

## Limitations

- **Controlled coverage is narrow.** Three repetitions per fault use one
  parameter, cadence, local offset, and fault strength. These controls do not
  estimate real-world false positives; schema recovery adds incidents too.
- Days without readings and without a manifest attempt cannot be inferred.
  Archive filename/local-day boundary assumptions still affect R7; see the
  data-source notes. An expected sensor roster needs prior observed readings.
- `contamination=0.05` is an assumption, not a measurement — it asserts 5% of
  station-days are anomalous.
- No accuracy metric without reference monitors.
- E1 (API retirement) is external to the measurement pipeline.
- E8 (metadata drift) needs longer history than we have ingested.
