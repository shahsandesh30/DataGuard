# Layer 2 Model Card — Pollution Event Detection Ensemble

## Purpose

Detect genuine pollution events (smoke, dust, industrial spikes) at
`(locationid, date_local, parameter)` grain. Layer 2 is **not** data-health
detection — stuck sensors, missing files, and schema drift belong to Layer 1.

## Status

| Component | Status |
|---|---|
| Feature engineering | **Active** — station-day-parameter features |
| Weak labels (eval only) | **Active** — regional PM2.5 elevation heuristic |
| IF + LOF + DBSCAN ensemble | **Active** — trains once ≥20 feature rows exist |
| Evaluation | **Active** — per-run metrics on every `detect`; offline benchmark via `evaluate` |

Latest audited local snapshot (342 bronze files, 14 stations): **995 feature
rows → 253 detector-backed alerts**, `ensemble_trained: true`. Fusion escalated
181 and quarantined 72.

The ensemble is gated on history by design — below `MIN_EVENT_ROWS` (20) the
stage writes features and skips scoring, rather than fitting on noise.

```bash
python -m pipelines ingest --locations 1544061 1601414 2455394 6430870 --start 2026-01-01 --end 2026-01-31
python -m pipelines run --locations 1544061 1601414 2455394 6430870 --start 2026-01-01 --end 2026-01-31
```

## Features (station-day-parameter)

| Feature | Dimension |
|---|---|
| `daily_mean`, `daily_max` | Baseline level |
| `z_score`, `iqr_exceedance` | Outliers vs trailing 7-day baseline |
| `roc_max`, `spike_count`, `sustained_elevation_hours` | Temporal anomalies |
| `mean_shift_ratio` | Distribution shift |
| `peer_z_score`, `spatial_isolation` | Sensor vs regional peers |
| `regional_agreement`, `pm_co_movement` | Multi-sensor consistency |
| `hourly_deviation_zscore_max`, `hourly_roll_std_24h_mean` | Within-day baseline and variability |
| `hourly_rate_of_change_1h_max`, `hourly_sustained_elevated_hours_max` | Hourly persistence and change |
| `hourly_spatial_*`, `hourly_regionally_coherent_hours` | Hourly spatial consistency |
| `hourly_possible_humidity_artifact_hours` | Humidity-confounding signal |

Parameters: `pm25`, `um003`, `pm1`. Regional bucket: `sydney_metro`.

## Model

- **Algorithms:** `IsolationForest`, `LocalOutlierFactor`, `DBSCAN`
- **Preprocessing:** `StandardScaler` on numeric feature columns
- **Artifact:** one fitted ensemble per parameter at
  `<MODELS_ROOT>/layer2/ensemble_<parameter>.joblib`, plus a copy per run under
  `layer2/history/run=<UTC timestamp>/` (default root `models/`, gitignored)
- **DBSCAN `eps`:** tuned per fit by `tune_dbscan_eps`. It is the
  `1 − contamination` quantile of each row's distance to its `min_samples`-th
  neighbour, so at most `contamination` (10%) of training rows can be noise,
  the same budget IF and LOF get. The former fixed `eps=1.5` is compared under
  *Evaluation*.
- **Scoring:** Each detector contributes one binary vote and one normalized
  score. `alert_score` is the mean of agreement share and mean detector score,
  so it remains in `[0, 1]`.
- **Output:** Rows with at least one detector vote, capped to the top 10 per
  region-day in `data/gold/layer2/event_alerts/`. Normal feature rows are not
  relabelled as alerts merely to fill a daily quota.

## Weak labels (evaluation only)

Per risk R4 in `docs/data-source.md`:

```
regional_pm25_mean > 2 × trailing_regional_median
AND ≥2 locations elevated same day
```

Used for the Precision@K narrative in `notebooks/04_detection_features.ipynb` —
**never** used as training labels for the ensemble.

## Evaluation

Layer 2 has no labelled ground truth, so it is measured three ways. Each check
answers a different question, and none of them relies on only one heuristic.

### 1. Per-run metrics (every `detect`)

`pipelines/detection/evaluation.py` appends one row per parameter per run to
`gold/layer2/model_runs/` (partitioned by `parameter`, never overwritten), so
the history can be queried across runs.

| Column | Meaning |
|---|---|
| `flag_rate` | `alert_rows / feature_rows` |
| `weak_label_agreement_rate` | Share of alerts on a weak-labelled row |
| `weak_label_base_rate` | Share of *all* scored rows that are weak-labelled |
| `weak_label_lift` | agreement ÷ base rate. **> 1.0 means alerts beat random selection**; `None` when the base rate is 0 |
| `precision_at_k` | Weak-label share among rank ≤ `PRECISION_AT_K` (3) alerts per region-day |
| `detector_agreement_mean`, `avg_alert_score` | Mean votes and score across alerts |

The agreement rate alone is misleading. In a month where every day is
weak-labelled, any model would score 100%. Lift is the number to compare.

### 2. Temporal holdout (`python -m pipelines evaluate`)

`benchmark.temporal_holdout` splits raw silver on each parameter's first 75%
of local dates before feature fitting, then scores the last 25% out of sample.
Training features are built separately. Hourly median/MAD reference baselines
and the scaler use training readings only. Held-out rolling and daily trailing
features may use earlier observations, but never future readings. Split dates
and parameters skipped for insufficient history are recorded in the report.
IF predicts held-out rows directly. LOF is refitted in novelty mode with the
same neighbours and contamination. A DBSCAN point counts as noise unless it
lies within `eps` of a core sample. "Flagged" means ≥ 1 detector vote, which is
the alert-table admission rule, measured before the top-K cap.

### 3. Injected-spike recall (`python -m pipelines evaluate`)

`benchmark.injection_recall` adds known PM2.5 spikes to real silver: +3 × the
station median for local hours 10–17. **Local** spikes hit one station-day.
**Regional** spikes hit every eligible station on a date with at least two
eligible locations. The benchmark then rebuilds
the features, refits, and counts how many injected station-days reach the final
alert table. A case needs seven prior observed station-days and finite
readings during the injection hours. Truth labels include only station-days
whose readings actually changed. The report records exclusions, unscored
cases, and local/regional denominators.

This is recall of synthetic perturbations after refitting on the modified
history, measured after the daily top-K cap. `untouched_alert_rate` is the
share of untouched feature rows that became alerts. Those real readings have
no verified normal labels, so this is not a false-positive rate.

Both offline checks write `gold/layer2/_detection_benchmark.json`. Methodology
version 2 records a row-order-independent dataset fingerprint, date/station
coverage, configuration, and library versions. Neither check runs inside
`detect` or `run`. The dashboard can read older reports, but warns that their
evaluation needs rerunning. Weak-label metrics for pm1 and um003 are
unavailable, including when reading older run history that stored zeros.

### Legacy results — August 2026 silver (methodology version 1)

The following historical comparison predates the corrected temporal split
and injection eligibility checks. Its holdout and injection numbers must not
be quoted as corrected validation. Methodology version 2 results are recorded
in [Assessment 4 progress](../docs/assessment4-progress.md).

50,233 measurements, 14 stations, 2026-08-01 → 2026-09-01, 1,280 feature rows.
"Fixed eps" is the original `eps=1.5`. "Tuned eps" is the k-distance rule
described under *Model* below. Both were run on the same data.

**Per-run (in-sample):**

| Parameter | Alerts (fixed → tuned) | Flag rate | Lift | Precision@3 |
|---|---|---|---|---|
| pm25 | 177 → **70** | 0.40 → **0.16** | 1.81 → **1.90** | 0.23 → 0.21 |
| pm1 | 263 → **71** | 0.63 → **0.17** | — | — |
| um003 | 223 → **75** | 0.54 → **0.18** | — | — |

PM2.5 base rate is 0.19 in both runs. pm1 and um003 have no weak labels.

**Temporal holdout** (24 training dates, 8 held out):

| Parameter | Held-out flag rate | DBSCAN held-out rate | IF | LOF | Held-out lift |
|---|---|---|---|---|---|
| pm25 | 0.73 → **0.35** | 0.72 → **0.10** | 0.29 | 0.25 | 1.27 → **1.50** |
| pm1 | 0.98 → **0.42** | 0.95 → **0.22** | 0.31 | 0.26 | — |
| um003 | 0.79 → **0.40** | 0.76 → **0.08** | 0.25 | 0.35 | — |

**Injected spikes** (pm25, magnitude 3.0, seed 42; 6 local and 28 regional
station-days across 2 dates):

| | Fixed eps | Tuned eps |
|---|---|---|
| Recall | 0.68 | **0.21** |
| Local recall | 0.67 | 0.50 |
| Regional recall | 0.68 | **0.14** |
| Untouched-data alert rate (originally labelled false-alarm rate) | 0.40 | **0.15** |

### Interpretation of the historical comparison

The in-sample counts document a reduction in alert volume after tuning `eps`.
The older holdout rates and injected-spike recalls need rerunning with
training-only hourly baselines and valid injection cases. They do not establish
the ensemble's out-of-sample performance or the size of a regional blind spot.
Use the corrected fixed-dataset benchmark before choosing another model or
feature change. PM2.5 weak-label agreement remains proxy evidence; pm1 and
um003 have no applicable weak-label evaluation.

## Limitations

- **Weak labels are a heuristic, not ground truth.** Lift above 1.0 shows the
  alerts correlate with regional elevation; it does not show they are correct.
- Injected spikes are clean rectangular bumps and do not establish performance
  on real smoke or dust. Large regional cases can exceed the daily top-K cap.
- Trailing 7-day baselines are noisy with <7 days of history per station.
- In temporal evaluation, hourly reference groups without enough training
  observations use the existing zero-filled feature fallback; new station/
  parameter pairs do not fit hourly statistics from held-out readings.
- No reference-monitor accuracy metric.
- `detect` still retrains and scores in-sample over the complete feature
  history. Its scores are not calibrated probabilities; offline holdout uses a
  separate training-only reference path.

## Corrected fixed-data benchmark (methodology version 2)

The January local snapshot contains 38,449 readings, 14 stations, and 995
feature rows, with 24 training and eight held-out local dates. PM2.5 held-out
flag rate is 29/74 (39.2%), with weak-label lift 1.47. Synthetic spike recall
is 5/27 (18.5%): 2/6 local and 3/21 regional station-days. The untouched-data
alert rate is 55/326 (16.9%); it is not a measured false-positive rate.

Low synthetic-event recall remains an open limitation. These corrected
January results cannot be compared directly with the older August figures.
The full configuration, denominators, coverage, and library versions are in
[`docs/evaluation/step2-benchmark.json`](../docs/evaluation/step2-benchmark.json).

## Agreement rates (synthetic fixtures)

On injected spike fixtures in `tests/test_detection.py`:

- Spike days show higher `z_score` and `roc_max` than baseline days
- Multi-station elevation triggers weak labels
- Single-station spikes show elevated `spatial_isolation` vs peers
- Agreement count equals the sum of IF, LOF, and DBSCAN votes; emitted scores
  stay within `[0, 1]`

`tests/test_detection_evaluation.py` covers the evaluation code itself: lift
and Precision@K arithmetic, run-history append/read, spike injection, holdout
date separation, out-of-sample DBSCAN scoring, the tuned-`eps` noise budget,
and the `evaluate` command.
