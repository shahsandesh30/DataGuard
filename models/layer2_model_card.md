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

## Limitations

- **No evaluation metrics yet.** Precision@K against the weak labels has not been
  computed on real gold — this is the main open task for the layer.
- Trailing 7-day baselines are noisy with <7 days of history per station.
- No reference-monitor accuracy metric.
- The ensemble is retrained and scored in-sample over complete feature history;
  a held-out temporal evaluation is still required before treating scores as
  calibrated probabilities.

## Agreement rates (synthetic fixtures)

On injected spike fixtures in `tests/test_detection.py`:

- Spike days show higher `z_score` and `roc_max` than baseline days
- Multi-station elevation triggers weak labels
- Single-station spikes show elevated `spatial_isolation` vs peers
- Agreement count equals the sum of IF, LOF, and DBSCAN votes; emitted scores
  stay within `[0, 1]`

Precision@K on real gold is still outstanding; a full month is now ingested, so
nothing blocks it.
