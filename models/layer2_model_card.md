# Layer 2 Model Card — Pollution Event Detection Ensemble

## Purpose

Detect genuine pollution events (smoke, dust, industrial spikes) at
`(location_id, date_local, parameter)` grain. Layer 2 is **not** data-health
detection — stuck sensors, missing files, and schema drift belong to Layer 1.

## Status

| Component | Status |
|---|---|
| Feature engineering | **Active** — station-day-parameter features |
| Weak labels (eval only) | **Active** — regional PM2.5 elevation heuristic |
| IF + LOF + DBSCAN ensemble | **Gated** — trains only when ≥20 feature rows |

With the current local sample (~1 day per location), features are written but
the ensemble is skipped. Fetch more history before expecting ranked alerts:

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

Parameters: `pm25`, `pm10`, `pm1`. Regional bucket: `sydney_metro`.

## Model

- **Algorithms:** `IsolationForest`, `LocalOutlierFactor`, `DBSCAN`
- **Preprocessing:** `StandardScaler` on numeric feature columns
- **Scoring:** Per-detector flags + normalized scores; combined rank uses
  detector agreement and mean score
- **Output:** Top-K alerts per region-day in `data/gold/layer2/event_alerts/`

## Weak labels (evaluation only)

Per risk R4 in `docs/data-source.md`:

```
regional_pm25_mean > 2 × trailing_regional_median
AND ≥2 locations elevated same day
```

Used for Precision@K narrative in `notebooks/04_layer2_features.ipynb` — **never**
used as training labels for the ensemble.

## Limitations

- Sparse sample: ensemble cannot train until `MIN_EVENT_ROWS` (20) feature rows
- Trailing 7-day baselines are noisy with <7 days of history per station
- No reference-monitor accuracy metric
- Fusion trust scoring deferred to Layer 3 (`pipelines/fusion/`)

## Agreement rates (synthetic fixtures)

On injected spike fixtures in `tests/test_detection.py`:

- Spike days show higher `z_score` and `roc_max` than baseline days
- Multi-station elevation triggers weak labels
- Single-station spikes show elevated `spatial_isolation` vs peers

Re-run notebook section 4 for Precision@K on real gold once 30+ days are ingested.
