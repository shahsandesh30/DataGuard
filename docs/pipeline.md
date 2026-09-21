# How the pipeline runs, step by step

A walkthrough of what executes, in which file, and what you should see at each
stage. For *why* the system is shaped this way, see [architecture.md](architecture.md).

Everything below matches a real local run: a full January 2026 for four Sydney
stations.

---

## The shape of it

Five stages. They are not connected by function calls — they are connected by
**Parquet tables on disk**:

```
OpenAQ S3 ──ingest──> bronze/ ──conform──> silver/ ──quality──> gold/layer1/ ─┐
                         │                                                     ├─fuse─> gold/fusion/
                         └──────────────detect──────────────> gold/layer2/ ────┘
```

That is why every stage runs standalone. The contract between any two stages is
a table at a known path, and the join key is always `(locationid, date_local)`.

Two helpers cut across all of them:

| File | Responsibility |
|---|---|
| `pipelines/storage.py` | The **only** module that touches `pathlib` or `boto3` for zone I/O. Local vs. S3 lives here and nowhere else. |
| `pipelines/config.py` | Every threshold and zone root, grouped by stage. |

---

## 0. Entry point

`pipelines/__main__.py`

```
main()  →  _parse_args()  →  _roots()  →  COMMANDS[stage](args)
```

1. **`_parse_args`** builds one subcommand per entry in `STAGES`. Every stage
   accepts `--bronze-root / --silver-root / --gold-root`; only `ingest` and
   `run` also take `--locations / --start / --end`.
2. **`_roots`** resolves the three zone roots — **CLI flag → `.env` → `data/bronze`**
   and friends. This is the only place that decision is made.
3. **`COMMANDS`** dispatches to `_ingest / _conform / _quality / _detect / _fuse`.
   `run` loops over all five in order.

A zone root is a plain string. `data/silver` is a folder; `s3://bucket/silver`
is an S3 prefix. Nothing downstream knows which — `storage.py` branches on the
`s3://` prefix. That is why there is no `--target aws` flag.

---

## 1. `ingest` — OpenAQ archive → bronze

`_ingest` → `fetch_range` → `fetch_location_day` *(once per station × per day)*

Inside one `fetch_location_day` (`pipelines/ingestion/fetch.py`):

| Step | Function | What it does |
|---|---|---|
| 1 | `archive_key` | Builds OpenAQ's object key: `records/csv.gz/locationid=2178/year=2023/month=01/location-2178-20230105.csv.gz` |
| 2 | `storage.file_info` | Already in bronze? → return `skipped`, no download |
| 3 | `download_archive_object` | Downloads to a **temp file** with an *unsigned* S3 client — OpenAQ's bucket is public |
| 4 | `storage.put_file` | Moves the temp file into the bronze zone |
| 5 | `_append_manifest` | Appends one JSON line to `_manifest.jsonl` |

**Why stage through a temp file.** The source bucket is read with no
credentials; your destination may need different ones. Staging keeps the two
apart, and means a failed download never leaves a half-written file in bronze.

**Why the manifest matters.** It records `missing` files too. A gap in the
source *is* a signal, and this log is the only record of *when* a file arrived —
Layer 1 reads it later for the freshness metric.

**Expect:**

```
INFO Ingest: {'copied': 90, 'missing': 31}
```

`missing` is normal. OpenAQ genuinely has no file for some station-days.

**Lands at:** `data/bronze/locationid=<ID>/year=<YYYY>/location-<ID>-<YYYYMMDD>.csv.gz`
plus `data/bronze/_manifest.jsonl`.

---

## 2. `conform` — bronze → silver

`_conform` → `build_silver` → `_conform_all` (`pipelines/conformance/conform.py`)

For each bronze file, **`conform_measurements`** does five things in order:

1. **`_rename_raw_columns`** — maps provider spellings to canonical ones via
   `COLUMN_ALIASES`. The lookup key is the column name **lowercased with
   underscores stripped**, so one entry covers `location_id` / `locationId` /
   `LOCATIONID`.
2. **Required-column check** — raises `ValueError` if anything in
   `REQUIRED_AFTER_RENAME` is absent.
3. **`convert_series`** (`units.py`) — converts to canonical units. ppm/ppb/mg
   → µg/m³ at 25 °C and 1 atm.
4. **Timestamps** — UTC `datetime`, local `datetime_local`, and `date_local`.
   **`date_local` is the join key every later stage uses.**
5. Returns exactly the 12 `SILVER_COLUMNS`.

Back in `_conform_all`: concatenate everything, then
`drop_duplicates(ROW_KEY)` where `ROW_KEY = [locationid, sensor_id, datetime, parameter]`
— one sensor cannot report the same parameter twice at one instant.

Finally `write_silver` → `storage.write_parquet(..., glue_table="silver_data")`.

> ⚠️ **Step 2 is where the pipeline's longest-lived bug lived.** A missing alias
> for `location_id` meant every archive file failed the required-column check,
> and the failure was only a `logger.warning` — so silver was empty for months
> while the pipeline reported success. If `files_failed` is ever non-zero, stop
> and read `data/silver/_build.json` → `failed[]`.

**Nothing is dropped or clipped.** Negative concentrations and stuck values
*must* survive into silver — Layer 1's whole job is finding them.
`original_unit` is kept alongside the converted value so Layer 1 can tell a
converted reading from a mislabelled one.

**Expect:**

```
INFO Silver: 9779 rows, 94 files read, 0 failed -> data/silver
```

**Lands at:** `data/silver/locationid=<ID>/year=<YYYY>/part-0.parquet` +
`_build.json` (rows, parameters, units, date range, failures).

---

## 3. `quality` — silver + bronze → gold/layer1

`_quality` → `build_quality` (`pipelines/quality/build.py`)

**The only stage that reads two zones.** Five steps:

### 3a. `read_silver`

The 9,779 conformed rows back out.

### 3b. `compute_sensor_day_metrics` — `metrics.py`

Groups by `(locationid, sensor_id, parameter, date_local)`. Per group:
readings received, `negative_count`, `max_stuck_run`, variance, duplicates,
unit mismatches.

`readings_expected` comes from **`_expected_readings`** — the sensor's own
**trailing 7-day median cadence**, not a fixed 24. A station that reports four
times a day is not 83% incomplete.

→ **426 sensor-days.**

### 3c. `compute_station_day_metrics` — `metrics.py`

Rolls up to `(locationid, date_local)` and adds the three signals that **cannot
come from the readings** — this is why bronze is read here:

| Signal | Source |
|---|---|
| `file_present` | `storage.exists()` on the expected archive filename |
| `file_lateness_hours` | `load_bronze_manifest` — the arrival log |
| `schema_changed` | `_schema_drift` — reads each file's **header row only** |

Plus `sensor_dropout_count`: sensors that reported yesterday and not today.

→ **98 station-days, 18 columns.**

### 3d. `apply_quality_rules` — `rules.py`

Walks the `_RULES` list. Each rule is a dict with a `check` lambda over one
metrics row. Every rule that fires emits an incident carrying a
`metric_snapshot` — the whole metrics row as JSON — so an incident is always
auditable back to the numbers that caused it.

→ **13 incidents:**

| Rule | Type | Severity | Count |
|---|---|---|---|
| R2 | `stuck_sensor` | high | 5 |
| R4 | `completeness_gap` | medium | 4 |
| R7 | `missing_file` | high | 4 |

### 3e. The model — `detector.py`

```
fit_quality_model  →  save_quality_model  →  score_quality  →  model_incidents
```

`fit_quality_model` **returns `None`** unless some location has at least
`MIN_STATION_DAYS` (14) of history. Otherwise: `StandardScaler` +
`IsolationForest` over the 12 numeric `METRIC_COLUMNS`.

`_drop_duplicate_of_rules` then keeps model incidents **only for station-days no
rule already caught** — the model has to earn its place by finding what the
rules missed.

→ **1 model incident (`M1`).**

**Expect:**

```
INFO Layer 1: 98 station-days, 13 rule incidents, 1 model incidents (trained=True) -> data\gold\layer1
```

**`trained=False` is expected on a small sample, not a bug.** It means no
location reached 14 station-days, so the stage ran rules-only and said so.

**Lands at:** `gold/layer1/quality_metrics`, `quality_sensor_metrics`,
`quality_incidents`.

---

## 4. `detect` — bronze → gold/layer2

`_detect` → `build_detection` (`pipelines/detection/build.py`)

> This package is owned by another team member. Treat it as read-only.

```
read_conformed(bronze)  →  build_event_features  →  weak_labels
                        →  fit_ensemble  →  score_events
```

Features are per `(locationid, date_local, parameter)`: baseline deviation,
z-score, spikes, sustained elevation, plus **regional peer comparison** — a real
smoke event lifts several stations together, while one station alone is more
likely a fault. Detectors: **IsolationForest + LOF + DBSCAN**, gated on
`MIN_EVENT_ROWS` (20).

Two things to know about how it connects:

- It calls `read_conformed`, which **re-conforms bronze in memory** rather than
  reading the silver zone. Same conformance code, but the two layers can
  silently disagree about the same day. Migrating it is a known follow-up.
- `_detect` in `__main__.py` has an `if storage.is_s3(gold)` branch that runs
  Layer 2 into a local temp directory and republishes the two tables. That
  exists only because `detection/build.py` writes its summary with
  `Path.write_text`, which cannot address S3.

**Expect:**

```
INFO Layer 2: 164 feature rows, 164 alerts (trained=True) -> data\gold\layer2
```

**Lands at:** `gold/layer2/event_features`, `event_alerts`, plus
`_detection_build.json`.

---

## 5. `fuse` — gold → gold/fusion

`_fuse` → `build_fusion` → `fuse` (`pipelines/fusion/trust_score.py`)

```
read_quality_incidents (layer1)  ─┐
                                  ├→  fuse()  →  fusion/trust_alerts
read_event_alerts      (layer2)  ─┘
```

Inside `fuse`:

1. **`aggregate_incidents`** collapses Layer 1 to one row per
   `(locationid, date_local)` carrying `max_severity` and the rule ids that fired.
2. **Left merge** onto the alerts. Left, not inner — alerts on healthy days must
   survive with nulls.
3. `trust_score = clip(alert_score × (1 − penalty), 0, 1)`, penalty
   **0.2 / 0.4 / 0.7** for low / medium / high. When several incidents fire the
   same day the **maximum** severity is used — the conservative choice.
4. `status` = `quarantined` if any Layer 1 incident that day, else `escalated`.

**Expect:**

```
INFO Fusion: 164 alerts (147 escalated, 17 quarantined) -> data\gold\fusion
```

Trust scores span 0.105 – 1.000. The 17 quarantines break down as R2 ×9,
R4+R7 ×7, M1 ×1.

> ⚠️ **Fusion is alert-driven.** No Layer 2 alerts means no fusion rows, *even if
> Layer 1 found plenty*. Those Layer-1-only findings are not lost — the dashboard
> renders them as `quality_only`.

**Lands at:** `gold/fusion/trust_alerts` + `_build.json`.

---

## Running it and checking the receipts

```bash
python -m pipelines conform
python -m pipelines quality
python -m pipelines detect
python -m pipelines fuse
```

Each zone writes a summary of its own last run:

```bash
cat data/silver/_build.json                 # rows, parameters, units, date range, failures
cat data/gold/layer1/_build.json            # station-days, incidents, model_trained
cat data/gold/layer2/_detection_build.json  # feature rows, alerts, ensemble_trained
cat data/gold/fusion/_build.json            # escalated vs quarantined
```

The fastest sanity check is the chain of row counts:

**94 files → 9,779 rows → 426 sensor-days → 98 station-days → 164 alerts → 164 fused.**

If any link collapses to zero, that is the stage to open.

## When a number looks wrong

| Symptom | Almost always means |
|---|---|
| `files_failed` > 0 | A bronze column name has no entry in `COLUMN_ALIASES`. Check `_build.json` → `failed[]`. |
| Silver rows = 0 | Same cause. Conformance failures are warnings, not errors. |
| `trained=False` | Not enough history — 14 station-days (L1) or 20 feature rows (L2). Ingest a longer range. |
| 0 fusion rows but L1 incidents exist | Expected. Fusion is alert-driven; look for `quality_only` on the dashboard. |
| Almost everything quarantined | A rule is firing on nearly every station-day. Check the `rule_id` histogram in `quality_incidents` before trusting the rate. |
