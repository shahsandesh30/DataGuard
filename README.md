# DataGuard: Trust-Aware Anomaly Detection for Untrusted Data Pipelines

**PRT661 – Data Science Practice | Charles Darwin University | Semester 2, 2026**

## The problem

A monitoring system usually cannot tell a real event from a broken pipeline. If
three of a city's five air-quality sensors go offline, the average reading
changes — and a naive system reports that as "air quality improved."

DataGuard runs two detectors that never talk to each other:

- **Layer 1** asks *was the data healthy?* (stuck sensors, missing files, gaps)
- **Layer 2** asks *did something happen?* (pollution spikes)

Only at the end are they joined. An alert raised on a day when the data was
broken is **quarantined** for review rather than escalated. Quarantined alerts
are always shown, never deleted — this is public-safety data.

## Team

| Name | Role | Responsibility |
|---|---|---|
| Sandesh Shahi | Project lead | Fusion layer, repo governance, architecture, integration, reporting |
| Aadarsh Ghimire | Data engineer | Ingestion, bronze/silver/gold zone design, partitioning, Glue Catalog |
| Orchid Shrestha | Data quality engineer | Layer 1 quality metrics, schema drift testing, quality anomaly model |
| Sandesh Prasad Paudel | ML engineer | Layer 2 feature engineering, detector ensemble, model evaluation |
| Shuvechchha Pun | Analytics & visualisation | Gold table design, Athena queries, dashboard, station mapping |

## The pipeline

Five stages. Each one reads the zone the previous one wrote — there is no hidden
state between them, so any stage can be re-run on its own.

| Stage | Reads | Writes | What it does |
|---|---|---|---|
| `ingest` | OpenAQ public archive | `bronze/` | Copies daily `.csv.gz` files, byte for byte. Logs every attempt — including files that were *missing*, because a gap in the source is itself a signal. |
| `conform` | `bronze/` | `silver/` | One harmonised table: canonical names and units, parsed timestamps, de-duplicated. Nothing is dropped. |
| `quality` | `silver/` + `bronze/` | `gold/layer1/` | **Layer 1.** Per-sensor-day and per-station-day metrics → rules → Isolation Forest. |
| `detect` | `bronze/` | `gold/layer2/` | **Layer 2.** PM features per station-day → IF + LOF + DBSCAN ensemble → ranked alerts. |
| `fuse` | `gold/` | `gold/fusion/` | Joins the two on (station, day). `trust = alert_score × (1 − penalty)`. |

[`docs/pipeline.md`](docs/pipeline.md) traces this properly: which file runs at
each step, and what you should see.

Two things are worth knowing about the shape of this:

- **`quality` reads bronze as well as silver.** Whether a file arrived, when it
  arrived, and whether its columns changed are properties of the *delivered
  file*, not of the readings inside it.
- **`fuse` is alert-driven.** No Layer 2 alerts means no fusion rows, even if
  Layer 1 found plenty. Those Layer-1-only findings still reach the dashboard as
  `quality_only`.

## Running it

```bash
python -m venv venv
venv\Scripts\activate          # macOS/Linux: source venv/bin/activate
pip install -r requirements.txt
cp .env.example .env
```

Everything defaults to the local `data/` directory. No AWS account is needed.

```bash
# everything, end to end
python -m pipelines run --locations 1544061 1601414 2455394 6430870 \
                        --start 2026-01-01 --end 2026-01-31

# or one stage at a time, against whatever is already on disk
python -m pipelines ingest --locations 2178 --start 2023-01-01 --end 2023-01-31
python -m pipelines conform
python -m pipelines quality
python -m pipelines detect
python -m pipelines fuse

streamlit run dashboard/app.py
```

Both models are gated on history — Layer 1's Isolation Forest needs 14
station-days for some location, Layer 2's ensemble needs 20 feature rows. Below
that a stage runs rules-only and says so in its log line, rather than fitting on
noise. **Empty model output on a small sample is expected, not a bug.**

## Layout

```
pipelines/
  config.py         every threshold and zone root, in one file
  storage.py        read/write a zone — local directory or S3, same calls
  ingestion/        OpenAQ archive -> bronze
  conformance/      bronze -> silver  (conform.py, units.py)
  quality/          Layer 1  (metrics.py -> rules.py -> detector.py -> build.py)
  detection/        Layer 2  — owned by another team member, treat as read-only
  fusion/           trust scoring  (trust_score.py, build.py)
  __main__.py       the CLI
dashboard/app.py    Streamlit: alerts, Layer 1 health, station map
tests/              pytest; no test needs AWS credentials
```

### Zones on disk

| Zone | Path | Format |
|---|---|---|
| Bronze | `data/bronze/locationid=<ID>/year=<YYYY>/location-<ID>-<YYYYMMDD>.csv.gz` | gzip CSV, as fetched |
| Silver | `data/silver/locationid=<ID>/year=<YYYY>/part-0.parquet` | Parquet |
| Gold | `data/gold/{layer1,layer2,fusion}/<table>/locationid=<ID>/year=<YYYY>/` | Parquet |

Each zone also holds a `_build.json` summary of the last run, and bronze holds
`_manifest.jsonl`, the arrival log that `quality` reads.

**Silver schema** (12 columns): `locationid`, `sensor_id`, `location_name`,
`datetime` (UTC), `datetime_local`, `date_local`, `latitude`, `longitude`,
`parameter`, `unit`, `value`, `original_unit`.

Readings are conformed but never dropped or clipped. Negative concentrations and
stuck values survive into silver on purpose — Layer 1 exists to find them — and
`original_unit` is kept so it can tell a converted value from a mislabelled one.

## Running against AWS

A zone root is just a string: `data/silver` is a directory, `s3://bucket/silver`
is an S3 prefix. `pipelines/storage.py` dispatches on the scheme, so moving a
zone to AWS is a config change, not a code change.

```bash
python -m pipelines run --locations 2392564 --start 2026-08-01 --end 2026-08-31 \
  --bronze-root s3://dataguard-openaq-bronze \
  --silver-root s3://dataguard-openaq-silver \
  --gold-root   s3://dataguard-openaq-gold
```

…or permanently, by uncommenting the `s3://` lines in `.env`. Zones are
independent — S3 bronze with local gold is valid.

Parquet written to S3 is registered in the **Glue Catalog** on the way out, so
Athena sees it with no crawler run: `silver_data`, `layer1_quality_metrics`,
`layer1_quality_sensor_metrics`, `layer1_quality_incidents`,
`layer2_event_features`, `layer2_event_alerts`, `fusion_trust_alerts`.

Silver registers as `silver_data` on purpose — that is the table
`pipelines/detection/io.py` queries, so Layer 2 reads the silver this pipeline
produced. Reads go straight to S3 rather than through Athena: same bytes, no
workgroup needed, no per-query charge.

## Documentation

| File | Contents |
|---|---|
| [`docs/pipeline.md`](docs/pipeline.md) | **Start here.** Step-by-step: what runs, in which file, what to expect |
| [`docs/architecture.md`](docs/architecture.md) | Why it is shaped this way — two-layer design, medallion zones, batch |
| [`docs/data-source.md`](docs/data-source.md) | OpenAQ, the archive layout, and the documented degradation events E1-E8 |
| [`docs/risk-register.md`](docs/risk-register.md) | Project risks and the mid-project go/no-go gate |
| [`models/`](models/) | Model cards for both layers, and the fusion trust-score spec |
| [`CLAUDE.md`](CLAUDE.md) | Repo conventions and gotchas, for anyone (or anything) editing the code |

## Tests

```bash
python -m pytest                                  # 66 tests
python -m ruff check pipelines dashboard tests
```

The S3 path is tested against a fake awswrangler, so the suite runs with no
credentials. Ruff reports 9 findings, all inside `pipelines/detection/`.

## Where it stands

Current local run — a full January 2026 for four Sydney stations:

| | |
|---|---|
| Bronze | 94 files (90 copied, 31 missing at source) |
| Silver | 9,779 rows, 5 parameters |
| Layer 1 | 98 station-days → 13 rule incidents + 1 model incident |
| Layer 2 | 164 feature rows → 164 ranked alerts |
| Fusion | 164 alerts — **147 escalated, 17 quarantined** |

| Phase | Gate | Status |
|---|---|---|
| Foundation | Data queryable via Athena | Working, local and S3 |
| Conformance | Single queryable table | Working |
| Layer 1 | Detects known failures unprompted | Rules + model working; not yet tested against seeded failures |
| Layer 2 | Ranked anomaly output | Working; no evaluation metrics yet |
| Fusion | Public dashboard URL | Trust engine working; dashboard runs locally, not deployed |
| Consolidation | Final report | Not started |

## Next steps

1. **Seed known failures and measure.** Corrupt a copy of silver on purpose —
   freeze a sensor, delete a day, mislabel a unit — and check Layer 1 catches
   each one. Until this exists, "it works" is an assertion, not a result.
2. **Evaluate Layer 2** against the weak labels it already generates: precision
   at top-k, and how many alerts fusion correctly holds back.
3. **Point Layer 2 at silver instead of bronze.** It re-conforms bronze in
   memory today, so the two layers can silently disagree about the same day.
4. **Restore rule R6 (freshness) behind a scheduled run** — see below.
5. **Deploy the dashboard** and put the AWS resources under Terraform.

### Why R6 is currently disabled

`file_lateness_hours` is measured from when *we downloaded* a file, against
OpenAQ's 72-hour publication commitment. Backfilling January in September made
every file ~240 days "late", so R6 fired on 90 of 98 station-days and dragged
almost every alert into quarantine — 160 of 164. The metric is still computed
and still feeds the Layer 1 model; only the rule is off. Restore it once the
pipeline runs on a schedule, where the measurement means something.
