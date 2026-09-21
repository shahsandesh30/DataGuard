# Architecture

Why the system is shaped this way. For a step-by-step trace of what actually
executes, see [pipeline.md](pipeline.md).

Diagram source: `diagrams/architecture_openaq.drawio` (PNG alongside it).

## The core decision: two independent detectors

A monitoring system usually cannot tell a real event from a broken pipeline. If
three of a city's five sensors go offline, the average reading changes — and a
naive system reports that as "air quality improved."

So DataGuard runs two detectors that **never see each other's output**:

- **Layer 1 — data health.** A quality metric vector per station-day: readings
  received vs. expected, stuck-value runs, negative concentrations, sensor
  dropout, file presence, schema drift, unit conformance. Deterministic rules
  first, then an Isolation Forest over the metric vectors to catch multivariate
  failures no single rule describes.
- **Layer 2 — pollution events.** Station- and region-level features over the
  measurements. Ensemble of Isolation Forest, LOF and DBSCAN, with
  deterministic rules supplying weak labels for evaluation only.

**Fusion** joins them on `(station, day)` at the very end. Each Layer 2 alert
gets a trust score discounted by Layer 1's severity that day. Alerts coinciding
with a quality incident are **quarantined** for human review.

Independence is the whole premise. If Layer 2 could see Layer 1's verdict it
would learn to suppress alerts on unhealthy days, and the system would lose the
ability to say *"something happened, and also the data was broken"* — which is
exactly the case a human needs to see.

**Quarantined alerts are held, never deleted or hidden.** This is public-safety
data; the system reduces noise, it does not withhold information. The dashboard
always renders them.

## Why a medallion lake

| Zone | Format | Contents | Guarantee |
|---|---|---|---|
| **Bronze** | gzip CSV, byte-unchanged | Raw daily files under `locationid=<ID>/year=<YYYY>/`, plus `_manifest.jsonl` | Immutable. What the source actually sent. |
| **Silver** | Parquet | Conformed measurements under `locationid=<ID>/year=<YYYY>/`. Registered in Glue as `silver_data` | Harmonised, never filtered. |
| **Gold** | Parquet | `layer1/` metrics + incidents, `layer2/` features + alerts, `fusion/trust_alerts` | Derived, reproducible from silver. |

Three properties make this work for a trust-detection system specifically:

1. **Bronze is kept byte-identical.** Layer 1 needs to inspect the delivered
   file — its arrival time, its header row, whether it exists at all. A
   pipeline that parsed on ingest would destroy the evidence.
2. **Silver never drops or clips.** Negative concentrations and stuck values
   survive conformance on purpose, because they are the signal Layer 1 exists to
   find. `original_unit` is carried next to the converted value so a mislabelled
   unit stays visible.
3. **Every stage is re-runnable.** Stages share no in-memory state; the contract
   between them is a table at a known path.

Partitioning is `locationid=<ID>/year=<YYYY>` in both zones, which keeps Athena
scans bounded (risk R2) and matches the source's own layout.

## Local and AWS are the same code

A zone root is a plain string: `data/silver` is a directory,
`s3://bucket/silver` is an S3 prefix. `pipelines/storage.py` dispatches on the
scheme and is the only module that touches `pathlib` or `boto3` for zone I/O.
There is no `--target aws` flag, and zones are independent — S3 bronze with
local gold is valid.

Parquet written to S3 is **registered in the Glue Catalog on the way out**, so
Athena sees each table with no crawler run. Reads go straight to S3 rather than
through Athena: same bytes, no workgroup needed, no per-query charge.

Silver registers as `silver_data` deliberately — that is the table
`pipelines/detection/io.py` queries, so Layer 2 reads the silver this pipeline
produced.

## Why batch, not streaming

OpenAQ writes archive files roughly **72 hours after the end of each day** in
the location's timezone. There is no low-latency source to stream from, so
batch is not a compromise here — it matches the data's real cadence.

That 72-hour figure is a *published delivery commitment*, which is what makes
freshness measurable against a stated promise rather than a threshold we
invented. See [data-source.md](data-source.md).

Scheduled orchestration (AWS Step Functions) is designed but **not yet built**:

```
fetch → conform → L1 metrics → L1 score ─┐
                → L2 features → L2 score ─┴→ fusion → publish gold
```

Today the five stages are run from the CLI, individually or via
`python -m pipelines run`.

## Serving

Gold is queried through Amazon Athena and rendered by a Streamlit dashboard
(`dashboard/`). Local DuckDB serving is the fallback if AWS Academy credits run
out — risk R2 in [risk-register.md](risk-register.md).

## Known architectural gaps

| Gap | Consequence |
|---|---|
| Layer 2 reads **bronze**, re-conforming in memory, rather than silver | The two layers can silently disagree about the same day |
| `detection/build.py` writes its summary with `Path.write_text` | `detect` needs a local staging hop when gold is on S3 |
| No evaluation metrics for either layer | "It works" is currently an assertion, not a result |
| AWS resources created by hand | Not reproducible; contradicts risk R3's mitigation |
