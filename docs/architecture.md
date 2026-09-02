# Architecture

See `diagrams/architecture_openaq.drawio` for the diagram source.

## Zones

| Zone | Bucket | Format | Contents |
|---|---|---|---|
| Bronze | `dataguard-bronze` | csv.gz | Raw daily files under `locationid=<ID>/year=<YYYY>/` |
| Silver | `dataguard-silver` | Parquet | Conformed exports under `locationid=<ID>/year=<YYYY>/` |
| Gold | `dataguard-gold` | Parquet | Layer 1 under `layer1/` (quality metrics, incidents); Layer 2 under `layer2/` (event features, ranked alerts); fusion output with trust scores |

A Glue Crawler catalogues every bronze file, so structural changes in the
source are recorded over time and become detectable as schema drift.

## Detection layers

- **Layer 1 — data health.** Quality metric vector per station-day: readings
  received, missing readings, stuck-value runs, negative concentrations, file
  lateness against the stated 72-hour delivery commitment, schema drift.
  Isolation Forest over the metric time-series.
- **Layer 2 — pollution events.** Station and region level features over the
  silver measurements. Ensemble of Isolation Forest, LOF and DBSCAN with
  deterministic rules as weak labels.
- **Fusion.** Joins the layers on station and time window. Every Layer 2 alert
  receives a trust score from Layer 1; alerts coinciding with quality incidents
  are quarantined for human review, never deleted.

## Orchestration

Batch processing (not streaming) — OpenAQ archive files are written roughly
72 hours after the end of each day in the location's timezone. AWS Step
Functions on a schedule:

```
fetch → crawl (Glue) → conform → L1 metrics → L1 score
                                → L2 features → L2 score
                                → fusion → publish gold
```

## Serving

Gold zone queried through Amazon Athena and displayed on a Streamlit
dashboard. Local DuckDB serving is the fallback if AWS Academy credits run
out (risk R2).

## Delivery phases

| Phase | Deliverable | Gate |
|---|---|---|
| Foundation | Ingestion automated; bronze zone populated; Glue Catalog active | Data queryable via Athena |
| Conformance | Silver zone; units and types consistent across providers | Single queryable table |
| Layer 1 | Quality metrics per station-day; drift tests; anomaly model | Detects known failures unprompted |
| Layer 2 | Pollution event features; detector ensemble | Ranked anomaly output |
| Fusion | Trust scoring; dashboard deployed | Public URL live |
| Consolidation | Documentation, final report, presentation | Submission |
