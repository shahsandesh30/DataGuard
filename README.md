# DataGuard: Trust-Aware Anomaly Detection for Untrusted Data Pipelines

**PRT661 – Data Science Practice | Charles Darwin University | Semester 2, 2026**

DataGuard is a two-layer anomaly detection system for global air quality data. Instead of raising an alert every time a reading looks unusual, it first checks whether the *data itself* was healthy that day. Alerts raised while the underlying data was broken are quarantined for review rather than escalated — reducing false alarms caused by sensor failures, not real pollution events.

## Problem

Monitoring systems can't always tell the difference between a real event and a broken pipeline. If three of a city's five air quality sensors go offline, the average reading changes — a naive system reports this as "improvement," which is false. DataGuard addresses this by running data-quality detection and pollution-event detection as two separate layers, then fusing them into a trust score before anything reaches a human.

## Team

| Name | Role | Responsibility |
|---|---|---|
| Sandesh Shahi | Project lead | Fusion layer, repo governance, architecture, integration, reporting |
| Aadarsh Ghimire | Data engineer | Ingestion pipeline, bronze/silver/gold zone design, partitioning, Glue Crawler & Catalog, orchestration |
| Orchid Shrestha | Data quality engineer | Layer 1 quality metrics, schema drift testing, quality anomaly model |
| Sandesh Prasad Paudel | ML engineer | Layer 2 feature engineering, anomaly detector ensemble, model evaluation |
| Shuvechchha Pun | Analytics & visualisation | Gold table design, Athena queries, dashboard build, station mapping |

## Architecture

```
OpenAQ Open Data Archive (public S3)
        │
        ▼
  Bronze zone (S3)  — raw, immutable, per station/day
        │
        ▼
  Silver zone (S3)  — harmonised units, types, schema
        │
   ┌────┴────┐
   ▼         ▼
Layer 1    Layer 2
(data      (pollution
 health)    events)
   │         │
   └────┬────┘
        ▼
  Trust Fusion — joins on (station, day), escalates or quarantines
        │
        ▼
  Gold zone (S3) — scored alerts + quality incidents
        │
        ▼
  Amazon Athena (SQL serving layer)
        │
        ▼
  Streamlit dashboard (public URL)
```

Batch processing is used throughout — OpenAQ source data is published with an approximate two-month lag, so real-time streaming isn't needed. AWS Step Functions handles scheduled orchestration.

## Tech stack

- **Language:** Python (pandas, scikit-learn, PyOD, scipy)
- **Storage & processing:** Amazon S3, AWS Glue (Crawler + Catalog)
- **Orchestration:** AWS Lambda, AWS Step Functions
- **Serving:** Amazon Athena
- **Dashboard:** Streamlit
- **Infra:** CloudFormation / Terraform (kept as reproducible scripts, not manual console setup)
- **Project management:** Jira (Scrum board)

## Repository structure

```
DataGuard/
├── data/
│   ├── bronze/locationid=<ID>/year=<YYYY>/location-<ID>-<YYYYMMDD>.csv.gz
│   └── silver/locationid=<ID>/year=<YYYY>/<export_id>   # Parquet
├── notebooks/
├── pipelines/
│   ├── config.py
│   ├── ingestion/        # pulls raw files into bronze
│   ├── conformance/      # bronze → silver harmonisation
│   ├── quality/          # Layer 1 — data health detection
│   ├── detection/        # Layer 2 — pollution event detection
│   └── fusion/           # trust scoring, escalate/quarantine logic
├── dashboard/
│   └── app.py
├── tests/
├── requirements.txt
└── README.md
```

## Getting started

```bash
python3 -m venv venv
source venv/bin/activate   # Windows: venv\Scripts\activate
pip install -r requirements.txt
```

Copy `.env.example` to `.env` and adjust if needed. Local bronze/silver paths default to `data/bronze` and `data/silver` — no AWS account is required for development.

Fetch OpenAQ archive files into bronze (public bucket, unsigned requests) and build the silver table:

```bash
python -m pipelines run --locations 2178 --start 2023-01-01 --end 2023-01-31
```

That command:

1. Adopts any flat `location-*.csv.gz` files (or legacy `records/csv.gz/...` tree) into bronze layout
2. Downloads missing location-days from `s3://openaq-data-archive`
3. Writes conformed Parquet exports to `data/silver/locationid=<ID>/year=<YYYY>/`

**Local zone layouts:**

| Zone | Path pattern | Format |
|---|---|---|
| Bronze | `data/bronze/locationid=<ID>/year=<YYYY>/location-<ID>-<YYYYMMDD>.csv.gz` | gzip CSV, as fetched |
| Silver | `data/silver/locationid=<ID>/year=<YYYY>/<timestamp>_qnxhe_<uuid>` | Parquet (8 columns) |

Silver columns: `sensor_id`, `location`, `datetime`, `latitude`, `longitude`, `parameter`, `unit`, `value`.

Ingest or conform can also be run separately:

```bash
python -m pipelines ingest --locations 1544061 1601414 --start 2026-01-01 --end 2026-01-31
python -m pipelines conform
```

Build silver and Layer 1 from bronze already on disk (no ingest):

```bash
python -m pipelines conform
python -m pipelines quality
```

Gold output:

- Layer 1: `data/gold/layer1/quality_metrics/`, `data/gold/layer1/quality_incidents/`
- Layer 2: `data/gold/layer2/event_features/`, `data/gold/layer2/event_alerts/`

```bash
python -m pipelines conform
python -m pipelines quality
python -m pipelines detect
```

Or run the full pipeline:

```bash
python -m pipelines run --locations 1544061 1601414 2455394 6430870 --start 2026-01-01 --end 2026-01-31
```

See `notebooks/01_bronze_profiling.ipynb`, `notebooks/02_silver_conformance.ipynb`, `notebooks/03_quality_metrics_eda.ipynb`, `notebooks/04_layer2_features.ipynb`, and `notebooks/data_test.ipynb`.

## Project status

| Phase | Deliverable | Gate | Status |
|---|---|---|---|
| Foundation | Ingestion automated; bronze zone populated; Glue Catalog active | Data queryable via Athena | In progress (local bronze working) |
| Conformance | Silver zone; consistent units/types across providers | Single queryable table | In progress (local silver: locationid/year Parquet exports) |
| Layer 1 | Quality metrics; drift tests; anomaly model | Detects known failures unprompted | In progress (rules + gold working; IF gated on history) |
| Layer 2 | Pollution event features; detector ensemble | Ranked anomaly output | In progress (features + ensemble; gated on history) |
| Fusion | Trust scoring; dashboard deployed | Public URL live | Not started |
| Consolidation | Documentation, final report, presentation | Submission | Not started |

## Links

- Jira board: see team workspace
- Assessment 1 (Project Proposal and Design) — submitted Aug 11, 2026