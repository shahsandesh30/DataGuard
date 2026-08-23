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
├── data/sample/          # local OpenAQ samples used for development (gitignored)
├── notebooks/            # exploration notebooks
├── src/dataguard/
│   ├── config.py         # shared constants — bucket names, schema column names
│   ├── ingestion/        # pulls raw files into bronze
│   ├── conform/          # bronze → silver harmonisation
│   ├── quality/          # Layer 1 — data health detection
│   ├── features/         # Layer 2 — pollution event detection
│   └── fusion/           # trust scoring, escalate/quarantine logic
├── dashboard/
│   └── app.py            # Streamlit app
├── infra/                # CloudFormation / Terraform scripts
├── tests/
├── requirements.txt
└── README.md
```

## Getting started

```bash
python3 -m venv venv
source venv/bin/activate
pip install -r requirements.txt
```

Create a `.env` file (not committed) with:
```
AWS_PROFILE=dataguard-academy
AWS_REGION=us-east-1
BUCKET_RAW=dataguard-raw
BUCKET_SILVER=dataguard-silver
```

Pull a small OpenAQ sample to develop against (no AWS account required — the archive is public):
```bash
aws s3 cp --no-sign-request --recursive \
  s3://openaq-data-archive/records/csv.gz/locationid=2178/year=2023/month=01/ \
  ./data/sample/
```

## Project status

| Phase | Deliverable | Gate | Status |
|---|---|---|---|
| Foundation | Ingestion automated; bronze zone populated; Glue Catalog active | Data queryable via Athena | In progress |
| Conformance | Silver zone; consistent units/types across providers | Single queryable table | Not started |
| Layer 1 | Quality metrics; drift tests; anomaly model | Detects known failures unprompted | Not started |
| Layer 2 | Pollution event features; detector ensemble | Ranked anomaly output | Not started |
| Fusion | Trust scoring; dashboard deployed | Public URL live | Not started |
| Consolidation | Documentation, final report, presentation | Submission | Not started |

## Links

- Jira board: see team workspace
- Assessment 1 (Project Proposal and Design) — submitted Aug 11, 2026