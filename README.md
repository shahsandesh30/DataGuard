# DataGuard

**Trust-aware anomaly detection for untrusted data pipelines.**

Anomaly detection systems raise alarms when data looks strange. But data can look strange for two very different reasons: something happened in the world, or something broke in the pipeline. Systems that cannot tell these apart flood their users with false alerts until the alerts get ignored.

DataGuard runs two detection layers over the same pipeline and cross-references them, so every real-world alert carries a confidence score derived from the health of the data it came from.

---

## The problem, in one example

Two events in the NYC taxi record data:

| | April 2020 | May 2022 |
|---|---|---|
| What the data shows | Trip volume collapses ~90% | Fare and passenger statistics change for 2019–2021 |
| What actually happened | COVID lockdown — a real event | TLC replaced historical files during a format migration |
| Correct response | Investigate and report | Fix the pipeline, ignore the "anomaly" |

To a conventional detector these look identical. DataGuard separates them.

---

## Architecture

Two layers plus a fusion step:

- **Layer 1 — data health.** Extracts a quality metric vector per partition (row counts, null rates, type conformance, freshness, distribution drift) and fits an Isolation Forest over the metric time-series to catch multivariate degradation no single threshold would flag.
- **Layer 2 — domain anomalies.** Unsupervised detection over trip-level and zone-hour features using an ensemble of Isolation Forest, LOF and DBSCAN, with deterministic rules providing weak labels.
- **Fusion.** Joins the two on partition and time window. Alerts coinciding with quality incidents are quarantined rather than escalated.

See [`diagrams/architecture.drawio`](diagrams/architecture.drawio) and [`docs/architecture.md`](docs/architecture.md).

## Data

NYC Taxi & Limousine Commission trip records, 2019–2025. Yellow and green taxi, with a high-volume FHV slice.

The TLC does not collect this data — it is submitted by third-party TPEP/LPEP technology providers, and the TLC states it makes no representation as to its accuracy. That is precisely why it suits this project.

- Source: https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page
- AWS Open Data: https://registry.opendata.aws/nyc-tlc-trip-records-pds/

See [`docs/data-source.md`](docs/data-source.md) for the documented degradation events used as validation ground truth.

## Stack

Amazon S3 · AWS Glue Data Catalog · Amazon Athena · AWS Step Functions · Python (pandas, scikit-learn, scipy, pyarrow) · Streamlit

## Repository layout

```
docs/          Design documents, planning, risk register, assessment reports
diagrams/      draw.io sources and exported images
pipelines/     ingestion, curation, quality (L1), detection (L2), fusion
models/        Trained model artefacts and evaluation output
dashboard/     Streamlit application
notebooks/     Exploratory analysis
tests/         Unit tests
```

## Unit context

PRT661 Data Science Practice, Charles Darwin University. Theme 3 — Anomaly Detection and Intelligent Monitoring.
