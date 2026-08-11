# DataGuard

**Trust-aware anomaly detection for untrusted data pipelines.**

Anomaly detection systems raise alarms when data looks strange. But data can look strange for two very different reasons: something happened in the world, or something broke in the pipeline. Systems that cannot tell these apart flood their users with false alerts until the alerts get ignored.

DataGuard runs two detection layers over the same pipeline and cross-references them, so every real-world alert carries a trust score derived from the health of the data it came from.

---

## The problem, in one example

A city has five air quality sensors. Three of them stop reporting.

| | What a normal monitor sees | What actually happened |
|---|---|---|
| Average pollution reading | Drops sharply — "air quality improved" | Nothing changed in the air |
| Correct response | — | Fix the pipeline, ignore the "improvement" |

Now compare that with bushfire smoke rolling over the same city: readings spike, and this time the data is healthy and the event is real.

To a conventional detector these look identical — both are large changes in the same measurements. DataGuard separates them: Layer 1 checks whether the data itself is healthy, Layer 2 looks for genuine pollution events, and a fusion component gives every Layer 2 alert a trust score based on Layer 1. Alerts raised while the data was broken are quarantined for review instead of being escalated.

---

## Objectives

1. Build an automated pipeline that collects and stores global air quality measurements.
2. Detect data quality problems (Layer 1).
3. Detect genuine air pollution events using unsupervised methods (Layer 2).
4. Measure how much the trust score reduces false alerts.
5. Deploy a publicly accessible monitoring dashboard.

## Architecture

Two layers plus a fusion step:

- **Layer 1 — data health.** Computes a quality metric vector per station-day (readings received, missing readings, stuck values, negative concentrations, file lateness against OpenAQ's stated 72-hour delivery commitment, schema drift) and fits an Isolation Forest over the metric time-series to catch multivariate degradation no single threshold would flag.
- **Layer 2 — domain anomalies.** Unsupervised detection over station-level and region-level pollution features using an ensemble of Isolation Forest, LOF and DBSCAN, with deterministic rules providing weak labels. Targets genuine events: bushfire smoke, dust storms, industrial incidents.
- **Fusion.** Joins the two on station and time window. Alerts coinciding with quality incidents are quarantined rather than escalated — always retained and displayed for human review, never deleted.

Raw files land unchanged in an S3 **bronze** zone, organised by location and year. A Glue Crawler records the structure of every file, which lets us detect schema changes over time. A **silver** zone holds cleaned data with consistent units and types across all providers. Both detection layers read from silver and write to a **gold** zone, which is queried through Athena and served on a Streamlit dashboard. Step Functions runs the pipeline on a schedule.

See [`diagrams/architecture_openaq.drawio`](diagrams/architecture_openaq.drawio) and [`docs/architecture.md`](docs/architecture.md).

## Data

OpenAQ — global air quality measurements from thousands of sensors, aggregated from governments, research groups and other organisations into a single uniform format.

OpenAQ does not own or operate the sensors; it republishes whatever the underlying sources publish. Sensor-level failures are directly observable in the data (stuck values, negative concentrations, silent dropouts), and platform-level changes are documented and verifiable (the v1/v2 API retirement, the 72-hour file delivery commitment). That is precisely why it suits this project.

- OpenAQ docs: https://docs.openaq.org/
- AWS Open Data registry: https://registry.opendata.aws/openaq/
- Archive bucket: `s3://openaq-data-archive` (public, no credentials required)

See [`docs/data-source.md`](docs/data-source.md) for the documented degradation events used as validation ground truth.

## Stack

Amazon S3 · AWS Glue Data Catalog · Amazon Athena · AWS Step Functions · Python (pandas, scikit-learn, scipy, pyarrow) · Streamlit · DuckDB (local fallback)

## Repository layout

```
docs/          Design documents, planning, risk register, assessment reports
diagrams/      draw.io sources and exported images
pipelines/     ingestion, conformance, quality (L1), detection (L2), fusion
models/        Model cards, evaluation output (trained artefacts gitignored)
dashboard/     Streamlit application
notebooks/     Exploratory analysis
tests/         Unit tests
```

## Getting started

```bash
python -m venv .venv
.venv\Scripts\activate        # Windows
pip install -r requirements.txt
copy .env.example .env         # then fill in your values
pytest
```

## Team

| Member | Role |
|---|---|
| Sandesh Shahi | Project lead — fusion layer, repository governance, integration |
| Aadarsh Ghimire | Data engineer — ingestion, zone design, Glue, orchestration |
| Orchid Shrestha | Data quality engineer — Layer 1 metrics, drift testing, quality model |
| Sandesh Prasad Paudel | Machine learning engineer — Layer 2 features, detector ensemble, evaluation |
| Shuvechchha Pun | Analytics and visualisation — gold tables, Athena queries, dashboard |

## Unit context

PRT661 Data Science Practice, Charles Darwin University. Theme 3 — Operational Anomaly Detection and Intelligent Monitoring.
