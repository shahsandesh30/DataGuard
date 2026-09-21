# Notebooks

Exploratory work only. Anything that becomes part of the pipeline moves into
`pipelines/` as a tested module.

**Clear all outputs before committing** — notebook diffs are unreadable otherwise.

| Notebook | Purpose |
|---|---|
| `01_bronze_profiling.ipynb` | Bronze profiling — schema, cadence, E3/E4 signals |
| `02_silver_conformance.ipynb` | Validate `build_silver` output — schema and partitions |
| `03_quality_metrics_eda.ipynb` | Layer 1 metrics, rule fire rates, threshold sensitivity |
| `04_detection_features.ipynb` | Layer 2 pollution-event feature exploration, Precision@K |
| `05_fusion_trust.ipynb` | Trust scoring — escalated vs quarantined, severity penalties |
| `06_dashboard_map.ipynb` | Station map status join |
| `data_test.ipynb` | Scratch — inspect Parquet exports and file formats |
| `layer2/` | Layer 2 EDA, owned by the ML engineer |

## Reading the zones

Every reader takes a zone root, and a root may be a local directory or an
`s3://` prefix:

```python
from pipelines.conformance.conform import read_silver
from pipelines.quality.build import read_quality_metrics, read_quality_incidents
from pipelines.fusion.build import read_trust_alerts, read_event_alerts

silver  = read_silver("data/silver")
metrics = read_quality_metrics("data/gold")
alerts  = read_trust_alerts("s3://dataguard-openaq-gold")   # same call, S3
```

The same tables are in the Glue Catalog, so Athena works too — see "Running
against AWS" in the top-level README.

Build the zones first if they are empty:

```bash
python -m pipelines conform
python -m pipelines quality
python -m pipelines detect
python -m pipelines fuse
```

For zone paths, the silver schema, and what each stage produces, see
[`docs/pipeline.md`](../docs/pipeline.md).

Sample locations currently in `data/bronze`: 1544061 (Anzac Memorial), 1601414
(Caringbah), 2455394 (Rozelle), 6430870 (Newport) — all of January 2026.
