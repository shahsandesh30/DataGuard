# Notebooks

Exploratory work only. Anything that becomes part of the pipeline moves into
`pipelines/` as a tested module.

Clear all outputs before committing — notebook diffs are unreadable otherwise.

## Data zone layouts

| Zone | Path | Format |
|---|---|---|
| Bronze | `data/bronze/locationid=<ID>/year=<YYYY>/location-<ID>-<YYYYMMDD>.csv.gz` | gzip CSV (OpenAQ archive columns) |
| Silver | `data/silver/locationid=<ID>/year=<YYYY>/<timestamp>_qnxhe_<uuid>` | Parquet export (8 columns) |
| Gold | `data/gold/layer1/` (quality metrics, incidents), `data/gold/layer2/` (event features, alerts) | Layer 1 + Layer 2 parquet tables |

Silver columns: `sensor_id`, `location`, `datetime`, `latitude`, `longitude`, `parameter`, `unit`, `value`.

```bash
python -m pipelines conform
python -m pipelines quality
python -m pipelines detect
```

## Notebooks

| Notebook | Purpose |
|---|---|
| `data_review.ipynb` | Del Norte (2178) archive exploration |
| `01_bronze_profiling.ipynb` | Bronze profiling — schema, cadence, E3/E4 signals |
| `02_silver_conformance.ipynb` | Validate `build_silver` output — schema and partitions |
| `03_quality_metrics_eda.ipynb` | Layer 1 metrics, rule fire rates, threshold sensitivity |
| `04_layer2_features.ipynb` | Station and region level pollution-event feature exploration |
| `data_test.ipynb` | Inspect Parquet exports and file formats |

Working sample locations in `data/bronze`: 1544061 (Anzac Memorial), 1601414 (Caringbah), 2455394 (Rozelle), 6430870 (Newport).

## Planned

| Notebook | Purpose |
|---|---|
| `02_provider_reconciliation.ipynb` | Compare units, types and reporting cadence across providers |
