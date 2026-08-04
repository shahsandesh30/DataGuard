# Notebooks

Exploratory work only. Anything that becomes part of the pipeline moves into
`pipelines/` as a tested module.

Clear all outputs before committing — notebook diffs are unreadable otherwise.

## Planned

| Notebook | Purpose |
|---|---|
| `01_source_profiling.ipynb` | First look at a single month; confirm the Parquet type inconsistency by direct inspection |
| `02_schema_evolution.ipynb` | Compare schemas across 2019–2025; document every difference |
| `03_quality_metrics_eda.ipynb` | Metric vector distributions; baseline selection |
| `04_layer2_features.ipynb` | Trip and zone-hour feature exploration |
