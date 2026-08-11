# Notebooks

Exploratory work only. Anything that becomes part of the pipeline moves into
`pipelines/` as a tested module.

Clear all outputs before committing — notebook diffs are unreadable otherwise.

## Planned

| Notebook | Purpose |
|---|---|
| `01_source_profiling.ipynb` | First look at a handful of OpenAQ locations; confirm archive layout, cadence, and self-evident failures (stuck values, negatives) by direct inspection |
| `02_provider_reconciliation.ipynb` | Compare units, types and reporting cadence across providers; document every difference |
| `03_quality_metrics_eda.ipynb` | Station-day metric vector distributions; baseline selection for Layer 1 |
| `04_layer2_features.ipynb` | Station and region level pollution-event feature exploration |
