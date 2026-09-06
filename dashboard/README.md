# Dashboard

Streamlit application serving the gold zone from local parquet (Athena /
DuckDB serving deferred — risk R2). Deployed publicly later as the project's
public-URL deliverable.

```bash
streamlit run dashboard/app.py
```

Sidebar: bronze/gold roots, as-of date filter, KPI strip.

Views:

- **Trust-scored alerts** — Layer 2 events with fusion trust score;
  quarantined alerts are always visible, never hidden.
- **Data health** — Layer 1 station-day quality metrics and incidents.
- **Station map** — color-coded OpenAQ stations (escalated / quarantined /
  quality_only / monitored) from conformed lat/lon + gold status.
