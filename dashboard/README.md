# Dashboard

Streamlit application serving the gold zone. Deployed publicly later as the
project's public-URL deliverable; DuckDB serving is the fallback if AWS credits
run out (risk R2).

```bash
python -m pipelines conform   # the station map needs the silver zone
streamlit run dashboard/app.py
```

Sidebar: silver/gold roots, as-of date filter, KPI strip. A root may be a
local directory or an `s3://` prefix, so the dashboard can serve the AWS lake
directly; it defaults to whatever `.env` sets.

Views:

- **Trust-scored alerts** — Layer 2 events with fusion trust score;
  quarantined alerts are always visible, never hidden.
- **Data health** — Layer 1 station-day quality metrics and incidents.
- **Station map** — color-coded OpenAQ stations (escalated / quarantined /
  quality_only / monitored) from silver coordinates joined to gold status.

`dashboard/data.py` holds the loading and join logic and is unit tested;
`app.py` is layout only.
