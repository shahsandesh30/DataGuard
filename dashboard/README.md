# Dashboard

Streamlit application serving the gold zone via Athena, with a local DuckDB
fallback (risk R2). Deployed publicly (Streamlit Community Cloud) as the
project's public-URL deliverable.

```bash
streamlit run dashboard/app.py
```

Views:

- **Trust-scored alerts** — Layer 2 events with their fusion trust score;
  quarantined alerts are always visible, never hidden.
- **Data health** — Layer 1 station-day quality metrics and incidents.
- **Station map** — monitored OpenAQ stations and active alerts.
