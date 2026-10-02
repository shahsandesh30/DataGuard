# Dashboard

Streamlit application serving the silver and gold zones locally or from S3.

```bash
python -m pipelines conform   # the station map needs the silver zone
streamlit run dashboard/app.py
```

Sidebar: silver/gold roots, as-of date filter, KPI strip. A root may be a
local directory or an `s3://` prefix, so the dashboard can serve the AWS lake
directly; it defaults to whatever `.env` sets.

For Streamlit Community Cloud, copy the values from
`.streamlit/secrets.toml.example` into the app's managed secrets and set
`DATAGUARD_PUBLIC_DASHBOARD=true`. Public mode hides root editing, S3 reads are
cached for five minutes, and the Refresh data button invalidates that cache.
The page shows the newest station day and build timestamp and warns when the
last successful build is more than 36 hours old. Missing credentials or data
produce a friendly error without displaying secret values.

Use a dedicated IAM principal that can list the bucket and read only
`silver/*` and `gold/*`; see `docs/aws-deployment.md`.

Views:

- **Trust-scored alerts** — Layer 2 events with fusion trust score;
  quarantined alerts are always visible, never hidden.
- **Data health** — Layer 1 station-day quality metrics and incidents.
- **Station map** — color-coded OpenAQ stations (escalated / quarantined /
  quality_only / monitored) from silver coordinates joined to gold status.

`dashboard/data.py` holds the loading and join logic and is unit tested;
`app.py` is layout only.
