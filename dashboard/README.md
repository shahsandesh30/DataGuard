# Dashboard

Streamlit application serving the silver and gold zones locally or from S3.

The dashboard reads existing silver and gold outputs. Check the configured
roots before launching; it defaults to whatever `.env` sets.

```powershell
venv\Scripts\python.exe -m streamlit run dashboard/app.py
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

- **Trust-scored alerts** — Layer 2 events with fusion trust score. The
  **Explain one alert** view selects a station, local date, and pollutant within
  the sidebar's date scope; quarantined alerts remain selectable. Station
  history and aggregate trends remain below it.
- **Data health** — Layer 1 station-day quality metrics and incidents.
- **Station map** — color-coded OpenAQ stations (escalated / quarantined /
  quality_only / monitored) from silver coordinates joined to gold status.
- **Models** — which models the last run trained or skipped (and their run id),
  Layer 2 evaluation metrics for the latest run and their trend across runs
  (`gold/layer2/model_runs`), and the offline benchmark written by
  `python -m pipelines evaluate`. Everything comes from gold, so the read-only
  dashboard role needs no access to `models/`; model files are never loaded.
  Weak-label metrics apply to PM2.5 only. Legacy benchmark reports remain
  readable with a rerun warning; untouched-data alert rate is not labelled as
  a measured false-alarm rate.

## Explain one alert

The view brings together the saved decision, original anomaly score,
highest-severity penalty, stored trust score, IF/LOF/DBSCAN votes, recorded
feature values, and every Layer 1 incident for the station-day. Incidents
apply across pollutants. Penalties do not stack; the displayed calculation
is marked verified only when it matches the saved score under the current
policy. Scores are ranking signals, not event probabilities. Feature values
provide context, not causal attribution or feature importance.

The chart uses the selected station, local day, and pollutant's conformed
silver measurements. Sensors have separate series and different units have
separate charts. Negative and constant readings remain visible. The exact
readings expander retains UTC/local timestamps and original units; lines
connect observations and do not establish that gaps were fully observed.

**Download this alert's evidence** exports a read-only JSON record containing
the selected key, decision, votes, features, incidents, measurements, available
build times, and warnings. It excludes model paths and weak labels. No models
are loaded, no scores are recomputed by a detector, and no pipeline or review
records are written. Silver is loaded once into the existing five-minute
cache and reused by the station map and explanation.

Missing evidence and disagreements with saved fusion metadata produce
warnings. A newer detector build than fusion also produces a warning.
Matching timestamps are a consistency check, not proof that independently
configured silver and gold roots belong to the same run; use coherent outputs.
Human review is shown as **Not recorded in this dataset** because gold has
no human-review field. Quarantine means held for review, not human rejection.
The independent Step 4 review remains separate: reviewers should use only
the blind evidence page until both submit their judgments; see the
[review guide](../docs/fusion-review.md).

For the frozen Step 4 demonstration, after that local evaluation has been
generated, launch from the repository root with explicit local roots:

```powershell
$env:SILVER_ROOT = "data/evaluation/step4-20261015/inputs/silver"
$env:GOLD_ROOT = "data/evaluation/step4-20261015/gold"
venv\Scripts\python.exe -m streamlit run dashboard/app.py
```

Choose **Show all dates in tables**, then select an alert. The sidebar otherwise
defaults to the newest station-day, which may contain quality findings without
fusion alerts. This is an expected empty selection.

`data.py` holds loading and joins, `explanation.py` assembles bounded evidence,
and `alert_view.py` renders the selection and explanation; `app.py` composes
the tabs. Unit tests cover key matching, penalty calculations, missing and
conflicting evidence, JSON export, and read-only behavior. Streamlit AppTest
exercises actual widget reruns and dependent selection resets without a server.
The [saved-data check](../docs/evaluation/step5-dashboard-check.json) verifies
all 175 Step 4 alerts and two native UI cases. It measures evidence consistency,
not detector accuracy; browser layout was not visually verified.
