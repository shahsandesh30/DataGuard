# Assessment 3 feedback evidence

This is an offline description of the existing frozen January snapshot, not a
new detector experiment. No models are fitted, production settings changed,
AWS requests made, or human labels created.

## Reproduce

Use the repository venv and the existing local evaluation bundle:

```powershell
.\venv\Scripts\python.exe docs/evaluation/assessment3-feedback/analyse_snapshot.py `
  --snapshot data/evaluation/step4-20261015 `
  --output docs/evaluation/assessment3-feedback
```

The bundle is gitignored. A fresh clone needs a copy from the team's retained
evidence handover; it does not obtain the data from these aggregate files.
The script checks that all 562 files in the complete snapshot remain unchanged.
These include generated gold/model/review artifacts as well as the 454 original
input files checked by the earlier technical verifier.

- `analysis.json`: input fingerprints, method, runtime versions, results and
  three deterministically selected worked examples.
- `penalty-sensitivity.csv`: 12 parameter/scenario comparisons.
- `rankings.csv`: all 175 candidate rankings in each of four scenarios.
- `detector-votes.csv`: detector coverage and one/two/three-vote counts.

## What was compared

All 175 retained candidates are preserved: PM1 56, PM2.5 60, particle number
(`um003`) 59. The status rule remains incident presence, giving 110 escalated
and 65 quarantined under every penalty scenario. Alternative low/medium/high
penalties are none (0/0/0), mild (0.10/0.30/0.50), current
(0.20/0.40/0.70), and stronger (0.30/0.50/0.90).

Rank within each parameter over the entire frozen date window by descending
scenario trust, breaking ties by station ID, local date and parameter.
Mean absolute rank movement and top-ten overlap compare with the current saved
trust ranking. This investigation ranking is separate from the pipeline's
region/day top-ten cap. The CSV also includes overlap with anomaly-only ranking.

Mild and stronger penalties retain all ten current leading candidates for each
parameter. Removing penalties retains 10/10 for PM1, 9/10 for PM2.5 and 9/10
for particle number. PM2.5 has the largest mean absolute movement: 9.87 places
with no penalty, 1.97 with mild penalties, and 0.70 with stronger penalties.
This measures ranking sensitivity, not better prioritisation against true events.

The retained candidates contain 133 one-vote, 32 two-vote and ten three-vote
alerts. IF votes on 99, LOF on 100, and DBSCAN on 28. Coverage overlaps and
must not be summed as separate events. Without unselected rows, new fits and
reference labels this cannot establish individual model accuracy, recall,
ensemble superiority or a benefit from requiring two votes.

The quality context contains 56 high-severity and nine medium-severity
quarantined candidates, with no low-severity candidates. The low-severity
penalty's effect therefore remains unevaluated. All penalty alternatives are
illustrative; the current settings are not empirically calibrated.

## Provenance and boundaries

The producing pipeline source is the frozen release recorded in
[`../step4-fusion-evaluation.json`](../step4-fusion-evaluation.json), with manifest
SHA-256 `b4e6dfce4b445f01c5746b5004b57a125e137bd6c22621ee9e9cb60ec4debf2a`.
The operational 4,153-alert S3 result and historical 253-alert local result
are different populations and are not used in this calculation.

To run the strict historical `scripts.verify_snapshot` check, use an isolated
checkout of producing release `d131715` with explicit local roots and the
frozen bundle's absolute path. The verifier intentionally rejects changed
pipeline source, including the later daylight-saving correction. Its
`--code-revision` argument records a label; it does not bypass that check.

Worked examples are selected for explanation, not expert adjudication: the
highest saved PM2.5 anomaly score within each status, followed by the earliest
manifest-recorded missing day and lowest eligible station ID. A missing day
without an event candidate has no fabricated trust score or fusion row.

Independent expert review, public dashboard hosting, the final cloud-package
alignment and verified daily end-to-end cloud execution remain outside this work.
