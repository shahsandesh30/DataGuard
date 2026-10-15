# Fusion evaluation and independent alert review

Step 4 measures the current fusion policy and supplies a bounded review task.
It does not change either detector or the quarantine rule. A quality incident
on a station-day still holds every coincident alert for review; the alerts
remain in gold and on the dashboard.

## Run the offline evaluation

Use the repository venv and explicit local inputs. The silver argument below
uses the frozen Step 2 dataset; the quality argument uses the measured Step 3
outputs. These local snapshots are gitignored, so a new machine needs those
inputs restored before reproducing this exact run.

```powershell
venv\Scripts\python.exe -m pipelines.fusion.evaluation `
  --bronze-root data/bronze `
  --silver-root data/evaluation/step2-20261015/silver `
  --quality-benchmark-root data/evaluation/step3-20261015-staged `
  --output-root data/evaluation/step4-rerun
```

The output directory must be new or empty and outside the input directories.
S3 inputs are refused. The command copies inputs byte for byte, records their
hashes, rebuilds Layer 1 and Layer 2 independently, runs production fusion,
and creates:

- `inputs/snapshot.json`: file hashes and sizes for the frozen copies.
- `gold/` and `models/`: isolated stage outputs and model history.
- `fusion-evaluation.json`: descriptive results, conditional policy checks,
  source hashes, versions, sampling coverage, and pending human-review metrics.
- `review/review.html`: self-contained evidence page with charts and answer export.
- `review/review-key.json`: the lead's link from case IDs to fusion decisions.
- `review/reviewer-a.json` and `reviewer-b.json`: blank alternatives for editing
  responses directly when a browser is unavailable.

The versioned [measured report](evaluation/step4-fusion-evaluation.json) and
[ready-to-use review page](evaluation/step4-review/review.html) let the team
review the fixed sample without rebuilding models or using AWS.

## What the automated results mean

The real-data section compares all Layer 2 candidates with the escalated and
quarantined subsets, reports trust-score averages and incident-rule overlaps,
and verifies that every candidate survives fusion. Rule overlaps are not
additive: one alert may coincide with several rules. PM2.5 weak labels are
shown as context only; they do not verify events or establish accuracy.

The controlled section feeds actual Step 3 quality incidents into fusion with
constructed candidate alerts. It checks faulty-data candidates, healthy-data
controls, and schema-recovery days. It also assumes a regional event at three
stations with an unrelated quality issue at one station. That case makes the
cost of the policy visible: one candidate is held while two are escalated.
The candidate alerts and event assumption are supplied by the experiment;
these checks do not measure whether Layer 2 would detect those events.
An entirely missing-file day can have quality incidents without any Layer 2
candidate; constructing a candidate for a policy check does not change that.

The detectors refit on the real snapshot in the same way as production. This
run describes operational decisions; Step 2 remains the separate evidence for
temporal holdout behavior and synthetic-spike recall.

## Two-person review: about 45–60 minutes per reviewer

1. The lead shares **only `review.html`** with two teammates. Each opens their
   own fresh copy in a normal browser. Keep `review-key.json`, the report,
   fusion outputs, and the other person's answers away from reviewers until
   both have submitted. The shared page withholds decisions, model scores,
   rule IDs, detector votes, and weak labels; this is a review protocol, not
   a security restriction on someone who has repository access.
2. Review the same 20 PM2.5 cases independently. Sampling is deterministic,
   with up to ten candidates from each fusion status and any shortfall recorded.
   Case order is shuffled. Original units, negative values, and missing readings
   remain visible. Context includes the preceding seven local days and up to
   three stations within 50 km that measured the same parameter that day.
3. For each case select pollution evidence (`supported`, `not_supported`, or
   `uncertain`) and data quality (`issue`, `no_issue`, or `uncertain`). A possible
   event and a data issue can coexist. Add a short note explaining the evidence.
   These are judgments about the supplied pattern, not confirmed event labels.
4. Enter a distinct reviewer name/ID and download the JSON before closing or
   refreshing. Answers stay in memory; they are not sent anywhere or saved in
   the browser. Partial reviews can be downloaded and their missing coverage
   remains visible in the summary. The blank JSON files offer a direct-edit
   alternative; use each case ID and the existing `pack_id` unchanged.
5. The lead collects both JSON files and runs the command below. Different
   packs, duplicate/unknown cases, identical reviewer IDs, invalid labels, and
   completed judgments without evidence notes are rejected.

```powershell
venv\Scripts\python.exe -m pipelines.fusion.review `
  --key docs/evaluation/step4-review/review-key.json `
  --review-a path/to/review-A.json `
  --review-b path/to/review-B.json `
  --output data/evaluation/step4-review-results.json
```

Choose a new output filename for another summary; existing results are not
overwritten. The summary counts only paired complete responses, reports
agreement, lists uncertainty/disagreements, and keeps missing cases in the
coverage totals. It reports consensus-supported events escalated or held,
consensus non-events held, and the data-issue subset, both by status and for
the reviewed sample. A rate with no eligible judgments is unavailable, not zero.

## Claims and the completion boundary

The balanced sample is a small diagnostic set. Its pooled rates are not
population precision, recall, or false-positive estimates. Human consensus is
still not reference-monitor ground truth. Do not relabel uncertain or disputed
cases automatically, or count agreement on `uncertain` as evidence of accuracy.
Discuss those cases only after independent submission and record any later
adjudication separately, preserving both original answers.

The implementation and automated measurements are ready. **Independent human
review remains pending** until two teammates supply their judgments. A claim
that fusion improves real-event accuracy or reduces real false alerts remains
unmeasured until that evidence is collected, and will still need these limits.
