# Assessment 4 technical progress

## Step 1: reliable baseline (15 October 2026)

The sprint starts from commit `a93bc52` on branch `sandesh_shahi`, after the
Layer 2 evaluation and model-persistence changes merged from main.

The pre-change Windows check recorded 116 passing tests and one failing
`test_local_text_round_trip`. Ruff and the Glue ZIP import check passed.

Existing local work was present in the README, deployment documentation,
Glue runner, conformance timestamp handling, AWS configuration/upload scripts,
and their tests. Those edits remain in the working tree. The starting status,
binary Git diff, and copies of those tracked files were captured locally under
`build/assessment4-step1/baseline/` before Step 1 edits. Generated artifacts and
existing untracked files are excluded from the Step 1 commit.

### Changes

- Restore universal newline handling in `storage.read_text` on both backends.
  UTF-8 text reads use LF consistently; `read_bytes` preserves original bytes
  for model artifacts. Regression cases cover LF, CRLF, and CR with string and
  Path inputs, including non-ASCII text.
- Set the generated Glue job timeout to 60 minutes. This changes the renderer;
  an existing AWS job is unchanged until its configuration is updated.
- Add an offline integration check that passes rendered roots, model storage,
  station defaults, and the Glue database through the scheduled entry point
  into the pipeline CLI. Existing policy tests check model-storage access and
  the read-only dashboard boundary.

### Verification

- Full combined working tree: **124 tests passed**.
- Exact staged source, excluding unrelated local edits: **123 tests passed**.
  The one-test difference is the existing local daylight-saving regression
  test, which is preserved outside this commit.
- Ruff passed for `pipelines dashboard tests glue scripts` in both source trees.
- The staged Glue ZIP imported the pipeline, storage, model store, and offline
  benchmark successfully from outside the repository.
- Existing LOF fixture and joblib/NumPy deprecation warnings remain; neither
  test run had failures.

JUnit results are saved locally as
`build/assessment4-step1/working-tree-tests.xml` and
`build/assessment4-step1/staged-source-tests.xml`.

All checks use the repository venv and explicit local bronze, silver, gold,
and model roots. S3 behaviour uses fake clients; no AWS pipeline is started.
Step 1 did not change evaluation methodology; Step 2 below addresses that work.

## Step 2: corrected Layer 2 evaluation (15 October 2026)

Step 2 starts from `12761ec`. The existing local silver was frozen into
`data/evaluation/step2-20261015/silver/`: 14 byte-identical Parquet files. The
snapshot manifest is `data/evaluation/step2-20261015/snapshot.json`; its file
manifest SHA-256 is
`bdc57bf64a4e439a2ea4ff6247d31c31f4d7549e3f5a5d8a7916113bb4b7d71d`.
Raw inputs and working outputs stay outside version control under `data/`.
A small versioned copy of the measured results is kept in
[`evaluation/step2-benchmark.json`](evaluation/step2-benchmark.json).

### Methodology changes

- Split raw silver by local date before building training features. Freeze
  hourly median/MAD baselines, eligible station/parameter pairs, and neighbour
  geography to training readings. Fit the scaler and ensemble on training
  feature rows. Held-out rolling and daily trailing features retain earlier
  observations without contributing future readings to the fitted reference.
- Require seven prior observed days and finite injection-hour readings. Mark
  only station-days whose values actually changed. Record history/window
  exclusions, sample shortfalls, unscored cases, and local/regional denominators.
- Rename the unlabelled comparison population's metric to
  `untouched_alert_rate`. It is not a measured false-positive rate. Synthetic
  recall still refits on perturbed history and includes the daily top-K cap.
- Record all weak-label metrics as unavailable for pm1 and um003. Preserve
  numeric nullable Parquet columns for local/S3 compatibility. Mask misleading
  zeros in older histories on read without rewriting stored data.
- Keep legacy benchmark reports readable, map their former metric name, and
  show a dashboard rerun warning. Mark historical model-card results as
  superseded for holdout/injection interpretation.
- Add methodology version 2, a logical dataset fingerprint, coverage,
  configuration, split dates, skipped parameters, and runtime library versions
  to the benchmark JSON. Source hashes and a patch are captured beside the
  snapshot in `source-provenance.json` and `source.patch`.

### Reproduce the fixed-data run

Use the repository venv and the frozen local silver copy:

```powershell
$env:BRONZE_ROOT = "data/bronze"
$env:SILVER_ROOT = "data/evaluation/step2-20261015/silver"
$env:GOLD_ROOT = "data/evaluation/step2-20261015/gold"
$env:MODELS_ROOT = "build/assessment4-step2/test-models"
venv\Scripts\python.exe -m pipelines evaluate `
  --silver-root data/evaluation/step2-20261015/silver `
  --gold-root data/evaluation/step2-20261015/gold
```

The output is
`data/evaluation/step2-20261015/gold/layer2/_detection_benchmark.json`.
This isolated run leaves operational silver, gold, and model artifacts intact.

### Results and verification

The frozen input contains **38,449 readings at 14 locations**, covering local
dates 1 January to 1 February 2026, and produces **995 feature rows**. Each
parameter uses 24 training dates ending 24 January and eight held-out dates
starting 25 January. No parameter was skipped.

- **PM2.5:** 45/279 training rows flagged (16.1%), compared with 29/74 held-out
  rows (39.2%). Of those held-out flags, 27/29 match the weak label (93.1%);
  its population base rate is 47/74 (63.5%), giving **lift 1.47**. These are
  heuristic agreements, not verified event accuracy.
- **Other parameters:** held-out flag rates are 30/66 for pm1 (45.5%) and
  26/66 for um003 (39.4%). Weak-label metrics are unavailable for both.
- **Synthetic PM2.5 spikes:** **5/27 detected (18.5%)**, including 2/6 local
  cases (33.3%) and 3/21 regional cases (14.3%). All 27 injected cases reached
  scoring; there were no requested-sample shortfalls. Eligibility excluded
  92 station-days without seven prior observed days and 15 without usable
  injection-hour readings.
- **Untouched population:** 55/326 rows became alerts (16.9%). They have no
  verified normal labels, so this is not a false-positive rate.

The corrected experiment still shows limited synthetic-event recall. This
step establishes more reliable measurement; it does not improve or establish
the detector's real-event accuracy. Independent alert review and fusion
evaluation remain later steps. The January experiment is not directly
comparable with the older August dataset in the model card.

Validation: **140 tests passed** in the combined working tree and **139 passed**
in the isolated staged source. The one-test difference is the preserved local
daylight-saving test. Ruff and both Glue ZIP import checks passed. Existing
LOF and joblib/NumPy warnings remain without test failures. Local JUnit logs
are `build/assessment4-step2/tests.xml` and
`build/assessment4-step2/staged-source-tests.xml`.

## Step 3: missing days and Layer 1 fault benchmark (15 October 2026)

Step 3 starts from `a839085` on `sandesh_shahi`. The existing nine tracked
local edits were captured under `build/assessment4-step3/baseline/` before
implementation. Unrelated local work remains outside this step's commit.

### Missing-day fix

Layer 1 now builds station-days from silver observations **and bronze manifest
attempts**. Previously, a completely missing daily file could disappear from
the metric table before R7 ran. Manifest-only rows now retain zero readings,
missing rate 1.0, and file checks, even when silver is entirely empty.
Repeated attempts produce one row; a successful retry recovers when the file
and rebuilt silver arrive. The last observed sensor roster survives consecutive
missing days. No unrecorded calendar gaps are invented. Negative/stuck readings
remain in silver, and R6 remains disabled.

### Reproduce the controlled run

```powershell
venv\Scripts\python.exe -m pipelines.quality.benchmark `
  --output-root data/evaluation/step3-quality-rerun
```

The output root must be a new, empty local directory; the benchmark refuses
S3 and existing files. Each scenario gets its own bronze, silver, gold, and
model roots independently of operational `.env` settings. No live archive
download or AWS write is needed. The generated `quality-benchmark.json` records
input/source hashes, versions, expected/observed rules, misses, and controls.
The versioned result is
[`evaluation/step3-quality-benchmark.json`](evaluation/step3-quality-benchmark.json).

### Results

The baseline has three synthetic stations, 21 local days, two PM2.5 sensors
per station, and hourly readings: **3,024 readings and 63 station-days**.
Six separate copies each receive one fault type at three interior station-days.

- Frozen sensor: **3/3**, both R2 and R3.
- Negative readings: **3/3**, R1.
- Sensor dropout: **3/3**, R5.
- Completely missing daily file: **3/3**, R4, R5, and R7.
- Schema drift: **3/3**, R8.
- Incorrect unit label: **3/3**, R10.

All **18/18 cases** reached scoring and all **27/27 expected rule checks**
fired. There were no misses. The healthy baseline had no incidents. There
were **three additional control incidents in 360 control station-day
occurrences (0.83%)**: R8 when the original schema returned on the following
day. These are recorded rather than removed from the denominator.

Isolation Forest trained in every scenario. Its raw flags covered 15/18
fault targets, with no control flags; all 15 were already caught by rules,
so it added zero published incidents. These models refit on the perturbed
data and do not provide a held-out accuracy estimate.

This passes the narrow synthetic-fault gate in the risk register. Regular
controls, three repetitions, and one parameter/region do not establish
real-world accuracy or false-positive rates. Step 4 remains fusion evaluation
and independent review of real alerts.

### Verification

- Full combined working tree: **151 tests passed**.
- Exact staged source: **150 tests passed**. The difference is the preserved
  local daylight-saving regression test, excluded from this commit.
- Ruff passed for `pipelines dashboard tests glue scripts` in both trees.
- Both Glue source ZIPs imported the pipeline, storage, model store, and new
  quality benchmark from outside the repository.
- The exact staged source reproduced every benchmark result. The versioned
  report is copied from
  `data/evaluation/step3-20261015-staged/quality-benchmark.json`; every normalized
  source hash matches the Git index. Initial working-tree results are saved
  separately under `data/evaluation/step3-20261015/`.
- Regressions cover consecutive missing days, no silver, duplicate/invalid
  manifest keys, successful retries, string/Path roots, fake S3, preserved
  negative values/unit labels, unscored misses, and schema-reversion controls.
- Eight unrelated tracked files remain byte-identical to the starting copies.
  The README's original local edits are also preserved outside the commit.

JUnit logs are `build/assessment4-step3/tests.xml` and
`build/assessment4-step3/staged-source-tests.xml`. Existing LOF duplicate-fixture
and joblib/NumPy deprecation warnings remain without failures. Operational
data and AWS resources were not changed.
