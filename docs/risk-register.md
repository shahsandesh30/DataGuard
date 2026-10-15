# Risk register

L = likelihood, I = impact. Reviewed at every weekly checkpoint.

These R-numbers are **project risks**. They are unrelated to the Layer 1
quality rule ids (also R1, R2, ...) in `pipelines/quality/rules.py`.

| # | Risk | L | I | Mitigation | Owner |
|---|---|---|---|---|---|
| R1 | Reconciling thousands of providers takes longer than planned | M | H | Five working day timebox; narrow to a single region if exceeded | Data engineer |
| R2 | AWS Academy credits exhausted | M | H | Mandatory partition filtering on every query; weekly spend review; local DuckDB fallback for serving | Project lead |
| R3 | Fragmented individual AWS accounts impede integration | M | H | Infrastructure as reproducible scripts; one nominated integration account | Data engineer |
| R4 | No ground truth for Layer 2 evaluation | H | M | Rule-based weak labels; Precision@K; independent review by two members; declared as a limitation | ML engineer |
| R5 | Concept not understood by stakeholders | M | H | Worked example (broken sensors vs bushfire smoke) precedes architecture in all communication | Project lead |
| R6 | Integration deferred to final weeks | H | H | Weekly integration checkpoint; no branch unmerged beyond five days | All |
| R7 | Scope expansion beyond team capacity | H | H | Mid-project go/no-go gate; final week reserved as buffer with no planned work | Project lead |

## Mid-project go/no-go gate

**Test:** does Layer 1 detect self-evident sensor failures (stuck values,
negative concentrations, dropouts) without being told they exist?

**If no:** reduce Layer 2 to deterministic rules only and ship a complete
single-layer system. A finished one-layer project outperforms two half-built
layers.

**Status: controlled-fault gate passed, 15 October 2026.** Layer 1 detected
18/18 synthetic cases covering frozen sensors, negative readings, sensor
dropout, missing daily files, schema drift, and incorrect unit labels. All
27 expected rule checks fired. Three additional R8 incidents occurred on
untouched days when schemas reverted; the healthy baseline had no incidents.
See the [recorded evidence](evaluation/step3-quality-benchmark.json) and
[methodology](../models/layer1_model_card.md#controlled-fault-benchmark-15-october-2026).

This answers the narrow constructed-fault question. Three repetitions per
fault and regular hourly controls do not establish real-world accuracy or a
false-positive rate. Independent real-alert review and fusion evaluation
remain open before making broader claims about reliability.

Step 4 (15 October 2026) measures the unchanged fusion policy on a fresh
isolated build: 175 candidates remain visible, 110 escalated and 65 quarantined.
All 42 conditional policy checks pass, including the explicit risk of holding
one station's candidate during an assumed three-station event. The
[20-case review pack and instructions](fusion-review.md) are ready for two
independent teammates (about 45–60 minutes each). Project risk R4 remains open:
human judgments are pending, and quarantine counts alone do not establish
false-alert reduction or genuine-event retention.
