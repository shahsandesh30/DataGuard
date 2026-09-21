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

**Status: not yet answered.** Both layers build end to end and both models
train, but no seeded-failure test exists, so the gate question has not actually
been put to Layer 1. Writing that test is the top open task — see "Next steps"
in the top-level README.
