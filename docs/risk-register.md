# Risk register

L = likelihood, I = impact. Reviewed at every weekly checkpoint.

| # | Risk | L | I | Mitigation | Owner |
|---|---|---|---|---|---|
| R1 | Schema reconciliation across 2019–2025 exceeds allocated time | M | H | Five working day timebox; fall back to 2022–2025 post-migration data | Data engineer |
| R2 | AWS Academy credits exhausted | M | H | Mandatory partition filtering on every query; weekly spend review; DuckDB/Streamlit Cloud serving fallback | Project lead |
| R3 | No ground truth for Layer 2 evaluation | H | M | Deterministic weak labels; Precision@K; two-member manual review protocol; declared as a limitation | ML engineer |
| R4 | Concept not understood by assessors | M | H | Worked example (April 2020 vs May 2022) precedes architecture in every document and the presentation | Project lead |
| R5 | Integration deferred to final weeks | M | H | Friday integration checkpoint; no branch unmerged beyond five days | All |
| R6 | Scope expansion beyond team capacity | H | H | Week 5 go/no-go gate; week 8 reserved as buffer with no planned work | Project lead |
| R7 | Uneven understanding across the team before individual assessment | M | M | Weekly 30-minute session where each member explains their component | All |
| R8 | Layer 2 anomalies prove trivial (meter errors only) | M | M | Escalate aggregation level to zone-hour and vendor segment | ML engineer |

## Week 5 go/no-go gate

**Test:** does Layer 1 detect E1 (COVID collapse) and E2 (2022 migration)
without being told they exist?

**If no:** reduce Layer 2 to deterministic rules only and ship a complete
single-layer system. A finished one-layer project outperforms two half-built
layers.
