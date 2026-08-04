# Architecture

See `diagrams/architecture.drawio` for the diagram source.

## Zones

| Zone | Bucket | Format | Contents |
|---|---|---|---|
| Bronze | `dataguard-bronze` | Parquet, as published | Raw monthly files, byte-identical to source, never modified |
| Silver | `dataguard-silver` | Parquet, Snappy | Type-conformed, schema unified across 2019–2025 |
| Gold | `dataguard-gold` | Parquet | Scored alerts, quality incidents, fusion output |


## Orchestration

AWS Step Functions on a monthly schedule:

```
fetch → crawl (Glue) → conform → L1 metrics → L1 score
                                → L2 features → L2 score
                                → fusion → publish gold
```
