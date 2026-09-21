# Data source and documented degradation events

## Source

OpenAQ — open, global air quality data aggregated from governments, research
institutions and other organisations into a single uniform format.

- Documentation: https://docs.openaq.org/
- About Open Data on AWS: https://docs.openaq.org/aws/about
- AWS Open Data registry: https://registry.opendata.aws/openaq/
- Archive bucket: `s3://openaq-data-archive` (public, `us-east-1`, no credentials required)

### Archive layout

Daily gzipped CSV files per location:

```
records/csv.gz/locationid=<ID>/year=<YYYY>/month=<MM>/location-<ID>-<YYYYMMDD>.csv.gz
```

The header row, verbatim:

```
"location_id","sensors_id","location","datetime","lat","lon","parameter","units","value"
```

Note **`location_id`, not `locationid`** — and `sensors_id`, not `sensor_id`.
Every one of these is renamed on the way into silver by `COLUMN_ALIASES` in
`pipelines/conformance/conform.py`, which keys on the name lowercased with
underscores stripped so a single entry covers `location_id` / `locationId` /
`LOCATIONID`. A missing entry makes the file fail conformance behind a warning
and contribute zero rows; this went undetected for a long time.

`parameter` values seen so far: `pm25`, `pm1`, `um003`, `temperature`,
`relativehumidity`. `units` arrives as `µg/m³`, `%`, `c`, `particles/cm³`.

OpenAQ states that files are written approximately **72 hours after the end of
day in the location's timezone**. This is a published delivery commitment —
late files can be measured against a stated promise rather than a threshold we
invented.

## Why this source

OpenAQ does not own or operate the sensors. It republishes whatever
governments, research groups and other organisations publish, in one uniform
format. This is a real, untrusted, publicly republished production pipeline —
the exact scenario DataGuard targets. Data degradation is documented and
verifiable at the platform level, and sensor failures are directly observable
in the measurements themselves.

## Validation ground truth

Layer 1 is validated against degradation that genuinely occurs in the source.
These are not injected by the team. Detection of these events without prior
labelling is the project's primary Layer 1 result.

| # | Event | Evidence | Expected Layer 1 signal |
|---|---|---|---|
| E1 | v1 and v2 API endpoints retired 31 January 2025; now return HTTP 410 | Documented by OpenAQ | Platform-level discontinuity for any consumer still on old endpoints |
| E2 | Files delivered later than the stated 72-hour commitment | Measurable against published promise | Freshness anomaly — **rule currently disabled**, see below |
| E3 | Stuck sensor: identical value reported for days or weeks | Directly observable | Zero-variance run; distribution collapse |
| E4 | Negative concentrations | Physically impossible | Validity violation |
| E5 | Sensor stops reporting entirely | Directly observable | Completeness gap |
| E6 | Partial station outage shifting the regional aggregate | Directly observable | Volume anomaly without corresponding event |
| E7 | Unit or type inconsistency between providers | Cross-provider comparison | Conformance violation |
| E8 | Station metadata change (relocation, sensor replacement) | Metadata history | Segment-level distribution drift |

### A caveat on E2

Freshness is measured from the bronze manifest's `arrived_at` — *when we
downloaded* the file — against the 72-hour commitment. On a **backfill** that is
meaningless: ingesting January in September makes every file read as ~240 days
late. Rule R6 therefore fired on 90 of 98 station-days and dragged 160 of 164
alerts into quarantine, so it has been removed from
`pipelines/quality/rules.py`.

The `file_lateness_hours` metric is still computed and still feeds the Layer 1
Isolation Forest. Only the rule is off. Restore it once the pipeline runs on a
schedule near the present, where the measurement means what it claims.

## Layer 2 target events

Genuine pollution events appear as large changes in the same measurements:
bushfire smoke, dust storms, industrial incidents. Rule-based weak labels
(e.g. sustained multi-station PM2.5 elevation) plus Precision@K with
two-member independent review, since no ground-truth event list exists.

## Synthetic injection

Synthetic corruption is used **only** to produce precision/recall curves at
varying severity on held-out partitions. It is never the primary evidence.
Injection types: reading deletion, null flooding, stuck-value insertion, unit
flipping, distribution shift, partition removal, timestamp skew.
