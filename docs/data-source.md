# Data source and documented degradation events

## Source

NYC Taxi & Limousine Commission trip record data.

- Landing page: https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page
- AWS Open Data registry: https://registry.opendata.aws/nyc-tlc-trip-records-pds/
- Trip Record User Guide: https://www.nyc.gov/assets/tlc/downloads/pdf/trip_record_user_guide.pdf
- Working with Parquet: https://www.nyc.gov/assets/tlc/downloads/pdf/working_parquet_format.pdf

### URL pattern


| Service | Prefix | First published |
|---|---|---|
| Yellow taxi | `yellow_tripdata` | 2009-01 |
| Green taxi | `green_tripdata` | 2013-08 |
| For-hire vehicle | `fhv_tripdata` | 2015-01 |
| High-volume FHV | `fhvhv_tripdata` | 2019-02 |

### Supporting files

- Taxi zone lookup: https://d37ci6vzurychx.cloudfront.net/misc/taxi_zone_lookup.csv
- Taxi zone shapefile: https://d37ci6vzurychx.cloudfront.net/misc/taxi_zones.zip

## Why this source

The TLC does not collect this data. It is submitted by third-party technology
providers authorised under the TPEP/LPEP programs, and the TLC states plainly
that it makes no representation as to its accuracy. This is a real, untrusted,
publicly republished production pipeline — the exact scenario DataGuard targets.

## Validation ground truth

Layer 1 is validated against degradation events that genuinely occurred. These
are not injected by the team. Detection of these events without prior labelling
is the project's primary Layer 1 result.

| # | Event | Period | Expected Layer 1 signal |
|---|---|---|---|
| E1 | COVID-19 demand collapse | 2020-03 to 2020-04 | Volume anomaly (~90% drop) |
| E2 | CSV to Parquet migration, historical files replaced | 2022-05 | Format/schema discontinuity across back years |
| E3 | Inconsistent Parquet column types between monthly files | ongoing | Type conformance violation |
| E4 | HVFHV driver pay / passenger fare columns backfilled to 2019-02 | 2022 | Schema addition + retroactive backfill |
| E5 | `cbd_congestion_fee` column added (congestion pricing) | 2025-01 | Schema addition |
| E6 | Publication lag, variable by month (~2 months) | ongoing | Freshness anomaly |
| E7 | Vendor reporting differences (CMT vs VeriFone) | ongoing | Segment-level distribution drift |
| E8 | TLC published errata: FHV files re-issued 2017; `improvement_surcharge` added to already-published 2015 files | 2015, 2017 | Silent republication (checksum change on unchanged partition) |
| E9 | FHV file absent for a month where sibling services published | observed 2026-05 | Completeness gap |

E8 is drawn from the TLC's own published Errata section — an official log of
retrospective corrections to already-released data. E9 is a live gap in the
current source.

## Synthetic injection

Synthetic corruption is used **only** to produce precision/recall curves at
varying severity on held-out partitions. It is never the primary evidence.
Injection types: row deletion, null flooding, type flipping, distribution shift,
partition removal, timestamp skew.
