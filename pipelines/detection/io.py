"""
S3 / Athena / Glue I/O for the detection layer (Layer 2 — pollution event
detection)

Read silver data in, write gold data (features + weak labels) out as two separate
Parquet datasets, both registered in the Glue Catalog.
"""

from __future__ import annotations

import awswrangler as wr
import pandas as pd

from pipelines.config import Settings, load_settings

# constant prefixes for S3 paths and Glue tables
SILVER_PREFIX = "silver/"              
GOLD_FEATURES_PREFIX = "event_features/"   
GOLD_LABELS_PREFIX = "event_weak_labels/"  

GOLD_FEATURES_TABLE = "event_features"
GOLD_LABELS_TABLE = "event_weak_labels"


def _s3_output(settings: Settings) -> str | None:
    """awswrangler wants either a real S3 path or None — not an empty string."""
    return settings.athena_output or None


def read_silver(
    settings: Settings | None = None,
    prefix: str = SILVER_PREFIX,
) -> pd.DataFrame:
    """Read silver air-quality data from S3 as a single DataFrame.
    Purpose: Feature engineering and weak-label generation for the detection layer (Layer 2).
    """
    settings = settings or load_settings()
    path = f"s3://{settings.silver_bucket}/{prefix}"
    return wr.s3.read_parquet(path=path, dataset=True)


def read_silver_via_athena(
    sql: str,
    settings: Settings | None = None,
) -> pd.DataFrame:
    """Read a subset of silver via an Athena SQL query instead of a full S3
    read — useful for EDA or for pulling a filtered slice (e.g. one station,
    one month) without loading everything into memory.
    """
    settings = settings or load_settings()
    return wr.athena.read_sql_query(
        sql=sql,
        database=settings.glue_database,
        s3_output=_s3_output(settings),
        ctas_approach=False
    )


def write_gold_features(
    features: pd.DataFrame,
    settings: Settings | None = None,
    prefix: str = GOLD_FEATURES_PREFIX,
) -> None:
    """Write the engineered feature table to the gold bucket, partitioned by
    year and parameter, and register/update it in the Glue Catalog so it's
    queryable via Athena immediately.
    """
    settings = settings or load_settings()
    path = f"s3://{settings.gold_bucket}/{prefix}"
    wr.s3.to_parquet(
        df=features,
        path=path,
        dataset=True,
        mode="overwrite_partitions",
        partition_cols=["year", "parameter"],
        database=settings.glue_database,
        table=GOLD_FEATURES_TABLE,
    )


def write_gold_labels(
    labels: pd.DataFrame,
    settings: Settings | None = None,
    prefix: str = GOLD_LABELS_PREFIX,
) -> None:
    """Write the weak-label table to the gold bucket as its own dataset,
    kept separate from the feature table since labels are heuristic
    evaluation signals, not features to train on.

    Expects `labels` to at minimum carry the join keys
    (location_id, parameter, datetime_utc, year) plus the label column,
    so it can be joined back to event_features by anyone downstream.
    """
    settings = settings or load_settings()
    path = f"s3://{settings.gold_bucket}/{prefix}"
    wr.s3.to_parquet(
        df=labels,
        path=path,
        dataset=True,
        mode="overwrite_partitions",
        partition_cols=["locationid", "year"],
        database=settings.glue_database,
        table=GOLD_LABELS_TABLE,
    )