"""
S3 / Athena / Glue I/O for the detection layer (Layer 2 — pollution event
detection)

Read silver data in, write gold data (features + weak labels) out as two separate
Parquet datasets, both registered in the Glue Catalog.
"""

from __future__ import annotations

import re
from pathlib import Path

import pandas as pd

from pipelines.config import Settings, load_settings

# constant prefixes for S3 paths and Glue tables
SILVER_PREFIX = "silver"      
SILVER_DERIVED_PREFIX = "derived"    
SILVER_WEAK_LABELS_PREFIX = "weak_labels"  

DERIVED_FEATURES_TABLE = "event_features"
WEAK_LABELS_TABLE = "event_weak_labels"


def _s3_output(settings: Settings) -> str | None:
    """awswrangler wants either a real S3 path or None — not an empty string."""
    return settings.athena_output or None


def read_silver(
    settings: Settings | None = None,
    *,
    source: str = "aws",
    silver_root: Path | None = None,
    sql: str = "SELECT * FROM silver_data",
) -> pd.DataFrame:
    """Read silver data from local Parquet or Athena.

    Local reads recover ``locationid`` from the Hive-style partition path and
    normalise the export schema to the columns expected by feature engineering.
    """
    settings = settings or load_settings()
    if source == "local":
        root = Path(silver_root or settings.silver_root)
        files = [
            path for path in sorted(root.rglob("*.parquet"))
            if not path.name.startswith("_")
        ]
        if not files:
            return pd.DataFrame()
        frames = []
        for path in files:
            frame = pd.read_parquet(path)
            match = re.search(r"(?:^|/)locationid=(\d+)(?:/|$)", path.as_posix())
            if "locationid" not in frame.columns and match:
                frame["locationid"] = int(match.group(1))
            frames.append(frame)
        return _normalise_silver(pd.concat(frames, ignore_index=True))

    if source != "aws":
        raise ValueError("source must be either 'local' or 'aws'")
    return _normalise_silver(_athena_query(sql, settings))


def read_silver_via_athena(
    sql: str,
    settings: Settings | None = None,
) -> pd.DataFrame:
    """Read a subset of silver via an Athena SQL query instead of a full S3
    read — useful for EDA or for pulling a filtered slice (e.g. one station,
    one month) without loading everything into memory.
    """
    settings = settings or load_settings()
    return _athena_query(sql, settings)


def _athena_query(sql: str, settings: Settings) -> pd.DataFrame:
    try:
        import awswrangler as wr
    except ImportError as exc:
        raise RuntimeError(
            "AWS reads require awswrangler; install requirements.txt or use source='local'."
        ) from exc
    return wr.athena.read_sql_query(
        sql=sql,
        database=settings.glue_database,
        s3_output=_s3_output(settings),
        ctas_approach=False,
    )


def _normalise_silver(frame: pd.DataFrame) -> pd.DataFrame:
    """Make local and Athena silver column names interchangeable."""
    rename = {
        "latitude": "latitude",
        "longitude": "longitude",
        "lat": "latitude",
        "lon": "longitude",
        "location_name": "location",
    }
    frame = frame.rename(columns={k: v for k, v in rename.items() if k in frame.columns})
    required = {"locationid", "datetime", "parameter", "value"}
    missing = required - set(frame.columns)
    if missing:
        raise ValueError(f"Silver data is missing required columns: {sorted(missing)}")
    frame["datetime"] = pd.to_datetime(frame["datetime"], utc=True, errors="coerce")
    frame["parameter"] = frame["parameter"].astype("string").str.lower()
    return frame.dropna(subset=["locationid", "datetime", "parameter", "value"])


def write_derived_features(
    features: pd.DataFrame,
    settings: Settings | None = None,
    prefix: str = SILVER_DERIVED_PREFIX,
) -> None:
    """Write the engineered feature table to the silver bucket, derived folder
    partitioned by year and parameter
    """
    settings = settings or load_settings()
    path = f"s3://{settings.silver_bucket}/{prefix}/"
    try:
        import awswrangler as wr
    except ImportError as exc:
        raise RuntimeError(
            "AWS writes require awswrangler; install requirements.txt or use local output."
        ) from exc
    partition_cols = [column for column in ("locationid", "parameter") if column in features]
    wr.s3.to_parquet(
        df=features,
        path=path,
        dataset=True,
        mode="overwrite_partitions",
        partition_cols=partition_cols,
        database=settings.glue_database,
        table=DERIVED_FEATURES_TABLE,
    )


def write_local_dataset(
    frame: pd.DataFrame,
    root: Path,
    table_name: str,
    *,
    partition_cols: tuple[str, ...] = ("locationid",),
) -> Path:
    """Write a deterministic, queryable local Parquet dataset."""
    output = Path(root) / table_name
    output.mkdir(parents=True, exist_ok=True)
    for existing in output.rglob("*.parquet"):
        existing.unlink()
    if frame.empty:
        frame.to_parquet(output / "_empty.parquet", index=False)
        return output
    for keys, part in frame.groupby(list(partition_cols), dropna=False, sort=False):
        if not isinstance(keys, tuple):
            keys = (keys,)
        partition = output
        for column, value in zip(partition_cols, keys, strict=True):
            partition /= f"{column}={value}"
        partition.mkdir(parents=True, exist_ok=True)
        part.to_parquet(partition / "part-0.parquet", index=False, compression="snappy")
    return output


def read_local_dataset(root: Path, table_name: str) -> pd.DataFrame:
    """Read a local dataset written by :func:`write_local_dataset`."""
    files = sorted((Path(root) / table_name).rglob("*.parquet"))
    files = [path for path in files if not path.name.startswith("_")]
    if not files:
        return pd.DataFrame()
    return pd.concat((pd.read_parquet(path) for path in files), ignore_index=True)


def write_scored_results(
    scored: pd.DataFrame,
    settings: Settings | None = None,
    *,
    source: str = "local",
    output_root: Path | None = None,
    prefix: str = "detection_scores",
) -> Path | None:
    """Persist scores locally or as an S3 Glue dataset."""
    settings = settings or load_settings()
    if source == "local":
        return write_local_dataset(scored, output_root or settings.gold_root, prefix)
    if source != "aws":
        raise ValueError("source must be either 'local' or 'aws'")
    try:
        import awswrangler as wr
    except ImportError as exc:
        raise RuntimeError("AWS writes require awswrangler.") from exc
    wr.s3.to_parquet(
        df=scored,
        path=f"s3://{settings.gold_bucket}/{prefix}/",
        dataset=True,
        mode="overwrite_partitions",
        partition_cols=[c for c in ("locationid", "year") if c in scored.columns],
        database=settings.glue_database,
        table=prefix,
    )
    return None

def read_derived_features(
    settings: Settings | None = None
) -> pd.DataFrame:
    """Read derived features from the Glue database as a single DataFrame.
    Purpose: Fitting detection models.
    """
    settings = settings or load_settings()
    print(f"Reading derived features from Glue database: {settings.glue_database}")
    return _athena_query("SELECT * FROM event_features", settings)

# def write_gold_labels(
#     labels: pd.DataFrame,
#     settings: Settings | None = None,
#     prefix: str = GOLD_LABELS_PREFIX,
# ) -> None:
#     """Write the weak-label table to the gold bucket as its own dataset,
#     kept separate from the feature table since labels are heuristic
#     evaluation signals, not features to train on.

#     Expects `labels` to at minimum carry the join keys
#     (locationid, parameter, datetime, year) plus the label column,
#     so it can be joined back to event_features by anyone downstream.
#     """
#     settings = settings or load_settings()
#     path = f"s3://{settings.gold_bucket}/{prefix}"
#     wr.s3.to_parquet(
#         df=labels,
#         path=path,
#         dataset=True,
#         mode="overwrite_partitions",
#         partition_cols=["locationid", "year"],
#         database=settings.glue_database,
#         table=WEAK_LABELS_TABLE,
#     )