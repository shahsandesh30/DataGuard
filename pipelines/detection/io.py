"""Optional Athena helper for the Layer 2 EDA script.

The scheduled pipeline does not use Athena for reads or writes; it uses
``pipelines.storage``. This small helper remains only for
``notebooks/layer2/athena_eda.py``, where arbitrary SQL is the point.
"""

from __future__ import annotations

import os

import awswrangler as wr
import pandas as pd

from pipelines.config import load_settings


def read_silver_via_athena(
    sql: str,
    *,
    database: str | None = None,
    s3_output: str | None = None,
) -> pd.DataFrame:
    """Run an ad-hoc Athena query for EDA, outside the scheduled pipeline."""
    return wr.athena.read_sql_query(
        sql=sql,
        database=database or load_settings().glue_database,
        s3_output=s3_output or os.getenv("ATHENA_OUTPUT") or None,
        ctas_approach=False,
    )
