"""
Run the EDA queries in athena_eda_queries.sql against Athena and print/save
the results.

Requires:
    pip install awswrangler boto3

Requires AWS credentials configured locally (aws configure / SSO login) with
permission to run Athena queries against the Glue database, and an Athena
query result S3 bucket/location set up (ask your data engineer teammate if
unsure — most AWS accounts have a default one, or your team may have a
dedicated "athena-query-results" bucket).
"""

from __future__ import annotations

import re
from pathlib import Path

import awswrangler as wr

# ---- adjust these for your environment ----
GLUE_DATABASE = "dataguard_db"
SQL_FILE = Path(__file__).parent / "athena_queries.sql"
OUTPUT_DIR = Path(__file__).parent / "eda_results"
S3_OUTPUT_PATH = "s3://aws-athena-query-results-025779546330-ap-southeast-2/" 


def split_sql_statements(sql_text: str) -> list[tuple[str, str]]:
    # Split the .sql file into (label, query) pairs.
    lines = sql_text.splitlines()
    blocks: list[str] = []
    current: list[str] = []
    labels: list[str] = []
    for line in lines:
        if line.strip().startswith("-- "):
            label = line.strip().removeprefix("-- ").strip()
            labels.append(label)
            continue
        current.append(line)
        if line.strip().endswith(";"):
            blocks.append("\n".join(current))
            current = []
    if current and any(l.strip() for l in current):
        blocks.append("\n".join(current))

    labeled = []
    for i, block in enumerate(blocks, start=1):
        if labels[i-1] and block:
            labeled.append((labels[i-1], block.strip()))
    return labeled


def main() -> None:
    sql_text = SQL_FILE.read_text()
    queries = split_sql_statements(sql_text)
    print(f"Found {len(queries)} queries in {SQL_FILE.name}\n")
    print(queries[0][0])  # print first query for sanity check

    OUTPUT_DIR.mkdir(exist_ok=True)

    for label, query in queries[:1]:
        print(f"--- Running: {label} ---")
        try:
            df = wr.athena.read_sql_query(
                sql=query,
                database=GLUE_DATABASE,
                s3_output=S3_OUTPUT_PATH,
                ctas_approach=False,  # simpler/faster for small EDA result sets
            )
        except Exception as e:
            print(f"  FAILED: {e}\n")
            continue

        print(df.head(20).to_string(index=False))
        print(f"  ({len(df)} rows)\n")

        # save each result to CSV for later reference / sharing with teammates
        # safe_name = re.sub(r"[^\w]+", "_", label)[:60].strip("_")
        # out_path = OUTPUT_DIR / f"{safe_name}.csv"
        # df.to_csv(out_path, index=False)
        # print(f"  saved -> {out_path}\n")


if __name__ == "__main__":
    main()