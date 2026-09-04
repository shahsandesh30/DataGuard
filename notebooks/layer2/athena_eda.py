"""
Run the EDA queries in athena_eda_queries.sql against Athena and print/save
the results.
"""

from __future__ import annotations
from pathlib import Path
import sys

PROJECT_ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(PROJECT_ROOT))

from pipelines.detection.io import read_silver_via_athena

# ---- adjust these for your environment ----
# GLUE_DATABASE = "dataguard_db"
SQL_FILE = Path(__file__).parent / "athena_queries.sql"
OUTPUT_DIR = Path(__file__).parent / "eda_results/query_results.txt"
# S3_OUTPUT_PATH = "s3://aws-athena-query-results-025779546330-ap-southeast-2/" 


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

    OUTPUT_DIR.parent.mkdir(exist_ok=True)

    for i, (label, query) in enumerate(queries):
        print(f"--- Running: {label} ---")
        try:
            # df = wr.athena.read_sql_query(
            #     sql=query,
            #     database=GLUE_DATABASE,
            #     s3_output=S3_OUTPUT_PATH,
            #     ctas_approach=False,  # simpler/faster for small EDA result sets
            # )
            df = read_silver_via_athena(sql=query)
        except Exception as e:
            print(f"  FAILED: {e}\n")
            continue

        print(df.head(10).to_string(index=False))
        print(f"  ({len(df)} rows)\n")

        # save each result to CSV for later reference / sharing with teammates
        # safe_name = re.sub(r"[^\w]+", "_", label)[:60].strip("_")
        # out_path = OUTPUT_DIR / f"{safe_name}.csv"
        # df.to_csv(out_path, index=False)
        # print(f"  saved -> {out_path}\n")
        mode = 'w' if i == 0 else 'a'
        with open(OUTPUT_DIR, mode) as f:
            f.write(f"\n--- {label} ---\n")
            f.flush()
            df.to_csv(OUTPUT_DIR, sep='\t', index=False, header=True, mode='a')


if __name__ == "__main__":
    main()