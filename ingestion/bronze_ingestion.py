

import re
import datetime
from pathlib import Path

import boto3
from botocore import UNSIGNED
from botocore.config import Config

# ---------------------------------------------------------------------------
# CONFIG
# ---------------------------------------------------------------------------

SOURCE_BUCKET = "openaq-data-archive"

SYDNEY_LOCATIONS = [
    (1544061, "Anzac Memorial"),
    (1601414, "Caringbah NSW"),
    (1707188, "North Ryde"),
    (2392564, "Sydney, Australia"),
    (2455393, "Rozelle"),
    (2455394, "Rozelle"),
    (2904356, "Luna Lewisham"),
    (3772130, "Ingalara Ave"),
    (6146402, "West Hoxton"),
    (6209161, "Albert Parade"),
    (3229203, "Ryde"),
    (3358634, "Knapsack"),
    (4719604, "Kurrajong Hills"),
    (6430870, "Newport NSW"),
]

EXPLICIT_START = datetime.date(2026, 1, 1)   # earliest confirmed available
FRESHNESS_BUFFER_DAYS = 7                     # end = today minus this

# Anchored to the script's own location, not the terminal's current
# directory - avoids "Read-only file system" errors if you run this
# from somewhere else (like /).
OUTPUT_FOLDER = Path(__file__).resolve().parent / "bronze"

# ---------------------------------------------------------------------------
# Date window
# ---------------------------------------------------------------------------

def compute_date_window():
    today = datetime.date.today()
    end = today - datetime.timedelta(days=FRESHNESS_BUFFER_DAYS)
    return EXPLICIT_START, end


def months_spanned(start, end):
    months = []
    cur = start.replace(day=1)
    while cur <= end:
        months.append((cur.year, cur.month))
        cur = (cur.replace(year=cur.year + 1, month=1) if cur.month == 12
               else cur.replace(month=cur.month + 1))
    return months

# ---------------------------------------------------------------------------
# S3 (anonymous read of the public archive)
# ---------------------------------------------------------------------------

s3 = boto3.client("s3", config=Config(signature_version=UNSIGNED))
DATE_IN_FILENAME = re.compile(r"(\d{8})\.csv\.gz$")


def list_source_files(locationid, year, month):
    prefix = f"records/csv.gz/locationid={locationid}/year={year}/month={month:02d}/"
    paginator = s3.get_paginator("list_objects_v2")
    keys = []
    for page in paginator.paginate(Bucket=SOURCE_BUCKET, Prefix=prefix):
        for obj in page.get("Contents", []):
            keys.append(obj["Key"])
    return keys


def file_date_in_window(key, start, end):
    match = DATE_IN_FILENAME.search(key)
    if not match:
        return False
    file_date = datetime.datetime.strptime(match.group(1), "%Y%m%d").date()
    return start <= file_date <= end


def download_one(key, locationid):
    year = key.split("year=")[1].split("/")[0]
    filename = key.rsplit("/", 1)[-1]

    # NOTE: month is deliberately dropped here - Bronze only partitions
    # by locationid/year, matching what's already correctly in S3.
    local_dir = OUTPUT_FOLDER / f"locationid={locationid}" / f"year={year}"
    local_path = local_dir / filename

    if local_path.exists():
        return "skipped"

    body = s3.get_object(Bucket=SOURCE_BUCKET, Key=key)["Body"].read()
    local_dir.mkdir(parents=True, exist_ok=True)
    local_path.write_bytes(body)
    return "downloaded"


def process_location(locationid, name, start, end):
    downloaded = skipped = 0
    empty_months = []

    for year, month in months_spanned(start, end):
        keys = list_source_files(locationid, year, month)
        if not keys:
            empty_months.append(f"{year}-{month:02d}")
            continue
        for key in keys:
            if not file_date_in_window(key, start, end):
                continue
            result = download_one(key, locationid)
            if result == "downloaded":
                downloaded += 1
            else:
                skipped += 1

    note = f" (no data: {empty_months})" if empty_months else ""
    print(f"[{locationid:>8}] {name:<20} downloaded={downloaded:<3} skipped={skipped:<3}{note}")
    return downloaded, skipped


def main():
    start, end = compute_date_window()
    print(f"Window: {start} -> {end}")
    print(f"Saving to: {OUTPUT_FOLDER.resolve()}")
    print("-" * 70)

    total_downloaded = total_skipped = 0
    for locationid, name in SYDNEY_LOCATIONS:
        d, s = process_location(locationid, name, start, end)
        total_downloaded += d
        total_skipped += s

    print("-" * 70)
    print(f"Done. Downloaded: {total_downloaded}   Already had: {total_skipped}")
    print(f"\nNext step - upload to S3 manually:")
    print(f"  aws s3 sync {OUTPUT_FOLDER}/ s3://amzn-s3-bucket-dataguard/bronze/")


if __name__ == "__main__":
    main()