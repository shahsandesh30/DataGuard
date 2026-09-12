import re
import datetime

import boto3
from botocore import UNSIGNED
from botocore.config import Config

SOURCE_BUCKET = "openaq-data-archive"

# Untested candidates from the 37-location bbox search (already-confirmed
# working ones and your current 10 solid stations are excluded)
CANDIDATES = [
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

PERIOD_DAYS = 30
FRESHNESS_BUFFER_DAYS = 7

s3 = boto3.client("s3", config=Config(signature_version=UNSIGNED))
DATE_IN_FILENAME = re.compile(r"(\d{8})\.csv\.gz$")


def compute_date_window():
    today = datetime.date.today()
    latest_usable = today - datetime.timedelta(days=FRESHNESS_BUFFER_DAYS)
    start = latest_usable - datetime.timedelta(days=PERIOD_DAYS - 1)
    return start, latest_usable


def months_spanned(start, end):
    months = []
    cur = start.replace(day=1)
    while cur <= end:
        months.append((cur.year, cur.month))
        cur = (cur.replace(year=cur.year + 1, month=1) if cur.month == 12
               else cur.replace(month=cur.month + 1))
    return months


def count_files_in_window(locationid, start, end):
    total = 0
    for year, month in months_spanned(start, end):
        prefix = f"records/csv.gz/locationid={locationid}/year={year}/month={month:02d}/"
        paginator = s3.get_paginator("list_objects_v2")
        for page in paginator.paginate(Bucket=SOURCE_BUCKET, Prefix=prefix):
            for obj in page.get("Contents", []):
                match = DATE_IN_FILENAME.search(obj["Key"])
                if match:
                    file_date = datetime.datetime.strptime(match.group(1), "%Y%m%d").date()
                    if start <= file_date <= end:
                        total += 1
    return total


def main():
    start, end = compute_date_window()
    print(f"Checking real archive data for window {start} -> {end}\n")

    results = []
    for locationid, name in CANDIDATES:
        count = count_files_in_window(locationid, start, end)
        results.append((locationid, name, count))
        status = "GOOD" if count >= 20 else ("PARTIAL" if count > 0 else "EMPTY")
        print(f"  {locationid:>8}  {name:<20} {count:>3} files in window  [{status}]")

    print("\nUse GOOD candidates first, PARTIAL only if you need to, skip EMPTY entirely.")


if __name__ == "__main__":
    main()