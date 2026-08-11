"""Fetch daily OpenAQ archive files for a set of locations into the bronze zone.

Bronze files are byte-identical to the source and never modified. The archive
layout is:

    records/csv.gz/locationid=<ID>/year=<YYYY>/month=<MM>/location-<ID>-<YYYYMMDD>.csv.gz
"""

from datetime import date


def archive_key(location_id: int, day: date) -> str:
    """Return the OpenAQ archive object key for one location-day."""
    return (
        f"records/csv.gz/locationid={location_id}"
        f"/year={day.year}/month={day.month:02d}"
        f"/location-{location_id}-{day.strftime('%Y%m%d')}.csv.gz"
    )


def fetch_location_day(location_id: int, day: date) -> None:
    """Copy one location-day file from the OpenAQ archive to the bronze bucket.

    TODO(data engineer): boto3 copy from s3://openaq-data-archive (public,
    unsigned requests) to the bronze bucket, preserving the key. Record the
    arrival time so Layer 1 can measure lateness against the 72-hour
    delivery commitment. Missing objects are a completeness signal, not an
    error — log and continue.
    """
    raise NotImplementedError


def fetch_range(location_ids: list[int], start: date, end: date) -> None:
    """Fetch every location-day in [start, end] for the given locations."""
    raise NotImplementedError
