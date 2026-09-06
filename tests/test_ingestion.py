from datetime import date
from pathlib import Path

from pipelines.ingestion.fetch import (
    archive_key,
    adopt_flat_bronze,
    bronze_key,
    bronze_path,
    fetch_location_day,
    fetch_range,
    parse_bronze_filename,
)


def test_archive_key_matches_openaq_layout():
    key = archive_key(2178, date(2023, 1, 5))
    assert key == (
        "records/csv.gz/locationid=2178/year=2023/month=01/"
        "location-2178-20230105.csv.gz"
    )


def test_parse_bronze_filename():
    assert parse_bronze_filename("location-2178-20230105.csv.gz") == (
        2178,
        date(2023, 1, 5),
    )
    assert parse_bronze_filename("notes.txt") is None


def test_bronze_key_local_layout():
    key = bronze_key(2178, date(2023, 1, 5))
    assert key == "locationid=2178/year=2023/location-2178-20230105.csv.gz"


def test_adopt_flat_bronze_moves_into_archive_layout(tmp_path: Path):
    src = tmp_path / "location-2178-20230101.csv.gz"
    src.write_bytes(b"gzip-bytes")
    results = adopt_flat_bronze(tmp_path)
    dest = bronze_path(tmp_path, 2178, date(2023, 1, 1))
    assert dest.exists()
    assert dest.read_bytes() == b"gzip-bytes"
    assert not src.exists()
    assert results[0].status == "adopted"
    assert (tmp_path / "_manifest.jsonl").exists()


def test_fetch_location_day_copies_and_skips(tmp_path: Path):
    def fake_download(bucket: str, key: str, dest: Path) -> int:
        dest.parent.mkdir(parents=True, exist_ok=True)
        dest.write_bytes(b"archive")
        return dest.stat().st_size

    copied = fetch_location_day(
        2178, date(2023, 1, 1), bronze_root=tmp_path, downloader=fake_download
    )
    assert copied.status == "copied"
    assert Path(copied.local_path).read_bytes() == b"archive"

    skipped = fetch_location_day(
        2178, date(2023, 1, 1), bronze_root=tmp_path, downloader=fake_download
    )
    assert skipped.status == "skipped"


def test_fetch_range_records_missing_as_completeness_gap(tmp_path: Path):
    def missing(_bucket: str, key: str, _dest: Path) -> int:
        raise FileNotFoundError(key)

    results = fetch_range(
        [2178],
        date(2023, 1, 12),
        date(2023, 1, 14),
        bronze_root=tmp_path,
        downloader=missing,
    )
    assert len(results) == 3
    assert {item.status for item in results} == {"missing"}
    assert all(item.local_path is None for item in results)
