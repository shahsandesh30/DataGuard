import io
import json
from datetime import UTC, date, datetime
from pathlib import Path

import boto3
import pytest
from botocore.exceptions import ClientError

from pipelines import storage
from pipelines.ingestion.fetch import (
    archive_key,
    bronze_key,
    download_archive_object,
    fetch_location_day,
    fetch_range,
    parse_bronze_filename,
)


def test_archive_key_matches_openaq_layout():
    key = archive_key(2178, date(2023, 1, 5))
    assert key == (
        "records/csv.gz/locationid=2178/year=2023/month=01/location-2178-20230105.csv.gz"
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


class _FakeBucket:
    """Minimal stand-in for one S3 bucket, shared by the wrangler and boto3 fakes."""

    def __init__(self):
        self.objects: dict[str, bytes] = {}

    # --- awswrangler surface -------------------------------------------------
    def upload(self, local_file, path):
        self.objects[_key(path)] = Path(local_file).read_bytes()

    def describe_objects(self, paths):
        key = _key(paths[0])
        if key not in self.objects:
            raise ClientError({"Error": {"Code": "404"}}, "HeadObject")
        return {
            paths[0]: {
                "ContentLength": len(self.objects[key]),
                "LastModified": datetime(2026, 1, 1, tzinfo=UTC),
            }
        }

    def does_object_exist(self, path):
        return _key(path) in self.objects

    # --- boto3 client surface ------------------------------------------------
    def put_object(self, Bucket, Key, Body):
        self.objects[Key] = Body

    def get_object(self, Bucket, Key):
        return {"Body": io.BytesIO(self.objects[Key])}


def _key(uri: str) -> str:
    return storage.normalize(uri).split("/", 3)[3]


def _use_fake_s3(monkeypatch) -> _FakeBucket:
    bucket = _FakeBucket()
    monkeypatch.setattr(storage, "_wr", lambda: type("wr", (), {"s3": bucket}))
    monkeypatch.setattr(boto3, "client", lambda service, **kwargs: bucket)
    return bucket


BRONZE_S3 = "s3://dataguard-openaq-bronze"


def test_fetch_location_day_lands_in_an_s3_bronze_zone(tmp_path, monkeypatch):
    """The OpenAQ download is staged locally, then handed to the zone."""
    bucket = _use_fake_s3(monkeypatch)

    def fake_download(_bucket, _key, dest: Path) -> int:
        dest.parent.mkdir(parents=True, exist_ok=True)
        dest.write_bytes(b"archive-bytes")
        return dest.stat().st_size

    result = fetch_location_day(
        2178, date(2023, 1, 1), bronze_root=BRONZE_S3, downloader=fake_download
    )

    assert result.status == "copied"
    assert result.local_path == (
        "s3://dataguard-openaq-bronze/locationid=2178/year=2023/location-2178-20230101.csv.gz"
    )
    assert bucket.objects["locationid=2178/year=2023/location-2178-20230101.csv.gz"] == (
        b"archive-bytes"
    )
    # Nothing may be left behind in the system temp directory.
    assert not list(tmp_path.rglob("*.csv.gz"))


def test_second_fetch_skips_an_object_already_in_the_zone(monkeypatch):
    bucket = _use_fake_s3(monkeypatch)
    bucket.objects["locationid=2178/year=2023/location-2178-20230101.csv.gz"] = b"already here"

    def explode(*_args):
        raise AssertionError("must not re-download an object already in bronze")

    result = fetch_location_day(2178, date(2023, 1, 1), bronze_root=BRONZE_S3, downloader=explode)
    assert result.status == "skipped"
    assert result.bytes == len(b"already here")


def test_missing_archive_object_is_recorded_in_the_s3_manifest(monkeypatch):
    """A gap in the source is a completeness signal, so it must be written down."""
    bucket = _use_fake_s3(monkeypatch)

    def missing(_bucket, key, _dest):
        raise FileNotFoundError(key)

    results = fetch_range(
        [2178],
        date(2023, 1, 12),
        date(2023, 1, 13),
        bronze_root=BRONZE_S3,
        downloader=missing,
    )

    assert {r.status for r in results} == {"missing"}
    manifest = bucket.objects["_manifest.jsonl"].decode("utf-8").strip().splitlines()
    assert len(manifest) == 2, "each attempt appends one line, not a rewrite of one"
    assert {json.loads(line)["status"] for line in manifest} == {"missing"}
    assert json.loads(manifest[0])["day"] == "2023-01-12"


@pytest.mark.parametrize("backend", ["local", "s3"])
@pytest.mark.parametrize("error_code,first_status", [("404", "missing"), ("SlowDown", "error")])
def test_retry_after_source_failure_delivers_original_bytes_then_skips(
    tmp_path,
    monkeypatch,
    backend,
    error_code,
    first_status,
):
    """Exercise the real ClientError handling and manifest across three scheduled runs."""
    bucket = _use_fake_s3(monkeypatch) if backend == "s3" else None
    root = BRONZE_S3 if bucket else str(tmp_path / "bronze")
    content = b"unchanged OpenAQ archive bytes"
    calls = []

    class PublicArchive:
        def download_file(self, bucket_name, key, destination):
            calls.append((bucket_name, key))
            if len(calls) == 1:
                raise ClientError({"Error": {"Code": error_code}}, "GetObject")
            Path(destination).write_bytes(content)

    client = PublicArchive()

    def download(bucket_name, key, destination):
        return download_archive_object(bucket_name, key, destination, client=client)

    def run():
        return fetch_range(
            [2178],
            date(2023, 1, 12),
            date(2023, 1, 12),
            bronze_root=root,
            downloader=download,
        )[0]

    failed = run()
    assert failed.status == first_status
    assert not storage.exists(storage.join(root, bronze_key(2178, date(2023, 1, 12))))
    copied = run()
    assert copied.status == "copied"
    assert copied.bytes == len(content)
    assert storage.read_bytes(copied.local_path) == content
    skipped = run()
    assert skipped.status == "skipped"
    assert skipped.bytes == len(content)
    assert len(calls) == 2, "An existing delivery must not trigger a third download"
    records = [
        json.loads(line)
        for line in storage.read_text(storage.join(root, "_manifest.jsonl")).splitlines()
    ]
    assert [row["status"] for row in records] == [first_status, "copied"]
    assert all(row["day"] == "2023-01-12" and row["locationid"] == 2178 for row in records)
