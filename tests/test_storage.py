"""Backend dispatch: one code path over local directories or S3.

The S3 branch is exercised against a fake awswrangler rather than a real
bucket, so these run in CI with no credentials.
"""

from datetime import UTC, datetime
from pathlib import Path

import pandas as pd
import pytest
from botocore.exceptions import ClientError

from pipelines import storage


@pytest.mark.parametrize(
    ("given", "expected"),
    [
        ("s3://bucket/silver", "s3://bucket/silver"),
        ("s3://bucket/silver/", "s3://bucket/silver"),
        # pipelines/detection wraps roots in Path(), which collapses the double
        # slash and flips separators on Windows. Both forms must survive.
        ("s3:/bucket/silver", "s3://bucket/silver"),
        ("s3:\\bucket\\silver", "s3://bucket/silver"),
        ("S3://bucket/silver", "s3://bucket/silver"),
        ("data/silver", "data/silver"),
    ],
)
def test_normalize_repairs_mangled_uris(given, expected):
    assert storage.normalize(given) == expected


def test_is_s3_distinguishes_backends():
    assert storage.is_s3("s3://bucket/gold")
    assert storage.is_s3("s3:/bucket/gold")  # survived a Path() round trip
    assert not storage.is_s3("data/gold")
    assert not storage.is_s3("C:/lake/gold")


def test_join_uses_forward_slashes_on_s3():
    assert storage.join("s3://bucket/gold", "layer1/quality_metrics") == (
        "s3://bucket/gold/layer1/quality_metrics"
    )
    assert storage.join("s3://bucket/gold/", "fusion", "trust_alerts") == (
        "s3://bucket/gold/fusion/trust_alerts"
    )
    assert storage.join("s3://bucket/gold", "") == "s3://bucket/gold"


def test_join_local_root_stays_a_local_path(tmp_path):
    joined = storage.join(tmp_path, "layer1/quality_metrics")
    assert not storage.is_s3(joined)
    assert joined.endswith("quality_metrics")


def test_local_text_round_trip(tmp_path):
    target = storage.join(tmp_path, "nested/_manifest.jsonl")
    assert storage.read_text(target) is None

    storage.append_text(target, '{"a": 1}\n')
    storage.append_text(target, '{"a": 2}\n')
    assert storage.read_text(target) == '{"a": 1}\n{"a": 2}\n'

    storage.write_text(target, "replaced\n")
    assert storage.read_text(target) == "replaced\n"


def test_local_file_info_reports_size(tmp_path):
    target = storage.join(tmp_path, "note.txt")
    assert storage.file_info(target) is None
    storage.write_text(target, "12345")
    size, modified = storage.file_info(target)
    assert size == 5
    assert modified.startswith("20")


def _client_error(code: str) -> ClientError:
    return ClientError({"Error": {"Code": code, "Message": code}}, "HeadObject")


@pytest.mark.parametrize("code", ["404", "NoSuchKey", "NotFound"])
def test_s3_file_info_returns_none_when_absent(monkeypatch, code):
    """Absent must be None, not an exception.

    fetch_location_day asks this about every location-day before downloading
    it, so a key that is not there yet is the normal case. Raising here broke
    ingestion against S3 on the very first new file.
    """

    class FakeS3:
        @staticmethod
        def describe_objects(paths):
            raise _client_error(code)

    monkeypatch.setattr(storage, "_wr", lambda: type("wr", (), {"s3": FakeS3}))
    assert storage.file_info("s3://bucket/bronze/missing.csv.gz") is None


def test_s3_file_info_reraises_permission_errors(monkeypatch):
    """A 403 means the credentials are wrong, not that the key is missing."""

    class FakeS3:
        @staticmethod
        def describe_objects(paths):
            raise _client_error("403")

    monkeypatch.setattr(storage, "_wr", lambda: type("wr", (), {"s3": FakeS3}))
    with pytest.raises(ClientError):
        storage.file_info("s3://bucket/bronze/forbidden.csv.gz")


def test_s3_file_info_reports_size_and_modified(monkeypatch):
    class FakeS3:
        @staticmethod
        def describe_objects(paths):
            return {
                paths[0]: {
                    "ContentLength": 721345,
                    "LastModified": datetime(2026, 9, 1, 3, 46, 50, tzinfo=UTC),
                }
            }

    monkeypatch.setattr(storage, "_wr", lambda: type("wr", (), {"s3": FakeS3}))
    size, modified = storage.file_info("s3://bucket/bronze/there.csv")
    assert size == 721345
    assert modified.startswith("2026-09-01T03:46:50")


def test_put_file_moves_into_a_local_zone(tmp_path):
    staging = tmp_path / "staging" / "location-2178-20230101.csv.gz"
    staging.parent.mkdir(parents=True)
    staging.write_bytes(b"archive-bytes")

    destination = storage.join(tmp_path, "bronze/locationid=2178/year=2023/f.csv.gz")
    assert storage.put_file(staging, destination) == 13

    assert Path(destination).read_bytes() == b"archive-bytes"
    assert not staging.exists(), "the staging copy must not be left behind"


def test_put_file_uploads_to_s3_and_clears_staging(tmp_path, monkeypatch):
    """Ingestion stages every download locally, then hands it to the zone."""
    staging = tmp_path / "location-2178-20230101.csv.gz"
    staging.write_bytes(b"archive-bytes")
    captured = {}

    class FakeS3:
        @staticmethod
        def upload(local_file, path):
            captured["local_file"] = local_file
            captured["path"] = path

    monkeypatch.setattr(storage, "_wr", lambda: type("wr", (), {"s3": FakeS3}))
    target = "s3://dataguard-openaq-bronze/locationid=2178/year=2023/location-2178-20230101.csv.gz"

    assert storage.put_file(staging, target) == 13
    assert captured["local_file"] == str(staging)
    assert captured["path"] == target
    assert not staging.exists(), "the staging copy must not be left behind"


def test_put_file_repairs_a_path_mangled_s3_target(tmp_path, monkeypatch):
    staging = tmp_path / "f.csv.gz"
    staging.write_bytes(b"x")
    captured = {}

    class FakeS3:
        @staticmethod
        def upload(local_file, path):
            captured["path"] = path

    monkeypatch.setattr(storage, "_wr", lambda: type("wr", (), {"s3": FakeS3}))
    storage.put_file(staging, "s3:/bucket/bronze/f.csv.gz")
    assert captured["path"] == "s3://bucket/bronze/f.csv.gz"


def _rows() -> pd.DataFrame:
    return pd.DataFrame(
        [
            {"locationid": 1, "date_local": "2026-01-08", "value": 1.0},
            {"locationid": 2, "date_local": "2025-12-31", "value": 3.0},
        ]
    )


def test_local_parquet_keeps_locationid_in_the_file(tmp_path):
    """Layer 2 reads gold with a plain rglob, so it cannot recover partitions."""
    storage.write_parquet(_rows(), tmp_path, "layer1/metrics")

    part = tmp_path / "layer1" / "metrics" / "locationid=1" / "year=2026" / "part-0.parquet"
    direct = pd.read_parquet(part)
    assert "locationid" in direct.columns
    assert "year" not in direct.columns


def test_read_parquet_schema_matches_across_backends(tmp_path, monkeypatch):
    """A frame written and read back must look the same on either backend."""
    storage.write_parquet(_rows(), tmp_path, "layer1/metrics")
    local = storage.read_parquet(tmp_path, "layer1/metrics")

    # awswrangler restores partition columns as strings; emulate that shape.
    s3_shaped = _rows().assign(year=["2026", "2025"], locationid=["1", "2"])

    class FakeS3:
        @staticmethod
        def list_objects(prefix, suffix=None):
            return [prefix + "part-0.parquet"]

        @staticmethod
        def read_parquet(prefix, dataset=False, path_suffix=None):
            assert path_suffix == ".parquet", "sidecars such as _build.json must be skipped"
            return s3_shaped.copy()

    monkeypatch.setattr(storage, "_wr", lambda: type("wr", (), {"s3": FakeS3}))
    from_s3 = storage.read_parquet("s3://bucket/gold", "layer1/metrics")

    assert list(local.columns) == list(from_s3.columns)
    assert local["locationid"].tolist() == from_s3["locationid"].tolist()
    assert str(local["locationid"].dtype) == str(from_s3["locationid"].dtype) == "Int64"
    assert "year" not in from_s3.columns


def test_s3_writes_register_a_glue_table(tmp_path, monkeypatch):
    captured = {}

    class FakeS3:
        @staticmethod
        def to_parquet(**kwargs):
            captured.update(kwargs)

    class FakeCatalog:
        @staticmethod
        def databases(limit=None):
            return pd.DataFrame({"Database": ["dataguard_db"]})

        @staticmethod
        def create_database(name, exist_ok=False):
            captured["created_database"] = name

    monkeypatch.setattr(
        storage, "_wr", lambda: type("wr", (), {"s3": FakeS3, "catalog": FakeCatalog})
    )
    target = storage.write_parquet(_rows(), "s3://bucket/gold", "layer1/quality_metrics")

    assert target == "s3://bucket/gold/layer1/quality_metrics"
    assert captured["path"] == "s3://bucket/gold/layer1/quality_metrics/"
    # Named from the S3 key, not the dataset argument, so the zone prefix is kept.
    assert captured["table"] == "gold_layer1_quality_metrics"
    assert captured["partition_cols"] == ["locationid", "year"]
    assert captured["mode"] == "overwrite"
    assert captured["dataset"] is True


def test_glue_table_name_keeps_the_zone_prefix():
    """Layer 2 gold must not collide with the event_features table detection owns."""
    assert storage._glue_table_name("s3://bucket/layer2/event_features") == (
        "layer2_event_features"
    )
    assert storage._glue_table_name("s3://bucket/layer1/quality_metrics") == (
        "layer1_quality_metrics"
    )
    # Characters Glue will not accept in a table name are folded to underscores.
    assert storage._glue_table_name("s3://my-bucket") == "my_bucket"


def test_s3_write_uses_explicit_glue_table_name_when_given(monkeypatch):
    captured = {}

    class FakeS3:
        @staticmethod
        def to_parquet(**kwargs):
            captured.update(kwargs)

    class FakeCatalog:
        @staticmethod
        def databases(limit=None):
            return pd.DataFrame({"Database": ["dataguard_db"]})

    monkeypatch.setattr(
        storage, "_wr", lambda: type("wr", (), {"s3": FakeS3, "catalog": FakeCatalog})
    )
    storage.write_parquet(_rows(), "s3://bucket/silver", "", glue_table="silver_data")

    assert captured["table"] == "silver_data"
