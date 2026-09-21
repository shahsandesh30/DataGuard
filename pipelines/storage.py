"""Zone storage: the same pipeline against local directories or S3 + Glue.

A zone root is a string. ``data/silver`` is a local directory; ``s3://bucket/silver``
is an S3 prefix. Every stage calls the helpers here instead of touching
``pathlib`` or ``boto3`` directly, so switching a zone to AWS is a config
change, not a code change::

    SILVER_ROOT=s3://dataguard-openaq-silver
    python -m pipelines conform --bronze-root s3://dataguard-openaq-bronze

Parquet written to S3 is registered in the Glue Catalog, so the same table is
readable through Athena. Reads go straight to S3 rather than through Athena:
same bytes, no workgroup or query-output bucket required, no per-query cost.
"""

from __future__ import annotations

import logging
import re
import shutil
from datetime import UTC, datetime
from pathlib import Path

import pandas as pd
from botocore.exceptions import ClientError

from pipelines.config import load_settings

logger = logging.getLogger(__name__)

S3_PREFIX = "s3://"

# HeadObject reports a missing key through any of these, depending on whether
# the caller may list the bucket.
_S3_NOT_FOUND = {"404", "NoSuchKey", "NotFound", "404 Not Found"}

# Path("s3://bucket") collapses the double slash and, on Windows, flips the
# separators — so a root that has been round-tripped through pathlib arrives as
# "s3:/bucket" or "s3:\bucket". pipelines/detection wraps its roots in Path()
# before handing them back to us, so repair the URI rather than trusting it.
_MANGLED_S3 = re.compile(r"^s3:[\\/]+", re.IGNORECASE)


def normalize(root: str | Path) -> str:
    """Return a zone root as a clean string, repairing mangled ``s3://`` URIs."""
    text = str(root).strip()
    if _MANGLED_S3.match(text):
        return S3_PREFIX + text[3:].lstrip("/\\").replace("\\", "/").rstrip("/")
    return text


def is_s3(root: str | Path) -> bool:
    return normalize(root).startswith(S3_PREFIX)


def join(root: str | Path, *parts: str) -> str:
    """Join path segments under a zone root, for either backend."""
    base = normalize(root)
    segments = [str(p).strip("/") for p in parts if str(p).strip("/")]
    if not segments:
        return base
    if is_s3(base):
        return "/".join([base.rstrip("/"), *segments])
    return str(Path(base).joinpath(*segments))


def _bucket_and_key(uri: str) -> tuple[str, str]:
    without_scheme = normalize(uri)[len(S3_PREFIX) :]
    bucket, _, key = without_scheme.partition("/")
    return bucket, key


def _wr():
    """Import awswrangler lazily so local runs never need the AWS stack."""
    import awswrangler

    return awswrangler


# --------------------------------------------------------------------------- #
# Discovery and raw file reads (bronze)
# --------------------------------------------------------------------------- #


def list_files(root: str | Path, suffixes: tuple[str, ...]) -> list[str]:
    """Every file under ``root`` ending in one of ``suffixes``, sorted."""
    base = normalize(root)
    if is_s3(base):
        paths = _wr().s3.list_objects(base.rstrip("/") + "/")
    else:
        directory = Path(base)
        if not directory.exists():
            return []
        paths = [str(p) for p in directory.rglob("*") if p.is_file()]
    return sorted(p for p in paths if p.endswith(suffixes))


def exists(uri: str | Path) -> bool:
    target = normalize(uri)
    if is_s3(target):
        return bool(_wr().s3.does_object_exist(target))
    return Path(target).exists()


def file_info(uri: str | Path) -> tuple[int, str] | None:
    """``(size_bytes, last_modified_iso)`` for a file, or ``None`` if absent.

    Absent has to mean ``None``, not an exception: ingestion asks this about
    every location-day before fetching it, and a file that is not there yet is
    the normal case rather than an error.
    """
    target = normalize(uri)
    if is_s3(target):
        try:
            described = _wr().s3.describe_objects([target])
        except ClientError as exc:
            # A 403 means the credentials are wrong, not that the key is
            # missing, so only absence is swallowed.
            if str(exc.response.get("Error", {}).get("Code", "")) in _S3_NOT_FOUND:
                return None
            raise
        if not described:
            return None
        meta = next(iter(described.values()))
        return int(meta["ContentLength"]), meta["LastModified"].isoformat()
    path = Path(target)
    if not path.exists():
        return None
    stat = path.stat()
    return stat.st_size, datetime.fromtimestamp(stat.st_mtime, tz=UTC).isoformat()


def read_csv(uri: str | Path, **kwargs) -> pd.DataFrame:
    """Read one CSV (optionally gzipped) from either backend."""
    target = normalize(uri)
    compression = "gzip" if target.endswith(".gz") else None
    if is_s3(target):
        return _wr().s3.read_csv(target, compression=compression, encoding="utf-8", **kwargs)
    return pd.read_csv(target, compression=compression, encoding="utf-8", **kwargs)


def read_text(uri: str | Path) -> str | None:
    """Read a small text file, or ``None`` when it does not exist."""
    target = normalize(uri)
    if is_s3(target):
        if not exists(target):
            return None
        bucket, key = _bucket_and_key(target)
        import boto3

        body = boto3.client("s3").get_object(Bucket=bucket, Key=key)["Body"].read()
        return body.decode("utf-8")
    path = Path(target)
    return path.read_text(encoding="utf-8") if path.exists() else None


def write_text(uri: str | Path, text: str) -> None:
    """Write a small text file, replacing any existing one."""
    target = normalize(uri)
    if is_s3(target):
        bucket, key = _bucket_and_key(target)
        import boto3

        boto3.client("s3").put_object(Bucket=bucket, Key=key, Body=text.encode("utf-8"))
        return
    path = Path(target)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(text, encoding="utf-8")


def append_text(uri: str | Path, line: str) -> None:
    """Append one line to a text file.

    S3 objects cannot be appended to, so the object is rewritten. The bronze
    manifest is small and written once per ingested location-day, so the
    read-modify-write is acceptable; it is not safe for concurrent writers.
    """
    target = normalize(uri)
    if is_s3(target):
        existing = read_text(target) or ""
        bucket, key = _bucket_and_key(target)
        import boto3

        boto3.client("s3").put_object(
            Bucket=bucket, Key=key, Body=(existing + line).encode("utf-8")
        )
        return
    path = Path(target)
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a", encoding="utf-8") as handle:
        handle.write(line)


def put_file(local_path: Path, uri: str | Path) -> int:
    """Move a local file into a zone, returning its size in bytes."""
    target = normalize(uri)
    size = local_path.stat().st_size
    if is_s3(target):
        _wr().s3.upload(local_file=str(local_path), path=target)
        local_path.unlink()
        return size
    destination = Path(target)
    destination.parent.mkdir(parents=True, exist_ok=True)
    shutil.move(str(local_path), str(destination))
    return size


# --------------------------------------------------------------------------- #
# Parquet datasets (silver and gold)
# --------------------------------------------------------------------------- #

PARTITION_COLUMNS = ["locationid", "year"]


def _glue_table_name(target: str) -> str:
    """Glue table name for a dataset: its full S3 key with separators flattened.

    ``s3://bucket/layer2/event_features`` becomes ``layer2_event_features``. The
    name is taken from the key rather than the caller's dataset argument because
    the zone lives in the root — Layer 1 writes ``quality_metrics`` under a
    ``gold/layer1`` root, and pipelines/detection registers its own
    ``event_features`` table over ``s3://<silver>/derived/``. Glue rejects two
    tables of one name pointing at different locations, so the prefix matters.
    """
    bucket, key = _bucket_and_key(target)
    return re.sub(r"[^a-z0-9_]+", "_", (key.strip("/") or bucket).lower()).strip("_")


def write_parquet(
    frame: pd.DataFrame,
    root: str | Path,
    dataset: str,
    *,
    year_from: str = "date_local",
    glue_table: str | None = None,
) -> str:
    """Overwrite ``dataset`` under ``root``, partitioned by locationid and year.

    On S3 the dataset is also registered in the Glue Catalog so Athena can
    query it. ``year_from`` names the column the partition year is derived from.
    """
    target = join(root, dataset)
    if frame.empty:
        _write_empty(frame, target)
        return target

    partitioned = frame.copy()
    partitioned["year"] = _years(partitioned, year_from)
    partitioned = partitioned[partitioned["locationid"].notna() & partitioned["year"].notna()]
    if partitioned.empty:
        _write_empty(frame, target)
        return target
    partitioned["locationid"] = partitioned["locationid"].astype(int)
    partitioned["year"] = partitioned["year"].astype(int)

    if is_s3(target):
        settings = load_settings()
        database = _ensure_database(settings.glue_database)
        _wr().s3.to_parquet(
            df=partitioned,
            path=target.rstrip("/") + "/",
            dataset=True,
            mode="overwrite",
            partition_cols=PARTITION_COLUMNS,
            compression="snappy",
            database=database,
            table=glue_table or _glue_table_name(target),
        )
        return target

    directory = Path(target)
    for stale in directory.rglob("*.parquet"):
        stale.unlink()
    for (locationid, year), part in partitioned.groupby(PARTITION_COLUMNS, sort=False):
        partition = directory / f"locationid={locationid}" / f"year={year}"
        partition.mkdir(parents=True, exist_ok=True)
        # `year` is derived, so it lives only in the path. `locationid` is part
        # of the logical schema and stays in the file as well: pipelines/detection
        # reads gold with a plain rglob that cannot recover partition values.
        part.drop(columns=["year"]).to_parquet(
            partition / "part-0.parquet", index=False, compression="snappy"
        )
    return target


def _years(frame: pd.DataFrame, column: str) -> pd.Series:
    values = frame[column]
    if pd.api.types.is_datetime64_any_dtype(values):
        return values.dt.year
    return pd.to_numeric(values.astype(str).str.slice(0, 4), errors="coerce")


def _write_empty(frame: pd.DataFrame, target: str) -> None:
    """Keep an empty dataset addressable so readers return a typed frame."""
    if is_s3(target):
        # Nothing to keep addressable on S3: an empty prefix reads back as an
        # empty frame, and writing a marker object would only confuse a crawler.
        _wr().s3.delete_objects(_wr().s3.list_objects(target.rstrip("/") + "/", suffix=".parquet"))
        return
    directory = Path(target)
    directory.mkdir(parents=True, exist_ok=True)
    for stale in directory.rglob("*.parquet"):
        stale.unlink()
    frame.to_parquet(directory / "_empty.parquet", index=False, compression="snappy")


def read_parquet(root: str | Path, dataset: str) -> pd.DataFrame:
    """Read a dataset back, with an identical schema from either backend."""
    target = join(root, dataset)
    if is_s3(target):
        prefix = target.rstrip("/") + "/"
        # A zone root also holds sidecars such as _build.json. Restrict the read
        # to Parquet or awswrangler tries to parse them as data files.
        if not _wr().s3.list_objects(prefix, suffix=".parquet"):
            return pd.DataFrame()
        frame = _wr().s3.read_parquet(prefix, dataset=True, path_suffix=".parquet")
    else:
        directory = Path(target)
        files = sorted(directory.rglob("*.parquet")) if directory.exists() else []
        if not files:
            return pd.DataFrame()
        frame = pd.concat(
            (_read_local_partition(f, directory) for f in files), ignore_index=True
        )

    # The partition columns live in the object key, so S3 restores them and the
    # local backend does not. Drop the derived one and restore the dtype.
    frame = frame.drop(columns=["year"], errors="ignore")
    if "locationid" in frame.columns:
        frame["locationid"] = pd.to_numeric(frame["locationid"], errors="coerce").astype("Int64")
    return frame.reset_index(drop=True)


def _read_local_partition(path: Path, root: Path) -> pd.DataFrame:
    """Read one local part file, restoring ``key=value`` columns from its path.

    Partition values live in the directory names under both backends. S3 reads
    put them back automatically; the local reader has to do it by hand, or the
    two backends would disagree on the schema.
    """
    frame = pd.read_parquet(path)
    for segment in path.relative_to(root).parts[:-1]:
        key, sep, value = segment.partition("=")
        if sep and key not in frame.columns:
            frame[key] = value
    return frame


def _ensure_database(name: str) -> str:
    """Create the Glue database on first write; harmless if it already exists."""
    wr = _wr()
    if name not in wr.catalog.databases(limit=1000).values:
        logger.info("Creating Glue database %s", name)
        wr.catalog.create_database(name=name, exist_ok=True)
    return name
