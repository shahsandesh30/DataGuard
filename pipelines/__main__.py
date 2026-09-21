"""DataGuard pipeline CLI.

The pipeline is a straight line: each stage reads the zone the previous stage
wrote.

    ingest   OpenAQ archive -> bronze         (raw files, unchanged)
    conform  bronze         -> silver         (canonical units, types, timestamps)
    quality  silver         -> gold/layer1    (Layer 1: is the data healthy?)
    detect   bronze         -> gold/layer2    (Layer 2: did something happen?)
    fuse     gold           -> gold/fusion    (trust score, escalate/quarantine)

    python -m pipelines run --locations 2178 --start 2023-01-01 --end 2023-01-31
    python -m pipelines conform

Any zone root may be a local directory or an ``s3://`` prefix, set per run or via
BRONZE_ROOT / SILVER_ROOT / GOLD_ROOT in .env::

    python -m pipelines conform --bronze-root s3://dataguard-openaq-bronze \
                                --silver-root s3://dataguard-openaq-silver

Parquet written to S3 is registered in the Glue Catalog, so Athena sees the same
tables. Layer 2 still conforms bronze in memory rather than reading silver; it is
owned by another team member, so migrating it is a follow-up.
"""

from __future__ import annotations

import argparse
import logging
import sys
import tempfile
from collections import Counter
from datetime import date

from pipelines import storage
from pipelines.config import DEFAULT_locationidS, load_settings
from pipelines.conformance.conform import build_silver
from pipelines.detection.build import build_detection
from pipelines.fusion.build import build_fusion
from pipelines.ingestion.fetch import fetch_range
from pipelines.quality.build import build_quality

STAGES = ["ingest", "conform", "quality", "detect", "fuse"]


def _roots(args: argparse.Namespace) -> tuple[str, str, str]:
    """Resolve the three zone roots: CLI flag, else .env, else local defaults."""
    settings = load_settings()
    return (
        storage.normalize(args.bronze_root or settings.bronze_root),
        storage.normalize(args.silver_root or settings.silver_root),
        storage.normalize(args.gold_root or settings.gold_root),
    )


def _parse_args(argv: list[str] | None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        prog="python -m pipelines",
        description="DataGuard: bronze ingestion, silver conformance, Layer 1, Layer 2, fusion.",
    )
    sub = parser.add_subparsers(dest="command", required=True)

    helps = {
        "ingest": "Fetch OpenAQ archive files into bronze",
        "conform": "Build the silver measurement table from bronze",
        "quality": "Build Layer 1 quality metrics and incidents (gold)",
        "detect": "Build Layer 2 event features and ranked alerts (gold)",
        "fuse": "Trust-score Layer 2 alerts using Layer 1 incidents (gold)",
        "run": "Run every stage end to end",
    }
    for name in [*STAGES, "run"]:
        cmd = sub.add_parser(name, help=helps[name])
        if name in ("ingest", "run"):
            cmd.add_argument("--locations", type=int, nargs="+", default=DEFAULT_locationidS)
            cmd.add_argument("--start", type=date.fromisoformat, required=True)
            cmd.add_argument("--end", type=date.fromisoformat, required=True)
            cmd.add_argument("--force", action="store_true", help="Re-download existing files")
        cmd.add_argument(
            "--bronze-root",
            default=None,
            help="local directory or s3:// prefix (default: data/bronze)",
        )
        cmd.add_argument(
            "--silver-root",
            default=None,
            help="local directory or s3:// prefix (default: data/silver)",
        )
        cmd.add_argument(
            "--gold-root",
            default=None,
            help="local directory or s3:// prefix (default: data/gold)",
        )

    return parser.parse_args(argv)


def _ingest(args: argparse.Namespace) -> int:
    bronze, _, _ = _roots(args)
    results = fetch_range(
        args.locations, args.start, args.end, bronze_root=bronze, force=args.force
    )
    logging.info("Ingest: %s", dict(Counter(item.status for item in results)))
    for item in results:
        if item.status == "error":
            logging.warning("  error %s %s", item.archive_key, item.error or "")
    return 1 if any(item.status == "error" for item in results) else 0


def _conform(args: argparse.Namespace) -> int:
    bronze, silver, _ = _roots(args)
    result = build_silver(bronze_root=bronze, silver_root=silver)
    logging.info(
        "Silver: %s rows, %s files read, %s failed -> %s",
        result.rows,
        result.files_read,
        result.files_failed,
        result.output_path,
    )
    return 1 if result.files_failed else 0


def _quality(args: argparse.Namespace) -> int:
    bronze, silver, gold = _roots(args)
    result = build_quality(silver_root=silver, bronze_root=bronze, gold_root=gold)
    logging.info(
        "Layer 1: %s station-days, %s rule incidents, %s model incidents (trained=%s) -> %s",
        result.station_day_rows,
        result.rule_incidents,
        result.model_incidents,
        result.model_trained,
        result.output_path,
    )
    return 0


LAYER2_TABLES = ("event_features", "event_alerts")


def _detect(args: argparse.Namespace) -> int:
    bronze, silver, gold = _roots(args)

    if storage.is_s3(gold):
        # pipelines/detection writes its build summary with Path.write_text, which
        # cannot address S3. Run it against a local staging directory and publish
        # the two gold tables from there. Reading bronze from S3 works as-is,
        # because that goes through pipelines.conformance.
        with tempfile.TemporaryDirectory() as staging:
            result = build_detection(bronze_root=bronze, gold_root=staging)
            for table in LAYER2_TABLES:
                frame = storage.read_parquet(staging, f"layer2/{table}")
                storage.write_parquet(frame, storage.join(gold, "layer2"), table)
        published = storage.join(gold, "layer2")
    else:
        result = build_detection(silver_root=silver, gold_root=gold)
        published = result.output_path

    logging.info(
        "Layer 2: %s feature rows, %s alerts (trained=%s) -> %s",
        result.feature_rows,
        result.alert_rows,
        result.ensemble_trained,
        published,
    )
    return 0


def _fuse(args: argparse.Namespace) -> int:
    _, _, gold = _roots(args)
    result = build_fusion(gold_root=gold)
    logging.info(
        "Fusion: %s alerts (%s escalated, %s quarantined) -> %s",
        result.alert_rows,
        result.escalated,
        result.quarantined,
        result.output_path,
    )
    return 0


COMMANDS = {
    "ingest": _ingest,
    "conform": _conform,
    "quality": _quality,
    "detect": _detect,
    "fuse": _fuse,
}


def main(argv: list[str] | None = None) -> int:
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
    args = _parse_args(argv)

    if args.command != "run":
        return COMMANDS[args.command](args)

    exit_code = 0
    for stage in STAGES:
        exit_code = COMMANDS[stage](args) or exit_code
    return exit_code


if __name__ == "__main__":
    sys.exit(main())
