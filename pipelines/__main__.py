"""DataGuard pipeline CLI.

    python -m pipelines ingest --locations 2178 --start 2023-01-01 --end 2023-01-31
    python -m pipelines conform
    python -m pipelines quality
    python -m pipelines run --locations 2178 --start 2023-01-01 --end 2023-01-31
"""

from __future__ import annotations

import argparse
import logging
import sys
from collections import Counter
from datetime import date
from pathlib import Path

from pipelines.config import DEFAULT_LOCATION_IDS, load_settings
from pipelines.conformance.conform import build_silver
from pipelines.detection.build import build_detection
from pipelines.ingestion.fetch import adopt_flat_bronze, fetch_range
from pipelines.quality.build import build_quality


def _add_common(parser: argparse.ArgumentParser) -> None:
    parser.add_argument(
        "--bronze-root",
        type=Path,
        default=None,
        help="Local bronze directory (default: data/bronze)",
    )
    parser.add_argument(
        "--silver-root",
        type=Path,
        default=None,
        help="Local silver directory (default: data/silver)",
    )
    parser.add_argument(
        "--gold-root",
        type=Path,
        default=None,
        help="Local gold directory (default: data/gold)",
    )


def _parse_args(argv: list[str] | None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        prog="python -m pipelines",
        description="DataGuard bronze ingestion, silver conformance, Layer 1 quality, Layer 2 detection.",
    )
    sub = parser.add_subparsers(dest="command", required=True)

    ingest = sub.add_parser("ingest", help="Fetch OpenAQ archive files into bronze")
    ingest.add_argument("--locations", type=int, nargs="+", default=DEFAULT_LOCATION_IDS)
    ingest.add_argument("--start", type=date.fromisoformat, required=True)
    ingest.add_argument("--end", type=date.fromisoformat, required=True)
    ingest.add_argument("--force", action="store_true", help="Re-download files already in bronze")
    _add_common(ingest)

    conform = sub.add_parser("conform", help="Build the silver parquet dataset from bronze")
    _add_common(conform)

    quality = sub.add_parser("quality", help="Build Layer 1 quality metrics and incidents (gold)")
    _add_common(quality)

    detect = sub.add_parser("detect", help="Build Layer 2 event features and ranked alerts (gold)")
    _add_common(detect)

    run = sub.add_parser("run", help="Adopt/fetch bronze, build silver, Layer 1 quality, Layer 2 detection")
    run.add_argument("--locations", type=int, nargs="+", default=DEFAULT_LOCATION_IDS)
    run.add_argument("--start", type=date.fromisoformat, required=True)
    run.add_argument("--end", type=date.fromisoformat, required=True)
    run.add_argument("--force", action="store_true")
    _add_common(run)

    return parser.parse_args(argv)


def _summarise_fetch(results) -> None:
    counts = Counter(item.status for item in results)
    logging.info("Ingest summary: %s", dict(counts))
    for status in ("missing", "error"):
        for item in results:
            if item.status == status:
                logging.info("  %s %s %s", item.status, item.archive_key, item.error or "")


def _cmd_ingest(args: argparse.Namespace) -> int:
    settings = load_settings()
    bronze_root = args.bronze_root or settings.bronze_root
    adopted = adopt_flat_bronze(bronze_root)
    if adopted:
        logging.info("Adopted %s flat bronze files into archive layout", len(adopted))
    results = fetch_range(
        args.locations,
        args.start,
        args.end,
        bronze_root=bronze_root,
        force=args.force,
    )
    _summarise_fetch(results)
    return 0 if not any(item.status == "error" for item in results) else 1


def _cmd_conform(args: argparse.Namespace) -> int:
    settings = load_settings()
    bronze_root = args.bronze_root or settings.bronze_root
    silver_root = args.silver_root or settings.silver_root
    adopt_flat_bronze(bronze_root)
    result = build_silver(bronze_root=bronze_root, silver_root=silver_root)
    logging.info(
        "Silver: %s rows, %s files read, %s failed -> %s",
        result.rows,
        result.files_read,
        result.files_failed,
        result.output_path,
    )
    return 0 if result.files_failed == 0 else 1


def _cmd_quality(args: argparse.Namespace) -> int:
    settings = load_settings()
    bronze_root = args.bronze_root or settings.bronze_root
    gold_root = args.gold_root or settings.gold_root
    result = build_quality(bronze_root=bronze_root, gold_root=gold_root)
    logging.info(
        "Layer 1: %s station-days, %s rule incidents, %s model incidents (trained=%s) -> %s",
        result.station_day_rows,
        result.rule_incidents,
        result.model_incidents,
        result.model_trained,
        result.output_path,
    )
    return 0


def _cmd_detect(args: argparse.Namespace) -> int:
    settings = load_settings()
    bronze_root = args.bronze_root or settings.bronze_root
    gold_root = args.gold_root or settings.gold_root
    result = build_detection(bronze_root=bronze_root, gold_root=gold_root)
    logging.info(
        "Layer 2: %s feature rows, %s alerts (trained=%s) -> %s",
        result.feature_rows,
        result.alert_rows,
        result.ensemble_trained,
        result.output_path,
    )
    return 0


def main(argv: list[str] | None = None) -> int:
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
    args = _parse_args(argv)
    if args.command == "ingest":
        return _cmd_ingest(args)
    if args.command == "conform":
        return _cmd_conform(args)
    if args.command == "quality":
        return _cmd_quality(args)
    if args.command == "detect":
        return _cmd_detect(args)
    ingest_code = _cmd_ingest(args)
    conform_code = _cmd_conform(args)
    quality_code = _cmd_quality(args)
    detect_code = _cmd_detect(args)
    return ingest_code or conform_code or quality_code or detect_code


if __name__ == "__main__":
    sys.exit(main())
