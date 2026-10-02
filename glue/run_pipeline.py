"""Scheduled entry point: run the whole pipeline over a trailing window.

A scheduler invokes this with no dates. It works out which days to fetch and
then hands off to the ordinary CLI — there is no second copy of the pipeline
logic here, only the date arithmetic a cron-style trigger cannot do for itself.

    python glue/run_pipeline.py                                    # trailing window
    python glue/run_pipeline.py --start 2023-01-01 --end 2023-01-31  # backfill

OpenAQ writes each day's file roughly 72 hours after the end of that day, so
"yesterday" never exists yet. The window therefore ends ``--lag-days`` back and
covers ``--window-days``.

The overlap between consecutive runs is deliberate. ``fetch_location_day``
checks the destination before downloading and returns ``skipped``, so re-running
a day already in bronze costs one HeadObject. A file that OpenAQ published late
on Monday is picked up by Tuesday's run with no backfill logic, no catch-up
mode, and no state to keep between runs.
"""

from __future__ import annotations

import argparse
import logging
import os
import sys
from datetime import UTC, date, datetime, timedelta
from pathlib import Path

# Glue uploads this script on its own and puts the packaged pipeline on the path
# via --extra-py-files. Run locally, sys.path[0] is glue/, so the repo root has
# to be added before `pipelines` resolves.
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from pipelines.__main__ import main as run_pipeline

# The most recent day that can exist, given OpenAQ's 72h publication commitment.
DEFAULT_LAG_DAYS = 3

# Days re-fetched each run. More than one so late arrivals are caught; see the
# module docstring for why the overlap is close to free.
DEFAULT_WINDOW_DAYS = 7

# Keep the first scheduled deployment small and predictable. The ordinary CLI
# still supports the wider station list and explicit --locations overrides.
DEFAULT_LOCATIONS = [1544061, 1601414, 2455394, 6430870]


def parse_locations(values: list[str]) -> list[int]:
    """Accept normal CLI tokens or Glue's single comma-separated argument."""
    locations: list[int] = []
    for value in values:
        locations.extend(int(item) for item in value.replace(",", " ").split())
    if not locations:
        raise ValueError("at least one location is required")
    return locations


def trailing_window(
    today: date,
    *,
    lag_days: int = DEFAULT_LAG_DAYS,
    window_days: int = DEFAULT_WINDOW_DAYS,
) -> tuple[date, date]:
    """Return the inclusive ``(start, end)`` window to fetch as of ``today``."""
    if window_days < 1:
        raise ValueError("window_days must be at least 1")
    if lag_days < 0:
        raise ValueError("lag_days cannot be negative")
    end = today - timedelta(days=lag_days)
    return end - timedelta(days=window_days - 1), end


def build_pipeline_argv(
    start: date,
    end: date,
    *,
    locations: list[int] | None = None,
    bronze_root: str | None = None,
    silver_root: str | None = None,
    gold_root: str | None = None,
) -> list[str]:
    """Build the argv for ``python -m pipelines run``.

    Roots are only passed when set explicitly; otherwise the CLI resolves them
    from .env, keeping one place that decides where a zone lives.
    """
    argv = ["run", "--start", start.isoformat(), "--end", end.isoformat()]
    if locations:
        argv += ["--locations", *(str(location) for location in locations)]
    for flag, value in (
        ("--bronze-root", bronze_root),
        ("--silver-root", silver_root),
        ("--gold-root", gold_root),
    ):
        if value:
            argv += [flag, value]
    return argv


def _parse_args(argv: list[str] | None) -> tuple[argparse.Namespace, list[str]]:
    parser = argparse.ArgumentParser(
        prog="run_pipeline",
        description="Run every DataGuard stage over a trailing or explicit window.",
    )
    parser.add_argument("--start", type=date.fromisoformat, default=None)
    parser.add_argument("--end", type=date.fromisoformat, default=None)
    parser.add_argument(
        "--locations",
        nargs="+",
        default=[str(location) for location in DEFAULT_LOCATIONS],
    )
    parser.add_argument("--lag-days", type=int, default=DEFAULT_LAG_DAYS)
    parser.add_argument("--window-days", type=int, default=DEFAULT_WINDOW_DAYS)
    parser.add_argument("--bronze-root", default=None)
    parser.add_argument("--silver-root", default=None)
    parser.add_argument("--gold-root", default=None)
    parser.add_argument("--glue-database", default="dataguard")

    # Glue injects its own arguments (--JOB_NAME and friends). parse_known_args
    # so an unrecognised scheduler flag logs a line instead of killing the run.
    args, unknown = parser.parse_known_args(argv)
    try:
        args.locations = parse_locations(args.locations)
    except ValueError as exc:
        parser.error(str(exc))

    if (args.start is None) != (args.end is None):
        parser.error("--start and --end must be given together")
    if args.start is not None and args.end < args.start:
        parser.error("--end must be on or after --start")
    return args, unknown


def main(argv: list[str] | None = None) -> int:
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
    args, unknown = _parse_args(argv)
    if unknown:
        logging.info("Ignoring arguments supplied by the scheduler: %s", unknown)

    if args.start is not None:
        start, end = args.start, args.end
        logging.info("Explicit window: %s to %s", start, end)
    else:
        start, end = trailing_window(
            datetime.now(UTC).date(),
            lag_days=args.lag_days,
            window_days=args.window_days,
        )
        logging.info(
            "Trailing window: %s to %s (%s days, %s-day publication lag)",
            start,
            end,
            args.window_days,
            args.lag_days,
        )

    # Settings resolve environment variables per run. Scope the override to
    # this invocation so tests and embedded callers do not leak configuration.
    previous_database = os.environ.get("GLUE_DATABASE")
    os.environ["GLUE_DATABASE"] = args.glue_database
    try:
        return run_pipeline(
            build_pipeline_argv(
                start,
                end,
                locations=args.locations,
                bronze_root=args.bronze_root,
                silver_root=args.silver_root,
                gold_root=args.gold_root,
            )
        )
    finally:
        if previous_database is None:
            os.environ.pop("GLUE_DATABASE", None)
        else:
            os.environ["GLUE_DATABASE"] = previous_database


def run_job(argv: list[str] | None = None) -> None:
    """Run under Glue without raising ``SystemExit(0)`` on success.

    Glue's Spark launcher reports any ``SystemExit`` as a user-application
    failure, including the zero exit raised by the usual CLI entry-point
    pattern.  Preserve nonzero pipeline results as real job failures while
    allowing a successful run to return normally.
    """
    exit_code = main(argv)
    if exit_code:
        raise RuntimeError(f"DataGuard pipeline failed with exit code {exit_code}")


if __name__ == "__main__":
    run_job()
