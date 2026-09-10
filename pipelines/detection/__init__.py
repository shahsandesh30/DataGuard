"""Layer 2: did something genuinely unusual happen in the air? Event detection."""

from pipelines.detection.build import (
    DetectionBuildResult,
    build_detection,
    read_event_alerts,
    read_event_features,
)
from pipelines.detection.features import (
    build_event_features,
    build_hourly_event_features,
    weak_labels,
)

__all__ = [
    "DetectionBuildResult",
    "build_detection",
    "build_event_features",
    "build_hourly_event_features",
    "read_event_alerts",
    "read_event_features",
    "weak_labels",
]


def _build_and_write() -> None:
    """Read Athena silver, build hourly features, write Glue derived parquet."""
    from pipelines.detection.io import read_silver, write_derived_features

    silver = read_silver()
    print(f"Read {len(silver)} silver rows")

    features = build_hourly_event_features(silver)
    print(f"Built feature table: {len(features)} rows, {features.shape[1]} columns")

    write_derived_features(features)
    print("Wrote feature table to silver/derived")


def _score_from_derived() -> None:
    """Read derived features from Glue and run the baseline Isolation Forest."""
    from pipelines.detection.io import read_derived_features
    from pipelines.detection.models import run_baseline_for_all_parameters

    print("Reading derived features from glue table")
    silver_derived = read_derived_features()

    print(f"Running baseline Isolation Forest for {len(silver_derived)} derived rows")
    scored = run_baseline_for_all_parameters(silver_derived)

    filename = "scored_results.csv"
    scored.to_csv(filename, index=False)
    print(f"Saved scored results to {filename}")


def main() -> None:
    """Entry point for `python -m pipelines.detection`.

    --mode build  (default)  read silver, write derived features
    --mode score             score derived features with Isolation Forest
    """
    import argparse

    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--mode",
        choices=("build", "score"),
        default="build",
        help="build derived features (default) or score existing derived rows",
    )
    args = parser.parse_args()

    if args.mode == "build":
        _build_and_write()
    else:
        _score_from_derived()


if __name__ == "__main__":
    main()
