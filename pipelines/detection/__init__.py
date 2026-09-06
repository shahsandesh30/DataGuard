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

# Optional I/O / local scoring helpers used by the CLI mode below
from pipelines.detection.io import (
    read_silver,
    write_derived_features,
    read_derived_features,
)
from pipelines.detection.models import (
    run_baseline_for_all_parameters,
    inspect_top_anomalies,
    SCORABLE_PARAMETERS,
)

__all__ = [
    "DetectionBuildResult",
    "build_detection",
    "build_event_features",
    "build_hourly_event_features",
    "read_event_alerts",
    "read_event_features",
    "weak_labels",
    # exports for ad-hoc scoring (optional)
    "read_derived_features",
    "run_baseline_for_all_parameters",
    "SCORABLE_PARAMETERS",
]


def _build_and_write() -> None:
    """Default behaviour: read silver, build hourly derived features and write them.

    This preserves the original `python -m pipelines.detection` behaviour so the
    module can be used in production orchestration to generate the derived
    feature parquet dataset registered in Glue.
    """
    silver = read_silver()
    print(f"Read {len(silver)} silver rows")

    features = build_hourly_event_features(silver)
    print(f"Built feature table: {len(features)} rows, {features.shape[1]} columns")

    write_derived_features(features)
    print("Wrote feature table to silver/derived")


def _score_from_derived() -> None:
    """Ad-hoc local scoring flow: read derived features (Glue), run baseline model.

    This is useful for local exploration or CI-style checks; it intentionally
    does not write back to Glue but writes a small CSV with scored rows.
    """
    print("Reading derived features from glue table")
    silver_derived = read_derived_features()

    print(f"Running baseline Isolation Forest for {len(silver_derived)} derived rows")
    scored = run_baseline_for_all_parameters(silver_derived)

    print(f"Saving scored results in a file..")
    filename = "scored_results.csv"
    scored.to_csv(filename, index=False)
    print(f"Saved scored results to {filename}")


def main() -> None:
    """Entry point used when running `python -m pipelines.detection`.

    Supports two modes selected by an optional command line argument:
    - build (default): read silver, build hourly features, write derived dataset
    - score: read derived features and run the baseline IsolationForest score
    """
    import argparse

    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--mode",
        choices=("build", "score"),
        default="build",
        help="Mode to run: 'build' to produce derived features (default), 'score' to run baseline scoring",
    )
    args = parser.parse_args()

    if args.mode == "build":
        _build_and_write()
    else:
        _score_from_derived()


if __name__ == "__main__":
    main()
