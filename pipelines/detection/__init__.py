"""Layer 2 pollution-event detection."""

from pipelines.detection.build import (
    DetectionBuildResult,
    build_detection,
    read_event_alerts,
    read_event_features,
)
from pipelines.detection.features import build_event_features, weak_labels

__all__ = [
    "DetectionBuildResult",
    "build_detection",
    "build_event_features",
    "read_event_alerts",
    "read_event_features",
    "weak_labels",
]


def main() -> None:
    """Run the canonical conformed-data -> features -> alerts pipeline."""
    import argparse
    from pathlib import Path

    parser = argparse.ArgumentParser()
    parser.add_argument("--bronze-root", type=Path, default=None)
    parser.add_argument("--gold-root", type=Path, default=None)
    parser.add_argument("--model-dir", type=Path, default=None)
    args = parser.parse_args()

    result = build_detection(
        bronze_root=args.bronze_root,
        gold_root=args.gold_root,
        models_dir=args.model_dir,
    )
    print(
        f"Built {result.feature_rows} features and {result.alert_rows} alerts "
        f"(ensemble_trained={result.ensemble_trained})"
    )


if __name__ == "__main__":
    main()
