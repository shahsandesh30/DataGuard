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


def main() -> None:
    from pipelines.detection.io import read_silver, write_derived_features

    silver = read_silver()
    print(f"Read {len(silver)} silver rows")

    features = build_hourly_event_features(silver)
    print(f"Built feature table: {len(features)} rows, {features.shape[1]} columns")

    write_derived_features(features)
    print("Wrote feature table to silver/derived")


if __name__ == "__main__":
    main()
