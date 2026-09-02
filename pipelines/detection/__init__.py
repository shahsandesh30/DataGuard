"""Layer 2: did something genuinely unusual happen in the air? Event detection."""

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
