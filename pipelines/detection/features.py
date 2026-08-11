"""Layer 2 feature engineering over silver measurements.

Genuine pollution events (bushfire smoke, dust storms, industrial incidents)
appear as large, spatially coherent changes: multiple nearby stations moving
together distinguishes a real event from one broken sensor.
"""

import pandas as pd


def build_event_features(silver: pd.DataFrame) -> pd.DataFrame:
    """Return station-window and region-window features for event detection.

    TODO(ML engineer): rolling deviation from each station's own baseline,
    rate of change, cross-station agreement within a region, multi-parameter
    co-movement (e.g. PM2.5 and PM10 rising together suggests smoke/dust).
    """
    raise NotImplementedError


def weak_labels(features: pd.DataFrame) -> pd.Series:
    """Deterministic rule-based weak labels for evaluation (no ground truth
    exists — see risk R4).

    TODO(ML engineer): e.g. sustained multi-station PM2.5 elevation above the
    station's seasonal norm. Used for Precision@K, never for training.
    """
    raise NotImplementedError
