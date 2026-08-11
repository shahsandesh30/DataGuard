"""Layer 2 detector ensemble: Isolation Forest + LOF + DBSCAN.

Unsupervised — the ensemble ranks candidate events; agreement between
detectors raises confidence. Output is a ranked alert table written to gold
and consumed by the fusion layer.
"""

import pandas as pd


def fit_ensemble(features: pd.DataFrame):
    """TODO(ML engineer): fit the three detectors; record agreement rates
    and Precision@K in models/layer2_model_card.md."""
    raise NotImplementedError


def score_events(models, features: pd.DataFrame) -> pd.DataFrame:
    """Return features with per-detector scores and a combined rank."""
    raise NotImplementedError
