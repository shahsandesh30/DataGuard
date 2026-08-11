"""Layer 1 anomaly model over the quality metric time-series.

Isolation Forest (Liu, Ting & Zhou, 2008) over the station-day metric
vectors: catches multivariate degradation that no single-metric threshold
would flag. Output is a quality incident table written to gold and consumed
by the fusion layer.
"""

import pandas as pd


def fit_quality_model(metrics: pd.DataFrame):
    """Fit an Isolation Forest over historical station-day metric vectors.

    TODO(data quality engineer): scale features, fit
    sklearn.ensemble.IsolationForest, persist with joblib to models/
    (gitignored) and record hyperparameters in models/layer1_model_card.md.
    """
    raise NotImplementedError


def score_quality(model, metrics: pd.DataFrame) -> pd.DataFrame:
    """Return metrics with an anomaly score and is_incident flag per row."""
    raise NotImplementedError
