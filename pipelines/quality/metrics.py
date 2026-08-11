"""Layer 1 quality metric vector, computed per station-day.

These metrics ARE the training data — no labelled dataset of healthy/broken
days exists, so we build it. Each metric maps to a documented degradation
mode (see docs/data-source.md, events E2-E8).
"""

import pandas as pd

METRIC_COLUMNS = [
    "readings_received",      # count of rows that arrived
    "readings_expected",      # based on the sensor's historical cadence
    "missing_rate",           # 1 - received/expected
    "negative_count",         # physically impossible values (E4)
    "max_stuck_run",          # longest run of an identical repeated value (E3)
    "value_variance",         # collapse to ~0 indicates a stuck sensor
    "file_lateness_hours",    # arrival minus the 72-hour commitment (E2)
    "schema_changed",         # columns differ from previous day's file (E7)
]


def compute_station_day_metrics(silver: pd.DataFrame) -> pd.DataFrame:
    """Return one metric row per (location_id, parameter, date).

    TODO(data quality engineer): group silver measurements by station-day and
    compute METRIC_COLUMNS. Expected cadence comes from each sensor's own
    trailing history, not a global constant.
    """
    raise NotImplementedError
