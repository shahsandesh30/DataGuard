"""Join Layer 1 incidents with Layer 2 alerts and assign trust scores.

An alert raised while the underlying data was unhealthy is quarantined —
held back for human review, never deleted or hidden (see docs: public
safety). The headline evaluation metric is how much this reduces false
alerts while retaining genuine events.
"""

import pandas as pd


def fuse(quality_incidents: pd.DataFrame, event_alerts: pd.DataFrame) -> pd.DataFrame:
    """Return alerts with trust_score and status in {escalated, quarantined}.

    TODO(project lead): join on (location_id/region, time window); derive
    trust_score from Layer 1 health of the contributing stations; document
    the function and threshold in models/fusion_spec.md.
    """
    raise NotImplementedError
