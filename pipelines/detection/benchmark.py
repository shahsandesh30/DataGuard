"""Offline Layer 2 benchmark: temporal holdout and injected-spike recall.

Layer 2 has no labelled ground truth, so the per-run metrics in evaluation.py
can only measure agreement with the weak-label heuristic. This module adds two
checks that do not depend on that heuristic being right:

* **Temporal holdout** fits the ensemble on the earlier dates and scores the
  later dates out of sample. A scheduled run scores days the model has never
  seen, so the gap between the in-sample and holdout flag rates shows how far
  the in-sample numbers flatter the model.
* **Injected-spike recall** adds known spikes to real silver, rebuilds the
  features, and counts how many of those spikes come back as alerts. The truth
  is known by construction, so this is a genuine recall figure.

Neither runs inside ``detect``; ``python -m pipelines evaluate`` runs both and
writes ``gold/layer2/_detection_benchmark.json``.
"""

from __future__ import annotations

import json
import logging
from dataclasses import asdict, dataclass
from datetime import UTC, datetime
from pathlib import Path

import numpy as np
import pandas as pd
from sklearn.neighbors import LocalOutlierFactor

from pipelines import storage
from pipelines.config import LAYER2_BASELINE_DAYS, MIN_EVENT_ROWS, load_settings
from pipelines.conformance.conform import read_silver
from pipelines.detection.ensemble import _feature_matrix, fit_ensemble, score_events
from pipelines.detection.features import build_event_features, weak_labels

logger = logging.getLogger(__name__)

BENCHMARK_FILE = "_detection_benchmark.json"
HOLDOUT_FRACTION = 0.25
INJECTION_PARAMETER = "pm25"
# A spike adds ``magnitude`` × the station's median level to every reading in
# the local-hour window, so magnitude 3.0 makes those hours roughly 4× normal.
INJECTION_MAGNITUDE = 3.0
INJECTION_HOURS = range(10, 18)
KEY_COLUMNS = ["locationid", "date_local", "parameter"]


@dataclass
class HoldoutResult:
    parameter: str
    train_dates: int
    holdout_dates: int
    train_rows: int
    holdout_rows: int
    train_flag_rate: float
    holdout_flag_rate: float
    holdout_if_rate: float
    holdout_lof_rate: float
    holdout_dbscan_rate: float
    holdout_weak_label_precision: float | None
    holdout_weak_label_base_rate: float | None
    holdout_weak_label_lift: float | None


@dataclass
class InjectionResult:
    parameter: str
    magnitude: float
    injected: int
    injected_scored: int
    recall: float | None
    recall_local: float | None
    recall_regional: float | None
    false_alarm_rate: float | None


def _keys(frame: pd.DataFrame) -> pd.MultiIndex:
    keyed = frame[KEY_COLUMNS].copy()
    keyed["locationid"] = pd.to_numeric(keyed["locationid"]).astype("int64")
    keyed["date_local"] = keyed["date_local"].astype(str)
    keyed["parameter"] = keyed["parameter"].astype(str)
    return pd.MultiIndex.from_frame(keyed)


def _rate(flags: pd.Series | np.ndarray) -> float:
    return float(np.mean(flags)) if len(flags) else 0.0


def out_of_sample_votes(models: dict, rows: pd.DataFrame) -> pd.DataFrame:
    """Score rows the ensemble was not fitted on; one boolean column per detector.

    ``fit_ensemble`` keeps fixed-sample LOF and DBSCAN labels, which only cover
    the training rows. Held-out rows are scored the way each method extends to
    new points: Isolation Forest predicts directly, LOF is refitted in novelty
    mode with the same neighbours and contamination, and a DBSCAN point is noise
    unless it lies within ``eps`` of a core sample.
    """
    train_scaled = models["train_scaled"]
    scaled = models["scaler"].transform(_feature_matrix(rows))

    if_flags = models["isolation_forest"].predict(scaled) == -1

    lof = models["lof"]
    novelty = LocalOutlierFactor(
        n_neighbors=lof.n_neighbors, contamination=lof.contamination, novelty=True
    ).fit(train_scaled)
    lof_flags = novelty.predict(scaled) == -1

    dbscan = models["dbscan"]
    core = dbscan.components_
    if len(core) == 0:
        dbscan_flags = np.ones(len(scaled), dtype=bool)
    else:
        distances = np.sqrt(((scaled[:, None, :] - core[None, :, :]) ** 2).sum(axis=2))
        dbscan_flags = distances.min(axis=1) > dbscan.eps

    votes = pd.DataFrame(
        {"if_flag": if_flags, "lof_flag": lof_flags, "dbscan_flag": dbscan_flags},
        index=rows.index,
    )
    votes["agreement_count"] = votes.sum(axis=1)
    return votes


def _fit_with_matrix(rows: pd.DataFrame) -> dict | None:
    models = fit_ensemble(rows)
    if models is not None:
        models["train_scaled"] = models["scaler"].transform(_feature_matrix(rows))
    return models


def temporal_holdout(
    features: pd.DataFrame,
    weak_label: pd.Series | None = None,
    *,
    holdout_fraction: float = HOLDOUT_FRACTION,
) -> list[HoldoutResult]:
    """Fit each parameter on its earlier dates and score its later dates.

    "Flagged" means at least one detector vote — the same condition that admits
    a row to the alert table — measured before the per-day top-K cap.
    Parameters with fewer than ``MIN_EVENT_ROWS`` training rows are skipped, as
    the production build would skip them.
    """
    if weak_label is None:
        weak_label = pd.Series(False, index=features.index)
    results: list[HoldoutResult] = []

    for parameter, group in features.groupby("parameter", sort=False):
        dates = sorted(group["date_local"].astype(str).unique())
        n_holdout = max(1, int(round(len(dates) * holdout_fraction)))
        if len(dates) - n_holdout < 1:
            continue
        cutoff = dates[-n_holdout]
        train = group[group["date_local"].astype(str) < cutoff]
        holdout = group[group["date_local"].astype(str) >= cutoff]
        if len(train) < MIN_EVENT_ROWS or holdout.empty:
            logger.info(
                "Holdout skipped for %s: %s training rows (need %s)",
                parameter,
                len(train),
                MIN_EVENT_ROWS,
            )
            continue

        models = _fit_with_matrix(train)
        train_flagged = (
            (models["isolation_forest"].predict(models["train_scaled"]) == -1)
            | (models["lof_labels"] == -1)
            | (models["dbscan_labels"] == -1)
        )
        holdout_votes = out_of_sample_votes(models, holdout)
        flagged = holdout_votes["agreement_count"] > 0

        labels = weak_label.reindex(holdout.index, fill_value=False).astype(bool)
        precision = float(labels[flagged].mean()) if flagged.any() else None
        base_rate = float(labels.mean())
        lift = precision / base_rate if precision is not None and base_rate else None

        results.append(
            HoldoutResult(
                parameter=str(parameter),
                train_dates=len(dates) - n_holdout,
                holdout_dates=n_holdout,
                train_rows=len(train),
                holdout_rows=len(holdout),
                train_flag_rate=_rate(train_flagged),
                holdout_flag_rate=_rate(flagged),
                holdout_if_rate=_rate(holdout_votes["if_flag"]),
                holdout_lof_rate=_rate(holdout_votes["lof_flag"]),
                holdout_dbscan_rate=_rate(holdout_votes["dbscan_flag"]),
                holdout_weak_label_precision=precision,
                holdout_weak_label_base_rate=base_rate,
                holdout_weak_label_lift=lift,
            )
        )
    return results


def inject_spikes(
    silver: pd.DataFrame,
    *,
    parameter: str = INJECTION_PARAMETER,
    n_local: int = 6,
    n_regional: int = 2,
    magnitude: float = INJECTION_MAGNITUDE,
    min_history_days: int = LAYER2_BASELINE_DAYS,
    seed: int = 42,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Return ``(injected_silver, truth)`` with known spikes added.

    *Local* spikes hit one station on one day, the shape of a point source.
    *Regional* spikes hit every station reporting ``parameter`` on one day, the
    shape of smoke or dust. Only days after each station's first
    ``min_history_days`` are used, so every spike has a trailing baseline to be
    measured against. The input frame is not modified.
    """
    rng = np.random.default_rng(seed)
    injected = silver.copy()
    rows = injected[injected["parameter"] == parameter]
    if rows.empty:
        return injected, pd.DataFrame(columns=[*KEY_COLUMNS, "kind"])

    station_days = rows[["locationid", "date_local"]].drop_duplicates()
    station_days = station_days.assign(date_local=station_days["date_local"].astype(str))
    first_day = station_days.groupby("locationid")["date_local"].transform(
        lambda days: sorted(days.unique())[min(min_history_days, days.nunique() - 1)]
    )
    eligible = station_days[station_days["date_local"] >= first_day]

    eligible_dates = sorted(eligible["date_local"].unique())
    regional_dates = list(
        rng.choice(eligible_dates, size=min(n_regional, len(eligible_dates)), replace=False)
    )
    regional = eligible[eligible["date_local"].isin(regional_dates)].assign(kind="regional")

    local_pool = eligible[~eligible["date_local"].isin(regional_dates)]
    picks = rng.choice(len(local_pool), size=min(n_local, len(local_pool)), replace=False)
    local = local_pool.iloc[sorted(picks)].assign(kind="local")

    truth = pd.concat([local, regional], ignore_index=True).assign(parameter=parameter)
    truth = truth[[*KEY_COLUMNS, "kind"]]

    medians = rows.groupby("locationid")["value"].median().clip(lower=1.0)
    hours = pd.to_datetime(injected["datetime_local"], errors="coerce").dt.hour
    is_parameter = injected["parameter"] == parameter
    # Non-target parameters are keyed as ``parameter`` too, then excluded by
    # is_parameter — this keeps one key helper for every comparison.
    target = (
        is_parameter
        & hours.isin(list(INJECTION_HOURS))
        & _keys(injected.assign(parameter=parameter)).isin(_keys(truth))
    )
    boost = injected.loc[target, "locationid"].map(medians).astype(float) * magnitude
    injected.loc[target, "value"] = injected.loc[target, "value"] + boost
    return injected, truth


def injection_recall(
    silver: pd.DataFrame,
    *,
    parameter: str = INJECTION_PARAMETER,
    magnitude: float = INJECTION_MAGNITUDE,
    seed: int = 42,
    **inject_kwargs,
) -> InjectionResult:
    """Inject spikes, rerun features + ensemble, and measure how many come back.

    Recall counts an injected station-day as caught when it appears in the final
    alert table, after the per-day top-K cap — the table a reviewer sees.
    ``false_alarm_rate`` is the share of untouched rows that became alerts.
    """
    injected, truth = inject_spikes(
        silver, parameter=parameter, magnitude=magnitude, seed=seed, **inject_kwargs
    )
    features = build_event_features(injected)
    features = features[features["parameter"] == parameter]
    empty = InjectionResult(parameter, magnitude, len(truth), 0, None, None, None, None)
    if truth.empty or len(features) < MIN_EVENT_ROWS:
        return empty

    alerts = score_events(fit_ensemble(features), features)
    alert_keys = (
        _keys(alerts) if not alerts.empty else pd.MultiIndex.from_tuples([], names=KEY_COLUMNS)
    )

    truth = truth[_keys(truth).isin(_keys(features))]
    if truth.empty:
        return empty
    caught = _keys(truth).isin(alert_keys)

    def _recall(kind: str) -> float | None:
        mask = (truth["kind"] == kind).to_numpy()
        return float(caught[mask].mean()) if mask.any() else None

    clean = features[~_keys(features).isin(_keys(truth))]
    false_alarms = _keys(clean).isin(alert_keys)
    return InjectionResult(
        parameter=parameter,
        magnitude=magnitude,
        injected=empty.injected,
        injected_scored=len(truth),
        recall=float(caught.mean()),
        recall_local=_recall("local"),
        recall_regional=_recall("regional"),
        false_alarm_rate=_rate(false_alarms) if len(clean) else None,
    )


def run_benchmark(
    silver_root: str | Path | None = None,
    gold_root: str | Path | None = None,
) -> dict:
    """Run holdout and injection checks over current silver; write a JSON report."""
    settings = load_settings()
    silver = read_silver(silver_root or settings.silver_root)
    gold = storage.join(gold_root or settings.gold_root, "layer2")

    features = build_event_features(silver)
    labels = weak_labels(features, silver)
    holdout = temporal_holdout(features, labels)
    injection = injection_recall(silver)

    payload = {
        "built_at_utc": datetime.now(UTC).isoformat(),
        "input_measurement_rows": int(len(silver)),
        "feature_rows": int(len(features)),
        "holdout": [asdict(item) for item in holdout],
        "injection": asdict(injection),
    }
    storage.write_text(storage.join(gold, BENCHMARK_FILE), json.dumps(payload, indent=2) + "\n")
    return payload
