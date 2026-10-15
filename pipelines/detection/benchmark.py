"""Offline Layer 2 benchmark: temporal holdout and injected-spike recall.

Layer 2 has no labelled ground truth, so the per-run metrics in evaluation.py
can only measure agreement with the weak-label heuristic. This module adds two
checks that do not depend on that heuristic being right:

* **Temporal holdout** splits raw silver by local date, fits hourly reference
  baselines and the ensemble on earlier dates, and scores later dates. The
  difference in flag rates describes behaviour on unseen days, not accuracy.
* **Injected-spike recall** adds known spikes to real silver, rebuilds the
  features, and counts how many of those spikes come back as alerts. The truth
  is known by construction; recall describes these synthetic perturbations.

Neither runs inside ``detect``; ``python -m pipelines evaluate`` runs both and
writes ``gold/layer2/_detection_benchmark.json``.
"""

from __future__ import annotations

import hashlib
import json
import logging
import platform
from dataclasses import asdict, dataclass, field
from datetime import UTC, datetime
from pathlib import Path

import numpy as np
import pandas as pd
import sklearn
from sklearn.neighbors import LocalOutlierFactor

from pipelines import storage
from pipelines.config import (
    LAYER2_BASELINE_DAYS,
    LAYER2_PM_PARAMETERS,
    MIN_EVENT_ROWS,
    load_settings,
)
from pipelines.conformance.conform import read_silver
from pipelines.detection.ensemble import (
    TOP_K_ALERTS_PER_DAY,
    _feature_matrix,
    fit_ensemble,
    score_events,
)
from pipelines.detection.features import build_event_features, weak_labels

logger = logging.getLogger(__name__)

BENCHMARK_FILE = "_detection_benchmark.json"
METHODOLOGY_VERSION = 2
HOLDOUT_FRACTION = 0.25
INJECTION_PARAMETER = "pm25"
# A spike adds ``magnitude`` × the station's median level to every reading in
# the local-hour window, so magnitude 3.0 makes those hours roughly 4× normal.
INJECTION_MAGNITUDE = 3.0
INJECTION_HOURS = range(10, 18)
INJECTION_SEED = 42
LOCAL_SPIKES = 6
REGIONAL_DATES = 2
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
    train_date_max: str
    holdout_date_min: str


@dataclass
class InjectionResult:
    parameter: str
    magnitude: float
    injected: int
    injected_scored: int
    recall: float | None
    recall_local: float | None
    recall_regional: float | None
    untouched_alert_rate: float | None
    injected_by_kind: dict[str, int] = field(default_factory=dict)
    scored_by_kind: dict[str, int] = field(default_factory=dict)
    skipped: dict[str, int] = field(default_factory=dict)


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


def temporal_feature_split(
    silver: pd.DataFrame, cutoff: str
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Build training features separately and freeze their hourly baselines.

    Held-out days retain prior measurements for causal rolling/trailing
    statistics, but never contribute to the fitted diurnal reference.
    """
    training_silver = silver[silver["date_local"].astype(str) < cutoff].copy()
    train = build_event_features(training_silver)
    context = build_event_features(silver, baseline_reference=training_silver)
    holdout = context[context["date_local"].astype(str) >= cutoff].copy()
    return train, holdout


def temporal_holdout(
    silver: pd.DataFrame,
    *,
    holdout_fraction: float = HOLDOUT_FRACTION,
    skipped: list[dict] | None = None,
) -> list[HoldoutResult]:
    """Split raw silver before feature fitting; score later dates out of sample.

    "Flagged" means at least one detector vote — the same condition that admits
    a row to the alert table — measured before the per-day top-K cap.
    Parameters with fewer than ``MIN_EVENT_ROWS`` training rows are skipped, as
    the production build would skip them.
    """
    if not 0 < holdout_fraction < 1:
        raise ValueError("holdout_fraction must be between 0 and 1")
    if silver.empty:
        return []
    results: list[HoldoutResult] = []
    splits: dict[str, tuple[pd.DataFrame, pd.DataFrame]] = {}
    pm = silver[silver["parameter"].isin(LAYER2_PM_PARAMETERS)]

    for parameter, group in pm.groupby("parameter", sort=True):
        dates = sorted(group["date_local"].astype(str).unique())
        n_holdout = max(1, int(round(len(dates) * holdout_fraction)))
        if len(dates) - n_holdout < 1:
            if skipped is not None:
                skipped.append({"parameter": str(parameter), "reason": "fewer than two dates"})
            continue
        cutoff = dates[-n_holdout]
        if cutoff not in splits:
            splits[cutoff] = temporal_feature_split(silver, cutoff)
        train_all, holdout_all = splits[cutoff]
        train = train_all[train_all["parameter"] == parameter]
        holdout = holdout_all[holdout_all["parameter"] == parameter]
        if len(train) < MIN_EVENT_ROWS or holdout.empty:
            logger.info(
                "Holdout skipped for %s: %s training rows (need %s)",
                parameter,
                len(train),
                MIN_EVENT_ROWS,
            )
            if skipped is not None:
                skipped.append({
                    "parameter": str(parameter),
                    "reason": "insufficient training features or no held-out rows",
                    "train_rows": len(train),
                    "holdout_rows": len(holdout),
                })
            continue

        models = _fit_with_matrix(train)
        train_flagged = (
            (models["isolation_forest"].predict(models["train_scaled"]) == -1)
            | (models["lof_labels"] == -1)
            | (models["dbscan_labels"] == -1)
        )
        holdout_votes = out_of_sample_votes(models, holdout)
        flagged = holdout_votes["agreement_count"] > 0

        precision = base_rate = lift = None
        if parameter == "pm25":
            labels = weak_labels(holdout, silver)
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
                train_date_max=dates[-n_holdout - 1],
                holdout_date_min=cutoff,
            )
        )
    return results


def inject_spikes(
    silver: pd.DataFrame,
    *,
    parameter: str = INJECTION_PARAMETER,
    n_local: int = LOCAL_SPIKES,
    n_regional: int = REGIONAL_DATES,
    magnitude: float = INJECTION_MAGNITUDE,
    min_history_days: int = LAYER2_BASELINE_DAYS,
    seed: int = INJECTION_SEED,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Return ``(injected_silver, truth)`` with known spikes added.

    *Local* spikes hit one station on one day, the shape of a point source.
    *Regional* spikes hit all eligible stations on a date with at least two
    eligible locations. Each case needs ``min_history_days`` prior observed
    days and finite readings during the injection window. Truth contains only
    cases whose readings actually changed. The input frame is not modified.
    """
    if min_history_days < 0 or n_local < 0 or n_regional < 0:
        raise ValueError("history days and injection counts must be non-negative")
    if not np.isfinite(magnitude) or magnitude <= 0:
        raise ValueError("magnitude must be finite and positive")
    rng = np.random.default_rng(seed)
    injected = silver.copy()
    rows = injected[injected["parameter"] == parameter]
    skipped = {"insufficient_history": 0, "no_window_readings": 0}

    def finish(truth: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
        skipped["local_shortfall"] = max(0, n_local - int((truth["kind"] == "local").sum()))
        regional_days = truth.loc[truth["kind"] == "regional", "date_local"].nunique()
        skipped["regional_date_shortfall"] = max(0, n_regional - int(regional_days))
        truth.attrs["skipped"] = skipped
        return injected, truth

    empty_truth = pd.DataFrame(columns=[*KEY_COLUMNS, "kind"])
    if rows.empty:
        return finish(empty_truth)

    values = pd.to_numeric(rows["value"], errors="coerce")
    rows = rows[values.notna() & ~values.isin([np.inf, -np.inf])]
    station_days = rows[["locationid", "date_local"]].drop_duplicates()
    station_days = station_days.assign(date_local=station_days["date_local"].astype(str))
    station_days = station_days.sort_values(["locationid", "date_local"])
    prior_days = station_days.groupby("locationid").cumcount()
    eligible = station_days[prior_days >= min_history_days]
    skipped["insufficient_history"] = int((prior_days < min_history_days).sum())

    hours = pd.to_datetime(rows["datetime_local"], errors="coerce").dt.hour
    window_days = rows.loc[
        hours.isin(list(INJECTION_HOURS)), ["locationid", "date_local"]
    ].drop_duplicates()
    window_days = window_days.assign(date_local=window_days["date_local"].astype(str))
    with_window = eligible.merge(window_days, on=["locationid", "date_local"])
    skipped["no_window_readings"] = len(eligible) - len(with_window)
    eligible = with_window.sort_values(["locationid", "date_local"])
    if eligible.empty:
        return finish(empty_truth)

    regional_counts = eligible.groupby("date_local")["locationid"].nunique()
    eligible_dates = regional_counts[regional_counts >= 2].index.to_list()
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
    finite = pd.to_numeric(injected["value"], errors="coerce")
    target = (
        is_parameter
        & hours.isin(list(INJECTION_HOURS))
        & finite.notna()
        & ~finite.isin([np.inf, -np.inf])
        & _keys(injected.assign(parameter=parameter)).isin(_keys(truth))
    )
    boost = injected.loc[target, "locationid"].map(medians).astype(float) * magnitude
    before = injected.loc[target, "value"]
    after = before + boost
    changed = after.ne(before) & after.notna() & ~after.isin([np.inf, -np.inf])
    changed_rows = after.index[changed]
    injected.loc[changed_rows, "value"] = after.loc[changed_rows]
    truth = truth[_keys(truth).isin(_keys(injected.loc[changed_rows]))].copy()
    return finish(truth)


def injection_recall(
    silver: pd.DataFrame,
    *,
    parameter: str = INJECTION_PARAMETER,
    magnitude: float = INJECTION_MAGNITUDE,
    seed: int = INJECTION_SEED,
    **inject_kwargs,
) -> InjectionResult:
    """Inject spikes, rerun features + ensemble, and measure how many come back.

    Recall counts an injected station-day as caught when it appears in the final
    alert table, after the per-day top-K cap — the table a reviewer sees.
    ``untouched_alert_rate`` is the share of untouched rows that became alerts.
    Untouched real observations have no verified normal labels, so this is not
    a false-positive rate. The ensemble is refitted on the perturbed history.
    """
    injected, truth = inject_spikes(
        silver, parameter=parameter, magnitude=magnitude, seed=seed, **inject_kwargs
    )
    features = build_event_features(injected)
    features = features[features["parameter"] == parameter]
    counts = {kind: int((truth["kind"] == kind).sum()) for kind in ("local", "regional")}
    skipped = dict(truth.attrs.get("skipped", {}))
    skipped["unscored"] = len(truth)
    empty = InjectionResult(
        parameter, magnitude, len(truth), 0, None, None, None, None,
        injected_by_kind=counts, scored_by_kind={kind: 0 for kind in counts}, skipped=skipped,
    )
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

    untouched = features[~_keys(features).isin(_keys(truth))]
    untouched_alerts = _keys(untouched).isin(alert_keys)
    skipped["unscored"] = empty.injected - len(truth)
    return InjectionResult(
        parameter=parameter,
        magnitude=magnitude,
        injected=empty.injected,
        injected_scored=len(truth),
        recall=float(caught.mean()),
        recall_local=_recall("local"),
        recall_regional=_recall("regional"),
        untouched_alert_rate=_rate(untouched_alerts) if len(untouched) else None,
        injected_by_kind=counts,
        scored_by_kind={kind: int((truth["kind"] == kind).sum()) for kind in counts},
        skipped=skipped,
    )


def _dataset_summary(silver: pd.DataFrame) -> dict:
    """Describe and fingerprint the loaded rows independent of file/row order."""
    row_hashes = pd.util.hash_pandas_object(silver, index=False).to_numpy(dtype="uint64")
    schema = [(str(name), str(dtype)) for name, dtype in silver.dtypes.items()]
    digest = hashlib.sha256(json.dumps(schema).encode())
    digest.update(np.sort(row_hashes).astype("<u8").tobytes())
    dates = silver["date_local"].dropna().astype(str)
    return {
        "sha256": digest.hexdigest(),
        "rows": len(silver),
        "locations": sorted(int(loc) for loc in silver["locationid"].dropna().unique()),
        "date_local_min": dates.min() if len(dates) else None,
        "date_local_max": dates.max() if len(dates) else None,
        "parameter_rows": {str(p): int(n) for p, n in silver["parameter"].value_counts().items()},
    }


def run_benchmark(
    silver_root: str | Path | None = None,
    gold_root: str | Path | None = None,
) -> dict:
    """Run holdout and injection checks over current silver; write a JSON report."""
    settings = load_settings()
    silver = read_silver(silver_root or settings.silver_root)
    gold = storage.join(gold_root or settings.gold_root, "layer2")

    skipped: list[dict] = []
    holdout = temporal_holdout(silver, skipped=skipped)
    injection = injection_recall(silver)
    feature_rows = silver[silver["parameter"].isin(LAYER2_PM_PARAMETERS)][
        KEY_COLUMNS
    ].drop_duplicates()

    payload = {
        "methodology_version": METHODOLOGY_VERSION,
        "built_at_utc": datetime.now(UTC).isoformat(),
        "input_measurement_rows": int(len(silver)),
        "feature_rows": int(len(feature_rows)),
        "dataset": _dataset_summary(silver),
        "configuration": {
            "holdout_fraction": HOLDOUT_FRACTION,
            "hourly_baseline": "training dates only",
            "injection_seed": INJECTION_SEED,
            "injection_magnitude": INJECTION_MAGNITUDE,
            "local_spikes_requested": LOCAL_SPIKES,
            "regional_dates_requested": REGIONAL_DATES,
            "minimum_prior_observed_days": LAYER2_BASELINE_DAYS,
            "injection_hours_local": list(INJECTION_HOURS),
            "top_k_alerts_per_region_day": TOP_K_ALERTS_PER_DAY,
        },
        "versions": {
            "python": platform.python_version(),
            "pandas": pd.__version__,
            "numpy": np.__version__,
            "scikit_learn": sklearn.__version__,
        },
        "holdout": [asdict(item) for item in holdout],
        "holdout_skipped": skipped,
        "injection": asdict(injection),
        "limitations": [
            "PM2.5 weak labels are a heuristic, not verified pollution events.",
            "Spike recall refits on perturbed history and is measured after the top-K cap.",
            "Untouched real readings are unlabelled; their alert rate is not a false-alarm rate.",
        ],
    }
    storage.write_text(
        storage.join(gold, BENCHMARK_FILE), json.dumps(payload, indent=2, allow_nan=False) + "\n"
    )
    return payload
