"""Trained model artifacts, saved to a local directory or an S3 prefix.

Both layers re-fit their models on every run. Each fit is written twice under
``MODELS_ROOT``::

    <root>/<layer>/<name>.joblib                       latest, overwritten
    <root>/<layer>/history/run=<run_id>/<name>.joblib  kept, one per run

On a scheduled Glue run the local filesystem disappears with the job, so the
history copy is the only record of which model scored a given run's alerts.
"""

from __future__ import annotations

import io
from datetime import UTC, datetime
from pathlib import Path

import joblib

from pipelines import storage
from pipelines.config import load_settings


def new_run_id() -> str:
    """A sortable UTC timestamp identifying one training run."""
    return datetime.now(UTC).strftime("%Y%m%dT%H%M%SZ")


def latest_path(models_root: str | Path, layer: str, name: str) -> str:
    return storage.join(models_root, layer, f"{name}.joblib")


def history_path(models_root: str | Path, layer: str, name: str, run_id: str) -> str:
    return storage.join(models_root, layer, "history", f"run={run_id}", f"{name}.joblib")


def save_model(
    artifact: dict,
    layer: str,
    name: str,
    *,
    run_id: str,
    models_root: str | Path | None = None,
) -> str:
    """Write ``artifact`` as latest and into history; return the history path."""
    root = models_root or load_settings().models_root
    buffer = io.BytesIO()
    joblib.dump(artifact, buffer)
    payload = buffer.getvalue()

    versioned = history_path(root, layer, name, run_id)
    storage.write_bytes(versioned, payload)
    storage.write_bytes(latest_path(root, layer, name), payload)
    return versioned


def load_model(
    layer: str,
    name: str,
    *,
    run_id: str | None = None,
    models_root: str | Path | None = None,
) -> dict | None:
    """Load the latest artifact, or one run's, or ``None`` if never saved."""
    root = models_root or load_settings().models_root
    target = latest_path(root, layer, name) if run_id is None else history_path(
        root, layer, name, run_id
    )
    payload = storage.read_bytes(target)
    return None if payload is None else joblib.load(io.BytesIO(payload))
