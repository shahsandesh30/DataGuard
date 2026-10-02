"""Runtime configuration shared by local and Streamlit-hosted dashboards."""

from __future__ import annotations

import os
from collections.abc import Mapping, MutableMapping
from typing import Any

SECRET_ENV_KEYS = (
    "AWS_ACCESS_KEY_ID",
    "AWS_SECRET_ACCESS_KEY",
    "AWS_SESSION_TOKEN",
    "AWS_DEFAULT_REGION",
    "AWS_REGION",
    "SILVER_ROOT",
    "GOLD_ROOT",
    "GLUE_DATABASE",
    "DATAGUARD_PUBLIC_DASHBOARD",
)


def apply_runtime_secrets(
    secrets: Mapping[str, Any], environ: MutableMapping[str, str] | None = None
) -> None:
    """Copy supported root-level Streamlit secrets into environment variables."""
    target = environ if environ is not None else os.environ
    for key in SECRET_ENV_KEYS:
        value = secrets.get(key)
        if value not in (None, ""):
            target[key] = str(value)


def is_public_dashboard(environ: Mapping[str, str] | None = None) -> bool:
    values = environ if environ is not None else os.environ
    return values.get("DATAGUARD_PUBLIC_DASHBOARD", "").strip().lower() in {
        "1",
        "true",
        "yes",
        "on",
    }
