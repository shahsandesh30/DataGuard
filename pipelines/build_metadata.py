"""Shared helpers for machine-readable stage build summaries."""

from __future__ import annotations

import json
from dataclasses import asdict
from datetime import UTC, datetime
from typing import Any


def build_summary_json(result: Any, **inputs: Any) -> str:
    """Serialize a stage result with its completion time and input counts."""
    payload = {
        "built_at_utc": datetime.now(UTC).isoformat(),
        **inputs,
        **asdict(result),
    }
    return json.dumps(payload, indent=2) + "\n"
