"""Canonical unit conversion for silver measurements.

Conversions use the standard air-quality assumption of 25 °C and 1 atm
(molar volume 24.45 L/mol). Unknown parameter/unit pairs are left unchanged
so Layer 1 can still see the original reading.
"""

from __future__ import annotations

import math
import re
import unicodedata

import pandas as pd

# 25 °C, 101.325 kPa
MOLAR_VOLUME_L_PER_MOL = 24.45

# g/mol — used only for gas ppm/ppb ↔ µg/m³ conversion.
MOLECULAR_WEIGHTS = {
    "o3": 48.00,
    "no2": 46.0055,
    "so2": 64.066,
    "co": 28.0101,
    "no": 30.0061,
    "nox": 46.0055,  # conventionally expressed as NO2-equivalent
}

CANONICAL_UNITS = {
    "pm25": "µg/m³",
    "pm10": "µg/m³",
    "pm1": "µg/m³",
    "bc": "µg/m³",
    "o3": "µg/m³",
    "no2": "µg/m³",
    "so2": "µg/m³",
    "co": "µg/m³",
    "no": "µg/m³",
    "nox": "µg/m³",
}

PARAM_ALIASES = {
    "pm2.5": "pm25",
    "pm2_5": "pm25",
}


def canonical_parameter(parameter: str) -> str:
    name = str(parameter).strip().lower()
    return PARAM_ALIASES.get(name, name)


def _fold_micro(text: str) -> str:
    return text.replace("µ", "u").replace("μ", "u")


def normalize_unit(unit: str) -> str:
    """Collapse provider spelling variants to a small set of tokens."""
    if unit is None or (isinstance(unit, float) and math.isnan(unit)):
        return ""
    text = unicodedata.normalize("NFKC", str(unit)).strip().lower()
    text = _fold_micro(text)
    text = text.replace("³", "3").replace("²", "2")
    text = re.sub(r"[\s_\-]+", "", text)
    aliases = {
        "ugm3": "ug/m3",
        "ug/m3": "ug/m3",
        "ugm^3": "ug/m3",
        "microgramsm3": "ug/m3",
        "microgramspercubicmeter": "ug/m3",
        "mgm3": "mg/m3",
        "mg/m3": "mg/m3",
        "ppmv": "ppm",
        "ppbv": "ppb",
        "ppm": "ppm",
        "ppb": "ppb",
    }
    return aliases.get(text, text)


def _ppm_to_ugm3(value: float, parameter: str) -> float:
    mw = MOLECULAR_WEIGHTS[parameter]
    return value * mw * (1000.0 / MOLAR_VOLUME_L_PER_MOL)


def convert_value(parameter: str, unit: str, value: float) -> tuple[float, str]:
    """Return ``(value_in_canonical_unit, canonical_unit)``.

    If the parameter has no canonical unit, or the source unit cannot be
    converted, the original value and unit are returned unchanged.
    """
    param = canonical_parameter(parameter)
    target = CANONICAL_UNITS.get(param)
    token = normalize_unit(unit)
    if value is None or (isinstance(value, float) and math.isnan(value)):
        return value, target or (unit or "")
    if target is None:
        return value, unit or ""
    if token == "ug/m3":
        return value, target
    if token == "mg/m3":
        return value * 1000.0, target
    if param not in MOLECULAR_WEIGHTS:
        return value, unit or ""
    if token == "ppm":
        return _ppm_to_ugm3(value, param), target
    if token == "ppb":
        return _ppm_to_ugm3(value / 1000.0, param), target
    return value, unit or ""


def convert_series(
    parameter: pd.Series, unit: pd.Series, value: pd.Series
) -> tuple[pd.Series, pd.Series]:
    """Vectorized conversion; unique (parameter, unit) pairs are few."""
    converted_values = value.astype("float64").copy()
    converted_units = pd.Series([""] * len(value), index=value.index, dtype="string")
    pairs = pd.DataFrame({"parameter": parameter, "unit": unit}).drop_duplicates()
    for _, row in pairs.iterrows():
        mask = (parameter == row["parameter"]) & (unit == row["unit"])
        subset = value[mask]
        if subset.empty:
            continue
        new_values = []
        new_units = []
        for raw in subset.tolist():
            new_val, new_unit = convert_value(row["parameter"], row["unit"], raw)
            new_values.append(new_val)
            new_units.append(new_unit)
        converted_values.loc[mask] = new_values
        converted_units.loc[mask] = new_units
    return converted_values, converted_units
