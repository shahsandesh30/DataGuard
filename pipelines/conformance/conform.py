"""Conform raw OpenAQ measurements into the silver zone.

Silver is one queryable table: unified schema, one unit per parameter,
consistent types, partitioned by location and date. Values are conformed but
never dropped — Layer 1 needs to see the broken readings (negative
concentrations, stuck values) to detect them.
"""

import pandas as pd

SILVER_COLUMNS = [
    "locationid",
    "sensor_id",
    "location",
    "datetime",
    "latitude",
    "longitude",
    "parameter",
    "value",
    "unit",
    "year"
]

# Target unit per parameter. Providers disagree (e.g. ppm vs µg/m³); silver
# holds exactly one unit per parameter.
CANONICAL_UNITS = {
    "pm1": "µg/m³",
    "pm25": "µg/m³",
    "relativehumidity": "%",
    "temperature": "c",
    "um003": "particles/cm³"
}


def conform_measurements(raw: pd.DataFrame) -> pd.DataFrame:
    """Return raw archive rows conformed to the silver schema.

    TODO(data engineer): rename columns, parse datetimes to UTC, convert
    units to canonical, enforce dtypes. Do NOT drop or clip invalid values —
    flag nothing here; Layer 1 does the judging.
    """
    raise NotImplementedError
