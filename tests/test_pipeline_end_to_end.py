"""End-to-end: a bronze file on disk must reach the gold zone through silver.

These tests are the regression guard for the wiring between stages, not for the
detection logic inside any one of them.
"""

import gzip
import json
from datetime import date
from pathlib import Path

import pytest

from pipelines import __main__ as pipeline_cli
from pipelines import storage
from pipelines.conformance.conform import build_silver, read_silver
from pipelines.fusion.build import build_fusion
from pipelines.ingestion.fetch import MANIFEST_FILENAME, bronze_key
from pipelines.quality.build import build_quality, read_quality_incidents, read_quality_metrics

# Real OpenAQ archive files name the column location_id, not locationid.
ARCHIVE_HEADER = "location_id,sensors_id,location,datetime,lat,lon,parameter,units,value\n"


def _archive_day(bronze_root, locationid: int, day: date, values: list[float]) -> None:
    """Write one gzipped archive file, one hourly pm25 reading per value."""
    rows = "".join(
        f"{locationid},{locationid}01,Station-{locationid},"
        f"{day.isoformat()}T{hour:02d}:00:00+00:00,"
        f"-33.87,151.21,pm25,µg/m³,{value}\n"
        for hour, value in enumerate(values)
    )
    path = bronze_root / bronze_key(locationid, day)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(gzip.compress((ARCHIVE_HEADER + rows).encode("utf-8")))


def test_archive_column_naming_conforms(tmp_path):
    """location_id must fold to locationid, or silver silently builds empty."""
    bronze, silver = tmp_path / "bronze", tmp_path / "silver"
    _archive_day(bronze, 1544061, date(2026, 1, 1), [10.0] * 24)

    result = build_silver(bronze_root=bronze, silver_root=silver)
    assert result.files_failed == 0
    assert result.rows == 24

    frame = read_silver(silver)
    assert set(frame["locationid"]) == {1544061}
    assert frame["value"].notna().all()


def test_bronze_reaches_gold_through_silver(tmp_path):
    bronze, silver, gold = tmp_path / "bronze", tmp_path / "silver", tmp_path / "gold"

    # Day 1 healthy; day 2 stuck at one value for the whole day.
    _archive_day(bronze, 1544061, date(2026, 1, 1), [10.0 + h * 0.5 for h in range(24)])
    _archive_day(bronze, 1544061, date(2026, 1, 2), [7.0] * 24)

    build_silver(bronze_root=bronze, silver_root=silver)
    result = build_quality(
        silver_root=silver, bronze_root=bronze, gold_root=gold, models_root=tmp_path / "models"
    )

    assert result.station_day_rows == 2
    metrics = read_quality_metrics(gold)
    assert set(metrics["date_local"]) == {"2026-01-01", "2026-01-02"}

    stuck_day = metrics[metrics["date_local"] == "2026-01-02"].iloc[0]
    assert stuck_day["max_stuck_run_max"] == 24
    assert bool(stuck_day["file_present"])

    incidents = read_quality_incidents(gold)
    fired = set(incidents[incidents["date_local"] == "2026-01-02"]["rule_id"])
    assert "R2" in fired  # stuck sensor
    assert not incidents[incidents["date_local"] == "2026-01-01"]["rule_id"].tolist()


def test_stages_accept_string_roots(tmp_path):
    """The CLI passes roots as strings, so every stage must accept them.

    Tests that only ever passed Path objects hid a `str / str` crash in the
    build-summary write.
    """
    bronze = str(tmp_path / "bronze")
    silver = str(tmp_path / "silver")
    gold = str(tmp_path / "gold")
    _archive_day(Path(bronze), 1544061, date(2026, 1, 1), [10.0] * 24)

    assert build_silver(bronze_root=bronze, silver_root=silver).rows == 24
    assert (Path(silver) / "_build.json").exists()

    result = build_quality(
        silver_root=silver, bronze_root=bronze, gold_root=gold, models_root=tmp_path / "models"
    )
    assert result.station_day_rows == 1
    assert (Path(gold) / "layer1" / "_build.json").exists()
    quality_summary = json.loads(
        (Path(gold) / "layer1" / "_build.json").read_text(encoding="utf-8")
    )
    assert quality_summary["input_measurement_rows"] == 24
    assert quality_summary["built_at_utc"].endswith("+00:00")

    assert build_fusion(gold_root=gold).alert_rows == 0
    assert (Path(gold) / "fusion" / "_build.json").exists()
    fusion_summary = json.loads(
        (Path(gold) / "fusion" / "_build.json").read_text(encoding="utf-8")
    )
    assert fusion_summary["input_event_alert_rows"] == 0
    assert fusion_summary["built_at_utc"].endswith("+00:00")


def test_quality_without_silver_produces_empty_gold(tmp_path):
    """A missing upstream zone yields empty tables, not a crash."""
    result = build_quality(
        silver_root=tmp_path / "silver",
        bronze_root=tmp_path / "bronze",
        gold_root=tmp_path / "gold",
        models_root=tmp_path / "models",
    )
    assert result.station_day_rows == 0
    assert result.rule_incidents == 0
    assert not result.model_trained
    assert read_quality_metrics(tmp_path / "gold").empty


@pytest.mark.parametrize("as_string", [False, True])
def test_manifest_missing_day_reaches_gold_and_retry_recovers(tmp_path, as_string):
    bronze, silver, gold = [tmp_path / zone for zone in ("bronze", "silver", "gold")]
    roots = [str(root) if as_string else root for root in (bronze, silver, gold)]
    _archive_day(bronze, 1544061, date(2026, 1, 1), [10 + h for h in range(24)])
    _archive_day(bronze, 1544061, date(2026, 1, 3), [10 + h for h in range(24)])
    manifest_uri = storage.join(roots[0], MANIFEST_FILENAME)
    record = {"locationid": 1544061, "day": "2026-01-02", "status": "missing"}
    storage.write_text(manifest_uri, json.dumps(record) + "\n")

    def rebuild():
        build_silver(bronze_root=roots[0], silver_root=roots[1])
        return build_quality(bronze_root=roots[0], silver_root=roots[1], gold_root=roots[2],
                             models_root=tmp_path / "models")

    assert rebuild().station_day_rows == 3
    metrics = read_quality_metrics(roots[2]).set_index("date_local")
    assert metrics.loc["2026-01-02", "total_readings"] == 0
    incidents = read_quality_incidents(roots[2])
    assert set(incidents[incidents["date_local"] == "2026-01-02"]["rule_id"]) == {
        "R4", "R5", "R7",
    }

    _archive_day(bronze, 1544061, date(2026, 1, 2), [10 + h for h in range(24)])
    record.update(status="copied", arrived_at="2026-01-04T00:00:00+00:00")
    storage.append_text(manifest_uri, json.dumps(record) + "\n")
    assert rebuild().station_day_rows == 3
    metrics = read_quality_metrics(roots[2]).set_index("date_local")
    assert metrics.loc["2026-01-02", "total_readings"] == 24
    assert metrics.loc["2026-01-02", "file_lateness_hours"] == 0
    assert read_quality_incidents(roots[2]).empty


def test_manifest_only_missing_days_reach_gold_without_silver(tmp_path):
    bronze, gold = tmp_path / "bronze", tmp_path / "gold"
    record = {"locationid": 1544061, "day": "2026-01-02", "status": "missing"}
    storage.write_text(storage.join(bronze, MANIFEST_FILENAME), json.dumps(record) + "\n")
    result = build_quality(bronze_root=bronze, silver_root=tmp_path / "empty-silver",
                           gold_root=gold, models_root=tmp_path / "models")
    assert result.station_day_rows == 1
    assert result.sensor_day_rows == 0
    assert not result.model_trained
    assert set(read_quality_incidents(gold)["rule_id"]) == {"R4", "R7"}


def test_full_run_stops_after_a_failed_stage(monkeypatch):
    calls: list[str] = []

    def stage(name: str, exit_code: int = 0):
        def run(_args):
            calls.append(name)
            return exit_code

        return run

    monkeypatch.setitem(pipeline_cli.COMMANDS, "ingest", stage("ingest"))
    monkeypatch.setitem(pipeline_cli.COMMANDS, "conform", stage("conform", 1))
    monkeypatch.setitem(pipeline_cli.COMMANDS, "quality", stage("quality"))
    monkeypatch.setitem(pipeline_cli.COMMANDS, "detect", stage("detect"))
    monkeypatch.setitem(pipeline_cli.COMMANDS, "fuse", stage("fuse"))

    assert pipeline_cli.main(["run", "--start", "2026-01-01", "--end", "2026-01-01"]) == 1
    assert calls == ["ingest", "conform"]


def test_conform_command_rejects_an_empty_bronze_root(tmp_path):
    bronze = tmp_path / "bronze"
    silver = tmp_path / "silver"

    assert pipeline_cli.main(
        ["conform", "--bronze-root", str(bronze), "--silver-root", str(silver)]
    ) == 1
    assert not silver.exists()
