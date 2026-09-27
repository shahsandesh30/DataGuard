from datetime import UTC, date, datetime

import pytest

from glue import run_pipeline as entry


def _capture(store: list[list[str]]):
    """Stand in for the CLI, recording the argv it was handed."""

    def fake(argv: list[str]) -> int:
        store.append(argv)
        return 0

    return fake


def test_trailing_window_ends_before_the_publication_lag():
    start, end = trailing = entry.trailing_window(date(2026, 9, 24), lag_days=3, window_days=7)
    assert end == date(2026, 9, 21)
    assert start == date(2026, 9, 15)
    assert trailing == (start, end)


def test_trailing_window_is_inclusive():
    start, end = entry.trailing_window(date(2026, 9, 24), lag_days=0, window_days=1)
    assert start == end == date(2026, 9, 24)

    start, end = entry.trailing_window(date(2026, 9, 24), lag_days=0, window_days=7)
    assert (end - start).days == 6  # 7 days counted inclusively


def test_trailing_window_rejects_a_window_of_no_days():
    with pytest.raises(ValueError, match="window_days"):
        entry.trailing_window(date(2026, 9, 24), window_days=0)


def test_locations_accept_glue_comma_separated_argument():
    assert entry.parse_locations(["1544061,1601414,2455394,6430870"]) == [
        1544061,
        1601414,
        2455394,
        6430870,
    ]


def test_build_pipeline_argv_runs_every_stage_over_the_window():
    argv = entry.build_pipeline_argv(date(2023, 1, 1), date(2023, 1, 31))
    assert argv == ["run", "--start", "2023-01-01", "--end", "2023-01-31"]


def test_build_pipeline_argv_passes_locations_and_roots():
    argv = entry.build_pipeline_argv(
        date(2023, 1, 1),
        date(2023, 1, 2),
        locations=[2178, 1544061],
        bronze_root="s3://bronze",
        gold_root="s3://gold",
    )
    assert argv[argv.index("--locations") + 1 : argv.index("--locations") + 3] == [
        "2178",
        "1544061",
    ]
    assert "--bronze-root" in argv and "s3://bronze" in argv
    assert "--gold-root" in argv and "s3://gold" in argv
    # Unset roots stay out of argv so the CLI resolves them from .env.
    assert "--silver-root" not in argv


def test_main_forwards_an_explicit_window(monkeypatch):
    calls: list[list[str]] = []
    monkeypatch.setattr(entry, "run_pipeline", _capture(calls))

    assert entry.main(["--start", "2023-01-01", "--end", "2023-01-31"]) == 0
    assert calls == [
        [
            "run",
            "--start",
            "2023-01-01",
            "--end",
            "2023-01-31",
            "--locations",
            "1544061",
            "1601414",
            "2455394",
            "6430870",
        ]
    ]


def test_main_computes_a_window_when_none_is_given(monkeypatch):
    calls: list[list[str]] = []
    monkeypatch.setattr(entry, "run_pipeline", _capture(calls))

    entry.main(["--lag-days", "0", "--window-days", "1"])

    today = datetime.now(UTC).date().isoformat()
    assert calls == [
        [
            "run",
            "--start",
            today,
            "--end",
            today,
            "--locations",
            "1544061",
            "1601414",
            "2455394",
            "6430870",
        ]
    ]


def test_main_ignores_arguments_injected_by_the_scheduler(monkeypatch):
    calls: list[list[str]] = []
    monkeypatch.setattr(entry, "run_pipeline", _capture(calls))

    # Glue passes --JOB_NAME on every run; it must not abort the pipeline.
    entry.main(["--JOB_NAME", "dataguard-daily", "--start", "2023-01-01", "--end", "2023-01-01"])

    assert calls == [
        [
            "run",
            "--start",
            "2023-01-01",
            "--end",
            "2023-01-01",
            "--locations",
            "1544061",
            "1601414",
            "2455394",
            "6430870",
        ]
    ]


def test_main_sets_glue_database_for_storage(monkeypatch):
    captured = {}

    def capture_database(_argv):
        captured["database"] = entry.os.environ.get("GLUE_DATABASE")
        return 0

    monkeypatch.setattr(entry, "run_pipeline", capture_database)
    monkeypatch.delenv("GLUE_DATABASE", raising=False)

    entry.main(
        [
            "--start",
            "2023-01-01",
            "--end",
            "2023-01-01",
            "--glue-database",
            "demo_catalog",
        ]
    )

    assert captured["database"] == "demo_catalog"
    assert "GLUE_DATABASE" not in entry.os.environ


def test_main_rejects_half_a_window(monkeypatch):
    monkeypatch.setattr(entry, "run_pipeline", _capture([]))
    with pytest.raises(SystemExit):
        entry.main(["--start", "2023-01-01"])


def test_main_rejects_a_backwards_window(monkeypatch):
    monkeypatch.setattr(entry, "run_pipeline", _capture([]))
    with pytest.raises(SystemExit):
        entry.main(["--start", "2023-01-31", "--end", "2023-01-01"])
