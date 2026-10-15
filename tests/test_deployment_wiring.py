"""Rendered Glue configuration must reach the ordinary pipeline CLI unchanged."""

import json

from glue import run_pipeline as entry
from pipelines import __main__ as pipeline_cli
from pipelines.config import load_settings
from scripts.render_aws_config import render_configs


def test_rendered_job_forwards_station_defaults_and_roots(tmp_path, monkeypatch):
    render_configs("123456789012", "dataguard-demo", output_dir=tmp_path)
    job = json.loads((tmp_path / "glue-job.json").read_text(encoding="utf-8"))
    defaults = job["DefaultArguments"]
    captured = {}

    def capture_pipeline(argv):
        captured["args"] = pipeline_cli._parse_args(argv)
        captured["database"] = load_settings().glue_database
        return 0

    monkeypatch.setattr(entry, "run_pipeline", capture_pipeline)
    argv = ["--start", "2026-01-01", "--end", "2026-01-31"]
    for flag in (
        "--locations",
        "--bronze-root",
        "--silver-root",
        "--gold-root",
        "--models-root",
        "--glue-database",
    ):
        argv.extend([flag, defaults[flag]])

    assert entry.main(argv) == 0
    args = captured["args"]
    assert args.command == "run"
    assert args.locations == entry.DEFAULT_LOCATIONS
    for name in ("bronze", "silver", "gold", "models"):
        assert getattr(args, f"{name}_root") == defaults[f"--{name}-root"]
    assert captured["database"] == defaults["--glue-database"]
