"""Build the three artifacts uploaded for the DataGuard Glue job."""

from __future__ import annotations

import argparse
import shutil
import zipfile
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_OUTPUT = REPO_ROOT / "build" / "glue"


def build_artifacts(output_dir: Path = DEFAULT_OUTPUT) -> list[Path]:
    """Create the source ZIP and copy the runner and pinned requirements."""
    output_dir.mkdir(parents=True, exist_ok=True)
    source_zip = output_dir / "dataguard-pipelines.zip"

    with zipfile.ZipFile(source_zip, "w", compression=zipfile.ZIP_DEFLATED) as archive:
        for source in sorted((REPO_ROOT / "pipelines").rglob("*.py")):
            if "__pycache__" not in source.parts:
                archive.write(source, source.relative_to(REPO_ROOT).as_posix())

    runner = output_dir / "run_pipeline.py"
    requirements = output_dir / "requirements-glue.txt"
    shutil.copy2(REPO_ROOT / "glue" / "run_pipeline.py", runner)
    shutil.copy2(REPO_ROOT / "requirements-glue.txt", requirements)
    return [source_zip, runner, requirements]


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", type=Path, default=DEFAULT_OUTPUT)
    args = parser.parse_args(argv)
    for artifact in build_artifacts(args.output.resolve()):
        print(artifact)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
