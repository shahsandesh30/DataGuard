import zipfile

from scripts.package_glue import build_artifacts


def test_glue_package_contains_source_runner_and_requirements(tmp_path):
    artifacts = build_artifacts(tmp_path)

    assert [path.name for path in artifacts] == [
        "dataguard-pipelines.zip",
        "run_pipeline.py",
        "requirements-glue.txt",
    ]
    with zipfile.ZipFile(artifacts[0]) as archive:
        names = set(archive.namelist())
    assert "pipelines/__init__.py" in names
    assert "pipelines/__main__.py" in names
    assert not any("__pycache__" in name for name in names)

    runner_source = artifacts[1].read_text(encoding="utf-8")
    assert 'if __name__ == "__main__":\n    run_job()' in runner_source
    assert "sys.exit(main())" not in runner_source
