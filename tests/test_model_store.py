"""Trained models land under MODELS_ROOT, local or S3, as latest plus history.

The S3 branch runs against a fake boto3 client, so no credentials are needed.
"""

import io
from pathlib import Path

import boto3
from sklearn.ensemble import IsolationForest

from pipelines import __main__ as pipeline_cli
from pipelines import model_store, storage


def _artifact() -> dict:
    return {"model": IsolationForest(n_estimators=5, random_state=0).fit([[0.0], [1.0]])}


def test_local_save_writes_latest_and_history(tmp_path):
    saved = model_store.save_model(
        _artifact(), "layer1", "isolation_forest", run_id="20260101T000000Z", models_root=tmp_path
    )

    assert saved == str(
        tmp_path / "layer1" / "history" / "run=20260101T000000Z" / "isolation_forest.joblib"
    )
    assert (tmp_path / "layer1" / "isolation_forest.joblib").exists()
    loaded = model_store.load_model("layer1", "isolation_forest", models_root=str(tmp_path))
    assert isinstance(loaded["model"], IsolationForest)


def test_latest_is_replaced_and_history_is_kept(tmp_path):
    for run_id in ("20260101T000000Z", "20260102T000000Z"):
        artifact = _artifact()
        artifact["run"] = run_id
        model_store.save_model(
            artifact, "layer2", "ensemble_pm25", run_id=run_id, models_root=tmp_path
        )

    latest = model_store.load_model("layer2", "ensemble_pm25", models_root=tmp_path)
    first = model_store.load_model(
        "layer2", "ensemble_pm25", run_id="20260101T000000Z", models_root=tmp_path
    )
    assert latest["run"] == "20260102T000000Z"
    assert first["run"] == "20260101T000000Z"


def test_load_returns_none_before_any_save(tmp_path):
    assert model_store.load_model("layer1", "isolation_forest", models_root=tmp_path) is None


def test_models_root_comes_from_env(tmp_path, monkeypatch):
    monkeypatch.setenv("MODELS_ROOT", str(tmp_path / "from-env"))
    model_store.save_model(_artifact(), "layer1", "isolation_forest", run_id="r1")
    assert (tmp_path / "from-env" / "layer1" / "isolation_forest.joblib").exists()


class _FakeS3Client:
    def __init__(self):
        self.objects: dict[tuple[str, str], bytes] = {}

    def put_object(self, Bucket, Key, Body):
        self.objects[(Bucket, Key)] = Body

    def get_object(self, Bucket, Key):
        return {"Body": io.BytesIO(self.objects[(Bucket, Key)])}


def test_s3_save_and_load_round_trip(monkeypatch):
    client = _FakeS3Client()
    monkeypatch.setattr(boto3, "client", lambda service, **kwargs: client)
    monkeypatch.setattr(
        storage,
        "_wr",
        lambda: type(
            "wr",
            (),
            {
                "s3": type(
                    "s3",
                    (),
                    {
                        "does_object_exist": staticmethod(
                            lambda path: tuple(path[5:].split("/", 1)) in client.objects
                        )
                    },
                )
            },
        ),
    )

    saved = model_store.save_model(
        _artifact(), "layer1", "isolation_forest", run_id="r1", models_root="s3://demo/models"
    )

    assert saved == "s3://demo/models/layer1/history/run=r1/isolation_forest.joblib"
    assert set(client.objects) == {
        ("demo", "models/layer1/history/run=r1/isolation_forest.joblib"),
        ("demo", "models/layer1/isolation_forest.joblib"),
    }
    loaded = model_store.load_model("layer1", "isolation_forest", models_root="s3://demo/models")
    assert isinstance(loaded["model"], IsolationForest)


def test_cli_passes_models_root_to_both_layers(monkeypatch):
    calls: dict[str, object] = {}

    class _Result:
        def __getattr__(self, name):
            return {} if name == "model_paths" else 0

    def fake_quality(**kwargs):
        calls["quality"] = kwargs["models_root"]
        return _Result()

    def fake_detection(**kwargs):
        calls["detect"] = kwargs["models_root"]
        return _Result()

    monkeypatch.setattr(pipeline_cli, "build_quality", fake_quality)
    monkeypatch.setattr(pipeline_cli, "build_detection", fake_detection)

    for stage in ("quality", "detect"):
        assert pipeline_cli.main([stage, "--models-root", "s3://demo/models"]) == 0
    assert calls == {"quality": "s3://demo/models", "detect": "s3://demo/models"}


def test_cli_falls_back_to_models_root_env(monkeypatch, tmp_path):
    monkeypatch.setenv("MODELS_ROOT", str(Path(tmp_path)))
    seen = {}

    class _Result:
        def __getattr__(self, name):
            return None if name == "model_path" else 0

    def fake_quality(**kwargs):
        seen["root"] = kwargs["models_root"]
        return _Result()

    monkeypatch.setattr(pipeline_cli, "build_quality", fake_quality)
    pipeline_cli.main(["quality"])
    assert seen["root"] == str(tmp_path)
