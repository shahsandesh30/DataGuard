"""The final evidence check must reject changed data and unsafe artifact paths."""

import hashlib

import pytest

from scripts.verify_snapshot import _checked_file, _model_record, verify_snapshot


def test_evidence_fingerprint_detects_a_changed_input(tmp_path):
    original = b"original frozen bytes"
    (tmp_path / "input").write_bytes(original)
    digest = hashlib.sha256(original).hexdigest()
    assert _checked_file(tmp_path, "input", digest)["bytes"] == len(original)
    (tmp_path / "input").write_bytes(b"changed frozen bytes")
    with pytest.raises(ValueError, match="fingerprint differs"):
        _checked_file(tmp_path, "input", digest)
    with pytest.raises(ValueError, match="Missing evidence"):
        _checked_file(tmp_path, "missing")


def test_model_binding_is_portable_and_reads_history_bytes_without_unpickling(tmp_path):
    relative = "layer1/history/run=20260101T000000Z/isolation_forest.joblib"
    model = tmp_path / "models" / relative
    model.parent.mkdir(parents=True)
    model.write_bytes(b"opaque model bytes, never unpickled")
    record = _model_record(tmp_path, "C:\\old-laptop\\evaluation\\models\\" + relative)
    assert record["path"] == relative
    assert record["sha256"] == hashlib.sha256(model.read_bytes()).hexdigest()
    with pytest.raises(ValueError, match="versioned model"):
        _model_record(tmp_path, "C:/old-laptop/models/layer1/isolation_forest.joblib")
    with pytest.raises(ValueError, match="escapes"):
        _model_record(tmp_path, "C:/old-laptop/models/../history/run=bad/model.joblib")


@pytest.mark.parametrize(
    "evaluation,output",
    [
        ("s3://bucket/evaluation", "local"),
        ("local", "s3://bucket/check"),
    ],
)
def test_snapshot_check_rejects_s3_before_reading_any_data(evaluation, output):
    with pytest.raises(ValueError, match="explicit local"):
        verify_snapshot(evaluation, output)


def test_snapshot_check_never_overwrites_inputs_or_existing_outputs(tmp_path):
    evaluation = tmp_path / "evaluation"
    with pytest.raises(ValueError, match="overlap"):
        verify_snapshot(evaluation, evaluation / "check")
    with pytest.raises(ValueError, match="overlap"):
        verify_snapshot(evaluation, tmp_path)
    output = tmp_path / "existing"
    output.mkdir()
    (output / "keep").write_text("existing result")
    with pytest.raises(ValueError, match="must be empty"):
        verify_snapshot(evaluation, output)
    assert (output / "keep").read_text() == "existing result"
