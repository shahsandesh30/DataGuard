import json

from scripts.render_aws_config import render_configs


def test_rendered_aws_config_is_scoped_and_single_concurrency(tmp_path):
    render_configs("123456789012", "dataguard-demo", output_dir=tmp_path)

    job = json.loads((tmp_path / "glue-job.json").read_text(encoding="utf-8"))
    dashboard = json.loads(
        (tmp_path / "dashboard-read-policy.json").read_text(encoding="utf-8")
    )
    glue_policy = json.loads(
        (tmp_path / "glue-role-policy.json").read_text(encoding="utf-8")
    )

    assert job["GlueVersion"] == "5.1"
    assert job["ExecutionProperty"]["MaxConcurrentRuns"] == 1
    assert job["DefaultArguments"]["--locations"] == (
        "1544061,1601414,2455394,6430870"
    )
    glue_actions = {
        action
        for statement in glue_policy["Statement"]
        for action in statement["Action"]
    }
    assert "glue:BatchCreatePartition" in glue_actions
    assert "s3:PutObject" not in json.dumps(dashboard)
    assert "arn:aws:s3:::dataguard-demo/gold/*" in json.dumps(dashboard)
