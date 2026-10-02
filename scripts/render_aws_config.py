"""Render account-specific IAM policies and the Glue job definition."""

from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any

DEFAULT_OUTPUT = Path(__file__).resolve().parents[1] / "build" / "aws"
GLUE_JOB_NAME = "dataguard-daily"
GLUE_ROLE_NAME = "DataGuardGlueRole"
SCHEDULER_ROLE_NAME = "DataGuardSchedulerRole"


def _statement(effect: str, actions: list[str], resources: list[str], **extra: Any) -> dict:
    return {"Effect": effect, "Action": actions, "Resource": resources, **extra}


def render_configs(
    account_id: str,
    bucket: str,
    *,
    region: str = "ap-southeast-2",
    output_dir: Path = DEFAULT_OUTPUT,
) -> list[Path]:
    output_dir.mkdir(parents=True, exist_ok=True)
    glue_role_arn = f"arn:aws:iam::{account_id}:role/{GLUE_ROLE_NAME}"
    job_arn = f"arn:aws:glue:{region}:{account_id}:job/{GLUE_JOB_NAME}"
    bucket_arn = f"arn:aws:s3:::{bucket}"
    data_arns = [f"{bucket_arn}/{prefix}/*" for prefix in ("bronze", "silver", "gold", "artifacts")]

    trust = lambda service: {  # noqa: E731 - keeps the two trust documents visibly identical
        "Version": "2012-10-17",
        "Statement": [
            {
                "Effect": "Allow",
                "Principal": {"Service": service},
                "Action": "sts:AssumeRole",
            }
        ],
    }
    configs: dict[str, dict] = {
        "glue-role-trust.json": trust("glue.amazonaws.com"),
        "scheduler-role-trust.json": trust("scheduler.amazonaws.com"),
        "glue-role-policy.json": {
            "Version": "2012-10-17",
            "Statement": [
                _statement(
                    "Allow",
                    ["s3:ListBucket", "s3:GetBucketLocation"],
                    [bucket_arn],
                    Condition={
                        "StringLike": {
                            "s3:prefix": ["bronze/*", "silver/*", "gold/*", "artifacts/*"]
                        }
                    },
                ),
                _statement(
                    "Allow",
                    ["s3:GetObject", "s3:PutObject", "s3:DeleteObject"],
                    data_arns,
                ),
                _statement(
                    "Allow",
                    [
                        "glue:GetDatabase",
                        "glue:GetDatabases",
                        "glue:CreateDatabase",
                        "glue:GetTable",
                        "glue:GetTables",
                        "glue:CreateTable",
                        "glue:UpdateTable",
                        "glue:DeleteTable",
                        "glue:GetPartition",
                        "glue:GetPartitions",
                        "glue:BatchCreatePartition",
                        "glue:UpdatePartition",
                        "glue:DeletePartition",
                        "glue:BatchDeletePartition",
                    ],
                    [
                        f"arn:aws:glue:{region}:{account_id}:catalog",
                        f"arn:aws:glue:{region}:{account_id}:database/dataguard",
                        f"arn:aws:glue:{region}:{account_id}:table/dataguard/*",
                    ],
                ),
                _statement(
                    "Allow",
                    ["logs:CreateLogGroup"],
                    ["*"],
                ),
                _statement(
                    "Allow",
                    ["logs:CreateLogStream", "logs:PutLogEvents"],
                    [f"arn:aws:logs:{region}:{account_id}:log-group:/aws-glue/jobs/*:*"],
                ),
            ],
        },
        "scheduler-role-policy.json": {
            "Version": "2012-10-17",
            "Statement": [_statement("Allow", ["glue:StartJobRun"], [job_arn])],
        },
        "dashboard-read-policy.json": {
            "Version": "2012-10-17",
            "Statement": [
                _statement(
                    "Allow",
                    ["s3:ListBucket", "s3:GetBucketLocation"],
                    [bucket_arn],
                    Condition={"StringLike": {"s3:prefix": ["silver/*", "gold/*"]}},
                ),
                _statement(
                    "Allow",
                    ["s3:GetObject"],
                    [f"{bucket_arn}/silver/*", f"{bucket_arn}/gold/*"],
                ),
            ],
        },
        "bucket-lifecycle.json": {
            "Rules": [
                {
                    "ID": "expire-noncurrent-after-30-days",
                    "Status": "Enabled",
                    "Filter": {"Prefix": ""},
                    "NoncurrentVersionExpiration": {"NoncurrentDays": 30},
                }
            ]
        },
        "glue-failure-event-pattern.json": {
            "source": ["aws.glue"],
            "detail-type": ["Glue Job State Change"],
            "detail": {
                "jobName": [GLUE_JOB_NAME],
                "state": ["FAILED", "TIMEOUT", "STOPPED"],
            },
        },
        "glue-job.json": {
            "Name": GLUE_JOB_NAME,
            "Role": glue_role_arn,
            "Command": {
                "Name": "glueetl",
                "ScriptLocation": f"s3://{bucket}/artifacts/run_pipeline.py",
                "PythonVersion": "3",
            },
            "GlueVersion": "5.1",
            "WorkerType": "G.1X",
            "NumberOfWorkers": 2,
            "Timeout": 30,
            "MaxRetries": 0,
            "ExecutionProperty": {"MaxConcurrentRuns": 1},
            "DefaultArguments": {
                "--job-language": "python",
                "--enable-continuous-cloudwatch-log": "true",
                "--extra-py-files": f"s3://{bucket}/artifacts/dataguard-pipelines.zip",
                "--additional-python-modules": f"s3://{bucket}/artifacts/requirements-glue.txt",
                "--python-modules-installer-option": "-r",
                "--bronze-root": f"s3://{bucket}/bronze",
                "--silver-root": f"s3://{bucket}/silver",
                "--gold-root": f"s3://{bucket}/gold",
                "--glue-database": "dataguard",
                "--locations": "1544061,1601414,2455394,6430870",
                "--lag-days": "3",
                "--window-days": "7",
            },
        },
    }

    rendered: list[Path] = []
    for name, payload in configs.items():
        target = output_dir / name
        target.write_text(json.dumps(payload, indent=2) + "\n", encoding="utf-8")
        rendered.append(target)
    return rendered


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--account-id", required=True)
    parser.add_argument("--bucket", required=True)
    parser.add_argument("--region", default="ap-southeast-2")
    parser.add_argument("--output", type=Path, default=DEFAULT_OUTPUT)
    args = parser.parse_args(argv)
    for path in render_configs(
        args.account_id,
        args.bucket,
        region=args.region,
        output_dir=args.output.resolve(),
    ):
        print(path)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
