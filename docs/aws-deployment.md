# AWS + Streamlit demo deployment

This is the manual first deployment. It creates one private S3 lake, one Glue
5.1 job, one daily schedule, failure email notifications, and a read-only
identity for Streamlit Community Cloud. Terraform and GitHub-to-AWS deployment
are deliberately deferred.

Reference documentation: [Glue 5.1 runtime](https://docs.aws.amazon.com/glue/latest/dg/release-notes.html),
[Glue requirements files](https://docs.aws.amazon.com/glue/latest/dg/aws-glue-programming-python-libraries.html),
[EventBridge Scheduler targets](https://docs.aws.amazon.com/scheduler/latest/UserGuide/managing-targets.html),
[Streamlit deployment](https://docs.streamlit.io/deploy/streamlit-community-cloud/deploy-your-app/deploy),
and [Streamlit secrets](https://docs.streamlit.io/deploy/streamlit-community-cloud/deploy-your-app/secrets-management).

## 1. Choose names and render account-specific files

Configure an AWS CLI profile first, then run from the repository root:

```powershell
$Region = "ap-southeast-2"
$AccountId = aws sts get-caller-identity --query Account --output text
$Bucket = "dataguard-$AccountId-$Region"
venv\Scripts\python.exe scripts\render_aws_config.py `
  --account-id $AccountId --bucket $Bucket --region $Region
```

All generated JSON is under `build/aws/` and is gitignored.

## 2. Create the private versioned lake

```powershell
aws s3api create-bucket --bucket $Bucket --region $Region `
  --create-bucket-configuration LocationConstraint=$Region
aws s3api put-public-access-block --bucket $Bucket `
  --public-access-block-configuration `
  BlockPublicAcls=true,IgnorePublicAcls=true,BlockPublicPolicy=true,RestrictPublicBuckets=true
aws s3api put-bucket-versioning --bucket $Bucket `
  --versioning-configuration Status=Enabled
aws s3api put-bucket-lifecycle-configuration --bucket $Bucket `
  --lifecycle-configuration file://build/aws/bucket-lifecycle.json
aws glue create-database --database-input '{"Name":"dataguard"}' --region $Region
```

## 3. Create the Glue role and job

```powershell
aws iam create-role --role-name DataGuardGlueRole `
  --assume-role-policy-document file://build/aws/glue-role-trust.json
aws iam put-role-policy --role-name DataGuardGlueRole `
  --policy-name DataGuardLakeAccess `
  --policy-document file://build/aws/glue-role-policy.json

.\scripts\upload_glue_artifacts.ps1 -Bucket $Bucket -Region $Region
aws glue create-job --cli-input-json file://build/aws/glue-job.json --region $Region
```

If the AWS Academy role cannot create IAM roles, ask the lab administrator for
an existing Glue service role and replace `Role` in `build/aws/glue-job.json`.

Run the known backfill before scheduling:

```powershell
aws glue start-job-run --job-name dataguard-daily --region $Region `
  --arguments '{"--start":"2026-01-01","--end":"2026-01-31"}'
```

Verify `bronze/`, `silver/`, `gold/`, the Glue Catalog tables, and the four
timestamped build summaries before continuing.

## 4. Schedule 06:00 Australia/Sydney

```powershell
aws iam create-role --role-name DataGuardSchedulerRole `
  --assume-role-policy-document file://build/aws/scheduler-role-trust.json
aws iam put-role-policy --role-name DataGuardSchedulerRole `
  --policy-name StartDataGuardGlue `
  --policy-document file://build/aws/scheduler-role-policy.json

$SchedulerRoleArn = "arn:aws:iam::$AccountId`:role/DataGuardSchedulerRole"
$Target = '{"Arn":"arn:aws:scheduler:::aws-sdk:glue:startJobRun",' +
  '"RoleArn":"' + $SchedulerRoleArn + '",' +
  '"Input":"{\"JobName\":\"dataguard-daily\"}",' +
  '"RetryPolicy":{"MaximumEventAgeInSeconds":3600,"MaximumRetryAttempts":1}}'
aws scheduler create-schedule --name dataguard-daily `
  --schedule-expression "cron(0 6 * * ? *)" `
  --schedule-expression-timezone "Australia/Sydney" `
  --flexible-time-window Mode=OFF --target $Target --region $Region
```

If EventBridge Scheduler role creation is blocked, use a native Glue scheduled
trigger and document its UTC time; native Glue cron does not preserve Sydney
wall-clock time across daylight-saving changes.

## 5. Failure email and log retention

Create an SNS topic and email subscription, confirm the subscription email,
then create an EventBridge rule using
`build/aws/glue-failure-event-pattern.json`. Grant `events.amazonaws.com`
permission to publish to the topic and attach the topic as the rule target.
After the first Glue run creates its log groups, retain them for 14 days:

```powershell
aws logs put-retention-policy --log-group-name /aws-glue/jobs/output `
  --retention-in-days 14 --region $Region
aws logs put-retention-policy --log-group-name /aws-glue/jobs/error `
  --retention-in-days 14 --region $Region
```

## 6. Streamlit read-only identity and deployment

Create IAM user `dataguard-dashboard`, attach the rendered
`dashboard-read-policy.json` as an inline policy, and create one access key.
The policy can only list/read `silver/` and `gold/`; it cannot write data or
start jobs.

In Streamlit Community Cloud select the GitHub repository, `main` branch,
`dashboard/app.py`, and Python 3.12. Paste values matching
`.streamlit/secrets.toml.example` into Advanced settings. Never commit the real
secret file. The app caches reads for five minutes and its Refresh data button
forces an immediate reload.

## Acceptance check

- A repeated backfill skips existing bronze objects and completes.
- A bad bronze root exits before replacing silver or gold.
- One scheduled run completes without overlapping another run.
- A controlled failed run sends the SNS email.
- The public dashboard shows the latest build time and data date within five
  minutes, with escalated, quarantined, and quality-only states visible.
