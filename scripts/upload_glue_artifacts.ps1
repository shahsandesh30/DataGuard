param(
    [Parameter(Mandatory = $true)]
    [string]$Bucket,

    [string]$Region = "ap-southeast-2"
)

$ErrorActionPreference = "Stop"
$repo = (Resolve-Path (Join-Path $PSScriptRoot "..")).Path
$python = Join-Path $repo "venv\Scripts\python.exe"

& $python (Join-Path $repo "scripts\package_glue.py")
if ($LASTEXITCODE -ne 0) {
    throw "Glue packaging failed"
}

aws sts get-caller-identity --region $Region | Out-Null
$artifactRoot = Join-Path $repo "build\glue"
aws s3 cp (Join-Path $artifactRoot "dataguard-pipelines.zip") "s3://$Bucket/artifacts/dataguard-pipelines.zip" --region $Region
aws s3 cp (Join-Path $artifactRoot "run_pipeline.py") "s3://$Bucket/artifacts/run_pipeline.py" --region $Region
aws s3 cp (Join-Path $artifactRoot "requirements-glue.txt") "s3://$Bucket/artifacts/requirements-glue.txt" --region $Region

Write-Output "Uploaded Glue artifacts to s3://$Bucket/artifacts/"
