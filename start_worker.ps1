param(
    [string]$CredentialPath = (Join-Path $PSScriptRoot "sa_key.json"),
    [string]$RedisUrl = "redis://localhost:6379/0",
    [string]$Project = "fitcv-491123",
    [string]$Dataset = "fitcv",
    [switch]$AllowDockerWorker
)

$ErrorActionPreference = "Stop"

if (-not (Test-Path -LiteralPath $CredentialPath)) {
    throw "Credential file not found: $CredentialPath"
}

$dockerWorkerRunning = $false
try {
    $dockerWorkerRunning = [bool](
        docker ps --filter "name=job-project-worker-1" --filter "status=running" --format "{{.Names}}"
    )
} catch {
    $dockerWorkerRunning = $false
}

if ($dockerWorkerRunning -and -not $AllowDockerWorker) {
    throw "Docker worker 'job-project-worker-1' is running. Stop it before using local start_worker.ps1, or use a fully Docker-based setup."
}

$pythonExe = Join-Path $PSScriptRoot ".venv\Scripts\python.exe"
if (-not (Test-Path -LiteralPath $pythonExe)) {
    throw "Virtualenv Python not found: $pythonExe"
}

$env:PYTHONPATH = "src"
$env:REDIS_URL = $RedisUrl
$env:GCP_PROJECT = $Project
$env:BIGQUERY_DATASET = $Dataset
$env:GOOGLE_APPLICATION_CREDENTIALS = $CredentialPath

Write-Host "Starting FitCV SimpleWorker"
Write-Host "Credentials: $CredentialPath"

& $pythonExe -c "import os; import redis; import fitcv_cp.queue; from rq import Queue, SimpleWorker; redis_url=os.environ['REDIS_URL']; conn=redis.from_url(redis_url); q=Queue('fitcv', connection=conn); w=SimpleWorker([q], connection=conn); w.work()"
