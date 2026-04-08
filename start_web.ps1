param(
    [string]$CredentialPath = (Join-Path $PSScriptRoot "sa_key.json"),
    [string]$RedisUrl = "redis://localhost:6379/0",
    [string]$Project = "fitcv-491123",
    [string]$Dataset = "fitcv",
    [int]$Port = 8000,
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
    throw "Docker worker 'job-project-worker-1' is running. Stop it before using local start_web.ps1, or use a fully Docker-based setup."
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

Write-Host "Starting FitCV web server on port $Port"
Write-Host "Credentials: $CredentialPath"

& $pythonExe -m uvicorn fitcv_cp.main:app --host 0.0.0.0 --port $Port
