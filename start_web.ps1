param(
    [string]$CredentialPath = "",
    [string]$RedisUrl = "",
    [string]$Project = "",
    [string]$Dataset = "",
    [int]$Port = 8000,
    [switch]$AllowDockerWorker
)

$ErrorActionPreference = "Stop"

function Set-EnvFromDotEnv {
    param([string]$Path)
    if (-not (Test-Path -LiteralPath $Path)) { return }
    foreach ($rawLine in Get-Content -LiteralPath $Path) {
        $line = $rawLine.Trim()
        if (-not $line -or $line.StartsWith("#") -or -not $line.Contains("=")) { continue }
        $parts = $line.Split("=", 2)
        $key = $parts[0].Trim()
        $value = $parts[1].Trim().Trim("'`"")
        if (-not $key) { continue }
        $existing = [Environment]::GetEnvironmentVariable($key, "Process")
        if (-not [string]::IsNullOrWhiteSpace($existing)) { continue }
        [Environment]::SetEnvironmentVariable($key, $value, "Process")
    }
}

Set-EnvFromDotEnv -Path (Join-Path $PSScriptRoot ".env")

if (-not $env:FITCV_CP_DATA_BACKEND) { $env:FITCV_CP_DATA_BACKEND = "sqlite" }
if (-not $RedisUrl) { $RedisUrl = $env:REDIS_URL }
if (-not $Project) { $Project = $env:GCP_PROJECT }
if (-not $Dataset) { $Dataset = $env:BIGQUERY_DATASET }
if (-not $CredentialPath) { $CredentialPath = $env:GOOGLE_APPLICATION_CREDENTIALS }
if (-not $RedisUrl) { $RedisUrl = "redis://:myredissecret@localhost:6379/0" }
if (-not $Dataset) { $Dataset = "fitcv" }
if (-not $Project) { $Project = "local" }

if ($env:FITCV_CP_DATA_BACKEND -ne "sqlite") {
    if (-not $CredentialPath) {
        throw "GOOGLE_APPLICATION_CREDENTIALS is required when FITCV_CP_DATA_BACKEND is not sqlite."
    }
    if (-not (Test-Path -LiteralPath $CredentialPath)) {
        throw "Credential file not found: $CredentialPath"
    }
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
if (-not (Test-Path -LiteralPath $pythonExe)) { $pythonExe = "python" }

$env:PYTHONPATH = "src"
$env:REDIS_URL = $RedisUrl
$env:GCP_PROJECT = $Project
$env:BIGQUERY_DATASET = $Dataset
if ($CredentialPath) { $env:GOOGLE_APPLICATION_CREDENTIALS = $CredentialPath }
if (-not $env:FITCV_OTEL_ENABLED) { $env:FITCV_OTEL_ENABLED = "true" }
if (-not $env:FITCV_LANGFUSE_PROJECT_PUBLIC_KEY) { $env:FITCV_LANGFUSE_PROJECT_PUBLIC_KEY = "pk-lf-localdev" }
if (-not $env:FITCV_LANGFUSE_PROJECT_SECRET_KEY) { $env:FITCV_LANGFUSE_PROJECT_SECRET_KEY = "sk-lf-localdev-secret" }
if (-not $env:FITCV_OTEL_EXPORTER_OTLP_ENDPOINT) { $env:FITCV_OTEL_EXPORTER_OTLP_ENDPOINT = "http://localhost:3000/api/public/otel/v1/traces" }
if (-not $env:FITCV_OTEL_EXPORTER_OTLP_HEADERS) {
    $token = [Convert]::ToBase64String([Text.Encoding]::UTF8.GetBytes("$($env:FITCV_LANGFUSE_PROJECT_PUBLIC_KEY):$($env:FITCV_LANGFUSE_PROJECT_SECRET_KEY)"))
    $env:FITCV_OTEL_EXPORTER_OTLP_HEADERS = "Authorization=Basic $token"
}
if (-not $env:FITCV_OTEL_SERVICE_NAME) { $env:FITCV_OTEL_SERVICE_NAME = "fitcv-control-plane" }
if (-not $env:FITCV_LANGFUSE_ENABLED) { $env:FITCV_LANGFUSE_ENABLED = "true" }
if (-not $env:FITCV_LANGFUSE_BASE_URL) { $env:FITCV_LANGFUSE_BASE_URL = "http://localhost:3000" }

Write-Host "Starting FitCV web server on port $Port"
Write-Host "Backend: $($env:FITCV_CP_DATA_BACKEND)"
if ($CredentialPath) { Write-Host "Credentials: $CredentialPath" }

& $pythonExe -m uvicorn fitcv_cp.main:app --host 0.0.0.0 --port $Port
