param()

$ErrorActionPreference = "SilentlyContinue"

$repoRoot = [regex]::Escape((Resolve-Path $PSScriptRoot).Path)

$listenerPids = Get-NetTCPConnection -LocalPort 8000 -State Listen |
    Select-Object -ExpandProperty OwningProcess -Unique

foreach ($pid in $listenerPids) {
    Stop-Process -Id $pid -Force
}

$repoProcesses = Get-CimInstance Win32_Process |
    Where-Object {
        $_.Name -match "python|uv|pwsh|powershell" -and
        $_.CommandLine -match $repoRoot -and
        $_.CommandLine -match "fitcv_cp\.main:app|SimpleWorker|fitcv_cp\.queue|rq worker"
    }

foreach ($process in $repoProcesses) {
    Stop-Process -Id $process.ProcessId -Force
}

Write-Host "Stopped local FitCV web/worker processes if any were running."
