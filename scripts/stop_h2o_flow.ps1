$ErrorActionPreference = "Stop"

$projectRoot = Split-Path -Parent $PSScriptRoot
$logDir = Join-Path $projectRoot "outputs\logs"
$pidFiles = Get-ChildItem -LiteralPath $logDir -Filter "h2o_*.pid" -File -ErrorAction SilentlyContinue
$stopped = 0

foreach ($pidFile in $pidFiles) {
    $processId = 0
    if (-not [int]::TryParse((Get-Content -LiteralPath $pidFile.FullName -Raw).Trim(), [ref]$processId)) {
        Remove-Item -LiteralPath $pidFile.FullName -Force
        continue
    }

    $processInfo = Get-CimInstance Win32_Process -Filter "ProcessId = $processId" -ErrorAction SilentlyContinue
    if (-not $processInfo) {
        Remove-Item -LiteralPath $pidFile.FullName -Force
        continue
    }

    if ($processInfo.CommandLine -notmatch "water\.H2OApp") {
        Write-Warning "PID $processId no longer belongs to H2O; it was not stopped."
        Remove-Item -LiteralPath $pidFile.FullName -Force
        continue
    }

    Stop-Process -Id $processId -Force
    Remove-Item -LiteralPath $pidFile.FullName -Force
    Write-Host "Stopped H2O process PID $processId."
    $stopped++
}

if ($stopped -eq 0) {
    $listener = Get-NetTCPConnection -LocalPort 54321 -State Listen -ErrorAction SilentlyContinue |
        Select-Object -First 1
    if ($listener) {
        Write-Warning "Port 54321 is still used by PID $($listener.OwningProcess), but it was not started by the project script."
        exit 1
    }
    Write-Host "No project-managed H2O process is running."
}
