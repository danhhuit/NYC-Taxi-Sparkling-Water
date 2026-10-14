param(
    [Parameter(Mandatory = $true)]
    [string]$CloudName,

    [ValidateRange(1, 10)]
    [int]$HeapGb = 2,

    [ValidateRange(10, 180)]
    [int]$StartupTimeoutSeconds = 60,

    [switch]$OpenBrowser
)

$ErrorActionPreference = "Stop"

$projectRoot = Split-Path -Parent $PSScriptRoot
$h2oJar = Join-Path $projectRoot ".venv\Lib\site-packages\h2o\backend\bin\h2o.jar"
$swExtensionJar = "D:\BigData\Tools\SparklingWater\sparkling-water-3.46.0.6-1-3.5\jars\sparkling-water-assembly-extensions_2.12-3.46.0.6-1-3.5-all.jar"
$healthUrl = "http://127.0.0.1:54321/3/Cloud"
$flowUrl = "http://127.0.0.1:54321"
$logDir = Join-Path $projectRoot "outputs\logs"
$safeCloudName = $CloudName -replace "[^A-Za-z0-9_.-]", "_"
$stdoutLog = Join-Path $logDir "h2o_$safeCloudName.out.log"
$stderrLog = Join-Path $logDir "h2o_$safeCloudName.err.log"
$pidFile = Join-Path $logDir "h2o_$safeCloudName.pid"

New-Item -ItemType Directory -Path $logDir -Force | Out-Null

foreach ($requiredFile in @($h2oJar, $swExtensionJar)) {
    if (-not (Test-Path -LiteralPath $requiredFile -PathType Leaf)) {
        throw "Required H2O file was not found: $requiredFile"
    }
}

try {
    $existingCloud = Invoke-RestMethod -Uri $healthUrl -TimeoutSec 3
    if ($existingCloud.cloud_healthy -and $existingCloud.cloud_name -eq $CloudName) {
        Write-Host "H2O cloud '$CloudName' is already healthy at $flowUrl."
        if ($OpenBrowser) {
            Start-Process $flowUrl
        }
        exit 0
    }

    throw "Port 54321 is occupied by H2O cloud '$($existingCloud.cloud_name)', not '$CloudName'. Run scripts\stop_h2o_flow.cmd first."
}
catch {
    if ($_.Exception.Message -like "Port 54321 is occupied*") {
        throw
    }
}

$portOwner = Get-NetTCPConnection -LocalPort 54321 -State Listen -ErrorAction SilentlyContinue |
    Select-Object -First 1
if ($portOwner) {
    throw "Port 54321 is already used by PID $($portOwner.OwningProcess). Stop that process before starting H2O."
}

$javaCandidates = New-Object System.Collections.Generic.List[string]

if ($env:H2O_JAVA_HOME) {
    $javaCandidates.Add((Join-Path $env:H2O_JAVA_HOME "bin\java.exe"))
}

if (Test-Path -LiteralPath "C:\Program Files\Java") {
    Get-ChildItem -LiteralPath "C:\Program Files\Java" -Directory -ErrorAction SilentlyContinue |
        Where-Object { $_.Name -match "^(jre|jdk)1\.8" } |
        Sort-Object Name -Descending |
        ForEach-Object { $javaCandidates.Add((Join-Path $_.FullName "bin\java.exe")) }
}

if ($env:JAVA_HOME) {
    $javaCandidates.Add((Join-Path $env:JAVA_HOME "bin\java.exe"))
}

$javaExe = $javaCandidates |
    Where-Object { Test-Path -LiteralPath $_ -PathType Leaf } |
    Select-Object -First 1

if (-not $javaExe) {
    throw "No compatible Java executable was found. Install 64-bit Java 8 or set H2O_JAVA_HOME."
}

# Java 8 cannot parse the Java 17 module flags set by activate_project.cmd.
# Child processes copy the current environment, so clear the variable while
# invoking the standalone Java runtime and restore it before this script exits.
$savedJavaToolOptions = $env:JAVA_TOOL_OPTIONS
$env:JAVA_TOOL_OPTIONS = ""

Write-Host "Starting H2O cloud '$CloudName' with $javaExe"
Write-Host "Heap: ${HeapGb} GB"
Write-Host "Logs: $stdoutLog"
Write-Host "      $stderrLog"

Remove-Item -LiteralPath $stdoutLog, $stderrLog -Force -ErrorAction SilentlyContinue

$javaArgs = @(
    "-Dfile.encoding=UTF-8",
    "-Xms512m",
    "-Xmx${HeapGb}g",
    "-cp",
    "$h2oJar;$swExtensionJar",
    "water.H2OApp",
    "-allow_clients",
    "-name",
    $CloudName,
    "-ip",
    "127.0.0.1",
    "-web_ip",
    "127.0.0.1",
    "-port",
    "54321"
)

try {
    $h2oProcess = Start-Process `
        -FilePath $javaExe `
        -ArgumentList $javaArgs `
        -WorkingDirectory $projectRoot `
        -RedirectStandardOutput $stdoutLog `
        -RedirectStandardError $stderrLog `
        -WindowStyle Hidden `
        -PassThru
}
finally {
    $env:JAVA_TOOL_OPTIONS = $savedJavaToolOptions
}

Set-Content -LiteralPath $pidFile -Value $h2oProcess.Id -Encoding Ascii

$deadline = (Get-Date).AddSeconds($StartupTimeoutSeconds)
do {
    if ($h2oProcess.HasExited) {
        Write-Host ""
        Write-Host "H2O exited before becoming ready (exit code $($h2oProcess.ExitCode))." -ForegroundColor Red
        Get-Content -LiteralPath $stdoutLog -Tail 30 -ErrorAction SilentlyContinue
        Get-Content -LiteralPath $stderrLog -Tail 30 -ErrorAction SilentlyContinue
        exit 1
    }

    try {
        $cloud = Invoke-RestMethod -Uri $healthUrl -TimeoutSec 3
        if ($cloud.cloud_healthy -and $cloud.cloud_name -eq $CloudName -and $cloud.cloud_size -ge 1) {
            Write-Host ""
            Write-Host "H2O READY" -ForegroundColor Green
            Write-Host "Cloud name : $($cloud.cloud_name)"
            Write-Host "Cloud size : $($cloud.cloud_size)"
            Write-Host "Version    : $($cloud.version)"
            Write-Host "Flow URL   : $flowUrl"
            Write-Host "PID        : $($h2oProcess.Id)"
            if ($OpenBrowser) {
                Start-Process $flowUrl
            }
            exit 0
        }
    }
    catch {
        # The REST endpoint is expected to refuse connections during startup.
    }

    Start-Sleep -Seconds 1
} while ((Get-Date) -lt $deadline)

Write-Host "H2O did not become healthy within $StartupTimeoutSeconds seconds." -ForegroundColor Red
if (-not $h2oProcess.HasExited) {
    Stop-Process -Id $h2oProcess.Id -Force
}
Get-Content -LiteralPath $stdoutLog -Tail 30 -ErrorAction SilentlyContinue
Get-Content -LiteralPath $stderrLog -Tail 30 -ErrorAction SilentlyContinue
exit 1
