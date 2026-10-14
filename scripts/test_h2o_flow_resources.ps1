[CmdletBinding()]
param(
    [ValidateRange(4, 10)]
    [int]$HeapGb = 8,

    [ValidateRange(1, 8)]
    [int]$ReserveGb = 2
)

$ErrorActionPreference = "Stop"

$os = Get-CimInstance Win32_OperatingSystem
$freeGb = [math]::Round($os.FreePhysicalMemory / 1MB, 2)
$totalGb = [math]::Round($os.TotalVisibleMemorySize / 1MB, 2)
$requiredGb = $HeapGb + $ReserveGb
$heavyProcesses = @(Get-Process -ErrorAction SilentlyContinue | Where-Object {
    $_.ProcessName -match '^(java|python|spark|vmmem|com\.docker\.backend)$'
})

Write-Output "H2O Flow resource preflight"
Write-Output "Physical memory : $totalGb GB total; $freeGb GB free"
Write-Output "Requested heap  : $HeapGb GB (hard limit is 10 GB)"
Write-Output "Required free   : $requiredGb GB (heap + $ReserveGb GB OS reserve)"

if ($HeapGb -gt 10) {
    throw "Refusing to start H2O with more than 10 GB heap."
}

if ($freeGb -lt $requiredGb) {
    throw "Insufficient free RAM for H2O Flow: need at least $requiredGb GB free, found $freeGb GB. Stop or wait for Docker/Kubernetes/Spark work before retrying."
}

if ($heavyProcesses.Count -gt 0) {
    $active = $heavyProcesses |
        Select-Object ProcessName, Id, @{Name = 'WorkingSetMB'; Expression = { [math]::Round($_.WorkingSet64 / 1MB) } } |
        Format-Table -AutoSize | Out-String
    Write-Output "Potentially competing processes detected:"
    Write-Output $active
}

Write-Output "H2O Flow resource preflight passed."
