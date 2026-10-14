$ErrorActionPreference = "Stop"

$projectRoot = Split-Path -Parent $PSScriptRoot
$sourceRoot = Join-Path $projectRoot "data\processed\taxi_multisource.parquet"
$targetRoot = Join-Path $projectRoot "data\flow\full_12_months"

if (-not (Test-Path -LiteralPath $sourceRoot)) {
    throw "Missing final dataset: $sourceRoot"
}

New-Item -ItemType Directory -Force -Path $targetRoot | Out-Null

$sourceFiles = @(
    Get-ChildItem -LiteralPath $sourceRoot -Recurse -File |
        Where-Object { $_.Name -like "part-*.parquet" }
)

if ($sourceFiles.Count -eq 0) {
    throw "No Parquet part files found in $sourceRoot"
}

$created = 0
$existing = 0

foreach ($file in $sourceFiles) {
    $relativeDirectory = $file.DirectoryName.Substring($sourceRoot.Length).
        TrimStart("\").
        Replace("\", "__").
        Replace("=", "-")

    $linkName = if ($relativeDirectory) {
        "${relativeDirectory}__$($file.Name)"
    } else {
        $file.Name
    }

    $linkPath = Join-Path $targetRoot $linkName

    if (Test-Path -LiteralPath $linkPath) {
        $existing++
        continue
    }

    New-Item -ItemType HardLink -Path $linkPath -Target $file.FullName | Out-Null
    $created++
}

$linkedFiles = @(Get-ChildItem -LiteralPath $targetRoot -File -Filter "*.parquet")
$logicalSize = ($linkedFiles | Measure-Object Length -Sum).Sum

Write-Output "H2O FLOW FULL-DATA IMPORT DIRECTORY READY"
Write-Output "Source files : $($sourceFiles.Count)"
Write-Output "Created      : $created"
Write-Output "Existing     : $existing"
Write-Output "Linked files : $($linkedFiles.Count)"
Write-Output ("Logical size : {0:N2} MB" -f ($logicalSize / 1MB))
Write-Output "Import path  : $($targetRoot.Replace('\', '/'))"
