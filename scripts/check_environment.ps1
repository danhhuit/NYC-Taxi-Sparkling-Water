$ErrorActionPreference = "Continue"

$projectRoot = Split-Path -Parent $PSScriptRoot
$logDirectory = Join-Path $projectRoot "outputs\logs"
$logPath = Join-Path $logDirectory "environment_check.log"
New-Item -ItemType Directory -Force -Path $logDirectory | Out-Null

$python = Join-Path $projectRoot ".venv\Scripts\python.exe"
$java = "D:\BigData\Tools\Java\jdk-17\bin\java.exe"
$sparkSubmit = "D:\BigData\Tools\Spark\spark-3.5.0-bin-hadoop3\bin\spark-submit.cmd"
$psql = "C:\Program Files\PostgreSQL\18\bin\psql.exe"

$env:JAVA_HOME = "D:\BigData\Tools\Java\jdk-17"
$env:SPARK_HOME = "D:\BigData\Tools\Spark\spark-3.5.0-bin-hadoop3"
$env:HADOOP_HOME = "$env:SPARK_HOME\hadoop"
$env:PATH = "$env:JAVA_HOME\bin;$env:HADOOP_HOME\bin;$env:SPARK_HOME\bin;$env:PATH"

& {
    Write-Output "NYC TAXI FINAL PROJECT - ENVIRONMENT CHECK"
    Write-Output "Timestamp: $(Get-Date -Format o)"
    Write-Output ""

    Write-Output "[Python]"
    & $python --version 2>&1

    Write-Output "`n[Python packages]"
    & $python -c "import h2o,pyspark,pysparkling; print('PySpark:',pyspark.__version__); print('H2O:',h2o.__version__); print('Sparkling Water:',pysparkling.__file__)" 2>&1

    Write-Output "`n[Java]"
    & $java -version 2>&1

    Write-Output "`n[Spark / Scala]"
    & $sparkSubmit --version 2>&1

    Write-Output "`n[PostgreSQL]"
    & $psql --version 2>&1
    Get-Service -Name "postgresql-x64-18" -ErrorAction SilentlyContinue |
        Select-Object Name, Status | Format-Table -AutoSize

    Write-Output "`n[Docker]"
    docker --version 2>&1
    docker info --format "Docker server: {{.ServerVersion}}" 2>&1

    Write-Output "`n[Kubernetes]"
    kubectl version --client 2>&1
    kubectl config current-context 2>&1
    kubectl get nodes 2>&1
} | Tee-Object -FilePath $logPath

Write-Output "`nEnvironment log: $logPath"
