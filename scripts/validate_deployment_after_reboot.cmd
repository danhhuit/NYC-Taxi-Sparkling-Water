@echo off
setlocal EnableExtensions EnableDelayedExpansion

cd /d "%~dp0.."

set "LOG_DIR=%CD%\outputs\logs"
set "DOCKER_LOG=%LOG_DIR%\docker_final.log"
set "KUBERNETES_LOG=%LOG_DIR%\kubernetes_final.log"
set "IMAGE=nyc-taxi-final:1.0"
set "JOB=nyc-taxi-final-job"
set "CONTAINER=nyc-taxi-final-demo-%RANDOM%"
set "DEPLOYMENT_TEMP=%CD%\outputs\tmp\deployment"
set "DOCKER_STORAGE_DRIVE=%NYC_TAXI_DOCKER_STORAGE_DRIVE%"
set "KUBERNETES_ONLY=0"
if /i "%~1"=="--kubernetes-only" set "KUBERNETES_ONLY=1"
if not defined DOCKER_STORAGE_DRIVE set "DOCKER_STORAGE_DRIVE=%SystemDrive%"

if not exist "%LOG_DIR%" mkdir "%LOG_DIR%"
if not exist "%DEPLOYMENT_TEMP%" mkdir "%DEPLOYMENT_TEMP%"
set "TEMP=%DEPLOYMENT_TEMP%"
set "TMP=%DEPLOYMENT_TEMP%"

if "%KUBERNETES_ONLY%"=="1" (
    >> "%DOCKER_LOG%" echo.
    >> "%DOCKER_LOG%" echo Kubernetes-only resume: %DATE% %TIME%
) else (
    > "%DOCKER_LOG%" echo NYC TAXI FINAL - DOCKER VALIDATION
    >> "%DOCKER_LOG%" echo Started: %DATE% %TIME%
    >> "%DOCKER_LOG%" echo Image: %IMAGE%
    >> "%DOCKER_LOG%" echo.
)

> "%KUBERNETES_LOG%" echo NYC TAXI FINAL - KUBERNETES VALIDATION
>> "%KUBERNETES_LOG%" echo Started: %DATE% %TIME%
>> "%KUBERNETES_LOG%" echo Manifest: kubernetes\final-job.yaml
>> "%KUBERNETES_LOG%" echo Expected Job: %JOB%
>> "%KUBERNETES_LOG%" echo.

echo [0/8] Checking Docker storage-drive free space...
for /f "usebackq delims=" %%F in (`powershell -NoProfile -Command "$drive=[System.IO.DriveInfo]::new('%DOCKER_STORAGE_DRIVE%'); [math]::Floor($drive.AvailableFreeSpace / 1GB)"`) do set "DOCKER_FREE_GB=%%F"
if not defined DOCKER_FREE_GB (
    >> "%DOCKER_LOG%" echo BLOCKED: Could not determine free space on %DOCKER_STORAGE_DRIVE%.
    echo FAILED: Could not determine free space on %DOCKER_STORAGE_DRIVE%.
    exit /b 9
)
>> "%DOCKER_LOG%" echo Docker storage drive %DOCKER_STORAGE_DRIVE% free: !DOCKER_FREE_GB! GB
>> "%DOCKER_LOG%" echo Host TEMP/TMP: %DEPLOYMENT_TEMP%
if !DOCKER_FREE_GB! LSS 10 (
    >> "%DOCKER_LOG%" echo BLOCKED: At least 10 GB free is required for Docker build/image/container data.
    >> "%DOCKER_LOG%" echo Free space or move Docker Desktop disk image before retrying.
    echo FAILED: Only !DOCKER_FREE_GB! GB free on %DOCKER_STORAGE_DRIVE%; at least 10 GB is required.
    exit /b 9
)

if "%KUBERNETES_ONLY%"=="1" (
    echo [1/8] Checking Docker daemon and existing image...
    docker version >> "%DOCKER_LOG%" 2>&1
    if errorlevel 1 (
        echo FAILED: Docker daemon is unavailable. See "%DOCKER_LOG%".
        exit /b 10
    )
    docker image inspect "%IMAGE%" >nul 2>&1
    if errorlevel 1 (
        echo FAILED: Image "%IMAGE%" does not exist. Run the full validation first.
        exit /b 12
    )
    echo Reusing existing image "%IMAGE%"; Docker build and scoring are skipped.
    goto :kubernetes_validation
)

echo [1/8] Checking Docker daemon...
docker version >> "%DOCKER_LOG%" 2>&1
if errorlevel 1 (
    >> "%DOCKER_LOG%" echo.
    >> "%DOCKER_LOG%" echo BLOCKED: Docker daemon is unavailable.
    >> "%DOCKER_LOG%" echo If Docker Desktop reports engine.sock error 1920, restart Windows first.
    echo FAILED: Docker daemon is unavailable. See "%DOCKER_LOG%".
    exit /b 10
)

echo [2/8] Building current Docker image...
docker build -f docker\Dockerfile -t "%IMAGE%" . >> "%DOCKER_LOG%" 2>&1
if errorlevel 1 (
    >> "%DOCKER_LOG%" echo FAILED: docker build
    echo FAILED: Docker image build. See "%DOCKER_LOG%".
    exit /b 11
)

echo [3/8] Running Docker scoring container...
docker run --rm --name "%CONTAINER%" "%IMAGE%" >> "%DOCKER_LOG%" 2>&1
set "DOCKER_RC=!ERRORLEVEL!"
>> "%DOCKER_LOG%" echo.
>> "%DOCKER_LOG%" echo Container exit code: !DOCKER_RC!
if not "!DOCKER_RC!"=="0" (
    >> "%DOCKER_LOG%" echo FAILED: Docker scoring container
    echo FAILED: Docker scoring container. See "%DOCKER_LOG%".
    exit /b !DOCKER_RC!
)
>> "%DOCKER_LOG%" echo DOCKER FINAL VALIDATION SUCCESS

:kubernetes_validation
echo [4/8] Checking Kubernetes context and node...
docker desktop kubernetes status >> "%KUBERNETES_LOG%" 2>&1
kubectl config current-context >> "%KUBERNETES_LOG%" 2>&1
if errorlevel 1 goto :kubernetes_unavailable
kubectl get nodes -o wide >> "%KUBERNETES_LOG%" 2>&1
if errorlevel 1 goto :kubernetes_unavailable

echo [5/8] Loading Docker image into Kubernetes containerd...
call scripts\load_image_into_docker_desktop_kubernetes.cmd "%IMAGE%" >> "%KUBERNETES_LOG%" 2>&1
if errorlevel 1 (
    >> "%KUBERNETES_LOG%" echo FAILED: Could not load Docker image into Kubernetes containerd.
    echo FAILED: Kubernetes image import. See "%KUBERNETES_LOG%".
    exit /b 23
)

echo [6/8] Recreating Job %JOB%...
kubectl delete job "%JOB%" --ignore-not-found=true --wait=true >> "%KUBERNETES_LOG%" 2>&1
if errorlevel 1 goto :kubernetes_failed
kubectl apply -f kubernetes\final-job.yaml >> "%KUBERNETES_LOG%" 2>&1
if errorlevel 1 goto :kubernetes_failed

echo [7/8] Waiting up to 30 minutes for completion...
kubectl wait --for=condition=complete "job/%JOB%" --timeout=30m >> "%KUBERNETES_LOG%" 2>&1
if errorlevel 1 (
    >> "%KUBERNETES_LOG%" echo.
    >> "%KUBERNETES_LOG%" echo Job did not reach Complete. Diagnostics follow.
    kubectl get job "%JOB%" -o wide >> "%KUBERNETES_LOG%" 2>&1
    kubectl get pods -l app=nyc-taxi-final -o wide >> "%KUBERNETES_LOG%" 2>&1
    kubectl describe job "%JOB%" >> "%KUBERNETES_LOG%" 2>&1
    kubectl logs "job/%JOB%" --all-containers=true >> "%KUBERNETES_LOG%" 2>&1
    echo FAILED: Kubernetes Job did not complete. See "%KUBERNETES_LOG%".
    exit /b 20
)

echo [8/8] Saving Kubernetes evidence...
kubectl get job "%JOB%" -o wide >> "%KUBERNETES_LOG%" 2>&1
kubectl get pods -l app=nyc-taxi-final -o wide >> "%KUBERNETES_LOG%" 2>&1
kubectl logs "job/%JOB%" --all-containers=true >> "%KUBERNETES_LOG%" 2>&1
>> "%KUBERNETES_LOG%" echo.
>> "%KUBERNETES_LOG%" echo KUBERNETES FINAL VALIDATION SUCCESS

echo.
echo Docker and Kubernetes validation succeeded.
echo Docker log: %DOCKER_LOG%
echo Kubernetes log: %KUBERNETES_LOG%
exit /b 0

:kubernetes_unavailable
>> "%KUBERNETES_LOG%" echo.
>> "%KUBERNETES_LOG%" echo BLOCKED: Kubernetes context or node is unavailable.
echo FAILED: Kubernetes context or node is unavailable. See "%KUBERNETES_LOG%".
echo Wait for Docker Desktop Kubernetes to finish starting, then run:
echo kubectl config use-context docker-desktop
echo scripts\validate_deployment_after_reboot.cmd --kubernetes-only
exit /b 21

:kubernetes_failed
>> "%KUBERNETES_LOG%" echo.
>> "%KUBERNETES_LOG%" echo FAILED: Could not recreate/apply Kubernetes Job %JOB%.
echo FAILED: Kubernetes Job apply. See "%KUBERNETES_LOG%".
exit /b 22
