@echo off
setlocal EnableExtensions EnableDelayedExpansion

cd /d "%~dp0.."

set "LOG_DIR=%CD%\outputs\logs"
set "DOCKER_LOG=%LOG_DIR%\docker_final.log"
set "KUBERNETES_LOG=%LOG_DIR%\kubernetes_final.log"
set "IMAGE=nyc-taxi-final:1.0"
set "JOB=nyc-taxi-final-job"
set "CONTAINER=nyc-taxi-final-demo-%RANDOM%"

if not exist "%LOG_DIR%" mkdir "%LOG_DIR%"

> "%DOCKER_LOG%" echo NYC TAXI FINAL - DOCKER VALIDATION
>> "%DOCKER_LOG%" echo Started: %DATE% %TIME%
>> "%DOCKER_LOG%" echo Image: %IMAGE%
>> "%DOCKER_LOG%" echo.

> "%KUBERNETES_LOG%" echo NYC TAXI FINAL - KUBERNETES VALIDATION
>> "%KUBERNETES_LOG%" echo Started: %DATE% %TIME%
>> "%KUBERNETES_LOG%" echo Manifest: kubernetes\final-job.yaml
>> "%KUBERNETES_LOG%" echo Expected Job: %JOB%
>> "%KUBERNETES_LOG%" echo.

echo [1/7] Checking Docker daemon...
docker version >> "%DOCKER_LOG%" 2>&1
if errorlevel 1 (
    >> "%DOCKER_LOG%" echo.
    >> "%DOCKER_LOG%" echo BLOCKED: Docker daemon is unavailable.
    >> "%DOCKER_LOG%" echo If Docker Desktop reports engine.sock error 1920, restart Windows first.
    echo FAILED: Docker daemon is unavailable. See "%DOCKER_LOG%".
    exit /b 10
)

echo [2/7] Building current Docker image...
docker build -f docker\Dockerfile -t "%IMAGE%" . >> "%DOCKER_LOG%" 2>&1
if errorlevel 1 (
    >> "%DOCKER_LOG%" echo FAILED: docker build
    echo FAILED: Docker image build. See "%DOCKER_LOG%".
    exit /b 11
)

echo [3/7] Running Docker scoring container...
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

echo [4/7] Checking Kubernetes context and node...
kubectl config current-context >> "%KUBERNETES_LOG%" 2>&1
if errorlevel 1 goto :kubernetes_unavailable
kubectl get nodes -o wide >> "%KUBERNETES_LOG%" 2>&1
if errorlevel 1 goto :kubernetes_unavailable

echo [5/7] Recreating Job %JOB%...
kubectl delete job "%JOB%" --ignore-not-found=true --wait=true >> "%KUBERNETES_LOG%" 2>&1
if errorlevel 1 goto :kubernetes_failed
kubectl apply -f kubernetes\final-job.yaml >> "%KUBERNETES_LOG%" 2>&1
if errorlevel 1 goto :kubernetes_failed

echo [6/7] Waiting up to 30 minutes for completion...
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

echo [7/7] Saving Kubernetes evidence...
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
echo FAILED: Kubernetes is unavailable. See "%KUBERNETES_LOG%".
exit /b 21

:kubernetes_failed
>> "%KUBERNETES_LOG%" echo.
>> "%KUBERNETES_LOG%" echo FAILED: Could not recreate/apply Kubernetes Job %JOB%.
echo FAILED: Kubernetes Job apply. See "%KUBERNETES_LOG%".
exit /b 22
