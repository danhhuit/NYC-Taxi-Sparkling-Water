@echo off
setlocal EnableExtensions

set "IMAGE=%~1"
if "%IMAGE%"=="" set "IMAGE=nyc-taxi-final:1.0"
set "NODE_CONTAINER=desktop-control-plane"
set "K8S_IMAGE=docker.io/library/%IMAGE%"

docker desktop kubernetes status >nul 2>&1
if errorlevel 1 (
    echo FAILED: Docker Desktop Kubernetes is not running.
    exit /b 1
)

docker exec "%NODE_CONTAINER%" ctr -n k8s.io images list -q >nul 2>&1
if errorlevel 1 (
    echo FAILED: Cannot access containerd in Kubernetes node "%NODE_CONTAINER%".
    exit /b 2
)

docker exec "%NODE_CONTAINER%" ctr -n k8s.io images check "name==%K8S_IMAGE%" >nul 2>&1
if not errorlevel 1 (
    echo Kubernetes image already ready: %K8S_IMAGE%
    exit /b 0
)

echo Loading "%IMAGE%" into Docker Desktop Kubernetes containerd...
echo This image is large; the streaming import can take several minutes.
docker save "%IMAGE%" | docker exec -i "%NODE_CONTAINER%" ctr -n k8s.io images import -
if errorlevel 1 (
    echo FAILED: Could not import "%IMAGE%" into Kubernetes containerd.
    exit /b 3
)

docker exec "%NODE_CONTAINER%" ctr -n k8s.io images check "name==%K8S_IMAGE%" >nul 2>&1
if errorlevel 1 (
    echo FAILED: Kubernetes containerd does not contain a complete "%K8S_IMAGE%" after import.
    exit /b 4
)

echo Kubernetes image ready: %K8S_IMAGE%
exit /b 0
