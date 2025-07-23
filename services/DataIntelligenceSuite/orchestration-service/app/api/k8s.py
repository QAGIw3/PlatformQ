"""
Kubernetes orchestration API endpoints
"""

from typing import List, Optional, Dict, Any
from datetime import datetime
from fastapi import APIRouter, HTTPException, Depends, Query, Body
from pydantic import BaseModel, Field

from ..core import K8sManager
from data_intelligence_common import verify_token

router = APIRouter(prefix="/k8s", tags=["kubernetes"])

# Dependencies injected from main
k8s_manager: Optional[K8sManager] = None


def set_k8s_deps(k8s_mgr: K8sManager):
    """Set dependencies for K8s router"""
    global k8s_manager
    k8s_manager = k8s_mgr


class K8sJobRequest(BaseModel):
    """Request model for creating a K8s job"""
    name: str = Field(..., description="Job name")
    image: str = Field(..., description="Container image")
    command: List[str] = Field(..., description="Command to execute")
    args: List[str] = Field([], description="Command arguments")
    env_vars: Dict[str, str] = Field({}, description="Environment variables")
    resources: Dict[str, Any] = Field(
        default={
            "requests": {"cpu": "100m", "memory": "128Mi"},
            "limits": {"cpu": "1", "memory": "1Gi"}
        },
        description="Resource requirements"
    )
    labels: Dict[str, str] = Field({}, description="Job labels")


class K8sDeploymentRequest(BaseModel):
    """Request model for creating a K8s deployment"""
    name: str = Field(..., description="Deployment name")
    image: str = Field(..., description="Container image")
    replicas: int = Field(1, description="Number of replicas")
    port: int = Field(None, description="Container port")
    env_vars: Dict[str, str] = Field({}, description="Environment variables")
    resources: Dict[str, Any] = Field(
        default={
            "requests": {"cpu": "100m", "memory": "128Mi"},
            "limits": {"cpu": "1", "memory": "1Gi"}
        },
        description="Resource requirements"
    )


@router.post("/jobs")
async def create_job(
    request: K8sJobRequest,
    token_data: dict = Depends(verify_token)
) -> Dict[str, Any]:
    """Create a Kubernetes job"""
    if not k8s_manager:
        raise HTTPException(status_code=503, detail="K8s manager not initialized")
    
    try:
        job_name = await k8s_manager.create_job(
            name=request.name,
            image=request.image,
            command=request.command,
            args=request.args,
            env_vars=request.env_vars,
            resources=request.resources,
            labels=request.labels
        )
        
        return {
            "job_name": job_name,
            "status": "created",
            "message": f"Job {job_name} created successfully"
        }
        
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))


@router.get("/jobs/{job_name}")
async def get_job_status(
    job_name: str,
    token_data: dict = Depends(verify_token)
) -> Dict[str, Any]:
    """Get status of a Kubernetes job"""
    if not k8s_manager:
        raise HTTPException(status_code=503, detail="K8s manager not initialized")
    
    try:
        status = await k8s_manager.get_job_status(job_name)
        return status
        
    except Exception as e:
        raise HTTPException(status_code=404, detail=str(e))


@router.delete("/jobs/{job_name}")
async def delete_job(
    job_name: str,
    token_data: dict = Depends(verify_token)
) -> Dict[str, Any]:
    """Delete a Kubernetes job"""
    if not k8s_manager:
        raise HTTPException(status_code=503, detail="K8s manager not initialized")
    
    try:
        success = await k8s_manager.delete_job(job_name)
        
        if success:
            return {
                "job_name": job_name,
                "status": "deleted",
                "message": f"Job {job_name} deleted successfully"
            }
        else:
            raise HTTPException(status_code=500, detail="Failed to delete job")
            
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))


@router.get("/jobs/{job_name}/logs")
async def get_job_logs(
    job_name: str,
    token_data: dict = Depends(verify_token)
) -> Dict[str, Any]:
    """Get logs from a Kubernetes job"""
    if not k8s_manager:
        raise HTTPException(status_code=503, detail="K8s manager not initialized")
    
    try:
        logs = await k8s_manager.get_job_logs(job_name)
        
        return {
            "job_name": job_name,
            "logs": logs
        }
        
    except Exception as e:
        raise HTTPException(status_code=404, detail=str(e))


@router.post("/deployments")
async def create_deployment(
    request: K8sDeploymentRequest,
    token_data: dict = Depends(verify_token)
) -> Dict[str, Any]:
    """Create a Kubernetes deployment"""
    if not k8s_manager:
        raise HTTPException(status_code=503, detail="K8s manager not initialized")
    
    try:
        deployment_name = await k8s_manager.create_deployment(
            name=request.name,
            image=request.image,
            replicas=request.replicas,
            port=request.port,
            env_vars=request.env_vars,
            resources=request.resources
        )
        
        return {
            "deployment_name": deployment_name,
            "status": "created",
            "message": f"Deployment {deployment_name} created successfully"
        }
        
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))


@router.put("/deployments/{name}/scale")
async def scale_deployment(
    name: str,
    replicas: int = Body(..., ge=0, le=100),
    token_data: dict = Depends(verify_token)
) -> Dict[str, Any]:
    """Scale a Kubernetes deployment"""
    if not k8s_manager:
        raise HTTPException(status_code=503, detail="K8s manager not initialized")
    
    try:
        success = await k8s_manager.scale_deployment(name, replicas)
        
        if success:
            return {
                "deployment_name": name,
                "replicas": replicas,
                "status": "scaled",
                "message": f"Deployment {name} scaled to {replicas} replicas"
            }
        else:
            raise HTTPException(status_code=500, detail="Failed to scale deployment")
            
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e)) 