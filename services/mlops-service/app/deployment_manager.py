"""
Deployment Manager

Manages model deployments to Knative via functions-service.
"""

import logging
from typing import Dict, Any, List, Optional
from datetime import datetime
import httpx
import hashlib
import json
import uuid

from mlflow.tracking import MlflowClient

logger = logging.getLogger(__name__)


class DeploymentManager:
    """Manages model deployments"""
    
    def __init__(self, functions_service_url: str, mlflow_client: MlflowClient):
        self.functions_service_url = functions_service_url
        self.mlflow_client = mlflow_client
        self.deployments = {}  # In production, use persistent storage
        
    async def deploy_model(self,
                         tenant_id: str,
                         model_name: str,
                         version: Optional[str],
                         environment: str,
                         deployment_config: Dict[str, Any],
                         canary_config: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        """Deploy a model to specified environment"""
        full_model_name = f"{tenant_id}_{model_name}"
        
        # Get model version
        if version is None:
            model_version = self.mlflow_client.get_latest_versions(
                full_model_name,
                stages=["Production", "Staging", "None"]
            )[0]
            version = model_version.version
        else:
            model_version = self.mlflow_client.get_model_version(full_model_name, version)
        
        # Generate watermark for IP protection
        watermark = self._generate_watermark(tenant_id, model_name, version)
        
        # Prepare deployment request
        deployment_request = {
            "model_name": model_name,
            "model_version": version,
            "deployment_name": f"{model_name}-{environment}-v{version}",
            "resources": deployment_config.get("resources", {
                "cpu": deployment_config.get("cpu", "1000m"),
                "memory": deployment_config.get("memory", "2Gi")
            }),
            "autoscaling": deployment_config.get("autoscaling", {
                "min": 1,
                "max": 10,
                "target_cpu": 80
            }),
            "environment_variables": {
                "ENVIRONMENT": environment,
                "MODEL_NAME": model_name,
                "MODEL_VERSION": version,
                "TENANT_ID": tenant_id,
                "MODEL_WATERMARK": watermark
            }
        }
        
        # Call functions service to deploy
        async with httpx.AsyncClient() as client:
            response = await client.post(
                f"{self.functions_service_url}/api/v1/models/deployments",
                json=deployment_request,
                headers={"X-Tenant-ID": tenant_id}
            )
            
            if response.status_code != 200:
                raise Exception(f"Deployment failed: {response.text}")
                
            deployment_info = response.json()
            
        # Handle canary deployment if specified
        if canary_config and environment == "production":
            await self._configure_canary_deployment(
                tenant_id,
                model_name,
                version,
                canary_config,
                deployment_info
            )
            
        # Store deployment info
        deployment_key = f"{tenant_id}:{model_name}:{environment}"
        self.deployments[deployment_key] = {
            "deployment_id": deployment_info["deployment_id"],
            "model_name": model_name,
            "version": version,
            "environment": environment,
            "status": "deployed",
            "endpoint_url": deployment_info.get("endpoint_url"),
            "created_at": datetime.utcnow().isoformat(),
            "deployment_config": deployment_config,
            "canary_config": canary_config
        }
        
        return {
            "deployment_id": deployment_info["deployment_id"],
            "model_name": model_name,
            "version": version,
            "environment": environment,
            "status": "deployed",
            "endpoint_url": deployment_info.get("endpoint_url")
        }
        
    async def _configure_canary_deployment(self,
                                         tenant_id: str,
                                         model_name: str,
                                         new_version: str,
                                         canary_config: Dict[str, Any],
                                         deployment_info: Dict[str, Any]):
        """Configure canary deployment with traffic splitting"""
        # This would configure Knative traffic splitting
        # For now, it's a placeholder
        traffic_percentage = canary_config.get("traffic_percentage", 10)
        logger.info(f"Configuring canary deployment with {traffic_percentage}% traffic")
        
    async def list_deployments(self, tenant_id: str) -> List[Dict[str, Any]]:
        """List all deployments for a tenant"""
        # Call functions service to get deployments
        async with httpx.AsyncClient() as client:
            response = await client.get(
                f"{self.functions_service_url}/api/v1/models/deployments",
                headers={"X-Tenant-ID": tenant_id}
            )
            
            if response.status_code == 200:
                return response.json()
            else:
                logger.error(f"Failed to list deployments: {response.text}")
                return []
                
    async def get_deployment_status(self, 
                                  tenant_id: str,
                                  deployment_id: str) -> Dict[str, Any]:
        """Get deployment status"""
        # In production, query actual deployment status
        for key, deployment in self.deployments.items():
            if deployment["deployment_id"] == deployment_id:
                return deployment
                
        return {"status": "not_found"}
        
    async def scale_deployment(self,
                             tenant_id: str,
                             deployment_id: str,
                             replicas: int) -> Dict[str, Any]:
        """Scale a deployment"""
        # This would call Knative to scale the deployment
        logger.info(f"Scaling deployment {deployment_id} to {replicas} replicas")
        
        return {
            "deployment_id": deployment_id,
            "replicas": replicas,
            "status": "scaled"
        }
        
    async def rollback_deployment(self,
                                tenant_id: str,
                                model_name: str,
                                environment: str,
                                target_version: str) -> Dict[str, Any]:
        """Rollback to a previous model version"""
        # Deploy the target version
        result = await self.deploy_model(
            tenant_id=tenant_id,
            model_name=model_name,
            version=target_version,
            environment=environment,
            deployment_config={"replicas": 2}  # Default config
        )
        
        return {
            **result,
            "rollback": True,
            "previous_version": target_version
        } 

    def _generate_watermark(self, tenant_id: str, model_name: str, version: str) -> str:
        """Generate unique watermark for model IP protection"""
        watermark_data = {
            "tenant_id": tenant_id,
            "model_name": model_name,
            "version": version,
            "timestamp": datetime.utcnow().isoformat(),
            "deployment_id": str(uuid.uuid4())
        }
        
        # Create hash-based watermark
        watermark_string = json.dumps(watermark_data, sort_keys=True)
        watermark_hash = hashlib.sha256(watermark_string.encode()).hexdigest()
        
        # Store watermark for verification
        self.deployments[f"watermark_{watermark_hash}"] = watermark_data
        
        return watermark_hash 