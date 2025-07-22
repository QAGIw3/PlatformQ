"""ML Platform Service integration client"""

import httpx
from typing import Dict, Any, List, Optional
import structlog

logger = structlog.get_logger()


class MLPlatformClient:
    """Client for ML Platform Service integration"""
    
    def __init__(self, base_url: str):
        self.base_url = base_url
        self.client = httpx.AsyncClient(timeout=300.0)  # Longer timeout for ML operations
        
    async def submit_training(self, config: Dict[str, Any]) -> Dict[str, Any]:
        """Submit ML training job"""
        try:
            response = await self.client.post(
                f"{self.base_url}/api/v1/training/submit",
                json={
                    "dataset_id": config.get("dataset_id"),
                    "algorithm": config.get("algorithm", "xgboost"),
                    "hyperparameters": config.get("hyperparameters", {}),
                    "compute_requirements": config.get("compute_requirements", {})
                }
            )
            response.raise_for_status()
            return response.json()
            
        except Exception as e:
            logger.error(f"Training submission failed: {e}")
            raise
            
    async def get_training_status(self, training_id: str) -> Dict[str, Any]:
        """Get training job status"""
        try:
            response = await self.client.get(
                f"{self.base_url}/api/v1/training/{training_id}"
            )
            response.raise_for_status()
            return response.json()
            
        except Exception as e:
            logger.error(f"Failed to get training status: {e}")
            raise
            
    async def deploy_model(self, model_id: str, config: Dict[str, Any]) -> Dict[str, Any]:
        """Deploy trained model"""
        try:
            response = await self.client.post(
                f"{self.base_url}/api/v1/models/{model_id}/deploy",
                json={
                    "environment": config.get("environment", "production"),
                    "instances": config.get("instances", 1),
                    "resources": config.get("resources", {})
                }
            )
            response.raise_for_status()
            return response.json()
            
        except Exception as e:
            logger.error(f"Model deployment failed: {e}")
            raise
            
    async def close(self):
        """Close the client"""
        await self.client.aclose() 