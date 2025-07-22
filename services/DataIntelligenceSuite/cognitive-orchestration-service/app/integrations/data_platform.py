"""Data Platform Service integration client"""

import httpx
from typing import Dict, Any, List, Optional
import structlog

logger = structlog.get_logger()


class DataPlatformClient:
    """Client for Data Platform Service integration"""
    
    def __init__(self, base_url: str):
        self.base_url = base_url
        self.client = httpx.AsyncClient(timeout=60.0)
        
    async def execute_query(self, config: Dict[str, Any]) -> Dict[str, Any]:
        """Execute a query through data platform"""
        try:
            response = await self.client.post(
                f"{self.base_url}/api/v1/query/execute",
                json={
                    "query": config.get("query"),
                    "engine": config.get("engine", "trino"),
                    "parameters": config.get("parameters", {})
                }
            )
            response.raise_for_status()
            return response.json()
            
        except Exception as e:
            logger.error(f"Query execution failed: {e}")
            raise
            
    async def apply_transformation(self, config: Dict[str, Any]) -> Dict[str, Any]:
        """Apply data transformation"""
        try:
            response = await self.client.post(
                f"{self.base_url}/api/v1/transform",
                json={
                    "source_path": config.get("source"),
                    "transformations": config.get("transformations", []),
                    "target_path": config.get("target")
                }
            )
            response.raise_for_status()
            return response.json()
            
        except Exception as e:
            logger.error(f"Transformation failed: {e}")
            raise
            
    async def get_pipeline_status(self, pipeline_id: str) -> Dict[str, Any]:
        """Get pipeline status"""
        try:
            response = await self.client.get(
                f"{self.base_url}/api/v1/pipelines/{pipeline_id}"
            )
            response.raise_for_status()
            return response.json()
            
        except Exception as e:
            logger.error(f"Failed to get pipeline status: {e}")
            raise
            
    async def close(self):
        """Close the client"""
        await self.client.aclose() 