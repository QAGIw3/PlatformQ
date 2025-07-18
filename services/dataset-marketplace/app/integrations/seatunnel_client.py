"""
Apache SeaTunnel Client
"""

import httpx
from typing import Dict, Any, List, Optional
import logging

logger = logging.getLogger(__name__)


class SeaTunnelClient:
    """Client for Apache SeaTunnel data integration"""
    
    def __init__(self, base_url: str = "http://seatunnel-api:8080"):
        self.base_url = base_url
        self.client = httpx.AsyncClient(timeout=60.0)
    
    async def create_job(self, job_config: Dict[str, Any]) -> str:
        """Create a new SeaTunnel job"""
        try:
            response = await self.client.post(
                f"{self.base_url}/api/v1/jobs",
                json=job_config
            )
            response.raise_for_status()
            return response.json()["job_id"]
        except httpx.HTTPError as e:
            logger.error(f"Failed to create SeaTunnel job: {e}")
            raise
    
    async def start_job(self, job_id: str):
        """Start a SeaTunnel job"""
        try:
            response = await self.client.post(
                f"{self.base_url}/api/v1/jobs/{job_id}/start"
            )
            response.raise_for_status()
        except httpx.HTTPError as e:
            logger.error(f"Failed to start SeaTunnel job: {e}")
            raise
    
    async def stop_job(self, job_id: str):
        """Stop a SeaTunnel job"""
        try:
            response = await self.client.post(
                f"{self.base_url}/api/v1/jobs/{job_id}/stop"
            )
            response.raise_for_status()
        except httpx.HTTPError as e:
            logger.error(f"Failed to stop SeaTunnel job: {e}")
    
    async def get_job_status(self, job_id: str) -> Dict[str, Any]:
        """Get job status"""
        try:
            response = await self.client.get(
                f"{self.base_url}/api/v1/jobs/{job_id}/status"
            )
            response.raise_for_status()
            return response.json()
        except httpx.HTTPError as e:
            logger.error(f"Failed to get job status: {e}")
            return {"status": "unknown"}
    
    async def create_pipeline(self, pipeline_config: Dict[str, Any]) -> str:
        """Create a data pipeline"""
        return await self.create_job(pipeline_config)
    
    async def close(self):
        """Close the HTTP client"""
        await self.client.aclose() 