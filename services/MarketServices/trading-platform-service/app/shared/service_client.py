"""Service client for inter-service communication."""

import httpx
from typing import Dict, Any, Optional
import logging

logger = logging.getLogger(__name__)


class ServiceClient:
    """HTTP client for service-to-service communication."""
    
    def __init__(self, base_url: str, service_name: str):
        self.base_url = base_url.rstrip('/')
        self.service_name = service_name
        self.client = httpx.AsyncClient(
            base_url=self.base_url,
            timeout=30.0
        )
        
    async def post(self, endpoint: str, data: Dict[str, Any]) -> Dict[str, Any]:
        """Make POST request to service."""
        try:
            response = await self.client.post(endpoint, json=data)
            response.raise_for_status()
            return response.json()
        except httpx.HTTPError as e:
            logger.error(f"HTTP error calling {self.service_name}: {e}")
            raise
        except Exception as e:
            logger.error(f"Error calling {self.service_name}: {e}")
            raise
            
    async def get(self, endpoint: str, params: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        """Make GET request to service."""
        try:
            response = await self.client.get(endpoint, params=params)
            response.raise_for_status()
            return response.json()
        except httpx.HTTPError as e:
            logger.error(f"HTTP error calling {self.service_name}: {e}")
            raise
        except Exception as e:
            logger.error(f"Error calling {self.service_name}: {e}")
            raise
            
    async def delete(self, endpoint: str) -> Dict[str, Any]:
        """Make DELETE request to service."""
        try:
            response = await self.client.delete(endpoint)
            response.raise_for_status()
            return response.json()
        except httpx.HTTPError as e:
            logger.error(f"HTTP error calling {self.service_name}: {e}")
            raise
        except Exception as e:
            logger.error(f"Error calling {self.service_name}: {e}")
            raise
            
    async def close(self):
        """Close the client."""
        await self.client.aclose() 