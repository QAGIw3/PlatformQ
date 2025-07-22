from abc import ABC, abstractmethod
from typing import Optional, Dict, Any
import httpx
import logging
import os

logger = logging.getLogger(__name__)


class BaseConnector(ABC):
    """
    Abstract Base Class for all data connectors.
    
    Each connector must implement the run method, which contains the core
    logic for fetching, transforming, and loading data into the platform.
    """

    @property
    @abstractmethod
    def connector_type(self) -> str:
        """A unique identifier for the connector, e.g., 'suitecrm' or 'blender'."""
        pass

    @property
    def schedule(self) -> Optional[str]:
        """
        An optional cron-style schedule string (e.g., '0 * * * *' for hourly).
        If None, the connector is only triggered manually or by events.
        """
        return None

    @abstractmethod
    async def run(self, context: Optional[Dict[str, Any]] = None):
        """
        The main execution method for the connector.
        
        This method will be called by the service's scheduler or by a trigger.
        The context dictionary can be used to pass in trigger-specific data,
        such as the path to a newly uploaded file.
        """
        pass

    def __init__(self, config: Dict[str, Any]):
        self.config = config
        self.digital_asset_service_url = os.getenv(
            "DIGITAL_ASSET_SERVICE_URL", 
            "http://digital-asset-service:8000"
        )
        print(f"Initialized connector: {self.connector_type}")

    async def _create_digital_asset(self, asset_data: Dict[str, Any]):
        """
        A helper method to call the digital-asset-service to create an asset.
        This centralizes the logic for interacting with the asset service.
        """
        logger.info(f"[{self.connector_type}] Creating Digital Asset: {asset_data.get('asset_name')}")
        
        try:
            async with httpx.AsyncClient() as client:
                # Add tenant_id from config if not already present
                if 'tenant_id' not in asset_data and 'tenant_id' in self.config:
                    asset_data['tenant_id'] = self.config['tenant_id']
                
                response = await client.post(
                    f"{self.digital_asset_service_url}/api/v1/assets",
                    json=asset_data,
                    timeout=30.0
                )
                
                response.raise_for_status()
                result = response.json()
                
                logger.info(f"[{self.connector_type}] Successfully created asset: {result.get('id')}")
                return result
                
        except httpx.HTTPStatusError as e:
            logger.error(f"[{self.connector_type}] HTTP error creating asset: {e.response.status_code} - {e.response.text}")
            raise
        except Exception as e:
            logger.error(f"[{self.connector_type}] Error creating asset: {e}")
            raise
