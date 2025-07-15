from abc import ABC, abstractmethod
from typing import Optional, Dict, Any

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
        print(f"Initialized connector: {self.connector_type}")

    async def _create_digital_asset(self, asset_data: Dict[str, Any]):
        """
        A helper method to call the digital-asset-service to create an asset.
        This centralizes the logic for interacting with the asset service.
        """
        # TODO: Implement the HTTP call to the digital-asset-service using httpx.
        print(f"[{self.connector_type}] Creating Digital Asset: {asset_data.get('asset_name')}")
        pass
