from .base import BaseConnector
from typing import Optional, Dict, Any, List
import asyncio
import uuid

class MetasfreshConnector(BaseConnector):
    """
    A connector for ingesting data from a Metasfresh ERP instance.
    
    This PoC connector simulates fetching business partners from the API.
    """

    @property
    def connector_type(self) -> str:
        return "metasfresh"

    @property
    def schedule(self) -> Optional[str]:
        # Run once every hour
        return "0 * * * *"

    async def _fetch_mock_bpartners_from_api(self) -> List[Dict[str, Any]]:
        """
        Simulates making an API call to Metasfresh to get recent business partners.
        """
        print(f"[{self.connector_type}] Fetching business partners from Metasfresh API...")
        await asyncio.sleep(1) # Simulate network latency
        
        # In a real implementation, you would use httpx to call the API:
        # url = f"{self.config['api_url']}/bpartner"
        # headers = {"Authorization": f"Bearer {self.config['api_key']}"}
        # response = await client.get(url, headers=headers)
        # response.raise_for_status()
        # return response.json()

        return [
            { "id": "mf-bp-201", "name": "Global Supplies Ltd.", "value": "GS-001" },
            { "id": "mf-bp-202", "name": "Innovate Manufacturing", "value": "IM-002" },
        ]

    async def run(self, context: Optional[Dict[str, Any]] = None):
        """
        The core logic for the Metasfresh connector.
        """
        print(f"[{self.connector_type}] Running scheduled job.")
        try:
            bpartners = await self._fetch_mock_bpartners_from_api()
            
            for partner in bpartners:
                asset_data = {
                    "asset_name": partner["name"],
                    "asset_type": "ERP_BPARTNER",
                    "owner_id": uuid.UUID("00000000-0000-0000-0000-000000000001"), # Placeholder
                    "source_tool": self.connector_type,
                    "source_asset_id": partner["id"],
                    "metadata": {
                        "value": partner["value"]
                    }
                }
                await self._create_digital_asset(asset_data)
        
        except Exception as e:
            print(f"[{self.connector_type}] Error during execution: {e}") 