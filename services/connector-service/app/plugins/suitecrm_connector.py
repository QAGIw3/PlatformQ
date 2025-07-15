from .base import BaseConnector
from typing import Optional, Dict, Any, List
import asyncio
import uuid

class SuiteCRMConnector(BaseConnector):
    """
    A connector for ingesting data from a SuiteCRM instance.
    
    This PoC connector simulates fetching contacts from the SuiteCRM API,
    transforming them into the platform's DigitalAsset format, and creating
    them via the digital-asset-service.
    """

    @property
    def connector_type(self) -> str:
        return "suitecrm"

    @property
    def schedule(self) -> Optional[str]:
        # Run every 5 minutes for demonstration purposes.
        return "*/5 * * * *"

    async def _fetch_mock_contacts_from_api(self) -> List[Dict[str, Any]]:
        """
        Simulates making an API call to SuiteCRM to get recent contacts.
        """
        print(f"[{self.connector_type}] Fetching contacts from SuiteCRM API...")
        await asyncio.sleep(1) # Simulate network latency
        
        # In a real implementation, you would use httpx to call the API:
        # url = f"{self.config['api_url']}/V8/module/Accounts"
        # headers = {"Authorization": f"Bearer {self.config['api_key']}"}
        # response = await client.get(url, headers=headers)
        # response.raise_for_status()
        # return response.json()['data']

        return [
            {
                "id": "crm-contact-101",
                "attributes": { "name": "Innovate Corp", "email1": "contact@innovate.com" }
            },
            {
                "id": "crm-contact-102",
                "attributes": { "name": "Synergy Solutions", "email1": "info@synergysolutions.net" }
            },
        ]

    async def run(self, context: Optional[Dict[str, Any]] = None):
        """
        The core logic for the SuiteCRM connector.
        
        It fetches contacts and creates a corresponding Digital Asset for each one.
        """
        print(f"[{self.connector_type}] Running scheduled job.")
        try:
            contacts = await self._fetch_mock_contacts_from_api()
            
            for contact in contacts:
                asset_data = {
                    "asset_name": contact["attributes"]["name"],
                    "asset_type": "CRM_CONTACT",
                    "owner_id": uuid.UUID("00000000-0000-0000-0000-000000000001"), # Placeholder owner
                    "source_tool": self.connector_type,
                    "source_asset_id": contact["id"],
                    "metadata": {
                        "email": contact["attributes"]["email1"]
                    }
                }
                await self._create_digital_asset(asset_data)
        
        except Exception as e:
            print(f"[{self.connector_type}] Error during execution: {e}") 