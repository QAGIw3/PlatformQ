from .base import BaseConnector
from typing import Optional, Dict, Any
import uuid

class WebhookConnector(BaseConnector):
    """
    A generic connector for ingesting data from webhooks.
    
    This connector is designed to be triggered by an HTTP request to the
    connector-service. It takes the JSON body of the webhook, treats it
    as a new Digital Asset, and persists it.
    """

    @property
    def connector_type(self) -> str:
        # This can be overridden by subclasses for more specific webhooks
        return "generic_webhook"

    async def run(self, context: Optional[Dict[str, Any]] = None):
        """
        The core logic for the Webhook connector.
        
        It transforms the webhook payload into a Digital Asset.
        """
        if not context or "payload" not in context:
            print(f"[{self.connector_type}] Skipping run: no payload in context.")
            return

        payload = context["payload"]
        
        # Create a name for the asset, defaulting to the asset type
        asset_name = payload.get("name", payload.get("id", self.connector_type))

        print(f"[{self.connector_type}] Processing webhook payload for asset: {asset_name}")

        try:
            asset_data = {
                "asset_name": asset_name,
                "asset_type": "WEBHOOK_PAYLOAD",
                "owner_id": uuid.UUID("00000000-0000-0000-0000-000000000001"), # Placeholder owner
                "source_tool": self.connector_type,
                "metadata": payload, # Store the entire payload in the metadata
            }
            await self._create_digital_asset(asset_data)
        
        except Exception as e:
            print(f"[{self.connector_type}] Error during execution: {e}") 