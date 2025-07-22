from .base import BaseConnector
from typing import Optional, Dict, Any
import asyncio
import uuid
import httpx

OVERPASS_API_URL = "https://overpass-api.de/api/interpreter"

class OpenstreetmapConnector(BaseConnector):
    """
    A connector for fetching data from the OpenStreetMap Overpass API.
    """

    @property
    def connector_type(self) -> str:
        return "openstreetmap"

    async def _query_overpass(self, query: str) -> Dict[str, Any]:
        """
        Sends a query to the Overpass API and returns the JSON response.
        """
        print(f"[{self.connector_type}] Querying Overpass API...")
        async with httpx.AsyncClient() as client:
            response = await client.post(OVERPASS_API_URL, data=query, timeout=60.0)
            response.raise_for_status()
            return response.json()

    async def run(self, context: Optional[Dict[str, Any]] = None):
        """
        The core logic for the OpenStreetMap connector.
        
        It expects an 'overpass_query' in the context.
        """
        if not context or "overpass_query" not in context:
            print(f"[{self.connector_type}] Skipping: 'overpass_query' not found in context.")
            return

        query = context["overpass_query"]
        asset_name = context.get("asset_name", f"OSM Query - {uuid.uuid4().hex[:8]}")

        print(f"[{self.connector_type}] Running job for: {asset_name}")
        try:
            geojson_data = await self._query_overpass(query)
            
            asset_data = {
                "asset_name": asset_name,
                "asset_type": "GEOJSON_FEATURE_COLLECTION",
                "owner_id": uuid.UUID("00000000-0000-0000-0000-000000000001"), # Placeholder
                "source_tool": self.connector_type,
                "source_asset_id": f"overpass_query_{uuid.uuid4().hex}",
                "metadata": {
                    "query": query,
                    "element_count": len(geojson_data.get("elements", [])),
                },
                # In a real app, the large GeoJSON payload would be stored
                # in a file and linked, not embedded directly.
                # "raw_data_uri": "s3://..."
            }
            await self._create_digital_asset(asset_data)
        
        except Exception as e:
            print(f"[{self.connector_type}] Error during execution: {e}") 