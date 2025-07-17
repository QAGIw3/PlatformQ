import logging
import httpx
from typing import Dict, Any, List

from ..core.config import settings

logger = logging.getLogger(__name__)

class GraphFeatureService:
    def __init__(self):
        self.graph_intelligence_url = settings.graph_intelligence_service_url

    async def get_asset_features(self, asset_ids: List[str]) -> Dict[str, Dict[str, Any]]:
        """
        Get graph-based features for a list of assets.
        """
        features = {}
        async with httpx.AsyncClient() as client:
            for asset_id in asset_ids:
                try:
                    # Get lineage information
                    lineage_res = await client.get(f"{self.graph_intelligence_url}/api/v1/graph/assets/{asset_id}/lineage")
                    lineage_res.raise_for_status()
                    lineage_data = lineage_res.json()
                    
                    # Example features
                    features[asset_id] = {
                        "downstream_lineage_depth": len(lineage_data.get("lineage", [])),
                        # We could add more features here, e.g., number of owners
                    }
                    
                except httpx.RequestError as e:
                    logger.error(f"Could not connect to graph intelligence service: {e}")
                    features[asset_id] = {} # Return empty features on error
                except httpx.HTTPStatusError as e:
                    logger.warning(f"Failed to get lineage for asset {asset_id}: {e.response.status_code}")
                    features[asset_id] = {}

        return features

graph_feature_service = GraphFeatureService() 