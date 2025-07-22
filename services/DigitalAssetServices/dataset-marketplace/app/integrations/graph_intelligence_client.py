"""
Graph Intelligence Service Client
"""

import httpx
from typing import Dict, Any, List, Optional
import logging
from datetime import datetime

logger = logging.getLogger(__name__)


class GraphIntelligenceClient:
    """Client for interacting with Graph Intelligence Service"""
    
    def __init__(self, base_url: str = "http://graph-intelligence-service:8000"):
        self.base_url = base_url
        self.client = httpx.AsyncClient(timeout=30.0)
    
    async def get_trust_scores(self, entity_id: str) -> Dict[str, float]:
        """Get multi-dimensional trust scores for an entity"""
        try:
            response = await self.client.get(
                f"{self.base_url}/api/v1/trust/scores/{entity_id}"
            )
            response.raise_for_status()
            return response.json()
        except httpx.HTTPError as e:
            logger.error(f"Failed to get trust scores: {e}")
            return {}
    
    async def calculate_trust_weighted_score(
        self,
        base_score: float,
        assessor_trust: float,
        dimensions: Optional[Dict[str, float]] = None
    ) -> float:
        """Calculate trust-weighted score"""
        try:
            response = await self.client.post(
                f"{self.base_url}/api/v1/trust/calculate",
                json={
                    "base_score": base_score,
                    "assessor_trust": assessor_trust,
                    "dimensions": dimensions or {}
                }
            )
            response.raise_for_status()
            return response.json()["weighted_score"]
        except httpx.HTTPError as e:
            logger.error(f"Failed to calculate trust-weighted score: {e}")
            return base_score * assessor_trust  # Fallback calculation
    
    async def create_knowledge_node(
        self,
        node_type: str,
        node_id: str,
        properties: Dict[str, Any],
        service_origin: str = "dataset-marketplace"
    ) -> str:
        """Create a node in the federated knowledge graph"""
        try:
            response = await self.client.post(
                f"{self.base_url}/api/v1/graph/nodes",
                json={
                    "node_type": node_type,
                    "node_id": node_id,
                    "properties": properties,
                    "service_origin": service_origin,
                    "timestamp": datetime.utcnow().isoformat()
                }
            )
            response.raise_for_status()
            return response.json()["node_id"]
        except httpx.HTTPError as e:
            logger.error(f"Failed to create knowledge node: {e}")
            raise
    
    async def create_knowledge_edge(
        self,
        source_node_id: str,
        target_node_id: str,
        edge_type: str,
        properties: Optional[Dict[str, Any]] = None,
        confidence: float = 1.0
    ) -> str:
        """Create an edge in the federated knowledge graph"""
        try:
            response = await self.client.post(
                f"{self.base_url}/api/v1/graph/edges",
                json={
                    "source_node_id": source_node_id,
                    "target_node_id": target_node_id,
                    "edge_type": edge_type,
                    "properties": properties or {},
                    "confidence": confidence,
                    "service_origin": "dataset-marketplace"
                }
            )
            response.raise_for_status()
            return response.json()["edge_id"]
        except httpx.HTTPError as e:
            logger.error(f"Failed to create knowledge edge: {e}")
            raise
    
    async def query_graph(
        self,
        query: str,
        query_type: str = "GREMLIN",
        max_hops: int = 3,
        trust_threshold: float = 0.0
    ) -> List[Dict[str, Any]]:
        """Query the federated knowledge graph"""
        try:
            response = await self.client.post(
                f"{self.base_url}/api/v1/graph/query",
                json={
                    "query": query,
                    "query_type": query_type,
                    "max_hops": max_hops,
                    "trust_threshold": trust_threshold
                }
            )
            response.raise_for_status()
            return response.json()["results"]
        except httpx.HTTPError as e:
            logger.error(f"Failed to query graph: {e}")
            return []
    
    async def get_trust_chain(self, dataset_id: str) -> List[Dict[str, Any]]:
        """Get trust chain for a dataset"""
        try:
            response = await self.client.get(
                f"{self.base_url}/api/v1/trust/chain/{dataset_id}"
            )
            response.raise_for_status()
            return response.json()["trust_chain"]
        except httpx.HTTPError as e:
            logger.error(f"Failed to get trust chain: {e}")
            return []
    
    async def close(self):
        """Close the HTTP client"""
        await self.client.aclose() 