"""
Graph Intelligence Service Client

Provides a client interface for communicating with the Graph Intelligence Service
for graph-based operations required by the fraud detection functionality.
"""

import logging
import httpx
from typing import Dict, Any, List, Optional
from datetime import datetime

logger = logging.getLogger(__name__)


class GraphIntelligenceClient:
    """
    Client for Graph Intelligence Service communication
    """
    
    def __init__(self, base_url: str = "http://graph-intelligence-service:8000"):
        self.base_url = base_url
        self.client = httpx.AsyncClient(timeout=30.0)
        
    async def close(self):
        """Close the HTTP client"""
        await self.client.aclose()
        
    async def submit_graph_analytics_job(
        self,
        algorithm: str,
        tenant_id: str,
        parameters: Dict[str, Any]
    ) -> str:
        """
        Submit a graph analytics job to the Graph Intelligence Service
        
        Args:
            algorithm: Algorithm to run (e.g., 'fraud_detection')
            tenant_id: Tenant ID
            parameters: Algorithm parameters
            
        Returns:
            Job ID
        """
        try:
            response = await self.client.post(
                f"{self.base_url}/api/v1/graph/algorithm/run",
                json={
                    "algorithm": algorithm,
                    "parameters": parameters,
                    "entity_filter": {"tenant_id": tenant_id},
                    "save_results": True
                },
                headers={"X-Tenant-ID": tenant_id}
            )
            response.raise_for_status()
            result = response.json()
            return result["job_id"]
            
        except Exception as e:
            logger.error(f"Error submitting graph analytics job: {e}")
            raise
            
    async def get_job_results(self, job_id: str, tenant_id: str) -> Optional[Dict[str, Any]]:
        """
        Get results of a graph analytics job
        
        Args:
            job_id: Job ID to retrieve results for
            tenant_id: Tenant ID
            
        Returns:
            Job results or None if still processing
        """
        try:
            response = await self.client.get(
                f"{self.base_url}/api/v1/jobs/{job_id}/result",
                headers={"X-Tenant-ID": tenant_id}
            )
            
            if response.status_code == 404:
                return None
                
            response.raise_for_status()
            return response.json()
            
        except Exception as e:
            logger.error(f"Error getting job results: {e}")
            raise
            
    async def query_entity_properties(
        self,
        entity_ids: List[str],
        properties: List[str],
        tenant_id: str
    ) -> List[Dict[str, Any]]:
        """
        Query specific properties for entities using Gremlin
        
        Args:
            entity_ids: List of entity IDs to query
            properties: Properties to retrieve
            tenant_id: Tenant ID
            
        Returns:
            List of entity property maps
        """
        try:
            response = await self.client.post(
                f"{self.base_url}/api/v1/graph/query/entities",
                json={
                    "entity_ids": entity_ids,
                    "properties": properties
                },
                headers={"X-Tenant-ID": tenant_id}
            )
            response.raise_for_status()
            result = response.json()
            return result.get("entities", [])
            
        except Exception as e:
            logger.error(f"Error querying entity properties: {e}")
            raise
            
    async def get_entity_relationships(
        self,
        entity_id: str,
        relationship_types: List[str],
        depth: int,
        tenant_id: str
    ) -> Dict[str, Any]:
        """
        Get relationships for an entity up to a certain depth
        
        Args:
            entity_id: Entity to start from
            relationship_types: Types of relationships to follow
            depth: Maximum traversal depth
            tenant_id: Tenant ID
            
        Returns:
            Graph of relationships
        """
        try:
            response = await self.client.post(
                f"{self.base_url}/api/v1/graph/query",
                json={
                    "query_type": "relationships",
                    "entity_id": entity_id,
                    "relationship_types": relationship_types,
                    "max_depth": depth
                },
                headers={"X-Tenant-ID": tenant_id}
            )
            response.raise_for_status()
            return response.json()
            
        except Exception as e:
            logger.error(f"Error getting entity relationships: {e}")
            raise
            
    async def update_entity_fraud_properties(
        self,
        entity_id: str,
        fraud_score: float,
        is_suspicious: bool,
        fraud_indicators: List[str],
        tenant_id: str
    ) -> bool:
        """
        Update fraud-related properties for an entity
        
        Args:
            entity_id: Entity to update
            fraud_score: Calculated fraud score
            is_suspicious: Whether entity is flagged as suspicious
            fraud_indicators: List of fraud indicators found
            tenant_id: Tenant ID
            
        Returns:
            Success status
        """
        try:
            response = await self.client.put(
                f"{self.base_url}/api/v1/graph/entities/{entity_id}/properties",
                json={
                    "fraud_score": fraud_score,
                    "is_suspicious": is_suspicious,
                    "fraud_indicators": fraud_indicators,
                    "fraud_last_checked": datetime.utcnow().isoformat()
                },
                headers={"X-Tenant-ID": tenant_id}
            )
            response.raise_for_status()
            return True
            
        except Exception as e:
            logger.error(f"Error updating entity fraud properties: {e}")
            return False 