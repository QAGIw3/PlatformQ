"""
Graph Intelligence Integration

Manages interactions with the Graph Intelligence Service for
federated knowledge graph operations and trust scoring.
"""

import asyncio
from typing import Dict, Any, List, Optional
import logging
from datetime import datetime
import json

from .graph_intelligence_client import GraphIntelligenceClient
from .ignite_cache import IgniteCache
from .pulsar_publisher import PulsarEventPublisher

logger = logging.getLogger(__name__)


class GraphIntelligenceIntegration:
    """Integration with Graph Intelligence Service"""
    
    def __init__(
        self,
        graph_service_url: str,
        ignite_cache: IgniteCache,
        pulsar_publisher: PulsarEventPublisher
    ):
        self.graph_client = GraphIntelligenceClient(graph_service_url)
        self.ignite = ignite_cache
        self.pulsar = pulsar_publisher
    
    async def register_dataset_node(
        self,
        dataset_id: str,
        dataset_info: Dict[str, Any],
        creator_id: str
    ) -> str:
        """Register a dataset in the federated knowledge graph"""
        
        # Get creator trust scores
        creator_trust = await self.graph_client.get_trust_scores(creator_id)
        
        # Create dataset node
        node_properties = {
            "name": dataset_info.get("name"),
            "type": dataset_info.get("dataset_type"),
            "size_bytes": dataset_info.get("size_bytes"),
            "num_samples": dataset_info.get("num_samples"),
            "quality_score": dataset_info.get("quality_score", 0.0),
            "created_at": datetime.utcnow().isoformat(),
            "visibility": "TENANT" if dataset_info.get("private") else "PUBLIC"
        }
        
        node_id = await self.graph_client.create_knowledge_node(
            node_type="dataset",
            node_id=dataset_id,
            properties=node_properties
        )
        
        # Create creator edge
        await self.graph_client.create_knowledge_edge(
            source_node_id=creator_id,
            target_node_id=dataset_id,
            edge_type="created",
            properties={
                "timestamp": datetime.utcnow().isoformat(),
                "trust_score": creator_trust.get("overall_trust", 0.5)
            },
            confidence=1.0
        )
        
        # Publish to knowledge graph topic
        await self.pulsar.publish_knowledge_node({
            "node_id": node_id,
            "node_type": "dataset",
            "service_origin": "dataset-marketplace",
            "properties": node_properties,
            "trust_score": creator_trust.get("overall_trust", 0.5)
        })
        
        # Cache the node
        await self.ignite.put(
            "knowledge_nodes",
            f"dataset:{dataset_id}",
            node_properties,
            ttl=3600  # 1 hour cache
        )
        
        return node_id
    
    async def register_quality_assessment(
        self,
        assessment_id: str,
        dataset_id: str,
        assessor_id: str,
        quality_score: float,
        dimensions: Dict[str, float]
    ):
        """Register a quality assessment in the knowledge graph"""
        
        # Create quality check node
        node_properties = {
            "score": quality_score,
            "dimensions": dimensions,
            "timestamp": datetime.utcnow().isoformat()
        }
        
        check_node_id = await self.graph_client.create_knowledge_node(
            node_type="quality_check",
            node_id=assessment_id,
            properties=node_properties
        )
        
        # Create assessment edges
        await self.graph_client.create_knowledge_edge(
            source_node_id=dataset_id,
            target_node_id=check_node_id,
            edge_type="assessed_by",
            properties={
                "timestamp": datetime.utcnow().isoformat(),
                "method": "automated"
            },
            confidence=quality_score
        )
        
        await self.graph_client.create_knowledge_edge(
            source_node_id=assessor_id,
            target_node_id=check_node_id,
            edge_type="performed",
            properties={
                "timestamp": datetime.utcnow().isoformat()
            }
        )
    
    async def get_dataset_trust_chain(self, dataset_id: str) -> List[Dict[str, Any]]:
        """Get the trust chain for a dataset"""
        
        # Check cache first
        cached = await self.ignite.get("trust_chains", dataset_id)
        if cached:
            return cached
        
        # Query graph for trust chain
        trust_chain = await self.graph_client.get_trust_chain(dataset_id)
        
        # Calculate aggregated trust
        if trust_chain:
            trust_scores = [node["trust_score"] for node in trust_chain]
            aggregated_trust = sum(trust_scores) / len(trust_scores)
            weakest_link = min(trust_scores)
            
            result = {
                "trust_chain": trust_chain,
                "aggregated_trust_score": aggregated_trust,
                "weakest_link_score": weakest_link,
                "chain_length": len(trust_chain)
            }
            
            # Cache the result
            await self.ignite.put(
                "trust_chains",
                dataset_id,
                result,
                ttl=1800  # 30 minutes cache
            )
            
            return result
        
        return {"trust_chain": [], "aggregated_trust_score": 0.0, "weakest_link_score": 0.0}
    
    async def find_similar_datasets(
        self,
        dataset_id: str,
        similarity_threshold: float = 0.7
    ) -> List[Dict[str, Any]]:
        """Find similar datasets using graph intelligence"""
        
        # Query for similar datasets
        query = f"""
            g.V().has('dataset', 'id', '{dataset_id}')
              .aggregate('self')
              .repeat(
                both().simplePath()
              ).times(2)
              .has('label', 'dataset')
              .where(without('self'))
              .dedup()
              .limit(10)
        """
        
        results = await self.graph_client.query_graph(
            query=query,
            query_type="GREMLIN",
            max_hops=2
        )
        
        # Calculate similarity scores
        similar_datasets = []
        for result in results:
            # Simple similarity based on shared connections
            similarity_score = self._calculate_similarity(dataset_id, result["id"])
            if similarity_score >= similarity_threshold:
                similar_datasets.append({
                    "dataset_id": result["id"],
                    "name": result.get("name"),
                    "similarity_score": similarity_score,
                    "shared_attributes": result.get("shared_attributes", [])
                })
        
        return sorted(similar_datasets, key=lambda x: x["similarity_score"], reverse=True)
    
    def _calculate_similarity(self, dataset1_id: str, dataset2_id: str) -> float:
        """Calculate similarity between two datasets"""
        # Simplified similarity calculation
        # In a real implementation, this would use embeddings, graph metrics, etc.
        return 0.75  # Placeholder
    
    async def get_trust_weighted_recommendations(
        self,
        user_id: str,
        dataset_type: Optional[str] = None,
        min_trust: float = 0.5
    ) -> List[Dict[str, Any]]:
        """Get dataset recommendations weighted by trust"""
        
        # Get user trust scores
        user_trust = await self.graph_client.get_trust_scores(user_id)
        
        # Query for recommendations
        query = f"""
            g.V().has('user', 'id', '{user_id}')
              .out('purchased', 'accessed', 'rated')
              .aggregate('accessed')
              .in('purchased', 'accessed', 'rated')
              .out('purchased', 'accessed', 'rated')
              .has('label', 'dataset')
              .where(without('accessed'))
              .dedup()
              .limit(20)
        """
        
        if dataset_type:
            query = query.replace(".limit(20)", f".has('type', '{dataset_type}').limit(20)")
        
        recommendations = await self.graph_client.query_graph(
            query=query,
            query_type="GREMLIN",
            trust_threshold=min_trust
        )
        
        # Weight by trust
        weighted_recommendations = []
        for rec in recommendations:
            # Get dataset creator trust
            creator_trust_query = f"""
                g.V().has('dataset', 'id', '{rec["id"]}')
                  .in('created')
                  .values('trust_score')
            """
            creator_trust_result = await self.graph_client.query_graph(creator_trust_query)
            creator_trust = creator_trust_result[0] if creator_trust_result else 0.5
            
            # Calculate recommendation score
            rec_score = rec.get("score", 0.5) * creator_trust * user_trust.get("overall_trust", 0.5)
            
            if rec_score >= min_trust:
                weighted_recommendations.append({
                    "dataset_id": rec["id"],
                    "name": rec.get("name"),
                    "type": rec.get("type"),
                    "recommendation_score": rec_score,
                    "creator_trust": creator_trust,
                    "quality_score": rec.get("quality_score", 0.0)
                })
        
        return sorted(weighted_recommendations, key=lambda x: x["recommendation_score"], reverse=True)[:10]
    
    async def close(self):
        """Close connections"""
        await self.graph_client.close() 