"""
Federated Graph Integration for Graph Intelligence Service

Shows how the Graph Intelligence Service contributes to and queries
the federated knowledge graph to enhance trust scoring and relationship analysis.
"""

from typing import Dict, List, Optional, Any, Tuple
from datetime import datetime, timedelta
import logging
import asyncio
from enum import Enum

from platformq_shared.federated_knowledge_graph import (
    FederatedKnowledgeGraph,
    NodeType,
    EdgeType,
    GraphNode,
    GraphEdge
)
from platformq_shared import EventPublisher

from app.models import UserReputation, TrustRelationship
from app.engines import TrustScoringEngine, ReputationEngine

logger = logging.getLogger(__name__)


class TrustNodeType(Enum):
    """Extended node types for trust-specific entities"""
    REPUTATION_SCORE = "reputation_score"
    TRUST_ATTESTATION = "trust_attestation"
    SKILL_CREDENTIAL = "skill_credential"
    COLLABORATION_EVENT = "collaboration_event"


class FederatedGraphEnhancement:
    """
    Enhances Graph Intelligence Service with federated knowledge graph capabilities
    """
    
    def __init__(
        self,
        trust_engine: TrustScoringEngine,
        reputation_engine: ReputationEngine,
        event_publisher: EventPublisher,
        janusgraph_host: str = "janusgraph",
        janusgraph_port: int = 8182
    ):
        self.trust_engine = trust_engine
        self.reputation_engine = reputation_engine
        
        # Initialize federated graph client
        self.federated_graph = FederatedKnowledgeGraph(
            service_name="graph-intelligence-service",
            janusgraph_host=janusgraph_host,
            janusgraph_port=janusgraph_port,
            event_publisher=event_publisher
        )
        
        # Track contributed nodes
        self._contributed_nodes = set()
        
    async def initialize(self):
        """Initialize the federated graph connection"""
        await self.federated_graph.connect()
        
        # Set service trust score based on system configuration
        self.federated_graph._service_trust_score = 0.95  # High trust for core service
        
        logger.info("Federated graph integration initialized")
        
    async def contribute_reputation_update(
        self,
        user_id: str,
        reputation_scores: Dict[str, float],
        tenant_id: str
    ) -> GraphNode:
        """
        Contribute a reputation update to the federated graph
        """
        try:
            # Create reputation node
            node = await self.federated_graph.create_node(
                node_type=NodeType.CUSTOM,
                properties={
                    'id': f"rep_{user_id}_{datetime.utcnow().timestamp()}",
                    'user_id': user_id,
                    'technical_prowess': reputation_scores.get('technical_prowess', 0),
                    'collaboration_rating': reputation_scores.get('collaboration_rating', 0),
                    'governance_influence': reputation_scores.get('governance_influence', 0),
                    'creativity_index': reputation_scores.get('creativity_index', 0),
                    'reliability_score': reputation_scores.get('reliability_score', 0),
                    'overall_score': reputation_scores.get('overall_score', 0),
                    'custom_type': TrustNodeType.REPUTATION_SCORE.value
                },
                trust_score=reputation_scores.get('overall_score', 0),
                visibility="TENANT",
                tenant_id=tenant_id
            )
            
            # Link to user node
            user_node_id = f"auth-service:user:{user_id}"
            await self.federated_graph.create_edge(
                source_node_id=user_node_id,
                target_node_id=node.node_id,
                edge_type=EdgeType.OWNS,
                properties={'timestamp': datetime.utcnow().isoformat()},
                confidence=1.0
            )
            
            # Track contribution
            self._contributed_nodes.add(node.node_id)
            
            return node
            
        except Exception as e:
            logger.error(f"Error contributing reputation update: {e}")
            raise
            
    async def contribute_trust_relationship(
        self,
        trustor_id: str,
        trustee_id: str,
        trust_score: float,
        context: str,
        tenant_id: str
    ) -> GraphEdge:
        """
        Contribute a trust relationship to the federated graph
        """
        try:
            # Create trust edge
            trustor_node_id = f"auth-service:user:{trustor_id}"
            trustee_node_id = f"auth-service:user:{trustee_id}"
            
            edge = await self.federated_graph.create_edge(
                source_node_id=trustor_node_id,
                target_node_id=trustee_node_id,
                edge_type=EdgeType.TRUSTS,
                properties={
                    'trust_score': trust_score,
                    'context': context,
                    'tenant_id': tenant_id,
                    'last_updated': datetime.utcnow().isoformat()
                },
                confidence=trust_score,
                trust_weighted=True
            )
            
            return edge
            
        except Exception as e:
            logger.error(f"Error contributing trust relationship: {e}")
            raise
            
    async def contribute_collaboration_event(
        self,
        participants: List[str],
        project_id: str,
        quality_score: float,
        tenant_id: str
    ) -> GraphNode:
        """
        Contribute a collaboration event to the federated graph
        """
        try:
            # Create collaboration event node
            node = await self.federated_graph.create_node(
                node_type=NodeType.CUSTOM,
                properties={
                    'id': f"collab_{project_id}_{datetime.utcnow().timestamp()}",
                    'project_id': project_id,
                    'participant_count': len(participants),
                    'quality_score': quality_score,
                    'custom_type': TrustNodeType.COLLABORATION_EVENT.value
                },
                trust_score=quality_score,
                visibility="TENANT",
                tenant_id=tenant_id
            )
            
            # Link participants
            for participant_id in participants:
                user_node_id = f"auth-service:user:{participant_id}"
                await self.federated_graph.create_edge(
                    source_node_id=user_node_id,
                    target_node_id=node.node_id,
                    edge_type=EdgeType.COLLABORATES_WITH,
                    properties={'role': 'participant'},
                    confidence=1.0
                )
                
            # Link to project
            project_node_id = f"projects-service:project:{project_id}"
            await self.federated_graph.create_edge(
                source_node_id=node.node_id,
                target_node_id=project_node_id,
                edge_type=EdgeType.BELONGS_TO,
                confidence=1.0
            )
            
            return node
            
        except Exception as e:
            logger.error(f"Error contributing collaboration event: {e}")
            raise
            
    async def enhance_trust_calculation(
        self,
        user_id: str,
        base_trust_scores: Dict[str, float]
    ) -> Dict[str, float]:
        """
        Enhance trust calculation using federated graph insights
        """
        try:
            enhanced_scores = base_trust_scores.copy()
            
            # Query user's network in federated graph
            user_node_id = f"auth-service:user:{user_id}"
            
            # Get direct trust relationships
            trust_neighbors = await self.federated_graph.query_neighbors(
                node_id=user_node_id,
                edge_types=[EdgeType.TRUSTS],
                max_hops=2,
                trust_threshold=0.5
            )
            
            # Calculate network trust bonus
            if trust_neighbors:
                avg_neighbor_trust = sum(
                    n.get('trust_score', [0])[0] for n in trust_neighbors
                ) / len(trust_neighbors)
                
                network_bonus = min(0.1, avg_neighbor_trust * 0.15)
                enhanced_scores['network_trust'] = network_bonus
                enhanced_scores['overall_score'] = min(
                    1.0,
                    enhanced_scores['overall_score'] + network_bonus
                )
                
            # Get collaboration quality
            collab_query = f"""
                g.V().has('node_id', '{user_node_id}')
                .out('COLLABORATES_WITH')
                .has('custom_type', '{TrustNodeType.COLLABORATION_EVENT.value}')
                .values('quality_score')
            """
            
            collab_scores = await self.federated_graph.federated_query(collab_query)
            
            if collab_scores:
                avg_collab_quality = sum(collab_scores) / len(collab_scores)
                collab_bonus = min(0.05, avg_collab_quality * 0.1)
                enhanced_scores['collaboration_rating'] = min(
                    1.0,
                    enhanced_scores.get('collaboration_rating', 0) + collab_bonus
                )
                
            # Get cross-service reputation
            cross_service_reps = await self._get_cross_service_reputation(user_id)
            
            if cross_service_reps:
                # Weight reputations by service trust scores
                weighted_sum = 0
                total_weight = 0
                
                for rep in cross_service_reps:
                    service_trust = rep.get('trust_score', 0.5)
                    rep_score = rep.get('overall_score', 0)
                    
                    weighted_sum += rep_score * service_trust
                    total_weight += service_trust
                    
                if total_weight > 0:
                    cross_service_score = weighted_sum / total_weight
                    # Blend with existing score
                    enhanced_scores['overall_score'] = (
                        enhanced_scores['overall_score'] * 0.7 +
                        cross_service_score * 0.3
                    )
                    
            return enhanced_scores
            
        except Exception as e:
            logger.error(f"Error enhancing trust calculation: {e}")
            return base_trust_scores
            
    async def discover_trust_communities(
        self,
        min_size: int = 5,
        min_density: float = 0.3
    ) -> List[Dict[str, Any]]:
        """
        Discover trust communities in the federated graph
        """
        try:
            # Use graph algorithms to find communities
            community_query = """
                g.V().has('node_type', 'user')
                .repeat(both('TRUSTS').simplePath()).times(3)
                .dedup()
                .group().by().by(both('TRUSTS').dedup().count())
                .unfold()
                .where(select(values).is(gte(5)))
            """
            
            communities = await self.federated_graph.federated_query(community_query)
            
            # Analyze each community
            analyzed_communities = []
            
            for community in communities:
                members = community.get('members', [])
                if len(members) >= min_size:
                    # Calculate community metrics
                    trust_density = await self._calculate_trust_density(members)
                    
                    if trust_density >= min_density:
                        analyzed_communities.append({
                            'community_id': f"comm_{datetime.utcnow().timestamp()}",
                            'members': members,
                            'size': len(members),
                            'trust_density': trust_density,
                            'discovered_at': datetime.utcnow().isoformat()
                        })
                        
            return analyzed_communities
            
        except Exception as e:
            logger.error(f"Error discovering trust communities: {e}")
            return []
            
    async def get_trust_insights(
        self,
        entity_id: str,
        insight_types: List[str]
    ) -> Dict[str, Any]:
        """
        Get various trust insights from the federated graph
        """
        insights = {}
        
        try:
            if 'influence' in insight_types:
                # Calculate influence based on network position
                influence_query = f"""
                    g.V().has('node_id', '{entity_id}')
                    .repeat(out().simplePath()).times(2)
                    .dedup().count()
                """
                
                reach = await self.federated_graph.federated_query(influence_query)
                insights['influence_reach'] = reach[0] if reach else 0
                
            if 'trust_paths' in insight_types:
                # Find trust paths to high-reputation entities
                paths = await self.federated_graph.find_path(
                    source_node_id=entity_id,
                    target_node_id="*",  # Any high-trust node
                    max_length=4,
                    edge_types=[EdgeType.TRUSTS, EdgeType.COLLABORATES_WITH]
                )
                insights['trust_paths'] = paths
                
            if 'risk_score' in insight_types:
                # Calculate risk based on network analysis
                risk = await self._calculate_network_risk(entity_id)
                insights['risk_score'] = risk
                
            if 'recommendations' in insight_types:
                # Get trust-based recommendations
                recommendations = await self.federated_graph.get_recommendations(
                    entity_id=entity_id,
                    recommendation_type="collaborative",
                    limit=10
                )
                insights['recommended_collaborators'] = recommendations
                
            return insights
            
        except Exception as e:
            logger.error(f"Error getting trust insights: {e}")
            return insights
            
    async def sync_trust_subgraph(
        self,
        tenant_id: str,
        since: Optional[datetime] = None
    ) -> Dict[str, Any]:
        """
        Sync trust-related subgraph for a tenant
        """
        try:
            # Define trust-related node and edge types
            trust_node_types = [
                NodeType.USER,
                NodeType.CREDENTIAL,
                NodeType.PROJECT,
                NodeType.CUSTOM  # For reputation scores
            ]
            
            trust_edge_types = [
                EdgeType.TRUSTS,
                EdgeType.VALIDATES,
                EdgeType.COLLABORATES_WITH,
                EdgeType.OWNS
            ]
            
            # Sync subgraph
            subgraph = await self.federated_graph.sync_subgraph(
                node_types=trust_node_types,
                edge_types=trust_edge_types,
                since_timestamp=since
            )
            
            # Filter by tenant
            filtered_nodes = [
                n for n in subgraph['nodes']
                if n.get('tenant_id', [None])[0] == tenant_id
            ]
            
            filtered_edges = [
                e for e in subgraph['edges']
                if any(n.get('node_id', [None])[0] in [e.get('source_node_id'), e.get('target_node_id')]
                      for n in filtered_nodes)
            ]
            
            return {
                'nodes': filtered_nodes,
                'edges': filtered_edges,
                'sync_timestamp': subgraph['sync_timestamp'],
                'tenant_id': tenant_id
            }
            
        except Exception as e:
            logger.error(f"Error syncing trust subgraph: {e}")
            raise
            
    # Private helper methods
    
    async def _get_cross_service_reputation(
        self,
        user_id: str
    ) -> List[Dict[str, Any]]:
        """Get reputation scores from other services"""
        query = f"""
            g.V().has('user_id', '{user_id}')
            .has('custom_type', '{TrustNodeType.REPUTATION_SCORE.value}')
            .has('service_origin', neq('graph-intelligence-service'))
            .valueMap(true)
        """
        
        return await self.federated_graph.federated_query(query)
        
    async def _calculate_trust_density(
        self,
        member_ids: List[str]
    ) -> float:
        """Calculate trust density within a group"""
        if len(member_ids) < 2:
            return 0.0
            
        total_possible = len(member_ids) * (len(member_ids) - 1) / 2
        trust_edges = 0
        
        for i, member1 in enumerate(member_ids):
            for member2 in member_ids[i+1:]:
                # Check if trust edge exists
                query = f"""
                    g.V().has('node_id', '{member1}')
                    .out('TRUSTS').has('node_id', '{member2}')
                    .count()
                """
                
                result = await self.federated_graph.federated_query(query)
                if result and result[0] > 0:
                    trust_edges += 1
                    
        return trust_edges / total_possible
        
    async def _calculate_network_risk(
        self,
        entity_id: str
    ) -> float:
        """Calculate risk score based on network analysis"""
        # Check for suspicious patterns
        risk_factors = []
        
        # Check for unusually high number of new connections
        recent_connections_query = f"""
            g.V().has('node_id', '{entity_id}')
            .outE().has('created_at', gte({int((datetime.utcnow() - timedelta(days=7)).timestamp() * 1000)}))
            .count()
        """
        
        recent_count = await self.federated_graph.federated_query(recent_connections_query)
        if recent_count and recent_count[0] > 50:
            risk_factors.append(0.2)
            
        # Check for connections to low-trust entities
        low_trust_query = f"""
            g.V().has('node_id', '{entity_id}')
            .out().has('trust_score', lt(0.3))
            .count()
        """
        
        low_trust_count = await self.federated_graph.federated_query(low_trust_query)
        if low_trust_count and low_trust_count[0] > 5:
            risk_factors.append(0.3)
            
        # Calculate overall risk
        if not risk_factors:
            return 0.1  # Base risk
            
        return min(0.9, sum(risk_factors))
        
    async def contribute_embedding(
        self,
        entity_id: str,
        entity_type: NodeType,
        embedding: List[float],
        metadata: Dict[str, Any]
    ) -> GraphNode:
        """
        Contribute an entity embedding for similarity search
        """
        try:
            # Create node with embedding
            node = await self.federated_graph.create_node(
                node_type=entity_type,
                properties={
                    'id': entity_id,
                    **metadata
                },
                trust_score=metadata.get('trust_score', 0.5),
                embedding=embedding
            )
            
            return node
            
        except Exception as e:
            logger.error(f"Error contributing embedding: {e}")
            raise
            
    async def find_similar_entities(
        self,
        reference_entity_id: str,
        top_k: int = 10
    ) -> List[Dict[str, Any]]:
        """
        Find similar entities based on embeddings
        """
        try:
            # Get reference entity embedding
            query = f"g.V().has('node_id', '{reference_entity_id}').values('embedding')"
            embeddings = await self.federated_graph.federated_query(query)
            
            if not embeddings:
                return []
                
            reference_embedding = embeddings[0]
            
            # Find similar entities
            similar = await self.federated_graph.similarity_search(
                embedding=reference_embedding,
                top_k=top_k,
                min_similarity=0.7
            )
            
            # Get entity details
            results = []
            for entity_id, similarity in similar:
                if entity_id != reference_entity_id:
                    entity_query = f"g.V().has('node_id', '{entity_id}').valueMap(true)"
                    entity_data = await self.federated_graph.federated_query(entity_query)
                    
                    if entity_data:
                        results.append({
                            'entity_id': entity_id,
                            'similarity': similarity,
                            'properties': entity_data[0]
                        })
                        
            return results
            
        except Exception as e:
            logger.error(f"Error finding similar entities: {e}")
            return [] 