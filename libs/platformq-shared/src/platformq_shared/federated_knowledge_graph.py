"""
Federated Knowledge Graph Library

Shared library that enables all services to contribute to and query
a unified, federated knowledge graph built on JanusGraph.
"""

from typing import Dict, List, Optional, Any, Tuple, Set
from datetime import datetime
from dataclasses import dataclass
from enum import Enum
import asyncio
import logging
import json
from gremlin_python.driver import client, serializer
from gremlin_python.process.anonymous_traversal import traversal
from gremlin_python.driver.driver_remote_connection import DriverRemoteConnection

from .event_publisher import EventPublisher
from .cache import DistributedCache

logger = logging.getLogger(__name__)


class NodeType(Enum):
    """Standard node types across the platform"""
    USER = "user"
    ASSET = "asset"
    DATASET = "dataset"
    MODEL = "model"
    PROJECT = "project"
    SERVICE = "service"
    CREDENTIAL = "credential"
    TRANSACTION = "transaction"
    SIMULATION = "simulation"
    WORKFLOW = "workflow"
    DERIVATIVE = "derivative"
    CUSTOM = "custom"


class EdgeType(Enum):
    """Standard edge types for relationships"""
    CREATED_BY = "created_by"
    DERIVED_FROM = "derived_from"
    USES = "uses"
    REFERENCES = "references"
    BELONGS_TO = "belongs_to"
    SIMILAR_TO = "similar_to"
    PRECEDES = "precedes"
    FOLLOWS = "follows"
    VALIDATES = "validates"
    TRUSTS = "trusts"
    OWNS = "owns"
    COLLABORATES_WITH = "collaborates_with"
    TRADES = "trades"
    PROVIDES = "provides"
    CONSUMES = "consumes"


@dataclass
class GraphNode:
    """Represents a node in the federated graph"""
    node_id: str
    node_type: NodeType
    service_origin: str
    properties: Dict[str, Any]
    trust_score: float
    visibility: str = "TENANT"
    tenant_id: Optional[str] = None
    embedding: Optional[List[float]] = None
    created_at: Optional[datetime] = None


@dataclass
class GraphEdge:
    """Represents an edge in the federated graph"""
    edge_id: str
    source_node_id: str
    target_node_id: str
    edge_type: EdgeType
    service_origin: str
    properties: Dict[str, Any]
    confidence: float = 1.0
    trust_weighted: bool = True
    created_at: Optional[datetime] = None


class FederatedKnowledgeGraph:
    """
    Client for interacting with the federated knowledge graph.
    This can be used by any service to contribute and query knowledge.
    """
    
    def __init__(
        self,
        service_name: str,
        janusgraph_host: str = "janusgraph",
        janusgraph_port: int = 8182,
        event_publisher: Optional[EventPublisher] = None,
        cache: Optional[DistributedCache] = None
    ):
        self.service_name = service_name
        self.janusgraph_host = janusgraph_host
        self.janusgraph_port = janusgraph_port
        self.event_publisher = event_publisher
        self.cache = cache
        
        # Initialize Gremlin connection
        self._gremlin_client = None
        self._g = None
        
        # Local cache for frequently accessed nodes
        self._node_cache = {}
        
        # Service metadata
        self._service_trust_score = 1.0
        
    async def connect(self):
        """Connect to JanusGraph"""
        try:
            # Create Gremlin client
            self._gremlin_client = client.Client(
                f'ws://{self.janusgraph_host}:{self.janusgraph_port}/gremlin',
                'g',
                message_serializer=serializer.GraphSONSerializersV3d0()
            )
            
            # Create traversal source
            connection = DriverRemoteConnection(
                f'ws://{self.janusgraph_host}:{self.janusgraph_port}/gremlin',
                'g'
            )
            self._g = traversal().withRemote(connection)
            
            logger.info(f"Connected to JanusGraph for service {self.service_name}")
            
        except Exception as e:
            logger.error(f"Failed to connect to JanusGraph: {e}")
            raise
            
    async def create_node(
        self,
        node_type: NodeType,
        properties: Dict[str, Any],
        trust_score: Optional[float] = None,
        visibility: str = "TENANT",
        tenant_id: Optional[str] = None,
        embedding: Optional[List[float]] = None
    ) -> GraphNode:
        """
        Create a node in the federated graph
        """
        try:
            # Generate node ID
            node_id = f"{self.service_name}:{node_type.value}:{properties.get('id', datetime.utcnow().timestamp())}"
            
            # Use service trust score if not provided
            if trust_score is None:
                trust_score = self._service_trust_score
                
            # Create node object
            node = GraphNode(
                node_id=node_id,
                node_type=node_type,
                service_origin=self.service_name,
                properties=properties,
                trust_score=trust_score,
                visibility=visibility,
                tenant_id=tenant_id,
                embedding=embedding,
                created_at=datetime.utcnow()
            )
            
            # Add to JanusGraph
            await self._add_node_to_graph(node)
            
            # Publish event
            if self.event_publisher:
                await self._publish_node_created(node)
                
            # Cache node
            self._node_cache[node_id] = node
            
            logger.info(f"Created node {node_id} in federated graph")
            
            return node
            
        except Exception as e:
            logger.error(f"Error creating node: {e}")
            raise
            
    async def create_edge(
        self,
        source_node_id: str,
        target_node_id: str,
        edge_type: EdgeType,
        properties: Optional[Dict[str, Any]] = None,
        confidence: float = 1.0,
        trust_weighted: bool = True
    ) -> GraphEdge:
        """
        Create an edge between two nodes
        """
        try:
            # Generate edge ID
            edge_id = f"{source_node_id}-{edge_type.value}-{target_node_id}"
            
            # Create edge object
            edge = GraphEdge(
                edge_id=edge_id,
                source_node_id=source_node_id,
                target_node_id=target_node_id,
                edge_type=edge_type,
                service_origin=self.service_name,
                properties=properties or {},
                confidence=confidence,
                trust_weighted=trust_weighted,
                created_at=datetime.utcnow()
            )
            
            # Add to JanusGraph
            await self._add_edge_to_graph(edge)
            
            # Publish event
            if self.event_publisher:
                await self._publish_edge_created(edge)
                
            logger.info(f"Created edge {edge_id} in federated graph")
            
            return edge
            
        except Exception as e:
            logger.error(f"Error creating edge: {e}")
            raise
            
    async def query_neighbors(
        self,
        node_id: str,
        edge_types: Optional[List[EdgeType]] = None,
        max_hops: int = 1,
        trust_threshold: float = 0.0
    ) -> List[Dict[str, Any]]:
        """
        Query neighbors of a node with optional filtering
        """
        try:
            # Build Gremlin query
            query = f"g.V().has('node_id', '{node_id}')"
            
            # Add edge type filter
            if edge_types:
                edge_labels = [et.value for et in edge_types]
                query += f".out({','.join(repr(e) for e in edge_labels)})"
            else:
                query += ".out()"
                
            # Apply trust threshold
            if trust_threshold > 0:
                query += f".has('trust_score', gte({trust_threshold}))"
                
            # Limit hops
            if max_hops > 1:
                for _ in range(max_hops - 1):
                    query += ".out()"
                    
            # Get properties
            query += ".valueMap(true)"
            
            # Execute query
            results = await self._execute_query(query)
            
            return results
            
        except Exception as e:
            logger.error(f"Error querying neighbors: {e}")
            raise
            
    async def find_path(
        self,
        source_node_id: str,
        target_node_id: str,
        max_length: int = 5,
        edge_types: Optional[List[EdgeType]] = None
    ) -> List[List[str]]:
        """
        Find paths between two nodes
        """
        try:
            # Build path query
            query = f"g.V().has('node_id', '{source_node_id}')"
            
            if edge_types:
                edge_labels = [et.value for et in edge_types]
                edge_filter = f"({','.join(repr(e) for e in edge_labels)})"
            else:
                edge_filter = ""
                
            query += f".repeat(out{edge_filter}.simplePath()).until(has('node_id', '{target_node_id}')).limit({max_length}).path().by('node_id')"
            
            # Execute query
            paths = await self._execute_query(query)
            
            return paths
            
        except Exception as e:
            logger.error(f"Error finding path: {e}")
            raise
            
    async def similarity_search(
        self,
        embedding: List[float],
        node_type: Optional[NodeType] = None,
        top_k: int = 10,
        min_similarity: float = 0.7
    ) -> List[Tuple[str, float]]:
        """
        Find similar nodes based on embeddings
        """
        try:
            # For now, use a simple approach
            # In production, integrate with a vector database
            
            similar_nodes = []
            
            # Build query to get nodes with embeddings
            query = "g.V()"
            if node_type:
                query += f".has('node_type', '{node_type.value}')"
            query += ".has('embedding').valueMap(true)"
            
            nodes = await self._execute_query(query)
            
            # Calculate similarities
            for node_data in nodes:
                node_id = node_data.get('node_id', [None])[0]
                node_embedding = node_data.get('embedding', [None])[0]
                
                if node_embedding:
                    similarity = self._cosine_similarity(embedding, node_embedding)
                    if similarity >= min_similarity:
                        similar_nodes.append((node_id, similarity))
                        
            # Sort by similarity and return top K
            similar_nodes.sort(key=lambda x: x[1], reverse=True)
            
            return similar_nodes[:top_k]
            
        except Exception as e:
            logger.error(f"Error in similarity search: {e}")
            raise
            
    async def get_recommendations(
        self,
        entity_id: str,
        recommendation_type: str = "similar",
        limit: int = 10
    ) -> List[Dict[str, Any]]:
        """
        Get recommendations based on graph relationships
        """
        try:
            recommendations = []
            
            if recommendation_type == "similar":
                # Find similar entities based on shared connections
                query = f"""
                    g.V().has('node_id', '{entity_id}')
                    .out().in().has('node_id', neq('{entity_id}'))
                    .groupCount().order(local).by(values, desc)
                    .limit({limit}).select(keys).valueMap(true)
                """
                
            elif recommendation_type == "collaborative":
                # Find entities used by similar users
                query = f"""
                    g.V().has('node_id', '{entity_id}')
                    .in('created_by').as('creator')
                    .out('created_by').has('node_id', neq('{entity_id}'))
                    .dedup().limit({limit}).valueMap(true)
                """
                
            elif recommendation_type == "complementary":
                # Find entities often used together
                query = f"""
                    g.V().has('node_id', '{entity_id}')
                    .in('uses').out('uses').has('node_id', neq('{entity_id}'))
                    .groupCount().order(local).by(values, desc)
                    .limit({limit}).select(keys).valueMap(true)
                """
                
            else:
                # Default: random walk based recommendations
                query = f"""
                    g.V().has('node_id', '{entity_id}')
                    .repeat(out().simplePath()).times(2)
                    .dedup().limit({limit}).valueMap(true)
                """
                
            results = await self._execute_query(query)
            
            # Format recommendations
            for i, result in enumerate(results):
                rec = {
                    'entity_id': result.get('node_id', [None])[0],
                    'entity_type': result.get('node_type', [None])[0],
                    'score': 1.0 - (i / len(results)),  # Simple scoring
                    'reasoning': f"{recommendation_type} recommendation"
                }
                recommendations.append(rec)
                
            return recommendations
            
        except Exception as e:
            logger.error(f"Error getting recommendations: {e}")
            raise
            
    async def infer_relationships(
        self,
        node_ids: List[str],
        inference_type: str = "similarity",
        min_confidence: float = 0.7
    ) -> List[Dict[str, Any]]:
        """
        Infer new relationships between nodes
        """
        try:
            inferred_edges = []
            
            if inference_type == "similarity":
                # Infer similarity based on shared properties
                for i, node1 in enumerate(node_ids):
                    for node2 in node_ids[i+1:]:
                        # Get properties of both nodes
                        query1 = f"g.V().has('node_id', '{node1}').valueMap(true)"
                        query2 = f"g.V().has('node_id', '{node2}').valueMap(true)"
                        
                        props1 = await self._execute_query(query1)
                        props2 = await self._execute_query(query2)
                        
                        if props1 and props2:
                            similarity = self._calculate_property_similarity(
                                props1[0], props2[0]
                            )
                            
                            if similarity >= min_confidence:
                                inferred_edges.append({
                                    'source': node1,
                                    'target': node2,
                                    'relationship': 'similar_to',
                                    'confidence': similarity,
                                    'inference_type': inference_type
                                })
                                
            elif inference_type == "causality":
                # Infer causal relationships based on temporal patterns
                # This would require more sophisticated analysis
                pass
                
            elif inference_type == "transitive":
                # Infer transitive relationships
                for node in node_ids:
                    query = f"""
                        g.V().has('node_id', '{node}').as('a')
                        .out().as('b').out().as('c')
                        .select('a', 'c').by('node_id')
                        .where('a', neq('c'))
                    """
                    
                    results = await self._execute_query(query)
                    
                    for result in results:
                        if result['c'] in node_ids:
                            inferred_edges.append({
                                'source': result['a'],
                                'target': result['c'],
                                'relationship': 'transitively_related',
                                'confidence': 0.8,
                                'inference_type': inference_type
                            })
                            
            # Publish inferred relationships
            if inferred_edges and self.event_publisher:
                await self._publish_knowledge_inferred(
                    node_ids,
                    inferred_edges,
                    inference_type
                )
                
            return inferred_edges
            
        except Exception as e:
            logger.error(f"Error inferring relationships: {e}")
            raise
            
    async def federated_query(
        self,
        query: str,
        query_type: str = "GREMLIN",
        target_services: Optional[List[str]] = None,
        timeout_ms: int = 30000
    ) -> List[Dict[str, Any]]:
        """
        Execute a federated query across service boundaries
        """
        try:
            # If targeting specific services, filter nodes
            if target_services:
                # Modify query to filter by service origin
                service_filter = f".has('service_origin', within({','.join(repr(s) for s in target_services)}))"
                # This is simplified - in practice would need proper query parsing
                query = query.replace("g.V()", f"g.V()){service_filter}")
                
            # Execute query with timeout
            results = await asyncio.wait_for(
                self._execute_query(query),
                timeout=timeout_ms / 1000
            )
            
            return results
            
        except asyncio.TimeoutError:
            logger.error(f"Federated query timeout after {timeout_ms}ms")
            raise
        except Exception as e:
            logger.error(f"Error in federated query: {e}")
            raise
            
    async def sync_subgraph(
        self,
        node_types: Optional[List[NodeType]] = None,
        edge_types: Optional[List[EdgeType]] = None,
        since_timestamp: Optional[datetime] = None
    ) -> Dict[str, Any]:
        """
        Sync a subgraph from the federated graph
        """
        try:
            # Build query for nodes
            node_query = "g.V()"
            
            if node_types:
                type_values = [nt.value for nt in node_types]
                node_query += f".has('node_type', within({','.join(repr(t) for t in type_values)}))"
                
            if since_timestamp:
                timestamp_ms = int(since_timestamp.timestamp() * 1000)
                node_query += f".has('created_at', gte({timestamp_ms}))"
                
            node_query += ".valueMap(true)"
            
            # Build query for edges
            edge_query = "g.E()"
            
            if edge_types:
                type_values = [et.value for et in edge_types]
                edge_query += f".has('edge_type', within({','.join(repr(t) for t in type_values)}))"
                
            if since_timestamp:
                timestamp_ms = int(since_timestamp.timestamp() * 1000)
                edge_query += f".has('created_at', gte({timestamp_ms}))"
                
            edge_query += ".valueMap(true)"
            
            # Execute queries
            nodes = await self._execute_query(node_query)
            edges = await self._execute_query(edge_query)
            
            return {
                'nodes': nodes,
                'edges': edges,
                'sync_timestamp': datetime.utcnow().isoformat()
            }
            
        except Exception as e:
            logger.error(f"Error syncing subgraph: {e}")
            raise
            
    # Private helper methods
    
    async def _add_node_to_graph(self, node: GraphNode):
        """Add node to JanusGraph"""
        properties = {
            'node_id': node.node_id,
            'node_type': node.node_type.value,
            'service_origin': node.service_origin,
            'trust_score': node.trust_score,
            'visibility': node.visibility,
            'created_at': int(node.created_at.timestamp() * 1000)
        }
        
        if node.tenant_id:
            properties['tenant_id'] = node.tenant_id
            
        if node.embedding:
            properties['embedding'] = node.embedding
            
        # Add custom properties
        for key, value in node.properties.items():
            if key not in properties:
                properties[key] = value
                
        # Build Gremlin query
        query = "g.addV('entity')"
        for key, value in properties.items():
            query += f".property('{key}', {repr(value)})"
            
        await self._execute_query(query)
        
    async def _add_edge_to_graph(self, edge: GraphEdge):
        """Add edge to JanusGraph"""
        properties = {
            'edge_id': edge.edge_id,
            'edge_type': edge.edge_type.value,
            'service_origin': edge.service_origin,
            'confidence': edge.confidence,
            'trust_weighted': edge.trust_weighted,
            'created_at': int(edge.created_at.timestamp() * 1000)
        }
        
        # Add custom properties
        for key, value in edge.properties.items():
            if key not in properties:
                properties[key] = value
                
        # Build Gremlin query
        query = f"""
            g.V().has('node_id', '{edge.source_node_id}').as('source')
            .V().has('node_id', '{edge.target_node_id}').as('target')
            .addE('{edge.edge_type.value}').from('source').to('target')
        """
        
        for key, value in properties.items():
            query += f".property('{key}', {repr(value)})"
            
        await self._execute_query(query)
        
    async def _execute_query(self, query: str) -> List[Dict[str, Any]]:
        """Execute Gremlin query"""
        if not self._gremlin_client:
            raise RuntimeError("Not connected to JanusGraph")
            
        result = self._gremlin_client.submit(query)
        return result.all().result()
        
    def _cosine_similarity(self, vec1: List[float], vec2: List[float]) -> float:
        """Calculate cosine similarity between two vectors"""
        import numpy as np
        
        vec1 = np.array(vec1)
        vec2 = np.array(vec2)
        
        dot_product = np.dot(vec1, vec2)
        norm1 = np.linalg.norm(vec1)
        norm2 = np.linalg.norm(vec2)
        
        if norm1 == 0 or norm2 == 0:
            return 0.0
            
        return dot_product / (norm1 * norm2)
        
    def _calculate_property_similarity(
        self,
        props1: Dict[str, Any],
        props2: Dict[str, Any]
    ) -> float:
        """Calculate similarity between two property sets"""
        # Get common keys
        common_keys = set(props1.keys()) & set(props2.keys())
        
        if not common_keys:
            return 0.0
            
        # Calculate similarity for each common property
        similarities = []
        
        for key in common_keys:
            val1 = props1.get(key, [None])[0] if isinstance(props1.get(key), list) else props1.get(key)
            val2 = props2.get(key, [None])[0] if isinstance(props2.get(key), list) else props2.get(key)
            
            if val1 == val2:
                similarities.append(1.0)
            elif isinstance(val1, (int, float)) and isinstance(val2, (int, float)):
                # Numeric similarity
                max_val = max(abs(val1), abs(val2))
                if max_val > 0:
                    similarities.append(1.0 - abs(val1 - val2) / max_val)
                else:
                    similarities.append(1.0)
            elif isinstance(val1, str) and isinstance(val2, str):
                # String similarity (simple approach)
                similarities.append(1.0 if val1.lower() == val2.lower() else 0.5)
            else:
                similarities.append(0.0)
                
        return sum(similarities) / len(similarities)
        
    async def _publish_node_created(self, node: GraphNode):
        """Publish node created event"""
        await self.event_publisher.publish(
            'knowledge-node-created',
            {
                'node_id': node.node_id,
                'node_type': node.node_type.value,
                'service_origin': node.service_origin,
                'properties': node.properties,
                'trust_score': node.trust_score,
                'visibility': node.visibility,
                'tenant_id': node.tenant_id,
                'embedding': node.embedding,
                'timestamp': int(datetime.utcnow().timestamp() * 1000)
            }
        )
        
    async def _publish_edge_created(self, edge: GraphEdge):
        """Publish edge created event"""
        await self.event_publisher.publish(
            'knowledge-edge-created',
            {
                'edge_id': edge.edge_id,
                'source_node_id': edge.source_node_id,
                'target_node_id': edge.target_node_id,
                'edge_type': edge.edge_type.value,
                'service_origin': edge.service_origin,
                'properties': edge.properties,
                'confidence': edge.confidence,
                'trust_weighted': edge.trust_weighted,
                'timestamp': int(datetime.utcnow().timestamp() * 1000)
            }
        )
        
    async def _publish_knowledge_inferred(
        self,
        source_nodes: List[str],
        inferred_edges: List[Dict[str, Any]],
        inference_type: str
    ):
        """Publish knowledge inferred event"""
        await self.event_publisher.publish(
            'knowledge-inferred',
            {
                'inference_id': f"{self.service_name}:{datetime.utcnow().timestamp()}",
                'inference_type': inference_type,
                'source_nodes': source_nodes,
                'inferred_edges': inferred_edges,
                'confidence': sum(e['confidence'] for e in inferred_edges) / len(inferred_edges),
                'inferring_service': self.service_name,
                'algorithm_used': inference_type,
                'timestamp': int(datetime.utcnow().timestamp() * 1000)
            }
        ) 