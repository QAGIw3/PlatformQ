"""
Repository for Graph Intelligence Service

Uses the enhanced repository patterns from platformq-shared with JanusGraph.
"""

import uuid
import logging
from typing import List, Optional, Dict, Any, Tuple
from datetime import datetime
import json

from platformq_shared import BaseRepository, QueryBuilder
from platformq_shared.event_publisher import EventPublisher
from platformq_events import (
    GraphNodeCreatedEvent,
    GraphEdgeCreatedEvent,
    GraphCommunityDetectedEvent,
    GraphInsightGeneratedEvent
)

from gremlin_python.structure.graph import Graph
from gremlin_python.driver.driver_remote_connection import DriverRemoteConnection
from gremlin_python.process.traversal import T, P, Cardinality
from gremlin_python.process.graph_traversal import __

logger = logging.getLogger(__name__)


class GraphNodeRepository(BaseRepository):
    """Repository for graph nodes"""
    
    def __init__(self, gremlin_url: str, event_publisher: Optional[EventPublisher] = None):
        self.gremlin_url = gremlin_url
        self.g = Graph().traversal().withRemote(DriverRemoteConnection(gremlin_url, 'g'))
        self.event_publisher = event_publisher
        
    def create_node(self, node_type: str, properties: Dict[str, Any],
                   tenant_id: str) -> Dict[str, Any]:
        """Create a new node in the graph"""
        try:
            # Add node with properties
            node_id = str(uuid.uuid4())
            properties['node_id'] = node_id
            properties['tenant_id'] = tenant_id
            properties['created_at'] = datetime.utcnow().isoformat()
            
            v = self.g.addV(node_type)
            for key, value in properties.items():
                v = v.property(key, value)
            
            result = v.next()
            
            # Publish event
            if self.event_publisher:
                event = GraphNodeCreatedEvent(
                    node_id=node_id,
                    node_type=node_type,
                    tenant_id=tenant_id,
                    properties=properties,
                    created_at=properties['created_at']
                )
                
                self.event_publisher.publish_event(
                    topic=f"persistent://platformq/{tenant_id}/graph-node-created-events",
                    event=event
                )
                
            logger.info(f"Created {node_type} node: {node_id}")
            return {
                "id": result.id,
                "node_id": node_id,
                "type": node_type,
                "properties": properties
            }
            
        except Exception as e:
            logger.error(f"Error creating node: {e}")
            raise
            
    def get_node(self, node_id: str, tenant_id: str) -> Optional[Dict[str, Any]]:
        """Get node by ID"""
        try:
            result = self.g.V().has('node_id', node_id).has('tenant_id', tenant_id).elementMap().next()
            return dict(result) if result else None
        except StopIteration:
            return None
            
    def update_node(self, node_id: str, tenant_id: str,
                   properties: Dict[str, Any]) -> bool:
        """Update node properties"""
        try:
            v = self.g.V().has('node_id', node_id).has('tenant_id', tenant_id)
            
            for key, value in properties.items():
                v = v.property(Cardinality.single, key, value)
                
            v.property('updated_at', datetime.utcnow().isoformat()).iterate()
            return True
            
        except Exception as e:
            logger.error(f"Error updating node: {e}")
            return False
            
    def delete_node(self, node_id: str, tenant_id: str) -> bool:
        """Delete node and its edges"""
        try:
            self.g.V().has('node_id', node_id).has('tenant_id', tenant_id).drop().iterate()
            return True
        except Exception as e:
            logger.error(f"Error deleting node: {e}")
            return False
            
    def find_nodes(self, node_type: Optional[str] = None,
                  filters: Optional[Dict[str, Any]] = None,
                  tenant_id: Optional[str] = None,
                  limit: int = 100) -> List[Dict[str, Any]]:
        """Find nodes by type and filters"""
        try:
            traversal = self.g.V()
            
            if tenant_id:
                traversal = traversal.has('tenant_id', tenant_id)
            
            if node_type:
                traversal = traversal.hasLabel(node_type)
                
            if filters:
                for key, value in filters.items():
                    traversal = traversal.has(key, value)
                    
            results = traversal.limit(limit).elementMap().toList()
            return [dict(r) for r in results]
            
        except Exception as e:
            logger.error(f"Error finding nodes: {e}")
            return []


class GraphEdgeRepository(BaseRepository):
    """Repository for graph edges"""
    
    def __init__(self, gremlin_url: str, event_publisher: Optional[EventPublisher] = None):
        self.gremlin_url = gremlin_url
        self.g = Graph().traversal().withRemote(DriverRemoteConnection(gremlin_url, 'g'))
        self.event_publisher = event_publisher
        
    def create_edge(self, from_node_id: str, to_node_id: str,
                   edge_type: str, properties: Dict[str, Any],
                   tenant_id: str) -> Dict[str, Any]:
        """Create edge between nodes"""
        try:
            edge_id = str(uuid.uuid4())
            properties['edge_id'] = edge_id
            properties['tenant_id'] = tenant_id
            properties['created_at'] = datetime.utcnow().isoformat()
            
            # Create edge
            edge = self.g.V().has('node_id', from_node_id).has('tenant_id', tenant_id).as_('from') \
                         .V().has('node_id', to_node_id).has('tenant_id', tenant_id).as_('to') \
                         .addE(edge_type).from_('from').to('to')
                         
            for key, value in properties.items():
                edge = edge.property(key, value)
                
            result = edge.next()
            
            # Publish event
            if self.event_publisher:
                event = GraphEdgeCreatedEvent(
                    edge_id=edge_id,
                    edge_type=edge_type,
                    from_node_id=from_node_id,
                    to_node_id=to_node_id,
                    tenant_id=tenant_id,
                    properties=properties,
                    created_at=properties['created_at']
                )
                
                self.event_publisher.publish_event(
                    topic=f"persistent://platformq/{tenant_id}/graph-edge-created-events",
                    event=event
                )
                
            logger.info(f"Created {edge_type} edge: {from_node_id} -> {to_node_id}")
            
            return {
                "id": result.id,
                "edge_id": edge_id,
                "type": edge_type,
                "from_node_id": from_node_id,
                "to_node_id": to_node_id,
                "properties": properties
            }
            
        except Exception as e:
            logger.error(f"Error creating edge: {e}")
            raise
            
    def get_edges(self, node_id: str, direction: str = "both",
                 edge_type: Optional[str] = None,
                 tenant_id: Optional[str] = None) -> List[Dict[str, Any]]:
        """Get edges for a node"""
        try:
            v = self.g.V().has('node_id', node_id)
            
            if tenant_id:
                v = v.has('tenant_id', tenant_id)
                
            if direction == "out":
                edges = v.outE()
            elif direction == "in":
                edges = v.inE()
            else:
                edges = v.bothE()
                
            if edge_type:
                edges = edges.hasLabel(edge_type)
                
            results = edges.elementMap().toList()
            return [dict(r) for r in results]
            
        except Exception as e:
            logger.error(f"Error getting edges: {e}")
            return []
            
    def delete_edge(self, edge_id: str, tenant_id: str) -> bool:
        """Delete edge by ID"""
        try:
            self.g.E().has('edge_id', edge_id).has('tenant_id', tenant_id).drop().iterate()
            return True
        except Exception as e:
            logger.error(f"Error deleting edge: {e}")
            return False


class GraphAnalyticsRepository(BaseRepository):
    """Repository for graph analytics and insights"""
    
    def __init__(self, gremlin_url: str, event_publisher: Optional[EventPublisher] = None):
        self.gremlin_url = gremlin_url
        self.g = Graph().traversal().withRemote(DriverRemoteConnection(gremlin_url, 'g'))
        self.event_publisher = event_publisher
        
    def find_communities(self, tenant_id: str,
                        algorithm: str = "label_propagation") -> List[Dict[str, Any]]:
        """Detect communities in the graph"""
        try:
            # Simple connected components for now
            # In production, use proper community detection algorithms
            communities = []
            
            # Get all nodes for tenant
            nodes = self.g.V().has('tenant_id', tenant_id).toList()
            
            visited = set()
            for node in nodes:
                if node.id not in visited:
                    # BFS to find connected component
                    community = self._bfs_community(node, tenant_id, visited)
                    if len(community) > 1:
                        communities.append({
                            "community_id": str(uuid.uuid4()),
                            "size": len(community),
                            "members": community
                        })
                        
            # Publish event
            if self.event_publisher and communities:
                event = GraphCommunityDetectedEvent(
                    tenant_id=tenant_id,
                    communities=communities,
                    algorithm=algorithm,
                    detected_at=datetime.utcnow().isoformat()
                )
                
                self.event_publisher.publish_event(
                    topic=f"persistent://platformq/{tenant_id}/graph-community-events",
                    event=event
                )
                
            return communities
            
        except Exception as e:
            logger.error(f"Error finding communities: {e}")
            return []
            
    def _bfs_community(self, start_node, tenant_id: str,
                      visited: set) -> List[str]:
        """BFS to find connected component"""
        community = []
        queue = [start_node]
        
        while queue:
            node = queue.pop(0)
            if node.id in visited:
                continue
                
            visited.add(node.id)
            node_data = self.g.V(node).elementMap().next()
            community.append(node_data.get('node_id'))
            
            # Get connected nodes
            neighbors = self.g.V(node).both().has('tenant_id', tenant_id).toList()
            queue.extend([n for n in neighbors if n.id not in visited])
            
        return community
        
    def calculate_centrality(self, tenant_id: str,
                           centrality_type: str = "degree") -> Dict[str, float]:
        """Calculate node centrality scores"""
        try:
            scores = {}
            
            if centrality_type == "degree":
                # Simple degree centrality
                nodes = self.g.V().has('tenant_id', tenant_id).toList()
                
                for node in nodes:
                    node_id = self.g.V(node).values('node_id').next()
                    degree = self.g.V(node).bothE().count().next()
                    scores[node_id] = degree
                    
            elif centrality_type == "betweenness":
                # Simplified betweenness centrality
                # In production, use proper algorithm
                pass
                
            return scores
            
        except Exception as e:
            logger.error(f"Error calculating centrality: {e}")
            return {}
            
    def find_paths(self, from_node_id: str, to_node_id: str,
                  tenant_id: str, max_depth: int = 5) -> List[List[str]]:
        """Find paths between two nodes"""
        try:
            paths = self.g.V().has('node_id', from_node_id).has('tenant_id', tenant_id) \
                         .repeat(__.out().simplePath()).times(max_depth) \
                         .has('node_id', to_node_id) \
                         .path().by('node_id').toList()
                         
            return paths
            
        except Exception as e:
            logger.error(f"Error finding paths: {e}")
            return []
            
    def get_node_neighborhood(self, node_id: str, tenant_id: str,
                            hops: int = 2) -> Dict[str, Any]:
        """Get node's neighborhood within N hops"""
        try:
            # Get center node
            center = self.g.V().has('node_id', node_id).has('tenant_id', tenant_id).elementMap().next()
            
            # Get neighbors within N hops
            neighbors = self.g.V().has('node_id', node_id).has('tenant_id', tenant_id) \
                             .repeat(__.both().simplePath()).times(hops).dedup() \
                             .elementMap().toList()
                             
            # Get edges in neighborhood
            node_ids = [n.get('node_id') for n in neighbors] + [node_id]
            edges = self.g.V().has('node_id', P.within(node_ids)).has('tenant_id', tenant_id) \
                         .bothE().where(__.otherV().has('node_id', P.within(node_ids))) \
                         .elementMap().toList()
                         
            return {
                "center": dict(center),
                "nodes": [dict(n) for n in neighbors],
                "edges": [dict(e) for e in edges]
            }
            
        except Exception as e:
            logger.error(f"Error getting neighborhood: {e}")
            return {}
            
    def generate_insight(self, insight_type: str, data: Dict[str, Any],
                        tenant_id: str) -> Dict[str, Any]:
        """Generate and store graph insight"""
        try:
            insight_id = str(uuid.uuid4())
            
            insight = {
                "insight_id": insight_id,
                "type": insight_type,
                "data": data,
                "tenant_id": tenant_id,
                "created_at": datetime.utcnow().isoformat()
            }
            
            # Store insight as node
            self.g.addV('Insight').property('insight_id', insight_id) \
                                 .property('type', insight_type) \
                                 .property('data', json.dumps(data)) \
                                 .property('tenant_id', tenant_id) \
                                 .property('created_at', insight['created_at']) \
                                 .iterate()
                                 
            # Publish event
            if self.event_publisher:
                event = GraphInsightGeneratedEvent(
                    insight_id=insight_id,
                    insight_type=insight_type,
                    tenant_id=tenant_id,
                    data=data,
                    created_at=insight['created_at']
                )
                
                self.event_publisher.publish_event(
                    topic=f"persistent://platformq/{tenant_id}/graph-insight-events",
                    event=event
                )
                
            return insight
            
        except Exception as e:
            logger.error(f"Error generating insight: {e}")
            raise 