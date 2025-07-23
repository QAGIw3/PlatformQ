"""JanusGraph client for graph database operations"""

import logging
import asyncio
from typing import Dict, Any, List, Optional, Tuple
from datetime import datetime
import json
from concurrent.futures import ThreadPoolExecutor

from gremlin_python.driver import client, serializer
from gremlin_python.driver.protocol import GremlinServerError
from gremlin_python.process.graph_traversal import __
from gremlin_python.process.traversal import T, P, Cardinality

from app.core.config import Settings


logger = logging.getLogger(__name__)


class JanusGraphClient:
    """Client for JanusGraph operations"""
    
    def __init__(self, settings: Settings):
        self.settings = settings
        self.client: Optional[client.Client] = None
        self.executor = ThreadPoolExecutor(max_workers=settings.janusgraph_pool_size)
        self.connected = False
        
    async def connect(self):
        """Connect to JanusGraph"""
        logger.info(f"Connecting to JanusGraph at {self.settings.janusgraph_url}")
        
        try:
            # Create Gremlin client
            self.client = client.Client(
                self.settings.janusgraph_url,
                'g',
                pool_size=self.settings.janusgraph_pool_size,
                message_serializer=serializer.GraphSONSerializersV3d0()
            )
            
            # Test connection
            result = await self._execute_async("g.V().count()")
            logger.info(f"Connected to JanusGraph. Vertex count: {result[0]}")
            
            self.connected = True
            
        except Exception as e:
            logger.error(f"Failed to connect to JanusGraph: {e}")
            raise
            
    async def disconnect(self):
        """Disconnect from JanusGraph"""
        if self.client:
            self.client.close()
            self.connected = False
            logger.info("Disconnected from JanusGraph")
            
        self.executor.shutdown(wait=True)
        
    async def create_node(self, label: str, properties: Dict[str, Any], 
                         node_id: Optional[str] = None) -> str:
        """Create a node in the graph"""
        try:
            # Build query
            query = f"g.addV('{label}')"
            bindings = {}
            
            # Add ID if provided
            if node_id:
                query += f".property(T.id, node_id)"
                bindings['node_id'] = node_id
                
            # Add properties
            for key, value in properties.items():
                if isinstance(value, list):
                    # Handle multi-value properties
                    for i, v in enumerate(value):
                        query += f".property(Cardinality.set_, prop_{key}_{i}, val_{key}_{i})"
                        bindings[f'prop_{key}_{i}'] = key
                        bindings[f'val_{key}_{i}'] = v
                else:
                    query += f".property(prop_{key}, val_{key})"
                    bindings[f'prop_{key}'] = key
                    bindings[f'val_{key}'] = value
                    
            # Add creation timestamp
            query += ".property('created_at', created_at)"
            bindings['created_at'] = datetime.utcnow().isoformat()
            
            # Execute query
            result = await self._execute_async(query, bindings)
            vertex_id = result[0].id if result else None
            
            logger.info(f"Created node {label} with ID: {vertex_id}")
            return str(vertex_id)
            
        except Exception as e:
            logger.error(f"Failed to create node: {e}")
            raise
            
    async def create_edge(self, label: str, from_id: str, to_id: str,
                         properties: Optional[Dict[str, Any]] = None) -> str:
        """Create an edge between two nodes"""
        try:
            # Build query
            query = f"g.V(from_id).addE('{label}').to(g.V(to_id))"
            bindings = {'from_id': from_id, 'to_id': to_id}
            
            # Add properties
            if properties:
                for key, value in properties.items():
                    query += f".property(prop_{key}, val_{key})"
                    bindings[f'prop_{key}'] = key
                    bindings[f'val_{key}'] = value
                    
            # Add creation timestamp
            query += ".property('created_at', created_at)"
            bindings['created_at'] = datetime.utcnow().isoformat()
            
            # Execute query
            result = await self._execute_async(query, bindings)
            edge_id = result[0].id if result else None
            
            logger.info(f"Created edge {label} from {from_id} to {to_id}")
            return str(edge_id)
            
        except Exception as e:
            logger.error(f"Failed to create edge: {e}")
            raise
            
    async def get_node(self, node_id: str) -> Optional[Dict[str, Any]]:
        """Get a node by ID"""
        try:
            query = "g.V(node_id).valueMap(true)"
            result = await self._execute_async(query, {'node_id': node_id})
            
            if result:
                return self._format_vertex(result[0])
            return None
            
        except Exception as e:
            logger.error(f"Failed to get node {node_id}: {e}")
            raise
            
    async def update_node(self, node_id: str, properties: Dict[str, Any]) -> bool:
        """Update node properties"""
        try:
            query = "g.V(node_id)"
            bindings = {'node_id': node_id}
            
            # Update properties
            for key, value in properties.items():
                if value is None:
                    # Remove property
                    query += f".properties('{key}').drop()"
                else:
                    query += f".property(prop_{key}, val_{key})"
                    bindings[f'prop_{key}'] = key
                    bindings[f'val_{key}'] = value
                    
            # Update timestamp
            query += ".property('updated_at', updated_at)"
            bindings['updated_at'] = datetime.utcnow().isoformat()
            
            await self._execute_async(query, bindings)
            logger.info(f"Updated node {node_id}")
            return True
            
        except Exception as e:
            logger.error(f"Failed to update node {node_id}: {e}")
            raise
            
    async def delete_node(self, node_id: str) -> bool:
        """Delete a node and its edges"""
        try:
            query = "g.V(node_id).drop()"
            await self._execute_async(query, {'node_id': node_id})
            
            logger.info(f"Deleted node {node_id}")
            return True
            
        except Exception as e:
            logger.error(f"Failed to delete node {node_id}: {e}")
            raise
            
    async def execute_query(self, query: str, bindings: Optional[Dict[str, Any]] = None) -> List[Any]:
        """Execute a Gremlin query"""
        try:
            result = await self._execute_async(query, bindings or {})
            return result
            
        except Exception as e:
            logger.error(f"Failed to execute query: {e}")
            raise
            
    async def find_shortest_path(self, from_id: str, to_id: str, 
                                max_depth: Optional[int] = None) -> Optional[List[str]]:
        """Find shortest path between two nodes"""
        try:
            query = "g.V(from_id).repeat(out().simplePath()).until(hasId(to_id))"
            
            if max_depth:
                query += f".or().loops().is(P.gte({max_depth}))"
                
            query += ".limit(1).path().by(id)"
            
            result = await self._execute_async(query, {
                'from_id': from_id,
                'to_id': to_id
            })
            
            if result:
                # Extract node IDs from path
                path = [str(node_id) for node_id in result[0]]
                return path
                
            return None
            
        except Exception as e:
            logger.error(f"Failed to find path from {from_id} to {to_id}: {e}")
            raise
            
    async def get_neighbors(self, node_id: str, direction: str = "both",
                           edge_label: Optional[str] = None, 
                           limit: int = 100) -> List[Dict[str, Any]]:
        """Get neighbors of a node"""
        try:
            # Build traversal based on direction
            if direction == "out":
                query = "g.V(node_id).out"
            elif direction == "in":
                query = "g.V(node_id).in"
            else:
                query = "g.V(node_id).both"
                
            # Add edge label filter if specified
            if edge_label:
                query += f"('{edge_label}')"
            else:
                query += "()"
                
            query += f".limit({limit}).valueMap(true)"
            
            result = await self._execute_async(query, {'node_id': node_id})
            
            return [self._format_vertex(v) for v in result]
            
        except Exception as e:
            logger.error(f"Failed to get neighbors for {node_id}: {e}")
            raise
            
    async def count_nodes(self, label: Optional[str] = None) -> int:
        """Count nodes in the graph"""
        try:
            if label:
                query = f"g.V().hasLabel('{label}').count()"
            else:
                query = "g.V().count()"
                
            result = await self._execute_async(query)
            return result[0] if result else 0
            
        except Exception as e:
            logger.error(f"Failed to count nodes: {e}")
            raise
            
    async def count_edges(self, label: Optional[str] = None) -> int:
        """Count edges in the graph"""
        try:
            if label:
                query = f"g.E().hasLabel('{label}').count()"
            else:
                query = "g.E().count()"
                
            result = await self._execute_async(query)
            return result[0] if result else 0
            
        except Exception as e:
            logger.error(f"Failed to count edges: {e}")
            raise
            
    async def batch_create_nodes(self, nodes: List[Dict[str, Any]]) -> List[str]:
        """Create multiple nodes in batch"""
        node_ids = []
        
        # Process in batches
        batch_size = self.settings.batch_size
        for i in range(0, len(nodes), batch_size):
            batch = nodes[i:i + batch_size]
            
            # Build batch query
            query = "g"
            bindings = {}
            
            for j, node in enumerate(batch):
                label = node.get('label', 'vertex')
                properties = node.get('properties', {})
                
                query += f".addV('{label}')"
                
                # Add properties
                for key, value in properties.items():
                    query += f".property(prop_{j}_{key}, val_{j}_{key})"
                    bindings[f'prop_{j}_{key}'] = key
                    bindings[f'val_{j}_{key}'] = value
                    
                query += f".property('created_at', created_at_{j})"
                bindings[f'created_at_{j}'] = datetime.utcnow().isoformat()
                
                query += ".id().store('x')"
                
            query += ".cap('x')"
            
            # Execute batch
            result = await self._execute_async(query, bindings)
            if result:
                node_ids.extend([str(id) for id in result[0]])
                
        logger.info(f"Created {len(node_ids)} nodes in batch")
        return node_ids
        
    async def _execute_async(self, query: str, bindings: Optional[Dict[str, Any]] = None) -> List[Any]:
        """Execute query asynchronously"""
        loop = asyncio.get_event_loop()
        return await loop.run_in_executor(
            self.executor,
            self._execute_sync,
            query,
            bindings
        )
        
    def _execute_sync(self, query: str, bindings: Optional[Dict[str, Any]] = None) -> List[Any]:
        """Execute query synchronously"""
        if not self.client:
            raise RuntimeError("JanusGraph client not connected")
            
        try:
            result = self.client.submit(query, bindings or {})
            return list(result)
        except GremlinServerError as e:
            logger.error(f"Gremlin query error: {e}")
            raise
            
    def _format_vertex(self, vertex_map: Dict[str, Any]) -> Dict[str, Any]:
        """Format vertex map for response"""
        formatted = {
            'id': str(vertex_map.get(T.id)),
            'label': vertex_map.get(T.label)
        }
        
        # Extract properties
        for key, value in vertex_map.items():
            if key not in [T.id, T.label]:
                # Handle multi-value properties
                if isinstance(value, list) and len(value) == 1:
                    formatted[key] = value[0]
                else:
                    formatted[key] = value
                    
        return formatted 