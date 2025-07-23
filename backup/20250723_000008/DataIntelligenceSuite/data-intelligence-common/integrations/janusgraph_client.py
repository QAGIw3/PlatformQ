"""
JanusGraph Client Integration

Provides high-level client for JanusGraph graph database operations.
"""

import logging
from typing import Any, Dict, List, Optional, Union, Tuple
from dataclasses import dataclass, field
from datetime import datetime
import asyncio

from gremlin_python.driver import client, serializer
from gremlin_python.driver.driver_remote_connection import DriverRemoteConnection
from gremlin_python.process.anonymous_traversal import traversal
from gremlin_python.process.graph_traversal import __, GraphTraversalSource
from gremlin_python.process.traversal import T, P, Order

logger = logging.getLogger(__name__)


@dataclass
class JanusGraphConfig:
    """Configuration for JanusGraph client"""
    host: str = "localhost"
    port: int = 8182
    
    # Connection settings
    traversal_source: str = "g"
    protocol: str = "websocket"
    transport_factory: Optional[Any] = None
    
    # Authentication
    username: Optional[str] = None
    password: Optional[str] = None
    
    # Pool settings
    pool_size: int = 8
    max_workers: int = 5
    
    # Serializer
    message_serializer: serializer.GraphSONSerializersV3d0 = field(
        default_factory=lambda: serializer.GraphSONSerializersV3d0()
    )
    
    # Timeouts
    connection_timeout: float = 10.0
    request_timeout: float = 30.0


@dataclass
class Vertex:
    """Represents a graph vertex"""
    id: Any
    label: str
    properties: Dict[str, Any] = field(default_factory=dict)


@dataclass
class Edge:
    """Represents a graph edge"""
    id: Any
    label: str
    in_vertex: Vertex
    out_vertex: Vertex
    properties: Dict[str, Any] = field(default_factory=dict)


class JanusGraphClient:
    """
    High-level client for JanusGraph operations.
    
    Features:
    - Vertex and edge CRUD
    - Graph traversals
    - Schema management
    - Index management
    - Transaction support
    - Gremlin queries
    """
    
    def __init__(self, config: JanusGraphConfig):
        self.config = config
        self._client: Optional[client.Client] = None
        self._connection: Optional[DriverRemoteConnection] = None
        self._g: Optional[GraphTraversalSource] = None
        
    def connect(self):
        """Connect to JanusGraph server"""
        try:
            # Build connection URL
            url = f"{self.config.protocol}://{self.config.host}:{self.config.port}/gremlin"
            
            # Create client for raw Gremlin queries
            self._client = client.Client(
                url,
                self.config.traversal_source,
                pool_size=self.config.pool_size,
                max_workers=self.config.max_workers,
                message_serializer=self.config.message_serializer,
                username=self.config.username,
                password=self.config.password,
                transport_factory=self.config.transport_factory
            )
            
            # Create remote connection for traversals
            self._connection = DriverRemoteConnection(
                url,
                self.config.traversal_source,
                pool_size=self.config.pool_size,
                max_workers=self.config.max_workers,
                message_serializer=self.config.message_serializer,
                username=self.config.username,
                password=self.config.password,
                transport_factory=self.config.transport_factory
            )
            
            # Get traversal source
            self._g = traversal().withRemote(self._connection)
            
            logger.info(f"Connected to JanusGraph: {self.config.host}:{self.config.port}")
            
        except Exception as e:
            logger.error(f"Failed to connect to JanusGraph: {e}")
            raise
            
    def disconnect(self):
        """Disconnect from JanusGraph"""
        if self._client:
            self._client.close()
            self._client = None
            
        if self._connection:
            self._connection.close()
            self._connection = None
            
        self._g = None
        logger.info("Disconnected from JanusGraph")
        
    @property
    def g(self) -> GraphTraversalSource:
        """Get graph traversal source"""
        if not self._g:
            raise RuntimeError("Not connected to JanusGraph")
        return self._g
        
    # Vertex operations
    
    def add_vertex(
        self,
        label: str,
        properties: Optional[Dict[str, Any]] = None
    ) -> Vertex:
        """Add a vertex"""
        if not self._g:
            raise RuntimeError("Not connected to JanusGraph")
            
        # Build vertex
        v = self._g.addV(label)
        
        # Add properties
        if properties:
            for key, value in properties.items():
                v = v.property(key, value)
                
        # Execute and get result
        result = v.next()
        
        # Get vertex details
        vertex_data = self._g.V(result).valueMap(True).next()
        
        return Vertex(
            id=vertex_data[T.id],
            label=vertex_data[T.label],
            properties={
                k: v[0] if isinstance(v, list) and len(v) == 1 else v
                for k, v in vertex_data.items()
                if k not in [T.id, T.label]
            }
        )
        
    def get_vertex(self, vertex_id: Any) -> Optional[Vertex]:
        """Get vertex by ID"""
        if not self._g:
            raise RuntimeError("Not connected to JanusGraph")
            
        try:
            vertex_data = self._g.V(vertex_id).valueMap(True).next()
            
            return Vertex(
                id=vertex_data[T.id],
                label=vertex_data[T.label],
                properties={
                    k: v[0] if isinstance(v, list) and len(v) == 1 else v
                    for k, v in vertex_data.items()
                    if k not in [T.id, T.label]
                }
            )
        except StopIteration:
            return None
            
    def update_vertex(
        self,
        vertex_id: Any,
        properties: Dict[str, Any]
    ) -> bool:
        """Update vertex properties"""
        if not self._g:
            raise RuntimeError("Not connected to JanusGraph")
            
        try:
            v = self._g.V(vertex_id)
            
            for key, value in properties.items():
                v = v.property(key, value)
                
            v.iterate()
            return True
        except Exception:
            return False
            
    def delete_vertex(self, vertex_id: Any) -> bool:
        """Delete a vertex"""
        if not self._g:
            raise RuntimeError("Not connected to JanusGraph")
            
        try:
            self._g.V(vertex_id).drop().iterate()
            return True
        except Exception:
            return False
            
    def find_vertices(
        self,
        label: Optional[str] = None,
        properties: Optional[Dict[str, Any]] = None,
        limit: Optional[int] = None
    ) -> List[Vertex]:
        """Find vertices by label and/or properties"""
        if not self._g:
            raise RuntimeError("Not connected to JanusGraph")
            
        # Start traversal
        t = self._g.V()
        
        # Filter by label
        if label:
            t = t.hasLabel(label)
            
        # Filter by properties
        if properties:
            for key, value in properties.items():
                t = t.has(key, value)
                
        # Apply limit
        if limit:
            t = t.limit(limit)
            
        # Get results
        vertices = []
        for vertex_data in t.valueMap(True).toList():
            vertices.append(Vertex(
                id=vertex_data[T.id],
                label=vertex_data[T.label],
                properties={
                    k: v[0] if isinstance(v, list) and len(v) == 1 else v
                    for k, v in vertex_data.items()
                    if k not in [T.id, T.label]
                }
            ))
            
        return vertices
        
    # Edge operations
    
    def add_edge(
        self,
        out_vertex_id: Any,
        in_vertex_id: Any,
        label: str,
        properties: Optional[Dict[str, Any]] = None
    ) -> Edge:
        """Add an edge between vertices"""
        if not self._g:
            raise RuntimeError("Not connected to JanusGraph")
            
        # Build edge
        e = self._g.V(out_vertex_id).addE(label).to(__.V(in_vertex_id))
        
        # Add properties
        if properties:
            for key, value in properties.items():
                e = e.property(key, value)
                
        # Execute and get result
        result = e.next()
        
        # Get edge details
        edge_data = self._g.E(result).valueMap(True).next()
        
        # Get vertices
        out_vertex = self.get_vertex(out_vertex_id)
        in_vertex = self.get_vertex(in_vertex_id)
        
        return Edge(
            id=edge_data[T.id],
            label=edge_data[T.label],
            out_vertex=out_vertex,
            in_vertex=in_vertex,
            properties={
                k: v for k, v in edge_data.items()
                if k not in [T.id, T.label]
            }
        )
        
    def get_edge(self, edge_id: Any) -> Optional[Edge]:
        """Get edge by ID"""
        if not self._g:
            raise RuntimeError("Not connected to JanusGraph")
            
        try:
            edge_data = self._g.E(edge_id).valueMap(True).next()
            
            # Get vertices
            vertices = self._g.E(edge_id).bothV().toList()
            out_vertex = self.get_vertex(vertices[0])
            in_vertex = self.get_vertex(vertices[1])
            
            return Edge(
                id=edge_data[T.id],
                label=edge_data[T.label],
                out_vertex=out_vertex,
                in_vertex=in_vertex,
                properties={
                    k: v for k, v in edge_data.items()
                    if k not in [T.id, T.label]
                }
            )
        except StopIteration:
            return None
            
    def delete_edge(self, edge_id: Any) -> bool:
        """Delete an edge"""
        if not self._g:
            raise RuntimeError("Not connected to JanusGraph")
            
        try:
            self._g.E(edge_id).drop().iterate()
            return True
        except Exception:
            return False
            
    # Traversal operations
    
    def get_neighbors(
        self,
        vertex_id: Any,
        direction: str = "both",
        edge_labels: Optional[List[str]] = None,
        limit: Optional[int] = None
    ) -> List[Vertex]:
        """Get neighboring vertices"""
        if not self._g:
            raise RuntimeError("Not connected to JanusGraph")
            
        # Start traversal
        t = self._g.V(vertex_id)
        
        # Choose direction
        if direction == "out":
            t = t.out(*edge_labels) if edge_labels else t.out()
        elif direction == "in":
            t = t.in_(*edge_labels) if edge_labels else t.in_()
        else:  # both
            t = t.both(*edge_labels) if edge_labels else t.both()
            
        # Apply limit
        if limit:
            t = t.limit(limit)
            
        # Get results
        vertices = []
        for vertex_data in t.valueMap(True).toList():
            vertices.append(Vertex(
                id=vertex_data[T.id],
                label=vertex_data[T.label],
                properties={
                    k: v[0] if isinstance(v, list) and len(v) == 1 else v
                    for k, v in vertex_data.items()
                    if k not in [T.id, T.label]
                }
            ))
            
        return vertices
        
    def shortest_path(
        self,
        source_id: Any,
        target_id: Any,
        max_distance: Optional[int] = None
    ) -> Optional[List[Vertex]]:
        """Find shortest path between vertices"""
        if not self._g:
            raise RuntimeError("Not connected to JanusGraph")
            
        try:
            # Build path query
            t = self._g.V(source_id).repeat(__.out().simplePath()).until(
                __.hasId(target_id)
            )
            
            if max_distance:
                t = t.limit(max_distance)
                
            # Get path
            path = t.path().limit(1).next()
            
            # Convert to vertices
            vertices = []
            for vertex_id in path:
                vertex = self.get_vertex(vertex_id)
                if vertex:
                    vertices.append(vertex)
                    
            return vertices if vertices else None
            
        except StopIteration:
            return None
            
    # Schema management
    
    def create_vertex_label(
        self,
        label: str,
        properties: Optional[Dict[str, str]] = None
    ):
        """Create vertex label in schema"""
        if not self._client:
            raise RuntimeError("Not connected to JanusGraph")
            
        # Build schema script
        script = f"mgmt = graph.openManagement(); "
        script += f"if (!mgmt.getVertexLabel('{label}')) {{ "
        script += f"mgmt.makeVertexLabel('{label}').make(); "
        
        # Add properties
        if properties:
            for prop_name, prop_type in properties.items():
                script += f"if (!mgmt.getPropertyKey('{prop_name}')) {{ "
                script += f"mgmt.makePropertyKey('{prop_name}').dataType({prop_type}).make(); "
                script += "} "
                
        script += "} mgmt.commit();"
        
        # Execute
        self._client.submit(script).all().result()
        
    def create_edge_label(
        self,
        label: str,
        multiplicity: str = "MULTI",
        properties: Optional[Dict[str, str]] = None
    ):
        """Create edge label in schema"""
        if not self._client:
            raise RuntimeError("Not connected to JanusGraph")
            
        # Build schema script
        script = f"mgmt = graph.openManagement(); "
        script += f"if (!mgmt.getEdgeLabel('{label}')) {{ "
        script += f"mgmt.makeEdgeLabel('{label}').multiplicity({multiplicity}).make(); "
        
        # Add properties
        if properties:
            for prop_name, prop_type in properties.items():
                script += f"if (!mgmt.getPropertyKey('{prop_name}')) {{ "
                script += f"mgmt.makePropertyKey('{prop_name}').dataType({prop_type}).make(); "
                script += "} "
                
        script += "} mgmt.commit();"
        
        # Execute
        self._client.submit(script).all().result()
        
    def create_index(
        self,
        name: str,
        label: str,
        properties: List[str],
        unique: bool = False,
        composite: bool = True
    ):
        """Create an index"""
        if not self._client:
            raise RuntimeError("Not connected to JanusGraph")
            
        # Build index script
        script = f"mgmt = graph.openManagement(); "
        
        if composite:
            script += f"if (!mgmt.getGraphIndex('{name}')) {{ "
            script += f"builder = mgmt.buildIndex('{name}', Vertex.class); "
            
            for prop in properties:
                script += f"builder.addKey(mgmt.getPropertyKey('{prop}')); "
                
            if unique:
                script += "builder.unique(); "
                
            script += f"builder.indexOnly(mgmt.getVertexLabel('{label}')); "
            script += "builder.buildCompositeIndex(); "
            script += "} mgmt.commit();"
        else:
            # Mixed index (requires external index backend like Elasticsearch)
            script += f"if (!mgmt.getGraphIndex('{name}')) {{ "
            script += f"mgmt.buildIndex('{name}', Vertex.class)"
            
            for prop in properties:
                script += f".addKey(mgmt.getPropertyKey('{prop}'))"
                
            script += f".indexOnly(mgmt.getVertexLabel('{label}'))"
            script += ".buildMixedIndex('search'); "
            script += "} mgmt.commit();"
            
        # Execute
        self._client.submit(script).all().result()
        
    # Raw Gremlin queries
    
    def execute_gremlin(self, query: str) -> List[Any]:
        """Execute raw Gremlin query"""
        if not self._client:
            raise RuntimeError("Not connected to JanusGraph")
            
        result_set = self._client.submit(query)
        return result_set.all().result()
        
    # Transaction support
    
    def begin_transaction(self):
        """Begin a transaction"""
        # JanusGraph transactions are handled automatically
        # This is a placeholder for explicit transaction management
        pass
        
    def commit_transaction(self):
        """Commit current transaction"""
        if self._g:
            self._g.tx().commit()
            
    def rollback_transaction(self):
        """Rollback current transaction"""
        if self._g:
            self._g.tx().rollback() 