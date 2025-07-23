"""
JanusGraph Client

Client for interacting with JanusGraph graph database.
"""

import asyncio
import uuid
from typing import Dict, Any, List, Optional
from datetime import datetime
from gremlin_python.driver import client, serializer
from gremlin_python.process.anonymous_traversal import traversal
from gremlin_python.driver.driver_remote_connection import DriverRemoteConnection
from gremlin_python.process.graph_traversal import __

from data_intelligence_common import StructuredLogger
from data_intelligence_common.vault_consul import VaultConsulIntegration

logger = StructuredLogger.get_logger(__name__)


class JanusGraphClient:
    """
    Client for JanusGraph operations
    """
    
    def __init__(self, connection_url: str, vault_consul: VaultConsulIntegration):
        self.connection_url = connection_url
        self.vault_consul = vault_consul
        self.client = None
        self.g = None
        self.connection = None
        
        # Schema definitions
        self.vertex_labels = [
            "Entity", "Dataset", "Pipeline", "Model", "User", "Organization",
            "Asset", "Transaction", "Event", "Metric", "Rule", "Policy"
        ]
        
        self.edge_labels = [
            "DERIVES_FROM", "DEPENDS_ON", "OWNS", "CREATED_BY", "MODIFIED_BY",
            "TRUSTS", "VALIDATES", "TRIGGERS", "PRODUCES", "CONSUMES",
            "MEMBER_OF", "RELATES_TO", "TEMPORAL_LINK"
        ]
        
        self.property_keys = {
            "id": "String",
            "name": "String",
            "type": "String",
            "created_at": "Date",
            "updated_at": "Date",
            "version": "Integer",
            "status": "String",
            "metadata": "String",  # JSON string
            "trust_score": "Double",
            "quality_score": "Double",
            "importance": "Double"
        }
        
        self.indices = [
            ("composite", "byId", ["id"], ["Entity", "Dataset", "Pipeline", "Model"]),
            ("composite", "byName", ["name"], ["Entity", "Dataset", "Pipeline", "Model"]),
            ("composite", "byType", ["type"], ["Entity", "Event"]),
            ("mixed", "search", ["name", "metadata"], None)  # Full-text search
        ]
    
    async def initialize(self):
        """Initialize JanusGraph connection"""
        logger.info("initializing_janusgraph_client")
        
        try:
            # Get credentials from Vault if available
            credentials = await self._get_credentials()
            
            # Create Gremlin client
            self.client = client.Client(
                self.connection_url,
                'g',
                message_serializer=serializer.GraphSONSerializersV3d0()
            )
            
            # Create traversal source
            self.connection = DriverRemoteConnection(
                self.connection_url,
                'g',
                message_serializer=serializer.GraphSONSerializersV3d0()
            )
            self.g = traversal().withRemote(self.connection)
            
            # Create schema if needed
            await self.create_schema()
            
            logger.info("janusgraph_client_initialized")
            
        except Exception as e:
            logger.error(f"Failed to initialize JanusGraph client: {e}")
            raise
    
    async def cleanup(self):
        """Cleanup resources"""
        if self.client:
            self.client.close()
        if self.connection:
            self.connection.close()
    
    async def _get_credentials(self) -> Dict[str, str]:
        """Get JanusGraph credentials from Vault"""
        try:
            creds = await self.vault_consul.get_secret("janusgraph/creds")
            return {
                "username": creds.get("username"),
                "password": creds.get("password")
            }
        except Exception:
            return {}
    
    async def create_schema(self):
        """Create JanusGraph schema"""
        logger.info("creating_janusgraph_schema")
        
        try:
            # Check if schema exists
            result = await self.execute_query("g.V().hasLabel('Entity').limit(1).count()")
            if result and result[0] > 0:
                logger.info("Schema already exists")
                return
            
            # Create property keys
            for key, datatype in self.property_keys.items():
                query = f"mgmt = graph.openManagement(); " \
                       f"if (!mgmt.containsPropertyKey('{key}')) {{ " \
                       f"mgmt.makePropertyKey('{key}').dataType({datatype}.class).make(); " \
                       f"}}; mgmt.commit()"
                await self.execute_management_query(query)
            
            # Create vertex labels
            for label in self.vertex_labels:
                query = f"mgmt = graph.openManagement(); " \
                       f"if (!mgmt.containsVertexLabel('{label}')) {{ " \
                       f"mgmt.makeVertexLabel('{label}').make(); " \
                       f"}}; mgmt.commit()"
                await self.execute_management_query(query)
            
            # Create edge labels
            for label in self.edge_labels:
                query = f"mgmt = graph.openManagement(); " \
                       f"if (!mgmt.containsEdgeLabel('{label}')) {{ " \
                       f"mgmt.makeEdgeLabel('{label}').multiplicity(MULTI).make(); " \
                       f"}}; mgmt.commit()"
                await self.execute_management_query(query)
            
            # Create indices
            for index_type, index_name, keys, labels in self.indices:
                if index_type == "composite":
                    query = self._build_composite_index_query(index_name, keys, labels)
                elif index_type == "mixed":
                    query = self._build_mixed_index_query(index_name, keys)
                
                await self.execute_management_query(query)
            
            logger.info("janusgraph_schema_created")
            
        except Exception as e:
            logger.error(f"Failed to create schema: {e}")
            raise
    
    def _build_composite_index_query(self, name: str, keys: List[str], 
                                   labels: Optional[List[str]]) -> str:
        """Build composite index creation query"""
        query = f"mgmt = graph.openManagement(); "
        query += f"if (!mgmt.containsGraphIndex('{name}')) {{ "
        query += f"idx = mgmt.buildIndex('{name}', Vertex.class)"
        
        for key in keys:
            query += f".addKey(mgmt.getPropertyKey('{key}'))"
        
        if labels:
            for label in labels:
                query += f".indexOnly(mgmt.getVertexLabel('{label}'))"
        
        query += ".buildCompositeIndex(); "
        query += "}; mgmt.commit()"
        
        return query
    
    def _build_mixed_index_query(self, name: str, keys: List[str]) -> str:
        """Build mixed index creation query"""
        query = f"mgmt = graph.openManagement(); "
        query += f"if (!mgmt.containsGraphIndex('{name}')) {{ "
        query += f"idx = mgmt.buildIndex('{name}', Vertex.class)"
        
        for key in keys:
            query += f".addKey(mgmt.getPropertyKey('{key}'))"
        
        query += ".buildMixedIndex('search'); "
        query += "}; mgmt.commit()"
        
        return query
    
    async def execute_query(self, query: str, bindings: Dict[str, Any] = None) -> List[Any]:
        """Execute a Gremlin query"""
        try:
            if bindings:
                result = self.client.submit(query, bindings)
            else:
                result = self.client.submit(query)
            
            return result.all().result()
            
        except Exception as e:
            logger.error(f"Query execution failed: {e}")
            raise
    
    async def execute_management_query(self, query: str) -> None:
        """Execute a management query"""
        try:
            self.client.submit(query).all().result()
        except Exception as e:
            logger.error(f"Management query failed: {e}")
            raise
    
    async def create_vertex(self, label: str, properties: Dict[str, Any]) -> str:
        """Create a new vertex"""
        vertex_id = properties.get("id", str(uuid.uuid4()))
        properties["id"] = vertex_id
        properties["created_at"] = datetime.utcnow().isoformat()
        properties["updated_at"] = datetime.utcnow().isoformat()
        
        # Build property string
        prop_parts = []
        for key, value in properties.items():
            if isinstance(value, str):
                prop_parts.append(f"property('{key}', '{value}')")
            elif isinstance(value, (int, float)):
                prop_parts.append(f"property('{key}', {value})")
            elif isinstance(value, dict):
                import json
                prop_parts.append(f"property('{key}', '{json.dumps(value)}')")
        
        query = f"g.addV('{label}').{'.'.join(prop_parts)}"
        
        await self.execute_query(query)
        return vertex_id
    
    async def create_edge(self, label: str, source_id: str, target_id: str,
                         properties: Dict[str, Any] = None) -> str:
        """Create a new edge"""
        edge_id = str(uuid.uuid4())
        
        if properties is None:
            properties = {}
        
        properties["created_at"] = datetime.utcnow().isoformat()
        
        # Build property string
        prop_parts = []
        for key, value in properties.items():
            if isinstance(value, str):
                prop_parts.append(f"property('{key}', '{value}')")
            elif isinstance(value, (int, float)):
                prop_parts.append(f"property('{key}', {value})")
        
        if prop_parts:
            prop_string = f".{'.'.join(prop_parts)}"
        else:
            prop_string = ""
        
        query = f"g.V().has('id', source).addE(label).to(g.V().has('id', target)){prop_string}"
        
        await self.execute_query(query, {
            "source": source_id,
            "target": target_id,
            "label": label
        })
        
        return edge_id
    
    async def get_vertex(self, vertex_id: str) -> Optional[Dict[str, Any]]:
        """Get a vertex by ID"""
        query = "g.V().has('id', vertex_id).valueMap(true)"
        
        results = await self.execute_query(query, {"vertex_id": vertex_id})
        
        if results:
            return self._format_vertex(results[0])
        return None
    
    async def update_vertex(self, vertex_id: str, properties: Dict[str, Any]) -> bool:
        """Update vertex properties"""
        properties["updated_at"] = datetime.utcnow().isoformat()
        
        # Build property update string
        prop_parts = []
        for key, value in properties.items():
            if isinstance(value, str):
                prop_parts.append(f"property('{key}', '{value}')")
            elif isinstance(value, (int, float)):
                prop_parts.append(f"property('{key}', {value})")
            elif isinstance(value, dict):
                import json
                prop_parts.append(f"property('{key}', '{json.dumps(value)}')")
        
        query = f"g.V().has('id', vertex_id).{'.'.join(prop_parts)}"
        
        await self.execute_query(query, {"vertex_id": vertex_id})
        return True
    
    async def delete_vertex(self, vertex_id: str) -> bool:
        """Delete a vertex"""
        query = "g.V().has('id', vertex_id).drop()"
        
        await self.execute_query(query, {"vertex_id": vertex_id})
        return True
    
    async def search_vertices(self, label: Optional[str] = None,
                            properties: Dict[str, Any] = None,
                            limit: int = 100) -> List[Dict[str, Any]]:
        """Search for vertices"""
        query_parts = ["g.V()"]
        
        if label:
            query_parts.append(f"hasLabel('{label}')")
        
        if properties:
            for key, value in properties.items():
                if isinstance(value, str):
                    query_parts.append(f"has('{key}', '{value}')")
                else:
                    query_parts.append(f"has('{key}', {value})")
        
        query_parts.append(f"limit({limit})")
        query_parts.append("valueMap(true)")
        
        query = ".".join(query_parts)
        
        results = await self.execute_query(query)
        return [self._format_vertex(v) for v in results]
    
    async def get_neighbors(self, vertex_id: str, edge_label: Optional[str] = None,
                          direction: str = "both") -> List[Dict[str, Any]]:
        """Get neighbors of a vertex"""
        if edge_label:
            if direction == "out":
                query = f"g.V().has('id', vertex_id).out('{edge_label}').valueMap(true)"
            elif direction == "in":
                query = f"g.V().has('id', vertex_id).in('{edge_label}').valueMap(true)"
            else:
                query = f"g.V().has('id', vertex_id).both('{edge_label}').valueMap(true)"
        else:
            if direction == "out":
                query = "g.V().has('id', vertex_id).out().valueMap(true)"
            elif direction == "in":
                query = "g.V().has('id', vertex_id).in().valueMap(true)"
            else:
                query = "g.V().has('id', vertex_id).both().valueMap(true)"
        
        results = await self.execute_query(query, {"vertex_id": vertex_id})
        return [self._format_vertex(v) for v in results]
    
    def _format_vertex(self, raw_vertex: Dict) -> Dict[str, Any]:
        """Format raw vertex data"""
        formatted = {}
        
        for key, value in raw_vertex.items():
            if isinstance(value, list) and len(value) == 1:
                formatted[key] = value[0]
            else:
                formatted[key] = value
        
        return formatted
    
    async def health_check(self) -> Dict[str, Any]:
        """Check JanusGraph health"""
        try:
            # Simple query to check connection
            result = await self.execute_query("g.V().limit(1).count()")
            
            return {
                "healthy": True,
                "vertex_count": result[0] if result else 0,
                "connection_url": self.connection_url
            }
            
        except Exception as e:
            return {
                "healthy": False,
                "error": str(e),
                "connection_url": self.connection_url
            } 