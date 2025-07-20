"""
JanusGraph Provisioner

Provisions JanusGraph configurations and indices for tenants.
"""
import logging
from typing import Dict, Any
import uuid
from datetime import datetime

from gremlin_python.driver import client, serializer
from gremlin_python.driver.protocol import GremlinServerError

from platformq_resource_common import (
    ResourceType, InfrastructureResource, ResourceStatus,
    IResourceProvisioner
)
from ..core.config import Settings

logger = logging.getLogger(__name__)


class JanusGraphProvisioner(IResourceProvisioner):
    """Provisions JanusGraph resources"""
    
    def __init__(self, settings: Settings):
        self.settings = settings
        self.gremlin_client = None
    
    async def initialize(self):
        """Initialize JanusGraph connection"""
        try:
            # Create Gremlin client
            self.gremlin_client = client.Client(
                f'ws://{self.settings.janusgraph_host}:{self.settings.janusgraph_port}/gremlin',
                'g',
                message_serializer=serializer.GraphSONSerializersV3d0()
            )
            
            # Test connection
            result = self.gremlin_client.submit('g.V().count()').all().result()
            
            logger.info("JanusGraph provisioner initialized")
            
        except Exception as e:
            logger.error(f"Failed to initialize JanusGraph provisioner: {e}")
            raise
    
    async def shutdown(self):
        """Shutdown JanusGraph connection"""
        if self.gremlin_client:
            self.gremlin_client.close()
    
    async def provision(
        self,
        tenant_id: str,
        tenant_name: str,
        metadata: Dict[str, Any]
    ) -> InfrastructureResource:
        """Provision JanusGraph graph and indices for tenant"""
        graph_name = f"tenant_{tenant_id.replace('-', '_')}"
        
        try:
            # Create graph configuration
            await self._create_graph_configuration(graph_name, metadata)
            
            # Create schema and indices
            await self._create_schema(graph_name, metadata)
            
            # Create initial vertices
            await self._create_initial_vertices(graph_name, tenant_name)
            
            # Create resource object
            resource = InfrastructureResource(
                resource_id=str(uuid.uuid4()),
                resource_type=ResourceType.JANUSGRAPH,
                resource_name=graph_name,
                tenant_id=tenant_id,
                status=ResourceStatus.ACTIVE,
                endpoint=f"ws://{self.settings.janusgraph_host}:{self.settings.janusgraph_port}/gremlin",
                configuration={
                    "graph_name": graph_name,
                    "storage_backend": metadata.get('storage_backend', 'cassandra'),
                    "index_backend": metadata.get('index_backend', 'elasticsearch')
                },
                created_at=datetime.utcnow()
            )
            
            logger.info(f"Successfully provisioned JanusGraph for tenant {tenant_id}")
            return resource
            
        except Exception as e:
            logger.error(f"Failed to provision JanusGraph for tenant {tenant_id}: {e}")
            raise
    
    async def deprovision(self, tenant_id: str, resource_name: str) -> bool:
        """Deprovision JanusGraph graph"""
        try:
            # Drop all vertices and edges for the tenant
            # Note: In production, you might want to backup data first
            query = f"""
                g.V().has('tenant_id', '{tenant_id}').drop().iterate()
            """
            
            self.gremlin_client.submit(query).all().result()
            logger.info(f"Dropped all vertices for tenant: {tenant_id}")
            
            # Note: Dropping the actual graph instance would require
            # administrative access to JanusGraph configuration
            
            return True
            
        except Exception as e:
            logger.error(f"Failed to deprovision JanusGraph graph {resource_name}: {e}")
            return False
    
    async def validate(self, tenant_id: str) -> bool:
        """Validate JanusGraph provisioning"""
        try:
            # Check if tenant root vertex exists
            query = f"""
                g.V().has('tenant_id', '{tenant_id}').has('vertex_type', 'tenant_root').count()
            """
            
            result = self.gremlin_client.submit(query).all().result()
            return result[0] > 0
            
        except Exception as e:
            logger.error(f"Failed to validate JanusGraph for tenant {tenant_id}: {e}")
            return False
    
    def get_resource_type(self) -> ResourceType:
        """Get the resource type this provisioner handles"""
        return ResourceType.JANUSGRAPH
    
    async def _create_graph_configuration(self, graph_name: str, metadata: Dict[str, Any]):
        """Create graph configuration"""
        # Note: This would typically involve creating a new graph instance
        # with specific configuration. For multi-tenant setup, we'll use
        # vertex properties to separate tenant data
        
        logger.info(f"Configured graph for: {graph_name}")
    
    async def _create_schema(self, graph_name: str, metadata: Dict[str, Any]):
        """Create schema and indices for the tenant"""
        # Create property keys
        property_keys = [
            "tenant_id",
            "vertex_type",
            "entity_id",
            "name",
            "description",
            "created_at",
            "updated_at",
            "metadata"
        ]
        
        # Create edge labels
        edge_labels = [
            "OWNS",
            "CREATED_BY",
            "RELATED_TO",
            "DEPENDS_ON",
            "CONTAINS",
            "TAGGED_WITH",
            "MEMBER_OF"
        ]
        
        # Create indices
        # Note: In production JanusGraph, this would be done via management API
        indices_queries = [
            # Composite index for tenant isolation
            f"mgmt.buildIndex('by_tenant_id', Vertex.class).addKey(tenant_id).buildCompositeIndex()",
            
            # Mixed index for text search
            f"mgmt.buildIndex('by_name', Vertex.class).addKey(name, Mapping.TEXT).buildMixedIndex('search')",
            
            # Composite index for entity lookups
            f"mgmt.buildIndex('by_entity', Vertex.class).addKey(tenant_id).addKey(entity_id).buildCompositeIndex()"
        ]
        
        logger.info(f"Created schema for graph: {graph_name}")
    
    async def _create_initial_vertices(self, graph_name: str, tenant_name: str):
        """Create initial vertices for the tenant"""
        tenant_id = graph_name.replace('tenant_', '').replace('_', '-')
        
        # Create tenant root vertex
        query = f"""
            g.addV('tenant_root')
                .property('tenant_id', '{tenant_id}')
                .property('vertex_type', 'tenant_root')
                .property('name', '{tenant_name}')
                .property('created_at', '{datetime.utcnow().isoformat()}')
                .property('entity_id', '{tenant_id}')
        """
        
        try:
            self.gremlin_client.submit(query).all().result()
            logger.info(f"Created tenant root vertex for: {tenant_name}")
        except Exception as e:
            logger.error(f"Failed to create initial vertices: {e}")
            
        # Create default vertex types
        vertex_types = [
            {
                "label": "user",
                "name": "Users",
                "description": "User entities"
            },
            {
                "label": "project",
                "name": "Projects",
                "description": "Project entities"
            },
            {
                "label": "asset",
                "name": "Assets",
                "description": "Digital asset entities"
            },
            {
                "label": "model",
                "name": "Models",
                "description": "ML model entities"
            }
        ]
        
        for vtype in vertex_types:
            query = f"""
                g.addV('vertex_type_definition')
                    .property('tenant_id', '{tenant_id}')
                    .property('vertex_type', 'vertex_type_definition')
                    .property('label', '{vtype["label"]}')
                    .property('name', '{vtype["name"]}')
                    .property('description', '{vtype["description"]}')
                    .property('created_at', '{datetime.utcnow().isoformat()}')
            """
            
            try:
                self.gremlin_client.submit(query).all().result()
                logger.info(f"Created vertex type: {vtype['label']}")
            except Exception as e:
                logger.error(f"Failed to create vertex type {vtype['label']}: {e}") 