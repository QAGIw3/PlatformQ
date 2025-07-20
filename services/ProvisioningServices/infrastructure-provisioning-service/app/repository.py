"""
Infrastructure Provisioning Repository

Handles data persistence for infrastructure provisioning.
"""
import json
import logging
from datetime import datetime
from typing import Dict, List, Optional, Any
import uuid

from cassandra.cluster import Cluster
from cassandra.auth import PlainTextAuthProvider

from platformq_resource_common import (
    InfrastructureResource, ProvisioningRequest, ProvisioningResult,
    ProvisioningStatus, ResourceStatus, ResourceType
)

logger = logging.getLogger(__name__)


class InfrastructureRepository:
    """Repository for infrastructure provisioning data"""
    
    def __init__(self, settings):
        self.settings = settings
        self.cluster = None
        self.session = None
        self._initialized = False
    
    async def initialize(self):
        """Initialize database connection and schema"""
        if self._initialized:
            return
        
        try:
            # Create Cassandra connection
            auth_provider = None
            if self.settings.cassandra_username:
                auth_provider = PlainTextAuthProvider(
                    username=self.settings.cassandra_username,
                    password=self.settings.cassandra_password
                )
            
            self.cluster = Cluster(
                contact_points=self.settings.cassandra_hosts,
                auth_provider=auth_provider
            )
            self.session = self.cluster.connect()
            
            # Create keyspace if not exists
            self.session.execute(f"""
                CREATE KEYSPACE IF NOT EXISTS {self.settings.cassandra_keyspace}
                WITH replication = {{
                    'class': 'SimpleStrategy',
                    'replication_factor': 3
                }}
            """)
            
            self.session.set_keyspace(self.settings.cassandra_keyspace)
            
            # Create tables
            await self._create_tables()
            
            self._initialized = True
            logger.info("Infrastructure repository initialized")
            
        except Exception as e:
            logger.error(f"Failed to initialize repository: {e}")
            raise
    
    async def close(self):
        """Close database connections"""
        if self.cluster:
            self.cluster.shutdown()
    
    async def _create_tables(self):
        """Create database tables"""
        # Infrastructure resources table
        self.session.execute("""
            CREATE TABLE IF NOT EXISTS infrastructure_resources (
                resource_id UUID PRIMARY KEY,
                tenant_id TEXT,
                resource_type TEXT,
                resource_name TEXT,
                status TEXT,
                endpoint TEXT,
                configuration TEXT,
                credentials TEXT,
                created_at TIMESTAMP,
                updated_at TIMESTAMP,
                metadata TEXT
            )
        """)
        
        # Create index on tenant_id
        self.session.execute("""
            CREATE INDEX IF NOT EXISTS infrastructure_by_tenant
            ON infrastructure_resources (tenant_id)
        """)
        
        # Provisioning requests table
        self.session.execute("""
            CREATE TABLE IF NOT EXISTS provisioning_requests (
                request_id UUID PRIMARY KEY,
                tenant_id TEXT,
                tenant_name TEXT,
                status TEXT,
                resources TEXT,
                requested_by TEXT,
                requested_at TIMESTAMP,
                completed_at TIMESTAMP,
                errors TEXT,
                metadata TEXT
            )
        """)
        
        logger.info("Database tables created")
    
    async def create_request(self, request: ProvisioningRequest) -> str:
        """Create a new provisioning request"""
        request_id = request.request_id or str(uuid.uuid4())
        
        query = """
            INSERT INTO provisioning_requests (
                request_id, tenant_id, tenant_name, status,
                resources, requested_by, requested_at, metadata
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """
        
        self.session.execute(query, [
            uuid.UUID(request_id),
            request.tenant_id,
            request.tenant_name,
            ProvisioningStatus.IN_PROGRESS.value,
            json.dumps([r.value for r in request.resources]),
            request.requested_by,
            request.requested_at,
            json.dumps(request.metadata)
        ])
        
        return request_id
    
    async def update_request_status(
        self,
        request_id: str,
        status: ProvisioningStatus
    ) -> bool:
        """Update provisioning request status"""
        query = """
            UPDATE provisioning_requests
            SET status = ?, completed_at = ?
            WHERE request_id = ?
        """
        
        self.session.execute(query, [
            status.value,
            datetime.utcnow() if status in [
                ProvisioningStatus.COMPLETED,
                ProvisioningStatus.FAILED,
                ProvisioningStatus.ROLLED_BACK
            ] else None,
            uuid.UUID(request_id)
        ])
        
        return True
    
    async def add_provisioned_resource(
        self,
        request_id: str,
        resource: InfrastructureResource
    ) -> bool:
        """Add a provisioned resource"""
        query = """
            INSERT INTO infrastructure_resources (
                resource_id, tenant_id, resource_type, resource_name,
                status, endpoint, configuration, credentials,
                created_at, updated_at, metadata
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """
        
        self.session.execute(query, [
            uuid.UUID(resource.resource_id),
            resource.tenant_id,
            resource.resource_type.value,
            resource.resource_name,
            resource.status.value,
            resource.endpoint,
            json.dumps(resource.configuration),
            json.dumps(resource.credentials) if resource.credentials else None,
            resource.created_at,
            resource.updated_at,
            json.dumps(resource.metadata)
        ])
        
        return True
    
    async def get_tenant_resources(
        self,
        tenant_id: str
    ) -> List[InfrastructureResource]:
        """Get all resources for a tenant"""
        query = """
            SELECT * FROM infrastructure_resources
            WHERE tenant_id = ?
            ALLOW FILTERING
        """
        
        rows = self.session.execute(query, [tenant_id])
        resources = []
        
        for row in rows:
            resource = InfrastructureResource(
                resource_id=str(row.resource_id),
                resource_type=ResourceType(row.resource_type),
                resource_name=row.resource_name,
                tenant_id=row.tenant_id,
                status=ResourceStatus(row.status),
                endpoint=row.endpoint,
                credentials=json.loads(row.credentials) if row.credentials else None,
                configuration=json.loads(row.configuration),
                created_at=row.created_at,
                updated_at=row.updated_at,
                metadata=json.loads(row.metadata) if row.metadata else {}
            )
            resources.append(resource)
        
        return resources
    
    async def get_resource(
        self,
        tenant_id: str,
        resource_type: ResourceType
    ) -> Optional[InfrastructureResource]:
        """Get specific resource for a tenant"""
        query = """
            SELECT * FROM infrastructure_resources
            WHERE tenant_id = ? AND resource_type = ?
            ALLOW FILTERING
        """
        
        rows = self.session.execute(query, [tenant_id, resource_type.value])
        
        for row in rows:
            return InfrastructureResource(
                resource_id=str(row.resource_id),
                resource_type=ResourceType(row.resource_type),
                resource_name=row.resource_name,
                tenant_id=row.tenant_id,
                status=ResourceStatus(row.status),
                endpoint=row.endpoint,
                credentials=json.loads(row.credentials) if row.credentials else None,
                configuration=json.loads(row.configuration),
                created_at=row.created_at,
                updated_at=row.updated_at,
                metadata=json.loads(row.metadata) if row.metadata else {}
            )
        
        return None
    
    async def update_resource_status(
        self,
        resource_id: str,
        status: ResourceStatus
    ) -> bool:
        """Update resource status"""
        query = """
            UPDATE infrastructure_resources
            SET status = ?, updated_at = ?
            WHERE resource_id = ?
        """
        
        self.session.execute(query, [
            status.value,
            datetime.utcnow(),
            uuid.UUID(resource_id)
        ])
        
        return True
    
    async def get_provisioning_result(
        self,
        request_id: str
    ) -> Optional[ProvisioningResult]:
        """Get provisioning result"""
        # Get request
        query = """
            SELECT * FROM provisioning_requests
            WHERE request_id = ?
        """
        
        rows = self.session.execute(query, [uuid.UUID(request_id)])
        request_row = next(iter(rows), None)
        
        if not request_row:
            return None
        
        # Get provisioned resources
        resources = await self._get_request_resources(request_row.tenant_id)
        
        result = ProvisioningResult(
            request_id=str(request_row.request_id),
            status=ProvisioningStatus(request_row.status),
            provisioned_resources=resources,
            failed_resources=[],  # TODO: Track failed resources
            errors=json.loads(request_row.errors) if request_row.errors else [],
            started_at=request_row.requested_at,
            completed_at=request_row.completed_at,
            metadata=json.loads(request_row.metadata) if request_row.metadata else {}
        )
        
        if result.started_at and result.completed_at:
            result.duration_seconds = (
                result.completed_at - result.started_at
            ).total_seconds()
        
        return result
    
    async def _get_request_resources(
        self,
        tenant_id: str
    ) -> List[InfrastructureResource]:
        """Get resources for a request"""
        return await self.get_tenant_resources(tenant_id) 