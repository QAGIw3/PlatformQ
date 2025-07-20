"""Repository for Tenant Provisioning Service"""

import logging
from typing import List, Optional, Dict, Any
from datetime import datetime
import json

from cassandra.cluster import Cluster
from pyignite import Client as IgniteClient

from platformq_provisioning_common import (
    IProvisioningRepository,
    ProvisioningRequest,
    ProvisioningResult,
    ProvisioningStatus,
    InfrastructureResource
)

from .config import Settings

logger = logging.getLogger(__name__)


class ProvisioningRepository(IProvisioningRepository):
    """Repository for provisioning data using Cassandra and Ignite"""
    
    def __init__(self, settings: Settings):
        self.settings = settings
        self.cassandra_cluster = None
        self.cassandra_session = None
        self.ignite_client = None
        
    async def initialize(self):
        """Initialize database connections"""
        # Initialize Cassandra
        self.cassandra_cluster = Cluster(self.settings.cassandra_hosts)
        self.cassandra_session = self.cassandra_cluster.connect()
        
        # Create keyspace if not exists
        await self._create_keyspace()
        
        # Use keyspace
        self.cassandra_session.set_keyspace(self.settings.cassandra_keyspace)
        
        # Create tables
        await self._create_tables()
        
        # Initialize Ignite for caching
        self.ignite_client = IgniteClient()
        self.ignite_client.connect([
            (self.settings.ignite_host, self.settings.ignite_port)
        ])
        
        # Create caches
        self.requests_cache = self.ignite_client.get_or_create_cache('provisioning_requests')
        self.results_cache = self.ignite_client.get_or_create_cache('provisioning_results')
        
        logger.info("Provisioning repository initialized")
    
    async def close(self):
        """Close database connections"""
        if self.cassandra_cluster:
            self.cassandra_cluster.shutdown()
        if self.ignite_client:
            self.ignite_client.close()
    
    async def _create_keyspace(self):
        """Create Cassandra keyspace"""
        query = f"""
            CREATE KEYSPACE IF NOT EXISTS {self.settings.cassandra_keyspace}
            WITH replication = {{'class': 'SimpleStrategy', 'replication_factor': 1}}
        """
        self.cassandra_session.execute(query)
    
    async def _create_tables(self):
        """Create Cassandra tables"""
        # Provisioning requests table
        requests_table = """
            CREATE TABLE IF NOT EXISTS provisioning_requests (
                request_id text PRIMARY KEY,
                tenant_id text,
                tenant_name text,
                tier text,
                requested_by text,
                requested_at timestamp,
                status text,
                metadata text
            )
        """
        self.cassandra_session.execute(requests_table)
        
        # Infrastructure resources table
        resources_table = """
            CREATE TABLE IF NOT EXISTS infrastructure_resources (
                resource_id text PRIMARY KEY,
                request_id text,
                tenant_id text,
                resource_type text,
                resource_name text,
                status text,
                provisioned_at timestamp,
                provisioned_by text,
                metadata text,
                error_message text
            )
        """
        self.cassandra_session.execute(resources_table)
        
        # Create indexes
        self.cassandra_session.execute(
            "CREATE INDEX IF NOT EXISTS ON infrastructure_resources (tenant_id)"
        )
        self.cassandra_session.execute(
            "CREATE INDEX IF NOT EXISTS ON infrastructure_resources (request_id)"
        )
    
    async def create_request(self, request: ProvisioningRequest) -> str:
        """Store a new provisioning request"""
        query = """
            INSERT INTO provisioning_requests (
                request_id, tenant_id, tenant_name, tier,
                requested_by, requested_at, status, metadata
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """
        
        self.cassandra_session.execute(query, [
            request.request_id,
            request.tenant_id,
            request.tenant_name,
            request.tier.value,
            request.requested_by,
            request.requested_at,
            ProvisioningStatus.PENDING.value,
            json.dumps(request.metadata)
        ])
        
        # Cache the request
        self.requests_cache.put(request.request_id, request.dict())
        
        return request.request_id
    
    async def update_request_status(
        self,
        request_id: str,
        status: ProvisioningStatus
    ) -> bool:
        """Update the status of a provisioning request"""
        query = """
            UPDATE provisioning_requests
            SET status = ?
            WHERE request_id = ?
        """
        
        self.cassandra_session.execute(query, [status.value, request_id])
        
        # Update cache
        if self.requests_cache.contains_key(request_id):
            request_dict = self.requests_cache.get(request_id)
            request_dict['status'] = status.value
            self.requests_cache.put(request_id, request_dict)
        
        return True
    
    async def add_provisioned_resource(
        self,
        request_id: str,
        resource: InfrastructureResource
    ) -> bool:
        """Add a provisioned resource to a request"""
        query = """
            INSERT INTO infrastructure_resources (
                resource_id, request_id, tenant_id, resource_type,
                resource_name, status, provisioned_at, provisioned_by,
                metadata, error_message
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """
        
        self.cassandra_session.execute(query, [
            resource.resource_id,
            request_id,
            resource.tenant_id,
            resource.resource_type.value,
            resource.resource_name,
            resource.status.value,
            resource.provisioned_at,
            resource.provisioned_by,
            json.dumps(resource.metadata),
            resource.error_message
        ])
        
        return True
    
    async def get_request(self, request_id: str) -> Optional[ProvisioningRequest]:
        """Get a provisioning request by ID"""
        # Check cache first
        if self.requests_cache.contains_key(request_id):
            request_dict = self.requests_cache.get(request_id)
            return ProvisioningRequest(**request_dict)
        
        # Query database
        query = """
            SELECT * FROM provisioning_requests
            WHERE request_id = ?
        """
        
        result = self.cassandra_session.execute(query, [request_id])
        row = result.one()
        
        if row:
            request = ProvisioningRequest(
                request_id=row.request_id,
                tenant_id=row.tenant_id,
                tenant_name=row.tenant_name,
                tier=row.tier,
                requested_by=row.requested_by,
                requested_at=row.requested_at,
                metadata=json.loads(row.metadata) if row.metadata else {}
            )
            
            # Cache it
            self.requests_cache.put(request_id, request.dict())
            
            return request
        
        return None
    
    async def get_tenant_resources(
        self,
        tenant_id: str
    ) -> List[InfrastructureResource]:
        """Get all resources for a tenant"""
        query = """
            SELECT * FROM infrastructure_resources
            WHERE tenant_id = ?
        """
        
        result = self.cassandra_session.execute(query, [tenant_id])
        
        resources = []
        for row in result:
            resource = InfrastructureResource(
                resource_id=row.resource_id,
                resource_type=row.resource_type,
                resource_name=row.resource_name,
                tenant_id=row.tenant_id,
                status=row.status,
                provisioned_at=row.provisioned_at,
                provisioned_by=row.provisioned_by,
                metadata=json.loads(row.metadata) if row.metadata else {},
                error_message=row.error_message
            )
            resources.append(resource)
        
        return resources
    
    async def get_provisioning_result(
        self,
        request_id: str
    ) -> Optional[ProvisioningResult]:
        """Get provisioning result for a request"""
        # Check cache first
        if self.results_cache.contains_key(request_id):
            result_dict = self.results_cache.get(request_id)
            return ProvisioningResult(**result_dict)
        
        # Get request
        request = await self.get_request(request_id)
        if not request:
            return None
        
        # Get resources
        resources_query = """
            SELECT * FROM infrastructure_resources
            WHERE request_id = ?
        """
        
        resources_result = self.cassandra_session.execute(resources_query, [request_id])
        
        provisioned_resources = []
        failed_resources = []
        
        for row in resources_result:
            resource = InfrastructureResource(
                resource_id=row.resource_id,
                resource_type=row.resource_type,
                resource_name=row.resource_name,
                tenant_id=row.tenant_id,
                status=row.status,
                provisioned_at=row.provisioned_at,
                provisioned_by=row.provisioned_by,
                metadata=json.loads(row.metadata) if row.metadata else {},
                error_message=row.error_message
            )
            
            if resource.status == ProvisioningStatus.FAILED:
                failed_resources.append(resource)
            else:
                provisioned_resources.append(resource)
        
        # Create result
        result = ProvisioningResult(
            request_id=request_id,
            tenant_id=request.tenant_id,
            status=ProvisioningStatus.COMPLETED if not failed_resources else ProvisioningStatus.PARTIALLY_COMPLETED,
            started_at=request.requested_at,
            provisioned_resources=provisioned_resources,
            failed_resources=failed_resources,
            metadata=request.metadata
        )
        
        # Cache it
        self.results_cache.put(request_id, result.dict())
        
        return result 