"""
Cassandra Provisioner

Provisions Cassandra keyspaces and tables for tenants.
"""
import logging
from typing import Dict, Any
import uuid
from datetime import datetime

from cassandra.cluster import Cluster
from cassandra.auth import PlainTextAuthProvider

from platformq_resource_common import (
    ResourceType, InfrastructureResource, ResourceStatus,
    IResourceProvisioner
)
from ..core.config import Settings

logger = logging.getLogger(__name__)


class CassandraProvisioner(IResourceProvisioner):
    """Provisions Cassandra resources"""
    
    def __init__(self, settings: Settings):
        self.settings = settings
        self.cluster = None
        self.session = None
    
    async def initialize(self):
        """Initialize Cassandra connection"""
        try:
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
            
            logger.info("Cassandra provisioner initialized")
            
        except Exception as e:
            logger.error(f"Failed to initialize Cassandra provisioner: {e}")
            raise
    
    async def shutdown(self):
        """Shutdown Cassandra connection"""
        if self.cluster:
            self.cluster.shutdown()
    
    async def provision(
        self,
        tenant_id: str,
        tenant_name: str,
        metadata: Dict[str, Any]
    ) -> InfrastructureResource:
        """Provision Cassandra keyspace for tenant"""
        keyspace_name = f"tenant_{tenant_id.replace('-', '_')}"
        
        try:
            # Create keyspace
            replication_factor = metadata.get(
                'replication_factor',
                self.settings.default_cassandra_replication_factor
            )
            
            create_keyspace_query = f"""
                CREATE KEYSPACE IF NOT EXISTS {keyspace_name}
                WITH replication = {{
                    'class': 'SimpleStrategy',
                    'replication_factor': {replication_factor}
                }}
                AND durable_writes = true
            """
            
            self.session.execute(create_keyspace_query)
            logger.info(f"Created Cassandra keyspace: {keyspace_name}")
            
            # Create default tables
            await self._create_default_tables(keyspace_name)
            
            # Create resource object
            resource = InfrastructureResource(
                resource_id=str(uuid.uuid4()),
                resource_type=ResourceType.CASSANDRA,
                resource_name=keyspace_name,
                tenant_id=tenant_id,
                status=ResourceStatus.ACTIVE,
                configuration={
                    "keyspace": keyspace_name,
                    "replication_factor": replication_factor,
                    "contact_points": self.settings.cassandra_hosts,
                },
                created_at=datetime.utcnow()
            )
            
            return resource
            
        except Exception as e:
            logger.error(f"Failed to provision Cassandra for tenant {tenant_id}: {e}")
            raise
    
    async def deprovision(self, tenant_id: str, resource_name: str) -> bool:
        """Deprovision Cassandra keyspace"""
        try:
            # Drop keyspace
            drop_query = f"DROP KEYSPACE IF EXISTS {resource_name}"
            self.session.execute(drop_query)
            
            logger.info(f"Dropped Cassandra keyspace: {resource_name}")
            return True
            
        except Exception as e:
            logger.error(f"Failed to deprovision Cassandra keyspace {resource_name}: {e}")
            return False
    
    async def validate(self, tenant_id: str) -> bool:
        """Validate Cassandra provisioning"""
        keyspace_name = f"tenant_{tenant_id.replace('-', '_')}"
        
        try:
            # Check if keyspace exists
            query = """
                SELECT keyspace_name 
                FROM system_schema.keyspaces 
                WHERE keyspace_name = %s
            """
            result = self.session.execute(query, [keyspace_name])
            
            return len(list(result)) > 0
            
        except Exception as e:
            logger.error(f"Failed to validate Cassandra for tenant {tenant_id}: {e}")
            return False
    
    def get_resource_type(self) -> ResourceType:
        """Get the resource type this provisioner handles"""
        return ResourceType.CASSANDRA
    
    async def _create_default_tables(self, keyspace_name: str):
        """Create default tables for the tenant"""
        # Use the keyspace
        self.session.set_keyspace(keyspace_name)
        
        # Create default tables
        tables = [
            """
            CREATE TABLE IF NOT EXISTS events (
                id UUID PRIMARY KEY,
                event_type TEXT,
                timestamp TIMESTAMP,
                data TEXT
            )
            """,
            """
            CREATE TABLE IF NOT EXISTS configurations (
                key TEXT PRIMARY KEY,
                value TEXT,
                updated_at TIMESTAMP
            )
            """,
            """
            CREATE TABLE IF NOT EXISTS audit_log (
                id UUID PRIMARY KEY,
                action TEXT,
                user_id TEXT,
                timestamp TIMESTAMP,
                details TEXT
            )
            """
        ]
        
        for table_query in tables:
            self.session.execute(table_query)
        
        logger.info(f"Created default tables in keyspace: {keyspace_name}") 