"""Cassandra Provisioner"""

import logging
from typing import Dict, Any
from datetime import datetime

from cassandra.cluster import Cluster
from cassandra.auth import PlainTextAuthProvider

from platformq_provisioning_common import (
    IResourceProvisioner,
    InfrastructureResource,
    ResourceType,
    ProvisioningStatus,
    ProvisioningError
)

from ..config import Settings

logger = logging.getLogger(__name__)


class CassandraProvisioner(IResourceProvisioner):
    """Provisions Cassandra keyspaces for tenants"""
    
    def __init__(self, settings: Settings):
        self.settings = settings
        self.cluster = None
        self.session = None
    
    async def initialize(self):
        """Initialize Cassandra connection"""
        try:
            auth_provider = None
            if hasattr(self.settings, 'cassandra_username'):
                auth_provider = PlainTextAuthProvider(
                    username=self.settings.cassandra_username,
                    password=self.settings.cassandra_password
                )
            
            self.cluster = Cluster(
                self.settings.cassandra_hosts,
                auth_provider=auth_provider
            )
            self.session = self.cluster.connect()
            logger.info("Connected to Cassandra cluster")
        except Exception as e:
            logger.error(f"Failed to connect to Cassandra: {e}")
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
        
        resource = InfrastructureResource(
            resource_type=ResourceType.CASSANDRA_KEYSPACE,
            resource_name=keyspace_name,
            tenant_id=tenant_id,
            provisioned_by="cassandra-provisioner"
        )
        
        try:
            # Validate keyspace name
            if not await self.validate(tenant_id):
                raise ProvisioningError("Invalid tenant ID for keyspace name")
            
            # Get replication factor from metadata or use default
            replication_factor = metadata.get('cassandra_replication_factor', 3)
            
            # Create keyspace with NetworkTopologyStrategy for production
            if self.settings.environment == 'production':
                replication_strategy = f"""
                    {{'class': 'NetworkTopologyStrategy', 
                     'datacenter1': {replication_factor}}}
                """
            else:
                replication_strategy = f"""
                    {{'class': 'SimpleStrategy', 
                     'replication_factor': {replication_factor}}}
                """
            
            create_keyspace_query = f"""
                CREATE KEYSPACE IF NOT EXISTS {keyspace_name}
                WITH replication = {replication_strategy}
                AND durable_writes = true
            """
            
            self.session.execute(create_keyspace_query)
            logger.info(f"Created keyspace {keyspace_name}")
            
            # Create default tables
            await self._create_default_tables(keyspace_name)
            
            # Set resource metadata
            resource.status = ProvisioningStatus.COMPLETED
            resource.provisioned_at = datetime.utcnow()
            resource.metadata = {
                'keyspace_name': keyspace_name,
                'replication_strategy': replication_strategy,
                'tables_created': ['metadata', 'events', 'user_data']
            }
            
            return resource
            
        except Exception as e:
            logger.error(f"Failed to provision Cassandra keyspace: {e}")
            resource.status = ProvisioningStatus.FAILED
            resource.error_message = str(e)
            raise ProvisioningError(
                f"Failed to create keyspace {keyspace_name}",
                ResourceType.CASSANDRA_KEYSPACE,
                {'error': str(e)}
            )
    
    async def deprovision(self, tenant_id: str, resource_name: str) -> bool:
        """Deprovision Cassandra keyspace"""
        try:
            # Drop keyspace
            drop_query = f"DROP KEYSPACE IF EXISTS {resource_name}"
            self.session.execute(drop_query)
            
            logger.info(f"Dropped keyspace {resource_name}")
            return True
            
        except Exception as e:
            logger.error(f"Failed to deprovision Cassandra keyspace: {e}")
            return False
    
    async def validate(self, tenant_id: str) -> bool:
        """Validate if provisioning is possible"""
        # Check if tenant_id is valid for keyspace naming
        keyspace_name = f"tenant_{tenant_id.replace('-', '_')}"
        
        # Cassandra keyspace names must be alphanumeric and underscore
        if not keyspace_name.replace('_', '').isalnum():
            return False
        
        # Check if keyspace already exists
        check_query = """
            SELECT keyspace_name 
            FROM system_schema.keyspaces 
            WHERE keyspace_name = %s
        """
        
        result = self.session.execute(check_query, [keyspace_name])
        if result.one():
            logger.warning(f"Keyspace {keyspace_name} already exists")
            # Return True as this is idempotent
            return True
        
        return True
    
    def get_resource_type(self) -> ResourceType:
        """Get the resource type this provisioner handles"""
        return ResourceType.CASSANDRA_KEYSPACE
    
    async def _create_default_tables(self, keyspace_name: str):
        """Create default tables in the keyspace"""
        # Metadata table
        metadata_table = f"""
            CREATE TABLE IF NOT EXISTS {keyspace_name}.metadata (
                key text PRIMARY KEY,
                value text,
                created_at timestamp,
                updated_at timestamp
            )
        """
        self.session.execute(metadata_table)
        
        # Events table with time-based partitioning
        events_table = f"""
            CREATE TABLE IF NOT EXISTS {keyspace_name}.events (
                event_date date,
                event_id timeuuid,
                event_type text,
                user_id text,
                data text,
                created_at timestamp,
                PRIMARY KEY ((event_date), event_id)
            ) WITH CLUSTERING ORDER BY (event_id DESC)
        """
        self.session.execute(events_table)
        
        # User data table
        user_data_table = f"""
            CREATE TABLE IF NOT EXISTS {keyspace_name}.user_data (
                user_id text PRIMARY KEY,
                profile_data text,
                preferences text,
                created_at timestamp,
                updated_at timestamp
            )
        """
        self.session.execute(user_data_table)
        
        logger.info(f"Created default tables in keyspace {keyspace_name}") 