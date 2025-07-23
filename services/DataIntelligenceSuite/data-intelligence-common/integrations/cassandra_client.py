"""
Cassandra Client Integration

Provides high-level client for Apache Cassandra operations with Vault/Consul support.
"""

import logging
from typing import Any, Dict, List, Optional, Tuple, AsyncIterator
from contextlib import asynccontextmanager
from dataclasses import dataclass, field
from datetime import datetime
import asyncio

from cassandra.cluster import Cluster, Session
from cassandra.auth import PlainTextAuthProvider
from cassandra.policies import DCAwareRoundRobinPolicy, RetryPolicy
from cassandra.query import SimpleStatement, BatchStatement, ConsistencyLevel
from cassandra import OperationTimedOut, Unavailable

from platformq_shared.vault.vault_client import VaultClient
from platformq_shared.consul.consul_client import ConsulClient
from ..clients.base_client import BaseServiceClient, ClientConfig

logger = logging.getLogger(__name__)


@dataclass
class CassandraConfig(ClientConfig):
    """Configuration for Cassandra client with Vault/Consul support"""
    # Cassandra specific settings
    port: int = 9042
    keyspace: Optional[str] = None
    
    # Connection settings
    protocol_version: int = 4
    connection_timeout: float = 10.0
    request_timeout: float = 10.0
    
    # Consistency
    consistency_level: ConsistencyLevel = ConsistencyLevel.LOCAL_QUORUM
    serial_consistency_level: ConsistencyLevel = ConsistencyLevel.LOCAL_SERIAL
    
    # Pool settings
    max_connections_per_host: int = 8
    min_connections_per_host: int = 2
    
    # Vault specific
    vault_database_mount: str = "database"
    vault_database_role: str = "cassandra-readonly"
    
    def __post_init__(self):
        # Set service name for base client
        if not hasattr(self, 'service_name'):
            self.service_name = "cassandra"
        # Override vault role with Cassandra specific role
        self.vault_role = self.vault_database_role


class CassandraClient(BaseServiceClient):
    """
    High-level client for Cassandra operations with Vault/Consul support.
    
    Features:
    - Dynamic credentials from Vault
    - Service discovery via Consul
    - Connection pooling
    - Automatic credential rotation
    - Batch operations
    - Prepared statements
    - Async operations
    """
    
    def __init__(
        self,
        config: CassandraConfig,
        vault_client: Optional[VaultClient] = None,
        consul_client: Optional[ConsulClient] = None
    ):
        super().__init__(config, vault_client, consul_client)
        self.cassandra_config = config
        self._cluster: Optional[Cluster] = None
        self._session: Optional[Session] = None
        self._prepared_statements: Dict[str, Any] = {}
        self._reconnect_task: Optional[asyncio.Task] = None
        
    async def connect(self):
        """Connect to Cassandra cluster with dynamic credentials"""
        # Initialize base client (starts credential renewal)
        await super().connect()
        
        try:
            await self._connect_to_cassandra()
            
            # Start reconnection task for credential rotation
            self._reconnect_task = asyncio.create_task(self._reconnect_loop())
            
            logger.info(f"Connected to Cassandra cluster")
            
        except Exception as e:
            logger.error(f"Failed to connect to Cassandra: {e}")
            await self.close()
            raise
            
    async def _connect_to_cassandra(self):
        """Establish connection to Cassandra"""
        # Get hosts from service discovery
        hosts = await self._get_cassandra_hosts()
        
        # Get credentials from Vault
        auth_provider = None
        if self.config.use_vault_credentials:
            creds = await self._get_credentials()
            if creds:
                auth_provider = PlainTextAuthProvider(
                    username=creds.get('username'),
                    password=creds.get('password')
                )
                logger.info(f"Using dynamic credentials for Cassandra")
        
        # Create cluster
        self._cluster = Cluster(
            contact_points=hosts,
            port=self.cassandra_config.port,
            auth_provider=auth_provider,
            protocol_version=self.cassandra_config.protocol_version,
            connect_timeout=self.cassandra_config.connection_timeout,
            control_connection_timeout=self.cassandra_config.connection_timeout,
            load_balancing_policy=DCAwareRoundRobinPolicy(),
            default_retry_policy=RetryPolicy()
        )
        
        # Connect
        self._session = self._cluster.connect()
        
        # Set keyspace if provided
        if self.cassandra_config.keyspace:
            self._session.set_keyspace(self.cassandra_config.keyspace)
            
        # Clear prepared statements cache (they're invalid with new connection)
        self._prepared_statements.clear()
        
    async def _get_cassandra_hosts(self) -> List[str]:
        """Get Cassandra hosts from Consul or config"""
        if self.config.use_service_discovery and self._service_instances:
            return [instance['address'] for instance in self._service_instances]
        elif self.config.base_url:
            # Parse host from base URL
            from urllib.parse import urlparse
            parsed = urlparse(self.config.base_url)
            return [parsed.hostname or 'localhost']
        else:
            return ['localhost']
            
    async def _reconnect_loop(self):
        """Reconnect when credentials are about to expire"""
        while True:
            try:
                # Wait until credentials are about to expire
                if self._credentials_expiry:
                    time_until_expiry = (self._credentials_expiry - datetime.utcnow()).total_seconds()
                    # Reconnect 5 minutes before expiry
                    wait_time = max(time_until_expiry - 300, 60)
                else:
                    wait_time = 3600  # Check every hour if no expiry set
                    
                await asyncio.sleep(wait_time)
                
                # Reconnect with new credentials
                logger.info("Reconnecting Cassandra with refreshed credentials")
                
                # Close old connection
                if self._session:
                    self._session.shutdown()
                if self._cluster:
                    self._cluster.shutdown()
                    
                # Reconnect
                await self._connect_to_cassandra()
                
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Reconnection failed: {e}")
                await asyncio.sleep(60)
                
    async def close(self):
        """Close Cassandra connection"""
        # Cancel reconnection task
        if self._reconnect_task:
            self._reconnect_task.cancel()
            try:
                await self._reconnect_task
            except asyncio.CancelledError:
                pass
                
        # Close Cassandra connection
        if self._session:
            self._session.shutdown()
        if self._cluster:
            self._cluster.shutdown()
            
        # Close base client
        await super().close()
        
        logger.info("Disconnected from Cassandra")
        
    async def execute(
        self,
        query: str,
        parameters: Optional[Tuple] = None,
        consistency_level: Optional[ConsistencyLevel] = None,
        timeout: Optional[float] = None
    ) -> List[Dict[str, Any]]:
        """Execute CQL query"""
        if not self._session:
            raise RuntimeError("Not connected to Cassandra")
            
        try:
            # Create statement
            if consistency_level is None:
                consistency_level = self.cassandra_config.consistency_level
                
            statement = SimpleStatement(
                query,
                consistency_level=consistency_level
            )
            
            # Execute
            if timeout is None:
                timeout = self.cassandra_config.request_timeout
                
            result = await asyncio.get_event_loop().run_in_executor(
                None,
                lambda: self._session.execute(
                    statement,
                    parameters,
                    timeout=timeout
                )
            )
            
            # Convert to list of dicts
            return [dict(row._asdict()) for row in result]
            
        except Exception as e:
            logger.error(f"Query execution failed: {e}")
            raise
            
    async def execute_async(
        self,
        query: str,
        parameters: Optional[Tuple] = None,
        consistency_level: Optional[ConsistencyLevel] = None
    ) -> List[Dict[str, Any]]:
        """Execute query asynchronously"""
        if not self._session:
            raise RuntimeError("Not connected to Cassandra")
            
        try:
            # Create statement
            if consistency_level is None:
                consistency_level = self.cassandra_config.consistency_level
                
            statement = SimpleStatement(
                query,
                consistency_level=consistency_level
            )
            
            # Execute async
            future = self._session.execute_async(statement, parameters)
            
            # Wait for result
            result = await asyncio.get_event_loop().run_in_executor(
                None,
                future.result
            )
            
            return [dict(row._asdict()) for row in result]
            
        except Exception as e:
            logger.error(f"Async query execution failed: {e}")
            raise
            
    async def execute_batch(
        self,
        statements: List[Tuple[str, Optional[Tuple]]],
        consistency_level: Optional[ConsistencyLevel] = None,
        batch_type: BatchStatement = BatchStatement.LOGGED
    ):
        """Execute batch of statements"""
        if not self._session:
            raise RuntimeError("Not connected to Cassandra")
            
        try:
            batch = BatchStatement(
                consistency_level=consistency_level or self.cassandra_config.consistency_level,
                batch_type=batch_type
            )
            
            for query, params in statements:
                batch.add(query, params)
                
            await asyncio.get_event_loop().run_in_executor(
                None,
                lambda: self._session.execute(batch)
            )
            
        except Exception as e:
            logger.error(f"Batch execution failed: {e}")
            raise
            
    def prepare(self, query: str) -> str:
        """Prepare statement for repeated execution"""
        if not self._session:
            raise RuntimeError("Not connected to Cassandra")
            
        if query not in self._prepared_statements:
            self._prepared_statements[query] = self._session.prepare(query)
            
        return query
        
    async def execute_prepared(
        self,
        query: str,
        parameters: Tuple,
        consistency_level: Optional[ConsistencyLevel] = None
    ) -> List[Dict[str, Any]]:
        """Execute prepared statement"""
        if not self._session:
            raise RuntimeError("Not connected to Cassandra")
            
        # Prepare if not already prepared
        if query not in self._prepared_statements:
            self.prepare(query)
            
        prepared = self._prepared_statements[query]
        
        if consistency_level:
            prepared.consistency_level = consistency_level
            
        result = await asyncio.get_event_loop().run_in_executor(
            None,
            lambda: self._session.execute(prepared, parameters)
        )
        
        return [dict(row._asdict()) for row in result]
        
    async def create_keyspace(
        self,
        keyspace: str,
        replication_strategy: str = "SimpleStrategy",
        replication_factor: int = 3,
        durable_writes: bool = True
    ):
        """Create keyspace"""
        query = f"""
        CREATE KEYSPACE IF NOT EXISTS {keyspace}
        WITH replication = {{
            'class': '{replication_strategy}',
            'replication_factor': {replication_factor}
        }}
        AND durable_writes = {str(durable_writes).lower()}
        """
        
        await self.execute(query)
        logger.info(f"Created keyspace: {keyspace}")
        
    async def create_table(
        self,
        table_name: str,
        columns: Dict[str, str],
        primary_key: List[str],
        clustering_order: Optional[Dict[str, str]] = None
    ):
        """Create table"""
        # Build column definitions
        column_defs = []
        for name, dtype in columns.items():
            column_defs.append(f"{name} {dtype}")
            
        # Build primary key
        if len(primary_key) == 1:
            pk_def = f"PRIMARY KEY ({primary_key[0]})"
        else:
            partition_key = primary_key[0]
            clustering_keys = ", ".join(primary_key[1:])
            pk_def = f"PRIMARY KEY ({partition_key}, {clustering_keys})"
            
        # Build query
        query = f"""
        CREATE TABLE IF NOT EXISTS {table_name} (
            {', '.join(column_defs)},
            {pk_def}
        )
        """
        
        # Add clustering order if specified
        if clustering_order:
            order_clauses = []
            for col, order in clustering_order.items():
                order_clauses.append(f"{col} {order}")
            query += f" WITH CLUSTERING ORDER BY ({', '.join(order_clauses)})"
            
        await self.execute(query)
        logger.info(f"Created table: {table_name}")
        
    # Helper methods for common operations
    async def insert(
        self,
        table: str,
        data: Dict[str, Any],
        ttl: Optional[int] = None,
        if_not_exists: bool = False
    ):
        """Insert data into table"""
        columns = list(data.keys())
        values = list(data.values())
        placeholders = ["?" for _ in columns]
        
        query = f"INSERT INTO {table} ({', '.join(columns)}) VALUES ({', '.join(placeholders)})"
        
        if if_not_exists:
            query += " IF NOT EXISTS"
            
        if ttl:
            query += f" USING TTL {ttl}"
            
        await self.execute(query, tuple(values))
        
    async def update(
        self,
        table: str,
        data: Dict[str, Any],
        where: Dict[str, Any],
        if_exists: bool = False
    ):
        """Update data in table"""
        # Build SET clause
        set_columns = []
        set_values = []
        for col, val in data.items():
            set_columns.append(f"{col} = ?")
            set_values.append(val)
            
        # Build WHERE clause
        where_columns = []
        where_values = []
        for col, val in where.items():
            where_columns.append(f"{col} = ?")
            where_values.append(val)
            
        query = f"""
        UPDATE {table}
        SET {', '.join(set_columns)}
        WHERE {' AND '.join(where_columns)}
        """
        
        if if_exists:
            query += " IF EXISTS"
            
        await self.execute(query, tuple(set_values + where_values))
        
    async def delete(
        self,
        table: str,
        where: Dict[str, Any],
        if_exists: bool = False
    ):
        """Delete data from table"""
        where_columns = []
        where_values = []
        for col, val in where.items():
            where_columns.append(f"{col} = ?")
            where_values.append(val)
            
        query = f"DELETE FROM {table} WHERE {' AND '.join(where_columns)}"
        
        if if_exists:
            query += " IF EXISTS"
            
        await self.execute(query, tuple(where_values))
        
    async def select(
        self,
        table: str,
        columns: Optional[List[str]] = None,
        where: Optional[Dict[str, Any]] = None,
        limit: Optional[int] = None,
        allow_filtering: bool = False
    ) -> List[Dict[str, Any]]:
        """Select data from table"""
        # Build SELECT clause
        if columns:
            select_clause = ", ".join(columns)
        else:
            select_clause = "*"
            
        query = f"SELECT {select_clause} FROM {table}"
        
        # Build WHERE clause
        parameters = []
        if where:
            where_columns = []
            for col, val in where.items():
                where_columns.append(f"{col} = ?")
                parameters.append(val)
            query += f" WHERE {' AND '.join(where_columns)}"
            
        # Add LIMIT
        if limit:
            query += f" LIMIT {limit}"
            
        # Add ALLOW FILTERING if needed
        if allow_filtering:
            query += " ALLOW FILTERING"
            
        return await self.execute(query, tuple(parameters) if parameters else None)
        
    async def count(
        self,
        table: str,
        where: Optional[Dict[str, Any]] = None
    ) -> int:
        """Count rows in table"""
        query = f"SELECT COUNT(*) FROM {table}"
        
        parameters = []
        if where:
            where_columns = []
            for col, val in where.items():
                where_columns.append(f"{col} = ?")
                parameters.append(val)
            query += f" WHERE {' AND '.join(where_columns)}"
            
        result = await self.execute(query, tuple(parameters) if parameters else None)
        return result[0]['count'] if result else 0
        
    def get_metrics(self) -> Dict[str, Any]:
        """Get Cassandra client metrics"""
        metrics = {
            "prepared_statements": len(self._prepared_statements),
            "connected": self._session is not None
        }
        
        if self._cluster:
            # Add cluster metrics
            metrics.update({
                "known_hosts": len(self._cluster.metadata.all_hosts()),
                "open_connections": sum(
                    pool.open_count 
                    for pool in self._session._pools.values()
                ) if self._session else 0
            })
            
        return metrics
        
    async def get_client_specific_config(self) -> Dict[str, Any]:
        """Get Cassandra-specific configuration from Consul"""
        if not self.consul_client:
            return {}
            
        try:
            # Get Cassandra-specific config
            config = await self.consul_client.get_config(
                f"data-intelligence/cassandra/config"
            )
            
            return config or {}
            
        except Exception as e:
            logger.error(f"Failed to get Cassandra config from Consul: {e}")
            return {}
            
    @asynccontextmanager
    async def transaction(self):
        """Context manager for batch operations (Cassandra doesn't have real transactions)"""
        batch = BatchStatement(
            consistency_level=self.cassandra_config.consistency_level
        )
        
        try:
            yield batch
            
            # Execute batch
            await asyncio.get_event_loop().run_in_executor(
                None,
                lambda: self._session.execute(batch)
            )
            
        except Exception as e:
            logger.error(f"Batch operation failed: {e}")
            raise 