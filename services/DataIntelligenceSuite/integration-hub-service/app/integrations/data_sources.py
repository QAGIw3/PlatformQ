"""Data source management for DIH service."""

from typing import Dict, Any, Optional, List
from dataclasses import dataclass
import asyncio
import logging

from data_intelligence_common import VaultConsulIntegration, get_logger
from platformq_shared.event_publisher import EventPublisher

logger = get_logger(__name__)


@dataclass
class DataSourceConfig:
    """Data source configuration."""
    name: str
    type: str  # postgres, cassandra, elasticsearch, mongodb, janusgraph
    connection_params: Dict[str, Any]
    vault_role: str = "readonly"
    batch_size: int = 1000
    fetch_timeout: int = 30


class DataSourceManager:
    """
    Manages connections to various data sources.
    
    Features:
    - Dynamic credential management via Vault
    - Connection pooling
    - Health monitoring
    - Event publishing
    """
    
    def __init__(
        self,
        vault_consul: VaultConsulIntegration,
        event_publisher: Optional[EventPublisher] = None
    ):
        self.vault_consul = vault_consul
        self.event_publisher = event_publisher
        self.data_sources: Dict[str, DataSourceConfig] = {}
        self._connections: Dict[str, Any] = {}
        
    async def initialize(self):
        """Initialize data source manager."""
        # Load data source configurations
        await self._load_configurations()
        
        logger.info("Data source manager initialized")
        
    async def cleanup(self):
        """Cleanup data source connections."""
        # Close all connections
        for name in list(self._connections.keys()):
            await self.disconnect(name)
            
        logger.info("Data source manager cleaned up")
        
    async def _load_configurations(self):
        """Load data source configurations from Consul."""
        # Default configurations
        self.data_sources = {
            "postgres": DataSourceConfig(
                name="postgres",
                type="postgres",
                connection_params={
                    "host": "postgres",
                    "port": 5432,
                    "database": "platformq"
                },
                vault_role="readonly"
            ),
            "cassandra": DataSourceConfig(
                name="cassandra",
                type="cassandra",
                connection_params={
                    "contact_points": ["cassandra"],
                    "port": 9042
                },
                vault_role="reader"
            ),
            "elasticsearch": DataSourceConfig(
                name="elasticsearch",
                type="elasticsearch",
                connection_params={
                    "hosts": ["https://elasticsearch:9200"]
                },
                vault_role="search"
            )
        }
        
    async def get_connection(self, source_name: str) -> Any:
        """Get connection to data source."""
        if source_name in self._connections:
            return self._connections[source_name]
            
        config = self.data_sources.get(source_name)
        if not config:
            raise ValueError(f"Unknown data source: {source_name}")
            
        # Create connection with dynamic credentials
        async with self.vault_consul.get_database_connection(
            config.type,
            config.vault_role
        ) as conn:
            self._connections[source_name] = conn
            return conn
            
    async def disconnect(self, source_name: str):
        """Disconnect from data source."""
        if source_name in self._connections:
            # Connection cleanup handled by context manager
            del self._connections[source_name]
            logger.info(f"Disconnected from {source_name}")
            
    async def execute_query(
        self,
        source_name: str,
        query: str,
        params: Optional[Dict[str, Any]] = None
    ) -> List[Dict[str, Any]]:
        """Execute query on data source."""
        conn = await self.get_connection(source_name)
        config = self.data_sources[source_name]
        
        try:
            # Execute based on source type
            if config.type == "postgres":
                # PostgreSQL query
                result = await conn.fetch(query, **(params or {}))
                return [dict(row) for row in result]
                
            elif config.type == "cassandra":
                # Cassandra query
                result = conn.execute(query, params or {})
                return [dict(row._asdict()) for row in result]
                
            elif config.type == "elasticsearch":
                # Elasticsearch query
                result = await conn.search(body=query)
                return result["hits"]["hits"]
                
            else:
                raise ValueError(f"Unsupported source type: {config.type}")
                
        except Exception as e:
            logger.error(f"Error executing query on {source_name}: {e}")
            
            # Publish error event
            if self.event_publisher:
                await self.event_publisher.publish(
                    "dih.query.error",
                    {
                        "source": source_name,
                        "error": str(e),
                        "query": query[:100]  # Truncate for safety
                    }
                )
                
            raise
            
    async def get_source_health(self, source_name: str) -> Dict[str, Any]:
        """Check health of a data source."""
        try:
            conn = await self.get_connection(source_name)
            config = self.data_sources[source_name]
            
            # Simple connectivity check
            if config.type == "postgres":
                await conn.fetchval("SELECT 1")
            elif config.type == "cassandra":
                conn.execute("SELECT now() FROM system.local")
            elif config.type == "elasticsearch":
                await conn.info()
                
            return {
                "status": "healthy",
                "source": source_name,
                "type": config.type
            }
            
        except Exception as e:
            return {
                "status": "unhealthy",
                "source": source_name,
                "error": str(e)
            } 