"""
Abstract data source management for integration patterns.

Provides connection pooling, credential management, and health monitoring.
"""

from typing import Dict, Any, Optional, List, AsyncContextManager, Protocol
from dataclasses import dataclass, field
from abc import ABC, abstractmethod
from contextlib import asynccontextmanager
from datetime import datetime, timedelta
import asyncio
from enum import Enum

from ...monitoring import StructuredLogger
from ...vault_consul import VaultConsulIntegration
from ..events import EventBus, Event

logger = StructuredLogger.get_logger(__name__)


class ConnectionState(str, Enum):
    """Connection states"""
    DISCONNECTED = "disconnected"
    CONNECTING = "connecting"
    CONNECTED = "connected"
    ERROR = "error"
    RECONNECTING = "reconnecting"


@dataclass
class ConnectionHealth:
    """Connection health information"""
    state: ConnectionState = ConnectionState.DISCONNECTED
    last_check: Optional[datetime] = None
    last_error: Optional[str] = None
    consecutive_failures: int = 0
    latency_ms: Optional[float] = None
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary"""
        return {
            "state": self.state.value,
            "last_check": self.last_check.isoformat() if self.last_check else None,
            "last_error": self.last_error,
            "consecutive_failures": self.consecutive_failures,
            "latency_ms": self.latency_ms
        }


@dataclass
class ConnectionPool:
    """Connection pool configuration"""
    min_size: int = 1
    max_size: int = 10
    acquire_timeout: float = 10.0
    idle_timeout: Optional[float] = 3600.0  # 1 hour
    max_queries: Optional[int] = 50000
    
    # Health check
    health_check_interval: float = 30.0
    health_check_timeout: float = 5.0
    max_consecutive_failures: int = 3


class DataSourceConnection(Protocol):
    """Protocol for data source connections"""
    
    async def execute(self, query: str, params: Optional[Dict[str, Any]] = None) -> Any:
        """Execute query on connection"""
        ...
        
    async def fetch(self, query: str, params: Optional[Dict[str, Any]] = None) -> List[Dict[str, Any]]:
        """Fetch results from query"""
        ...
        
    async def close(self) -> None:
        """Close connection"""
        ...


class BaseDataSourceManager(ABC):
    """
    Abstract base class for data source management.
    
    Features:
    - Connection pooling
    - Dynamic credential management
    - Health monitoring
    - Automatic reconnection
    - Event publishing
    """
    
    def __init__(
        self,
        vault_consul: Optional[VaultConsulIntegration] = None,
        event_bus: Optional[EventBus] = None
    ):
        self.vault_consul = vault_consul
        self.event_bus = event_bus
        
        # Storage
        self._connections: Dict[str, Any] = {}
        self._pools: Dict[str, Any] = {}
        self._health: Dict[str, ConnectionHealth] = {}
        self._configs: Dict[str, Any] = {}
        
        # Tasks
        self._health_check_tasks: Dict[str, asyncio.Task] = {}
        self._credential_refresh_task: Optional[asyncio.Task] = None
        
    async def initialize(self):
        """Initialize data source manager"""
        logger.info("Initializing data source manager")
        
        # Start credential refresh if Vault configured
        if self.vault_consul:
            self._credential_refresh_task = asyncio.create_task(
                self._credential_refresh_loop()
            )
            
        await self._initialize_impl()
        
        logger.info("Data source manager initialized")
        
    async def shutdown(self):
        """Shutdown data source manager"""
        logger.info("Shutting down data source manager")
        
        # Cancel tasks
        if self._credential_refresh_task:
            self._credential_refresh_task.cancel()
            
        for task in self._health_check_tasks.values():
            task.cancel()
            
        # Close all connections
        for name in list(self._connections.keys()):
            await self.disconnect(name)
            
        await self._shutdown_impl()
        
        logger.info("Data source manager shutdown complete")
        
    @abstractmethod
    async def _initialize_impl(self):
        """Initialize implementation-specific components"""
        pass
        
    @abstractmethod
    async def _shutdown_impl(self):
        """Shutdown implementation-specific components"""
        pass
        
    async def register_data_source(
        self,
        name: str,
        source_type: str,
        connection_params: Dict[str, Any],
        pool_config: Optional[ConnectionPool] = None,
        vault_role: Optional[str] = None
    ):
        """
        Register a data source.
        
        Args:
            name: Unique data source name
            source_type: Type of data source
            connection_params: Connection parameters
            pool_config: Optional pool configuration
            vault_role: Optional Vault role for credentials
        """
        logger.info(f"Registering data source: {name} (type: {source_type})")
        
        # Store configuration
        self._configs[name] = {
            "source_type": source_type,
            "connection_params": connection_params,
            "pool_config": pool_config or ConnectionPool(),
            "vault_role": vault_role
        }
        
        # Initialize health tracking
        self._health[name] = ConnectionHealth()
        
        # Create connection
        await self._create_connection(name)
        
        # Start health monitoring
        if pool_config and pool_config.health_check_interval > 0:
            task = asyncio.create_task(
                self._health_check_loop(name)
            )
            self._health_check_tasks[name] = task
            
        # Publish event
        if self.event_bus:
            await self.event_bus.publish(Event(
                type="datasource.registered",
                source="data_source_manager",
                data={
                    "name": name,
                    "source_type": source_type
                }
            ))
            
        logger.info(f"Data source registered: {name}")
        
    async def _create_connection(self, name: str):
        """Create connection for data source"""
        config = self._configs[name]
        
        # Get credentials from Vault if configured
        connection_params = config["connection_params"].copy()
        
        if self.vault_consul and config.get("vault_role"):
            try:
                creds = await self.vault_consul.get_database_credentials(
                    config["source_type"],
                    config["vault_role"]
                )
                connection_params.update(creds)
            except Exception as e:
                logger.error(f"Failed to get credentials from Vault: {e}")
                
        # Update health
        self._health[name].state = ConnectionState.CONNECTING
        
        try:
            # Create connection/pool
            connection = await self._create_connection_impl(
                name,
                config["source_type"],
                connection_params,
                config["pool_config"]
            )
            
            self._connections[name] = connection
            self._health[name].state = ConnectionState.CONNECTED
            self._health[name].consecutive_failures = 0
            
        except Exception as e:
            logger.error(f"Failed to create connection for {name}: {e}")
            self._health[name].state = ConnectionState.ERROR
            self._health[name].last_error = str(e)
            raise
            
    @abstractmethod
    async def _create_connection_impl(
        self,
        name: str,
        source_type: str,
        connection_params: Dict[str, Any],
        pool_config: ConnectionPool
    ) -> Any:
        """Create implementation-specific connection"""
        pass
        
    @asynccontextmanager
    async def get_connection(self, name: str) -> AsyncContextManager[DataSourceConnection]:
        """
        Get connection from pool.
        
        Args:
            name: Data source name
            
        Yields:
            Connection object
        """
        if name not in self._connections:
            raise ValueError(f"Unknown data source: {name}")
            
        health = self._health[name]
        
        # Check if reconnection needed
        if health.state == ConnectionState.ERROR:
            if health.consecutive_failures < self._configs[name]["pool_config"].max_consecutive_failures:
                await self._reconnect(name)
            else:
                raise ConnectionError(f"Data source {name} is unavailable")
                
        # Get connection from pool
        conn = await self._acquire_connection(name)
        
        try:
            yield conn
        finally:
            await self._release_connection(name, conn)
            
    @abstractmethod
    async def _acquire_connection(self, name: str) -> DataSourceConnection:
        """Acquire connection from pool"""
        pass
        
    @abstractmethod
    async def _release_connection(self, name: str, conn: DataSourceConnection):
        """Release connection back to pool"""
        pass
        
    async def execute_query(
        self,
        source_name: str,
        query: str,
        params: Optional[Dict[str, Any]] = None,
        timeout: Optional[float] = None
    ) -> List[Dict[str, Any]]:
        """
        Execute query on data source.
        
        Args:
            source_name: Data source name
            query: Query to execute
            params: Query parameters
            timeout: Query timeout
            
        Returns:
            Query results
        """
        start_time = datetime.utcnow()
        
        try:
            async with self.get_connection(source_name) as conn:
                # Execute with timeout
                if timeout:
                    results = await asyncio.wait_for(
                        conn.fetch(query, params),
                        timeout=timeout
                    )
                else:
                    results = await conn.fetch(query, params)
                    
            # Update latency
            latency = (datetime.utcnow() - start_time).total_seconds() * 1000
            self._health[source_name].latency_ms = latency
            
            # Publish event
            if self.event_bus:
                await self.event_bus.publish(Event(
                    type="datasource.query.success",
                    source="data_source_manager",
                    data={
                        "source": source_name,
                        "query_hash": hash(query),
                        "latency_ms": latency,
                        "row_count": len(results)
                    }
                ))
                
            return results
            
        except Exception as e:
            logger.error(f"Query failed on {source_name}: {e}")
            
            # Update health
            self._health[source_name].last_error = str(e)
            
            # Publish event
            if self.event_bus:
                await self.event_bus.publish(Event(
                    type="datasource.query.error",
                    source="data_source_manager",
                    data={
                        "source": source_name,
                        "error": str(e)
                    }
                ))
                
            raise
            
    async def disconnect(self, name: str):
        """
        Disconnect from data source.
        
        Args:
            name: Data source name
        """
        if name not in self._connections:
            return
            
        logger.info(f"Disconnecting from data source: {name}")
        
        # Cancel health check
        if name in self._health_check_tasks:
            self._health_check_tasks[name].cancel()
            del self._health_check_tasks[name]
            
        # Close connection
        try:
            await self._close_connection_impl(name)
        except Exception as e:
            logger.error(f"Error closing connection for {name}: {e}")
            
        # Clean up
        del self._connections[name]
        self._health[name].state = ConnectionState.DISCONNECTED
        
        logger.info(f"Disconnected from data source: {name}")
        
    @abstractmethod
    async def _close_connection_impl(self, name: str):
        """Close implementation-specific connection"""
        pass
        
    async def _reconnect(self, name: str):
        """Reconnect to data source"""
        logger.info(f"Reconnecting to data source: {name}")
        
        self._health[name].state = ConnectionState.RECONNECTING
        
        try:
            # Close existing connection
            if name in self._connections:
                await self._close_connection_impl(name)
                
            # Create new connection
            await self._create_connection(name)
            
            logger.info(f"Reconnected to data source: {name}")
            
        except Exception as e:
            logger.error(f"Reconnection failed for {name}: {e}")
            self._health[name].state = ConnectionState.ERROR
            self._health[name].consecutive_failures += 1
            raise
            
    async def _health_check_loop(self, name: str):
        """Health check loop for data source"""
        config = self._configs[name]["pool_config"]
        
        while True:
            try:
                await asyncio.sleep(config.health_check_interval)
                
                # Perform health check
                health = await self._check_health(name)
                self._health[name] = health
                
                # Reconnect if needed
                if health.state == ConnectionState.ERROR:
                    if health.consecutive_failures < config.max_consecutive_failures:
                        await self._reconnect(name)
                        
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Error in health check for {name}: {e}")
                
    async def _check_health(self, name: str) -> ConnectionHealth:
        """Check health of data source"""
        health = self._health[name]
        health.last_check = datetime.utcnow()
        
        try:
            # Perform health check query
            start_time = datetime.utcnow()
            await self._health_check_impl(name)
            
            # Update health
            health.state = ConnectionState.CONNECTED
            health.consecutive_failures = 0
            health.latency_ms = (datetime.utcnow() - start_time).total_seconds() * 1000
            
        except Exception as e:
            health.state = ConnectionState.ERROR
            health.last_error = str(e)
            health.consecutive_failures += 1
            
        return health
        
    @abstractmethod
    async def _health_check_impl(self, name: str):
        """Implementation-specific health check"""
        pass
        
    async def _credential_refresh_loop(self):
        """Refresh credentials periodically"""
        while True:
            try:
                # Wait for refresh interval (typically 1 hour)
                await asyncio.sleep(3600)
                
                # Refresh credentials for each data source with Vault role
                for name, config in self._configs.items():
                    if config.get("vault_role"):
                        try:
                            await self._refresh_credentials(name)
                        except Exception as e:
                            logger.error(f"Failed to refresh credentials for {name}: {e}")
                            
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Error in credential refresh loop: {e}")
                
    async def _refresh_credentials(self, name: str):
        """Refresh credentials for data source"""
        logger.info(f"Refreshing credentials for data source: {name}")
        
        # Reconnect with new credentials
        await self._reconnect(name)
        
    async def get_health(self, name: Optional[str] = None) -> Dict[str, Any]:
        """
        Get health status of data sources.
        
        Args:
            name: Specific data source or None for all
            
        Returns:
            Health status dictionary
        """
        if name:
            health = self._health.get(name)
            return health.to_dict() if health else {}
        else:
            return {
                name: health.to_dict()
                for name, health in self._health.items()
            } 