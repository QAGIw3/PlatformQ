"""
Connection pool management for blockchain adapters.
Provides efficient connection reuse and management.
"""

import asyncio
import logging
from typing import Dict, Optional, Any, List
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from contextlib import asynccontextmanager

from .types import ChainType, ChainConfig
from .interfaces import IBlockchainAdapter

logger = logging.getLogger(__name__)


@dataclass
class ConnectionInfo:
    """Information about a pooled connection"""
    adapter: IBlockchainAdapter
    chain_type: ChainType
    created_at: datetime
    last_used: datetime
    use_count: int = 0
    is_healthy: bool = True
    
    
class ConnectionPool:
    """
    Manages a pool of blockchain connections for efficient reuse.
    Features:
    - Connection reuse
    - Health checking  
    - Automatic reconnection
    - Load balancing across multiple RPC endpoints
    """
    
    def __init__(self, 
                 max_connections_per_chain: int = 5,
                 connection_timeout: int = 30,
                 health_check_interval: int = 60,
                 max_idle_time: int = 300):
        self.max_connections_per_chain = max_connections_per_chain
        self.connection_timeout = connection_timeout
        self.health_check_interval = health_check_interval
        self.max_idle_time = max_idle_time
        
        self._pools: Dict[ChainType, List[ConnectionInfo]] = {}
        self._configs: Dict[ChainType, ChainConfig] = {}
        self._locks: Dict[ChainType, asyncio.Lock] = {}
        self._health_check_task: Optional[asyncio.Task] = None
        self._closed = False
        
    async def initialize(self):
        """Initialize the connection pool"""
        if not self._closed:
            self._health_check_task = asyncio.create_task(self._health_check_loop())
            logger.info("Connection pool initialized")
            
    async def close(self):
        """Close all connections and cleanup"""
        self._closed = True
        
        if self._health_check_task:
            self._health_check_task.cancel()
            try:
                await self._health_check_task
            except asyncio.CancelledError:
                pass
                
        # Close all connections
        for chain_type, connections in self._pools.items():
            for conn_info in connections:
                try:
                    await conn_info.adapter.disconnect()
                except Exception as e:
                    logger.error(f"Error closing connection for {chain_type}: {e}")
                    
        self._pools.clear()
        logger.info("Connection pool closed")
        
    def register_chain(self, config: ChainConfig):
        """Register a chain configuration"""
        self._configs[config.chain_type] = config
        self._locks[config.chain_type] = asyncio.Lock()
        self._pools[config.chain_type] = []
        logger.info(f"Registered chain {config.chain_type.value} with pool")
        
    @asynccontextmanager
    async def get_connection(self, chain_type: ChainType) -> IBlockchainAdapter:
        """
        Get a connection from the pool.
        Usage:
            async with pool.get_connection(ChainType.ETHEREUM) as adapter:
                await adapter.get_balance(address)
        """
        if chain_type not in self._configs:
            raise ValueError(f"Chain {chain_type} not registered")
            
        conn_info = await self._acquire_connection(chain_type)
        try:
            yield conn_info.adapter
        finally:
            await self._release_connection(chain_type, conn_info)
            
    async def _acquire_connection(self, chain_type: ChainType) -> ConnectionInfo:
        """Acquire a connection from the pool or create a new one"""
        async with self._locks[chain_type]:
            pool = self._pools[chain_type]
            
            # Try to find an available healthy connection
            for conn_info in pool:
                if conn_info.is_healthy:
                    conn_info.last_used = datetime.utcnow()
                    conn_info.use_count += 1
                    return conn_info
                    
            # Create new connection if under limit
            if len(pool) < self.max_connections_per_chain:
                conn_info = await self._create_connection(chain_type)
                pool.append(conn_info)
                return conn_info
                
            # Wait for a connection to become available
            # TODO In production, implement proper waiting queue
            raise RuntimeError(f"No available connections for {chain_type}")
            
    async def _release_connection(self, chain_type: ChainType, conn_info: ConnectionInfo):
        """Release a connection back to the pool"""
        # Connection is automatically available again
        # In a more complex implementation, we might mark it as available
        pass
        
    async def _create_connection(self, chain_type: ChainType) -> ConnectionInfo:
        """Create a new connection"""
        config = self._configs[chain_type]
        
        # Import adapter dynamically based on chain type
        # TODO In production, use a factory pattern
        adapter = await self._create_adapter(chain_type, config)
        
        try:
            await asyncio.wait_for(
                adapter.connect(),
                timeout=self.connection_timeout
            )
        except asyncio.TimeoutError:
            raise ConnectionError(f"Connection timeout for {chain_type}")
            
        conn_info = ConnectionInfo(
            adapter=adapter,
            chain_type=chain_type,
            created_at=datetime.utcnow(),
            last_used=datetime.utcnow()
        )
        
        logger.info(f"Created new connection for {chain_type}")
        return conn_info
        
    async def _create_adapter(self, chain_type: ChainType, config: ChainConfig) -> IBlockchainAdapter:
        """Create adapter instance based on chain type"""
        from .adapter_factory import AdapterFactory
        
        return AdapterFactory.create_adapter(config)
            
    async def _health_check_loop(self):
        """Periodic health check for all connections"""
        while not self._closed:
            try:
                await asyncio.sleep(self.health_check_interval)
                await self._check_all_connections()
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Error in health check loop: {e}")
                
    async def _check_all_connections(self):
        """Check health of all connections"""
        for chain_type, connections in self._pools.items():
            for conn_info in connections[:]:  # Copy to allow removal
                try:
                    # Check if connection is still alive
                    await asyncio.wait_for(
                        conn_info.adapter.get_block_number(),
                        timeout=10
                    )
                    
                    # Check if connection is idle too long
                    idle_time = datetime.utcnow() - conn_info.last_used
                    if idle_time.total_seconds() > self.max_idle_time:
                        await self._remove_connection(chain_type, conn_info)
                        
                except Exception as e:
                    logger.warning(f"Health check failed for {chain_type}: {e}")
                    conn_info.is_healthy = False
                    
                    # Try to reconnect
                    try:
                        await conn_info.adapter.connect()
                        conn_info.is_healthy = True
                        logger.info(f"Reconnected to {chain_type}")
                    except Exception:
                        await self._remove_connection(chain_type, conn_info)
                        
    async def _remove_connection(self, chain_type: ChainType, conn_info: ConnectionInfo):
        """Remove a connection from the pool"""
        try:
            await conn_info.adapter.disconnect()
        except Exception as e:
            logger.error(f"Error disconnecting from {chain_type}: {e}")
            
        async with self._locks[chain_type]:
            if conn_info in self._pools[chain_type]:
                self._pools[chain_type].remove(conn_info)
                logger.info(f"Removed connection for {chain_type}")
                
    def get_pool_stats(self) -> Dict[str, Any]:
        """Get statistics about the connection pool"""
        stats = {}
        for chain_type, connections in self._pools.items():
            stats[chain_type.value] = {
                "total": len(connections),
                "healthy": sum(1 for c in connections if c.is_healthy),
                "total_uses": sum(c.use_count for c in connections)
            }
        return stats 