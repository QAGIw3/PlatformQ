"""
Database Connection Pooling

High-performance connection pooling for multiple databases
"""

from typing import Dict, List, Optional, Any, Callable
from contextlib import asynccontextmanager
import asyncio
import logging
from datetime import datetime, timedelta
from dataclasses import dataclass, field
from cassandra.cluster import Cluster, Session
from cassandra.policies import DCAwareRoundRobinPolicy, TokenAwarePolicy
from cassandra.auth import PlainTextAuthProvider
from cassandra.concurrent import execute_concurrent_with_args
import asyncpg
import motor.motor_asyncio
from aiocassandra import aiosession

logger = logging.getLogger(__name__)


@dataclass
class PoolConfig:
    """Configuration for connection pool"""
    min_size: int = 5
    max_size: int = 20
    max_idle_time: int = 300  # seconds
    connection_timeout: int = 10
    command_timeout: int = 30
    max_queries_per_connection: int = 50000
    retry_attempts: int = 3
    retry_delay: float = 0.1


@dataclass 
class ConnectionStats:
    """Connection pool statistics"""
    total_connections: int = 0
    active_connections: int = 0
    idle_connections: int = 0
    total_queries: int = 0
    failed_queries: int = 0
    avg_query_time: float = 0.0
    connection_errors: int = 0
    last_error: Optional[str] = None
    created_at: datetime = field(default_factory=datetime.utcnow)


class CassandraConnectionPool:
    """Optimized Cassandra connection pool"""
    
    def __init__(self, hosts: List[str], keyspace: str, 
                 config: PoolConfig = None):
        self.hosts = hosts
        self.keyspace = keyspace
        self.config = config or PoolConfig()
        
        self.cluster: Optional[Cluster] = None
        self.session_pool: List[Session] = []
        self.async_session: Optional[aiosession] = None
        
        self.stats = ConnectionStats()
        self._lock = asyncio.Lock()
        self._prepared_statements: Dict[str, Any] = {}
        
    async def initialize(self):
        """Initialize connection pool"""
        
        # Configure cluster
        auth_provider = PlainTextAuthProvider(
            username='cassandra',
            password='cassandra'
        )
        
        load_balancing = TokenAwarePolicy(
            DCAwareRoundRobinPolicy(local_dc='datacenter1')
        )
        
        self.cluster = Cluster(
            contact_points=self.hosts,
            auth_provider=auth_provider,
            load_balancing_policy=load_balancing,
            protocol_version=4,
            connect_timeout=self.config.connection_timeout,
            control_connection_timeout=self.config.connection_timeout,
            executor_threads=4,
            max_schema_agreement_wait=10
        )
        
        # Create session pool
        for _ in range(self.config.min_size):
            session = self.cluster.connect(self.keyspace)
            session.default_timeout = self.config.command_timeout
            self.session_pool.append(session)
            
        # Create async session wrapper
        self.async_session = aiosession(self.session_pool[0])
        
        self.stats.total_connections = len(self.session_pool)
        self.stats.idle_connections = len(self.session_pool)
        
        logger.info(f"Cassandra pool initialized with {len(self.session_pool)} connections")
        
    @asynccontextmanager
    async def acquire(self):
        """Acquire connection from pool"""
        async with self._lock:
            if not self.session_pool:
                # Create new connection if needed
                if self.stats.total_connections < self.config.max_size:
                    session = self.cluster.connect(self.keyspace)
                    self.session_pool.append(session)
                    self.stats.total_connections += 1
                else:
                    # Wait for available connection
                    while not self.session_pool:
                        await asyncio.sleep(0.01)
                        
            session = self.session_pool.pop()
            self.stats.idle_connections -= 1
            self.stats.active_connections += 1
            
        try:
            yield session
        finally:
            async with self._lock:
                self.session_pool.append(session)
                self.stats.idle_connections += 1
                self.stats.active_connections -= 1
                
    async def execute(self, query: str, params: Optional[tuple] = None,
                     consistency_level: Any = None) -> Any:
        """Execute query with connection from pool"""
        
        start_time = asyncio.get_event_loop().time()
        
        async with self.acquire() as session:
            try:
                # Prepare statement if not cached
                if query not in self._prepared_statements:
                    self._prepared_statements[query] = session.prepare(query)
                    
                prepared = self._prepared_statements[query]
                
                if consistency_level:
                    prepared.consistency_level = consistency_level
                    
                # Execute async
                result = await self.async_session.execute_future(
                    session.execute_async(prepared, params or [])
                )
                
                # Update stats
                elapsed = asyncio.get_event_loop().time() - start_time
                self._update_stats(elapsed, success=True)
                
                return result
                
            except Exception as e:
                self._update_stats(0, success=False, error=str(e))
                raise
                
    async def execute_batch(self, queries: List[tuple]) -> List[Any]:
        """Execute batch of queries efficiently"""
        
        async with self.acquire() as session:
            # Use concurrent execution
            results = execute_concurrent_with_args(
                session, 
                queries[0][0],  # Statement
                [q[1] for q in queries],  # Parameters
                concurrency=100
            )
            
            return [r[1] for r in results]
            
    def _update_stats(self, query_time: float, success: bool, error: str = None):
        """Update connection statistics"""
        
        self.stats.total_queries += 1
        
        if success:
            # Update average query time
            current_avg = self.stats.avg_query_time
            self.stats.avg_query_time = (
                (current_avg * (self.stats.total_queries - 1) + query_time) /
                self.stats.total_queries
            )
        else:
            self.stats.failed_queries += 1
            self.stats.last_error = error
            
    async def close(self):
        """Close all connections"""
        for session in self.session_pool:
            session.shutdown()
            
        if self.cluster:
            self.cluster.shutdown()
            
        logger.info("Cassandra pool closed")


class PostgresConnectionPool:
    """PostgreSQL connection pool using asyncpg"""
    
    def __init__(self, dsn: str, config: PoolConfig = None):
        self.dsn = dsn
        self.config = config or PoolConfig()
        self.pool: Optional[asyncpg.Pool] = None
        self.stats = ConnectionStats()
        
    async def initialize(self):
        """Initialize connection pool"""
        
        self.pool = await asyncpg.create_pool(
            self.dsn,
            min_size=self.config.min_size,
            max_size=self.config.max_size,
            max_inactive_connection_lifetime=self.config.max_idle_time,
            command_timeout=self.config.command_timeout,
            max_queries=self.config.max_queries_per_connection
        )
        
        self.stats.total_connections = self.pool.get_size()
        
        logger.info(f"PostgreSQL pool initialized with {self.pool.get_size()} connections")
        
    async def execute(self, query: str, *args, timeout: float = None) -> Any:
        """Execute query"""
        
        start_time = asyncio.get_event_loop().time()
        
        try:
            async with self.pool.acquire() as conn:
                result = await conn.fetch(query, *args, timeout=timeout)
                
            elapsed = asyncio.get_event_loop().time() - start_time
            self._update_stats(elapsed, success=True)
            
            return result
            
        except Exception as e:
            self._update_stats(0, success=False, error=str(e))
            raise
            
    async def execute_many(self, query: str, args: List[tuple]) -> None:
        """Execute query with multiple parameter sets"""
        
        async with self.pool.acquire() as conn:
            await conn.executemany(query, args)
            
    @asynccontextmanager
    async def transaction(self):
        """Execute in transaction"""
        async with self.pool.acquire() as conn:
            async with conn.transaction():
                yield conn
                
    def _update_stats(self, query_time: float, success: bool, error: str = None):
        """Update statistics"""
        self.stats.total_queries += 1
        
        if success:
            current_avg = self.stats.avg_query_time
            self.stats.avg_query_time = (
                (current_avg * (self.stats.total_queries - 1) + query_time) /
                self.stats.total_queries
            )
        else:
            self.stats.failed_queries += 1
            self.stats.last_error = error
            
    async def close(self):
        """Close pool"""
        await self.pool.close()
        logger.info("PostgreSQL pool closed")


class MongoConnectionPool:
    """MongoDB connection pool using motor"""
    
    def __init__(self, uri: str, database: str, config: PoolConfig = None):
        self.uri = uri
        self.database = database
        self.config = config or PoolConfig()
        self.client: Optional[motor.motor_asyncio.AsyncIOMotorClient] = None
        self.db: Optional[motor.motor_asyncio.AsyncIOMotorDatabase] = None
        self.stats = ConnectionStats()
        
    async def initialize(self):
        """Initialize connection pool"""
        
        self.client = motor.motor_asyncio.AsyncIOMotorClient(
            self.uri,
            maxPoolSize=self.config.max_size,
            minPoolSize=self.config.min_size,
            maxIdleTimeMS=self.config.max_idle_time * 1000,
            connectTimeoutMS=self.config.connection_timeout * 1000,
            serverSelectionTimeoutMS=self.config.connection_timeout * 1000
        )
        
        self.db = self.client[self.database]
        
        # Test connection
        await self.db.command("ping")
        
        logger.info(f"MongoDB pool initialized for database {self.database}")
        
    async def find(self, collection: str, filter: Dict, 
                  projection: Optional[Dict] = None, 
                  limit: Optional[int] = None) -> List[Dict]:
        """Find documents"""
        
        start_time = asyncio.get_event_loop().time()
        
        try:
            cursor = self.db[collection].find(filter, projection)
            
            if limit:
                cursor = cursor.limit(limit)
                
            results = await cursor.to_list(length=limit)
            
            elapsed = asyncio.get_event_loop().time() - start_time
            self._update_stats(elapsed, success=True)
            
            return results
            
        except Exception as e:
            self._update_stats(0, success=False, error=str(e))
            raise
            
    async def insert_many(self, collection: str, documents: List[Dict]) -> List[str]:
        """Insert multiple documents"""
        
        result = await self.db[collection].insert_many(documents)
        return result.inserted_ids
        
    async def update_many(self, collection: str, filter: Dict, 
                         update: Dict) -> int:
        """Update multiple documents"""
        
        result = await self.db[collection].update_many(filter, update)
        return result.modified_count
        
    async def bulk_write(self, collection: str, operations: List[Any]) -> Dict:
        """Execute bulk operations"""
        
        result = await self.db[collection].bulk_write(operations)
        return {
            "inserted": result.inserted_count,
            "updated": result.modified_count,
            "deleted": result.deleted_count
        }
        
    def _update_stats(self, query_time: float, success: bool, error: str = None):
        """Update statistics"""
        self.stats.total_queries += 1
        
        if success:
            current_avg = self.stats.avg_query_time
            self.stats.avg_query_time = (
                (current_avg * (self.stats.total_queries - 1) + query_time) /
                self.stats.total_queries
            )
        else:
            self.stats.failed_queries += 1
            self.stats.last_error = error
            
    async def close(self):
        """Close client"""
        self.client.close()
        logger.info("MongoDB pool closed")


class UnifiedDatabasePool:
    """Unified interface for all database pools"""
    
    def __init__(self):
        self.cassandra: Optional[CassandraConnectionPool] = None
        self.postgres: Optional[PostgresConnectionPool] = None
        self.mongo: Optional[MongoConnectionPool] = None
        
    async def initialize(self, config: Dict[str, Any]):
        """Initialize all configured pools"""
        
        # Initialize Cassandra
        if "cassandra" in config:
            self.cassandra = CassandraConnectionPool(
                hosts=config["cassandra"]["hosts"],
                keyspace=config["cassandra"]["keyspace"],
                config=PoolConfig(**config["cassandra"].get("pool", {}))
            )
            await self.cassandra.initialize()
            
        # Initialize PostgreSQL
        if "postgres" in config:
            self.postgres = PostgresConnectionPool(
                dsn=config["postgres"]["dsn"],
                config=PoolConfig(**config["postgres"].get("pool", {}))
            )
            await self.postgres.initialize()
            
        # Initialize MongoDB
        if "mongo" in config:
            self.mongo = MongoConnectionPool(
                uri=config["mongo"]["uri"],
                database=config["mongo"]["database"],
                config=PoolConfig(**config["mongo"].get("pool", {}))
            )
            await self.mongo.initialize()
            
        logger.info("Unified database pool initialized")
        
    def get_stats(self) -> Dict[str, ConnectionStats]:
        """Get statistics for all pools"""
        
        stats = {}
        
        if self.cassandra:
            stats["cassandra"] = self.cassandra.stats
            
        if self.postgres:
            stats["postgres"] = self.postgres.stats
            
        if self.mongo:
            stats["mongo"] = self.mongo.stats
            
        return stats
        
    async def close_all(self):
        """Close all pools"""
        
        if self.cassandra:
            await self.cassandra.close()
            
        if self.postgres:
            await self.postgres.close()
            
        if self.mongo:
            await self.mongo.close()
            
        logger.info("All database pools closed")


# Global pool instance
_db_pool: Optional[UnifiedDatabasePool] = None


async def get_db_pool() -> UnifiedDatabasePool:
    """Get or create database pool"""
    global _db_pool
    
    if _db_pool is None:
        _db_pool = UnifiedDatabasePool()
        
        # Load configuration
        config = {
            "cassandra": {
                "hosts": ["cassandra"],
                "keyspace": "platformq",
                "pool": {
                    "min_size": 5,
                    "max_size": 20
                }
            }
            # Add other databases as needed
        }
        
        await _db_pool.initialize(config)
        
    return _db_pool 