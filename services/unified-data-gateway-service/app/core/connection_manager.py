"""
Unified Connection Manager

Manages connections to all data stores with pooling, health checks, and failover.
"""

import asyncio
import logging
from typing import Dict, Any, Optional, List
from dataclasses import dataclass
from datetime import datetime, timedelta
from contextlib import asynccontextmanager

import asyncpg
from aiocassandra import aiosession
from cassandra.cluster import Cluster
from cassandra.policies import DCAwareRoundRobinPolicy, TokenAwarePolicy
from cassandra.auth import PlainTextAuthProvider
import motor.motor_asyncio
from pyignite import AsyncClient as IgniteClient
from gremlin_python.driver import client as gremlin_client
from gremlin_python.driver.protocol import GremlinServerError
from elasticsearch import AsyncElasticsearch
from influxdb_client.client.influxdb_client_async import InfluxDBClientAsync

logger = logging.getLogger(__name__)


@dataclass
class ConnectionConfig:
    """Configuration for a database connection"""
    db_type: str  # postgresql, cassandra, mongodb, ignite, janusgraph, elasticsearch, influxdb
    hosts: List[str]
    port: int
    database: Optional[str] = None
    username: Optional[str] = None
    password: Optional[str] = None
    pool_size: int = 10
    max_pool_size: int = 50
    timeout: int = 30
    retry_attempts: int = 3
    health_check_interval: int = 60
    extra_params: Dict[str, Any] = None


@dataclass
class ConnectionHealth:
    """Health status of a connection"""
    healthy: bool
    last_check: datetime
    error_count: int = 0
    last_error: Optional[str] = None
    latency_ms: Optional[float] = None


class UnifiedConnectionManager:
    """Manages all database connections with pooling and health monitoring"""
    
    def __init__(self, config: Dict[str, Any]):
        self.config = config
        self.connections: Dict[str, Any] = {}
        self.pools: Dict[str, Any] = {}
        self.health_status: Dict[str, ConnectionHealth] = {}
        self._health_check_task: Optional[asyncio.Task] = None
        self._initialized = False
        
    async def initialize(self):
        """Initialize all configured database connections"""
        if self._initialized:
            return
            
        logger.info("Initializing database connections...")
        
        # Initialize PostgreSQL connections
        if "postgresql" in self.config:
            await self._init_postgresql(self.config["postgresql"])
            
        # Initialize Cassandra connections
        if "cassandra" in self.config:
            await self._init_cassandra(self.config["cassandra"])
            
        # Initialize MongoDB connections
        if "mongodb" in self.config:
            await self._init_mongodb(self.config["mongodb"])
            
        # Initialize Ignite connections
        if "ignite" in self.config:
            await self._init_ignite(self.config["ignite"])
            
        # Initialize JanusGraph connections
        if "janusgraph" in self.config:
            await self._init_janusgraph(self.config["janusgraph"])
            
        # Initialize Elasticsearch connections
        if "elasticsearch" in self.config:
            await self._init_elasticsearch(self.config["elasticsearch"])
            
        # Initialize InfluxDB connections
        if "influxdb" in self.config:
            await self._init_influxdb(self.config["influxdb"])
            
        self._initialized = True
        logger.info("All database connections initialized")
        
    async def _init_postgresql(self, configs: List[ConnectionConfig]):
        """Initialize PostgreSQL connection pools"""
        for config in configs:
            pool_name = f"postgresql_{config.database}"
            try:
                dsn = f"postgresql://{config.username}:{config.password}@{config.hosts[0]}:{config.port}/{config.database}"
                
                pool = await asyncpg.create_pool(
                    dsn,
                    min_size=config.pool_size,
                    max_size=config.max_pool_size,
                    timeout=config.timeout,
                    command_timeout=config.timeout
                )
                
                self.pools[pool_name] = pool
                self.health_status[pool_name] = ConnectionHealth(
                    healthy=True,
                    last_check=datetime.utcnow()
                )
                
                logger.info(f"PostgreSQL pool '{pool_name}' created successfully")
                
            except Exception as e:
                logger.error(f"Failed to create PostgreSQL pool '{pool_name}': {e}")
                self.health_status[pool_name] = ConnectionHealth(
                    healthy=False,
                    last_check=datetime.utcnow(),
                    error_count=1,
                    last_error=str(e)
                )
                
    async def _init_cassandra(self, configs: List[ConnectionConfig]):
        """Initialize Cassandra connections"""
        for config in configs:
            pool_name = f"cassandra_{config.database or 'default'}"
            try:
                auth_provider = None
                if config.username and config.password:
                    auth_provider = PlainTextAuthProvider(
                        username=config.username,
                        password=config.password
                    )
                
                load_balancing = TokenAwarePolicy(
                    DCAwareRoundRobinPolicy(local_dc='datacenter1')
                )
                
                cluster = Cluster(
                    contact_points=config.hosts,
                    port=config.port,
                    auth_provider=auth_provider,
                    load_balancing_policy=load_balancing,
                    protocol_version=4,
                    connect_timeout=config.timeout
                )
                
                session = cluster.connect()
                if config.database:
                    session.set_keyspace(config.database)
                    
                # Create async wrapper
                async_session = aiosession(session)
                
                self.connections[pool_name] = {
                    'cluster': cluster,
                    'session': session,
                    'async_session': async_session
                }
                
                self.health_status[pool_name] = ConnectionHealth(
                    healthy=True,
                    last_check=datetime.utcnow()
                )
                
                logger.info(f"Cassandra connection '{pool_name}' created successfully")
                
            except Exception as e:
                logger.error(f"Failed to create Cassandra connection '{pool_name}': {e}")
                self.health_status[pool_name] = ConnectionHealth(
                    healthy=False,
                    last_check=datetime.utcnow(),
                    error_count=1,
                    last_error=str(e)
                )
                
    async def _init_mongodb(self, configs: List[ConnectionConfig]):
        """Initialize MongoDB connections"""
        for config in configs:
            pool_name = f"mongodb_{config.database}"
            try:
                uri = f"mongodb://{config.username}:{config.password}@{','.join(config.hosts)}/{config.database}"
                
                client = motor.motor_asyncio.AsyncIOMotorClient(
                    uri,
                    maxPoolSize=config.max_pool_size,
                    minPoolSize=config.pool_size,
                    serverSelectionTimeoutMS=config.timeout * 1000
                )
                
                # Test connection
                await client.server_info()
                
                self.connections[pool_name] = client
                self.health_status[pool_name] = ConnectionHealth(
                    healthy=True,
                    last_check=datetime.utcnow()
                )
                
                logger.info(f"MongoDB connection '{pool_name}' created successfully")
                
            except Exception as e:
                logger.error(f"Failed to create MongoDB connection '{pool_name}': {e}")
                self.health_status[pool_name] = ConnectionHealth(
                    healthy=False,
                    last_check=datetime.utcnow(),
                    error_count=1,
                    last_error=str(e)
                )
                
    async def _init_ignite(self, configs: List[ConnectionConfig]):
        """Initialize Apache Ignite connections"""
        for config in configs:
            pool_name = f"ignite_{config.hosts[0]}"
            try:
                client = IgniteClient()
                await client.connect(config.hosts[0], config.port)
                
                self.connections[pool_name] = client
                self.health_status[pool_name] = ConnectionHealth(
                    healthy=True,
                    last_check=datetime.utcnow()
                )
                
                logger.info(f"Ignite connection '{pool_name}' created successfully")
                
            except Exception as e:
                logger.error(f"Failed to create Ignite connection '{pool_name}': {e}")
                self.health_status[pool_name] = ConnectionHealth(
                    healthy=False,
                    last_check=datetime.utcnow(),
                    error_count=1,
                    last_error=str(e)
                )
                
    async def _init_janusgraph(self, configs: List[ConnectionConfig]):
        """Initialize JanusGraph connections"""
        for config in configs:
            pool_name = f"janusgraph_{config.hosts[0]}"
            try:
                # Create Gremlin client
                client = gremlin_client.Client(
                    f"ws://{config.hosts[0]}:{config.port}/gremlin",
                    'g',
                    pool_size=config.pool_size,
                    max_workers=config.max_pool_size
                )
                
                # Test connection
                result = client.submit("g.V().limit(1)").all().result()
                
                self.connections[pool_name] = client
                self.health_status[pool_name] = ConnectionHealth(
                    healthy=True,
                    last_check=datetime.utcnow()
                )
                
                logger.info(f"JanusGraph connection '{pool_name}' created successfully")
                
            except Exception as e:
                logger.error(f"Failed to create JanusGraph connection '{pool_name}': {e}")
                self.health_status[pool_name] = ConnectionHealth(
                    healthy=False,
                    last_check=datetime.utcnow(),
                    error_count=1,
                    last_error=str(e)
                )
                
    async def _init_elasticsearch(self, configs: List[ConnectionConfig]):
        """Initialize Elasticsearch connections"""
        for config in configs:
            pool_name = f"elasticsearch_{config.hosts[0]}"
            try:
                hosts = [f"http://{host}:{config.port}" for host in config.hosts]
                
                client = AsyncElasticsearch(
                    hosts,
                    basic_auth=(config.username, config.password) if config.username else None,
                    timeout=config.timeout,
                    max_retries=config.retry_attempts
                )
                
                # Test connection
                await client.ping()
                
                self.connections[pool_name] = client
                self.health_status[pool_name] = ConnectionHealth(
                    healthy=True,
                    last_check=datetime.utcnow()
                )
                
                logger.info(f"Elasticsearch connection '{pool_name}' created successfully")
                
            except Exception as e:
                logger.error(f"Failed to create Elasticsearch connection '{pool_name}': {e}")
                self.health_status[pool_name] = ConnectionHealth(
                    healthy=False,
                    last_check=datetime.utcnow(),
                    error_count=1,
                    last_error=str(e)
                )
                
    async def _init_influxdb(self, configs: List[ConnectionConfig]):
        """Initialize InfluxDB connections"""
        for config in configs:
            pool_name = f"influxdb_{config.hosts[0]}"
            try:
                url = f"http://{config.hosts[0]}:{config.port}"
                
                client = InfluxDBClientAsync(
                    url=url,
                    token=config.password,
                    org=config.extra_params.get('org', 'platformq'),
                    timeout=config.timeout * 1000
                )
                
                # Test connection
                await client.ping()
                
                self.connections[pool_name] = client
                self.health_status[pool_name] = ConnectionHealth(
                    healthy=True,
                    last_check=datetime.utcnow()
                )
                
                logger.info(f"InfluxDB connection '{pool_name}' created successfully")
                
            except Exception as e:
                logger.error(f"Failed to create InfluxDB connection '{pool_name}': {e}")
                self.health_status[pool_name] = ConnectionHealth(
                    healthy=False,
                    last_check=datetime.utcnow(),
                    error_count=1,
                    last_error=str(e)
                )
                
    @asynccontextmanager
    async def get_connection(self, db_type: str, database: Optional[str] = None):
        """Get a connection from the appropriate pool"""
        pool_name = f"{db_type}_{database or 'default'}"
        
        if pool_name not in self.pools and pool_name not in self.connections:
            raise ValueError(f"No connection pool found for {pool_name}")
            
        # Check health status
        if pool_name in self.health_status and not self.health_status[pool_name].healthy:
            raise ConnectionError(f"Connection {pool_name} is unhealthy")
            
        # Return connection based on type
        if db_type == "postgresql":
            async with self.pools[pool_name].acquire() as conn:
                yield conn
        elif db_type in ["cassandra", "mongodb", "ignite", "janusgraph", "elasticsearch", "influxdb"]:
            yield self.connections[pool_name]
        else:
            raise ValueError(f"Unsupported database type: {db_type}")
            
    async def health_check_loop(self):
        """Continuously check health of all connections"""
        while True:
            try:
                await self._check_all_connections()
                await asyncio.sleep(60)  # Check every minute
            except Exception as e:
                logger.error(f"Error in health check loop: {e}")
                await asyncio.sleep(60)
                
    async def _check_all_connections(self):
        """Check health of all connections"""
        tasks = []
        
        # PostgreSQL health checks
        for pool_name, pool in self.pools.items():
            if pool_name.startswith("postgresql"):
                tasks.append(self._check_postgresql_health(pool_name, pool))
                
        # Cassandra health checks
        for conn_name, conn_data in self.connections.items():
            if conn_name.startswith("cassandra"):
                tasks.append(self._check_cassandra_health(conn_name, conn_data))
            elif conn_name.startswith("mongodb"):
                tasks.append(self._check_mongodb_health(conn_name, conn_data))
            elif conn_name.startswith("ignite"):
                tasks.append(self._check_ignite_health(conn_name, conn_data))
            elif conn_name.startswith("janusgraph"):
                tasks.append(self._check_janusgraph_health(conn_name, conn_data))
            elif conn_name.startswith("elasticsearch"):
                tasks.append(self._check_elasticsearch_health(conn_name, conn_data))
            elif conn_name.startswith("influxdb"):
                tasks.append(self._check_influxdb_health(conn_name, conn_data))
                
        await asyncio.gather(*tasks, return_exceptions=True)
        
    async def _check_postgresql_health(self, pool_name: str, pool):
        """Check PostgreSQL connection health"""
        try:
            start_time = asyncio.get_event_loop().time()
            async with pool.acquire() as conn:
                await conn.fetchval("SELECT 1")
            latency_ms = (asyncio.get_event_loop().time() - start_time) * 1000
            
            self.health_status[pool_name] = ConnectionHealth(
                healthy=True,
                last_check=datetime.utcnow(),
                latency_ms=latency_ms
            )
        except Exception as e:
            logger.error(f"PostgreSQL health check failed for {pool_name}: {e}")
            if pool_name in self.health_status:
                self.health_status[pool_name].healthy = False
                self.health_status[pool_name].error_count += 1
                self.health_status[pool_name].last_error = str(e)
                self.health_status[pool_name].last_check = datetime.utcnow()
                
    async def close_all(self):
        """Close all database connections"""
        logger.info("Closing all database connections...")
        
        # Close PostgreSQL pools
        for pool_name, pool in self.pools.items():
            try:
                await pool.close()
                logger.info(f"Closed PostgreSQL pool: {pool_name}")
            except Exception as e:
                logger.error(f"Error closing PostgreSQL pool {pool_name}: {e}")
                
        # Close other connections
        for conn_name, conn in self.connections.items():
            try:
                if conn_name.startswith("cassandra"):
                    conn['cluster'].shutdown()
                elif conn_name.startswith("mongodb"):
                    conn.close()
                elif conn_name.startswith("ignite"):
                    await conn.close()
                elif conn_name.startswith("janusgraph"):
                    conn.close()
                elif conn_name.startswith("elasticsearch"):
                    await conn.close()
                elif conn_name.startswith("influxdb"):
                    await conn.close()
                    
                logger.info(f"Closed connection: {conn_name}")
            except Exception as e:
                logger.error(f"Error closing connection {conn_name}: {e}")
                
        logger.info("All database connections closed") 