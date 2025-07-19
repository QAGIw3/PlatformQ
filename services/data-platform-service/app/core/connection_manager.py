"""
Unified Connection Manager for all data sources
"""
import asyncio
from typing import Dict, Any, Optional, List, Union
from contextlib import asynccontextmanager
from datetime import datetime
import logging

# Database drivers
import asyncpg  # PostgreSQL
import motor.motor_asyncio  # MongoDB
from cassandra.cluster import Cluster
from cassandra.auth import PlainTextAuthProvider
from cassandra.policies import DCAwareRoundRobinPolicy
from elasticsearch import AsyncElasticsearch
from minio import Minio
from aioinflux import InfluxDBClient
import trino
import pulsar
from pyignite import AsyncClient as IgniteClient
import redis.asyncio as aioredis

from platformq_shared.utils.logger import get_logger
from .config import settings

logger = get_logger(__name__)


class ConnectionPool:
    """Generic connection pool wrapper"""
    
    def __init__(self, name: str, max_size: int = 10):
        self.name = name
        self.max_size = max_size
        self.pool: List[Any] = []
        self.in_use: Dict[int, Any] = {}
        self._lock = asyncio.Lock()
        
    async def acquire(self) -> Any:
        """Acquire a connection from the pool"""
        async with self._lock:
            if self.pool:
                conn = self.pool.pop()
            else:
                conn = None
            
            if conn:
                self.in_use[id(conn)] = conn
                return conn
            
        return None
        
    async def release(self, conn: Any) -> None:
        """Release a connection back to the pool"""
        async with self._lock:
            if id(conn) in self.in_use:
                del self.in_use[id(conn)]
                
            if len(self.pool) < self.max_size:
                self.pool.append(conn)
            else:
                # Close excess connection
                await self._close_connection(conn)
    
    async def _close_connection(self, conn: Any) -> None:
        """Close a connection (override in subclasses)"""
        if hasattr(conn, 'close'):
            await conn.close()


class UnifiedConnectionManager:
    """
    Manages connections to all data sources with pooling and health checks.
    
    Supported sources:
    - PostgreSQL
    - MongoDB
    - Cassandra
    - Elasticsearch
    - MinIO (S3-compatible)
    - InfluxDB
    - Trino
    - Apache Pulsar
    - Apache Ignite
    """
    
    def __init__(self, config: Dict[str, Any]):
        self.config = config
        self.connections: Dict[str, Any] = {}
        self.pools: Dict[str, ConnectionPool] = {}
        self._health_status: Dict[str, bool] = {}
        self._initialized = False
        
    async def initialize(self) -> None:
        """Initialize all configured connections"""
        try:
            # PostgreSQL
            if self.config.get("postgresql_enabled", True):
                await self._init_postgresql()
                
            # MongoDB
            if self.config.get("mongodb_enabled", True):
                await self._init_mongodb()
                
            # Cassandra
            if self.config.get("cassandra_enabled", True):
                await self._init_cassandra()
                
            # Elasticsearch
            if self.config.get("elasticsearch_enabled", True):
                await self._init_elasticsearch()
                
            # MinIO
            if self.config.get("minio_enabled", True):
                await self._init_minio()
                
            # InfluxDB
            if self.config.get("influxdb_enabled", False):
                await self._init_influxdb()
                
            # Trino
            if self.config.get("trino_enabled", True):
                await self._init_trino()
                
            # Pulsar
            if self.config.get("pulsar_enabled", True):
                await self._init_pulsar()
                
            # Ignite
            if self.config.get("ignite_enabled", True):
                await self._init_ignite()
                
            self._initialized = True
            logger.info("Connection manager initialized successfully")
            
        except Exception as e:
            logger.error(f"Failed to initialize connection manager: {e}")
            raise
    
    async def _init_postgresql(self) -> None:
        """Initialize PostgreSQL connection pool"""
        try:
            pool = await asyncpg.create_pool(
                host=self.config.get("postgresql_host", "localhost"),
                port=self.config.get("postgresql_port", 5432),
                user=self.config.get("postgresql_user", "postgres"),
                password=self.config.get("postgresql_password", "postgres"),
                database=self.config.get("postgresql_database", "platformq"),
                min_size=5,
                max_size=self.config.get("connection_pool_size", 20),
                command_timeout=60
            )
            
            self.connections["postgresql"] = pool
            self._health_status["postgresql"] = True
            logger.info("PostgreSQL connection pool created")
            
        except Exception as e:
            logger.error(f"Failed to initialize PostgreSQL: {e}")
            self._health_status["postgresql"] = False
    
    async def _init_mongodb(self) -> None:
        """Initialize MongoDB connection"""
        try:
            mongo_uri = self.config.get(
                "mongodb_uri",
                "mongodb://localhost:27017"
            )
            
            client = motor.motor_asyncio.AsyncIOMotorClient(
                mongo_uri,
                maxPoolSize=self.config.get("connection_pool_size", 20),
                minPoolSize=5
            )
            
            # Test connection
            await client.admin.command('ismaster')
            
            self.connections["mongodb"] = client
            self._health_status["mongodb"] = True
            logger.info("MongoDB connection established")
            
        except Exception as e:
            logger.error(f"Failed to initialize MongoDB: {e}")
            self._health_status["mongodb"] = False
    
    async def _init_cassandra(self) -> None:
        """Initialize Cassandra connection"""
        try:
            auth_provider = None
            username = self.config.get("cassandra_username")
            password = self.config.get("cassandra_password")
            
            if username and password:
                auth_provider = PlainTextAuthProvider(
                    username=username,
                    password=password
                )
            
            cluster = Cluster(
                contact_points=settings.cassandra_hosts,
                port=settings.cassandra_port,
                auth_provider=auth_provider,
                load_balancing_policy=DCAwareRoundRobinPolicy(),
                protocol_version=4
            )
            
            session = cluster.connect()
            
            # Set keyspace if configured
            if settings.cassandra_keyspace:
                session.set_keyspace(settings.cassandra_keyspace)
            
            self.connections["cassandra"] = {
                "cluster": cluster,
                "session": session
            }
            self._health_status["cassandra"] = True
            logger.info("Cassandra connection established")
            
        except Exception as e:
            logger.error(f"Failed to initialize Cassandra: {e}")
            self._health_status["cassandra"] = False
    
    async def _init_elasticsearch(self) -> None:
        """Initialize Elasticsearch connection"""
        try:
            es_config = settings.get_elasticsearch_config()
            
            client = AsyncElasticsearch(**es_config)
            
            # Test connection
            info = await client.info()
            logger.info(f"Connected to Elasticsearch {info['version']['number']}")
            
            self.connections["elasticsearch"] = client
            self._health_status["elasticsearch"] = True
            
        except Exception as e:
            logger.error(f"Failed to initialize Elasticsearch: {e}")
            self._health_status["elasticsearch"] = False
    
    async def _init_minio(self) -> None:
        """Initialize MinIO connection"""
        try:
            minio_config = settings.get_minio_config()
            
            client = Minio(**minio_config)
            
            # Test connection by listing buckets
            buckets = client.list_buckets()
            logger.info(f"Connected to MinIO, found {len(buckets)} buckets")
            
            self.connections["minio"] = client
            self._health_status["minio"] = True
            
        except Exception as e:
            logger.error(f"Failed to initialize MinIO: {e}")
            self._health_status["minio"] = False
    
    async def _init_influxdb(self) -> None:
        """Initialize InfluxDB connection"""
        try:
            client = InfluxDBClient(
                host=self.config.get("influxdb_host", "localhost"),
                port=self.config.get("influxdb_port", 8086),
                username=self.config.get("influxdb_username"),
                password=self.config.get("influxdb_password"),
                database=self.config.get("influxdb_database", "platformq")
            )
            
            # Test connection
            await client.ping()
            
            self.connections["influxdb"] = client
            self._health_status["influxdb"] = True
            logger.info("InfluxDB connection established")
            
        except Exception as e:
            logger.error(f"Failed to initialize InfluxDB: {e}")
            self._health_status["influxdb"] = False
    
    async def _init_trino(self) -> None:
        """Initialize Trino connection"""
        try:
            trino_params = settings.get_trino_connection_params()
            
            # Create a connection factory
            def create_trino_connection():
                return trino.dbapi.connect(**trino_params)
            
            # Create a simple pool
            pool = []
            for _ in range(5):  # Pre-create 5 connections
                conn = create_trino_connection()
                pool.append(conn)
            
            self.connections["trino"] = {
                "factory": create_trino_connection,
                "pool": pool
            }
            self._health_status["trino"] = True
            logger.info("Trino connection pool created")
            
        except Exception as e:
            logger.error(f"Failed to initialize Trino: {e}")
            self._health_status["trino"] = False
    
    async def _init_pulsar(self) -> None:
        """Initialize Pulsar client"""
        try:
            pulsar_config = settings.get_pulsar_config()
            
            client = pulsar.Client(
                service_url=pulsar_config["service_url"],
                authentication=pulsar_config.get("authentication"),
                operation_timeout_seconds=pulsar_config.get("operation_timeout_seconds", 30)
            )
            
            self.connections["pulsar"] = client
            self._health_status["pulsar"] = True
            logger.info("Pulsar client initialized")
            
        except Exception as e:
            logger.error(f"Failed to initialize Pulsar: {e}")
            self._health_status["pulsar"] = False
    
    async def _init_ignite(self) -> None:
        """Initialize Ignite connection"""
        try:
            client = IgniteClient()
            await client.connect(settings.ignite_host, settings.ignite_port)
            
            self.connections["ignite"] = client
            self._health_status["ignite"] = True
            logger.info("Ignite connection established")
            
        except Exception as e:
            logger.error(f"Failed to initialize Ignite: {e}")
            self._health_status["ignite"] = False
    
    def get_connection(self, source: str) -> Any:
        """Get a connection for the specified source"""
        if not self._initialized:
            raise RuntimeError("Connection manager not initialized")
            
        if source not in self.connections:
            raise ValueError(f"Unknown data source: {source}")
            
        return self.connections[source]
    
    @asynccontextmanager
    async def get_postgresql_connection(self):
        """Get a PostgreSQL connection from the pool"""
        pool = self.get_connection("postgresql")
        async with pool.acquire() as conn:
            yield conn
    
    @asynccontextmanager
    async def get_mongodb_database(self, database: str):
        """Get a MongoDB database instance"""
        client = self.get_connection("mongodb")
        yield client[database]
    
    def get_cassandra_session(self):
        """Get Cassandra session"""
        conn = self.get_connection("cassandra")
        return conn["session"]
    
    def get_elasticsearch_client(self) -> AsyncElasticsearch:
        """Get Elasticsearch client"""
        return self.get_connection("elasticsearch")
    
    def get_minio_client(self) -> Minio:
        """Get MinIO client"""
        return self.get_connection("minio")
    
    @asynccontextmanager
    async def get_trino_connection(self):
        """Get a Trino connection"""
        trino_info = self.get_connection("trino")
        pool = trino_info["pool"]
        factory = trino_info["factory"]
        
        # Try to get from pool
        if pool:
            conn = pool.pop()
        else:
            conn = factory()
            
        try:
            yield conn
        finally:
            # Return to pool if space available
            if len(pool) < 10:
                pool.append(conn)
            else:
                conn.close()
    
    def get_pulsar_client(self) -> pulsar.Client:
        """Get Pulsar client"""
        return self.get_connection("pulsar")
    
    def get_ignite_client(self) -> IgniteClient:
        """Get Ignite client"""
        return self.get_connection("ignite")
    
    async def health_check(self) -> Dict[str, Any]:
        """Perform health check on all connections"""
        health_report = {
            "timestamp": datetime.utcnow().isoformat(),
            "overall": "healthy",
            "sources": {}
        }
        
        # Check PostgreSQL
        if "postgresql" in self.connections:
            try:
                async with self.get_postgresql_connection() as conn:
                    await conn.fetchval("SELECT 1")
                health_report["sources"]["postgresql"] = "healthy"
            except Exception as e:
                health_report["sources"]["postgresql"] = f"unhealthy: {str(e)}"
                health_report["overall"] = "degraded"
        
        # Check MongoDB
        if "mongodb" in self.connections:
            try:
                client = self.get_connection("mongodb")
                await client.admin.command('ping')
                health_report["sources"]["mongodb"] = "healthy"
            except Exception as e:
                health_report["sources"]["mongodb"] = f"unhealthy: {str(e)}"
                health_report["overall"] = "degraded"
        
        # Check Cassandra
        if "cassandra" in self.connections:
            try:
                session = self.get_cassandra_session()
                session.execute("SELECT now() FROM system.local")
                health_report["sources"]["cassandra"] = "healthy"
            except Exception as e:
                health_report["sources"]["cassandra"] = f"unhealthy: {str(e)}"
                health_report["overall"] = "degraded"
        
        # Check Elasticsearch
        if "elasticsearch" in self.connections:
            try:
                client = self.get_elasticsearch_client()
                await client.ping()
                health_report["sources"]["elasticsearch"] = "healthy"
            except Exception as e:
                health_report["sources"]["elasticsearch"] = f"unhealthy: {str(e)}"
                health_report["overall"] = "degraded"
        
        # Check MinIO
        if "minio" in self.connections:
            try:
                client = self.get_minio_client()
                client.list_buckets()
                health_report["sources"]["minio"] = "healthy"
            except Exception as e:
                health_report["sources"]["minio"] = f"unhealthy: {str(e)}"
                health_report["overall"] = "degraded"
        
        return health_report
    
    async def health_check_loop(self) -> None:
        """Background health check loop"""
        while True:
            try:
                await asyncio.sleep(60)  # Check every minute
                
                health = await self.health_check()
                
                if health["overall"] != "healthy":
                    logger.warning(f"Health check failed: {health}")
                    
                    # Try to reconnect failed sources
                    for source, status in health["sources"].items():
                        if "unhealthy" in status:
                            logger.info(f"Attempting to reconnect to {source}")
                            await self._reconnect_source(source)
                            
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Health check loop error: {e}")
    
    async def _reconnect_source(self, source: str) -> None:
        """Attempt to reconnect to a failed source"""
        try:
            if source == "postgresql":
                await self._init_postgresql()
            elif source == "mongodb":
                await self._init_mongodb()
            elif source == "cassandra":
                await self._init_cassandra()
            elif source == "elasticsearch":
                await self._init_elasticsearch()
            elif source == "minio":
                await self._init_minio()
            elif source == "influxdb":
                await self._init_influxdb()
            elif source == "trino":
                await self._init_trino()
            elif source == "pulsar":
                await self._init_pulsar()
            elif source == "ignite":
                await self._init_ignite()
                
            logger.info(f"Successfully reconnected to {source}")
            
        except Exception as e:
            logger.error(f"Failed to reconnect to {source}: {e}")
    
    async def close_all(self) -> None:
        """Close all connections"""
        logger.info("Closing all connections...")
        
        # PostgreSQL
        if "postgresql" in self.connections:
            try:
                await self.connections["postgresql"].close()
            except Exception as e:
                logger.error(f"Error closing PostgreSQL: {e}")
        
        # MongoDB
        if "mongodb" in self.connections:
            try:
                self.connections["mongodb"].close()
            except Exception as e:
                logger.error(f"Error closing MongoDB: {e}")
        
        # Cassandra
        if "cassandra" in self.connections:
            try:
                self.connections["cassandra"]["cluster"].shutdown()
            except Exception as e:
                logger.error(f"Error closing Cassandra: {e}")
        
        # Elasticsearch
        if "elasticsearch" in self.connections:
            try:
                await self.connections["elasticsearch"].close()
            except Exception as e:
                logger.error(f"Error closing Elasticsearch: {e}")
        
        # InfluxDB
        if "influxdb" in self.connections:
            try:
                await self.connections["influxdb"].close()
            except Exception as e:
                logger.error(f"Error closing InfluxDB: {e}")
        
        # Trino
        if "trino" in self.connections:
            try:
                for conn in self.connections["trino"]["pool"]:
                    conn.close()
            except Exception as e:
                logger.error(f"Error closing Trino connections: {e}")
        
        # Pulsar
        if "pulsar" in self.connections:
            try:
                self.connections["pulsar"].close()
            except Exception as e:
                logger.error(f"Error closing Pulsar: {e}")
        
        # Ignite
        if "ignite" in self.connections:
            try:
                await self.connections["ignite"].close()
            except Exception as e:
                logger.error(f"Error closing Ignite: {e}")
        
        logger.info("All connections closed") 