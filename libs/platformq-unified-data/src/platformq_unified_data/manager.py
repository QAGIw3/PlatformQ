"""Main unified data manager for orchestrating data access"""

import asyncio
import logging
from typing import Any, Dict, List, Optional, Type, Union
from contextlib import asynccontextmanager

from cassandra.cluster import Cluster, Session
from cassandra.auth import PlainTextAuthProvider
from pyignite import Client as IgniteClient
from elasticsearch import AsyncElasticsearch
from minio import Minio
import trino
from pulsar import Client as PulsarClient
import aiogremlin

from .repository import Repository, AsyncRepository
from .cache import CacheManager
from .models import BaseModel
from .adapters import (
    CassandraAdapter,
    ElasticsearchAdapter,
    IgniteAdapter,
    MinioAdapter,
    JanusGraphAdapter
)
from .exceptions import ConnectionError, DataAccessError

logger = logging.getLogger(__name__)


class UnifiedDataManager:
    """Main entry point for unified data access across all stores"""
    
    def __init__(self, config: Optional[Dict[str, Any]] = None):
        self.config = config or self._get_default_config()
        self.connections = {}
        self.adapters = {}
        self.repositories = {}
        self.cache_manager = None
        self._initialized = False
    
    async def initialize(self):
        """Initialize all connections and adapters"""
        if self._initialized:
            return
        
        try:
            # Initialize connections
            await self._init_cassandra()
            await self._init_ignite()
            await self._init_elasticsearch()
            await self._init_minio()
            await self._init_janusgraph()
            await self._init_trino()
            await self._init_pulsar()
            
            # Initialize cache manager
            await self._init_cache()
            
            # Initialize store adapters
            await self._init_adapters()
            
            self._initialized = True
            logger.info("UnifiedDataManager initialized successfully")
            
        except Exception as e:
            logger.error(f"Failed to initialize UnifiedDataManager: {e}")
            raise ConnectionError("UnifiedDataManager", str(e))
    
    async def close(self):
        """Close all connections"""
        # Close Cassandra
        if 'cassandra' in self.connections:
            self.connections['cassandra'].shutdown()
        
        # Close Ignite
        if 'ignite' in self.connections:
            self.connections['ignite'].close()
        
        # Close Elasticsearch
        if 'elasticsearch' in self.connections:
            await self.connections['elasticsearch'].close()
        
        # Close Pulsar
        if 'pulsar' in self.connections:
            self.connections['pulsar'].close()
        
        # Close JanusGraph
        if 'janusgraph' in self.connections:
            await self.connections['janusgraph'].close()
        
        self._initialized = False
        logger.info("UnifiedDataManager closed")
    
    @asynccontextmanager
    async def session(self):
        """Context manager for data access session"""
        await self.initialize()
        try:
            yield self
        finally:
            # Could implement session-specific cleanup here
            pass
    
    def get_repository(self, model_class: Type[BaseModel], tenant_id: Optional[str] = None) -> Repository:
        """Get or create a repository for the given model"""
        cache_key = f"{model_class.__name__}:{tenant_id or 'default'}"
        
        if cache_key not in self.repositories:
            # Determine which adapters to use based on model configuration
            store_adapters = {}
            for store_name in model_class.__stores__:
                if store_name in self.adapters:
                    store_adapters[store_name] = self.adapters[store_name]
            
            # Create repository
            self.repositories[cache_key] = AsyncRepository(
                model_class=model_class,
                store_adapters=store_adapters,
                cache_manager=self.cache_manager,
                tenant_id=tenant_id
            )
        
        return self.repositories[cache_key]
    
    async def federated_query(self, sql: str, params: Optional[Dict[str, Any]] = None) -> List[Dict[str, Any]]:
        """Execute a federated query across multiple data stores using Trino"""
        if 'trino' not in self.connections:
            raise DataAccessError("Trino connection not available for federated queries")
        
        conn = self.connections['trino']
        cursor = conn.cursor()
        
        try:
            if params:
                # Replace named parameters with positional ones for Trino
                for key, value in params.items():
                    sql = sql.replace(f":{key}", str(value))
            
            cursor.execute(sql)
            columns = [desc[0] for desc in cursor.description]
            
            results = []
            for row in cursor:
                results.append(dict(zip(columns, row)))
            
            return results
            
        except Exception as e:
            logger.error(f"Federated query failed: {e}")
            raise DataAccessError(f"Federated query failed: {e}")
        finally:
            cursor.close()
    
    async def execute_cross_store_transaction(self, operations: List[Dict[str, Any]]):
        """Execute operations across multiple stores with best-effort consistency"""
        results = []
        rollback_operations = []
        
        try:
            # Execute operations in order
            for op in operations:
                store = op['store']
                operation = op['operation']
                params = op.get('params', {})
                
                adapter = self.adapters.get(store)
                if not adapter:
                    raise DataAccessError(f"Store {store} not configured")
                
                # Execute operation
                if operation == 'create':
                    result = await adapter.create(**params)
                    rollback_operations.append({
                        'store': store,
                        'operation': 'delete',
                        'params': {'id': result.id, 'model': params['instance'].__class__}
                    })
                elif operation == 'update':
                    # Get original for rollback
                    original = await adapter.get_by_id(
                        params['instance'].__class__,
                        params['instance'].id
                    )
                    result = await adapter.update(**params)
                    rollback_operations.append({
                        'store': store,
                        'operation': 'update',
                        'params': {'instance': original}
                    })
                elif operation == 'delete':
                    # Get original for rollback
                    original = await adapter.get_by_id(
                        params['model'],
                        params['id']
                    )
                    result = await adapter.delete(**params)
                    rollback_operations.append({
                        'store': store,
                        'operation': 'create',
                        'params': {'instance': original}
                    })
                else:
                    raise DataAccessError(f"Unknown operation: {operation}")
                
                results.append(result)
            
            return results
            
        except Exception as e:
            logger.error(f"Cross-store transaction failed, attempting rollback: {e}")
            
            # Best-effort rollback
            for rollback_op in reversed(rollback_operations):
                try:
                    store = rollback_op['store']
                    operation = rollback_op['operation']
                    params = rollback_op.get('params', {})
                    
                    adapter = self.adapters.get(store)
                    if adapter:
                        if operation == 'create':
                            await adapter.create(**params)
                        elif operation == 'update':
                            await adapter.update(**params)
                        elif operation == 'delete':
                            await adapter.delete(**params)
                except Exception as rollback_error:
                    logger.error(f"Rollback failed for {rollback_op}: {rollback_error}")
            
            raise
    
    async def health_check(self) -> Dict[str, Any]:
        """Check health of all configured stores"""
        health = {
            'status': 'healthy',
            'stores': {}
        }
        
        # Check Cassandra
        if 'cassandra' in self.connections:
            try:
                session = self.connections['cassandra']
                session.execute("SELECT now() FROM system.local")
                health['stores']['cassandra'] = {'status': 'healthy'}
            except Exception as e:
                health['stores']['cassandra'] = {'status': 'unhealthy', 'error': str(e)}
                health['status'] = 'degraded'
        
        # Check Ignite
        if 'ignite' in self.connections:
            try:
                client = self.connections['ignite']
                client.get_cache_names()
                health['stores']['ignite'] = {'status': 'healthy'}
            except Exception as e:
                health['stores']['ignite'] = {'status': 'unhealthy', 'error': str(e)}
                health['status'] = 'degraded'
        
        # Check Elasticsearch
        if 'elasticsearch' in self.connections:
            try:
                es = self.connections['elasticsearch']
                await es.cluster.health()
                health['stores']['elasticsearch'] = {'status': 'healthy'}
            except Exception as e:
                health['stores']['elasticsearch'] = {'status': 'unhealthy', 'error': str(e)}
                health['status'] = 'degraded'
        
        # Check MinIO
        if 'minio' in self.connections:
            try:
                minio = self.connections['minio']
                minio.list_buckets()
                health['stores']['minio'] = {'status': 'healthy'}
            except Exception as e:
                health['stores']['minio'] = {'status': 'unhealthy', 'error': str(e)}
                health['status'] = 'degraded'
        
        return health
    
    # Private initialization methods
    
    async def _init_cassandra(self):
        """Initialize Cassandra connection"""
        if not self.config.get('cassandra', {}).get('enabled', True):
            return
        
        try:
            cassandra_config = self.config.get('cassandra', {})
            
            auth_provider = None
            if cassandra_config.get('username'):
                auth_provider = PlainTextAuthProvider(
                    username=cassandra_config['username'],
                    password=cassandra_config.get('password', '')
                )
            
            cluster = Cluster(
                cassandra_config.get('hosts', ['cassandra']),
                port=cassandra_config.get('port', 9042),
                auth_provider=auth_provider
            )
            
            session = cluster.connect()
            
            # Set default keyspace if provided
            if cassandra_config.get('keyspace'):
                session.set_keyspace(cassandra_config['keyspace'])
            
            self.connections['cassandra'] = session
            logger.info("Cassandra connection initialized")
            
        except Exception as e:
            logger.error(f"Failed to initialize Cassandra: {e}")
            if self.config.get('cassandra', {}).get('required', False):
                raise
    
    async def _init_ignite(self):
        """Initialize Ignite connection"""
        if not self.config.get('ignite', {}).get('enabled', True):
            return
        
        try:
            ignite_config = self.config.get('ignite', {})
            
            client = IgniteClient()
            client.connect(
                ignite_config.get('hosts', [('ignite', 10800)])
            )
            
            self.connections['ignite'] = client
            logger.info("Ignite connection initialized")
            
        except Exception as e:
            logger.error(f"Failed to initialize Ignite: {e}")
            if self.config.get('ignite', {}).get('required', False):
                raise
    
    async def _init_elasticsearch(self):
        """Initialize Elasticsearch connection"""
        if not self.config.get('elasticsearch', {}).get('enabled', True):
            return
        
        try:
            es_config = self.config.get('elasticsearch', {})
            
            es_client = AsyncElasticsearch(
                hosts=es_config.get('hosts', ['http://elasticsearch:9200']),
                basic_auth=(
                    es_config.get('username'),
                    es_config.get('password')
                ) if es_config.get('username') else None
            )
            
            # Test connection
            await es_client.info()
            
            self.connections['elasticsearch'] = es_client
            logger.info("Elasticsearch connection initialized")
            
        except Exception as e:
            logger.error(f"Failed to initialize Elasticsearch: {e}")
            if self.config.get('elasticsearch', {}).get('required', False):
                raise
    
    async def _init_minio(self):
        """Initialize MinIO connection"""
        if not self.config.get('minio', {}).get('enabled', True):
            return
        
        try:
            minio_config = self.config.get('minio', {})
            
            minio_client = Minio(
                minio_config.get('endpoint', 'minio:9000'),
                access_key=minio_config.get('access_key', 'minioadmin'),
                secret_key=minio_config.get('secret_key', 'minioadmin'),
                secure=minio_config.get('secure', False)
            )
            
            self.connections['minio'] = minio_client
            logger.info("MinIO connection initialized")
            
        except Exception as e:
            logger.error(f"Failed to initialize MinIO: {e}")
            if self.config.get('minio', {}).get('required', False):
                raise
    
    async def _init_janusgraph(self):
        """Initialize JanusGraph connection"""
        if not self.config.get('janusgraph', {}).get('enabled', False):
            return
        
        try:
            jg_config = self.config.get('janusgraph', {})
            
            graph = await aiogremlin.create(
                url=jg_config.get('url', 'ws://janusgraph:8182/gremlin')
            )
            
            self.connections['janusgraph'] = graph
            logger.info("JanusGraph connection initialized")
            
        except Exception as e:
            logger.error(f"Failed to initialize JanusGraph: {e}")
            if self.config.get('janusgraph', {}).get('required', False):
                raise
    
    async def _init_trino(self):
        """Initialize Trino connection for federated queries"""
        if not self.config.get('trino', {}).get('enabled', False):
            return
        
        try:
            trino_config = self.config.get('trino', {})
            
            conn = trino.dbapi.connect(
                host=trino_config.get('host', 'trino'),
                port=trino_config.get('port', 8080),
                user=trino_config.get('user', 'platformq'),
                catalog=trino_config.get('catalog', 'hive'),
                schema=trino_config.get('schema', 'default')
            )
            
            self.connections['trino'] = conn
            logger.info("Trino connection initialized")
            
        except Exception as e:
            logger.error(f"Failed to initialize Trino: {e}")
            if self.config.get('trino', {}).get('required', False):
                raise
    
    async def _init_pulsar(self):
        """Initialize Pulsar connection for event publishing"""
        if not self.config.get('pulsar', {}).get('enabled', True):
            return
        
        try:
            pulsar_config = self.config.get('pulsar', {})
            
            client = PulsarClient(
                pulsar_config.get('url', 'pulsar://pulsar:6650')
            )
            
            self.connections['pulsar'] = client
            logger.info("Pulsar connection initialized")
            
        except Exception as e:
            logger.error(f"Failed to initialize Pulsar: {e}")
            if self.config.get('pulsar', {}).get('required', False):
                raise
    
    async def _init_cache(self):
        """Initialize cache manager"""
        cache_config = self.config.get('cache', {})
        
        if cache_config.get('enabled', True) and 'ignite' in self.connections:
            self.cache_manager = CacheManager(
                ignite_client=self.connections['ignite'],
                l1_max_size=cache_config.get('l1_max_size', 1000),
                l1_ttl=cache_config.get('l1_ttl', 300),
                l2_ttl=cache_config.get('l2_ttl', 3600),
                enable_l1=cache_config.get('enable_l1', True),
                enable_l2=cache_config.get('enable_l2', True)
            )
            logger.info("Cache manager initialized")
    
    async def _init_adapters(self):
        """Initialize store adapters"""
        # Cassandra adapter
        if 'cassandra' in self.connections:
            self.adapters['cassandra'] = CassandraAdapter(self.connections['cassandra'])
        
        # Elasticsearch adapter
        if 'elasticsearch' in self.connections:
            self.adapters['elasticsearch'] = ElasticsearchAdapter(self.connections['elasticsearch'])
        
        # Ignite adapter
        if 'ignite' in self.connections:
            self.adapters['ignite'] = IgniteAdapter(self.connections['ignite'])
        
        # MinIO adapter
        if 'minio' in self.connections:
            self.adapters['minio'] = MinioAdapter(self.connections['minio'])
        
        # JanusGraph adapter
        if 'janusgraph' in self.connections:
            self.adapters['janusgraph'] = JanusGraphAdapter(self.connections['janusgraph'])
        
        logger.info(f"Initialized {len(self.adapters)} store adapters")
    
    def _get_default_config(self) -> Dict[str, Any]:
        """Get default configuration"""
        return {
            'cassandra': {
                'enabled': True,
                'hosts': ['cassandra'],
                'port': 9042,
                'keyspace': 'platformq',
                'consistency': 'QUORUM'
            },
            'ignite': {
                'enabled': True,
                'hosts': [('ignite', 10800)],
                'cache_ttl': 3600
            },
            'elasticsearch': {
                'enabled': True,
                'hosts': ['http://elasticsearch:9200'],
                'index_prefix': 'platformq_'
            },
            'minio': {
                'enabled': True,
                'endpoint': 'minio:9000',
                'access_key': 'minioadmin',
                'secret_key': 'minioadmin',
                'secure': False
            },
            'janusgraph': {
                'enabled': False,
                'url': 'ws://janusgraph:8182/gremlin'
            },
            'trino': {
                'enabled': False,
                'host': 'trino',
                'port': 8080,
                'user': 'platformq',
                'catalog': 'hive',
                'schema': 'default'
            },
            'pulsar': {
                'enabled': True,
                'url': 'pulsar://pulsar:6650'
            },
            'cache': {
                'enabled': True,
                'l1_max_size': 1000,
                'l1_ttl': 300,
                'l2_ttl': 3600,
                'enable_l1': True,
                'enable_l2': True
            }
        } 