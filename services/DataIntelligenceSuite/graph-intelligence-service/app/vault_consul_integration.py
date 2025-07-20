"""
Vault and Consul Integration for Graph Intelligence Service

Manages:
- JanusGraph database credentials
- Gremlin server authentication
- Trust network signing keys
- Graph analytics configuration
- Distributed graph processing locks
- Service-to-service credentials
"""

import os
import json
import asyncio
import logging
from typing import Dict, Any, Optional, List, Tuple
from datetime import datetime, timedelta
import hvac
import consul.aio
import base64
from cryptography.hazmat.primitives import serialization, hashes
from cryptography.hazmat.primitives.asymmetric import rsa
from cryptography.hazmat.backends import default_backend
import uuid

logger = logging.getLogger(__name__)


class VaultConsulIntegration:
    """Handles Vault and Consul integration for Graph Intelligence service"""
    
    def __init__(self):
        # Vault configuration
        self.vault_addr = os.environ.get('VAULT_ADDR', 'http://vault:8200')
        self.vault_token = os.environ.get('VAULT_TOKEN')
        self.service_name = os.environ.get('SERVICE_NAME', 'graph-intelligence-service')
        
        # Consul configuration
        self.consul_host = os.environ.get('CONSUL_HOST', 'consul')
        self.consul_port = int(os.environ.get('CONSUL_PORT', '8500'))
        
        # Service paths
        self.vault_graph_path = f"secret/data/graph/{self.service_name}"
        self.vault_transit_path = "transit"
        self.vault_pki_path = "pki"
        self.consul_kv_prefix = f"graph/{self.service_name}"
        
        # Clients
        self.vault_client = None
        self.consul_client = None
        
        # Cached credentials and configs
        self._janusgraph_credentials = {}
        self._gremlin_credentials = {}
        self._trust_keys = {}
        self._service_tokens = {}
        self._graph_config = {}
        self._analytics_config = {}
        self._processing_locks = {}
        
        # Rotation tracking
        self._rotation_tasks = {}
        self._lease_renewal_tasks = {}
        self._lock_refresh_tasks = {}

    async def initialize(self):
        """Initialize Vault and Consul clients"""
        try:
            # Initialize Vault client
            self.vault_client = hvac.Client(url=self.vault_addr, token=self.vault_token)
            if not self.vault_client.is_authenticated():
                raise Exception("Vault authentication failed")
            
            # Initialize Consul client
            self.consul_client = consul.aio.Consul(
                host=self.consul_host,
                port=self.consul_port
            )
            
            # Register service with Consul
            await self._register_service()
            
            # Initialize secrets and configurations
            await self._initialize_janusgraph_credentials()
            await self._initialize_gremlin_credentials()
            await self._initialize_trust_keys()
            await self._initialize_service_tokens()
            
            # Load graph configurations from Consul
            await self._load_graph_configurations()
            
            # Start rotation and maintenance tasks
            await self._start_maintenance_tasks()
            
            logger.info("Graph Intelligence service Vault and Consul integration initialized successfully")
            
        except Exception as e:
            logger.error(f"Failed to initialize Vault/Consul integration: {e}")
            raise

    async def _register_service(self):
        """Register graph intelligence service with Consul"""
        service_id = f"{self.service_name}-{os.environ.get('HOSTNAME', 'local')}"
        
        await self.consul_client.agent.service.register(
            name=self.service_name,
            service_id=service_id,
            address=os.environ.get('SERVICE_HOST', 'localhost'),
            port=int(os.environ.get('SERVICE_PORT', '8000')),
            tags=[
                "graph",
                "janusgraph",
                "knowledge-graph",
                "lineage",
                "trust-network",
                "analytics",
                "grpc"
            ],
            check=consul.Check.http(
                f"http://{os.environ.get('SERVICE_HOST', 'localhost')}:{os.environ.get('SERVICE_PORT', '8000')}/health",
                interval="10s",
                timeout="5s",
                deregister_critical_service_after="30s"
            )
        )

    async def _initialize_janusgraph_credentials(self):
        """Initialize JanusGraph database credentials"""
        try:
            # Get or create JanusGraph credentials
            janus_response = self.vault_client.read(f"{self.vault_graph_path}/janusgraph")
            if not janus_response:
                # Generate new credentials
                janus_creds = {
                    'storage_backend': 'cassandra',
                    'storage_hostname': os.environ.get('CASSANDRA_HOST', 'cassandra'),
                    'storage_port': int(os.environ.get('CASSANDRA_PORT', '9042')),
                    'storage_username': 'janusgraph',
                    'storage_password': self._generate_secure_password(),
                    'index_backend': 'elasticsearch',
                    'index_hostname': os.environ.get('ELASTICSEARCH_HOST', 'elasticsearch'),
                    'index_port': int(os.environ.get('ELASTICSEARCH_PORT', '9200')),
                    'index_username': 'janusgraph',
                    'index_password': self._generate_secure_password()
                }
                self.vault_client.write(f"{self.vault_graph_path}/janusgraph", **janus_creds)
                self._janusgraph_credentials = janus_creds
            else:
                self._janusgraph_credentials = janus_response['data']['data']
            
            # Get or create cache configuration
            cache_response = self.vault_client.read(f"{self.vault_graph_path}/cache")
            if not cache_response:
                cache_config = {
                    'cache_db': int(os.environ.get('JANUSGRAPH_CACHE_DB', '2')),
                    'cache_size': int(os.environ.get('JANUSGRAPH_CACHE_SIZE', '0.25')),
                    'cache_time': int(os.environ.get('JANUSGRAPH_CACHE_TIME', '10000'))
                }
                self.vault_client.write(f"{self.vault_graph_path}/cache", **cache_config)
                self._janusgraph_credentials['cache'] = cache_config
            else:
                self._janusgraph_credentials['cache'] = cache_response['data']['data']
            
        except Exception as e:
            logger.error(f"Failed to initialize JanusGraph credentials: {e}")
            raise

    async def _initialize_gremlin_credentials(self):
        """Initialize Gremlin server credentials"""
        try:
            # Get or create Gremlin server credentials
            gremlin_response = self.vault_client.read(f"{self.vault_graph_path}/gremlin")
            if not gremlin_response:
                # Generate new credentials
                gremlin_creds = {
                    'host': os.environ.get('GREMLIN_HOST', 'janusgraph'),
                    'port': int(os.environ.get('GREMLIN_PORT', '8182')),
                    'username': 'graph_service',
                    'password': self._generate_secure_password(),
                    'ssl_enabled': False,
                    'serializer': 'graphbinaryV1',
                    'connection_pool_size': 8
                }
                self.vault_client.write(f"{self.vault_graph_path}/gremlin", **gremlin_creds)
                self._gremlin_credentials = gremlin_creds
            else:
                self._gremlin_credentials = gremlin_response['data']['data']
            
        except Exception as e:
            logger.error(f"Failed to initialize Gremlin credentials: {e}")
            raise

    async def _initialize_trust_keys(self):
        """Initialize trust network signing keys"""
        try:
            # Create or get trust score signing key
            try:
                self.vault_client.read(f"{self.vault_transit_path}/keys/trust-score-signing")
            except:
                self.vault_client.write(
                    f"{self.vault_transit_path}/keys/trust-score-signing",
                    type="rsa-2048",
                    exportable=False
                )
            
            # Create or get lineage attestation key
            try:
                self.vault_client.read(f"{self.vault_transit_path}/keys/lineage-attestation")
            except:
                self.vault_client.write(
                    f"{self.vault_transit_path}/keys/lineage-attestation",
                    type="ed25519",
                    exportable=False
                )
            
            # Create or get community verification key
            try:
                self.vault_client.read(f"{self.vault_transit_path}/keys/community-verification")
            except:
                self.vault_client.write(
                    f"{self.vault_transit_path}/keys/community-verification",
                    type="rsa-2048",
                    exportable=False
                )
            
            self._trust_keys = {
                'trust_score': 'trust-score-signing',
                'lineage': 'lineage-attestation',
                'community': 'community-verification'
            }
            
        except Exception as e:
            logger.error(f"Failed to initialize trust keys: {e}")
            raise

    async def _initialize_service_tokens(self):
        """Initialize service-to-service authentication tokens"""
        try:
            services = [
                'verifiable-credential-service',
                'derivatives-engine-service',
                'analytics-service',
                'search-service',
                'blockchain-gateway-service'
            ]
            
            for service in services:
                token_response = self.vault_client.read(f"{self.vault_graph_path}/tokens/{service}")
                if not token_response:
                    # Generate new service token
                    token = {
                        'token': self._generate_api_key(64),
                        'refresh_token': self._generate_api_key(64),
                        'expires_at': (datetime.utcnow() + timedelta(days=30)).isoformat()
                    }
                    self.vault_client.write(f"{self.vault_graph_path}/tokens/{service}", **token)
                    self._service_tokens[service] = token
                else:
                    self._service_tokens[service] = token_response['data']['data']
            
        except Exception as e:
            logger.error(f"Failed to initialize service tokens: {e}")
            raise

    async def _load_graph_configurations(self):
        """Load graph configurations from Consul"""
        try:
            # Graph schema configuration
            schema_config = await self._get_consul_config("schema-config", {
                'auto_schema': False,
                'schema_default': 'none',
                'schema_constraints': True,
                'vertex_labels': [
                    'User', 'Asset', 'Project', 'Model', 'Dataset',
                    'Simulation', 'Task', 'Credential', 'Community'
                ],
                'edge_labels': [
                    'created', 'owns', 'derived_from', 'used_in',
                    'member_of', 'trusts', 'verified_by', 'parent_of'
                ],
                'indexes': {
                    'composite': ['by_user_id', 'by_asset_id', 'by_project_id'],
                    'mixed': ['by_timestamp', 'by_type'],
                    'vertex_centric': ['by_edge_timestamp']
                }
            })
            
            # Analytics configuration
            analytics_config = await self._get_consul_config("analytics-config", {
                'max_traversal_depth': 10,
                'timeout_ms': 30000,
                'result_iteration_batch_size': 64,
                'algorithms': {
                    'pagerank': {
                        'damping_factor': 0.85,
                        'iterations': 30
                    },
                    'community_detection': {
                        'algorithm': 'label_propagation',
                        'max_iterations': 10
                    },
                    'centrality': {
                        'types': ['degree', 'betweenness', 'closeness', 'eigenvector']
                    }
                }
            })
            
            # Trust network configuration
            trust_config = await self._get_consul_config("trust-config", {
                'initial_trust_score': 0.5,
                'trust_decay_factor': 0.95,
                'trust_propagation_depth': 3,
                'min_trust_threshold': 0.1,
                'verification_weight': 0.2,
                'interaction_weight': 0.8
            })
            
            # Performance configuration
            performance_config = await self._get_consul_config("performance-config", {
                'query_cache_enabled': True,
                'query_cache_size': 1000,
                'query_cache_ttl': 300,
                'batch_loading': True,
                'batch_size': 1000,
                'parallel_edge_loading': True
            })
            
            self._graph_config = {
                'schema': schema_config,
                'analytics': analytics_config,
                'trust': trust_config,
                'performance': performance_config
            }
            
        except Exception as e:
            logger.error(f"Failed to load graph configurations: {e}")
            raise

    async def get_janusgraph_config(self) -> Dict[str, Any]:
        """Get JanusGraph configuration with credentials"""
        config = {
            'graph.graphname': 'platformq',
            'storage.backend': self._janusgraph_credentials['storage_backend'],
            'storage.hostname': self._janusgraph_credentials['storage_hostname'],
            'storage.port': self._janusgraph_credentials['storage_port'],
            'storage.username': self._janusgraph_credentials['storage_username'],
            'storage.password': self._janusgraph_credentials['storage_password'],
            'index.search.backend': self._janusgraph_credentials['index_backend'],
            'index.search.hostname': self._janusgraph_credentials['index_hostname'],
            'index.search.port': self._janusgraph_credentials['index_port'],
            'index.search.elasticsearch.client-only': True,
            'index.search.elasticsearch.http.auth.type': 'basic',
            'index.search.elasticsearch.http.auth.basic.username': self._janusgraph_credentials['index_username'],
            'index.search.elasticsearch.http.auth.basic.password': self._janusgraph_credentials['index_password']
        }
        
        # Add cache configuration
        if 'cache' in self._janusgraph_credentials:
            config.update({
                'cache.db-cache': True,
                'cache.db-cache-clean-wait': 20,
                'cache.db-cache-time': self._janusgraph_credentials['cache']['cache_time'],
                'cache.db-cache-size': self._janusgraph_credentials['cache']['cache_size']
            })
        
        return config

    async def get_gremlin_connection_string(self) -> str:
        """Get Gremlin server connection string"""
        creds = self._gremlin_credentials
        protocol = 'wss' if creds.get('ssl_enabled', False) else 'ws'
        
        if creds.get('username'):
            return f"{protocol}://{creds['username']}:{creds['password']}@{creds['host']}:{creds['port']}/gremlin"
        else:
            return f"{protocol}://{creds['host']}:{creds['port']}/gremlin"

    async def get_service_token(self, service_name: str) -> Optional[str]:
        """Get authentication token for service-to-service communication"""
        if service_name in self._service_tokens:
            token_data = self._service_tokens[service_name]
            
            # Check if token is expired
            expires_at = datetime.fromisoformat(token_data['expires_at'])
            if datetime.utcnow() > expires_at:
                # Refresh token
                await self._refresh_service_token(service_name)
            
            return token_data['token']
        
        return None

    async def sign_trust_score(self, trust_data: Dict[str, Any]) -> str:
        """Sign trust score data"""
        try:
            # Serialize trust data
            data_bytes = json.dumps(trust_data, sort_keys=True).encode()
            data_b64 = base64.b64encode(data_bytes).decode()
            
            # Sign using Transit engine
            response = self.vault_client.write(
                f"{self.vault_transit_path}/sign/{self._trust_keys['trust_score']}",
                input=data_b64,
                signature_algorithm="pss"
            )
            
            return response['data']['signature']
            
        except Exception as e:
            logger.error(f"Failed to sign trust score: {e}")
            raise

    async def verify_trust_score(self, trust_data: Dict[str, Any], signature: str) -> bool:
        """Verify trust score signature"""
        try:
            data_bytes = json.dumps(trust_data, sort_keys=True).encode()
            data_b64 = base64.b64encode(data_bytes).decode()
            
            response = self.vault_client.write(
                f"{self.vault_transit_path}/verify/{self._trust_keys['trust_score']}",
                input=data_b64,
                signature=signature,
                signature_algorithm="pss"
            )
            
            return response['data']['valid']
            
        except Exception as e:
            logger.error(f"Failed to verify trust score: {e}")
            return False

    async def sign_lineage_attestation(self, lineage_data: bytes) -> str:
        """Sign lineage attestation"""
        try:
            data_b64 = base64.b64encode(lineage_data).decode()
            
            response = self.vault_client.write(
                f"{self.vault_transit_path}/sign/{self._trust_keys['lineage']}",
                input=data_b64
            )
            
            return response['data']['signature']
            
        except Exception as e:
            logger.error(f"Failed to sign lineage attestation: {e}")
            raise

    async def acquire_graph_processing_lock(self, job_id: str, job_type: str, ttl: int = 3600) -> bool:
        """Acquire distributed lock for graph processing job"""
        try:
            lock_key = f"{self.consul_kv_prefix}/locks/{job_type}/{job_id}"
            session_id = await self._create_consul_session(ttl)
            
            # Try to acquire lock
            success = await self.consul_client.kv.put(
                lock_key,
                json.dumps({
                    'holder': self.service_name,
                    'job_type': job_type,
                    'acquired_at': datetime.utcnow().isoformat(),
                    'session_id': session_id
                }),
                acquire=session_id
            )
            
            if success:
                self._processing_locks[job_id] = {
                    'session_id': session_id,
                    'lock_key': lock_key,
                    'job_type': job_type
                }
                
                # Start lock refresh task
                self._lock_refresh_tasks[job_id] = asyncio.create_task(
                    self._refresh_lock_periodically(job_id, session_id, ttl)
                )
            
            return success
            
        except Exception as e:
            logger.error(f"Failed to acquire graph processing lock: {e}")
            return False

    async def release_graph_processing_lock(self, job_id: str):
        """Release distributed lock for graph processing job"""
        try:
            if job_id in self._processing_locks:
                lock_data = self._processing_locks[job_id]
                
                # Cancel refresh task
                if job_id in self._lock_refresh_tasks:
                    self._lock_refresh_tasks[job_id].cancel()
                    del self._lock_refresh_tasks[job_id]
                
                # Release lock
                await self.consul_client.kv.delete(
                    lock_data['lock_key'],
                    recurse=False
                )
                
                # Destroy session
                await self.consul_client.session.destroy(lock_data['session_id'])
                
                del self._processing_locks[job_id]
                
        except Exception as e:
            logger.error(f"Failed to release graph processing lock: {e}")

    async def get_graph_config(self, config_type: str = 'schema') -> Dict[str, Any]:
        """Get graph configuration"""
        return self._graph_config.get(config_type, {})

    async def update_analytics_config(self, config: Dict[str, Any]):
        """Update analytics configuration in Consul"""
        try:
            await self.consul_client.kv.put(
                f"{self.consul_kv_prefix}/analytics-config",
                json.dumps(config)
            )
            self._graph_config['analytics'] = config
            
            # Fire event for config update
            await self.consul_client.event.fire(
                "graph-analytics-config-update",
                json.dumps({
                    'timestamp': datetime.utcnow().isoformat(),
                    'service': self.service_name
                })
            )
            
        except Exception as e:
            logger.error(f"Failed to update analytics config: {e}")
            raise

    async def get_graph_metrics(self) -> Dict[str, Any]:
        """Get graph processing metrics from Consul"""
        try:
            metrics_key = f"{self.consul_kv_prefix}/metrics"
            _, metrics_data = await self.consul_client.kv.get(metrics_key)
            
            if metrics_data:
                return json.loads(metrics_data['Value'])
            
            return {
                'total_vertices': 0,
                'total_edges': 0,
                'active_queries': 0,
                'average_query_time': 0,
                'cache_hit_rate': 0
            }
            
        except Exception as e:
            logger.error(f"Failed to get graph metrics: {e}")
            return {}

    async def rotate_janusgraph_credentials(self):
        """Rotate JanusGraph credentials"""
        try:
            logger.info("Rotating JanusGraph credentials")
            
            # Generate new passwords
            new_storage_password = self._generate_secure_password()
            new_index_password = self._generate_secure_password()
            
            # Update passwords in JanusGraph (would require admin API)
            # This is a placeholder - actual implementation would use JanusGraph admin API
            
            # Update in Vault
            self._janusgraph_credentials['storage_password'] = new_storage_password
            self._janusgraph_credentials['index_password'] = new_index_password
            
            self.vault_client.write(
                f"{self.vault_graph_path}/janusgraph",
                **self._janusgraph_credentials
            )
            
            # Notify services
            await self.consul_client.event.fire(
                "janusgraph-credential-rotation",
                json.dumps({
                    'timestamp': datetime.utcnow().isoformat(),
                    'service': self.service_name
                })
            )
            
        except Exception as e:
            logger.error(f"Failed to rotate JanusGraph credentials: {e}")
            raise

    # Helper methods
    async def _refresh_service_token(self, service_name: str):
        """Refresh expired service token"""
        if service_name in self._service_tokens:
            token_data = self._service_tokens[service_name]
            
            # Use refresh token to get new access token
            new_token = {
                'token': self._generate_api_key(64),
                'refresh_token': token_data['refresh_token'],
                'expires_at': (datetime.utcnow() + timedelta(days=30)).isoformat()
            }
            
            self.vault_client.write(
                f"{self.vault_graph_path}/tokens/{service_name}",
                **new_token
            )
            
            self._service_tokens[service_name] = new_token

    async def _create_consul_session(self, ttl: int) -> str:
        """Create Consul session for distributed locking"""
        response = await self.consul_client.session.create(
            name=f"{self.service_name}-lock",
            ttl=ttl,
            behavior='delete'
        )
        return response['ID']

    async def _refresh_lock_periodically(self, job_id: str, session_id: str, ttl: int):
        """Periodically refresh lock to prevent expiration"""
        refresh_interval = ttl // 3  # Refresh at 1/3 of TTL
        
        while job_id in self._processing_locks:
            try:
                await asyncio.sleep(refresh_interval)
                await self.consul_client.session.renew(session_id)
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Failed to refresh lock for job {job_id}: {e}")
                break

    async def _get_consul_config(self, key: str, default: Any = None) -> Any:
        """Get configuration from Consul KV"""
        try:
            _, data = await self.consul_client.kv.get(f"{self.consul_kv_prefix}/{key}")
            if data:
                return json.loads(data['Value'])
            elif default is not None:
                # Store default in Consul
                await self.consul_client.kv.put(
                    f"{self.consul_kv_prefix}/{key}",
                    json.dumps(default)
                )
                return default
            return {}
        except Exception as e:
            logger.error(f"Failed to get Consul config {key}: {e}")
            return default or {}

    def _generate_api_key(self, length: int = 32) -> str:
        """Generate a secure API key"""
        import secrets
        return secrets.token_urlsafe(length)

    def _generate_secure_password(self, length: int = 24) -> str:
        """Generate a secure password"""
        import secrets
        import string
        alphabet = string.ascii_letters + string.digits + string.punctuation
        return ''.join(secrets.choice(alphabet) for _ in range(length))

    async def _start_maintenance_tasks(self):
        """Start maintenance and rotation tasks"""
        # Rotate JanusGraph credentials every 90 days
        self._rotation_tasks['janusgraph'] = asyncio.create_task(
            self._rotate_periodically(
                self.rotate_janusgraph_credentials,
                interval=7776000  # 90 days
            )
        )
        
        # Refresh service tokens
        self._rotation_tasks['tokens'] = asyncio.create_task(
            self._refresh_tokens_periodically()
        )

    async def _rotate_periodically(self, rotation_func, interval: int):
        """Periodically rotate credentials"""
        while True:
            await asyncio.sleep(interval)
            try:
                await rotation_func()
            except Exception as e:
                logger.error(f"Rotation failed: {e}")

    async def _refresh_tokens_periodically(self):
        """Refresh service tokens before expiration"""
        while True:
            await asyncio.sleep(86400)  # Check daily
            try:
                for service, token_data in self._service_tokens.items():
                    expires_at = datetime.fromisoformat(token_data['expires_at'])
                    if expires_at - datetime.utcnow() < timedelta(days=7):
                        await self._refresh_service_token(service)
            except Exception as e:
                logger.error(f"Token refresh failed: {e}")

    async def close(self):
        """Cleanup resources"""
        # Release all active locks
        for job_id in list(self._processing_locks.keys()):
            await self.release_graph_processing_lock(job_id)
        
        # Cancel all tasks
        for task in self._rotation_tasks.values():
            task.cancel()
        
        for task in self._lock_refresh_tasks.values():
            task.cancel()
        
        # Deregister from Consul
        service_id = f"{self.service_name}-{os.environ.get('HOSTNAME', 'local')}"
        await self.consul_client.agent.service.deregister(service_id)
        
        # Close Consul client
        await self.consul_client.close() 