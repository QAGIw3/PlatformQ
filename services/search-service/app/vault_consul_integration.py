"""
Vault and Consul Integration for Search Service

Manages:
- Elasticsearch credentials and certificates
- Milvus vector database credentials
- Search API keys and rate limiting
- Encryption keys for sensitive data
- Graph intelligence service credentials
- OpenAI API keys for embeddings
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
import ssl
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.asymmetric import rsa
from cryptography.hazmat.backends import default_backend
from cryptography import x509
from cryptography.x509.oid import NameOID

logger = logging.getLogger(__name__)


class VaultConsulIntegration:
    """Handles Vault and Consul integration for Search service"""
    
    def __init__(self):
        # Vault configuration
        self.vault_addr = os.environ.get('VAULT_ADDR', 'http://vault:8200')
        self.vault_token = os.environ.get('VAULT_TOKEN')
        self.service_name = os.environ.get('SERVICE_NAME', 'search-service')
        
        # Consul configuration
        self.consul_host = os.environ.get('CONSUL_HOST', 'consul')
        self.consul_port = int(os.environ.get('CONSUL_PORT', '8500'))
        
        # Service paths
        self.vault_search_path = f"secret/data/search/{self.service_name}"
        self.vault_transit_path = "transit"
        self.vault_pki_path = "pki"
        self.consul_kv_prefix = f"search/{self.service_name}"
        
        # Clients
        self.vault_client = None
        self.consul_client = None
        
        # Cached credentials and configs
        self._es_credentials = {}
        self._milvus_credentials = {}
        self._api_keys = {}
        self._encryption_keys = {}
        self._external_api_keys = {}
        self._search_config = {}
        
        # Rotation tracking
        self._rotation_tasks = {}
        self._lease_renewal_tasks = {}
        self._rate_limit_counters = {}

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
            
            # Initialize credentials and configurations
            await self._initialize_elasticsearch_credentials()
            await self._initialize_milvus_credentials()
            await self._initialize_api_keys()
            await self._initialize_encryption_keys()
            await self._initialize_external_api_keys()
            
            # Load search configurations from Consul
            await self._load_search_configurations()
            
            # Start rotation tasks
            await self._start_rotation_tasks()
            
            logger.info("Search service Vault and Consul integration initialized successfully")
            
        except Exception as e:
            logger.error(f"Failed to initialize Vault/Consul integration: {e}")
            raise

    async def _register_service(self):
        """Register search service with Consul"""
        service_id = f"{self.service_name}-{os.environ.get('HOSTNAME', 'local')}"
        
        await self.consul_client.agent.service.register(
            name=self.service_name,
            service_id=service_id,
            address=os.environ.get('SERVICE_HOST', 'localhost'),
            port=int(os.environ.get('SERVICE_PORT', '8000')),
            tags=[
                "search",
                "elasticsearch",
                "vector-search",
                "graph-enhanced",
                "ml-powered"
            ],
            check=consul.Check.http(
                f"http://{os.environ.get('SERVICE_HOST', 'localhost')}:{os.environ.get('SERVICE_PORT', '8000')}/health",
                interval="10s",
                timeout="5s",
                deregister_critical_service_after="30s"
            )
        )

    async def _initialize_elasticsearch_credentials(self):
        """Initialize Elasticsearch credentials and certificates"""
        try:
            # Get or create Elasticsearch credentials
            es_response = self.vault_client.read(f"{self.vault_search_path}/elasticsearch")
            if not es_response:
                # Generate new credentials
                es_creds = {
                    'username': 'search_service',
                    'password': self._generate_secure_password(),
                    'api_key': self._generate_api_key(64)
                }
                self.vault_client.write(f"{self.vault_search_path}/elasticsearch", **es_creds)
                self._es_credentials = es_creds
            else:
                self._es_credentials = es_response['data']['data']
            
            # Get or create Elasticsearch SSL certificates
            await self._setup_elasticsearch_ssl()
            
            # Configure Elasticsearch security features
            await self._configure_es_security()
            
        except Exception as e:
            logger.error(f"Failed to initialize Elasticsearch credentials: {e}")
            raise

    async def _initialize_milvus_credentials(self):
        """Initialize Milvus vector database credentials"""
        try:
            # Get or create Milvus credentials
            milvus_response = self.vault_client.read(f"{self.vault_search_path}/milvus")
            if not milvus_response:
                # Generate new credentials
                milvus_creds = {
                    'username': 'search_service',
                    'password': self._generate_secure_password(),
                    'token': self._generate_api_key(32)
                }
                self.vault_client.write(f"{self.vault_search_path}/milvus", **milvus_creds)
                self._milvus_credentials = milvus_creds
            else:
                self._milvus_credentials = milvus_response['data']['data']
            
            # Configure Milvus collection settings
            await self._configure_milvus_collections()
            
        except Exception as e:
            logger.error(f"Failed to initialize Milvus credentials: {e}")
            raise

    async def _initialize_api_keys(self):
        """Initialize search API keys with rate limiting"""
        try:
            # Internal API keys for service-to-service communication
            internal_keys = {
                'admin': self._generate_api_key(64),
                'service': self._generate_api_key(32),
                'readonly': self._generate_api_key(32)
            }
            
            # External API keys for client applications
            external_keys = await self._generate_client_api_keys()
            
            self._api_keys = {
                'internal': internal_keys,
                'external': external_keys
            }
            
            # Store in Vault
            self.vault_client.write(
                f"{self.vault_search_path}/api-keys",
                **self._api_keys
            )
            
            # Configure rate limiting in Consul
            await self._configure_rate_limiting()
            
        except Exception as e:
            logger.error(f"Failed to initialize API keys: {e}")
            raise

    async def _initialize_encryption_keys(self):
        """Initialize encryption keys for sensitive search data"""
        try:
            # Create or get search query encryption key
            try:
                self.vault_client.read(f"{self.vault_transit_path}/keys/search-queries")
            except:
                self.vault_client.write(
                    f"{self.vault_transit_path}/keys/search-queries",
                    type="aes256-gcm96",
                    derived=False,
                    exportable=False
                )
            
            # Create or get search results encryption key
            try:
                self.vault_client.read(f"{self.vault_transit_path}/keys/search-results")
            except:
                self.vault_client.write(
                    f"{self.vault_transit_path}/keys/search-results",
                    type="aes256-gcm96",
                    derived=True,
                    exportable=False
                )
            
            # Create or get PII encryption key
            try:
                self.vault_client.read(f"{self.vault_transit_path}/keys/search-pii")
            except:
                self.vault_client.write(
                    f"{self.vault_transit_path}/keys/search-pii",
                    type="aes256-gcm96",
                    derived=True,
                    exportable=False
                )
            
            self._encryption_keys = {
                'queries': 'search-queries',
                'results': 'search-results',
                'pii': 'search-pii'
            }
            
        except Exception as e:
            logger.error(f"Failed to initialize encryption keys: {e}")
            raise

    async def _initialize_external_api_keys(self):
        """Initialize external API keys (OpenAI, etc.)"""
        try:
            # Get or create OpenAI API key for embeddings
            openai_response = self.vault_client.read(f"{self.vault_search_path}/openai")
            if not openai_response:
                # This would be manually set in production
                openai_key = os.environ.get('OPENAI_API_KEY', '')
                if openai_key:
                    self.vault_client.write(
                        f"{self.vault_search_path}/openai",
                        api_key=openai_key
                    )
                    self._external_api_keys['openai'] = openai_key
            else:
                self._external_api_keys['openai'] = openai_response['data']['data']['api_key']
            
            # Other external APIs can be added here
            
        except Exception as e:
            logger.error(f"Failed to initialize external API keys: {e}")
            raise

    async def _load_search_configurations(self):
        """Load search configurations from Consul"""
        try:
            # Search relevance configuration
            relevance_config = await self._get_consul_config("relevance-config", {
                'boost_factors': {
                    'title': 2.0,
                    'description': 1.5,
                    'tags': 1.2,
                    'content': 1.0
                },
                'fuzzy_matching': {
                    'enabled': True,
                    'fuzziness': 'AUTO',
                    'prefix_length': 2
                },
                'synonym_expansion': {
                    'enabled': True,
                    'synonym_file': 'synonyms.txt'
                }
            })
            
            # Vector search configuration
            vector_config = await self._get_consul_config("vector-config", {
                'embedding_model': 'text-embedding-ada-002',
                'embedding_dimensions': 1536,
                'similarity_metric': 'cosine',
                'top_k': 20,
                'score_threshold': 0.7
            })
            
            # Index configuration
            index_config = await self._get_consul_config("index-config", {
                'shards': 5,
                'replicas': 2,
                'refresh_interval': '1s',
                'max_result_window': 10000,
                'search_timeout': '30s'
            })
            
            # Facet configuration
            facet_config = await self._get_consul_config("facet-config", {
                'enabled_facets': [
                    'asset_type',
                    'tags',
                    'creator',
                    'date_range',
                    'status'
                ],
                'max_facet_values': 100,
                'facet_min_count': 1
            })
            
            self._search_config = {
                'relevance': relevance_config,
                'vector': vector_config,
                'index': index_config,
                'facets': facet_config
            }
            
        except Exception as e:
            logger.error(f"Failed to load search configurations: {e}")
            raise

    async def get_elasticsearch_config(self) -> Dict[str, Any]:
        """Get Elasticsearch configuration with credentials"""
        return {
            'hosts': [os.environ.get('ELASTICSEARCH_URL', 'https://elasticsearch:9200')],
            'http_auth': (self._es_credentials['username'], self._es_credentials['password']),
            'api_key': self._es_credentials.get('api_key'),
            'use_ssl': True,
            'verify_certs': True,
            'ssl_context': await self._get_es_ssl_context(),
            'timeout': 30,
            'max_retries': 3,
            'retry_on_timeout': True
        }

    async def get_milvus_config(self) -> Dict[str, Any]:
        """Get Milvus configuration with credentials"""
        return {
            'host': os.environ.get('MILVUS_HOST', 'milvus'),
            'port': int(os.environ.get('MILVUS_PORT', '19530')),
            'user': self._milvus_credentials['username'],
            'password': self._milvus_credentials['password'],
            'token': self._milvus_credentials.get('token'),
            'secure': True
        }

    async def get_search_config(self) -> Dict[str, Any]:
        """Get search configuration from Consul"""
        return self._search_config

    async def validate_api_key(self, api_key: str, required_scope: str = "read") -> Tuple[bool, Optional[Dict[str, Any]]]:
        """Validate API key and check rate limits"""
        try:
            # Check internal API keys
            for key_type, key in self._api_keys['internal'].items():
                if api_key == key:
                    # Check if key has required scope
                    key_scopes = {
                        'admin': ['read', 'write', 'admin'],
                        'service': ['read', 'write'],
                        'readonly': ['read']
                    }
                    
                    if required_scope in key_scopes.get(key_type, []):
                        return True, {'type': 'internal', 'role': key_type}
                    else:
                        return False, None
            
            # Check external API keys
            for client_id, client_data in self._api_keys['external'].items():
                if api_key == client_data['key']:
                    # Check rate limits
                    if await self._check_rate_limit(client_id, client_data['rate_limit']):
                        return True, {'type': 'external', 'client_id': client_id}
                    else:
                        return False, None
            
            return False, None
            
        except Exception as e:
            logger.error(f"Failed to validate API key: {e}")
            return False, None

    async def encrypt_search_query(self, query: str) -> str:
        """Encrypt search query for audit logging"""
        try:
            encoded_query = base64.b64encode(query.encode()).decode('utf-8')
            
            response = self.vault_client.write(
                f"{self.vault_transit_path}/encrypt/{self._encryption_keys['queries']}",
                plaintext=encoded_query
            )
            
            return response['data']['ciphertext']
            
        except Exception as e:
            logger.error(f"Failed to encrypt search query: {e}")
            raise

    async def encrypt_search_results(self, results: Dict[str, Any], context: str = "") -> str:
        """Encrypt sensitive search results"""
        try:
            result_bytes = json.dumps(results).encode()
            encoded_data = base64.b64encode(result_bytes).decode('utf-8')
            
            response = self.vault_client.write(
                f"{self.vault_transit_path}/encrypt/{self._encryption_keys['results']}",
                plaintext=encoded_data,
                context=base64.b64encode(context.encode()).decode('utf-8') if context else None
            )
            
            return response['data']['ciphertext']
            
        except Exception as e:
            logger.error(f"Failed to encrypt search results: {e}")
            raise

    async def encrypt_pii(self, pii_data: str) -> str:
        """Encrypt PII data in search results"""
        try:
            encoded_data = base64.b64encode(pii_data.encode()).decode('utf-8')
            
            response = self.vault_client.write(
                f"{self.vault_transit_path}/encrypt/{self._encryption_keys['pii']}",
                plaintext=encoded_data
            )
            
            return response['data']['ciphertext']
            
        except Exception as e:
            logger.error(f"Failed to encrypt PII: {e}")
            raise

    async def get_openai_api_key(self) -> Optional[str]:
        """Get OpenAI API key for embeddings"""
        return self._external_api_keys.get('openai')

    async def update_search_relevance_config(self, config: Dict[str, Any]):
        """Update search relevance configuration in Consul"""
        try:
            await self.consul_client.kv.put(
                f"{self.consul_kv_prefix}/relevance-config",
                json.dumps(config)
            )
            self._search_config['relevance'] = config
            
            # Notify search engines of config change
            await self.consul_client.event.fire(
                "search-config-update",
                json.dumps({
                    'type': 'relevance',
                    'timestamp': datetime.utcnow().isoformat()
                })
            )
            
        except Exception as e:
            logger.error(f"Failed to update relevance config: {e}")
            raise

    async def rotate_elasticsearch_credentials(self):
        """Rotate Elasticsearch credentials"""
        try:
            logger.info("Rotating Elasticsearch credentials")
            
            # Generate new credentials
            new_creds = {
                'username': self._es_credentials['username'],  # Keep username
                'password': self._generate_secure_password(),
                'api_key': self._generate_api_key(64)
            }
            
            # Update in Elasticsearch (would require admin API)
            # This is a placeholder - actual implementation would use ES API
            
            # Store new credentials in Vault
            self.vault_client.write(f"{self.vault_search_path}/elasticsearch", **new_creds)
            
            # Update local cache
            self._es_credentials = new_creds
            
            # Notify services via Consul event
            await self.consul_client.event.fire(
                "elasticsearch-credential-rotation",
                json.dumps({
                    'timestamp': datetime.utcnow().isoformat(),
                    'service': self.service_name
                })
            )
            
        except Exception as e:
            logger.error(f"Failed to rotate Elasticsearch credentials: {e}")
            raise

    async def get_search_index_settings(self, index_type: str) -> Dict[str, Any]:
        """Get index-specific settings from Consul"""
        try:
            index_key = f"{self.consul_kv_prefix}/indices/{index_type}"
            _, data = await self.consul_client.kv.get(index_key)
            
            if data:
                return json.loads(data['Value'])
            
            # Return defaults
            defaults = {
                'assets': {
                    'number_of_shards': 5,
                    'number_of_replicas': 2,
                    'refresh_interval': '1s'
                },
                'simulations': {
                    'number_of_shards': 3,
                    'number_of_replicas': 1,
                    'refresh_interval': '5s'
                },
                'documents': {
                    'number_of_shards': 3,
                    'number_of_replicas': 2,
                    'refresh_interval': '1s'
                }
            }
            
            return defaults.get(index_type, {})
            
        except Exception as e:
            logger.error(f"Failed to get index settings: {e}")
            return {}

    # Helper methods
    async def _setup_elasticsearch_ssl(self):
        """Setup SSL certificates for Elasticsearch"""
        try:
            # Generate or get CA certificate
            ca_cert = await self._get_or_create_ca_cert("elasticsearch-ca")
            
            # Generate client certificate
            client_cert = await self._generate_client_certificate("search-service")
            
            self._es_credentials['certificates'] = {
                'ca_cert': ca_cert,
                'client_cert': client_cert['certificate'],
                'client_key': client_cert['private_key']
            }
            
        except Exception as e:
            logger.error(f"Failed to setup Elasticsearch SSL: {e}")
            raise

    async def _configure_es_security(self):
        """Configure Elasticsearch security features"""
        # Store security configuration in Consul
        security_config = {
            'xpack.security.enabled': True,
            'xpack.security.transport.ssl.enabled': True,
            'xpack.security.http.ssl.enabled': True,
            'xpack.security.authc.api_key.enabled': True,
            'xpack.security.audit.enabled': True
        }
        
        await self.consul_client.kv.put(
            f"{self.consul_kv_prefix}/es-security-config",
            json.dumps(security_config)
        )

    async def _configure_milvus_collections(self):
        """Configure Milvus collection settings"""
        collection_config = {
            'platformq_vectors': {
                'dimension': 1536,
                'index_type': 'IVF_FLAT',
                'metric_type': 'L2',
                'nlist': 1024
            }
        }
        
        await self.consul_client.kv.put(
            f"{self.consul_kv_prefix}/milvus-collections",
            json.dumps(collection_config)
        )

    async def _generate_client_api_keys(self) -> Dict[str, Dict[str, Any]]:
        """Generate API keys for client applications"""
        clients = {}
        
        # Default client applications
        default_clients = [
            {'id': 'web-app', 'rate_limit': 1000},
            {'id': 'mobile-app', 'rate_limit': 500},
            {'id': 'api-client', 'rate_limit': 100}
        ]
        
        for client in default_clients:
            clients[client['id']] = {
                'key': self._generate_api_key(32),
                'rate_limit': client['rate_limit'],
                'created_at': datetime.utcnow().isoformat()
            }
        
        return clients

    async def _configure_rate_limiting(self):
        """Configure API rate limiting in Consul"""
        rate_limit_config = {
            'enabled': True,
            'window_size': 60,  # seconds
            'default_limit': 100,
            'burst_size': 20,
            'client_limits': {
                client_id: data['rate_limit']
                for client_id, data in self._api_keys['external'].items()
            }
        }
        
        await self.consul_client.kv.put(
            f"{self.consul_kv_prefix}/rate-limits",
            json.dumps(rate_limit_config)
        )

    async def _check_rate_limit(self, client_id: str, limit: int) -> bool:
        """Check if client has exceeded rate limit"""
        # Simple in-memory rate limiting - in production use Redis
        current_time = datetime.utcnow()
        window_start = current_time - timedelta(seconds=60)
        
        if client_id not in self._rate_limit_counters:
            self._rate_limit_counters[client_id] = []
        
        # Clean old entries
        self._rate_limit_counters[client_id] = [
            timestamp for timestamp in self._rate_limit_counters[client_id]
            if timestamp > window_start
        ]
        
        # Check limit
        if len(self._rate_limit_counters[client_id]) >= limit:
            return False
        
        # Add current request
        self._rate_limit_counters[client_id].append(current_time)
        return True

    async def _get_es_ssl_context(self) -> ssl.SSLContext:
        """Create SSL context for Elasticsearch"""
        context = ssl.create_default_context()
        
        if 'certificates' in self._es_credentials:
            # Add CA certificate
            context.load_verify_locations(
                cadata=self._es_credentials['certificates']['ca_cert']
            )
            
            # Add client certificate
            # In production, these would be written to temp files
            # context.load_cert_chain(certfile, keyfile)
        
        return context

    async def _get_or_create_ca_cert(self, ca_name: str) -> str:
        """Get or create CA certificate"""
        # This would integrate with Vault PKI backend
        return "ca-certificate-pem"

    async def _generate_client_certificate(self, common_name: str) -> Dict[str, str]:
        """Generate client certificate"""
        # This would integrate with Vault PKI backend
        return {
            'certificate': 'client-certificate-pem',
            'private_key': 'client-private-key-pem'
        }

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

    async def _start_rotation_tasks(self):
        """Start credential rotation tasks"""
        # Rotate Elasticsearch credentials every 30 days
        self._rotation_tasks['elasticsearch'] = asyncio.create_task(
            self._rotate_periodically(
                self.rotate_elasticsearch_credentials,
                interval=2592000  # 30 days
            )
        )
        
        # Rotate API keys every 90 days
        self._rotation_tasks['api_keys'] = asyncio.create_task(
            self._rotate_periodically(
                self._rotate_api_keys,
                interval=7776000  # 90 days
            )
        )

    async def _rotate_api_keys(self):
        """Rotate API keys"""
        logger.info("Rotating API keys")
        # Implementation for API key rotation

    async def _rotate_periodically(self, rotation_func, interval: int):
        """Periodically rotate credentials"""
        while True:
            await asyncio.sleep(interval)
            try:
                await rotation_func()
            except Exception as e:
                logger.error(f"Rotation failed: {e}")

    async def close(self):
        """Cleanup resources"""
        # Cancel rotation tasks
        for task in self._rotation_tasks.values():
            task.cancel()
        
        # Deregister from Consul
        service_id = f"{self.service_name}-{os.environ.get('HOSTNAME', 'local')}"
        await self.consul_client.agent.service.deregister(service_id)
        
        # Close Consul client
        await self.consul_client.close() 