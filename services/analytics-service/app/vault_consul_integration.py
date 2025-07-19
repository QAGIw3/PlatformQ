"""
Vault and Consul Integration for Analytics Service

Manages secure configuration, credentials, and service discovery for analytics infrastructure.
"""

import asyncio
import logging
from typing import Dict, Any, Optional, List
from datetime import datetime, timedelta
import json
import os

import hvac
import consul.aio
from platformq_shared.vault_consul_base import VaultConsulBase

logger = logging.getLogger(__name__)


class VaultConsulIntegration(VaultConsulBase):
    """
    Analytics service specific Vault and Consul integration.
    
    Features:
    - Trino/Presto credentials management
    - Data lake access credentials (MinIO, S3)
    - Apache Ignite credentials
    - Elasticsearch credentials for analytics
    - Encryption keys for sensitive data
    - Query result caching configuration
    - Distributed job coordination
    - Data pipeline secrets
    """
    
    def __init__(self, config: Dict[str, Any]):
        super().__init__(
            vault_addr=config["vault_addr"],
            vault_token=config.get("vault_token"),
            consul_addr=config["consul_addr"],
            service_name="analytics-service"
        )
        
        self.analytics_config = {}
        self._credential_cache = {}
        self._config_watchers = {}
        
    async def initialize(self):
        """Initialize analytics-specific Vault and Consul features"""
        await super().initialize()
        
        logger.info("Initializing analytics Vault/Consul integration")
        
        # Enable analytics-specific secret engines
        await self._setup_analytics_secrets()
        
        # Load analytics configuration
        await self._load_analytics_config()
        
        # Setup credential rotation
        await self._setup_credential_rotation()
        
        # Watch for configuration changes
        await self._setup_config_watchers()
        
        logger.info("Analytics Vault/Consul integration initialized")
        
    async def _setup_analytics_secrets(self):
        """Setup analytics-specific secret engines"""
        try:
            # Create analytics KV paths
            paths = [
                "secret/analytics/trino",
                "secret/analytics/data-lake",
                "secret/analytics/ignite",
                "secret/analytics/elasticsearch",
                "secret/analytics/encryption",
                "secret/analytics/pipelines"
            ]
            
            for path in paths:
                try:
                    self.vault.write(f"{path}/config", initialized=True)
                except Exception:
                    pass  # Path might already exist
                    
        except Exception as e:
            logger.error(f"Failed to setup analytics secrets: {e}")
            
    async def get_trino_credentials(self) -> Dict[str, Any]:
        """Get Trino/Presto credentials with automatic rotation"""
        cache_key = "trino_credentials"
        
        # Check cache
        if cache_key in self._credential_cache:
            cached = self._credential_cache[cache_key]
            if datetime.utcnow() < cached["expires"]:
                return cached["credentials"]
                
        try:
            # Get base credentials
            response = self.vault.read("secret/analytics/trino/credentials")
            if not response or "data" not in response:
                # Generate new credentials
                credentials = await self._generate_trino_credentials()
            else:
                credentials = response["data"]["data"]
                
            # Cache with expiry
            self._credential_cache[cache_key] = {
                "credentials": credentials,
                "expires": datetime.utcnow() + timedelta(minutes=30)
            }
            
            return credentials
            
        except Exception as e:
            logger.error(f"Failed to get Trino credentials: {e}")
            raise
            
    async def get_data_lake_credentials(self, lake_type: str = "minio") -> Dict[str, Any]:
        """Get data lake access credentials"""
        cache_key = f"data_lake_{lake_type}"
        
        # Check cache
        if cache_key in self._credential_cache:
            cached = self._credential_cache[cache_key]
            if datetime.utcnow() < cached["expires"]:
                return cached["credentials"]
                
        try:
            # Get credentials based on lake type
            if lake_type == "minio":
                response = self.vault.read("secret/analytics/data-lake/minio")
            elif lake_type == "s3":
                # Use dynamic AWS credentials if configured
                response = self.vault.read("aws/creds/analytics-s3-role")
            else:
                raise ValueError(f"Unsupported lake type: {lake_type}")
                
            if response and "data" in response:
                credentials = response["data"]
                
                # For dynamic credentials, extract the right fields
                if lake_type == "s3" and "access_key" in credentials:
                    credentials = {
                        "access_key": credentials["access_key"],
                        "secret_key": credentials["secret_key"],
                        "session_token": credentials.get("security_token"),
                        "expiration": credentials.get("expiration")
                    }
                else:
                    credentials = credentials.get("data", credentials)
                    
                # Cache with appropriate expiry
                expiry = timedelta(hours=1)
                if lake_type == "s3" and "expiration" in credentials:
                    # Use AWS credential expiration time
                    expiry = datetime.fromisoformat(credentials["expiration"]) - datetime.utcnow()
                    
                self._credential_cache[cache_key] = {
                    "credentials": credentials,
                    "expires": datetime.utcnow() + expiry
                }
                
                return credentials
            else:
                # Generate credentials for MinIO
                if lake_type == "minio":
                    return await self._generate_minio_credentials()
                raise Exception("No credentials found")
                
        except Exception as e:
            logger.error(f"Failed to get data lake credentials: {e}")
            raise
            
    async def get_ignite_credentials(self) -> Dict[str, str]:
        """Get Apache Ignite credentials"""
        try:
            response = self.vault.read("secret/analytics/ignite/credentials")
            if response and "data" in response:
                return response["data"]["data"]
                
            # Generate if not exists
            return await self._generate_ignite_credentials()
            
        except Exception as e:
            logger.error(f"Failed to get Ignite credentials: {e}")
            raise
            
    async def get_elasticsearch_credentials(self) -> Dict[str, str]:
        """Get Elasticsearch credentials for analytics"""
        try:
            # Try to get dynamic credentials first
            response = self.vault.read("database/creds/elasticsearch-analytics")
            
            if response and "data" in response:
                return {
                    "username": response["data"]["username"],
                    "password": response["data"]["password"],
                    "hosts": await self._get_elasticsearch_hosts()
                }
                
            # Fallback to static credentials
            response = self.vault.read("secret/analytics/elasticsearch/credentials")
            if response and "data" in response:
                return response["data"]["data"]
                
            raise Exception("No Elasticsearch credentials found")
            
        except Exception as e:
            logger.error(f"Failed to get Elasticsearch credentials: {e}")
            raise
            
    async def get_encryption_key(self, key_name: str) -> bytes:
        """Get encryption key for analytics data"""
        try:
            # Use Transit engine for encryption keys
            response = self.vault.read(f"transit/keys/{key_name}")
            
            if not response:
                # Create key if not exists
                self.vault.write(
                    f"transit/keys/{key_name}",
                    type="aes256-gcm96",
                    exportable=False,
                    allow_plaintext_backup=False
                )
                
            # Get data key for encryption
            response = self.vault.write(
                f"transit/datakey/plaintext/{key_name}",
                context=base64.b64encode(b"analytics").decode()
            )
            
            if response and "data" in response:
                return base64.b64decode(response["data"]["plaintext"])
                
            raise Exception("Failed to get encryption key")
            
        except Exception as e:
            logger.error(f"Failed to get encryption key: {e}")
            raise
            
    async def encrypt_data(self, data: bytes, key_name: str) -> str:
        """Encrypt data using Transit engine"""
        try:
            response = self.vault.write(
                f"transit/encrypt/{key_name}",
                plaintext=base64.b64encode(data).decode(),
                context=base64.b64encode(b"analytics").decode()
            )
            
            if response and "data" in response:
                return response["data"]["ciphertext"]
                
            raise Exception("Encryption failed")
            
        except Exception as e:
            logger.error(f"Failed to encrypt data: {e}")
            raise
            
    async def decrypt_data(self, ciphertext: str, key_name: str) -> bytes:
        """Decrypt data using Transit engine"""
        try:
            response = self.vault.write(
                f"transit/decrypt/{key_name}",
                ciphertext=ciphertext,
                context=base64.b64encode(b"analytics").decode()
            )
            
            if response and "data" in response:
                return base64.b64decode(response["data"]["plaintext"])
                
            raise Exception("Decryption failed")
            
        except Exception as e:
            logger.error(f"Failed to decrypt data: {e}")
            raise
            
    async def get_pipeline_secrets(self, pipeline_name: str) -> Dict[str, Any]:
        """Get secrets for a data pipeline"""
        try:
            response = self.vault.read(f"secret/analytics/pipelines/{pipeline_name}")
            
            if response and "data" in response:
                return response["data"]["data"]
                
            return {}
            
        except Exception as e:
            logger.error(f"Failed to get pipeline secrets: {e}")
            return {}
            
    async def store_pipeline_secrets(self, pipeline_name: str, secrets: Dict[str, Any]):
        """Store secrets for a data pipeline"""
        try:
            self.vault.write(
                f"secret/analytics/pipelines/{pipeline_name}",
                **secrets
            )
            
            # Also store in Consul for distribution
            await self.consul.kv.put(
                f"analytics/pipelines/{pipeline_name}/configured",
                json.dumps({"timestamp": datetime.utcnow().isoformat()})
            )
            
        except Exception as e:
            logger.error(f"Failed to store pipeline secrets: {e}")
            raise
            
    async def get_query_cache_config(self) -> Dict[str, Any]:
        """Get query result caching configuration"""
        try:
            # Get from Consul KV
            _, data = await self.consul.kv.get("analytics/config/query-cache")
            
            if data and data["Value"]:
                return json.loads(data["Value"])
                
            # Default configuration
            default_config = {
                "enabled": True,
                "ttl_seconds": 3600,
                "max_size_mb": 1024,
                "eviction_policy": "lru",
                "compression": "snappy"
            }
            
            # Store default
            await self.consul.kv.put(
                "analytics/config/query-cache",
                json.dumps(default_config)
            )
            
            return default_config
            
        except Exception as e:
            logger.error(f"Failed to get cache config: {e}")
            return {"enabled": False}
            
    async def coordinate_distributed_job(self, 
                                       job_name: str,
                                       job_config: Dict[str, Any]) -> Dict[str, Any]:
        """Coordinate distributed analytics job using Consul"""
        try:
            session_id = await self.create_session(ttl=3600)
            
            # Try to acquire job lock
            lock_key = f"analytics/jobs/{job_name}/lock"
            acquired = await self.consul.kv.put(
                lock_key,
                json.dumps({
                    "session_id": session_id,
                    "node": self.node_id,
                    "started_at": datetime.utcnow().isoformat(),
                    "config": job_config
                }),
                acquire=session_id
            )
            
            if not acquired:
                # Job already running
                _, current = await self.consul.kv.get(lock_key)
                if current and current["Value"]:
                    return {
                        "status": "already_running",
                        "current_job": json.loads(current["Value"])
                    }
                    
            # Register job workers
            worker_count = job_config.get("workers", 4)
            for i in range(worker_count):
                await self.consul.kv.put(
                    f"analytics/jobs/{job_name}/workers/{i}",
                    json.dumps({
                        "status": "ready",
                        "node": self.node_id
                    })
                )
                
            return {
                "status": "started",
                "session_id": session_id,
                "workers": worker_count
            }
            
        except Exception as e:
            logger.error(f"Failed to coordinate job: {e}")
            raise
            
    async def get_data_catalog_config(self) -> Dict[str, Any]:
        """Get data catalog configuration"""
        try:
            _, data = await self.consul.kv.get("analytics/config/data-catalog")
            
            if data and data["Value"]:
                return json.loads(data["Value"])
                
            # Default catalog config
            default_config = {
                "metadata_store": "hive",
                "schema_registry": "confluent",
                "lineage_tracking": True,
                "auto_discovery": True,
                "refresh_interval": 300
            }
            
            await self.consul.kv.put(
                "analytics/config/data-catalog",
                json.dumps(default_config)
            )
            
            return default_config
            
        except Exception as e:
            logger.error(f"Failed to get catalog config: {e}")
            return {}
            
    async def register_data_source(self,
                                 source_name: str,
                                 source_config: Dict[str, Any]):
        """Register a data source in the catalog"""
        try:
            # Encrypt sensitive fields
            if "password" in source_config:
                source_config["password"] = await self.encrypt_data(
                    source_config["password"].encode(),
                    "analytics-data-sources"
                )
                
            # Store in Vault
            self.vault.write(
                f"secret/analytics/data-sources/{source_name}",
                **source_config
            )
            
            # Register in Consul catalog
            await self.consul.kv.put(
                f"analytics/data-sources/{source_name}/metadata",
                json.dumps({
                    "type": source_config.get("type"),
                    "registered_at": datetime.utcnow().isoformat(),
                    "registered_by": self.node_id
                })
            )
            
        except Exception as e:
            logger.error(f"Failed to register data source: {e}")
            raise
            
    async def _load_analytics_config(self):
        """Load analytics configuration from Consul"""
        try:
            # Load various configurations
            configs = [
                "query-optimization",
                "resource-limits",
                "security-policies",
                "performance-tuning"
            ]
            
            for config_name in configs:
                _, data = await self.consul.kv.get(f"analytics/config/{config_name}")
                if data and data["Value"]:
                    self.analytics_config[config_name] = json.loads(data["Value"])
                    
            logger.info(f"Loaded {len(self.analytics_config)} analytics configurations")
            
        except Exception as e:
            logger.error(f"Failed to load analytics config: {e}")
            
    async def _setup_credential_rotation(self):
        """Setup automatic credential rotation"""
        async def rotate_credentials():
            while True:
                try:
                    # Rotate Trino credentials monthly
                    await self._rotate_trino_credentials()
                    
                    # Rotate data lake credentials weekly  
                    await self._rotate_data_lake_credentials()
                    
                    await asyncio.sleep(86400)  # Daily check
                    
                except Exception as e:
                    logger.error(f"Credential rotation error: {e}")
                    await asyncio.sleep(3600)
                    
        asyncio.create_task(rotate_credentials())
        
    async def _setup_config_watchers(self):
        """Setup configuration watchers"""
        async def watch_config(config_key: str):
            index = None
            while True:
                try:
                    index, data = await self.consul.kv.get(
                        f"analytics/config/{config_key}",
                        index=index,
                        wait="30s"
                    )
                    
                    if data and data["Value"]:
                        new_config = json.loads(data["Value"])
                        old_config = self.analytics_config.get(config_key)
                        
                        if new_config != old_config:
                            self.analytics_config[config_key] = new_config
                            logger.info(f"Analytics config updated: {config_key}")
                            
                            # Notify about config change
                            await self._on_config_change(config_key, new_config)
                            
                except Exception as e:
                    logger.error(f"Config watcher error for {config_key}: {e}")
                    await asyncio.sleep(10)
                    
        # Start watchers for important configs
        for config_key in ["query-cache", "resource-limits", "security-policies"]:
            self._config_watchers[config_key] = asyncio.create_task(
                watch_config(config_key)
            )
            
    async def _on_config_change(self, config_key: str, new_config: Dict[str, Any]):
        """Handle configuration changes"""
        logger.info(f"Processing config change for {config_key}")
        
        # Implement specific handlers for different configs
        if config_key == "query-cache":
            # Update cache settings
            pass
        elif config_key == "resource-limits":
            # Update resource limits
            pass
        elif config_key == "security-policies":
            # Update security policies
            pass
            
    async def _generate_trino_credentials(self) -> Dict[str, str]:
        """Generate new Trino credentials"""
        import secrets
        
        credentials = {
            "username": f"trino_analytics_{secrets.token_hex(4)}",
            "password": secrets.token_urlsafe(32),
            "catalog": "analytics",
            "schema": "default"
        }
        
        # Store in Vault
        self.vault.write("secret/analytics/trino/credentials", **credentials)
        
        return credentials
        
    async def _generate_minio_credentials(self) -> Dict[str, str]:
        """Generate new MinIO credentials"""
        import secrets
        
        credentials = {
            "endpoint": os.getenv("MINIO_ENDPOINT", "minio:9000"),
            "access_key": f"analytics_{secrets.token_hex(8)}",
            "secret_key": secrets.token_urlsafe(40),
            "secure": False
        }
        
        # Store in Vault
        self.vault.write("secret/analytics/data-lake/minio", **credentials)
        
        return credentials
        
    async def _generate_ignite_credentials(self) -> Dict[str, str]:
        """Generate Apache Ignite credentials"""
        import secrets
        
        credentials = {
            "username": f"ignite_analytics_{secrets.token_hex(4)}",
            "password": secrets.token_urlsafe(32)
        }
        
        # Store in Vault
        self.vault.write("secret/analytics/ignite/credentials", **credentials)
        
        return credentials
        
    async def _get_elasticsearch_hosts(self) -> List[str]:
        """Get Elasticsearch hosts from Consul"""
        try:
            _, services = await self.consul.health.service("elasticsearch", passing=True)
            
            hosts = []
            for service in services:
                host = service["Service"]["Address"]
                port = service["Service"]["Port"]
                hosts.append(f"{host}:{port}")
                
            return hosts or ["elasticsearch:9200"]
            
        except Exception:
            return ["elasticsearch:9200"]
            
    async def _rotate_trino_credentials(self):
        """Rotate Trino credentials"""
        try:
            # Check last rotation time
            response = self.vault.read("secret/analytics/trino/rotation")
            
            if response and "data" in response:
                last_rotation = datetime.fromisoformat(
                    response["data"]["data"]["last_rotation"]
                )
                
                # Rotate monthly
                if datetime.utcnow() - last_rotation < timedelta(days=30):
                    return
                    
            # Generate new credentials
            new_creds = await self._generate_trino_credentials()
            
            # Update rotation timestamp
            self.vault.write(
                "secret/analytics/trino/rotation",
                last_rotation=datetime.utcnow().isoformat()
            )
            
            # Clear credential cache
            if "trino_credentials" in self._credential_cache:
                del self._credential_cache["trino_credentials"]
                
            logger.info("Rotated Trino credentials")
            
        except Exception as e:
            logger.error(f"Failed to rotate Trino credentials: {e}")
            
    async def _rotate_data_lake_credentials(self):
        """Rotate data lake credentials"""
        try:
            # Check last rotation time for MinIO
            response = self.vault.read("secret/analytics/data-lake/rotation")
            
            if response and "data" in response:
                last_rotation = datetime.fromisoformat(
                    response["data"]["data"]["last_rotation"]
                )
                
                # Rotate weekly
                if datetime.utcnow() - last_rotation < timedelta(days=7):
                    return
                    
            # Generate new MinIO credentials
            new_creds = await self._generate_minio_credentials()
            
            # Update rotation timestamp
            self.vault.write(
                "secret/analytics/data-lake/rotation",
                last_rotation=datetime.utcnow().isoformat()
            )
            
            # Clear credential cache
            cache_keys = [k for k in self._credential_cache if k.startswith("data_lake_")]
            for key in cache_keys:
                del self._credential_cache[key]
                
            logger.info("Rotated data lake credentials")
            
        except Exception as e:
            logger.error(f"Failed to rotate data lake credentials: {e}")
            
    async def shutdown(self):
        """Cleanup resources"""
        # Cancel config watchers
        for task in self._config_watchers.values():
            task.cancel()
            
        await super().shutdown() 