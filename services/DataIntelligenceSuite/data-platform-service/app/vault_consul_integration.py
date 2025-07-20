"""
Data Platform Service - Vault & Consul Integration
"""

from typing import Dict, Any, Optional, List, Tuple
import asyncio
from datetime import datetime, timedelta
from contextlib import asynccontextmanager
from platformq_shared.vault.vault_client import VaultClient
from platformq_shared.consul.consul_client import ConsulClient
from dataclasses import dataclass
from enum import Enum
import pandas as pd
import numpy as np
from cryptography.fernet import Fernet
import logging

logger = logging.getLogger(__name__)

class DataQualityLevel(Enum):
    GOLD = "gold"      # Fully validated, production-ready
    SILVER = "silver"  # Cleaned, deduplicated
    BRONZE = "bronze"  # Raw data

@dataclass
class QueryConfig:
    """Query engine configuration"""
    default_timeout_seconds: int = 300
    max_result_size_mb: int = 100
    enable_query_cache: bool = True
    cache_ttl_seconds: int = 3600
    max_concurrent_queries: int = 50

@dataclass
class DataQualityRules:
    """Data quality validation rules"""
    null_threshold: float = 0.05
    duplicate_threshold: float = 0.01
    outlier_zscore: float = 3.0
    completeness_threshold: float = 0.95

@dataclass
class RetentionPolicy:
    """Data retention policies"""
    raw_data_days: int = 90
    aggregated_data_days: int = 365
    archived_data_days: int = 2555  # 7 years
    pii_data_days: int = 30


class DataServiceVaultIntegration:
    """
    Vault integration for data services with dynamic credentials
    and column-level encryption support.
    """
    
    def __init__(self, vault_client: VaultClient, service_name: str = "data-platform-service"):
        self.vault = vault_client
        self.service_name = service_name
        self._db_connections: Dict[str, Any] = {}
        self._encryption_keys: Dict[str, bytes] = {}
        self._credential_leases: Dict[str, str] = {}
        
    async def initialize(self):
        """Initialize Vault integration for data service"""
        # Set up database secret engines
        await self._setup_database_engines()
        
        # Load encryption keys
        await self._load_encryption_keys()
        
        # Set up credential renewal
        await self._start_credential_renewal()
        
        logger.info("Data service Vault integration initialized")
        
    async def _setup_database_engines(self):
        """Ensure database secret engines are configured"""
        databases = {
            "postgres": {
                "plugin": "postgresql-database-plugin",
                "connection_url": "postgresql://{{username}}:{{password}}@postgres:5432/analytics?sslmode=require",
                "allowed_roles": ["readonly", "readwrite", "analytics"],
                "username": "vault_admin",
                "password": await self._get_admin_password("postgres")
            },
            "cassandra": {
                "plugin": "cassandra-database-plugin", 
                "hosts": "cassandra",
                "username": "vault_admin",
                "password": await self._get_admin_password("cassandra"),
                "protocol_version": 4,
                "allowed_roles": ["reader", "writer"]
            },
            "elasticsearch": {
                "plugin": "elasticsearch-database-plugin",
                "url": "https://elasticsearch:9200",
                "username": "vault_admin",
                "password": await self._get_admin_password("elasticsearch"),
                "allowed_roles": ["search", "index"]
            },
            "druid": {
                "plugin": "custom-database-plugin",  # Druid uses REST API
                "broker_url": "http://druid-broker:8082",
                "coordinator_url": "http://druid-coordinator:8081",
                "overlord_url": "http://druid-overlord:8090",
                "allowed_roles": ["analytics", "timeseries"],
                "username": "vault_admin",
                "password": await self._get_admin_password("druid")
            }
        }
        
        for db_name, config in databases.items():
            try:
                # Check if already configured
                await self.vault.read_database_connection(db_name)
                logger.debug(f"Database engine {db_name} already configured")
            except:
                # Configure database engine
                await self.vault.configure_database_engine(db_name, config)
                logger.info(f"Configured database engine: {db_name}")
                
                # Create roles
                await self._create_database_roles(db_name, config["allowed_roles"])
                
    async def _get_admin_password(self, database: str) -> str:
        """Get admin password from Vault"""
        try:
            secret_path = f"{self.service_name}/databases/{database}/root-credentials"
            secret = await self.vault.get_secret(secret_path)
            return secret["password"]
        except:
            # Return placeholder for initialization
            return f"${{{database.upper()}_ADMIN_PASSWORD}}"
            
    async def _create_database_roles(self, database: str, roles: List[str]):
        """Create database roles for dynamic credentials"""
        role_statements = {
            "postgres": {
                "readonly": [
                    "CREATE ROLE \"{{name}}\" WITH LOGIN PASSWORD '{{password}}' VALID UNTIL '{{expiration}}';",
                    "GRANT SELECT ON ALL TABLES IN SCHEMA public TO \"{{name}}\";"
                ],
                "readwrite": [
                    "CREATE ROLE \"{{name}}\" WITH LOGIN PASSWORD '{{password}}' VALID UNTIL '{{expiration}}';",
                    "GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA public TO \"{{name}}\";"
                ],
                "analytics": [
                    "CREATE ROLE \"{{name}}\" WITH LOGIN PASSWORD '{{password}}' VALID UNTIL '{{expiration}}';",
                    "GRANT SELECT ON ALL TABLES IN SCHEMA public TO \"{{name}}\";",
                    "GRANT USAGE ON SCHEMA analytics TO \"{{name}}\";",
                    "GRANT SELECT ON ALL TABLES IN SCHEMA analytics TO \"{{name}}\";"
                ]
            },
            "cassandra": {
                "reader": [
                    "CREATE USER '{{username}}' WITH PASSWORD '{{password}}' NOSUPERUSER;",
                    "GRANT SELECT ON ALL KEYSPACES TO {{username}};"
                ],
                "writer": [
                    "CREATE USER '{{username}}' WITH PASSWORD '{{password}}' NOSUPERUSER;",
                    "GRANT ALL ON ALL KEYSPACES TO {{username}};"
                ]
            }
        }
        
        for role in roles:
            if database in role_statements and role in role_statements[database]:
                try:
                    await self.vault.create_database_role(
                        database=database,
                        role=role,
                        creation_statements=role_statements[database][role],
                        default_ttl="1h",
                        max_ttl="24h"
                    )
                    logger.info(f"Created database role: {database}/{role}")
                except Exception as e:
                    logger.error(f"Failed to create role {database}/{role}: {e}")
                    
    async def _load_encryption_keys(self):
        """Load column encryption keys"""
        key_types = ["pii-encryption-key", "financial-data-key", "health-data-key"]
        
        for key_type in key_types:
            key_path = f"{self.service_name}/encryption/column-keys/{key_type}"
            
            try:
                key_data = await self.vault.get_secret(key_path)
                self._encryption_keys[key_type] = key_data["value"].encode()
            except:
                # Generate new key if doesn't exist
                new_key = Fernet.generate_key()
                await self.vault.create_or_update_secret(
                    key_path,
                    {
                        "value": new_key.decode(),
                        "created_at": datetime.utcnow().isoformat(),
                        "algorithm": "AES-256-GCM"
                    }
                )
                self._encryption_keys[key_type] = new_key
                logger.info(f"Generated new encryption key: {key_type}")
                
    async def _start_credential_renewal(self):
        """Start background task for credential renewal"""
        asyncio.create_task(self._credential_renewal_loop())
        
    async def _credential_renewal_loop(self):
        """Renew credentials before they expire"""
        while True:
            try:
                await asyncio.sleep(1800)  # Check every 30 minutes
                
                for key, lease_id in list(self._credential_leases.items()):
                    try:
                        # Check lease
                        lease_info = await self.vault.lookup_lease(lease_id)
                        ttl = lease_info.get("ttl", 0)
                        
                        # Renew if less than 30 minutes remaining
                        if ttl < 1800:
                            await self.vault.renew_lease(lease_id)
                            logger.info(f"Renewed lease for {key}")
                            
                    except Exception as e:
                        logger.error(f"Failed to renew lease {key}: {e}")
                        # Remove expired lease
                        del self._credential_leases[key]
                        
            except Exception as e:
                logger.error(f"Credential renewal loop error: {e}")
                
    @asynccontextmanager
    async def get_database_connection(self, 
                                    database: str,
                                    role: str = "readonly",
                                    ttl: str = "1h") -> Any:
        """
        Get database connection with dynamic credentials.
        Credentials are automatically revoked when connection is closed.
        """
        # Generate dynamic credentials
        creds = await self.vault.generate_database_credentials(
            database=database,
            role=role,
            ttl=ttl
        )
        
        lease_id = creds["lease_id"]
        username = creds["data"]["username"]
        password = creds["data"]["password"]
        
        # Store lease for renewal
        self._credential_leases[f"{database}-{role}"] = lease_id
        
        connection = None
        try:
            # Create connection based on database type
            if database == "postgres":
                import asyncpg
                connection = await asyncpg.connect(
                    host="postgres",
                    port=5432,
                    user=username,
                    password=password,
                    database="analytics",
                    ssl="require"
                )
            elif database == "cassandra":
                from cassandra.cluster import Cluster
                from cassandra.auth import PlainTextAuthProvider
                
                auth_provider = PlainTextAuthProvider(username, password)
                cluster = Cluster(['cassandra'], auth_provider=auth_provider)
                connection = cluster.connect()
            elif database == "elasticsearch":
                from elasticsearch import AsyncElasticsearch
                
                connection = AsyncElasticsearch(
                    ["https://elasticsearch:9200"],
                    basic_auth=(username, password),
                    verify_certs=True
                )
            elif database == "druid":
                from pydruid.client import PyDruid
                
                connection = PyDruid(
                    url='http://druid-broker:8082',
                    endpoint='druid/v2',
                    username=username,
                    password=password
                )
                
            yield connection
            
        finally:
            # Clean up connection
            if connection:
                if database == "postgres":
                    await connection.close()
                elif database == "cassandra":
                    connection.shutdown()
                elif database == "elasticsearch":
                    await connection.close()
                elif database == "druid":
                    # PyDruid client doesn't require explicit disconnect
                    pass
                    
            # Revoke credentials
            try:
                await self.vault.revoke_lease(lease_id)
                del self._credential_leases[f"{database}-{role}"]
                logger.info(f"Revoked credentials for {database}/{role}")
            except Exception as e:
                logger.error(f"Failed to revoke lease: {e}")
                
    async def encrypt_dataframe_columns(self,
                                      df: pd.DataFrame,
                                      columns: List[str],
                                      encryption_type: str = "pii") -> pd.DataFrame:
        """Encrypt specific columns in a DataFrame"""
        # Get encryption key
        key = await self._get_encryption_key(f"{encryption_type}-encryption-key")
        fernet = Fernet(key)
        
        # Create copy to avoid modifying original
        encrypted_df = df.copy()
        
        # Encrypt specified columns
        for column in columns:
            if column in encrypted_df.columns:
                encrypted_df[f"{column}_encrypted"] = encrypted_df[column].apply(
                    lambda x: fernet.encrypt(str(x).encode()).decode() if pd.notna(x) else None
                )
                # Drop original column
                encrypted_df = encrypted_df.drop(column, axis=1)
                
        return encrypted_df
        
    async def decrypt_dataframe_columns(self,
                                      df: pd.DataFrame,
                                      columns: List[str],
                                      encryption_type: str = "pii") -> pd.DataFrame:
        """Decrypt specific columns in a DataFrame"""
        # Get encryption key
        key = await self._get_encryption_key(f"{encryption_type}-encryption-key")
        fernet = Fernet(key)
        
        # Create copy
        decrypted_df = df.copy()
        
        # Decrypt specified columns
        for column in columns:
            encrypted_col = f"{column}_encrypted"
            if encrypted_col in decrypted_df.columns:
                decrypted_df[column] = decrypted_df[encrypted_col].apply(
                    lambda x: fernet.decrypt(x.encode()).decode() if pd.notna(x) else None
                )
                # Drop encrypted column
                decrypted_df = decrypted_df.drop(encrypted_col, axis=1)
                
        return decrypted_df
        
    async def _get_encryption_key(self, key_name: str) -> bytes:
        """Get or cache encryption key"""
        if key_name not in self._encryption_keys:
            key_path = f"{self.service_name}/encryption/column-keys/{key_name}"
            key_data = await self.vault.get_secret(key_path)
            self._encryption_keys[key_name] = key_data["value"].encode()
            
        return self._encryption_keys[key_name]
        
    async def get_cloud_storage_client(self, provider: str, bucket: str = None):
        """Get authenticated cloud storage client"""
        if provider == "s3" or provider == "minio":
            creds_path = f"{self.service_name}/cloud-storage/s3/data-lake-credentials"
            creds = await self.vault.get_secret(creds_path)
            
            import boto3
            
            # Support MinIO with custom endpoint
            endpoint_url = creds.get("endpoint_url", "http://minio:9000") if provider == "minio" else None
            
            return boto3.client(
                's3',
                aws_access_key_id=creds["access_key"],
                aws_secret_access_key=creds["secret_key"],
                endpoint_url=endpoint_url,
                region_name=creds.get("region", "us-east-1")
            )
            
        elif provider == "gcs":
            creds_path = f"{self.service_name}/cloud-storage/gcs/analytics-bucket"
            creds = await self.vault.get_secret(creds_path)
            
            from google.cloud import storage
            return storage.Client.from_service_account_info(creds)
            
        elif provider == "azure":
            creds_path = f"{self.service_name}/cloud-storage/azure/blob-storage-key"
            creds = await self.vault.get_secret(creds_path)
            
            from azure.storage.blob import BlobServiceClient
            return BlobServiceClient(
                account_url=f"https://{creds['account_name']}.blob.core.windows.net",
                credential=creds['account_key']
            )
            
    async def rotate_encryption_keys(self):
        """Rotate column encryption keys with re-encryption"""
        logger.info("Starting encryption key rotation")
        
        key_types = ["pii", "financial-data", "health-data"]
        
        for key_type in key_types:
            old_key_name = f"{key_type}-encryption-key"
            new_key_name = f"{key_type}-encryption-key-new"
            
            # Generate new key
            new_key = Fernet.generate_key()
            
            # Store new key
            await self.vault.create_or_update_secret(
                f"{self.service_name}/encryption/column-keys/{new_key_name}",
                {
                    "value": new_key.decode(),
                    "created_at": datetime.utcnow().isoformat(),
                    "rotation_version": 2
                }
            )
            
            # Keep old key for re-encryption period
            old_key_path = f"{self.service_name}/encryption/column-keys/{old_key_name}"
            await self.vault.create_or_update_secret(
                f"{old_key_path}-previous",
                await self.vault.get_secret(old_key_path)
            )
            
            # Update current key
            await self.vault.create_or_update_secret(
                old_key_path,
                {
                    "value": new_key.decode(),
                    "created_at": datetime.utcnow().isoformat(),
                    "rotation_version": 2
                }
            )
            
            # Clear cache
            self._encryption_keys.pop(old_key_name, None)
            
            logger.info(f"Rotated encryption key: {key_type}")
            
        logger.info("Encryption key rotation completed")
        
    async def get_analytics_credentials(self, tool: str) -> Dict[str, Any]:
        """Get credentials for analytics tools"""
        if tool == "databricks":
            creds_path = f"{self.service_name}/analytics-tools/databricks"
            creds = await self.vault.get_secret(creds_path)
            
            return {
                "host": creds["workspace_url"],
                "token": creds["token"],
                "cluster_id": creds.get("cluster_id")
            }
            
        elif tool == "snowflake":
            creds_path = f"{self.service_name}/analytics-tools/snowflake/account-credentials"
            creds = await self.vault.get_secret(creds_path)
            
            return {
                "account": creds["account"],
                "user": creds["username"],
                "password": creds["password"],
                "warehouse": creds.get("warehouse", "COMPUTE_WH"),
                "database": creds.get("database", "ANALYTICS"),
                "schema": creds.get("schema", "PUBLIC")
            }
            
        elif tool == "tableau":
            creds_path = f"{self.service_name}/analytics-tools/tableau/server-api-key"
            creds = await self.vault.get_secret(creds_path)
            
            return {
                "server": creds["server_url"],
                "api_key": creds["api_key"],
                "site_id": creds.get("site_id", "")
            }
            
        else:
            raise ValueError(f"Unknown analytics tool: {tool}")


class DataServiceConsulIntegration:
    """Consul integration for data services"""
    
    def __init__(self, consul_client: ConsulClient, service_name: str = "data-platform-service"):
        self.consul = consul_client
        self.service_name = service_name
        self._query_config: Optional[QueryConfig] = None
        self._quality_rules: Optional[DataQualityRules] = None
        self._retention_policy: Optional[RetentionPolicy] = None
        self._dataset_registry: Dict[str, Dict] = {}
        self._active_queries: set = set()
        self._watchers: Dict[str, asyncio.Task] = {}
        
    async def initialize(self):
        """Initialize Consul integration"""
        # Register service
        await self._register_service()
        
        # Load configurations
        await self.reload_configurations()
        
        # Start configuration watchers
        await self._start_config_watchers()
        
        # Initialize data catalog
        await self._init_data_catalog()
        
        logger.info("Data service Consul integration initialized")
        
    async def _register_service(self):
        """Register data service with Consul"""
        from platformq_shared.consul.consul_client import ServiceDefinition
        
        service = ServiceDefinition(
            name=self.service_name,
            port=8000,
            tags=["data", "analytics", "critical", "vault-integrated"],
            meta={
                "version": "2.0.0",
                "capabilities": "query,catalog,quality,lineage,encryption",
                "databases": "postgres,cassandra,elasticsearch,druid"
            },
            check={
                "http": "http://localhost:8000/health",
                "interval": "10s",
                "timeout": "5s",
                "deregister_critical_service_after": "60s"
            }
        )
        
        await self.consul.register_service(service)
        
    async def reload_configurations(self):
        """Reload all configurations from Consul"""
        base_path = f"services/{self.service_name}/config"
        
        try:
            # Load query engine config
            query_config = await self.consul.kv_get_prefix(f"{base_path}/query-engine/")
            self._query_config = QueryConfig(
                default_timeout_seconds=int(query_config.get("default-timeout-seconds", 300)),
                max_result_size_mb=int(query_config.get("max-result-size-mb", 100)),
                enable_query_cache=query_config.get("enable-query-cache", "true").lower() == "true",
                cache_ttl_seconds=int(query_config.get("cache-ttl-seconds", 3600)),
                max_concurrent_queries=int(query_config.get("max-concurrent-queries", 50))
            )
            
            # Load data quality rules
            quality_rules = await self.consul.kv_get_prefix(f"{base_path}/data-quality/validation-rules/")
            self._quality_rules = DataQualityRules(
                null_threshold=float(quality_rules.get("null-threshold", 0.05)),
                duplicate_threshold=float(quality_rules.get("duplicate-threshold", 0.01)),
                outlier_zscore=float(quality_rules.get("outlier-zscore", 3.0)),
                completeness_threshold=float(quality_rules.get("completeness-threshold", 0.95))
            )
            
            # Load retention policies
            retention = await self.consul.kv_get_prefix(f"{base_path}/retention-policies/")
            self._retention_policy = RetentionPolicy(
                raw_data_days=int(retention.get("raw-data-days", 90)),
                aggregated_data_days=int(retention.get("aggregated-data-days", 365)),
                archived_data_days=int(retention.get("archived-data-days", 2555)),
                pii_data_days=int(retention.get("pii-data-days", 30))
            )
            
            logger.info("Reloaded data service configurations")
            
        except Exception as e:
            logger.error(f"Failed to reload configurations: {e}")
            # Use defaults
            self._query_config = QueryConfig()
            self._quality_rules = DataQualityRules()
            self._retention_policy = RetentionPolicy()
            
    async def _start_config_watchers(self):
        """Start configuration watchers"""
        watch_paths = [
            "config/query-engine",
            "config/data-quality",
            "config/retention-policies"
        ]
        
        for path in watch_paths:
            full_path = f"services/{self.service_name}/{path}"
            watcher = asyncio.create_task(
                self._watch_config_changes(full_path)
            )
            self._watchers[path] = watcher
            
    async def _watch_config_changes(self, path: str):
        """Watch for configuration changes"""
        try:
            async for event in self.consul.watch_prefix(path):
                logger.info(f"Configuration changed at {path}")
                await self.reload_configurations()
                
        except asyncio.CancelledError:
            logger.info(f"Config watcher cancelled for {path}")
            raise
        except Exception as e:
            logger.error(f"Config watcher error for {path}: {e}")
            
    async def _init_data_catalog(self):
        """Initialize data catalog from Consul"""
        catalog_path = f"services/{self.service_name}/data-catalog/datasets"
        
        try:
            datasets = await self.consul.kv_get_prefix(catalog_path)
            
            for dataset_name, metadata in datasets.items():
                if isinstance(metadata, dict) and "schema" in metadata:
                    self._dataset_registry[dataset_name] = metadata
                    
            logger.info(f"Loaded {len(self._dataset_registry)} datasets from catalog")
            
        except Exception as e:
            logger.error(f"Failed to initialize data catalog: {e}")
            
    async def get_query_config(self) -> QueryConfig:
        """Get query configuration"""
        if not self._query_config:
            await self.reload_configurations()
        return self._query_config
        
    async def get_quality_rules(self) -> DataQualityRules:
        """Get data quality rules"""
        if not self._quality_rules:
            await self.reload_configurations()
        return self._quality_rules
        
    async def get_retention_policy(self) -> RetentionPolicy:
        """Get retention policy"""
        if not self._retention_policy:
            await self.reload_configurations()
        return self._retention_policy
        
    async def acquire_query_slot(self, query_id: str) -> bool:
        """Acquire slot for query execution with concurrency control"""
        config = await self.get_query_config()
        
        # Check current query count
        query_count_key = f"services/{self.service_name}/metrics/active-queries"
        
        try:
            current_count = int(await self.consul.kv_get(query_count_key, default="0"))
            
            if current_count >= config.max_concurrent_queries:
                logger.warning(f"Query slot unavailable, {current_count} queries running")
                return False
                
            # Try to increment atomically
            new_count = current_count + 1
            success = await self.consul.kv_put_cas(
                query_count_key,
                str(new_count),
                cas=current_count
            )
            
            if success:
                self._active_queries.add(query_id)
                
                # Record query start
                await self.consul.kv_put(
                    f"services/{self.service_name}/pipeline-state/active-jobs/{query_id}",
                    {
                        "status": "running",
                        "started": datetime.utcnow().isoformat()
                    },
                    ttl=config.default_timeout_seconds
                )
                
            return success
            
        except Exception as e:
            logger.error(f"Failed to acquire query slot: {e}")
            return False
            
    async def release_query_slot(self, query_id: str):
        """Release query execution slot"""
        if query_id not in self._active_queries:
            return
            
        try:
            # Decrement query count
            query_count_key = f"services/{self.service_name}/metrics/active-queries"
            current_count = int(await self.consul.kv_get(query_count_key, default="1"))
            await self.consul.kv_put(query_count_key, str(max(0, current_count - 1)))
            
            # Remove from active jobs
            await self.consul.kv_delete(
                f"services/{self.service_name}/pipeline-state/active-jobs/{query_id}"
            )
            
            self._active_queries.discard(query_id)
            
        except Exception as e:
            logger.error(f"Failed to release query slot: {e}")
            
    async def register_dataset(self, 
                             dataset_name: str,
                             schema: Dict[str, Any],
                             metadata: Dict[str, Any]):
        """Register dataset in data catalog"""
        catalog_path = f"services/{self.service_name}/data-catalog/datasets/{dataset_name}"
        
        dataset_info = {
            "schema": schema,
            "owner": metadata.get("owner", "unknown"),
            "tags": metadata.get("tags", []),
            "quality_score": metadata.get("quality_score", 0.0),
            "created_at": metadata.get("created_at", datetime.utcnow().isoformat()),
            "last_updated": datetime.utcnow().isoformat(),
            "row_count": metadata.get("row_count", 0),
            "size_bytes": metadata.get("size_bytes", 0),
            "quality_level": metadata.get("quality_level", DataQualityLevel.BRONZE.value),
            "encryption": metadata.get("encryption", {"enabled": False}),
            "sensitive_columns": metadata.get("sensitive_columns", [])
        }
        
        # Store in Consul
        await self.consul.kv_put(f"{catalog_path}/metadata", dataset_info)
        
        # Update lineage if provided
        if "upstream_datasets" in metadata:
            await self.consul.kv_put(
                f"{catalog_path}/lineage",
                {
                    "upstream": metadata["upstream_datasets"],
                    "downstream": []  # Will be updated by downstream datasets
                }
            )
            
        # Cache locally
        self._dataset_registry[dataset_name] = dataset_info
        
        logger.info(f"Registered dataset: {dataset_name}")
        
    async def get_dataset_metadata(self, dataset_name: str) -> Optional[Dict[str, Any]]:
        """Get dataset metadata from catalog"""
        if dataset_name in self._dataset_registry:
            return self._dataset_registry[dataset_name]
            
        catalog_path = f"services/{self.service_name}/data-catalog/datasets/{dataset_name}/metadata"
        metadata = await self.consul.kv_get(catalog_path)
        
        if metadata:
            self._dataset_registry[dataset_name] = metadata
            
        return metadata
        
    async def update_data_quality_score(self, 
                                      dataset_name: str,
                                      quality_metrics: Dict[str, float]):
        """Update data quality score for dataset"""
        # Calculate overall score
        weights = {
            "completeness": 0.3,
            "accuracy": 0.3,
            "consistency": 0.2,
            "timeliness": 0.2
        }
        
        overall_score = sum(
            quality_metrics.get(metric, 0) * weight
            for metric, weight in weights.items()
        )
        
        # Determine quality level
        if overall_score >= 0.95:
            quality_level = DataQualityLevel.GOLD
        elif overall_score >= 0.80:
            quality_level = DataQualityLevel.SILVER
        else:
            quality_level = DataQualityLevel.BRONZE
            
        # Update in Consul
        catalog_path = f"services/{self.service_name}/data-catalog/datasets/{dataset_name}"
        
        await self.consul.kv_merge(
            f"{catalog_path}/metadata",
            {
                "quality_score": overall_score,
                "quality_level": quality_level.value,
                "quality_metrics": quality_metrics,
                "quality_updated_at": datetime.utcnow().isoformat()
            }
        )
        
        # Update local cache
        if dataset_name in self._dataset_registry:
            self._dataset_registry[dataset_name]["quality_score"] = overall_score
            self._dataset_registry[dataset_name]["quality_level"] = quality_level.value
            
        # Send alert if quality drops
        if overall_score < 0.8:
            await self._send_quality_alert(dataset_name, overall_score)
            
    async def _send_quality_alert(self, dataset_name: str, score: float):
        """Send data quality alert"""
        alert_key = f"services/{self.service_name}/alerts/quality/{dataset_name}"
        await self.consul.kv_put(
            alert_key,
            {
                "dataset": dataset_name,
                "score": score,
                "timestamp": datetime.utcnow().isoformat(),
                "severity": "high" if score < 0.6 else "medium"
            },
            ttl=3600  # 1 hour
        )
        logger.warning(f"Data quality alert for {dataset_name}: score {score}")
        
    async def check_retention_policy(self, dataset_name: str, data_date: datetime) -> bool:
        """Check if data should be retained based on policy"""
        metadata = await self.get_dataset_metadata(dataset_name)
        if not metadata:
            return True  # Keep if no metadata
            
        policy = await self.get_retention_policy()
        data_age = (datetime.utcnow() - data_date).days
        
        # Check based on data type and quality level
        if "pii" in metadata.get("tags", []):
            return data_age <= policy.pii_data_days
            
        quality_level = metadata.get("quality_level", DataQualityLevel.BRONZE.value)
        
        if quality_level == DataQualityLevel.GOLD.value:
            return data_age <= policy.archived_data_days
        elif quality_level == DataQualityLevel.SILVER.value:
            return data_age <= policy.aggregated_data_days
        else:
            return data_age <= policy.raw_data_days
            
    async def coordinate_etl_pipeline(self, 
                                    pipeline_name: str,
                                    stages: List[str]) -> bool:
        """Coordinate multi-stage ETL pipeline execution"""
        pipeline_key = f"services/{self.service_name}/pipelines/{pipeline_name}"
        
        # Create pipeline coordination entry
        pipeline_state = {
            "name": pipeline_name,
            "stages": stages,
            "current_stage": 0,
            "status": "running",
            "started_at": datetime.utcnow().isoformat(),
            "completed_stages": []
        }
        
        await self.consul.kv_put(pipeline_key, pipeline_state, ttl=7200)  # 2 hour TTL
        
        # In production, this would coordinate actual pipeline stages
        # For now, simulate completion
        pipeline_state["status"] = "completed"
        pipeline_state["completed_at"] = datetime.utcnow().isoformat()
        await self.consul.kv_put(pipeline_key, pipeline_state)
        
        return True
        
    async def get_optimal_partition_strategy(self, 
                                           dataset_name: str,
                                           estimated_size_gb: float) -> Dict[str, Any]:
        """Get optimal partitioning strategy based on data characteristics"""
        # Get performance config
        perf_config = await self.consul.kv_get_prefix(
            f"services/{self.service_name}/config/performance/"
        )
        
        partition_size_mb = int(perf_config.get("partition-size-mb", 128))
        
        # Calculate number of partitions
        size_mb = estimated_size_gb * 1024
        num_partitions = max(1, int(size_mb / partition_size_mb))
        
        # Get dataset metadata for partition key selection
        metadata = await self.get_dataset_metadata(dataset_name)
        
        # Recommend partition key based on tags and schema
        partition_key = "date"  # Default
        if metadata:
            if "time-series" in metadata.get("tags", []):
                partition_key = "timestamp"
            elif "user-data" in metadata.get("tags", []):
                partition_key = "user_id"
            elif "geo-data" in metadata.get("tags", []):
                partition_key = "region"
                
        return {
            "num_partitions": num_partitions,
            "partition_size_mb": partition_size_mb,
            "partition_key": partition_key,
            "compression": perf_config.get("compression-type", "snappy"),
            "parallel_threads": int(perf_config.get("parallel-threads", 8))
        } 