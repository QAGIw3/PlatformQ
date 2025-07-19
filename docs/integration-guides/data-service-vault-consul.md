# Data & Analytics Service - Vault & Consul Integration Guide

## Overview
This guide covers integrating data and analytics services with Vault and Consul for secure credential management, dynamic database access, and configuration management.

## Vault Integration

### 1. Secret Structure

```yaml
# Vault path structure for data services
data-platform-service/
├── databases/
│   ├── postgres/
│   │   ├── root-credentials      # Root creds (never used by app)
│   │   └── roles/
│   │       ├── readonly          # Read-only access
│   │       ├── readwrite         # Read-write access
│   │       └── analytics         # Analytics user
│   ├── cassandra/
│   │   ├── root-credentials
│   │   └── roles/
│   │       ├── reader
│   │       └── writer
│   ├── elasticsearch/
│   │   ├── admin-credentials
│   │   └── api-keys/
│   │       ├── search-api-key
│   │       └── index-api-key
│   └── druid/
│       ├── admin-credentials
│       └── roles/
│           ├── analytics
│           └── timeseries
├── encryption/
│   ├── column-keys/              # Column-level encryption
│   │   ├── pii-encryption-key
│   │   ├── financial-data-key
│   │   └── health-data-key
│   └── file-encryption/
│       ├── parquet-encryption-key
│       └── backup-encryption-key
├── cloud-storage/
│   ├── s3/
│   │   ├── data-lake-credentials
│   │   └── backup-bucket-creds
│   ├── gcs/
│   │   └── analytics-bucket
│   └── azure/
│       └── blob-storage-key
├── analytics-tools/
│   ├── databricks/
│   │   ├── workspace-token
│   │   └── sql-endpoint-token
│   ├── snowflake/
│   │   ├── account-credentials
│   │   └── warehouse-tokens
│   └── tableau/
│       └── server-api-key
└── data-pipelines/
    ├── airflow/
    │   └── connections/
    │       ├── source-db
    │       └── target-db
    └── kafka/
        ├── producer-credentials
        └── consumer-credentials
```

### 2. Implementation Code

```python
# data_service/vault_integration.py
from typing import Dict, Any, Optional, List, Tuple
import asyncio
from datetime import datetime, timedelta
from contextlib import asynccontextmanager
from platformq_shared.vault.vault_client import VaultClient
import pandas as pd
from cryptography.fernet import Fernet
import logging

logger = logging.getLogger(__name__)

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
        
    async def _setup_database_engines(self):
        """Ensure database secret engines are configured"""
        databases = {
            "postgres": {
                "plugin": "postgresql-database-plugin",
                "connection_url": "postgresql://{{username}}:{{password}}@postgres:5432/analytics?sslmode=require",
                "allowed_roles": ["readonly", "readwrite", "analytics"],
                "username": "vault_admin",
                "password": "${POSTGRES_ADMIN_PASSWORD}"
            },
            "cassandra": {
                "plugin": "cassandra-database-plugin",
                "hosts": "cassandra",
                "username": "vault_admin",
                "password": "${CASSANDRA_ADMIN_PASSWORD}",
                "protocol_version": 4,
                "allowed_roles": ["reader", "writer"]
            },
            "elasticsearch": {
                "plugin": "elasticsearch-database-plugin",
                "url": "https://elasticsearch:9200",
                "username": "vault_admin",
                "password": "${ELASTIC_ADMIN_PASSWORD}",
                "allowed_roles": ["search", "index"]
            }
        }
        
        for db_name, config in databases.items():
            try:
                await self.vault.configure_database_engine(db_name, config)
                logger.info(f"Configured database engine: {db_name}")
            except Exception as e:
                logger.error(f"Failed to configure {db_name}: {e}")
                
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
                    
            # Revoke credentials
            await self.vault.revoke_lease(lease_id)
            del self._credential_leases[f"{database}-{role}"]
            
    async def encrypt_dataframe_columns(self,
                                      df: pd.DataFrame,
                                      columns: List[str],
                                      encryption_type: str = "pii") -> pd.DataFrame:
        """Encrypt specific columns in a DataFrame"""
        # Get encryption key
        key = await self._get_encryption_key(f"{encryption_type}-encryption-key")
        fernet = Fernet(key)
        
        # Encrypt specified columns
        for column in columns:
            if column in df.columns:
                df[f"{column}_encrypted"] = df[column].apply(
                    lambda x: fernet.encrypt(str(x).encode()).decode() if pd.notna(x) else None
                )
                # Optionally drop original column
                # df = df.drop(column, axis=1)
                
        return df
        
    async def decrypt_dataframe_columns(self,
                                      df: pd.DataFrame,
                                      columns: List[str],
                                      encryption_type: str = "pii") -> pd.DataFrame:
        """Decrypt specific columns in a DataFrame"""
        # Get encryption key
        key = await self._get_encryption_key(f"{encryption_type}-encryption-key")
        fernet = Fernet(key)
        
        # Decrypt specified columns
        for column in columns:
            encrypted_col = f"{column}_encrypted"
            if encrypted_col in df.columns:
                df[column] = df[encrypted_col].apply(
                    lambda x: fernet.decrypt(x.encode()).decode() if pd.notna(x) else None
                )
                
        return df
        
    async def _get_encryption_key(self, key_name: str) -> bytes:
        """Get or cache encryption key"""
        if key_name not in self._encryption_keys:
            key_path = f"{self.service_name}/encryption/column-keys/{key_name}"
            key_data = await self.vault.get_secret(key_path)
            self._encryption_keys[key_name] = key_data["value"].encode()
            
        return self._encryption_keys[key_name]
        
    async def get_cloud_storage_client(self, provider: str, bucket: str = None):
        """Get authenticated cloud storage client"""
        if provider == "s3":
            creds = await self.vault.get_secret(
                f"{self.service_name}/cloud-storage/s3/data-lake-credentials"
            )
            
            import boto3
            return boto3.client(
                's3',
                aws_access_key_id=creds["access_key"],
                aws_secret_access_key=creds["secret_key"],
                region_name=creds.get("region", "us-east-1")
            )
            
        elif provider == "gcs":
            creds = await self.vault.get_secret(
                f"{self.service_name}/cloud-storage/gcs/analytics-bucket"
            )
            
            from google.cloud import storage
            return storage.Client.from_service_account_info(creds)
            
        elif provider == "azure":
            creds = await self.vault.get_secret(
                f"{self.service_name}/cloud-storage/azure/blob-storage-key"
            )
            
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
            
            # Trigger re-encryption job
            await self._trigger_reencryption_job(key_type)
            
        logger.info("Encryption key rotation initiated")
        
    async def _trigger_reencryption_job(self, key_type: str):
        """Trigger background job to re-encrypt data with new key"""
        # This would typically trigger an Airflow DAG or similar
        pass

# Specialized class for analytics workloads
class AnalyticsVaultIntegration(DataServiceVaultIntegration):
    """Extended Vault integration for analytics-specific needs"""
    
    async def get_databricks_client(self):
        """Get authenticated Databricks client"""
        creds = await self.vault.get_secret(
            f"{self.service_name}/analytics-tools/databricks/workspace-token"
        )
        
        from databricks import sql
        return sql.connect(
            server_hostname=creds["server_hostname"],
            http_path=creds["http_path"],
            access_token=creds["token"]
        )
        
    async def get_snowflake_connection(self, warehouse: str = "ANALYTICS_WH"):
        """Get Snowflake connection with rotating credentials"""
        creds = await self.vault.get_secret(
            f"{self.service_name}/analytics-tools/snowflake/account-credentials"
        )
        
        import snowflake.connector
        return snowflake.connector.connect(
            user=creds["username"],
            password=creds["password"],
            account=creds["account"],
            warehouse=warehouse,
            database=creds.get("database", "ANALYTICS"),
            schema=creds.get("schema", "PUBLIC")
        )
        
    @asynccontextmanager
    async def create_secure_table(self, 
                                table_name: str,
                                sensitive_columns: List[str]):
        """Create table with automatic column encryption"""
        async with self.get_database_connection("postgres", "readwrite") as conn:
            # Create table with encryption metadata
            await conn.execute(f"""
                CREATE TABLE IF NOT EXISTS {table_name}_metadata (
                    column_name TEXT PRIMARY KEY,
                    encryption_type TEXT,
                    key_version INT,
                    encrypted_at TIMESTAMP
                )
            """)
            
            # Store encryption metadata
            for column in sensitive_columns:
                await conn.execute(f"""
                    INSERT INTO {table_name}_metadata 
                    (column_name, encryption_type, key_version, encrypted_at)
                    VALUES ($1, $2, $3, $4)
                    ON CONFLICT (column_name) DO UPDATE
                    SET key_version = $3, encrypted_at = $4
                """, column, "pii", 1, datetime.utcnow())
                
            yield conn
```

## Consul Integration

### 1. Configuration Structure

```yaml
# Consul KV structure for data services
services/data-platform-service/
├── config/
│   ├── query-engine/
│   │   ├── default-timeout-seconds    # 300
│   │   ├── max-result-size-mb        # 100
│   │   ├── enable-query-cache        # true
│   │   ├── cache-ttl-seconds         # 3600
│   │   └── max-concurrent-queries    # 50
│   ├── data-quality/
│   │   ├── validation-rules/
│   │   │   ├── null-threshold        # 0.05
│   │   │   ├── duplicate-threshold   # 0.01
│   │   │   └── outlier-zscore       # 3
│   │   └── monitoring/
│   │       ├── check-frequency       # 300
│   │       └── alert-channels       # ["slack", "email"]
│   ├── retention-policies/
│   │   ├── raw-data-days           # 90
│   │   ├── aggregated-data-days    # 365
│   │   ├── archived-data-days      # 2555
│   │   └── pii-data-days          # 30
│   └── performance/
│       ├── partition-size-mb       # 128
│       ├── compression-type        # snappy
│       └── parallel-threads       # 8
├── data-catalog/
│   ├── datasets/
│   │   ├── users/
│   │   │   ├── schema            # {columns: [...]}
│   │   │   ├── owner             # data-team
│   │   │   ├── tags              # ["pii", "core"]
│   │   │   └── quality-score     # 0.95
│   │   └── transactions/
│   │       ├── schema
│   │       ├── lineage           # {upstream: [...]}
│   │       └── refresh-schedule  # "0 * * * *"
│   └── glossary/
│       ├── business-terms/
│       └── technical-terms/
└── pipeline-state/
    ├── active-jobs/
    │   ├── etl-user-data          # {status: "running", started: "..."}
    │   └── ml-feature-pipeline    # {status: "queued"}
    └── job-history/
        └── 2024-01-01/            # Historical job data
```

### 2. Implementation Code

```python
# data_service/consul_integration.py
from typing import Dict, Any, Optional, List, Set
import asyncio
from dataclasses import dataclass
from enum import Enum
from platformq_shared.consul.consul_client import ConsulClient
import logging
from datetime import datetime, timedelta

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

class DataServiceConsulIntegration:
    """Consul integration for data services"""
    
    def __init__(self, consul_client: ConsulClient, service_name: str = "data-platform-service"):
        self.consul = consul_client
        self.service_name = service_name
        self._query_config: Optional[QueryConfig] = None
        self._quality_rules: Optional[DataQualityRules] = None
        self._retention_policy: Optional[RetentionPolicy] = None
        self._dataset_registry: Dict[str, Dict] = {}
        self._active_queries: Set[str] = set()
        
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
        
    async def _register_service(self):
        """Register data service with Consul"""
        from platformq_shared.consul.consul_client import ServiceDefinition
        
        service = ServiceDefinition(
            name=self.service_name,
            port=8000,
            tags=["data", "analytics", "critical"],
            meta={
                "version": "2.0.0",
                "capabilities": "query,catalog,quality,lineage"
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
            outlier_zscore=float(quality_rules.get("outlier-zscore", 3.0))
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
        
    async def acquire_query_slot(self, query_id: str) -> bool:
        """Acquire slot for query execution with concurrency control"""
        config = await self.get_query_config()
        
        # Check current query count
        query_count_key = f"services/{self.service_name}/metrics/active-queries"
        current_count = await self.consul.kv_get(query_count_key, default=0)
        
        if current_count >= config.max_concurrent_queries:
            logger.warning(f"Query slot unavailable, {current_count} queries running")
            return False
            
        # Try to increment atomically
        success = await self.consul.kv_put_cas(
            query_count_key,
            current_count + 1,
            cas=current_count
        )
        
        if success:
            self._active_queries.add(query_id)
            
            # Record query start
            await self.consul.kv_put(
                f"services/{self.service_name}/pipeline-state/active-jobs/{query_id}",
                {
                    "status": "running",
                    "started": datetime.utcnow().isoformat(),
                    "node": await self._get_node_id()
                },
                ttl=config.default_timeout_seconds
            )
            
        return success
        
    async def release_query_slot(self, query_id: str):
        """Release query execution slot"""
        if query_id not in self._active_queries:
            return
            
        # Decrement query count
        query_count_key = f"services/{self.service_name}/metrics/active-queries"
        current_count = await self.consul.kv_get(query_count_key, default=1)
        await self.consul.kv_put(query_count_key, max(0, current_count - 1))
        
        # Remove from active jobs
        await self.consul.kv_delete(
            f"services/{self.service_name}/pipeline-state/active-jobs/{query_id}"
        )
        
        self._active_queries.discard(query_id)
        
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
            "quality_level": metadata.get("quality_level", DataQualityLevel.BRONZE.value)
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
        
        # Send alert if quality drops
        if overall_score < 0.8:
            await self._send_quality_alert(dataset_name, overall_score)
            
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
        
        # Execute stages
        for i, stage in enumerate(stages):
            # Acquire distributed lock for stage
            lock = await self.consul.acquire_lock(
                f"{pipeline_key}/stage-{stage}",
                ttl=600  # 10 minute lock
            )
            
            if not lock:
                logger.error(f"Could not acquire lock for stage {stage}")
                await self._mark_pipeline_failed(pipeline_name, stage)
                return False
                
            try:
                # Update current stage
                pipeline_state["current_stage"] = i
                pipeline_state["current_stage_name"] = stage
                await self.consul.kv_put(pipeline_key, pipeline_state)
                
                # Stage would be executed here
                logger.info(f"Executing pipeline stage: {stage}")
                
                # Mark stage complete
                pipeline_state["completed_stages"].append({
                    "stage": stage,
                    "completed_at": datetime.utcnow().isoformat()
                })
                
            finally:
                await lock.release()
                
        # Mark pipeline complete
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
                
        return {
            "num_partitions": num_partitions,
            "partition_size_mb": partition_size_mb,
            "partition_key": partition_key,
            "compression": perf_config.get("compression-type", "snappy"),
            "parallel_threads": int(perf_config.get("parallel-threads", 8))
        }

# Usage in data service
class SecureDataService:
    def __init__(self):
        self.vault = DataServiceVaultIntegration(vault_client)
        self.consul = DataServiceConsulIntegration(consul_client)
        
    async def execute_query(self, query: str, dataset: str) -> pd.DataFrame:
        # Get query configuration
        config = await self.consul.get_query_config()
        
        # Acquire query slot
        query_id = f"query-{datetime.utcnow().timestamp()}"
        if not await self.consul.acquire_query_slot(query_id):
            raise Exception("Too many concurrent queries")
            
        try:
            # Get dataset metadata
            metadata = await self.consul.get_dataset_metadata(dataset)
            if not metadata:
                raise Exception(f"Dataset {dataset} not found")
                
            # Check permissions based on quality level
            if metadata["quality_level"] != DataQualityLevel.GOLD.value:
                logger.warning(f"Querying non-gold dataset: {dataset}")
                
            # Execute query with dynamic credentials
            async with self.vault.get_database_connection("postgres", "readonly") as conn:
                # Set query timeout
                await conn.execute(f"SET statement_timeout = {config.default_timeout_seconds * 1000}")
                
                # Execute query
                results = await conn.fetch(query)
                
                # Convert to DataFrame
                df = pd.DataFrame(results)
                
                # Decrypt sensitive columns if needed
                if "pii" in metadata.get("tags", []):
                    sensitive_columns = metadata.get("sensitive_columns", [])
                    df = await self.vault.decrypt_dataframe_columns(df, sensitive_columns)
                    
                return df
                
        finally:
            await self.consul.release_query_slot(query_id)
```

## Testing & Monitoring

### 1. Integration Tests

```python
# tests/test_data_vault_integration.py
import pytest
from data_service.vault_integration import DataServiceVaultIntegration

@pytest.mark.integration
async def test_dynamic_credentials(vault_client):
    integration = DataServiceVaultIntegration(vault_client)
    
    # Test credential generation and cleanup
    async with integration.get_database_connection("postgres", "readonly") as conn:
        # Should be able to query
        result = await conn.fetchval("SELECT current_user")
        assert result.startswith("v-")  # Vault-generated username
        
    # After context exit, credentials should be revoked
    # Attempting to use same credentials should fail

@pytest.mark.integration
async def test_column_encryption(vault_client):
    integration = DataServiceVaultIntegration(vault_client)
    
    # Test data
    df = pd.DataFrame({
        'user_id': [1, 2, 3],
        'email': ['user1@example.com', 'user2@example.com', 'user3@example.com'],
        'balance': [100.0, 200.0, 300.0]
    })
    
    # Encrypt PII columns
    encrypted_df = await integration.encrypt_dataframe_columns(
        df, ['email'], 'pii'
    )
    
    # Verify encryption
    assert 'email_encrypted' in encrypted_df.columns
    assert encrypted_df['email_encrypted'][0].startswith('gAAAAA')  # Fernet prefix
    
    # Decrypt
    decrypted_df = await integration.decrypt_dataframe_columns(
        encrypted_df, ['email'], 'pii'
    )
    
    # Verify decryption
    assert decrypted_df['email'][0] == 'user1@example.com'
```

### 2. Monitoring Dashboards

```yaml
# Grafana dashboard for data service
dashboard:
  title: "Data Service - Vault & Consul Integration"
  panels:
    - title: "Active Database Connections"
      query: |
        sum(data_service_db_connections_active) by (database, role)
        
    - title: "Credential Rotation Rate"
      query: |
        rate(vault_database_creds_created_total[5m])
        
    - title: "Query Concurrency"
      query: |
        data_service_active_queries / data_service_max_queries
        
    - title: "Data Quality Scores"
      query: |
        data_service_quality_score by (dataset, level)
        
    - title: "Encryption Operations"
      query: |
        rate(data_service_encryption_operations_total[5m]) by (operation)
```

## Security Best Practices

### 1. Credential Lifecycle

```python
# Always use context managers for database connections
async with vault.get_database_connection("postgres", "readonly", ttl="5m") as conn:
    # Connection automatically closed and credentials revoked
    pass

# Never cache credentials
# BAD
creds = await vault.generate_database_credentials("postgres", "readwrite")
global_connection = await connect(creds)  # DON'T DO THIS

# GOOD
async with vault.get_database_connection("postgres", "readwrite") as conn:
    # Use connection only within context
    pass
```

### 2. Data Classification

```yaml
# Consul data classification
data-catalog/classifications/
  pii:
    columns: ["email", "phone", "ssn", "address"]
    encryption: required
    retention_days: 30
    access_level: restricted
    
  financial:
    columns: ["credit_card", "bank_account", "salary"]
    encryption: required
    retention_days: 90
    access_level: confidential
    
  public:
    columns: ["product_name", "category", "description"]
    encryption: optional
    retention_days: unlimited
    access_level: public
```

### 3. Query Auditing

```python
class AuditedDataService(SecureDataService):
    async def execute_query(self, query: str, dataset: str, user_context: Dict) -> pd.DataFrame:
        # Log query execution
        audit_entry = {
            "timestamp": datetime.utcnow().isoformat(),
            "user": user_context["user_id"],
            "dataset": dataset,
            "query_hash": hashlib.sha256(query.encode()).hexdigest(),
            "query_length": len(query),
            "ip_address": user_context.get("ip_address")
        }
        
        # Store in Consul for real-time monitoring
        await self.consul.kv_put(
            f"audit/queries/{datetime.utcnow().strftime('%Y%m%d')}/{uuid.uuid4()}",
            audit_entry,
            ttl=2592000  # 30 days
        )
        
        # Execute query
        return await super().execute_query(query, dataset)
``` 