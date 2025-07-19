"""
Data Platform Service with Vault & Consul Integration
"""

from fastapi import FastAPI, Depends, HTTPException, Request, UploadFile, File
from contextlib import asynccontextmanager
import asyncio
from typing import Dict, Any, Optional, List
from datetime import datetime
import logging
import os
import pandas as pd
import json

from platformq_shared.vault.vault_client import VaultClient
from platformq_shared.consul.consul_client import ConsulClient
from platformq_shared.middleware.security_middleware import SecurityMiddleware

from .vault_consul_integration import (
    DataServiceVaultIntegration,
    DataServiceConsulIntegration,
    QueryConfig,
    DataQualityRules,
    RetentionPolicy,
    DataQualityLevel
)
from .models import Query, QueryResult, Dataset, DataQualityReport
from .analytics import DruidAnalyticsEngine

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class DataPlatformService:
    """Data Platform Service with Vault & Consul Integration"""
    
    def __init__(self):
        self.app = FastAPI(title="Data Platform Service", version="2.0.0")
        self.vault_integration: Optional[DataServiceVaultIntegration] = None
        self.consul_integration: Optional[DataServiceConsulIntegration] = None
        self._query_cache: Dict[str, Any] = {}
        self.druid_engine: Optional[DruidAnalyticsEngine] = None
        
    @asynccontextmanager
    async def lifespan(self, app: FastAPI):
        """Application lifespan management"""
        # Startup
        await self.startup()
        yield
        # Shutdown
        await self.shutdown()
        
    async def startup(self):
        """Service startup procedure"""
        logger.info("Starting Data Platform Service with Vault & Consul integration")
        
        try:
            # Initialize Vault client
            vault_client = VaultClient(
                vault_addr=os.getenv("VAULT_ADDR", "http://vault:8200"),
                role_id=os.getenv("VAULT_ROLE_ID"),
                secret_id=os.getenv("VAULT_SECRET_ID")
            )
            await vault_client.initialize()
            
            # Initialize Consul client
            consul_client = ConsulClient(
                host=os.getenv("CONSUL_HOST", "consul"),
                port=int(os.getenv("CONSUL_PORT", "8500"))
            )
            
            # Initialize integrations
            self.vault_integration = DataServiceVaultIntegration(vault_client)
            await self.vault_integration.initialize()
            
            self.consul_integration = DataServiceConsulIntegration(consul_client)
            await self.consul_integration.initialize()
            
            # Initialize Druid analytics engine
            druid_config = {
                'coordinator_url': os.getenv('DRUID_COORDINATOR_URL', 'http://druid-coordinator:8081'),
                'broker_url': os.getenv('DRUID_BROKER_URL', 'http://druid-broker:8082'),
                'overlord_url': os.getenv('DRUID_OVERLORD_URL', 'http://druid-overlord:8090')
            }
            self.druid_engine = DruidAnalyticsEngine(druid_config)
            logger.info("Initialized Druid analytics engine")
            
            # Set up routes
            self._setup_routes()
            
            # Add security middleware
            security_middleware = SecurityMiddleware(
                vault_client=vault_client,
                consul_client=consul_client,
                service_name="data-platform-service"
            )
            self.app.add_middleware(security_middleware)
            
            # Start background tasks
            asyncio.create_task(self._health_check_loop())
            asyncio.create_task(self._data_quality_monitor())
            asyncio.create_task(self._retention_policy_enforcer())
            
            logger.info("Data Platform Service started successfully")
            
        except Exception as e:
            logger.error(f"Failed to start Data Platform Service: {e}")
            raise
            
    async def shutdown(self):
        """Service shutdown procedure"""
        logger.info("Shutting down Data Platform Service")
        
        # Cancel background tasks
        for task in asyncio.all_tasks():
            if task.get_name() in ["health_check", "quality_monitor", "retention_enforcer"]:
                task.cancel()
        
        # Close Druid engine
        if self.druid_engine:
            await self.druid_engine.close()
            logger.info("Closed Druid analytics engine")
                
        # Deregister from Consul
        if self.consul_integration:
            await self.consul_integration.consul.deregister_service()
            
        logger.info("Data Platform Service shutdown complete")
        
    def _setup_routes(self):
        """Set up API routes"""
        
        @self.app.get("/health")
        async def health_check():
            """Health check endpoint"""
            try:
                # Check Vault connectivity
                vault_healthy = await self._check_vault_health()
                
                # Check Consul connectivity
                consul_healthy = await self._check_consul_health()
                
                # Check database connections
                db_health = await self._check_database_health()
                
                overall_status = "healthy"
                if not all([vault_healthy, consul_healthy]):
                    overall_status = "unhealthy"
                elif not all(db_health.values()):
                    overall_status = "degraded"
                    
                health_data = {
                    "status": overall_status,
                    "service": "data-platform-service",
                    "checks": {
                        "vault": "healthy" if vault_healthy else "unhealthy",
                        "consul": "healthy" if consul_healthy else "unhealthy",
                        "databases": db_health
                    },
                    "timestamp": datetime.utcnow().isoformat()
                }
                
                if overall_status == "unhealthy":
                    raise HTTPException(status_code=503, detail=health_data)
                    
                return health_data
                
            except Exception as e:
                logger.error(f"Health check failed: {e}")
                raise HTTPException(status_code=503, detail="Service unhealthy")
                
        @self.app.post("/api/v1/query")
        async def execute_query(query: Query, request: Request):
            """Execute a data query"""
            # Get query configuration
            config = await self.consul_integration.get_query_config()
            
            # Generate query ID
            query_id = f"query-{datetime.utcnow().timestamp()}"
            
            # Acquire query slot
            if not await self.consul_integration.acquire_query_slot(query_id):
                raise HTTPException(429, "Too many concurrent queries")
                
            try:
                # Get dataset metadata
                metadata = await self.consul_integration.get_dataset_metadata(query.dataset)
                if not metadata:
                    raise HTTPException(404, f"Dataset {query.dataset} not found")
                    
                # Check permissions based on quality level
                if metadata["quality_level"] != DataQualityLevel.GOLD.value:
                    logger.warning(f"Querying non-gold dataset: {query.dataset}")
                    
                # Check cache if enabled
                cache_key = f"{query.dataset}:{hash(query.sql)}"
                if config.enable_query_cache and cache_key in self._query_cache:
                    cached = self._query_cache[cache_key]
                    if (datetime.utcnow() - cached["timestamp"]).seconds < config.cache_ttl_seconds:
                        return cached["result"]
                        
                # Execute query with dynamic credentials
                async with self.vault_integration.get_database_connection(
                    query.database or "postgres",
                    "readonly"
                ) as conn:
                    # Set query timeout
                    if query.database == "postgres":
                        await conn.execute(
                            f"SET statement_timeout = {config.default_timeout_seconds * 1000}"
                        )
                        
                    # Execute query
                    if query.database == "postgres":
                        results = await conn.fetch(query.sql)
                        df = pd.DataFrame(results)
                    else:
                        # For other databases, adapt as needed
                        results = conn.execute(query.sql)
                        df = pd.DataFrame(results.fetchall())
                        
                    # Check result size
                    size_mb = df.memory_usage(deep=True).sum() / 1024 / 1024
                    if size_mb > config.max_result_size_mb:
                        raise HTTPException(413, f"Result size {size_mb}MB exceeds limit")
                        
                    # Decrypt sensitive columns if needed
                    if "pii" in metadata.get("tags", []):
                        sensitive_columns = metadata.get("sensitive_columns", [])
                        if sensitive_columns:
                            df = await self.vault_integration.decrypt_dataframe_columns(
                                df, sensitive_columns, "pii"
                            )
                            
                    # Convert to result format
                    result = QueryResult(
                        query_id=query_id,
                        dataset=query.dataset,
                        row_count=len(df),
                        columns=list(df.columns),
                        data=df.to_dict(orient="records"),
                        execution_time_ms=0,  # Would track actual time
                        cached=False
                    )
                    
                    # Cache result
                    if config.enable_query_cache:
                        self._query_cache[cache_key] = {
                            "result": result,
                            "timestamp": datetime.utcnow()
                        }
                        
                    return result
                    
            finally:
                await self.consul_integration.release_query_slot(query_id)
                
        @self.app.post("/api/v1/datasets")
        async def register_dataset(dataset: Dataset):
            """Register a new dataset in the catalog"""
            # Prepare metadata
            metadata = {
                "owner": dataset.owner,
                "tags": dataset.tags,
                "quality_score": 0.0,  # Will be calculated
                "created_at": datetime.utcnow().isoformat(),
                "quality_level": DataQualityLevel.BRONZE.value,
                "sensitive_columns": dataset.sensitive_columns,
                "upstream_datasets": dataset.upstream_datasets
            }
            
            # Register in Consul
            await self.consul_integration.register_dataset(
                dataset.name,
                dataset.schema,
                metadata
            )
            
            return {"status": "success", "dataset": dataset.name}
            
        @self.app.get("/api/v1/datasets")
        async def list_datasets():
            """List all datasets in the catalog"""
            datasets = []
            
            for name, metadata in self.consul_integration._dataset_registry.items():
                datasets.append({
                    "name": name,
                    "owner": metadata.get("owner"),
                    "quality_level": metadata.get("quality_level"),
                    "quality_score": metadata.get("quality_score"),
                    "tags": metadata.get("tags", []),
                    "created_at": metadata.get("created_at"),
                    "last_updated": metadata.get("last_updated")
                })
                
            return {"datasets": datasets}
            
        @self.app.get("/api/v1/datasets/{dataset_name}")
        async def get_dataset_info(dataset_name: str):
            """Get detailed information about a dataset"""
            metadata = await self.consul_integration.get_dataset_metadata(dataset_name)
            
            if not metadata:
                raise HTTPException(404, f"Dataset {dataset_name} not found")
                
            return metadata
            
        @self.app.post("/api/v1/encrypt")
        async def encrypt_data(
            file: UploadFile = File(...),
            columns: List[str] = [],
            encryption_type: str = "pii"
        ):
            """Encrypt sensitive columns in uploaded data"""
            # Read CSV file
            content = await file.read()
            df = pd.read_csv(pd.io.common.BytesIO(content))
            
            # Encrypt specified columns
            encrypted_df = await self.vault_integration.encrypt_dataframe_columns(
                df, columns, encryption_type
            )
            
            # Return encrypted CSV
            return {
                "status": "success",
                "encrypted_columns": [f"{col}_encrypted" for col in columns],
                "rows": len(encrypted_df),
                "data": encrypted_df.to_csv(index=False)
            }
            
        @self.app.post("/api/v1/quality/check")
        async def check_data_quality(
            dataset_name: str,
            sample_size: Optional[int] = None
        ):
            """Run data quality checks on a dataset"""
            # Get dataset metadata
            metadata = await self.consul_integration.get_dataset_metadata(dataset_name)
            if not metadata:
                raise HTTPException(404, f"Dataset {dataset_name} not found")
                
            # Get quality rules
            rules = await self.consul_integration.get_quality_rules()
            
            # In production, this would run actual quality checks
            # For now, simulate quality metrics
            import random
            
            quality_metrics = {
                "completeness": random.uniform(0.85, 0.99),
                "accuracy": random.uniform(0.80, 0.98),
                "consistency": random.uniform(0.82, 0.97),
                "timeliness": random.uniform(0.90, 1.0)
            }
            
            # Update quality score
            await self.consul_integration.update_data_quality_score(
                dataset_name,
                quality_metrics
            )
            
            return DataQualityReport(
                dataset=dataset_name,
                metrics=quality_metrics,
                rules_applied=rules.__dict__,
                timestamp=datetime.utcnow()
            )
            
        @self.app.post("/api/v1/etl/pipeline")
        async def create_etl_pipeline(
            pipeline_name: str,
            stages: List[str],
            source_dataset: str,
            target_dataset: str
        ):
            """Create and execute an ETL pipeline"""
            # Coordinate pipeline execution
            success = await self.consul_integration.coordinate_etl_pipeline(
                pipeline_name,
                stages
            )
            
            if success:
                return {
                    "status": "success",
                    "pipeline": pipeline_name,
                    "stages": stages,
                    "source": source_dataset,
                    "target": target_dataset
                }
            else:
                raise HTTPException(500, "Pipeline execution failed")
                
        @self.app.get("/api/v1/partitioning/strategy")
        async def get_partitioning_strategy(
            dataset_name: str,
            estimated_size_gb: float
        ):
            """Get optimal partitioning strategy for a dataset"""
            strategy = await self.consul_integration.get_optimal_partition_strategy(
                dataset_name,
                estimated_size_gb
            )
            
            return strategy
            
        @self.app.post("/api/v1/analytics/connect/{tool}")
        async def connect_analytics_tool(tool: str):
            """Get connection details for analytics tools"""
            try:
                credentials = await self.vault_integration.get_analytics_credentials(tool)
                
                # Return connection info (without sensitive data in logs)
                return {
                    "tool": tool,
                    "connection_available": True,
                    "host": credentials.get("host") or credentials.get("server"),
                    "instructions": f"Use provided credentials to connect to {tool}"
                }
                
            except ValueError as e:
                raise HTTPException(400, str(e))
            except Exception as e:
                logger.error(f"Failed to get {tool} credentials: {e}")
                raise HTTPException(500, f"Failed to get {tool} connection")
        
        @self.app.post("/api/v1/analytics/timeseries")
        async def query_timeseries(
            datasource: str,
            metrics: List[str],
            granularity: str = "hour",
            filter: Optional[Dict[str, Any]] = None,
            start_time: Optional[datetime] = None,
            end_time: Optional[datetime] = None
        ):
            """Query time-series data from Druid"""
            try:
                if not self.druid_engine:
                    raise HTTPException(503, "Druid analytics engine not available")
                
                results = await self.druid_engine.query_timeseries(
                    datasource=datasource,
                    metrics=metrics,
                    granularity=granularity,
                    filter=filter,
                    start_time=start_time,
                    end_time=end_time
                )
                
                return {
                    "datasource": datasource,
                    "metrics": metrics,
                    "data": results
                }
                
            except Exception as e:
                logger.error(f"Druid timeseries query failed: {e}")
                raise HTTPException(500, f"Analytics query failed: {str(e)}")
        
        @self.app.post("/api/v1/analytics/groupby")
        async def query_groupby(
            datasource: str,
            dimensions: List[str],
            metrics: List[str],
            filter: Optional[Dict[str, Any]] = None,
            start_time: Optional[datetime] = None,
            end_time: Optional[datetime] = None,
            limit: int = 100
        ):
            """Execute group-by analytics query on Druid"""
            try:
                if not self.druid_engine:
                    raise HTTPException(503, "Druid analytics engine not available")
                
                results = await self.druid_engine.query_groupby(
                    datasource=datasource,
                    dimensions=dimensions,
                    metrics=metrics,
                    filter=filter,
                    start_time=start_time,
                    end_time=end_time,
                    limit=limit
                )
                
                return {
                    "datasource": datasource,
                    "dimensions": dimensions,
                    "metrics": metrics,
                    "data": results
                }
                
            except Exception as e:
                logger.error(f"Druid groupby query failed: {e}")
                raise HTTPException(500, f"Analytics query failed: {str(e)}")
        
        @self.app.post("/api/v1/analytics/ingest")
        async def ingest_analytics_data(
            datasource: str,
            data: List[Dict[str, Any]],
            timestamp_column: str = "timestamp"
        ):
            """Ingest data into Druid for analytics"""
            try:
                if not self.druid_engine:
                    raise HTTPException(503, "Druid analytics engine not available")
                
                result = await self.druid_engine.ingest_batch(
                    datasource=datasource,
                    data=data,
                    timestamp_column=timestamp_column
                )
                
                return result
                
            except Exception as e:
                logger.error(f"Druid data ingestion failed: {e}")
                raise HTTPException(500, f"Data ingestion failed: {str(e)}")
        
        @self.app.get("/api/v1/analytics/datasources")
        async def list_datasources():
            """List available Druid datasources"""
            try:
                if not self.druid_engine:
                    raise HTTPException(503, "Druid analytics engine not available")
                
                datasources = await self.druid_engine.get_datasources()
                
                return {
                    "datasources": datasources,
                    "count": len(datasources)
                }
                
            except Exception as e:
                logger.error(f"Failed to list datasources: {e}")
                raise HTTPException(500, f"Failed to list datasources: {str(e)}")
                
        @self.app.post("/api/v1/keys/rotate")
        async def rotate_encryption_keys():
            """Rotate column encryption keys"""
            try:
                await self.vault_integration.rotate_encryption_keys()
                return {"status": "success", "message": "Encryption keys rotated"}
            except Exception as e:
                logger.error(f"Key rotation failed: {e}")
                raise HTTPException(500, f"Key rotation failed: {str(e)}")
                
    async def _check_vault_health(self) -> bool:
        """Check Vault connectivity"""
        try:
            await self.vault_integration.vault.get_secret("data-platform-service/health-check")
            return True
        except:
            return False
            
    async def _check_consul_health(self) -> bool:
        """Check Consul connectivity"""
        try:
            await self.consul_integration.consul.kv_get("services/data-platform-service/health/status")
            return True
        except:
            return False
            
    async def _check_database_health(self) -> Dict[str, bool]:
        """Check database connections"""
        db_health = {}
        databases = ["postgres", "cassandra", "elasticsearch", "druid"]
        
        for db in databases:
            try:
                async with self.vault_integration.get_database_connection(db, "readonly", "30s") as conn:
                    # Simple connectivity check
                    if db == "postgres":
                        await conn.fetchval("SELECT 1")
                    elif db == "elasticsearch":
                        await conn.ping()
                    elif db == "druid":
                        # Druid health check via REST API
                        import httpx
                        async with httpx.AsyncClient() as client:
                            response = await client.get("http://druid-broker:8082/status/health")
                            response.raise_for_status()
                    
                db_health[db] = True
            except:
                db_health[db] = False
                
        return db_health
        
    async def _health_check_loop(self):
        """Periodic health check"""
        while True:
            try:
                await asyncio.sleep(30)  # Every 30 seconds
                
                # Check database health
                db_health = await self._check_database_health()
                
                for db, healthy in db_health.items():
                    await self.consul_integration.consul.kv_put(
                        f"services/data-platform-service/health/databases/{db}",
                        "healthy" if healthy else "unhealthy"
                    )
                    
            except Exception as e:
                logger.error(f"Health check loop error: {e}")
                
    async def _data_quality_monitor(self):
        """Monitor data quality for registered datasets"""
        while True:
            try:
                await asyncio.sleep(3600)  # Every hour
                
                # Check quality for each dataset
                for dataset_name in self.consul_integration._dataset_registry:
                    try:
                        # In production, this would run actual quality checks
                        logger.info(f"Running quality check for {dataset_name}")
                    except Exception as e:
                        logger.error(f"Quality check failed for {dataset_name}: {e}")
                        
            except Exception as e:
                logger.error(f"Data quality monitor error: {e}")
                
    async def _retention_policy_enforcer(self):
        """Enforce data retention policies"""
        while True:
            try:
                await asyncio.sleep(86400)  # Daily
                
                policy = await self.consul_integration.get_retention_policy()
                
                # Check each dataset
                for dataset_name, metadata in self.consul_integration._dataset_registry.items():
                    created_at = datetime.fromisoformat(metadata.get("created_at", datetime.utcnow().isoformat()))
                    
                    # Check if data should be archived or deleted
                    should_retain = await self.consul_integration.check_retention_policy(
                        dataset_name,
                        created_at
                    )
                    
                    if not should_retain:
                        logger.info(f"Dataset {dataset_name} exceeded retention policy")
                        # In production, would trigger archival/deletion
                        
            except Exception as e:
                logger.error(f"Retention policy enforcer error: {e}")


# Create app instance
data_service = DataPlatformService()
app = data_service.app

# Set up lifespan
app.router.lifespan_context = data_service.lifespan

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000) 