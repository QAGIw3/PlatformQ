"""
Digital Integration Hub API Router

Provides endpoints for the in-memory data integration hub.
"""

from fastapi import APIRouter, Depends, HTTPException, Query, Body
from typing import Dict, List, Any, Optional, Tuple
from datetime import datetime
from pydantic import BaseModel, Field

from ..auth.dependencies import get_current_user, require_admin
from ..dih.digital_integration_hub import (
    DigitalIntegrationHub,
    CacheRegion,
    DataSourceConfig,
    APIEndpoint,
    DataSource,
    CacheStrategy,
    ConsistencyLevel,
    DataEntity
)

router = APIRouter(prefix="/dih", tags=["digital_integration_hub"])

# Global DIH instance (initialized in main.py)
dih_instance: Optional[DigitalIntegrationHub] = None


def get_dih() -> DigitalIntegrationHub:
    """Get DIH instance"""
    if not dih_instance:
        raise HTTPException(status_code=500, detail="DIH not initialized")
    return dih_instance


# Request/Response Models
class CreateCacheRegionRequest(BaseModel):
    name: str = Field(..., description="Cache region name")
    cache_mode: str = Field(default="PARTITIONED", description="Cache mode: PARTITIONED, REPLICATED, LOCAL")
    backups: int = Field(default=1, description="Number of backup copies")
    atomicity_mode: str = Field(default="TRANSACTIONAL", description="ATOMIC or TRANSACTIONAL")
    cache_strategy: str = Field(default="WRITE_THROUGH", description="Cache strategy")
    eviction_policy: str = Field(default="LRU", description="Eviction policy: LRU, FIFO, RANDOM")
    eviction_max_size: int = Field(default=10000000, description="Maximum cache size")
    expiry_policy: Optional[Dict[str, int]] = Field(default=None, description="TTL settings")
    indexes: List[Tuple[str, str]] = Field(default_factory=list, description="Field indexes")


class RegisterDataSourceRequest(BaseModel):
    source_name: str = Field(..., description="Data source name")
    source_type: str = Field(..., description="Source type")
    connection_params: Dict[str, Any] = Field(..., description="Connection parameters")
    target_regions: List[str] = Field(..., description="Target cache regions")
    sync_interval_seconds: Optional[int] = Field(default=None, description="Sync interval")
    batch_size: int = Field(default=1000, description="Batch size for sync")
    consistency_level: str = Field(default="EVENTUAL", description="Consistency level")
    transform_function: Optional[str] = Field(default=None, description="Transform function path")


class RegisterAPIEndpointRequest(BaseModel):
    path: str = Field(..., description="API endpoint path")
    method: str = Field(default="GET", description="HTTP method")
    cache_regions: List[str] = Field(..., description="Cache regions to query")
    query_template: Optional[str] = Field(default=None, description="SQL query template")
    cache_key_pattern: Optional[str] = Field(default=None, description="Cache key pattern")
    ttl_seconds: Optional[int] = Field(default=300, description="Cache TTL")
    auth_required: bool = Field(default=True, description="Authentication required")


class ExecuteTransactionRequest(BaseModel):
    operations: List[Dict[str, Any]] = Field(..., description="Transaction operations")
    isolation: str = Field(default="REPEATABLE_READ", description="Isolation level")
    concurrency: str = Field(default="PESSIMISTIC", description="Concurrency mode")
    timeout: int = Field(default=5000, description="Timeout in milliseconds")


class BulkLoadRequest(BaseModel):
    region_name: str = Field(..., description="Target cache region")
    data: List[Dict[str, Any]] = Field(..., description="Data to load")
    batch_size: int = Field(default=10000, description="Batch size")


# Endpoints
@router.post("/cache-regions")
async def create_cache_region(
    request: CreateCacheRegionRequest,
    current_user=Depends(get_current_user),
    dih: DigitalIntegrationHub = Depends(get_dih)
) -> Dict[str, Any]:
    """Create a new cache region"""
    try:
        # Convert request to CacheRegion
        region = CacheRegion(
            name=request.name,
            cache_mode=request.cache_mode,
            backups=request.backups,
            atomicity_mode=request.atomicity_mode,
            cache_strategy=CacheStrategy(request.cache_strategy),
            eviction_policy=request.eviction_policy,
            eviction_max_size=request.eviction_max_size,
            expiry_policy=request.expiry_policy,
            indexes=request.indexes
        )
        
        # Create region
        await dih.create_cache_region(region)
        
        return {
            "status": "success",
            "region_name": request.name,
            "message": f"Cache region '{request.name}' created successfully"
        }
        
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))


@router.get("/cache-regions")
async def list_cache_regions(
    current_user=Depends(get_current_user),
    dih: DigitalIntegrationHub = Depends(get_dih)
) -> List[Dict[str, Any]]:
    """List all cache regions"""
    regions = []
    
    for name, region in dih.cache_regions.items():
        regions.append({
            "name": name,
            "cache_mode": region.cache_mode,
            "backups": region.backups,
            "atomicity_mode": region.atomicity_mode,
            "cache_strategy": region.cache_strategy.value,
            "eviction_policy": region.eviction_policy,
            "eviction_max_size": region.eviction_max_size,
            "indexes": region.indexes
        })
        
    return regions


@router.get("/cache-regions/{region_name}/stats")
async def get_cache_statistics(
    region_name: str,
    current_user=Depends(get_current_user),
    dih: DigitalIntegrationHub = Depends(get_dih)
) -> Dict[str, Any]:
    """Get statistics for a cache region"""
    try:
        return await dih.get_cache_statistics(region_name)
    except ValueError as e:
        raise HTTPException(status_code=404, detail=str(e))


@router.post("/data-sources")
async def register_data_source(
    request: RegisterDataSourceRequest,
    current_user=Depends(get_current_user),
    dih: DigitalIntegrationHub = Depends(get_dih)
) -> Dict[str, Any]:
    """Register a new data source"""
    try:
        # Convert request to DataSourceConfig
        config = DataSourceConfig(
            source_type=DataSource(request.source_type),
            connection_params=request.connection_params,
            sync_interval_seconds=request.sync_interval_seconds,
            batch_size=request.batch_size,
            consistency_level=ConsistencyLevel(request.consistency_level),
            transform_function=request.transform_function
        )
        
        # Register source
        await dih.register_data_source(
            request.source_name,
            config,
            request.target_regions
        )
        
        return {
            "status": "success",
            "source_name": request.source_name,
            "message": f"Data source '{request.source_name}' registered successfully"
        }
        
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))


@router.get("/data-sources")
async def list_data_sources(
    current_user=Depends(get_current_user),
    dih: DigitalIntegrationHub = Depends(get_dih)
) -> List[Dict[str, Any]]:
    """List all registered data sources"""
    sources = []
    
    for name, config in dih.data_sources.items():
        sources.append({
            "name": name,
            "source_type": config.source_type.value,
            "sync_interval_seconds": config.sync_interval_seconds,
            "batch_size": config.batch_size,
            "consistency_level": config.consistency_level.value,
            "sync_active": name in dih.sync_tasks
        })
        
    return sources


@router.post("/api-endpoints")
async def register_api_endpoint(
    request: RegisterAPIEndpointRequest,
    current_user=Depends(require_admin),
    dih: DigitalIntegrationHub = Depends(get_dih)
) -> Dict[str, Any]:
    """Register a new API endpoint"""
    try:
        # Convert request to APIEndpoint
        endpoint = APIEndpoint(
            path=request.path,
            method=request.method,
            cache_regions=request.cache_regions,
            query_template=request.query_template,
            cache_key_pattern=request.cache_key_pattern,
            ttl_seconds=request.ttl_seconds,
            auth_required=request.auth_required
        )
        
        # Register endpoint
        await dih.register_api_endpoint(endpoint)
        
        return {
            "status": "success",
            "endpoint_path": request.path,
            "message": f"API endpoint '{request.path}' registered successfully"
        }
        
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))


@router.post("/query/{endpoint_path:path}")
async def execute_api_query(
    endpoint_path: str,
    params: Dict[str, Any] = Body(...),
    current_user=Depends(get_current_user),
    dih: DigitalIntegrationHub = Depends(get_dih)
) -> Any:
    """Execute a query through DIH API endpoint"""
    try:
        # Add user context
        user_context = {
            "user_id": current_user.user_id,
            "tenant_id": current_user.tenant_id,
            "roles": current_user.roles
        }
        
        # Execute query
        result = await dih.execute_api_query(
            endpoint_path,
            params,
            user_context
        )
        
        return result
        
    except ValueError as e:
        raise HTTPException(status_code=404, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/transactions")
async def execute_transaction(
    request: ExecuteTransactionRequest,
    current_user=Depends(get_current_user),
    dih: DigitalIntegrationHub = Depends(get_dih)
) -> Dict[str, Any]:
    """Execute an ACID transaction"""
    try:
        # Convert operations
        operations = []
        for op in request.operations:
            operations.append((
                op["region"],
                op["operation"],
                op["data"]
            ))
            
        # Execute transaction
        success = await dih.execute_transaction(
            operations,
            isolation=request.isolation,
            concurrency=request.concurrency,
            timeout=request.timeout
        )
        
        return {
            "status": "success" if success else "failed",
            "committed": success
        }
        
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))


@router.post("/sql-query")
async def execute_sql_query(
    query: str = Body(..., embed=True),
    params: Optional[List[Any]] = Body(default=None),
    page_size: int = Query(default=1000),
    current_user=Depends(get_current_user),
    dih: DigitalIntegrationHub = Depends(get_dih)
) -> List[Dict[str, Any]]:
    """Execute SQL query across cache regions"""
    try:
        # Add tenant filter to query if not admin
        if "admin" not in current_user.roles:
            # Simple tenant filtering (would be more sophisticated in production)
            if "WHERE" in query.upper():
                query += f" AND tenant_id = '{current_user.tenant_id}'"
            else:
                query += f" WHERE tenant_id = '{current_user.tenant_id}'"
                
        # Execute query
        results = await dih.query_cross_region(query, params, page_size)
        
        return results
        
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))


@router.post("/bulk-load")
async def bulk_load_data(
    request: BulkLoadRequest,
    current_user=Depends(require_admin),
    dih: DigitalIntegrationHub = Depends(get_dih)
) -> Dict[str, Any]:
    """Bulk load data into cache region"""
    try:
        # Convert data to tuples
        data_tuples = []
        for item in request.data:
            key = item.get("key") or item.get("id")
            if not key:
                raise ValueError("Each item must have 'key' or 'id' field")
            data_tuples.append((key, item))
            
        # Bulk load
        result = await dih.bulk_load(
            request.region_name,
            data_tuples,
            request.batch_size
        )
        
        return result
        
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))


@router.post("/cache-regions/{region_name}/invalidate")
async def invalidate_cache(
    region_name: str,
    pattern: Optional[str] = Query(default=None),
    current_user=Depends(require_admin),
    dih: DigitalIntegrationHub = Depends(get_dih)
) -> Dict[str, Any]:
    """Invalidate cache entries"""
    try:
        await dih.invalidate_cache(region_name, pattern)
        
        return {
            "status": "success",
            "region_name": region_name,
            "pattern": pattern,
            "message": f"Cache invalidated successfully"
        }
        
    except ValueError as e:
        raise HTTPException(status_code=404, detail=str(e))


@router.get("/metrics")
async def get_dih_metrics(
    current_user=Depends(get_current_user),
    dih: DigitalIntegrationHub = Depends(get_dih)
) -> Dict[str, Any]:
    """Get DIH performance metrics"""
    return dict(dih.metrics)


# Predefined API endpoints for common queries
@router.post("/_init_default_endpoints")
async def initialize_default_endpoints(
    current_user=Depends(require_admin),
    dih: DigitalIntegrationHub = Depends(get_dih)
) -> Dict[str, Any]:
    """Initialize default API endpoints"""
    
    endpoints = [
        # User session lookup
        APIEndpoint(
            path="user/session",
            cache_regions=["user_sessions"],
            query_template="SELECT * FROM user_sessions WHERE user_id = '{user_id}' AND tenant_id = '{tenant_id}'",
            ttl_seconds=3600
        ),
        
        # Asset metadata lookup
        APIEndpoint(
            path="asset/metadata",
            cache_regions=["asset_metadata"],
            query_template="SELECT * FROM asset_metadata WHERE asset_id = '{asset_id}'",
            ttl_seconds=1800
        ),
        
        # Real-time metrics
        APIEndpoint(
            path="metrics/realtime",
            cache_regions=["realtime_metrics"],
            query_template="SELECT * FROM realtime_metrics WHERE metric_name = '{metric_name}' AND timestamp > {start_time}",
            ttl_seconds=60
        ),
        
        # Transaction status
        APIEndpoint(
            path="transaction/status",
            cache_regions=["transactions"],
            query_template="SELECT * FROM transactions WHERE tx_id = '{tx_id}'",
            ttl_seconds=300
        )
    ]
    
    registered = []
    for endpoint in endpoints:
        try:
            await dih.register_api_endpoint(endpoint)
            registered.append(endpoint.path)
        except Exception as e:
            logger.error(f"Failed to register endpoint {endpoint.path}: {e}")
            
    return {
        "status": "success",
        "registered_endpoints": registered
    } 