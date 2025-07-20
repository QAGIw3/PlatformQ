#!/usr/bin/env python3
"""
Platform Monitoring Service
Implements multi-region Prometheus federation with Thanos for long-term storage
"""

import asyncio
import logging
from contextlib import asynccontextmanager
from typing import Dict, List, Optional

from fastapi import FastAPI, HTTPException, Request
from fastapi.responses import JSONResponse
import uvicorn

from config import settings
from federation_manager import FederationManager
from thanos_manager import ThanosManager
from service_discovery import ServiceDiscovery
from metrics_aggregator import MetricsAggregator
from models import (
    RegionConfig,
    FederationStatus,
    MetricsQuery,
    QueryResult,
    AlertRule,
    TenantMetrics
)

# Configure logging
logging.basicConfig(
    level=getattr(logging, settings.LOG_LEVEL.upper()),
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Application lifespan manager"""
    logger.info("Starting Platform Monitoring Service")
    
    # Initialize managers
    app.state.federation_manager = FederationManager()
    app.state.thanos_manager = ThanosManager()
    app.state.service_discovery = ServiceDiscovery()
    app.state.metrics_aggregator = MetricsAggregator()
    
    # Start background tasks
    asyncio.create_task(app.state.federation_manager.start())
    asyncio.create_task(app.state.service_discovery.start())
    asyncio.create_task(app.state.thanos_manager.start())
    
    yield
    
    # Cleanup
    logger.info("Shutting down Platform Monitoring Service")
    await app.state.federation_manager.stop()
    await app.state.service_discovery.stop()
    await app.state.thanos_manager.stop()


app = FastAPI(
    title="Platform Monitoring Service",
    description="Multi-region Prometheus federation with Thanos",
    version="1.0.0",
    lifespan=lifespan
)


@app.get("/health")
async def health():
    """Health check endpoint"""
    return {"status": "healthy"}


@app.get("/readiness")
async def readiness(request: Request):
    """Readiness check endpoint"""
    federation_ready = await request.app.state.federation_manager.is_ready()
    thanos_ready = await request.app.state.thanos_manager.is_ready()
    
    if federation_ready and thanos_ready:
        return {"status": "ready"}
    else:
        return JSONResponse(
            status_code=503,
            content={"status": "not ready"}
        )


@app.get("/api/v1/regions", response_model=List[RegionConfig])
async def list_regions(request: Request):
    """List all configured regions"""
    return await request.app.state.federation_manager.list_regions()


@app.post("/api/v1/regions/{region_id}")
async def register_region(
    region_id: str,
    config: RegionConfig,
    request: Request
):
    """Register a new region for federation"""
    try:
        await request.app.state.federation_manager.register_region(region_id, config)
        
        # Update service discovery
        await request.app.state.service_discovery.add_region(region_id, config)
        
        # Configure Thanos for the new region
        await request.app.state.thanos_manager.configure_region(region_id, config)
        
        return {"status": "success", "region_id": region_id}
    except Exception as e:
        logger.error(f"Failed to register region {region_id}: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.delete("/api/v1/regions/{region_id}")
async def unregister_region(region_id: str, request: Request):
    """Unregister a region from federation"""
    try:
        await request.app.state.federation_manager.unregister_region(region_id)
        await request.app.state.service_discovery.remove_region(region_id)
        await request.app.state.thanos_manager.remove_region(region_id)
        
        return {"status": "success", "region_id": region_id}
    except Exception as e:
        logger.error(f"Failed to unregister region {region_id}: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/v1/federation/status", response_model=FederationStatus)
async def federation_status(request: Request):
    """Get federation status for all regions"""
    return await request.app.state.federation_manager.get_status()


@app.post("/api/v1/query", response_model=QueryResult)
async def query_metrics(query: MetricsQuery, request: Request):
    """Execute a PromQL query across all regions via Thanos"""
    try:
        return await request.app.state.thanos_manager.query(
            promql=query.promql,
            time_range=query.time_range,
            regions=query.regions,
            tenant_id=query.tenant_id
        )
    except Exception as e:
        logger.error(f"Query failed: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/api/v1/query_range", response_model=QueryResult)
async def query_range_metrics(query: MetricsQuery, request: Request):
    """Execute a PromQL range query across all regions"""
    try:
        return await request.app.state.thanos_manager.query_range(
            promql=query.promql,
            time_range=query.time_range,
            step=query.step,
            regions=query.regions,
            tenant_id=query.tenant_id
        )
    except Exception as e:
        logger.error(f"Range query failed: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/v1/tenants/{tenant_id}/metrics", response_model=TenantMetrics)
async def get_tenant_metrics(
    tenant_id: str,
    request: Request,
    time_range: Optional[str] = "1h"
):
    """Get aggregated metrics for a specific tenant across all regions"""
    try:
        return await request.app.state.metrics_aggregator.get_tenant_metrics(
            tenant_id=tenant_id,
            time_range=time_range
        )
    except Exception as e:
        logger.error(f"Failed to get tenant metrics: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/api/v1/alerts/rules")
async def create_alert_rule(rule: AlertRule, request: Request):
    """Create a new alert rule across all regions"""
    try:
        await request.app.state.federation_manager.create_alert_rule(rule)
        return {"status": "success", "rule_id": rule.name}
    except Exception as e:
        logger.error(f"Failed to create alert rule: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/v1/alerts/rules", response_model=List[AlertRule])
async def list_alert_rules(request: Request):
    """List all configured alert rules"""
    return await request.app.state.federation_manager.list_alert_rules()


@app.get("/api/v1/service-discovery/{service_name}")
async def discover_service(
    service_name: str,
    request: Request,
    region_id: Optional[str] = None
):
    """Discover service endpoints across regions"""
    return await request.app.state.service_discovery.discover(
        service_name=service_name,
        region_id=region_id
    )


@app.post("/api/v1/federation/sync")
async def sync_federation(request: Request):
    """Force synchronization of federation configuration"""
    try:
        await request.app.state.federation_manager.sync_configuration()
        return {"status": "success"}
    except Exception as e:
        logger.error(f"Federation sync failed: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/metrics")
async def metrics(request: Request):
    """Expose Prometheus metrics for the monitoring service itself"""
    from prometheus_client import generate_latest, CONTENT_TYPE_LATEST
    from fastapi.responses import Response
    
    return Response(
        content=generate_latest(),
        media_type=CONTENT_TYPE_LATEST
    )


if __name__ == "__main__":
    uvicorn.run(
        "main:app",
        host="0.0.0.0",
        port=settings.SERVICE_PORT,
        reload=settings.DEBUG
    ) 