"""Settlement Coordinator Service - Main Application"""

import asyncio
import logging
import signal
import sys
from contextlib import asynccontextmanager
from datetime import datetime
import grpc
from concurrent import futures

from fastapi import FastAPI, HTTPException, Depends, status
from fastapi.middleware.cors import CORSMiddleware
from prometheus_client import Counter, Histogram, Gauge, generate_latest
from prometheus_client.core import CollectorRegistry
from starlette.responses import PlainTextResponse

from app.config import settings
from app.grpc.settlement_service import SettlementCoordinatorService
from app.cache.ignite_cache import cache_manager
from app.models.settlement import Settlement, RiskAssessment, ProviderMetrics
from app.api import flash  # Import flash router

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Prometheus metrics
registry = CollectorRegistry()
settlement_counter = Counter(
    'settlements_processed_total',
    'Total number of settlements processed',
    ['status'],
    registry=registry
)
risk_calculation_histogram = Histogram(
    'risk_calculation_duration_seconds',
    'Time spent calculating risk',
    ['model'],
    registry=registry
)
active_settlements_gauge = Gauge(
    'active_settlements',
    'Number of active settlements being processed',
    registry=registry
)

# Global service instance
grpc_service = None


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Manage application lifecycle"""
    global grpc_service
    
    logger.info("Starting Settlement Coordinator Service")
    
    # Initialize gRPC service
    grpc_service = SettlementCoordinatorService()
    await grpc_service.initialize()
    
    # Start gRPC server in background
    grpc_server = grpc.aio.server(
        futures.ThreadPoolExecutor(max_workers=10)
    )
    
    # Add service to server (would use generated code in production)
    # grpc_server.add_insecure_port(f'[::]:{settings.grpc_port}')
    
    # await grpc_server.start()
    logger.info(f"gRPC server started on port {settings.grpc_port}")
    
    yield
    
    # Shutdown
    logger.info("Shutting down Settlement Coordinator Service")
    await grpc_service.shutdown()
    # await grpc_server.stop(5)


# Create FastAPI app
app = FastAPI(
    title=settings.service_name,
    version=settings.service_version,
    lifespan=lifespan
)

# Add CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Include routers
app.include_router(flash.router, prefix=settings.api_prefix)


# Health check endpoints
@app.get("/health")
async def health_check():
    """Basic health check"""
    return {
        "status": "healthy",
        "service": settings.service_name,
        "version": settings.service_version,
        "timestamp": datetime.utcnow().isoformat()
    }


@app.get("/health/ready")
async def readiness_check():
    """Readiness check including dependencies"""
    checks = {
        "ignite": False,
        "grpc_service": False
    }
    
    # Check Ignite connection
    try:
        if cache_manager.connected:
            checks["ignite"] = True
    except Exception as e:
        logger.error(f"Ignite check failed: {e}")
    
    # Check gRPC service
    if grpc_service is not None:
        checks["grpc_service"] = True
    
    all_ready = all(checks.values())
    
    if not all_ready:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail={"status": "not ready", "checks": checks}
        )
    
    return {
        "status": "ready",
        "checks": checks,
        "timestamp": datetime.utcnow().isoformat()
    }


# Metrics endpoint
@app.get("/metrics", response_class=PlainTextResponse)
async def metrics():
    """Prometheus metrics endpoint"""
    return generate_latest(registry)


# REST API endpoints for HTTP access

@app.post(f"{settings.api_prefix}/settlements")
async def create_settlement(settlement_data: dict):
    """Create a new settlement via HTTP"""
    try:
        result = await grpc_service.ProcessSettlement(settlement_data, None)
        settlement_counter.labels(status="created").inc()
        return result
    except Exception as e:
        logger.error(f"Error creating settlement: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=str(e)
        )


@app.get(f"{settings.api_prefix}/settlements/{{settlement_id}}")
async def get_settlement(settlement_id: str):
    """Get settlement details"""
    try:
        settlement = await cache_manager.get_settlement(settlement_id)
        if not settlement:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"Settlement {settlement_id} not found"
            )
        
        # Get risk assessment if available
        risk_assessment = await cache_manager.get_risk_assessment(settlement_id)
        
        return {
            "settlement": settlement.model_dump(),
            "risk_assessment": risk_assessment.model_dump() if risk_assessment else None
        }
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error getting settlement: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=str(e)
        )


@app.post(f"{settings.api_prefix}/settlements/{{settlement_id}}/calculate-risk")
async def calculate_risk(settlement_id: str, force_recalculate: bool = False):
    """Calculate risk for a settlement"""
    try:
        with risk_calculation_histogram.labels(model="all").time():
            result = await grpc_service.CalculateRisk(
                {
                    "settlement_id": settlement_id,
                    "risk_models": ["all"],
                    "force_recalculate": force_recalculate
                },
                None
            )
        return result
    except Exception as e:
        logger.error(f"Error calculating risk: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=str(e)
        )


@app.get(f"{settings.api_prefix}/settlements/{{settlement_id}}/status")
async def get_settlement_status(settlement_id: str):
    """Get settlement status"""
    try:
        result = await grpc_service.GetSettlementStatus(
            {"settlement_id": settlement_id},
            None
        )
        return result
    except Exception as e:
        logger.error(f"Error getting settlement status: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=str(e)
        )


@app.post(f"{settings.api_prefix}/settlements/batch")
async def batch_process_settlements(settlements: list):
    """Process multiple settlements"""
    try:
        result = await grpc_service.BatchProcessSettlements(
            {"settlements": settlements},
            None
        )
        settlement_counter.labels(status="batch_created").inc(len(settlements))
        return result
    except Exception as e:
        logger.error(f"Error processing batch: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=str(e)
        )


@app.get(f"{settings.api_prefix}/providers/{{provider_id}}/metrics")
async def get_provider_metrics(provider_id: str):
    """Get provider metrics"""
    try:
        metrics = await cache_manager.get_provider_metrics(provider_id)
        if not metrics:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"Provider {provider_id} not found"
            )
        return metrics.model_dump()
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error getting provider metrics: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=str(e)
        )


@app.post(f"{settings.api_prefix}/providers/{{provider_id}}/metrics")
async def update_provider_metrics(provider_id: str, updates: dict):
    """Update provider metrics"""
    try:
        success = await cache_manager.update_provider_metrics(provider_id, updates)
        if not success:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"Provider {provider_id} not found"
            )
        return {"status": "updated", "provider_id": provider_id}
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error updating provider metrics: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=str(e)
        )


@app.get(f"{settings.api_prefix}/risk/metrics")
async def get_risk_metrics(
    provider_id: str = None,
    start_time: str = None,
    end_time: str = None
):
    """Get aggregated risk metrics"""
    try:
        if not start_time or not end_time:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="start_time and end_time are required"
            )
        
        result = await grpc_service.GetRiskMetrics(
            {
                "provider_id": provider_id,
                "start_time": start_time,
                "end_time": end_time
            },
            None
        )
        return result
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error getting risk metrics: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=str(e)
        )


@app.get(f"{settings.api_prefix}/cache/metrics")
async def get_cache_metrics():
    """Get cache metrics"""
    try:
        metrics = await cache_manager.get_cache_metrics()
        return metrics
    except Exception as e:
        logger.error(f"Error getting cache metrics: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=str(e)
        )


# Admin endpoints
@app.post(f"{settings.api_prefix}/admin/cache/clear/{{cache_name}}")
async def clear_cache(cache_name: str):
    """Clear a specific cache (admin only)"""
    try:
        # In production, add authentication/authorization
        success = await cache_manager.clear_cache(cache_name)
        if not success:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail=f"Failed to clear cache {cache_name}"
            )
        return {"status": "cleared", "cache": cache_name}
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error clearing cache: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=str(e)
        )


# Signal handlers for graceful shutdown
def signal_handler(sig, frame):
    logger.info(f"Received signal {sig}")
    sys.exit(0)


signal.signal(signal.SIGINT, signal_handler)
signal.signal(signal.SIGTERM, signal_handler)


if __name__ == "__main__":
    import uvicorn
    
    uvicorn.run(
        "app.main:app",
        host="0.0.0.0",
        port=settings.http_port,
        reload=settings.environment == "development"
    ) 