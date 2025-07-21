"""
DeFi Oracle API Endpoints

Provides oracle services for DeFi protocols including quality scores,
availability monitoring, price aggregation, and performance benchmarks.
"""

from typing import Dict, Any, List, Optional
from decimal import Decimal
from datetime import datetime
from fastapi import APIRouter, HTTPException, Depends, Query, Body
from pydantic import BaseModel, Field

from ..oracles import (
    QualityAggregator,
    AvailabilityMonitor,
    PriceAggregator,
    PerformanceOracle,
    BenchmarkType,
    ResourceStatus
)
from ..dependencies import (
    get_quality_aggregator,
    get_availability_monitor,
    get_price_aggregator,
    get_performance_oracle,
    verify_api_key
)

router = APIRouter(prefix="/api/v1/defi-oracles", tags=["defi-oracles"])


# Request models

class MonitoringRequest(BaseModel):
    """Request to start monitoring a resource"""
    resource_id: int
    resource_type: str = Field(..., regex="^(quantum|ai|network)$")
    endpoint: str
    check_config: Dict[str, Any] = Field(default_factory=dict)


class BenchmarkRequest(BaseModel):
    """Request to run performance benchmark"""
    resource_id: int
    resource_type: str = Field(..., regex="^(quantum|ai|network)$")
    benchmark_type: BenchmarkType = BenchmarkType.STANDARD
    custom_config: Optional[Dict[str, Any]] = None


class PerformanceClaimRequest(BaseModel):
    """Request to verify performance claims"""
    resource_id: int
    resource_type: str = Field(..., regex="^(quantum|ai|network)$")
    claimed_metrics: Dict[str, float]
    tolerance: float = Field(0.1, ge=0, le=0.5)


# Quality Oracle Endpoints

@router.get("/quality/{resource_id}")
async def get_quality_score(
    resource_id: int,
    resource_type: str = Query(..., regex="^(quantum|ai|network)$"),
    include_components: bool = True,
    quality_aggregator: QualityAggregator = Depends(get_quality_aggregator)
) -> Dict[str, Any]:
    """Get aggregated quality score for a resource"""
    try:
        return await quality_aggregator.get_quality_score(
            resource_id,
            resource_type,
            include_components
        )
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))


@router.get("/quality/{resource_id}/history")
async def get_quality_history(
    resource_id: int,
    hours: int = Query(24, ge=1, le=720),
    interval: str = Query("hourly", regex="^(hourly|daily)$"),
    quality_aggregator: QualityAggregator = Depends(get_quality_aggregator)
) -> Dict[str, Any]:
    """Get historical quality scores"""
    try:
        return await quality_aggregator.get_quality_history(
            resource_id,
            hours,
            interval
        )
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))


@router.post("/quality/{resource_id}/sign")
async def sign_quality_data(
    resource_id: int,
    quality_aggregator: QualityAggregator = Depends(get_quality_aggregator),
    api_key: str = Depends(verify_api_key)
) -> Dict[str, Any]:
    """Sign quality data for on-chain submission"""
    try:
        # Get current quality score
        quality_score = await quality_aggregator.get_quality_score(
            resource_id,
            "quantum",  # Would be determined from resource registry
            include_components=True
        )
        
        # Sign the data
        return await quality_aggregator.sign_quality_data(
            resource_id,
            quality_score
        )
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))


# Availability Monitor Endpoints

@router.post("/availability/monitor/start")
async def start_monitoring(
    request: MonitoringRequest,
    availability_monitor: AvailabilityMonitor = Depends(get_availability_monitor),
    api_key: str = Depends(verify_api_key)
) -> Dict[str, Any]:
    """Start monitoring resource availability"""
    try:
        await availability_monitor.start_monitoring(
            request.resource_id,
            request.resource_type,
            request.endpoint,
            request.check_config
        )
        
        return {
            "status": "monitoring_started",
            "resource_id": request.resource_id,
            "check_interval": availability_monitor.check_interval
        }
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))


@router.post("/availability/monitor/stop/{resource_id}")
async def stop_monitoring(
    resource_id: int,
    availability_monitor: AvailabilityMonitor = Depends(get_availability_monitor),
    api_key: str = Depends(verify_api_key)
) -> Dict[str, Any]:
    """Stop monitoring resource availability"""
    try:
        await availability_monitor.stop_monitoring(resource_id)
        
        return {
            "status": "monitoring_stopped",
            "resource_id": resource_id
        }
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))


@router.get("/availability/{resource_id}/status")
async def check_availability(
    resource_id: int,
    availability_monitor: AvailabilityMonitor = Depends(get_availability_monitor)
) -> Dict[str, Any]:
    """Check current availability status"""
    try:
        return await availability_monitor.check_availability(resource_id)
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))


@router.get("/availability/{resource_id}/metrics")
async def get_availability_metrics(
    resource_id: int,
    period_hours: int = Query(24, ge=1, le=720),
    availability_monitor: AvailabilityMonitor = Depends(get_availability_monitor)
) -> Dict[str, Any]:
    """Get availability metrics and SLA compliance"""
    try:
        return await availability_monitor.get_availability_metrics(
            resource_id,
            period_hours
        )
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))


@router.get("/availability/{resource_id}/downtime")
async def get_downtime_records(
    resource_id: int,
    start_time: datetime,
    end_time: datetime,
    availability_monitor: AvailabilityMonitor = Depends(get_availability_monitor)
) -> List[Dict[str, Any]]:
    """Get downtime records for a time period"""
    try:
        return await availability_monitor.get_downtime_records(
            resource_id,
            start_time,
            end_time
        )
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))


# Price Aggregator Endpoints

@router.get("/price/{resource_type}")
async def get_price(
    resource_type: str = Field(..., regex="^(quantum|ai|network)$"),
    base_currency: str = Query("USD", regex="^[A-Z]{3,4}$"),
    include_sources: bool = False,
    price_aggregator: PriceAggregator = Depends(get_price_aggregator)
) -> Dict[str, Any]:
    """Get aggregated price for resource type"""
    try:
        return await price_aggregator.get_price(
            resource_type,
            base_currency,
            include_sources
        )
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))


@router.get("/price/{resource_type}/twap")
async def get_twap(
    resource_type: str = Field(..., regex="^(quantum|ai|network)$"),
    window_seconds: int = Query(300, ge=60, le=3600),
    base_currency: str = Query("USD", regex="^[A-Z]{3,4}$"),
    price_aggregator: PriceAggregator = Depends(get_price_aggregator)
) -> Dict[str, Any]:
    """Get Time-Weighted Average Price"""
    try:
        return await price_aggregator.get_twap(
            resource_type,
            window_seconds,
            base_currency
        )
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))


@router.get("/price/{resource_type}/volatility")
async def get_price_volatility(
    resource_type: str = Field(..., regex="^(quantum|ai|network)$"),
    window_hours: int = Query(24, ge=1, le=720),
    price_aggregator: PriceAggregator = Depends(get_price_aggregator)
) -> Dict[str, Any]:
    """Get price volatility metrics"""
    try:
        return await price_aggregator.get_volatility(
            resource_type,
            window_hours
        )
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))


@router.post("/price/{resource_type}/sign")
async def sign_price_data(
    resource_type: str = Field(..., regex="^(quantum|ai|network)$"),
    price_aggregator: PriceAggregator = Depends(get_price_aggregator),
    api_key: str = Depends(verify_api_key)
) -> Dict[str, Any]:
    """Sign price data for on-chain submission"""
    try:
        # Get current price
        price_data = await price_aggregator.get_price(resource_type)
        
        # Sign the data
        return await price_aggregator.sign_price_data(
            resource_type,
            price_data
        )
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))


# Performance Oracle Endpoints

@router.post("/performance/benchmark")
async def run_benchmark(
    request: BenchmarkRequest,
    performance_oracle: PerformanceOracle = Depends(get_performance_oracle),
    api_key: str = Depends(verify_api_key)
) -> Dict[str, Any]:
    """Run performance benchmark on resource"""
    try:
        return await performance_oracle.run_benchmark(
            request.resource_id,
            request.resource_type,
            request.benchmark_type,
            request.custom_config
        )
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))


@router.post("/performance/verify")
async def verify_performance_claim(
    request: PerformanceClaimRequest,
    performance_oracle: PerformanceOracle = Depends(get_performance_oracle)
) -> Dict[str, Any]:
    """Verify performance claims for insurance/guarantees"""
    try:
        return await performance_oracle.verify_performance_claim(
            request.resource_id,
            request.resource_type,
            request.claimed_metrics,
            request.tolerance
        )
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))


@router.get("/performance/{resource_id}/history")
async def get_performance_history(
    resource_id: int,
    hours: int = Query(24, ge=1, le=720),
    metric_filter: Optional[List[str]] = Query(None),
    performance_oracle: PerformanceOracle = Depends(get_performance_oracle)
) -> Dict[str, Any]:
    """Get historical performance data"""
    try:
        return await performance_oracle.get_performance_history(
            resource_id,
            hours,
            metric_filter
        )
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))


@router.post("/performance/{resource_id}/sign")
async def sign_performance_data(
    resource_id: int,
    performance_oracle: PerformanceOracle = Depends(get_performance_oracle),
    api_key: str = Depends(verify_api_key)
) -> Dict[str, Any]:
    """Sign performance data for on-chain submission"""
    try:
        # Run verification benchmark
        benchmark_result = await performance_oracle.run_benchmark(
            resource_id,
            "quantum",  # Would be determined from resource registry
            BenchmarkType.VERIFICATION
        )
        
        # Sign the data
        return await performance_oracle.sign_performance_data(
            resource_id,
            benchmark_result
        )
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))


# Aggregated Oracle Data Endpoint

@router.get("/resource/{resource_id}/all")
async def get_all_oracle_data(
    resource_id: int,
    resource_type: str = Query(..., regex="^(quantum|ai|network)$"),
    quality_aggregator: QualityAggregator = Depends(get_quality_aggregator),
    availability_monitor: AvailabilityMonitor = Depends(get_availability_monitor),
    price_aggregator: PriceAggregator = Depends(get_price_aggregator),
    performance_oracle: PerformanceOracle = Depends(get_performance_oracle)
) -> Dict[str, Any]:
    """Get all oracle data for a resource (quality, availability, price, performance)"""
    try:
        # Gather all data in parallel
        quality_task = quality_aggregator.get_quality_score(
            resource_id, resource_type, include_components=False
        )
        availability_task = availability_monitor.get_availability_metrics(
            resource_id, period_hours=24
        )
        price_task = price_aggregator.get_price(
            resource_type, include_sources=False
        )
        performance_task = performance_oracle.get_performance_history(
            resource_id, hours=24
        )
        
        # Wait for all results
        import asyncio
        quality, availability, price, performance = await asyncio.gather(
            quality_task,
            availability_task,
            price_task,
            performance_task,
            return_exceptions=True
        )
        
        # Build response
        response = {
            "resource_id": resource_id,
            "resource_type": resource_type,
            "timestamp": datetime.utcnow(),
            "oracles": {}
        }
        
        # Add successful results
        if not isinstance(quality, Exception):
            response["oracles"]["quality"] = quality
        else:
            response["oracles"]["quality"] = {"error": str(quality)}
            
        if not isinstance(availability, Exception):
            response["oracles"]["availability"] = availability
        else:
            response["oracles"]["availability"] = {"error": str(availability)}
            
        if not isinstance(price, Exception):
            response["oracles"]["price"] = price
        else:
            response["oracles"]["price"] = {"error": str(price)}
            
        if not isinstance(performance, Exception):
            response["oracles"]["performance"] = performance
        else:
            response["oracles"]["performance"] = {"error": str(performance)}
        
        return response
        
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))


# Health check for DeFi oracles

@router.get("/health")
async def defi_oracles_health(
    quality_aggregator: QualityAggregator = Depends(get_quality_aggregator),
    availability_monitor: AvailabilityMonitor = Depends(get_availability_monitor),
    price_aggregator: PriceAggregator = Depends(get_price_aggregator),
    performance_oracle: PerformanceOracle = Depends(get_performance_oracle)
) -> Dict[str, Any]:
    """Check health of all DeFi oracle components"""
    
    health = {
        "status": "healthy",
        "timestamp": datetime.utcnow(),
        "components": {
            "quality_aggregator": {"status": "healthy"},
            "availability_monitor": {"status": "healthy"},
            "price_aggregator": {"status": "healthy"},
            "performance_oracle": {"status": "healthy"}
        }
    }
    
    # Check each component
    try:
        # Quick checks to verify components are responsive
        # In production, would do more thorough health checks
        
        return health
        
    except Exception as e:
        health["status"] = "unhealthy"
        health["error"] = str(e)
        return health 