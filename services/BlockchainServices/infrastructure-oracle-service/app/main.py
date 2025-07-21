"""Infrastructure Oracle Service

Provides real-time pricing and resource metrics for Infrastructure DeFi.
Aggregates data from multiple sources to provide accurate resource pricing.
"""

import asyncio
import logging
from contextlib import asynccontextmanager
from typing import Dict, Any, Optional, List
from datetime import datetime, timedelta
from decimal import Decimal
import statistics

from fastapi import FastAPI, HTTPException, Depends, BackgroundTasks
from fastapi.middleware.cors import CORSMiddleware
from prometheus_client import Counter, Histogram, Gauge, generate_latest
from pydantic import BaseModel, Field
import httpx

from platformq_shared import (
    create_base_app,
    ConfigLoader,
    ErrorCode,
    AppException,
    get_current_user
)
from platformq_blockchain_common import (
    ConnectionPool,
    AdapterFactory,
    ChainType,
    ChainConfig,
    EVMAdapter
)

from .config import settings
from .oracle_engine import InfrastructureOracleEngine
from .price_aggregator import PriceAggregator
from .data_sources import (
    CloudKittyDataSource,
    PrometheusDataSource,
    MarketDataSource,
    SpotPriceDataSource
)
from .models import (
    ResourceType,
    ServiceTier,
    ResourcePrice,
    PriceUpdate,
    ResourceMetrics,
    OracleUpdate
)

logger = logging.getLogger(__name__)

# Metrics
PRICE_UPDATES = Counter(
    'oracle_price_updates_total',
    'Total price updates published',
    ['resource_type', 'region', 'tier']
)
UPDATE_LATENCY = Histogram(
    'oracle_update_latency_seconds',
    'Oracle update latency',
    ['data_source']
)
PRICE_GAUGE = Gauge(
    'resource_price_usd',
    'Current resource price in USD',
    ['resource_type', 'region', 'tier']
)
UTILIZATION_GAUGE = Gauge(
    'resource_utilization_percent',
    'Resource utilization percentage',
    ['resource_type', 'region']
)

# Global instances
oracle_engine: Optional[InfrastructureOracleEngine] = None
price_aggregator: Optional[PriceAggregator] = None
blockchain_adapter: Optional[EVMAdapter] = None


# API Models
class ResourcePriceRequest(BaseModel):
    resource_type: ResourceType
    region: str = Field(default="us-east-1")
    tier: ServiceTier = Field(default=ServiceTier.STANDARD)
    quantity: float = Field(gt=0)
    duration_hours: float = Field(gt=0)


class ResourcePriceResponse(BaseModel):
    resource_type: ResourceType
    region: str
    tier: ServiceTier
    price_per_unit: Decimal
    total_price: Decimal
    currency: str = "USD"
    confidence: float
    sources: List[str]
    timestamp: datetime


class ResourceMetricsResponse(BaseModel):
    resource_type: ResourceType
    region: str
    utilization: float
    available_capacity: float
    total_capacity: float
    average_sla_compliance: float
    price_volatility: float
    timestamp: datetime


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Application lifespan manager"""
    global oracle_engine, price_aggregator, blockchain_adapter
    
    logger.info("Starting Infrastructure Oracle Service")
    
    # Initialize blockchain adapter
    blockchain_adapter = AdapterFactory.create_adapter(
        ChainType.ETHEREUM,
        ChainConfig(
            chain_id=settings.chain_id,
            rpc_url=settings.rpc_url,
            name=settings.chain_name
        )
    )
    await blockchain_adapter.connect()
    
    # Initialize data sources
    data_sources = [
        CloudKittyDataSource(settings.cloudkitty_url),
        PrometheusDataSource(settings.prometheus_url),
        MarketDataSource(settings.market_data_url),
        SpotPriceDataSource(settings.spot_price_providers)
    ]
    
    # Initialize price aggregator
    price_aggregator = PriceAggregator(data_sources)
    await price_aggregator.initialize()
    
    # Initialize oracle engine
    oracle_engine = InfrastructureOracleEngine(
        blockchain_adapter=blockchain_adapter,
        contract_address=settings.oracle_contract_address,
        private_key=settings.oracle_private_key,
        price_aggregator=price_aggregator
    )
    await oracle_engine.initialize()
    await oracle_engine.start()
    
    yield
    
    # Cleanup
    logger.info("Shutting down Infrastructure Oracle Service")
    if oracle_engine:
        await oracle_engine.stop()
    if price_aggregator:
        await price_aggregator.shutdown()
    if blockchain_adapter:
        await blockchain_adapter.disconnect()


# Create FastAPI app
app = create_base_app(
    title="Infrastructure Oracle Service",
    description="Real-time pricing and metrics for infrastructure resources",
    version="1.0.0",
    lifespan=lifespan
)

# Add CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=settings.cors_origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


@app.get("/health")
async def health_check():
    """Health check endpoint"""
    return {
        "status": "healthy",
        "service": "infrastructure-oracle",
        "version": "1.0.0",
        "timestamp": datetime.utcnow()
    }


@app.get("/api/v1/price", response_model=ResourcePriceResponse)
async def get_resource_price(
    request: ResourcePriceRequest,
    current_user: Dict = Depends(get_current_user)
) -> ResourcePriceResponse:
    """Get current price for a resource"""
    if not price_aggregator:
        raise HTTPException(status_code=503, detail="Service not ready")
    
    try:
        # Get aggregated price
        price = await price_aggregator.get_resource_price(
            resource_type=request.resource_type,
            region=request.region,
            tier=request.tier
        )
        
        # Calculate total price
        total_price = price.price_per_unit * Decimal(str(request.quantity)) * Decimal(str(request.duration_hours))
        
        # Update metrics
        PRICE_GAUGE.labels(
            resource_type=request.resource_type.value,
            region=request.region,
            tier=request.tier.value
        ).set(float(price.price_per_unit))
        
        return ResourcePriceResponse(
            resource_type=request.resource_type,
            region=request.region,
            tier=request.tier,
            price_per_unit=price.price_per_unit,
            total_price=total_price,
            confidence=price.confidence,
            sources=price.sources,
            timestamp=price.timestamp
        )
        
    except Exception as e:
        logger.error(f"Error getting resource price: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/v1/metrics/{resource_type}", response_model=ResourceMetricsResponse)
async def get_resource_metrics(
    resource_type: ResourceType,
    region: str = "us-east-1",
    current_user: Dict = Depends(get_current_user)
) -> ResourceMetricsResponse:
    """Get resource metrics including utilization and capacity"""
    if not oracle_engine:
        raise HTTPException(status_code=503, detail="Service not ready")
    
    try:
        metrics = await oracle_engine.get_resource_metrics(resource_type, region)
        
        # Update gauges
        UTILIZATION_GAUGE.labels(
            resource_type=resource_type.value,
            region=region
        ).set(metrics.utilization)
        
        return ResourceMetricsResponse(
            resource_type=resource_type,
            region=region,
            utilization=metrics.utilization,
            available_capacity=metrics.available_capacity,
            total_capacity=metrics.total_capacity,
            average_sla_compliance=metrics.average_sla_compliance,
            price_volatility=metrics.price_volatility,
            timestamp=metrics.timestamp
        )
        
    except Exception as e:
        logger.error(f"Error getting resource metrics: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/api/v1/price/update")
async def update_price_manual(
    update: PriceUpdate,
    background_tasks: BackgroundTasks,
    current_user: Dict = Depends(get_current_user)
):
    """Manually update resource price (admin only)"""
    # Check admin permissions
    if not current_user.get("is_admin", False):
        raise HTTPException(status_code=403, detail="Admin access required")
    
    if not oracle_engine:
        raise HTTPException(status_code=503, detail="Service not ready")
    
    try:
        # Schedule price update
        background_tasks.add_task(
            oracle_engine.update_price_on_chain,
            update.token_id,
            update.price_wei
        )
        
        PRICE_UPDATES.labels(
            resource_type=update.resource_type.value,
            region=update.region,
            tier=update.tier.value
        ).inc()
        
        return {"status": "update scheduled", "token_id": update.token_id}
        
    except Exception as e:
        logger.error(f"Error updating price: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/v1/price/history")
async def get_price_history(
    resource_type: ResourceType,
    region: str = "us-east-1",
    tier: ServiceTier = ServiceTier.STANDARD,
    hours: int = 24,
    current_user: Dict = Depends(get_current_user)
) -> List[ResourcePriceResponse]:
    """Get historical price data"""
    if not price_aggregator:
        raise HTTPException(status_code=503, detail="Service not ready")
    
    try:
        history = await price_aggregator.get_price_history(
            resource_type=resource_type,
            region=region,
            tier=tier,
            duration_hours=hours
        )
        
        return [
            ResourcePriceResponse(
                resource_type=resource_type,
                region=region,
                tier=tier,
                price_per_unit=price.price_per_unit,
                total_price=price.price_per_unit,  # Per unit for history
                confidence=price.confidence,
                sources=price.sources,
                timestamp=price.timestamp
            )
            for price in history
        ]
        
    except Exception as e:
        logger.error(f"Error getting price history: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/v1/volatility/{resource_type}")
async def get_price_volatility(
    resource_type: ResourceType,
    region: str = "us-east-1",
    tier: ServiceTier = ServiceTier.STANDARD,
    days: int = 7,
    current_user: Dict = Depends(get_current_user)
) -> Dict[str, float]:
    """Calculate price volatility for risk assessment"""
    if not price_aggregator:
        raise HTTPException(status_code=503, detail="Service not ready")
    
    try:
        # Get historical prices
        history = await price_aggregator.get_price_history(
            resource_type=resource_type,
            region=region,
            tier=tier,
            duration_hours=days * 24
        )
        
        if len(history) < 2:
            return {"volatility": 0.0, "sample_size": len(history)}
        
        # Calculate daily returns
        prices = [float(h.price_per_unit) for h in history]
        returns = []
        for i in range(1, len(prices)):
            if prices[i-1] > 0:
                returns.append((prices[i] - prices[i-1]) / prices[i-1])
        
        # Calculate volatility (standard deviation of returns)
        if returns:
            volatility = statistics.stdev(returns) if len(returns) > 1 else 0.0
        else:
            volatility = 0.0
        
        return {
            "volatility": volatility,
            "annualized_volatility": volatility * (365 ** 0.5),
            "sample_size": len(returns),
            "period_days": days
        }
        
    except Exception as e:
        logger.error(f"Error calculating volatility: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/v1/forecast/{resource_type}")
async def get_price_forecast(
    resource_type: ResourceType,
    region: str = "us-east-1",
    tier: ServiceTier = ServiceTier.STANDARD,
    hours_ahead: int = 24,
    current_user: Dict = Depends(get_current_user)
) -> Dict[str, Any]:
    """Get price forecast based on historical data and market signals"""
    if not oracle_engine:
        raise HTTPException(status_code=503, detail="Service not ready")
    
    try:
        forecast = await oracle_engine.forecast_price(
            resource_type=resource_type,
            region=region,
            tier=tier,
            hours_ahead=hours_ahead
        )
        
        return {
            "resource_type": resource_type.value,
            "region": region,
            "tier": tier.value,
            "current_price": float(forecast["current_price"]),
            "forecast_price": float(forecast["forecast_price"]),
            "confidence_interval": forecast["confidence_interval"],
            "trend": forecast["trend"],
            "factors": forecast["factors"],
            "hours_ahead": hours_ahead,
            "timestamp": datetime.utcnow()
        }
        
    except Exception as e:
        logger.error(f"Error generating forecast: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/metrics")
async def get_metrics():
    """Prometheus metrics endpoint"""
    return PlainTextResponse(generate_latest())


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=settings.service_port) 