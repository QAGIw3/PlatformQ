from fastapi import FastAPI, Request, Depends
from fastapi.middleware.cors import CORSMiddleware
from contextlib import asynccontextmanager
import asyncio
import time
from prometheus_client import Counter, Histogram, Gauge, generate_latest
from prometheus_client.core import CollectorRegistry
from starlette.responses import Response
import logging

from .config import RiskManagementConfig
from .core.risk_monitor import RiskMonitor
from .api import risk
from .dependencies import (
    get_risk_monitor, 
    get_config,
    get_event_publisher,
    get_market_data_client,
    get_position_client,
    get_pulsar_client,
    get_ignite_client
)


# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Prometheus metrics
registry = CollectorRegistry()
request_count = Counter(
    'rms_requests_total',
    'Total requests',
    ['method', 'endpoint', 'status'],
    registry=registry
)
request_duration = Histogram(
    'rms_request_duration_seconds',
    'Request duration',
    ['method', 'endpoint'],
    registry=registry
)
monitored_traders = Gauge(
    'rms_monitored_traders',
    'Number of traders being monitored',
    registry=registry
)
margin_calls = Counter(
    'rms_margin_calls_total',
    'Total margin calls triggered',
    registry=registry
)
liquidations = Counter(
    'rms_liquidations_total',
    'Total liquidations triggered',
    registry=registry
)
risk_alerts = Counter(
    'rms_risk_alerts_total',
    'Total risk alerts generated',
    ['severity'],
    registry=registry
)


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Application lifespan manager"""
    # Startup
    config = get_config()
    risk_monitor = get_risk_monitor()
    
    # Start risk monitoring
    await risk_monitor.start()
    
    logger.info(f"Risk Management Service started on port {config.SERVICE_PORT}")
    logger.info(f"Metrics available on port {config.METRICS_PORT}")
    logger.info("ML risk engine initialized with predictive capabilities")
    
    yield
    
    # Shutdown
    logger.info("Shutting down Risk Management Service...")
    
    # Stop risk monitoring
    await risk_monitor.stop()
    
    # Close clients
    event_publisher = get_event_publisher()
    event_publisher.close()
    
    pulsar_client = get_pulsar_client()
    pulsar_client.close()
    
    ignite_client = get_ignite_client()
    ignite_client.close()
    
    logger.info("Risk Management Service stopped")


# Create FastAPI app
app = FastAPI(
    title="Risk Management Service",
    description="Real-time risk monitoring and management with ML predictions",
    version="2.0.0",
    lifespan=lifespan
)

# Add CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # Configure appropriately for production
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


# Middleware for metrics
@app.middleware("http")
async def prometheus_middleware(request: Request, call_next):
    """Track request metrics"""
    start_time = time.time()
    
    # Process request
    response = await call_next(request)
    
    # Record metrics
    duration = time.time() - start_time
    request_count.labels(
        method=request.method,
        endpoint=request.url.path,
        status=response.status_code
    ).inc()
    request_duration.labels(
        method=request.method,
        endpoint=request.url.path
    ).observe(duration)
    
    return response


# Include routers
app.include_router(risk.router)


@app.get("/")
async def root():
    """Root endpoint"""
    return {
        "service": "Risk Management Service",
        "version": "2.0.0",
        "status": "running",
        "features": [
            "Real-time risk monitoring",
            "ML-based risk predictions",
            "Volatility forecasting",
            "Anomaly detection",
            "Stress testing",
            "Dynamic risk parameters"
        ]
    }


@app.get("/health")
async def health_check(risk_monitor: RiskMonitor = Depends(get_risk_monitor)):
    """Health check endpoint"""
    return {
        "status": "healthy",
        "timestamp": time.time_ns(),
        "monitored_traders": len(risk_monitor.monitored_traders),
        "active_alerts": sum(
            len(state.active_alerts) 
            for state in risk_monitor.trader_states.values()
        ),
        "ml_engine_status": "active" if risk_monitor.ml_engine else "inactive"
    }


@app.get("/metrics")
async def metrics(risk_monitor: RiskMonitor = Depends(get_risk_monitor)):
    """Prometheus metrics endpoint"""
    # Update custom metrics
    monitored_traders.set(len(risk_monitor.monitored_traders))
    
    # Count alerts by severity
    alert_counts = {"low": 0, "medium": 0, "high": 0, "critical": 0}
    for state in risk_monitor.trader_states.values():
        for alert in state.active_alerts:
            severity = alert.get("level").value
            alert_counts[severity] += 1
    
    for severity, count in alert_counts.items():
        risk_alerts.labels(severity=severity)._value.set(count)
    
    # Generate metrics
    return Response(
        content=generate_latest(registry),
        media_type="text/plain"
    )


@app.get("/api/v1/stats")
async def get_service_stats(risk_monitor: RiskMonitor = Depends(get_risk_monitor)):
    """Get service statistics"""
    # Calculate stats
    total_positions = 0
    total_margin_used = 0
    traders_at_risk = 0
    ml_predictions_count = 0
    
    for trader_id in risk_monitor.monitored_traders:
        portfolio = risk_monitor.trader_portfolios.get(trader_id)
        if portfolio:
            total_positions += len(portfolio.positions)
            total_margin_used += float(portfolio.total_margin_used)
            
        state = risk_monitor.trader_states.get(trader_id)
        if state and (state.has_high_alerts or state.has_critical_alerts):
            traders_at_risk += 1
    
    # Count ML predictions in cache
    ml_predictions_count = len(risk_monitor.market_data_cache)
    
    return {
        "monitored_traders": len(risk_monitor.monitored_traders),
        "total_positions": total_positions,
        "total_margin_used": total_margin_used,
        "traders_at_risk": traders_at_risk,
        "ml_predictions_active": ml_predictions_count,
        "cache_size": {
            "price_cache": len(risk_monitor.price_cache),
            "portfolio_cache": len(risk_monitor.trader_portfolios),
            "market_data_cache": len(risk_monitor.market_data_cache)
        },
        "ml_engine": {
            "models_loaded": sum(1 for m in risk_monitor.ml_engine.models.values() if m is not None),
            "features_tracked": 9  # Number of features in ML model
        }
    }


if __name__ == "__main__":
    import uvicorn
    
    config = RiskManagementConfig()
    
    uvicorn.run(
        app,
        host="0.0.0.0",
        port=config.SERVICE_PORT,
        log_level="info"
    ) 