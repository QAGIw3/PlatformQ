"""
PlatformQ Oracle Aggregator Service
Multi-source price aggregation with outlier detection and manipulation resistance
"""

from fastapi import FastAPI, HTTPException, WebSocket, WebSocketDisconnect
from fastapi.middleware.cors import CORSMiddleware
from contextlib import asynccontextmanager
import asyncio
from typing import Dict, List, Optional, Set
from decimal import Decimal
from datetime import datetime, timedelta
from dataclasses import dataclass
from enum import Enum
import statistics
import logging
import json

from app.aggregator.price_aggregator import PriceAggregator
from app.aggregator.volatility_calculator import VolatilityCalculator
from app.aggregator.correlation_engine import CorrelationEngine
from app.aggregator.manipulation_detector import ManipulationDetector
from app.config.oracle_sources import OracleSourceConfig
from app.integration import (
    IgniteCache,
    PulsarEventPublisher,
    ElasticsearchClient,
    InfluxDBClient,
    WebSocketManager
)

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class OracleStatus(Enum):
    HEALTHY = "healthy"
    DEGRADED = "degraded"
    OFFLINE = "offline"


@dataclass
class PriceFeed:
    """Represents a single price feed from an oracle"""
    source: str
    asset: str
    price: Decimal
    timestamp: datetime
    confidence: Decimal
    volume_24h: Optional[Decimal] = None


@dataclass
class AggregatedPrice:
    """Aggregated price data with metadata"""
    asset: str
    price: Decimal
    timestamp: datetime
    sources_count: int
    confidence: Decimal
    volatility_1h: Decimal
    volatility_24h: Decimal
    price_sources: List[PriceFeed]


# Global instances
price_aggregator: Optional[PriceAggregator] = None
volatility_calculator: Optional[VolatilityCalculator] = None
correlation_engine: Optional[CorrelationEngine] = None
manipulation_detector: Optional[ManipulationDetector] = None
websocket_manager: Optional[WebSocketManager] = None


@asynccontextmanager
async def lifespan(app: FastAPI):
    """
    Manage application lifecycle - startup and shutdown
    """
    # Startup
    logger.info("Starting Oracle Aggregator Service...")
    
    # Initialize storage clients
    ignite = IgniteCache()
    await ignite.connect()
    
    pulsar = PulsarEventPublisher()
    await pulsar.connect()
    
    elasticsearch = ElasticsearchClient()
    await elasticsearch.connect()
    
    influxdb = InfluxDBClient()
    await influxdb.connect()
    
    # Initialize components
    global price_aggregator, volatility_calculator
    global correlation_engine, manipulation_detector, websocket_manager
    
    # Load oracle configurations
    oracle_config = OracleSourceConfig()
    await oracle_config.load_sources()
    
    price_aggregator = PriceAggregator(
        ignite=ignite,
        pulsar=pulsar,
        influxdb=influxdb,
        oracle_sources=oracle_config.sources
    )
    
    volatility_calculator = VolatilityCalculator(
        ignite=ignite,
        influxdb=influxdb
    )
    
    correlation_engine = CorrelationEngine(
        ignite=ignite,
        elasticsearch=elasticsearch,
        influxdb=influxdb
    )
    
    manipulation_detector = ManipulationDetector(
        ignite=ignite,
        pulsar=pulsar,
        elasticsearch=elasticsearch
    )
    
    websocket_manager = WebSocketManager()
    
    # Start background tasks
    asyncio.create_task(price_aggregator.start_price_collection())
    asyncio.create_task(volatility_calculator.start_calculation_loop())
    asyncio.create_task(correlation_engine.start_correlation_updates())
    asyncio.create_task(manipulation_detector.start_monitoring())
    asyncio.create_task(websocket_manager.start_broadcasting())
    
    logger.info("Oracle Aggregator Service started successfully")
    
    yield
    
    # Shutdown
    logger.info("Shutting down Oracle Aggregator Service...")
    
    # Stop background tasks
    await price_aggregator.stop()
    await volatility_calculator.stop()
    await correlation_engine.stop()
    await manipulation_detector.stop()
    
    # Close connections
    await ignite.close()
    await pulsar.close()
    await elasticsearch.close()
    await influxdb.close()
    
    logger.info("Oracle Aggregator Service shut down successfully")


# Create FastAPI app
app = FastAPI(
    title="PlatformQ Oracle Aggregator",
    description="Multi-source price aggregation with manipulation resistance",
    version="1.0.0",
    lifespan=lifespan
)

# CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # Configure appropriately for production
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


# Health check
@app.get("/health")
async def health_check():
    """
    Health check endpoint
    """
    oracle_statuses = await price_aggregator.get_oracle_statuses()
    
    return {
        "status": "healthy",
        "service": "oracle-aggregator",
        "version": "1.0.0",
        "oracles": oracle_statuses,
        "active_feeds": await price_aggregator.get_active_feed_count()
    }


# Price endpoints
@app.get("/api/v1/price/{asset}")
async def get_price(asset: str):
    """
    Get aggregated price for an asset
    """
    try:
        # Get aggregated price
        aggregated_price = await price_aggregator.get_aggregated_price(asset)
        
        if not aggregated_price:
            raise HTTPException(status_code=404, detail=f"No price data for {asset}")
            
        # Check for manipulation
        manipulation_check = await manipulation_detector.check_price_manipulation(
            asset, aggregated_price
        )
        
        return {
            "asset": asset,
            "price": str(aggregated_price.price),
            "timestamp": aggregated_price.timestamp.isoformat(),
            "confidence": float(aggregated_price.confidence),
            "sources_count": aggregated_price.sources_count,
            "volatility": {
                "1h": float(aggregated_price.volatility_1h),
                "24h": float(aggregated_price.volatility_24h)
            },
            "manipulation_detected": manipulation_check.detected,
            "manipulation_confidence": manipulation_check.confidence if manipulation_check.detected else None
        }
        
    except Exception as e:
        logger.error(f"Error getting price for {asset}: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/v1/prices")
async def get_multiple_prices(assets: str):
    """
    Get prices for multiple assets
    """
    try:
        asset_list = assets.split(',')
        prices = {}
        
        for asset in asset_list:
            try:
                price_data = await price_aggregator.get_aggregated_price(asset.strip())
                if price_data:
                    prices[asset] = {
                        "price": str(price_data.price),
                        "timestamp": price_data.timestamp.isoformat(),
                        "confidence": float(price_data.confidence)
                    }
            except Exception as e:
                logger.error(f"Error getting price for {asset}: {e}")
                prices[asset] = {"error": str(e)}
                
        return {"prices": prices}
        
    except Exception as e:
        logger.error(f"Error getting multiple prices: {e}")
        raise HTTPException(status_code=500, detail=str(e))


# Volatility endpoints
@app.get("/api/v1/volatility/{asset}")
async def get_volatility(
    asset: str,
    window: str = "24h"
):
    """
    Get volatility data for an asset
    """
    try:
        # Parse window
        window_map = {
            "1h": 1,
            "4h": 4,
            "24h": 24,
            "7d": 168,
            "30d": 720
        }
        
        hours = window_map.get(window, 24)
        
        # Get volatility data
        volatility_data = await volatility_calculator.calculate_volatility(
            asset, hours
        )
        
        return {
            "asset": asset,
            "window": window,
            "volatility": {
                "realized": float(volatility_data['realized']),
                "implied": float(volatility_data.get('implied', 0)),
                "historical_percentile": float(volatility_data.get('percentile', 0))
            },
            "timestamp": datetime.utcnow().isoformat()
        }
        
    except Exception as e:
        logger.error(f"Error getting volatility for {asset}: {e}")
        raise HTTPException(status_code=500, detail=str(e))


# Correlation endpoints
@app.get("/api/v1/correlation/{asset1}/{asset2}")
async def get_correlation(
    asset1: str,
    asset2: str,
    window: str = "30d"
):
    """
    Get correlation between two assets
    """
    try:
        # Parse window
        days = int(window.rstrip('d'))
        
        # Get correlation
        correlation = await correlation_engine.calculate_correlation(
            asset1, asset2, days
        )
        
        return {
            "asset1": asset1,
            "asset2": asset2,
            "window": window,
            "correlation": float(correlation),
            "timestamp": datetime.utcnow().isoformat()
        }
        
    except Exception as e:
        logger.error(f"Error getting correlation: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/v1/correlation-matrix")
async def get_correlation_matrix(
    assets: str,
    window: str = "30d"
):
    """
    Get correlation matrix for multiple assets
    """
    try:
        asset_list = assets.split(',')
        days = int(window.rstrip('d'))
        
        # Calculate correlation matrix
        matrix = await correlation_engine.calculate_correlation_matrix(
            asset_list, days
        )
        
        return {
            "assets": asset_list,
            "window": window,
            "matrix": matrix,
            "timestamp": datetime.utcnow().isoformat()
        }
        
    except Exception as e:
        logger.error(f"Error getting correlation matrix: {e}")
        raise HTTPException(status_code=500, detail=str(e))


# Historical data endpoints
@app.get("/api/v1/historical/{asset}")
async def get_historical_prices(
    asset: str,
    start: datetime,
    end: datetime,
    interval: str = "1h"
):
    """
    Get historical price data
    """
    try:
        # Query InfluxDB for historical data
        query = f"""
        SELECT mean(price) as price, 
               mean(confidence) as confidence,
               count(price) as sources
        FROM prices
        WHERE asset = '{asset}'
        AND time >= '{start.isoformat()}'
        AND time <= '{end.isoformat()}'
        GROUP BY time({interval})
        """
        
        results = await price_aggregator.influxdb.query(query)
        
        # Format results
        data_points = []
        for point in results:
            data_points.append({
                "timestamp": point['time'],
                "price": float(point['price']),
                "confidence": float(point['confidence']),
                "sources": int(point['sources'])
            })
            
        return {
            "asset": asset,
            "start": start.isoformat(),
            "end": end.isoformat(),
            "interval": interval,
            "data": data_points
        }
        
    except Exception as e:
        logger.error(f"Error getting historical data: {e}")
        raise HTTPException(status_code=500, detail=str(e))


# WebSocket endpoint for real-time prices
@app.websocket("/ws/prices")
async def websocket_prices(websocket: WebSocket):
    """
    WebSocket endpoint for real-time price streaming
    """
    await websocket_manager.connect(websocket)
    try:
        while True:
            # Receive subscription messages
            data = await websocket.receive_text()
            message = json.loads(data)
            
            if message['action'] == 'subscribe':
                assets = message['assets']
                await websocket_manager.subscribe(websocket, assets)
            elif message['action'] == 'unsubscribe':
                assets = message['assets']
                await websocket_manager.unsubscribe(websocket, assets)
                
    except WebSocketDisconnect:
        websocket_manager.disconnect(websocket)


# Oracle source management
@app.get("/api/v1/sources")
async def get_oracle_sources():
    """
    Get list of configured oracle sources
    """
    sources = await price_aggregator.get_oracle_sources()
    return {
        "sources": sources,
        "total": len(sources)
    }


@app.post("/api/v1/sources/{source}/status")
async def update_oracle_status(
    source: str,
    status: OracleStatus
):
    """
    Update oracle source status (admin endpoint)
    """
    try:
        await price_aggregator.update_oracle_status(source, status)
        
        # Emit event
        await price_aggregator.pulsar.publish('oracle.status.updated', {
            'source': source,
            'status': status.value,
            'timestamp': datetime.utcnow().isoformat()
        })
        
        return {
            "source": source,
            "new_status": status.value,
            "updated": True
        }
        
    except Exception as e:
        logger.error(f"Error updating oracle status: {e}")
        raise HTTPException(status_code=500, detail=str(e))


# Manipulation detection endpoints
@app.get("/api/v1/manipulation/alerts")
async def get_manipulation_alerts(
    start_time: Optional[datetime] = None,
    end_time: Optional[datetime] = None
):
    """
    Get recent price manipulation alerts
    """
    try:
        if not start_time:
            start_time = datetime.utcnow() - timedelta(hours=24)
        if not end_time:
            end_time = datetime.utcnow()
            
        alerts = await manipulation_detector.get_alerts(start_time, end_time)
        
        return {
            "period": {
                "start": start_time.isoformat(),
                "end": end_time.isoformat()
            },
            "alerts": alerts,
            "total": len(alerts)
        }
        
    except Exception as e:
        logger.error(f"Error getting manipulation alerts: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/api/v1/manipulation/report")
async def report_manipulation(
    asset: str,
    details: Dict
):
    """
    Report suspected price manipulation
    """
    try:
        # Create manipulation report
        report_id = await manipulation_detector.create_report(
            asset=asset,
            reporter=details.get('reporter', 'anonymous'),
            evidence=details.get('evidence', {}),
            description=details.get('description', '')
        )
        
        return {
            "report_id": report_id,
            "status": "submitted",
            "asset": asset
        }
        
    except Exception as e:
        logger.error(f"Error reporting manipulation: {e}")
        raise HTTPException(status_code=500, detail=str(e))


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8007) 