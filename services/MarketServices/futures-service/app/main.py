"""Futures Service - Main application."""

import logging
from contextlib import asynccontextmanager
from datetime import datetime, timedelta
from decimal import Decimal

from fastapi import FastAPI, Depends, HTTPException
from fastapi.middleware.cors import CORSMiddleware
import httpx

from .models import FuturesContract, FuturesOrder, FundingRate


# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)


# Trading Core API client
trading_core_client = httpx.AsyncClient(base_url="http://localhost:8020")


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Manage application lifecycle."""
    logger.info("Starting Futures Service...")
    yield
    logger.info("Shutting down Futures Service...")
    await trading_core_client.aclose()


# Create FastAPI application
app = FastAPI(
    title="Futures Service",
    description="Futures-specific trading logic",
    version="1.0.0",
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


@app.get("/")
async def root():
    """Root endpoint."""
    return {
        "service": "Futures Service",
        "version": "1.0.0",
        "status": "operational"
    }


@app.post("/api/v1/futures/contracts")
async def create_futures_contract(contract: FuturesContract):
    """Create a new futures contract market."""
    # Create market in trading core
    market_data = {
        "market_id": f"futures_{contract.symbol}_{contract.expiry.strftime('%Y%m%d')}",
        "symbol": contract.symbol,
        "name": f"{contract.underlying} Futures {contract.expiry.strftime('%b %Y')}",
        "market_type": "futures",
        "product_type": "futures",
        "base_asset": contract.underlying,
        "quote_asset": contract.quote_currency,
        "tick_size": str(contract.tick_size),
        "lot_size": str(contract.contract_size),
        "min_notional": str(contract.contract_size * 100),  # Example min
        "product_config": {
            "contract_size": str(contract.contract_size),
            "expiry": contract.expiry.isoformat(),
            "settlement_type": contract.settlement_type,
            "initial_margin_rate": str(contract.initial_margin_rate),
            "maintenance_margin_rate": str(contract.maintenance_margin_rate)
        }
    }
    
    response = await trading_core_client.post(
        "/api/v1/markets",
        json=market_data
    )
    
    if response.status_code != 200:
        raise HTTPException(status_code=response.status_code, detail=response.text)
    
    return response.json()


@app.post("/api/v1/futures/orders")
async def place_futures_order(order: FuturesOrder):
    """Place a futures order."""
    # Add futures-specific validation
    if order.leverage > Decimal("20"):
        raise HTTPException(status_code=400, detail="Maximum leverage is 20x")
    
    # Convert to trading core order
    order_data = {
        "market_id": order.market_id,
        "product_type": "futures",
        "type": order.order_type,
        "side": order.side,
        "quantity": str(order.contracts),
        "price": str(order.price) if order.price else None,
        "time_in_force": order.time_in_force,
        "product_data": {
            "leverage": str(order.leverage),
            "reduce_only": order.reduce_only
        }
    }
    
    response = await trading_core_client.post(
        "/api/v1/orders",
        json=order_data,
        headers={"X-User-Id": order.user_id}
    )
    
    if response.status_code != 200:
        raise HTTPException(status_code=response.status_code, detail=response.text)
    
    return response.json()


@app.get("/api/v1/futures/funding-rates/{market_id}")
async def get_funding_rate(market_id: str):
    """Get current funding rate for a perpetual futures market."""
    # This would calculate funding rate based on market conditions
    # For now, return mock data
    return FundingRate(
        market_id=market_id,
        funding_rate=Decimal("0.0001"),  # 0.01%
        next_funding_time=datetime.utcnow() + timedelta(hours=8),
        interval_hours=8
    )


@app.get("/api/v1/futures/settlement/{market_id}")
async def get_settlement_info(market_id: str):
    """Get settlement information for a futures contract."""
    # Get market info from trading core
    response = await trading_core_client.get(f"/api/v1/markets/{market_id}")
    
    if response.status_code != 200:
        raise HTTPException(status_code=404, detail="Market not found")
    
    market = response.json()
    product_config = market.get("product_config", {})
    
    return {
        "market_id": market_id,
        "expiry": product_config.get("expiry"),
        "settlement_type": product_config.get("settlement_type", "cash"),
        "settlement_price": None,  # Would be set at expiry
        "time_to_expiry": None  # Calculate from expiry
    }


@app.get("/api/v1/futures/basis/{underlying}")
async def get_basis_info(underlying: str):
    """Get basis information for futures vs spot."""
    # This would calculate basis from futures and spot prices
    return {
        "underlying": underlying,
        "spot_price": "50000",
        "futures_prices": {
            "current_month": "50100",
            "next_month": "50200",
            "quarter": "50500"
        },
        "basis": {
            "current_month": "100",
            "next_month": "200",
            "quarter": "500"
        },
        "annualized_basis": {
            "current_month": "2.4%",
            "next_month": "2.4%",
            "quarter": "4.0%"
        }
    }


@app.get("/health")
async def health_check():
    """Health check endpoint."""
    return {"status": "healthy"}


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(
        "app.main:app",
        host="0.0.0.0",
        port=8025,
        reload=True,
        log_level="info"
    ) 