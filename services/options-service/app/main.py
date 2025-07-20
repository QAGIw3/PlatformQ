"""Options Service - Main application."""

import logging
from contextlib import asynccontextmanager
from datetime import datetime
from decimal import Decimal
import math

from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
import httpx

from .models import OptionContract, OptionOrder, Greeks, OptionStrategy


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
    logger.info("Starting Options Service...")
    yield
    logger.info("Shutting down Options Service...")
    await trading_core_client.aclose()


# Create FastAPI application
app = FastAPI(
    title="Options Service",
    description="Options-specific trading logic and pricing",
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
        "service": "Options Service",
        "version": "1.0.0",
        "status": "operational"
    }


@app.post("/api/v1/options/contracts")
async def create_option_contract(contract: OptionContract):
    """Create a new option contract market."""
    # Create market in trading core
    market_data = {
        "market_id": f"option_{contract.underlying}_{contract.strike}_{contract.option_type[:1]}_{contract.expiry.strftime('%Y%m%d')}",
        "symbol": f"{contract.underlying}-{contract.strike}-{contract.option_type[:1]}-{contract.expiry.strftime('%d%b%y')}",
        "name": f"{contract.underlying} {contract.strike} {contract.option_type.title()} {contract.expiry.strftime('%d %b %Y')}",
        "market_type": "options",
        "product_type": "vanilla_option",
        "base_asset": contract.underlying,
        "quote_asset": contract.quote_currency,
        "tick_size": "0.01",
        "lot_size": str(contract.contract_size),
        "min_notional": "10",
        "product_config": {
            "option_type": contract.option_type,
            "strike": str(contract.strike),
            "expiry": contract.expiry.isoformat(),
            "exercise_style": contract.exercise_style,
            "contract_size": str(contract.contract_size)
        }
    }
    
    response = await trading_core_client.post(
        "/api/v1/markets",
        json=market_data
    )
    
    if response.status_code != 200:
        raise HTTPException(status_code=response.status_code, detail=response.text)
    
    return response.json()


@app.post("/api/v1/options/orders")
async def place_option_order(order: OptionOrder):
    """Place an option order."""
    # Convert to trading core order
    order_data = {
        "market_id": order.market_id,
        "product_type": "vanilla_option",
        "type": order.order_type,
        "side": order.side,
        "quantity": str(order.contracts),
        "price": str(order.premium) if order.premium else None,
        "time_in_force": order.time_in_force,
        "product_data": {
            "option_side": order.option_side  # buy_to_open, sell_to_open, etc.
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


@app.get("/api/v1/options/greeks/{market_id}")
async def calculate_greeks(
    market_id: str,
    spot_price: Decimal,
    volatility: Decimal,
    risk_free_rate: Decimal = Decimal("0.05")
):
    """Calculate option Greeks."""
    # Get option details from market
    response = await trading_core_client.get(f"/api/v1/markets/{market_id}")
    
    if response.status_code != 200:
        raise HTTPException(status_code=404, detail="Market not found")
    
    market = response.json()
    config = market.get("product_config", {})
    
    # Simple Black-Scholes Greeks calculation (simplified)
    strike = Decimal(config.get("strike", "0"))
    expiry = datetime.fromisoformat(config.get("expiry"))
    option_type = config.get("option_type", "call")
    
    time_to_expiry = (expiry - datetime.utcnow()).total_seconds() / (365 * 24 * 3600)
    
    # Simplified Greeks calculation
    moneyness = float(spot_price / strike)
    
    if option_type == "call":
        delta = Decimal(str(0.5 + 0.5 * moneyness))  # Simplified
        theta = Decimal("-0.05")  # Simplified daily theta
    else:
        delta = Decimal(str(0.5 - 0.5 * moneyness))  # Simplified
        theta = Decimal("-0.05")
    
    gamma = Decimal("0.02")  # Simplified
    vega = Decimal("0.15")  # Simplified
    rho = Decimal("0.10") if option_type == "call" else Decimal("-0.10")
    
    return Greeks(
        delta=delta,
        gamma=gamma,
        theta=theta,
        vega=vega,
        rho=rho,
        implied_volatility=volatility
    )


@app.post("/api/v1/options/strategies/{strategy_type}")
async def create_option_strategy(
    strategy_type: str,
    strategy: OptionStrategy,
    user_id: str
):
    """Create a multi-leg option strategy."""
    supported_strategies = ["straddle", "strangle", "spread", "butterfly", "condor"]
    
    if strategy_type not in supported_strategies:
        raise HTTPException(status_code=400, detail=f"Unsupported strategy type: {strategy_type}")
    
    # Place orders for each leg
    results = []
    for leg in strategy.legs:
        order = OptionOrder(
            user_id=user_id,
            market_id=leg.market_id,
            order_type="limit",
            side=leg.side,
            contracts=leg.contracts,
            premium=leg.premium,
            option_side=leg.option_side,
            time_in_force="good_till_cancelled"
        )
        
        result = await place_option_order(order)
        results.append(result)
    
    return {
        "strategy_type": strategy_type,
        "legs": results,
        "net_premium": sum(Decimal(leg.premium or 0) * leg.contracts for leg in strategy.legs),
        "max_profit": strategy.max_profit,
        "max_loss": strategy.max_loss
    }


@app.get("/api/v1/options/chain/{underlying}")
async def get_option_chain(
    underlying: str,
    expiry: datetime,
    strike_range: int = 10
):
    """Get option chain for an underlying asset."""
    # This would fetch all options for the underlying and expiry
    # For now, return mock data
    current_price = Decimal("100")
    strikes = []
    
    for i in range(-strike_range, strike_range + 1):
        strike = current_price + (i * 5)
        strikes.append({
            "strike": str(strike),
            "call": {
                "market_id": f"option_{underlying}_{strike}_C_{expiry.strftime('%Y%m%d')}",
                "bid": str(max(current_price - strike + 1, Decimal("0.01"))),
                "ask": str(max(current_price - strike + 1.5, Decimal("0.01"))),
                "volume": 100,
                "open_interest": 500
            },
            "put": {
                "market_id": f"option_{underlying}_{strike}_P_{expiry.strftime('%Y%m%d')}",
                "bid": str(max(strike - current_price + 1, Decimal("0.01"))),
                "ask": str(max(strike - current_price + 1.5, Decimal("0.01"))),
                "volume": 80,
                "open_interest": 400
            }
        })
    
    return {
        "underlying": underlying,
        "spot_price": str(current_price),
        "expiry": expiry.isoformat(),
        "strikes": strikes
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
        port=8026,
        reload=True,
        log_level="info"
    ) 