"""
Compute Spot Market API endpoints

Real-time spot market trading for immediate compute allocation
"""

from fastapi import APIRouter, HTTPException, Depends, Query, WebSocket, WebSocketDisconnect
from typing import Dict, List, Optional, Any
from decimal import Decimal
from datetime import datetime, timedelta
from pydantic import BaseModel, Field

from app.auth import get_current_user
from app.engines.compute_spot_market import (
    ComputeSpotMarket,
    SpotOrder,
    SpotOrderType,
    SpotResource,
    ResourceState
)

router = APIRouter(
    prefix="/api/v1/compute-spot",
    tags=["compute_spot"]
)

# Global spot market instance (initialized in main.py)
spot_market: Optional[ComputeSpotMarket] = None


def set_spot_market(market: ComputeSpotMarket):
    """Set the spot market instance from main.py"""
    global spot_market
    spot_market = market


class SpotOrderRequest(BaseModel):
    """Request to place a spot order"""
    order_type: str = Field(..., pattern="^(market|limit|ioc|fok|post_only)$")
    side: str = Field(..., pattern="^(buy|sell)$")
    resource_type: str = Field(..., description="Type of compute resource")
    quantity: str = Field(..., description="Amount of resources")
    price: Optional[str] = Field(None, description="Price per unit (required for limit orders)")
    location_preference: Optional[str] = Field(None, description="Preferred location/region")
    urgency: str = Field(default="normal", pattern="^(normal|urgent|flexible)$")
    time_in_force: str = Field(default="GTC", pattern="^(GTC|IOC|FOK|GTD)$")
    expires_at: Optional[datetime] = Field(None, description="Expiration time for GTD orders")


class SpotPriceResponse(BaseModel):
    """Spot price information"""
    resource_type: str
    location: Optional[str]
    oracle_price: Optional[str]
    last_trade_price: Optional[str]
    best_bid: Optional[str]
    best_ask: Optional[str]
    spread: Optional[str]
    timestamp: str


class OrderBookResponse(BaseModel):
    """Order book depth"""
    resource_type: str
    location: Optional[str]
    bids: List[Dict[str, str]]
    asks: List[Dict[str, str]]
    timestamp: str


class ResourceAvailabilityResponse(BaseModel):
    """Available spot resources"""
    resources: List[Dict[str, Any]]
    total_count: int
    timestamp: str


@router.post("/order")
async def place_spot_order(
    request: SpotOrderRequest,
    current_user: dict = Depends(get_current_user)
):
    """
    Place a spot market order for immediate compute allocation
    
    Order types:
    - market: Execute immediately at best available price
    - limit: Execute at specified price or better
    - ioc: Immediate or cancel
    - fok: Fill or kill (all or nothing)
    - post_only: Only add liquidity (maker only)
    """
    if not spot_market:
        raise HTTPException(status_code=503, detail="Spot market not available")
    
    # Create order
    order = SpotOrder(
        order_id=f"SPOT_{datetime.utcnow().timestamp()}_{current_user['user_id'][:8]}",
        user_id=current_user["user_id"],
        order_type=SpotOrderType(request.order_type),
        side=request.side,
        resource_type=request.resource_type,
        quantity=Decimal(request.quantity),
        price=Decimal(request.price) if request.price else None,
        location_preference=request.location_preference,
        urgency=request.urgency,
        time_in_force=request.time_in_force,
        expires_at=request.expires_at
    )
    
    # Validate order
    if order.order_type != SpotOrderType.MARKET and not order.price:
        raise HTTPException(
            status_code=400,
            detail="Price required for non-market orders"
        )
    
    # Submit order
    result = await spot_market.submit_order(order)
    
    if not result["success"]:
        raise HTTPException(status_code=400, detail=result.get("error"))
    
    return result


@router.delete("/order/{order_id}")
async def cancel_spot_order(
    order_id: str,
    current_user: dict = Depends(get_current_user)
):
    """Cancel a spot order"""
    if not spot_market:
        raise HTTPException(status_code=503, detail="Spot market not available")
    
    result = await spot_market.cancel_order(order_id, current_user["user_id"])
    
    if not result["success"]:
        raise HTTPException(status_code=400, detail=result.get("error"))
    
    return result


@router.get("/price/{resource_type}", response_model=SpotPriceResponse)
async def get_spot_price(
    resource_type: str,
    location: Optional[str] = Query(None, description="Location/region filter")
):
    """
    Get current spot price for a resource type
    
    Returns:
    - Oracle aggregated price
    - Last trade price
    - Best bid/ask from order book
    - Spread
    """
    if not spot_market:
        raise HTTPException(status_code=503, detail="Spot market not available")
    
    price_data = await spot_market.get_spot_price(resource_type, location)
    
    return SpotPriceResponse(**price_data)


@router.get("/orderbook/{resource_type}", response_model=OrderBookResponse)
async def get_order_book(
    resource_type: str,
    location: Optional[str] = Query(None, description="Location/region filter"),
    depth: int = Query(default=20, ge=1, le=100, description="Order book depth")
):
    """Get order book depth for a resource type"""
    if not spot_market:
        raise HTTPException(status_code=503, detail="Spot market not available")
    
    orderbook = await spot_market.get_order_book(resource_type, location, depth)
    
    return OrderBookResponse(**orderbook)


@router.get("/resources", response_model=ResourceAvailabilityResponse)
async def get_available_resources(
    resource_type: Optional[str] = Query(None, description="Filter by resource type"),
    location: Optional[str] = Query(None, description="Filter by location"),
    min_capacity: Optional[str] = Query(None, description="Minimum available capacity"),
    max_price: Optional[str] = Query(None, description="Maximum price filter")
):
    """
    Get available spot resources
    
    Returns list of resources currently available for immediate allocation
    """
    if not spot_market:
        raise HTTPException(status_code=503, detail="Spot market not available")
    
    resources = await spot_market.get_available_resources(resource_type, location)
    
    # Apply additional filters
    if min_capacity:
        min_cap = Decimal(min_capacity)
        resources = [r for r in resources if Decimal(r["available_capacity"]) >= min_cap]
        
    if max_price:
        max_p = Decimal(max_price)
        resources = [r for r in resources if Decimal(r["current_price"]) <= max_p]
    
    return ResourceAvailabilityResponse(
        resources=resources,
        total_count=len(resources),
        timestamp=datetime.utcnow().isoformat()
    )


@router.get("/trades/recent")
async def get_recent_trades(
    resource_type: Optional[str] = Query(None, description="Filter by resource type"),
    location: Optional[str] = Query(None, description="Filter by location"),
    limit: int = Query(default=100, ge=1, le=500, description="Number of trades")
):
    """Get recent spot market trades"""
    if not spot_market:
        raise HTTPException(status_code=503, detail="Spot market not available")
    
    # Get trades from spot market
    trades = spot_market.recent_trades[-limit:]
    
    # Apply filters
    if resource_type:
        trades = [t for t in trades if t.resource_type == resource_type]
    if location:
        trades = [t for t in trades if t.location == location]
    
    return {
        "trades": [
            {
                "trade_id": t.trade_id,
                "resource_type": t.resource_type,
                "quantity": str(t.quantity),
                "price": str(t.price),
                "location": t.location,
                "execution_time": t.execution_time.isoformat(),
                "buyer_id": t.buyer_id[:8] + "...",  # Privacy
                "seller_id": t.seller_id[:8] + "..."
            }
            for t in trades
        ],
        "count": len(trades)
    }


@router.get("/stats")
async def get_spot_market_stats():
    """Get spot market statistics"""
    if not spot_market:
        raise HTTPException(status_code=503, detail="Spot market not available")
    
    stats = await spot_market.get_market_stats()
    
    return stats


@router.websocket("/ws/prices")
async def websocket_spot_prices(websocket: WebSocket):
    """
    WebSocket endpoint for real-time spot prices
    
    Client sends:
    {
        "action": "subscribe",
        "resource_types": ["gpu", "cpu"],
        "locations": ["us-east-1", "eu-west-1"]
    }
    
    Server sends:
    {
        "type": "price_update",
        "resource_type": "gpu",
        "location": "us-east-1",
        "spot_price": "12.50",
        "best_bid": "12.45",
        "best_ask": "12.55",
        "timestamp": "2024-01-15T10:30:00Z"
    }
    """
    await websocket.accept()
    
    subscriptions = set()
    
    try:
        while True:
            # Receive message
            data = await websocket.receive_json()
            
            if data.get("action") == "subscribe":
                # Add subscriptions
                for rt in data.get("resource_types", []):
                    for loc in data.get("locations", []):
                        subscriptions.add((rt, loc))
                        
            elif data.get("action") == "unsubscribe":
                # Remove subscriptions
                for rt in data.get("resource_types", []):
                    for loc in data.get("locations", []):
                        subscriptions.discard((rt, loc))
                        
            # Send price updates
            for resource_type, location in subscriptions:
                price_data = await spot_market.get_spot_price(resource_type, location)
                
                await websocket.send_json({
                    "type": "price_update",
                    "resource_type": resource_type,
                    "location": location,
                    "spot_price": price_data.get("last_trade_price"),
                    "best_bid": price_data.get("best_bid"),
                    "best_ask": price_data.get("best_ask"),
                    "timestamp": price_data.get("timestamp")
                })
                
            # Rate limit
            import asyncio
            await asyncio.sleep(1)  # Update every second
            
    except WebSocketDisconnect:
        pass
    except Exception as e:
        await websocket.close(code=1000)


@router.websocket("/ws/orderbook/{resource_type}")
async def websocket_orderbook(
    websocket: WebSocket,
    resource_type: str,
    location: Optional[str] = None
):
    """
    WebSocket endpoint for real-time order book updates
    
    Sends order book snapshots and incremental updates
    """
    await websocket.accept()
    
    try:
        while True:
            # Send orderbook snapshot
            orderbook = await spot_market.get_order_book(resource_type, location, depth=10)
            
            await websocket.send_json({
                "type": "orderbook_snapshot",
                "resource_type": resource_type,
                "location": location,
                "bids": orderbook["bids"],
                "asks": orderbook["asks"],
                "timestamp": orderbook["timestamp"]
            })
            
            # Rate limit
            import asyncio
            await asyncio.sleep(0.5)  # Update every 500ms
            
    except WebSocketDisconnect:
        pass
    except Exception as e:
        await websocket.close(code=1000)


@router.post("/arbitrage/opportunity")
async def find_arbitrage_opportunity(
    resource_type: str,
    min_profit_margin: Decimal = Query(default=Decimal("0.02"), description="Minimum profit margin"),
    current_user: dict = Depends(get_current_user)
):
    """
    Find arbitrage opportunities between spot and futures markets
    
    Identifies price discrepancies that can be exploited
    """
    if not spot_market:
        raise HTTPException(status_code=503, detail="Spot market not available")
    
    # Get spot price
    spot_price_data = await spot_market.get_spot_price(resource_type)
    spot_price = Decimal(spot_price_data.get("last_trade_price", "0"))
    
    if spot_price == 0:
        return {"opportunities": [], "message": "No spot price available"}
    
    # Get futures prices (would integrate with futures engine)
    # For now, simulate
    futures_price = spot_price * Decimal("1.05")  # 5% premium
    
    # Calculate arbitrage opportunity
    price_diff = futures_price - spot_price
    profit_margin = price_diff / spot_price
    
    opportunities = []
    if profit_margin >= min_profit_margin:
        opportunities.append({
            "type": "cash_and_carry",
            "resource_type": resource_type,
            "spot_price": str(spot_price),
            "futures_price": str(futures_price),
            "profit_margin": str(profit_margin),
            "strategy": "Buy spot, sell futures",
            "estimated_profit_per_unit": str(price_diff)
        })
    elif profit_margin <= -min_profit_margin:
        opportunities.append({
            "type": "reverse_cash_and_carry",
            "resource_type": resource_type,
            "spot_price": str(spot_price),
            "futures_price": str(futures_price),
            "profit_margin": str(abs(profit_margin)),
            "strategy": "Sell spot, buy futures",
            "estimated_profit_per_unit": str(abs(price_diff))
        })
    
    return {
        "opportunities": opportunities,
        "scanned_at": datetime.utcnow().isoformat()
    }


@router.post("/flash-provision")
async def flash_provision_resources(
    resource_type: str,
    quantity: str,
    max_duration_seconds: int = Query(default=300, ge=60, le=3600),
    current_user: dict = Depends(get_current_user)
):
    """
    Flash provisioning for ultra-short term compute needs
    
    Get resources for seconds to minutes with instant allocation
    """
    if not spot_market:
        raise HTTPException(status_code=503, detail="Spot market not available")
    
    # Create market order for immediate execution
    order = SpotOrder(
        order_id=f"FLASH_{datetime.utcnow().timestamp()}",
        user_id=current_user["user_id"],
        order_type=SpotOrderType.MARKET,
        side="buy",
        resource_type=resource_type,
        quantity=Decimal(quantity),
        urgency="urgent",
        metadata={
            "flash_provision": True,
            "max_duration": max_duration_seconds
        }
    )
    
    # Submit order
    result = await spot_market.submit_order(order)
    
    if not result["success"]:
        raise HTTPException(status_code=400, detail=result.get("error"))
    
    # Add termination schedule
    if result.get("trades"):
        result["auto_terminate_at"] = (
            datetime.utcnow() + timedelta(seconds=max_duration_seconds)
        ).isoformat()
    
    return result


def get_spot_market():
    """Dependency to get spot market instance"""
    if not spot_market:
        raise HTTPException(status_code=503, detail="Spot market not initialized")
    return spot_market 