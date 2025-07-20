from fastapi import APIRouter, Depends, HTTPException, Query
from pydantic import BaseModel, Field
from decimal import Decimal
from typing import Optional, List, Dict
import uuid

from ..models.order import Order, OrderType, OrderSide, TimeInForce
from ..core.matching_engine import MatchingEngine
from ..dependencies import get_matching_engine, get_current_user


router = APIRouter(prefix="/api/v1/orders", tags=["orders"])


class OrderRequest(BaseModel):
    """Request to submit an order"""
    market_id: str = Field(..., description="Market identifier")
    side: str = Field(..., pattern="^(buy|sell)$")
    order_type: str = Field(..., pattern="^(market|limit|stop|stop_limit|iceberg|post_only)$")
    quantity: Decimal = Field(..., gt=0, description="Order quantity")
    price: Optional[Decimal] = Field(None, gt=0, description="Limit price")
    stop_price: Optional[Decimal] = Field(None, gt=0, description="Stop trigger price")
    time_in_force: str = Field(default="gtc", pattern="^(gtc|ioc|fok|gtd|day)$")
    client_order_id: Optional[str] = Field(None, description="Client order ID")
    display_quantity: Optional[Decimal] = Field(None, gt=0, description="Display quantity for iceberg")
    metadata: Dict = Field(default_factory=dict, description="Additional metadata")


class OrderResponse(BaseModel):
    """Order submission response"""
    success: bool
    order_id: str
    status: Optional[str]
    filled_quantity: Optional[str]
    remaining_quantity: Optional[str]
    trades: List[Dict]
    reason: Optional[str]
    latency_ns: Optional[int]
    timestamp: int


class CancelOrderRequest(BaseModel):
    """Request to cancel an order"""
    market_id: str
    order_id: str


@router.post("/submit", response_model=OrderResponse)
async def submit_order(
    request: OrderRequest,
    matching_engine: MatchingEngine = Depends(get_matching_engine),
    current_user: Dict = Depends(get_current_user)
):
    """Submit a new order"""
    try:
        # Create order object
        order = Order(
            order_id=str(uuid.uuid4()),
            market_id=request.market_id,
            trader_id=current_user["user_id"],
            side=OrderSide(request.side),
            order_type=OrderType(request.order_type),
            quantity=request.quantity,
            price=request.price,
            stop_price=request.stop_price,
            time_in_force=TimeInForce(request.time_in_force.upper()),
            client_order_id=request.client_order_id,
            display_quantity=request.display_quantity,
            metadata={
                **request.metadata,
                "tenant_id": current_user["tenant_id"]
            }
        )
        
        # Submit to matching engine
        result = await matching_engine.submit_order(order)
        
        return OrderResponse(**result)
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.delete("/cancel", response_model=Dict)
async def cancel_order(
    request: CancelOrderRequest,
    matching_engine: MatchingEngine = Depends(get_matching_engine),
    current_user: Dict = Depends(get_current_user)
):
    """Cancel an existing order"""
    try:
        result = await matching_engine.cancel_order(
            request.market_id,
            request.order_id
        )
        
        if not result["success"]:
            raise HTTPException(status_code=404, detail=result["reason"])
        
        return result
        
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/book/{market_id}")
async def get_order_book(
    market_id: str,
    depth: int = Query(default=20, ge=1, le=100),
    matching_engine: MatchingEngine = Depends(get_matching_engine)
):
    """Get order book for a market"""
    try:
        order_book = matching_engine.get_order_book(market_id, depth)
        
        if not order_book:
            raise HTTPException(status_code=404, detail="Market not found")
        
        return order_book
        
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/trades/{market_id}")
async def get_recent_trades(
    market_id: str,
    limit: int = Query(default=100, ge=1, le=500),
    matching_engine: MatchingEngine = Depends(get_matching_engine)
):
    """Get recent trades for a market"""
    try:
        # This would query from Ignite cache
        trades = await matching_engine.ignite.get_market_trades(market_id, limit)
        
        return {
            "market_id": market_id,
            "trades": trades,
            "count": len(trades)
        }
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/metrics")
async def get_metrics(
    matching_engine: MatchingEngine = Depends(get_matching_engine)
):
    """Get matching engine metrics"""
    try:
        return matching_engine.get_metrics()
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/orders/{market_id}")
async def get_market_orders(
    market_id: str,
    status: Optional[str] = None,
    limit: int = Query(default=100, ge=1, le=1000),
    matching_engine: MatchingEngine = Depends(get_matching_engine),
    current_user: Dict = Depends(get_current_user)
):
    """Get orders for a market"""
    try:
        # This would query from Ignite cache with filters
        orders = await matching_engine.ignite.get_market_orders(market_id, limit)
        
        # Filter by trader_id for security
        user_orders = [
            o for o in orders 
            if o.get("trader_id") == current_user["user_id"]
        ]
        
        # Filter by status if provided
        if status:
            user_orders = [
                o for o in user_orders
                if o.get("status") == status
            ]
        
        return {
            "market_id": market_id,
            "orders": user_orders,
            "count": len(user_orders)
        }
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e)) 