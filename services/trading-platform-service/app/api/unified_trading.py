"""
Unified Trading API Endpoints

Common trading endpoints that serve both social trading and prediction markets.
"""

from fastapi import APIRouter, Depends, HTTPException, Query, WebSocket, WebSocketDisconnect
from typing import List, Optional, Dict, Any
from decimal import Decimal
from datetime import datetime
import uuid
import asyncio
import logging

from ..shared.order_matching import Order, OrderType, OrderSide, OrderStatus, UnifiedMatchingEngine
from ..dependencies import get_matching_engine, get_current_user

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/trading", tags=["unified-trading"])


@router.post("/orders")
async def submit_order(
    market_id: str,
    side: str,
    order_type: str,
    quantity: float,
    price: Optional[float] = None,
    metadata: Optional[Dict[str, Any]] = None,
    current_user: dict = Depends(get_current_user),
    matching_engine: UnifiedMatchingEngine = Depends(get_matching_engine)
):
    """
    Submit a new order to the unified trading platform.
    Works for both social trading strategies and prediction market positions.
    """
    try:
        # Create order object
        order = Order(
            order_id=str(uuid.uuid4()),
            market_id=market_id,
            trader_id=current_user["user_id"],
            side=OrderSide(side.lower()),
            order_type=OrderType(order_type.lower()),
            price=Decimal(str(price)) if price else None,
            quantity=Decimal(str(quantity)),
            metadata=metadata or {}
        )
        
        # Submit to matching engine
        order_id = await matching_engine.submit_order(order)
        
        return {
            "order_id": order_id,
            "status": "submitted",
            "market_id": market_id,
            "side": side,
            "order_type": order_type,
            "quantity": quantity,
            "price": price
        }
        
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        logger.error(f"Error submitting order: {e}")
        raise HTTPException(status_code=500, detail="Failed to submit order")


@router.delete("/orders/{order_id}")
async def cancel_order(
    order_id: str,
    current_user: dict = Depends(get_current_user),
    matching_engine: UnifiedMatchingEngine = Depends(get_matching_engine)
):
    """Cancel an existing order"""
    try:
        # Verify ownership
        order = matching_engine.orders_cache.get(order_id)
        if not order:
            raise HTTPException(status_code=404, detail="Order not found")
            
        if order.trader_id != current_user["user_id"]:
            raise HTTPException(status_code=403, detail="Not authorized to cancel this order")
            
        # Cancel order
        success = await matching_engine.cancel_order(order_id)
        
        if success:
            return {"status": "cancelled", "order_id": order_id}
        else:
            raise HTTPException(status_code=400, detail="Order cannot be cancelled")
            
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error cancelling order: {e}")
        raise HTTPException(status_code=500, detail="Failed to cancel order")


@router.get("/orders/{order_id}")
async def get_order(
    order_id: str,
    current_user: dict = Depends(get_current_user),
    matching_engine: UnifiedMatchingEngine = Depends(get_matching_engine)
):
    """Get order details"""
    try:
        order = matching_engine.orders_cache.get(order_id)
        if not order:
            raise HTTPException(status_code=404, detail="Order not found")
            
        # Check ownership or admin access
        if order.trader_id != current_user["user_id"] and "admin" not in current_user.get("roles", []):
            raise HTTPException(status_code=403, detail="Not authorized to view this order")
            
        return {
            "order_id": order.order_id,
            "market_id": order.market_id,
            "side": order.side.value,
            "order_type": order.order_type.value,
            "price": str(order.price) if order.price else None,
            "quantity": str(order.quantity),
            "filled_quantity": str(order.filled_quantity),
            "status": order.status.value,
            "timestamp": order.timestamp.isoformat(),
            "metadata": order.metadata
        }
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error getting order: {e}")
        raise HTTPException(status_code=500, detail="Failed to get order")


@router.get("/orders")
async def list_orders(
    market_id: Optional[str] = None,
    status: Optional[str] = None,
    limit: int = Query(100, le=1000),
    offset: int = Query(0, ge=0),
    current_user: dict = Depends(get_current_user),
    matching_engine: UnifiedMatchingEngine = Depends(get_matching_engine)
):
    """List user's orders with optional filtering"""
    try:
        # Get all user's orders from cache
        # In production, this would query a proper database
        user_orders = []
        
        # Filter orders
        for order_id in matching_engine.orders_cache.keys():
            order = matching_engine.orders_cache.get(order_id)
            
            if order.trader_id != current_user["user_id"]:
                continue
                
            if market_id and order.market_id != market_id:
                continue
                
            if status and order.status.value != status:
                continue
                
            user_orders.append({
                "order_id": order.order_id,
                "market_id": order.market_id,
                "side": order.side.value,
                "order_type": order.order_type.value,
                "price": str(order.price) if order.price else None,
                "quantity": str(order.quantity),
                "filled_quantity": str(order.filled_quantity),
                "status": order.status.value,
                "timestamp": order.timestamp.isoformat()
            })
            
        # Sort by timestamp descending
        user_orders.sort(key=lambda x: x["timestamp"], reverse=True)
        
        # Apply pagination
        total = len(user_orders)
        user_orders = user_orders[offset:offset + limit]
        
        return {
            "orders": user_orders,
            "total": total,
            "limit": limit,
            "offset": offset
        }
        
    except Exception as e:
        logger.error(f"Error listing orders: {e}")
        raise HTTPException(status_code=500, detail="Failed to list orders")


@router.get("/markets/{market_id}/orderbook")
async def get_order_book(
    market_id: str,
    depth: int = Query(20, le=100),
    matching_engine: UnifiedMatchingEngine = Depends(get_matching_engine)
):
    """Get order book snapshot for a market"""
    try:
        orderbook = await matching_engine.get_order_book_snapshot(market_id)
        
        # Apply depth limit
        orderbook["bids"] = orderbook["bids"][:depth]
        orderbook["asks"] = orderbook["asks"][:depth]
        
        return {
            "market_id": market_id,
            "timestamp": datetime.utcnow().isoformat(),
            "bids": orderbook["bids"],
            "asks": orderbook["asks"],
            "spread": calculate_spread(orderbook)
        }
        
    except Exception as e:
        logger.error(f"Error getting order book: {e}")
        raise HTTPException(status_code=500, detail="Failed to get order book")


@router.get("/markets/{market_id}/trades")
async def get_recent_trades(
    market_id: str,
    limit: int = Query(100, le=500),
    matching_engine: UnifiedMatchingEngine = Depends(get_matching_engine)
):
    """Get recent trades for a market"""
    try:
        # Get trades from cache
        # In production, this would query a proper database
        trades = []
        
        for trade_id in matching_engine.trades_cache.keys():
            trade = matching_engine.trades_cache.get(trade_id)
            
            if trade["market_id"] == market_id:
                trades.append(trade)
                
        # Sort by timestamp descending
        trades.sort(key=lambda x: x["timestamp"], reverse=True)
        
        # Apply limit
        trades = trades[:limit]
        
        return {
            "market_id": market_id,
            "trades": trades,
            "count": len(trades)
        }
        
    except Exception as e:
        logger.error(f"Error getting trades: {e}")
        raise HTTPException(status_code=500, detail="Failed to get trades")


@router.get("/markets/{market_id}/stats")
async def get_market_stats(
    market_id: str,
    period: str = Query("24h", regex="^(1h|24h|7d|30d)$"),
    matching_engine: UnifiedMatchingEngine = Depends(get_matching_engine)
):
    """Get market statistics"""
    try:
        # Calculate stats from trades
        # In production, this would use proper analytics
        trades = []
        
        for trade_id in matching_engine.trades_cache.keys():
            trade = matching_engine.trades_cache.get(trade_id)
            if trade["market_id"] == market_id:
                trades.append(trade)
                
        if not trades:
            return {
                "market_id": market_id,
                "period": period,
                "volume": "0",
                "trade_count": 0,
                "high": None,
                "low": None,
                "open": None,
                "close": None
            }
            
        # Calculate statistics
        prices = [Decimal(trade["price"]) for trade in trades]
        volumes = [Decimal(trade["quantity"]) for trade in trades]
        
        return {
            "market_id": market_id,
            "period": period,
            "volume": str(sum(volumes)),
            "trade_count": len(trades),
            "high": str(max(prices)),
            "low": str(min(prices)),
            "open": str(prices[-1]) if prices else None,
            "close": str(prices[0]) if prices else None,
            "average_price": str(sum(prices) / len(prices))
        }
        
    except Exception as e:
        logger.error(f"Error getting market stats: {e}")
        raise HTTPException(status_code=500, detail="Failed to get market stats")


@router.websocket("/markets/{market_id}/stream")
async def market_data_stream(
    websocket: WebSocket,
    market_id: str,
    matching_engine: UnifiedMatchingEngine = Depends(get_matching_engine)
):
    """WebSocket stream for real-time market data"""
    await websocket.accept()
    
    try:
        # Subscribe to market updates
        # In production, this would use proper pub/sub
        while True:
            # Send orderbook updates
            orderbook = await matching_engine.get_order_book_snapshot(market_id)
            
            await websocket.send_json({
                "type": "orderbook",
                "market_id": market_id,
                "data": orderbook,
                "timestamp": datetime.utcnow().isoformat()
            })
            
            # Wait before next update
            await asyncio.sleep(1)
            
    except WebSocketDisconnect:
        logger.info(f"WebSocket disconnected for market {market_id}")
    except Exception as e:
        logger.error(f"Error in market data stream: {e}")
        await websocket.close()


def calculate_spread(orderbook: dict) -> Optional[str]:
    """Calculate bid-ask spread"""
    if orderbook["bids"] and orderbook["asks"]:
        best_bid = Decimal(orderbook["bids"][0]["price"])
        best_ask = Decimal(orderbook["asks"][0]["price"])
        spread = best_ask - best_bid
        return str(spread)
    return None 