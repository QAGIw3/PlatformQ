"""
Unified Trading API Endpoints

Common trading endpoints that serve both social trading and prediction markets.
Delegates to trading-core-service for actual order matching.
"""

from fastapi import APIRouter, Depends, HTTPException, Query, WebSocket, WebSocketDisconnect
from typing import List, Optional, Dict, Any
from decimal import Decimal
from datetime import datetime
import uuid
import asyncio
import logging

from platformq_shared import ServiceClient
from ..dependencies import get_trading_core_client, get_current_user

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/trading", tags=["unified-trading"])


@router.post("/orders")
async def submit_order(
    market_id: str,
    side: str,
    order_type: str,
    quantity: float,
    price: Optional[float] = None,
    stop_price: Optional[float] = None,
    metadata: Optional[Dict[str, Any]] = None,
    current_user: dict = Depends(get_current_user),
    trading_core: ServiceClient = Depends(get_trading_core_client)
):
    """
    Submit a new order to the unified trading platform.
    Works for both social trading strategies and prediction market positions.
    """
    try:
        # Prepare order request for trading-core-service
        order_request = {
            "market_id": market_id,
            "product_type": metadata.get("product_type", "spot") if metadata else "spot",
            "side": side.lower(),
            "type": order_type.lower(),
            "quantity": str(quantity),
            "price": str(price) if price else None,
            "stop_price": str(stop_price) if stop_price else None,
            "time_in_force": metadata.get("time_in_force", "GTC") if metadata else "GTC",
            "client_order_id": metadata.get("client_order_id") if metadata else None
        }
        
        # Submit to trading-core-service
        result = await trading_core.request(
            method="POST",
            path="/api/v1/orders",
            json=order_request,
            headers={
                "X-User-ID": current_user["user_id"],
                "X-Tenant-ID": current_user["tenant_id"]
            }
        )
        
        if result.get("success"):
            # Add platform-specific metadata
            result["platform_metadata"] = {
                "source": "trading-platform",
                "original_metadata": metadata
            }
        
        return result
        
    except Exception as e:
        logger.error(f"Error submitting order: {e}")
        raise HTTPException(status_code=500, detail="Failed to submit order")


@router.delete("/orders/{order_id}")
async def cancel_order(
    order_id: str,
    current_user: dict = Depends(get_current_user),
    trading_core: ServiceClient = Depends(get_trading_core_client)
):
    """Cancel an order"""
    try:
        result = await trading_core.request(
            method="DELETE",
            path=f"/api/v1/orders/{order_id}",
            headers={
                "X-User-ID": current_user["user_id"],
                "X-Tenant-ID": current_user["tenant_id"]
            }
        )
        
        return result
        
    except Exception as e:
        logger.error(f"Error canceling order: {e}")
        raise HTTPException(status_code=500, detail="Failed to cancel order")


@router.get("/orders")
async def list_orders(
    market_id: Optional[str] = Query(None),
    status: Optional[str] = Query(None),
    side: Optional[str] = Query(None),
    page: int = Query(1, ge=1),
    page_size: int = Query(20, ge=1, le=100),
    current_user: dict = Depends(get_current_user),
    trading_core: ServiceClient = Depends(get_trading_core_client)
):
    """List user's orders"""
    try:
        params = {
            "page": page,
            "page_size": page_size
        }
        
        if market_id:
            params["market_id"] = market_id
        if status:
            params["status"] = status
        if side:
            params["side"] = side
        
        result = await trading_core.request(
            method="GET",
            path="/api/v1/orders",
            params=params,
            headers={
                "X-User-ID": current_user["user_id"],
                "X-Tenant-ID": current_user["tenant_id"]
            }
        )
        
        return result
        
    except Exception as e:
        logger.error(f"Error listing orders: {e}")
        raise HTTPException(status_code=500, detail="Failed to list orders")


@router.get("/markets/{market_id}/orderbook")
async def get_orderbook(
    market_id: str,
    depth: int = Query(10, ge=1, le=100),
    trading_core: ServiceClient = Depends(get_trading_core_client)
):
    """Get market orderbook"""
    try:
        result = await trading_core.request(
            method="GET",
            path=f"/api/v1/markets/{market_id}/orderbook",
            params={"depth": depth}
        )
        
        return result
        
    except Exception as e:
        logger.error(f"Error getting orderbook: {e}")
        raise HTTPException(status_code=500, detail="Failed to get orderbook")


@router.get("/markets/{market_id}/trades")
async def get_trades(
    market_id: str,
    limit: int = Query(50, ge=1, le=500),
    trading_core: ServiceClient = Depends(get_trading_core_client)
):
    """Get recent trades for a market"""
    try:
        result = await trading_core.request(
            method="GET",
            path=f"/api/v1/markets/{market_id}/trades",
            params={"limit": limit}
        )
        
        return result
        
    except Exception as e:
        logger.error(f"Error getting trades: {e}")
        raise HTTPException(status_code=500, detail="Failed to get trades")


@router.get("/metrics")
async def get_trading_metrics(
    market_id: Optional[str] = Query(None),
    trading_core: ServiceClient = Depends(get_trading_core_client)
):
    """Get trading metrics"""
    try:
        params = {}
        if market_id:
            params["market_id"] = market_id
            
        result = await trading_core.request(
            method="GET",
            path="/api/v1/orders/metrics/summary",
            params=params
        )
        
        return result
        
    except Exception as e:
        logger.error(f"Error getting metrics: {e}")
        raise HTTPException(status_code=500, detail="Failed to get metrics")


@router.websocket("/markets/{market_id}/stream")
async def market_stream(
    websocket: WebSocket,
    market_id: str,
    current_user: dict = Depends(get_current_user)
):
    """WebSocket endpoint for real-time market data"""
    await websocket.accept()
    
    # Create WebSocket client to trading-core-service
    trading_core_ws_url = f"ws://trading-core-service:8000/api/v1/ws/markets/{market_id}"
    
    try:
        # This would proxy WebSocket connection to trading-core
        # For now, send periodic updates
        while True:
            await websocket.send_json({
                "type": "heartbeat",
                "market_id": market_id,
                "timestamp": datetime.utcnow().isoformat()
            })
            await asyncio.sleep(5)
            
    except WebSocketDisconnect:
        logger.info(f"WebSocket disconnected for market {market_id}")
    except Exception as e:
        logger.error(f"WebSocket error: {e}")
        await websocket.close()


@router.post("/batch-orders")
async def submit_batch_orders(
    orders: List[Dict[str, Any]],
    current_user: dict = Depends(get_current_user),
    trading_core: ServiceClient = Depends(get_trading_core_client)
):
    """Submit multiple orders in batch"""
    try:
        # Convert to trading-core format
        batch_requests = []
        for order in orders:
            order_request = {
                "market_id": order["market_id"],
                "product_type": order.get("product_type", "spot"),
                "side": order["side"].lower(),
                "type": order["order_type"].lower(),
                "quantity": str(order["quantity"]),
                "price": str(order["price"]) if order.get("price") else None,
                "stop_price": str(order["stop_price"]) if order.get("stop_price") else None,
                "time_in_force": order.get("time_in_force", "GTC"),
                "client_order_id": order.get("client_order_id")
            }
            batch_requests.append(order_request)
        
        # Submit batch to trading-core-service
        result = await trading_core.request(
            method="POST",
            path="/api/v1/orders/batch",
            json=batch_requests,
            headers={
                "X-User-ID": current_user["user_id"],
                "X-Tenant-ID": current_user["tenant_id"]
            }
        )
        
        return result
        
    except Exception as e:
        logger.error(f"Error submitting batch orders: {e}")
        raise HTTPException(status_code=500, detail="Failed to submit batch orders") 