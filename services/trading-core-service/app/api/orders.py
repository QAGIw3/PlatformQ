"""Order API endpoints."""

import logging
from typing import Dict, List, Optional, Any
from datetime import datetime
from decimal import Decimal

from fastapi import APIRouter, HTTPException, Depends, Query, Response
from pydantic import BaseModel, Field, validator

from ..models.order import Order, OrderType, OrderSide, OrderStatus, TimeInForce
from ..dependencies import get_order_manager, get_matching_engine
from ..core import OrderManager, MatchingEngine


logger = logging.getLogger(__name__)


router = APIRouter(prefix="/orders", tags=["orders"])


class CreateOrderRequest(BaseModel):
    """Create order request."""
    market_id: str = Field(..., description="Market identifier")
    product_type: str = Field(default="spot", description="Product type")
    side: OrderSide = Field(..., description="Buy or sell")
    type: OrderType = Field(..., description="Order type")
    quantity: Decimal = Field(..., gt=0, description="Order quantity")
    price: Optional[Decimal] = Field(None, gt=0, description="Limit price")
    stop_price: Optional[Decimal] = Field(None, gt=0, description="Stop price")
    time_in_force: TimeInForce = Field(default=TimeInForce.GTC, description="Time in force")
    client_order_id: Optional[str] = Field(None, description="Client order ID")
    
    @validator('price')
    def validate_price(cls, v, values):
        order_type = values.get('type')
        if order_type == OrderType.LIMIT and v is None:
            raise ValueError("Price required for limit orders")
        return v
    
    @validator('stop_price')
    def validate_stop_price(cls, v, values):
        order_type = values.get('type')
        if order_type in [OrderType.STOP, OrderType.STOP_LIMIT] and v is None:
            raise ValueError("Stop price required for stop orders")
        return v


class OrderResponse(BaseModel):
    """Order response."""
    success: bool
    order_id: str
    status: Optional[str]
    filled_quantity: Optional[str]
    remaining_quantity: Optional[str]
    average_price: Optional[str]
    trades: Optional[List[Dict[str, Any]]]
    latency_ns: Optional[int]
    timestamp: int
    reason: Optional[str]
    client_order_id: Optional[str]


class OrderListResponse(BaseModel):
    """Order list response."""
    orders: List[Dict[str, Any]]
    total: int
    page: int
    page_size: int


@router.post("/", response_model=OrderResponse)
async def create_order(
    request: CreateOrderRequest,
    user_id: str = "test_user",  # Would come from auth
    order_manager: OrderManager = Depends(get_order_manager),
    matching_engine: MatchingEngine = Depends(get_matching_engine)
) -> OrderResponse:
    """Submit a new order."""
    try:
        # Create order object
        order = Order(
            user_id=user_id,
            market_id=request.market_id,
            product_type=request.product_type,
            side=request.side,
            type=request.type,
            quantity=request.quantity,
            price=request.price,
            stop_price=request.stop_price,
            time_in_force=request.time_in_force,
            client_order_id=request.client_order_id
        )
        
        # Process order through matching engine
        result = await matching_engine.process_order(order)
        
        # Calculate average price if there were trades
        avg_price = None
        if result.get('trades'):
            total_value = sum(
                Decimal(t['price']) * Decimal(t['quantity']) 
                for t in result['trades']
            )
            total_quantity = sum(Decimal(t['quantity']) for t in result['trades'])
            if total_quantity > 0:
                avg_price = str(total_value / total_quantity)
        
        return OrderResponse(
            success=result['success'],
            order_id=result['order_id'],
            status=result.get('status'),
            filled_quantity=result.get('filled_quantity'),
            remaining_quantity=result.get('remaining_quantity'),
            average_price=avg_price,
            trades=result.get('trades'),
            latency_ns=result.get('latency_ns'),
            timestamp=result['timestamp'],
            reason=result.get('reason'),
            client_order_id=request.client_order_id
        )
        
    except Exception as e:
        logger.error(f"Error creating order: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/{order_id}", response_model=Dict[str, Any])
async def get_order(
    order_id: str,
    user_id: str = "test_user",  # Would come from auth
    order_manager: OrderManager = Depends(get_order_manager)
) -> Dict[str, Any]:
    """Get order details."""
    order = await order_manager.get_order(order_id)
    
    if not order:
        raise HTTPException(status_code=404, detail="Order not found")
    
    # Check authorization
    if order.user_id != user_id:
        raise HTTPException(status_code=403, detail="Unauthorized")
    
    return order.dict()


@router.delete("/{order_id}", response_model=Dict[str, Any])
async def cancel_order(
    order_id: str,
    user_id: str = "test_user",  # Would come from auth
    order_manager: OrderManager = Depends(get_order_manager),
    matching_engine: MatchingEngine = Depends(get_matching_engine)
) -> Dict[str, Any]:
    """Cancel an order."""
    # Get order first to check authorization
    order = await order_manager.get_order(order_id)
    
    if not order:
        raise HTTPException(status_code=404, detail="Order not found")
    
    if order.user_id != user_id:
        raise HTTPException(status_code=403, detail="Unauthorized")
    
    # Cancel through matching engine
    success = await matching_engine.cancel_order(order_id)
    
    if not success:
        raise HTTPException(status_code=400, detail="Order cannot be cancelled")
    
    return {
        "success": True,
        "order_id": order_id,
        "status": "cancelled",
        "timestamp": datetime.utcnow().timestamp()
    }


@router.get("/", response_model=OrderListResponse)
async def list_orders(
    user_id: str = "test_user",  # Would come from auth
    market_id: Optional[str] = Query(None, description="Filter by market"),
    status: Optional[OrderStatus] = Query(None, description="Filter by status"),
    side: Optional[OrderSide] = Query(None, description="Filter by side"),
    start_time: Optional[datetime] = Query(None, description="Start time"),
    end_time: Optional[datetime] = Query(None, description="End time"),
    page: int = Query(1, ge=1, description="Page number"),
    page_size: int = Query(20, ge=1, le=100, description="Page size"),
    order_manager: OrderManager = Depends(get_order_manager)
) -> OrderListResponse:
    """List user orders with filters."""
    # Build filters
    filters = {
        "user_id": user_id
    }
    
    if market_id:
        filters["market_id"] = market_id
    if status:
        filters["status"] = status
    if side:
        filters["side"] = side
    if start_time:
        filters["start_time"] = start_time
    if end_time:
        filters["end_time"] = end_time
    
    # Get orders
    orders = await order_manager.list_orders(
        filters=filters,
        offset=(page - 1) * page_size,
        limit=page_size
    )
    
    # Get total count
    total = await order_manager.count_orders(filters)
    
    return OrderListResponse(
        orders=[order.dict() for order in orders],
        total=total,
        page=page,
        page_size=page_size
    )


@router.post("/batch", response_model=List[OrderResponse])
async def create_batch_orders(
    requests: List[CreateOrderRequest],
    user_id: str = "test_user",  # Would come from auth
    order_manager: OrderManager = Depends(get_order_manager),
    matching_engine: MatchingEngine = Depends(get_matching_engine)
) -> List[OrderResponse]:
    """Submit multiple orders in batch."""
    if len(requests) > 100:
        raise HTTPException(status_code=400, detail="Maximum 100 orders per batch")
    
    responses = []
    
    for request in requests:
        try:
            order = Order(
                user_id=user_id,
                market_id=request.market_id,
                product_type=request.product_type,
                side=request.side,
                type=request.type,
                quantity=request.quantity,
                price=request.price,
                stop_price=request.stop_price,
                time_in_force=request.time_in_force,
                client_order_id=request.client_order_id
            )
            
            result = await matching_engine.process_order(order)
            
            # Calculate average price
            avg_price = None
            if result.get('trades'):
                total_value = sum(
                    Decimal(t['price']) * Decimal(t['quantity']) 
                    for t in result['trades']
                )
                total_quantity = sum(Decimal(t['quantity']) for t in result['trades'])
                if total_quantity > 0:
                    avg_price = str(total_value / total_quantity)
            
            responses.append(OrderResponse(
                success=result['success'],
                order_id=result['order_id'],
                status=result.get('status'),
                filled_quantity=result.get('filled_quantity'),
                remaining_quantity=result.get('remaining_quantity'),
                average_price=avg_price,
                trades=result.get('trades'),
                latency_ns=result.get('latency_ns'),
                timestamp=result['timestamp'],
                reason=result.get('reason'),
                client_order_id=request.client_order_id
            ))
            
        except Exception as e:
            logger.error(f"Error processing batch order: {e}")
            responses.append(OrderResponse(
                success=False,
                order_id="",
                timestamp=int(datetime.utcnow().timestamp() * 1e9),
                reason=str(e),
                client_order_id=request.client_order_id
            ))
    
    return responses


@router.get("/metrics/summary", response_model=Dict[str, Any])
async def get_order_metrics(
    user_id: str = "test_user",  # Would come from auth
    market_id: Optional[str] = Query(None, description="Filter by market"),
    order_manager: OrderManager = Depends(get_order_manager),
    matching_engine: MatchingEngine = Depends(get_matching_engine),
    response: Response = None
) -> Dict[str, Any]:
    """Get order and matching engine metrics."""
    # Get user order stats
    user_stats = await order_manager.get_user_stats(user_id, market_id)
    
    # Get matching engine metrics
    engine_metrics = matching_engine.get_metrics()
    
    # Set cache headers
    if response:
        response.headers["Cache-Control"] = "public, max-age=5"
    
    return {
        "user_stats": user_stats,
        "engine_metrics": engine_metrics,
        "timestamp": datetime.utcnow().isoformat()
    } 