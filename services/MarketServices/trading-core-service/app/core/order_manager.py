"""Order manager for handling order lifecycle."""

import logging
from datetime import datetime
from decimal import Decimal
from typing import Dict, List, Optional, Any
import asyncio

from ..models.order import (
    Order, OrderRequest, OrderUpdate, OrderCancel, OrderFilter,
    OrderStatus, OrderType
)
from ..state import IgniteStateManager, CacheType
from .matching_engine import MatchingEngine
from ..events import FlinkEventProcessor, OrderEvent, EventType


logger = logging.getLogger(__name__)


class OrderManager:
    """Manages order lifecycle and operations."""
    
    def __init__(
        self,
        state_manager: IgniteStateManager,
        matching_engine: MatchingEngine,
        event_processor: FlinkEventProcessor
    ):
        self.state_manager = state_manager
        self.matching_engine = matching_engine
        self.event_processor = event_processor
        
        # Order validation rules by product type
        self.validation_rules: Dict[str, Any] = {}
        
        # Performance tracking
        self.metrics = {
            "orders_created": 0,
            "orders_updated": 0,
            "orders_cancelled": 0,
            "validation_failures": 0
        }
    
    async def create_order(
        self,
        user_id: str,
        request: OrderRequest
    ) -> Order:
        """Create a new order."""
        # Create order instance
        order = Order(
            user_id=user_id,
            market_id=request.market_id,
            product_type=request.product_type,
            type=request.type,
            side=request.side,
            quantity=request.quantity,
            price=request.price,
            stop_price=request.stop_price,
            time_in_force=request.time_in_force,
            display_quantity=request.display_quantity,
            expire_time=request.expire_time,
            client_order_id=request.client_order_id,
            product_data=request.product_data,
            metadata=request.metadata
        )
        
        # Validate order
        validation_result = await self._validate_order(order)
        if not validation_result["valid"]:
            logger.warning(f"Order validation failed: {validation_result['reason']}")
            self.metrics["validation_failures"] += 1
            raise ValueError(f"Order validation failed: {validation_result['reason']}")
        
        # Check risk limits
        risk_check = await self._check_risk_limits(user_id, order)
        if not risk_check["passed"]:
            logger.warning(f"Risk check failed: {risk_check['reason']}")
            raise ValueError(f"Risk check failed: {risk_check['reason']}")
        
        # Store order before processing
        await self.state_manager.put_order(order.order_id, order.dict())
        
        # Process order through matching engine
        try:
            trades = await self.matching_engine.process_order(order)
            logger.info(f"Order {order.order_id} processed, {len(trades)} trades executed")
        except Exception as e:
            logger.error(f"Failed to process order {order.order_id}: {e}")
            order.status = OrderStatus.REJECTED
            await self.state_manager.put_order(order.order_id, order.dict())
            raise
        
        self.metrics["orders_created"] += 1
        
        return order
    
    async def update_order(
        self,
        user_id: str,
        update: OrderUpdate
    ) -> Order:
        """Update an existing order."""
        # Get existing order
        order_data = await self.state_manager.get_order(update.order_id)
        if not order_data:
            raise ValueError(f"Order {update.order_id} not found")
        
        order = Order(**order_data)
        
        # Verify ownership
        if order.user_id != user_id:
            raise ValueError("Unauthorized order update")
        
        # Check if order can be updated
        if order.status not in [OrderStatus.OPEN, OrderStatus.PARTIALLY_FILLED]:
            raise ValueError(f"Cannot update order in status {order.status}")
        
        # Apply updates
        updated = False
        
        if update.quantity and update.quantity != order.quantity:
            # Validate new quantity
            if update.quantity < order.filled_quantity:
                raise ValueError("New quantity cannot be less than filled quantity")
            
            order.quantity = update.quantity
            order.remaining_quantity = order.quantity - order.filled_quantity
            updated = True
        
        if update.price and update.price != order.price:
            # Only limit orders can have price updated
            if order.type != OrderType.LIMIT:
                raise ValueError("Cannot update price for non-limit orders")
            
            order.price = update.price
            updated = True
        
        if update.metadata:
            order.metadata.update(update.metadata)
            updated = True
        
        if not updated:
            return order
        
        # Update timestamp
        order.updated_at = datetime.utcnow()
        order.version += 1
        
        # Cancel existing order in matching engine
        await self.matching_engine.cancel_order(order.order_id)
        
        # Resubmit with updates
        await self.matching_engine.process_order(order)
        
        # Store updated order
        await self.state_manager.put_order(order.order_id, order.dict())
        
        # Publish event
        await self._publish_order_event(order, EventType.ORDER_UPDATE)
        
        self.metrics["orders_updated"] += 1
        
        return order
    
    async def cancel_order(
        self,
        user_id: str,
        cancel: OrderCancel
    ) -> bool:
        """Cancel an order."""
        # Get order
        order_data = await self.state_manager.get_order(cancel.order_id)
        if not order_data:
            return False
        
        order = Order(**order_data)
        
        # Verify ownership
        if order.user_id != user_id:
            raise ValueError("Unauthorized order cancellation")
        
        # Cancel through matching engine
        success = await self.matching_engine.cancel_order(cancel.order_id)
        
        if success:
            self.metrics["orders_cancelled"] += 1
        
        return success
    
    async def get_order(
        self,
        user_id: str,
        order_id: str
    ) -> Optional[Order]:
        """Get a specific order."""
        order_data = await self.state_manager.get_order(order_id)
        if not order_data:
            return None
        
        order = Order(**order_data)
        
        # Verify ownership
        if order.user_id != user_id:
            return None
        
        return order
    
    async def list_orders(
        self,
        user_id: str,
        filter: OrderFilter
    ) -> List[Order]:
        """List orders based on filter criteria."""
        # Override user_id in filter
        filter.user_id = user_id
        
        # Query from Ignite
        orders_data = await self.state_manager.get_user_orders(
            user_id,
            status=filter.status[0].value if filter.status else None
        )
        
        # Convert to Order objects
        orders = [Order(**data) for data in orders_data]
        
        # Apply additional filters
        if filter.market_id:
            orders = [o for o in orders if o.market_id == filter.market_id]
        
        if filter.product_type:
            orders = [o for o in orders if o.product_type == filter.product_type]
        
        if filter.side:
            orders = [o for o in orders if o.side == filter.side]
        
        if filter.created_after:
            orders = [o for o in orders if o.created_at >= filter.created_after]
        
        if filter.created_before:
            orders = [o for o in orders if o.created_at <= filter.created_before]
        
        # Sort by creation time (newest first)
        orders.sort(key=lambda x: x.created_at, reverse=True)
        
        # Apply pagination
        start = filter.offset
        end = start + filter.limit
        
        return orders[start:end]
    
    async def _validate_order(self, order: Order) -> Dict[str, Any]:
        """Validate order based on product type and market rules."""
        # Get market configuration
        market = await self.state_manager.get_market(order.market_id)
        if not market:
            return {"valid": False, "reason": "Invalid market"}
        
        # Basic validations
        if order.quantity <= 0:
            return {"valid": False, "reason": "Invalid quantity"}
        
        if order.type in [OrderType.LIMIT, OrderType.STOP_LIMIT] and not order.price:
            return {"valid": False, "reason": "Price required for limit orders"}
        
        if order.type in [OrderType.STOP, OrderType.STOP_LIMIT] and not order.stop_price:
            return {"valid": False, "reason": "Stop price required for stop orders"}
        
        # Product-specific validation
        if order.product_type in self.validation_rules:
            validator = self.validation_rules[order.product_type]
            result = await validator(order, market)
            if not result["valid"]:
                return result
        
        return {"valid": True, "reason": None}
    
    async def _check_risk_limits(
        self,
        user_id: str,
        order: Order
    ) -> Dict[str, Any]:
        """Check user risk limits."""
        # Get user state
        user_state = await self.state_manager.get_user_state(user_id)
        if not user_state:
            user_state = {
                "total_exposure": "0",
                "open_orders": 0,
                "margin_used": "0"
            }
        
        # Calculate order value
        order_value = order.quantity * (order.price or Decimal("1"))
        
        # Check exposure limits
        total_exposure = Decimal(user_state.get("total_exposure", "0"))
        max_exposure = Decimal("1000000")  # Would come from config
        
        if total_exposure + order_value > max_exposure:
            return {
                "passed": False,
                "reason": f"Exceeds exposure limit: {max_exposure}"
            }
        
        # Check open order limits
        open_orders = user_state.get("open_orders", 0)
        max_open_orders = 100  # Would come from config
        
        if open_orders >= max_open_orders:
            return {
                "passed": False,
                "reason": f"Exceeds open order limit: {max_open_orders}"
            }
        
        return {"passed": True, "reason": None}
    
    async def _publish_order_event(self, order: Order, event_type: EventType):
        """Publish order event."""
        event = OrderEvent(
            event_id=f"{order.order_id}:{event_type.value}",
            event_type=event_type,
            order_id=order.order_id,
            user_id=order.user_id,
            market_id=order.market_id,
            product_type=order.product_type,
            order_data=order.dict()
        )
        
        # Would publish to Pulsar
        # await self.event_processor.publish(event)
    
    def register_validator(self, product_type: str, validator: Any):
        """Register a product-specific order validator."""
        self.validation_rules[product_type] = validator
    
    def get_metrics(self) -> Dict[str, Any]:
        """Get order manager metrics."""
        return self.metrics 