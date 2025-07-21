"""Direct platform integration for ultra-low latency."""

from typing import Dict, Any, List, Optional
from decimal import Decimal
import asyncio
from datetime import datetime

from ..models.order import Order, OrderType, OrderSide
from ..models.trade import Trade
from ..core.matching_engine import MatchingEngine
from ..core.order_manager import OrderManager
from ..core.position_manager import PositionManager
from ..state.ignite_manager import IgniteStateManager

# Import shared communication layer
import sys
sys.path.append('/app/services/MarketServices/shared')
from direct_communication import DirectCommunicator, MessageType


class PlatformDirectIntegration:
    """Direct integration with platform service for sub-millisecond operations."""
    
    def __init__(self,
                 matching_engine: MatchingEngine,
                 order_manager: OrderManager,
                 position_manager: PositionManager,
                 state_manager: IgniteStateManager):
        
        self.matching_engine = matching_engine
        self.order_manager = order_manager
        self.position_manager = position_manager
        self.state_manager = state_manager
        
        # Initialize direct communicator
        self.communicator = DirectCommunicator(
            service_id="trading-core",
            ignite_client=state_manager.client
        )
        
        # Pre-compile order validation rules for speed
        self._validation_cache = {}
        
    async def initialize(self):
        """Initialize direct handlers."""
        
        # Register high-performance handlers
        await self.communicator.register_handler(
            MessageType.ORDER_SUBMIT,
            self._handle_direct_order
        )
        
        await self.communicator.register_handler(
            MessageType.COPY_TRADE,
            self._handle_copy_trade
        )
        
        await self.communicator.register_handler(
            MessageType.RISK_CHECK,
            self._handle_risk_check
        )
        
        # Start message processor
        asyncio.create_task(self.communicator.process_incoming())
        
    async def _handle_direct_order(self, data: Dict[str, Any], msg) -> Dict[str, Any]:
        """Handle order submission with minimal overhead."""
        
        start_ns = asyncio.get_event_loop().time_ns()
        
        try:
            # Fast path validation
            if not self._fast_validate_order(data):
                return {
                    "success": False,
                    "reason": "validation_failed",
                    "latency_ns": asyncio.get_event_loop().time_ns() - start_ns
                }
            
            # Create order object (optimized)
            order = Order(
                order_id=data.get("order_id"),
                user_id=data["user_id"],
                market_id=data["market_id"],
                product_type=data.get("product_type", "spot"),
                type=OrderType(data["type"]),
                side=OrderSide(data["side"]),
                quantity=Decimal(data["quantity"]),
                price=Decimal(data.get("price")) if data.get("price") else None,
                client_order_id=data.get("client_order_id")
            )
            
            # Direct matching engine submission (bypass HTTP serialization)
            result = await self.matching_engine.process_order(order)
            
            # Update position immediately if filled
            if result.get("status") == "filled" and result.get("trades"):
                await self._update_positions_direct(result["trades"])
            
            return {
                "success": True,
                "order_id": order.order_id,
                "status": result.get("status"),
                "filled_quantity": str(result.get("filled_quantity", 0)),
                "trades": result.get("trades", []),
                "latency_ns": asyncio.get_event_loop().time_ns() - start_ns
            }
            
        except Exception as e:
            return {
                "success": False,
                "reason": str(e),
                "latency_ns": asyncio.get_event_loop().time_ns() - start_ns
            }
    
    async def _handle_copy_trade(self, data: Dict[str, Any], msg) -> Dict[str, Any]:
        """Handle copy trading with optimized batch processing."""
        
        start_ns = asyncio.get_event_loop().time_ns()
        
        # Extract copy trade data
        leader_trade = data["leader_trade"]
        follower_orders = data["follower_orders"]  # List of orders
        
        # Batch process all follower orders
        results = []
        
        # Use asyncio.gather for parallel processing
        tasks = []
        for follower_order in follower_orders:
            # Add leader trade reference
            follower_order["metadata"] = {
                "copy_trade": True,
                "leader_id": leader_trade["user_id"],
                "leader_order_id": leader_trade["order_id"]
            }
            
            # Create task for order processing
            task = self._process_follower_order(follower_order)
            tasks.append(task)
        
        # Execute all orders in parallel
        results = await asyncio.gather(*tasks, return_exceptions=True)
        
        # Calculate success rate
        successful = sum(1 for r in results if isinstance(r, dict) and r.get("success"))
        
        return {
            "success": True,
            "total_orders": len(follower_orders),
            "successful_orders": successful,
            "results": results,
            "latency_ns": asyncio.get_event_loop().time_ns() - start_ns
        }
    
    async def _handle_risk_check(self, data: Dict[str, Any], msg) -> Dict[str, Any]:
        """Ultra-fast risk validation."""
        
        user_id = data["user_id"]
        order_value = Decimal(data["order_value"])
        market_id = data["market_id"]
        
        # Get user risk state from cache
        risk_state = await self.state_manager.get_risk_state(user_id)
        
        if not risk_state:
            return {"approved": True, "reason": "no_limits"}
        
        # Fast risk checks
        if risk_state.get("total_exposure", 0) + float(order_value) > risk_state.get("max_exposure", float('inf')):
            return {"approved": False, "reason": "max_exposure_exceeded"}
        
        if risk_state.get("daily_loss", 0) > risk_state.get("max_daily_loss", float('inf')):
            return {"approved": False, "reason": "daily_loss_limit"}
        
        return {"approved": True, "reason": "within_limits"}
    
    def _fast_validate_order(self, data: Dict[str, Any]) -> bool:
        """Ultra-fast order validation using cached rules."""
        
        # Check required fields
        required = ["user_id", "market_id", "type", "side", "quantity"]
        if not all(field in data for field in required):
            return False
        
        # Validate order type specific fields
        order_type = data.get("type")
        if order_type in ["limit", "stop_limit"] and "price" not in data:
            return False
        
        if order_type in ["stop", "stop_limit"] and "stop_price" not in data:
            return False
        
        return True
    
    async def _process_follower_order(self, order_data: Dict[str, Any]) -> Dict[str, Any]:
        """Process a single follower order."""
        
        try:
            # Create order
            order = Order(
                user_id=order_data["user_id"],
                market_id=order_data["market_id"],
                product_type=order_data.get("product_type", "spot"),
                type=OrderType(order_data["type"]),
                side=OrderSide(order_data["side"]),
                quantity=Decimal(order_data["quantity"]),
                price=Decimal(order_data.get("price")) if order_data.get("price") else None,
                metadata=order_data.get("metadata", {})
            )
            
            # Process through matching engine
            result = await self.matching_engine.process_order(order)
            
            return {
                "success": True,
                "user_id": order.user_id,
                "order_id": order.order_id,
                "status": result.get("status"),
                "filled_quantity": str(result.get("filled_quantity", 0))
            }
            
        except Exception as e:
            return {
                "success": False,
                "user_id": order_data.get("user_id"),
                "error": str(e)
            }
    
    async def _update_positions_direct(self, trades: List[Dict[str, Any]]):
        """Update positions directly without going through HTTP."""
        
        # Group trades by user
        user_trades = {}
        for trade in trades:
            taker_id = trade["taker_user_id"]
            maker_id = trade["maker_user_id"]
            
            if taker_id not in user_trades:
                user_trades[taker_id] = []
            if maker_id not in user_trades:
                user_trades[maker_id] = []
                
            user_trades[taker_id].append(trade)
            user_trades[maker_id].append(trade)
        
        # Update positions in parallel
        tasks = []
        for user_id, user_trade_list in user_trades.items():
            for trade in user_trade_list:
                task = self.position_manager.update_position_from_trade(
                    Trade(**trade),
                    {"tick_size": Decimal("0.01")}  # Get from market config
                )
                tasks.append(task)
        
        await asyncio.gather(*tasks, return_exceptions=True)
    
    async def notify_platform_event(self, event_type: str, data: Dict[str, Any]):
        """Notify platform service of trading events."""
        
        await self.communicator.send_direct(
            target_service="trading-platform",
            msg_type=MessageType.TRADE_EXECUTE if event_type == "trade" else MessageType.POSITION_UPDATE,
            data=data,
            wait_response=False  # Fire and forget for events
        ) 