"""Position manager for tracking and managing user positions."""

import logging
from datetime import datetime
from decimal import Decimal
from typing import Dict, List, Optional, Any
import uuid

from ..models.position import Position, PositionSide, PositionUpdate, PositionEvent
from ..models.trade import Trade
from ..state import IgniteStateManager, CacheType
from ..events import FlinkEventProcessor, EventType


logger = logging.getLogger(__name__)


class PositionManager:
    """Manages user positions across all markets and products."""
    
    def __init__(
        self,
        state_manager: IgniteStateManager,
        event_processor: FlinkEventProcessor
    ):
        self.state_manager = state_manager
        self.event_processor = event_processor
        
        # Position aggregation rules by product type
        self.aggregation_rules: Dict[str, Any] = {}
        
        # Metrics
        self.metrics = {
            "positions_opened": 0,
            "positions_closed": 0,
            "positions_liquidated": 0,
            "total_pnl": Decimal("0")
        }
    
    async def update_position_from_trade(
        self,
        trade: Trade,
        market_config: Dict[str, Any]
    ) -> Position:
        """Update or create position based on trade execution."""
        # Get or create position
        position_key = f"{trade.taker_user_id}:{trade.market_id}"
        position_data = await self.state_manager.get_position(position_key)
        
        if position_data:
            position = Position(**position_data)
            is_new = False
        else:
            # Create new position
            position = await self._create_position(
                user_id=trade.taker_user_id,
                market_id=trade.market_id,
                product_type=trade.product_type,
                market_config=market_config
            )
            is_new = True
        
        # Calculate position changes
        if trade.is_buy:
            # Long position
            if position.side == PositionSide.SHORT:
                # Closing short position
                if trade.quantity >= abs(position.quantity):
                    # Close position and possibly reverse
                    closed_quantity = abs(position.quantity)
                    position.realized_pnl += self._calculate_pnl(
                        position.entry_price,
                        trade.price,
                        closed_quantity,
                        PositionSide.SHORT
                    )
                    
                    remaining = trade.quantity - closed_quantity
                    if remaining > 0:
                        # Reverse to long
                        position.side = PositionSide.LONG
                        position.quantity = remaining
                        position.entry_price = trade.price
                    else:
                        # Position closed
                        position.quantity = Decimal("0")
                        position.side = PositionSide.NEUTRAL
                        position.is_open = False
                        position.closed_at = datetime.utcnow()
                else:
                    # Partial close
                    position.realized_pnl += self._calculate_pnl(
                        position.entry_price,
                        trade.price,
                        trade.quantity,
                        PositionSide.SHORT
                    )
                    position.quantity = abs(position.quantity) - trade.quantity
            else:
                # Adding to long position
                total_value = (position.quantity * position.entry_price) + (trade.quantity * trade.price)
                total_quantity = position.quantity + trade.quantity
                position.entry_price = total_value / total_quantity
                position.quantity = total_quantity
                position.side = PositionSide.LONG
        else:
            # Short position
            if position.side == PositionSide.LONG:
                # Closing long position
                if trade.quantity >= position.quantity:
                    # Close position and possibly reverse
                    closed_quantity = position.quantity
                    position.realized_pnl += self._calculate_pnl(
                        position.entry_price,
                        trade.price,
                        closed_quantity,
                        PositionSide.LONG
                    )
                    
                    remaining = trade.quantity - closed_quantity
                    if remaining > 0:
                        # Reverse to short
                        position.side = PositionSide.SHORT
                        position.quantity = -remaining
                        position.entry_price = trade.price
                    else:
                        # Position closed
                        position.quantity = Decimal("0")
                        position.side = PositionSide.NEUTRAL
                        position.is_open = False
                        position.closed_at = datetime.utcnow()
                else:
                    # Partial close
                    position.realized_pnl += self._calculate_pnl(
                        position.entry_price,
                        trade.price,
                        trade.quantity,
                        PositionSide.LONG
                    )
                    position.quantity -= trade.quantity
            else:
                # Adding to short position
                total_value = (abs(position.quantity) * position.entry_price) + (trade.quantity * trade.price)
                total_quantity = abs(position.quantity) + trade.quantity
                position.entry_price = total_value / total_quantity
                position.quantity = -total_quantity
                position.side = PositionSide.SHORT
        
        # Update position metrics
        position.notional_value = abs(position.quantity) * trade.price
        position.mark_price = trade.price
        position.updated_at = datetime.utcnow()
        
        # Calculate unrealized P&L
        position.calculate_pnl(trade.price)
        
        # Update margin requirements
        await self._update_margin_requirements(position, market_config)
        
        # Store updated position
        await self.state_manager.put_position(position.position_id, position.dict())
        
        # Publish position event
        event_type = "open" if is_new else ("close" if not position.is_open else "update")
        await self._publish_position_event(
            position,
            event_type,
            trade_id=trade.trade_id,
            price_update=trade.price,
            quantity_change=trade.quantity if trade.is_buy else -trade.quantity,
            realized_pnl_change=position.realized_pnl - position_data.get("realized_pnl", 0) if position_data else Decimal("0")
        )
        
        # Update metrics
        if is_new:
            self.metrics["positions_opened"] += 1
        elif not position.is_open:
            self.metrics["positions_closed"] += 1
            self.metrics["total_pnl"] += position.total_pnl
        
        return position
    
    async def get_position(
        self,
        user_id: str,
        market_id: str
    ) -> Optional[Position]:
        """Get user position for a market."""
        position_key = f"{user_id}:{market_id}"
        position_data = await self.state_manager.get_position(position_key)
        
        if position_data:
            return Position(**position_data)
        return None
    
    async def get_user_positions(
        self,
        user_id: str,
        market_id: Optional[str] = None,
        open_only: bool = True
    ) -> List[Position]:
        """Get all positions for a user."""
        positions_data = await self.state_manager.get_user_positions(
            user_id,
            market_id
        )
        
        positions = [Position(**data) for data in positions_data]
        
        if open_only:
            positions = [p for p in positions if p.is_open]
        
        return positions
    
    async def update_mark_prices(self, price_updates: Dict[str, Decimal]):
        """Update mark prices for positions."""
        for market_id, price in price_updates.items():
            # This would be optimized to batch update positions
            # For now, simplified implementation
            logger.info(f"Updated mark price for {market_id}: {price}")
    
    async def check_liquidations(self) -> List[Position]:
        """Check for positions that need liquidation."""
        liquidations = []
        
        # This would scan all positions and check margin ratios
        # Simplified for now
        
        return liquidations
    
    async def liquidate_position(
        self,
        position: Position,
        liquidation_price: Decimal
    ) -> bool:
        """Liquidate a position."""
        if not position.is_open:
            return False
        
        # Calculate liquidation P&L
        position.realized_pnl += self._calculate_pnl(
            position.entry_price,
            liquidation_price,
            abs(position.quantity),
            position.side
        )
        
        # Close position
        position.is_open = False
        position.is_liquidated = True
        position.closed_at = datetime.utcnow()
        position.quantity = Decimal("0")
        position.side = PositionSide.NEUTRAL
        
        # Store updated position
        await self.state_manager.put_position(position.position_id, position.dict())
        
        # Publish liquidation event
        await self._publish_position_event(
            position,
            "liquidate",
            price_update=liquidation_price,
            realized_pnl_change=position.realized_pnl
        )
        
        self.metrics["positions_liquidated"] += 1
        
        return True
    
    async def _create_position(
        self,
        user_id: str,
        market_id: str,
        product_type: str,
        market_config: Dict[str, Any]
    ) -> Position:
        """Create a new position."""
        position = Position(
            position_id=f"{user_id}:{market_id}",
            user_id=user_id,
            market_id=market_id,
            product_type=product_type,
            side=PositionSide.NEUTRAL,
            quantity=Decimal("0"),
            entry_price=Decimal("0"),
            mark_price=Decimal("0")
        )
        
        return position
    
    def _calculate_pnl(
        self,
        entry_price: Decimal,
        exit_price: Decimal,
        quantity: Decimal,
        side: PositionSide
    ) -> Decimal:
        """Calculate P&L for a position."""
        if side == PositionSide.LONG:
            return (exit_price - entry_price) * quantity
        elif side == PositionSide.SHORT:
            return (entry_price - exit_price) * quantity
        return Decimal("0")
    
    async def _update_margin_requirements(
        self,
        position: Position,
        market_config: Dict[str, Any]
    ):
        """Update margin requirements for a position."""
        if not position.is_open or position.quantity == 0:
            position.initial_margin = Decimal("0")
            position.maintenance_margin = Decimal("0")
            return
        
        # Get margin rates from market config
        initial_rate = Decimal(market_config.get("initial_margin_rate", "0.1"))
        maintenance_rate = Decimal(market_config.get("maintenance_margin_rate", "0.05"))
        
        # Calculate margin requirements
        position.initial_margin = position.notional_value * initial_rate
        position.maintenance_margin = position.notional_value * maintenance_rate
        
        # Calculate margin ratio
        position.calculate_margin_ratio()
        
        # Calculate leverage
        if position.collateral > 0:
            position.leverage = position.notional_value / position.collateral
    
    async def _publish_position_event(
        self,
        position: Position,
        update_type: str,
        trade_id: Optional[str] = None,
        price_update: Optional[Decimal] = None,
        quantity_change: Optional[Decimal] = None,
        realized_pnl_change: Optional[Decimal] = None
    ):
        """Publish position update event."""
        position_update = PositionUpdate(
            position=position,
            update_type=update_type,
            trade_id=trade_id,
            price_update=price_update,
            quantity_change=quantity_change,
            realized_pnl_change=realized_pnl_change
        )
        
        event = PositionEvent(
            event_id=str(uuid.uuid4()),
            event_type=EventType.POSITION_UPDATE,
            user_id=position.user_id,
            position_update=position_update,
            risk_metrics={
                "margin_ratio": str(position.margin_ratio),
                "leverage": str(position.leverage),
                "liquidation_risk": str(position.liquidation_risk)
            }
        )
        
        # Would publish to Pulsar
        # await self.event_processor.publish(event)
    
    def register_aggregation_rule(self, product_type: str, rule: Any):
        """Register product-specific position aggregation rule."""
        self.aggregation_rules[product_type] = rule
    
    def get_metrics(self) -> Dict[str, Any]:
        """Get position manager metrics."""
        return self.metrics 