from decimal import Decimal
from typing import Dict, List, Optional, Tuple
import asyncio
from datetime import datetime
import logging

from app.models.position import Position, LiquidationEvent
from app.collateral.multi_tier_engine import MultiTierCollateralEngine
from app.engines.insurance_pool import InsurancePoolManager

logger = logging.getLogger(__name__)

class PartialLiquidationEngine:
    """
    Most advantageous liquidation approach: Partial liquidations to prevent cascades
    """
    
    def __init__(
        self,
        collateral_engine: MultiTierCollateralEngine,
        insurance_pool: InsurancePoolManager,
        ignite_client,
        pulsar_client
    ):
        self.collateral_engine = collateral_engine
        self.insurance_pool = insurance_pool
        self.ignite = ignite_client
        self.pulsar = pulsar_client
        
        # Liquidation parameters (optimized for stability)
        self.config = {
            "min_health_factor": Decimal("1.0"),  # Below this, liquidation starts
            "target_health_factor": Decimal("1.25"),  # Liquidate until this is reached
            "max_liquidation_percent": Decimal("0.5"),  # Max 50% position in one go
            "liquidation_incentive": Decimal("0.05"),  # 5% bonus for liquidators
            "close_factor": Decimal("0.5"),  # Can liquidate up to 50% of position
            "dust_threshold": Decimal("10"),  # Positions below $10 are fully liquidated
            "cascade_prevention_delay": 60,  # Seconds between liquidations
        }
        
        # Tracking for cascade prevention
        self.recent_liquidations = {}
        
    async def check_and_liquidate_positions(self, market_id: str):
        """
        Main liquidation loop - checks all positions and partially liquidates if needed
        """
        positions = await self._get_all_positions(market_id)
        
        liquidation_candidates = []
        
        for position in positions:
            health_factor = await self.collateral_engine.check_health_factor(
                position.user_id,
                position.borrowed_amount
            )
            
            if health_factor < self.config["min_health_factor"]:
                # Check cascade prevention
                if not self._should_delay_liquidation(position.user_id):
                    liquidation_candidates.append({
                        "position": position,
                        "health_factor": health_factor
                    })
        
        # Sort by health factor (lowest first)
        liquidation_candidates.sort(key=lambda x: x["health_factor"])
        
        # Process liquidations
        for candidate in liquidation_candidates:
            try:
                await self._execute_partial_liquidation(
                    candidate["position"],
                    candidate["health_factor"]
                )
            except Exception as e:
                logger.error(f"Liquidation failed for position {candidate['position'].id}: {e}")
    
    async def _execute_partial_liquidation(
        self,
        position: Position,
        current_health: Decimal
    ) -> LiquidationEvent:
        """
        Execute partial liquidation to restore health factor
        Most advantageous: Only liquidate what's necessary
        """
        # Calculate how much to liquidate
        liquidation_amount = self._calculate_liquidation_amount(
            position,
            current_health,
            self.config["target_health_factor"]
        )
        
        # Check if it's a dust position
        if position.value < self.config["dust_threshold"]:
            liquidation_amount = position.size  # Full liquidation for dust
        
        # Cap at max liquidation percent
        max_allowed = position.size * self.config["max_liquidation_percent"]
        liquidation_amount = min(liquidation_amount, max_allowed)
        
        # Calculate liquidation price with incentive
        mark_price = await self._get_mark_price(position.market_id)
        if position.side == "LONG":
            liquidation_price = mark_price * (Decimal("1") - self.config["liquidation_incentive"])
        else:
            liquidation_price = mark_price * (Decimal("1") + self.config["liquidation_incentive"])
        
        # Execute liquidation
        liquidation_event = LiquidationEvent(
            position_id=position.id,
            user_id=position.user_id,
            market_id=position.market_id,
            liquidated_size=liquidation_amount,
            remaining_size=position.size - liquidation_amount,
            liquidation_price=liquidation_price,
            mark_price=mark_price,
            liquidator_bonus=liquidation_amount * mark_price * self.config["liquidation_incentive"],
            timestamp=datetime.utcnow()
        )
        
        # Update position
        await self._update_position_after_liquidation(position, liquidation_event)
        
        # Transfer collateral to insurance pool
        await self._process_liquidation_settlement(position, liquidation_event)
        
        # Emit events
        await self._emit_liquidation_event(liquidation_event)
        
        # Track for cascade prevention
        self.recent_liquidations[position.user_id] = datetime.utcnow()
        
        return liquidation_event
    
    def _calculate_liquidation_amount(
        self,
        position: Position,
        current_health: Decimal,
        target_health: Decimal
    ) -> Decimal:
        """
        Calculate optimal liquidation amount to reach target health
        This prevents over-liquidation and reduces user losses
        """
        # Complex calculation to find minimum liquidation amount
        # Simplified version here - actual implementation would be more sophisticated
        
        health_deficit = target_health - current_health
        
        # Estimate how much position reduction improves health
        # This depends on collateral composition and market conditions
        position_impact = position.borrowed_amount / position.size
        
        # Calculate required reduction
        required_reduction = (health_deficit * position.borrowed_amount) / (
            position_impact * target_health
        )
        
        # Apply close factor limit
        max_liquidation = position.size * self.config["close_factor"]
        
        return min(required_reduction, max_liquidation)
    
    def _should_delay_liquidation(self, user_id: str) -> bool:
        """
        Cascade prevention: Don't liquidate same user too quickly
        """
        if user_id not in self.recent_liquidations:
            return False
        
        time_since_last = (
            datetime.utcnow() - self.recent_liquidations[user_id]
        ).total_seconds()
        
        return time_since_last < self.config["cascade_prevention_delay"]
    
    async def _process_liquidation_settlement(
        self,
        position: Position,
        event: LiquidationEvent
    ):
        """
        Settle liquidation with insurance pool
        """
        # Calculate proceeds
        liquidation_value = event.liquidated_size * event.liquidation_price
        
        # PnL calculation
        if position.side == "LONG":
            pnl = (event.liquidation_price - position.entry_price) * event.liquidated_size
        else:
            pnl = (position.entry_price - event.liquidation_price) * event.liquidated_size
        
        # If loss, insurance pool covers it
        if pnl < 0:
            await self.insurance_pool.cover_loss(
                position.market_id,
                abs(pnl),
                event
            )
        
        # Transfer remaining collateral
        remaining_collateral = await self._calculate_remaining_collateral(
            position,
            event
        )
        
        if remaining_collateral > 0:
            # Return to user
            await self._return_collateral_to_user(
                position.user_id,
                remaining_collateral
            )
    
    async def _emit_liquidation_event(self, event: LiquidationEvent):
        """
        Publish liquidation event for transparency
        """
        await self.pulsar.send_async(
            "persistent://public/default/liquidation-events",
            {
                "event_type": "partial_liquidation",
                "position_id": event.position_id,
                "user_id": event.user_id,
                "market_id": event.market_id,
                "liquidated_size": str(event.liquidated_size),
                "liquidation_price": str(event.liquidation_price),
                "liquidator_bonus": str(event.liquidator_bonus),
                "timestamp": event.timestamp.isoformat()
            }
        )
    
    # Liquidator incentive system
    async def claim_liquidation_opportunity(
        self,
        liquidator_id: str,
        position_id: str
    ) -> Dict:
        """
        Allow external liquidators to claim opportunities
        Most advantageous: Competitive liquidation reduces losses
        """
        position = await self._get_position(position_id)
        health_factor = await self.collateral_engine.check_health_factor(
            position.user_id,
            position.borrowed_amount
        )
        
        if health_factor >= self.config["min_health_factor"]:
            raise ValueError("Position is healthy, cannot liquidate")
        
        # Reserve liquidation opportunity
        reservation = await self._reserve_liquidation(
            liquidator_id,
            position_id,
            duration=30  # 30 seconds to execute
        )
        
        return {
            "reservation_id": reservation.id,
            "position": position,
            "expected_bonus": self._estimate_liquidator_bonus(position),
            "expires_at": reservation.expires_at
        } 