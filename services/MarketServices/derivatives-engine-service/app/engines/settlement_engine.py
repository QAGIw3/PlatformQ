from decimal import Decimal
from typing import Dict, List, Optional, Tuple, Any
from datetime import datetime
import asyncio
import logging
from enum import Enum

from app.models.order import Trade
from app.models.position import Position, PositionSide, PositionStatus
from app.models.market import Market
from app.models.collateral import UserCollateral
from app.collateral.multi_tier_engine import MultiTierCollateralEngine
from app.fees.dynamic_fee_engine import DynamicFeeEngine
from app.integrations import IgniteCache, PulsarEventPublisher

logger = logging.getLogger(__name__)


class SettlementType(Enum):
    """Types of settlement"""
    TRADE = "trade"
    FUNDING = "funding"
    LIQUIDATION = "liquidation"
    EXPIRY = "expiry"
    EXERCISE = "exercise"  # For options


class SettlementEngine:
    """
    Handles trade settlement, position updates, and margin calculations
    
    Responsibilities:
    - Settle trades between counterparties
    - Update positions after trades
    - Calculate and update margin requirements
    - Handle PnL realization
    - Process fee distribution
    - Manage collateral movements
    """
    
    def __init__(
        self,
        collateral_engine: MultiTierCollateralEngine,
        fee_engine: DynamicFeeEngine,
        ignite_client: IgniteCache,
        pulsar_client: PulsarEventPublisher
    ):
        self.collateral_engine = collateral_engine
        self.fee_engine = fee_engine
        self.ignite = ignite_client
        self.pulsar = pulsar_client
        
        # Settlement configuration
        self.settlement_batch_size = 100
        self.settlement_interval = 1  # Process every second
        
        # Queues for pending settlements
        self.pending_trades = asyncio.Queue()
        self.pending_expirations = asyncio.Queue()
        
        # Running state
        self.running = False
        self._settlement_task = None
        
        # Metrics
        self.settlements_processed = 0
        self.settlement_failures = 0
        
    async def start(self):
        """Start the settlement engine"""
        self.running = True
        self._settlement_task = asyncio.create_task(self._settlement_loop())
        logger.info("Settlement engine started")
        
    async def stop(self):
        """Stop the settlement engine"""
        self.running = False
        if self._settlement_task:
            self._settlement_task.cancel()
            try:
                await self._settlement_task
            except asyncio.CancelledError:
                pass
        logger.info("Settlement engine stopped")
        
    async def _settlement_loop(self):
        """Main settlement processing loop"""
        while self.running:
            try:
                # Process pending trades
                trades_to_settle = []
                while not self.pending_trades.empty() and len(trades_to_settle) < self.settlement_batch_size:
                    trade = await self.pending_trades.get()
                    trades_to_settle.append(trade)
                    
                if trades_to_settle:
                    await self._settle_trades_batch(trades_to_settle)
                    
                # Process expirations
                while not self.pending_expirations.empty():
                    expiration = await self.pending_expirations.get()
                    await self._process_expiration(expiration)
                    
                # Wait before next iteration
                await asyncio.sleep(self.settlement_interval)
                
            except Exception as e:
                logger.error(f"Error in settlement loop: {e}")
                self.settlement_failures += 1
                await asyncio.sleep(self.settlement_interval)
                
    async def settle_trade(self, trade: Trade, market: Market):
        """
        Queue a trade for settlement
        """
        await self.pending_trades.put({
            "trade": trade,
            "market": market,
            "timestamp": datetime.utcnow()
        })
        
    async def _settle_trades_batch(self, trades_data: List[Dict]):
        """Settle a batch of trades"""
        settlement_events = []
        
        for trade_data in trades_data:
            try:
                event = await self._settle_single_trade(
                    trade_data["trade"],
                    trade_data["market"]
                )
                if event:
                    settlement_events.append(event)
                    self.settlements_processed += 1
                    
            except Exception as e:
                logger.error(f"Failed to settle trade {trade_data['trade'].id}: {e}")
                self.settlement_failures += 1
                
                # Emit settlement failure event
                await self.pulsar.publish("settlement-failures", {
                    "trade_id": trade_data["trade"].id,
                    "error": str(e),
                    "timestamp": datetime.utcnow().isoformat()
                })
                
        # Emit batch settlement event
        if settlement_events:
            await self.pulsar.publish("settlement-events", {
                "type": "batch_settlement",
                "count": len(settlement_events),
                "events": settlement_events,
                "timestamp": datetime.utcnow().isoformat()
            })
            
    async def _settle_single_trade(self, trade: Trade, market: Market) -> Optional[Dict]:
        """
        Settle a single trade
        
        Steps:
        1. Update or create positions for both parties
        2. Calculate and apply fees
        3. Update margin requirements
        4. Transfer collateral if needed
        5. Update trade status
        """
        logger.debug(f"Settling trade {trade.id} for market {market.id}")
        
        # Get or create positions
        buyer_position = await self._get_or_create_position(
            trade.buyer_user_id,
            market,
            PositionSide.LONG if trade.buyer_is_maker else None
        )
        
        seller_position = await self._get_or_create_position(
            trade.seller_user_id,
            market,
            PositionSide.SHORT if not trade.buyer_is_maker else None
        )
        
        # Update positions
        await self._update_position_after_trade(
            buyer_position,
            trade,
            market,
            is_buyer=True
        )
        
        await self._update_position_after_trade(
            seller_position,
            trade,
            market,
            is_buyer=False
        )
        
        # Process fees
        await self._process_trade_fees(trade, market)
        
        # Update margin requirements
        buyer_margin_ok = await self._update_margin_requirements(buyer_position, market)
        seller_margin_ok = await self._update_margin_requirements(seller_position, market)
        
        # Check if positions are adequately margined
        if not buyer_margin_ok:
            logger.warning(f"Buyer {trade.buyer_user_id} has insufficient margin after trade")
            # In production, might trigger liquidation or margin call
            
        if not seller_margin_ok:
            logger.warning(f"Seller {trade.seller_user_id} has insufficient margin after trade")
            
        # Mark trade as settled
        trade.settled = True
        trade.settlement_timestamp = datetime.utcnow()
        await self._save_trade(trade)
        
        # Create settlement event
        settlement_event = {
            "trade_id": trade.id,
            "market_id": market.id,
            "buyer_user_id": trade.buyer_user_id,
            "seller_user_id": trade.seller_user_id,
            "price": str(trade.price),
            "size": str(trade.size),
            "buyer_fee": str(trade.buyer_fee),
            "seller_fee": str(trade.seller_fee),
            "buyer_position_id": buyer_position.id,
            "seller_position_id": seller_position.id,
            "settlement_type": SettlementType.TRADE.value,
            "timestamp": datetime.utcnow().isoformat()
        }
        
        return settlement_event
        
    async def _get_or_create_position(
        self,
        user_id: str,
        market: Market,
        side: Optional[PositionSide] = None
    ) -> Position:
        """Get existing position or create new one"""
        # Try to get existing position
        position_key = f"position:{user_id}:{market.id}"
        position_data = await self.ignite.get(position_key)
        
        if position_data:
            return Position(**position_data)
            
        # Create new position
        position = Position(
            user_id=user_id,
            market_id=market.id,
            side=side or PositionSide.LONG,
            status=PositionStatus.OPEN
        )
        
        # Calculate initial margin requirement
        position.initial_margin = market.get_margin_requirement(
            position.size,
            is_initial=True
        )
        position.maintenance_margin = market.get_margin_requirement(
            position.size,
            is_initial=False
        )
        
        await self._save_position(position)
        return position
        
    async def _update_position_after_trade(
        self,
        position: Position,
        trade: Trade,
        market: Market,
        is_buyer: bool
    ):
        """Update position after a trade"""
        trade_size = trade.size
        trade_price = trade.price
        
        # Determine if adding to or reducing position
        is_increasing = (is_buyer and position.side == PositionSide.LONG) or \
                       (not is_buyer and position.side == PositionSide.SHORT)
                       
        if position.size == 0:
            # New position
            position.side = PositionSide.LONG if is_buyer else PositionSide.SHORT
            position.size = trade_size
            position.entry_price = trade_price
            
        elif is_increasing:
            # Adding to position - calculate new average entry
            total_value = (position.size * position.entry_price) + (trade_size * trade_price)
            position.size += trade_size
            position.entry_price = total_value / position.size
            
        else:
            # Reducing position
            if trade_size >= position.size:
                # Position flip or close
                remaining_size = trade_size - position.size
                
                # Realize PnL on closed portion
                if position.side == PositionSide.LONG:
                    pnl = (trade_price - position.entry_price) * position.size
                else:
                    pnl = (position.entry_price - trade_price) * position.size
                    
                position.realized_pnl += pnl
                
                if remaining_size > 0:
                    # Position flip
                    position.side = PositionSide.LONG if is_buyer else PositionSide.SHORT
                    position.size = remaining_size
                    position.entry_price = trade_price
                else:
                    # Position closed
                    position.size = Decimal("0")
                    position.status = PositionStatus.CLOSED
                    position.closed_at = datetime.utcnow()
                    
            else:
                # Partial close
                if position.side == PositionSide.LONG:
                    pnl = (trade_price - position.entry_price) * trade_size
                else:
                    pnl = (position.entry_price - trade_price) * trade_size
                    
                position.realized_pnl += pnl
                position.size -= trade_size
                
        # Add fees
        fee = trade.buyer_fee if is_buyer else trade.seller_fee
        position.fees_paid += fee
        position.realized_pnl -= fee  # Fees reduce PnL
        
        # Update mark price
        position.mark_price = market.mark_price
        
        # Update margin requirements
        position.initial_margin = market.get_margin_requirement(
            position.size,
            is_initial=True
        )
        position.maintenance_margin = market.get_margin_requirement(
            position.size,
            is_initial=False
        )
        
        # Calculate liquidation price
        if position.size > 0:
            position.liquidation_price = market.get_liquidation_price(
                position.size,
                position.entry_price,
                position.collateral,
                position.side == PositionSide.LONG
            )
            
        # Update calculations
        position.update_calculations()
        
        # Save position
        await self._save_position(position)
        
    async def _process_trade_fees(self, trade: Trade, market: Market):
        """Process and distribute trade fees"""
        # Get fee distribution config
        fee_distribution = await self.fee_engine.distribute_fees(
            trade.id,
            trade.buyer_fee + trade.seller_fee
        )
        
        # Update various recipients
        for recipient, amount in fee_distribution.items():
            if amount > 0:
                await self._credit_fee_recipient(recipient, amount, market.quote_asset)
                
    async def _credit_fee_recipient(self, recipient: str, amount: Decimal, asset: str):
        """Credit fees to a recipient"""
        if recipient == "insurance_pool":
            pool_key = f"insurance_pool:{asset}"
            current_balance = await self.ignite.get(pool_key) or Decimal("0")
            await self.ignite.set(pool_key, str(Decimal(current_balance) + amount))
            
        elif recipient == "platform_treasury":
            treasury_key = f"treasury:{asset}"
            current_balance = await self.ignite.get(treasury_key) or Decimal("0")
            await self.ignite.set(treasury_key, str(Decimal(current_balance) + amount))
            
        elif recipient == "liquidity_rewards":
            # Add to liquidity provider rewards pool
            rewards_key = f"lp_rewards:{asset}"
            current_rewards = await self.ignite.get(rewards_key) or Decimal("0")
            await self.ignite.set(rewards_key, str(Decimal(current_rewards) + amount))
            
        elif recipient == "burn":
            # Record burned amount
            burn_key = f"burned:{asset}"
            current_burned = await self.ignite.get(burn_key) or Decimal("0")
            await self.ignite.set(burn_key, str(Decimal(current_burned) + amount))
            
        # Handle referrer fees separately
        
    async def _update_margin_requirements(self, position: Position, market: Market) -> bool:
        """
        Update margin requirements and check adequacy
        Returns True if position is adequately margined
        """
        if position.size == 0:
            return True
            
        # Get user's collateral
        user_collateral = await self._get_user_collateral(position.user_id)
        
        # Calculate total collateral value
        total_collateral_value = Decimal("0")
        for collateral in user_collateral:
            if collateral.is_active:
                total_collateral_value += collateral.collateral_value_usd
                
        # Calculate required margin
        required_margin = position.initial_margin
        
        # Check if adequate
        if total_collateral_value < required_margin:
            # Insufficient margin
            logger.warning(f"User {position.user_id} has insufficient margin: "
                         f"required={required_margin}, available={total_collateral_value}")
            
            # Emit margin call event
            await self.pulsar.publish("margin-calls", {
                "user_id": position.user_id,
                "position_id": position.id,
                "market_id": market.id,
                "required_margin": str(required_margin),
                "available_margin": str(total_collateral_value),
                "timestamp": datetime.utcnow().isoformat()
            })
            
            return False
            
        # Update position's collateral tracking
        position.collateral = total_collateral_value
        await self._save_position(position)
        
        return True
        
    async def _get_user_collateral(self, user_id: str) -> List[UserCollateral]:
        """Get user's collateral positions"""
        collateral_data = await self.ignite.get_pattern(f"collateral:{user_id}:*")
        collateral_list = []
        
        for data in collateral_data.values():
            collateral = UserCollateral(**data)
            collateral_list.append(collateral)
            
        return collateral_list
        
    async def _save_position(self, position: Position):
        """Save position to storage"""
        key = f"position:{position.user_id}:{position.market_id}"
        await self.ignite.set(key, position.to_dict())
        
    async def _save_trade(self, trade: Trade):
        """Save trade to storage"""
        key = f"trade:{trade.id}"
        await self.ignite.set(key, trade.to_dict())
        
    async def process_liquidation(
        self,
        position: Position,
        market: Market,
        liquidator_id: str,
        liquidation_price: Decimal
    ):
        """Process a position liquidation"""
        # Calculate liquidation details
        liquidation_fee = position.collateral * market.taker_fee_rate * Decimal("2")  # Double fees
        remaining_collateral = position.collateral - liquidation_fee
        
        # Close position
        position.close_position(liquidation_price, liquidation_fee)
        position.status = PositionStatus.LIQUIDATED
        
        # Distribute remaining collateral
        if remaining_collateral > 0:
            # Return to user
            await self._credit_user_balance(position.user_id, remaining_collateral, market.quote_asset)
        else:
            # Socialized loss
            loss = abs(remaining_collateral)
            await self._process_socialized_loss(loss, market)
            
        # Reward liquidator
        liquidator_reward = liquidation_fee * Decimal("0.5")  # 50% of liquidation fee
        await self._credit_user_balance(liquidator_id, liquidator_reward, market.quote_asset)
        
        # Save position
        await self._save_position(position)
        
        # Emit liquidation event
        await self.pulsar.publish("liquidation-events", {
            "position_id": position.id,
            "user_id": position.user_id,
            "market_id": market.id,
            "liquidation_price": str(liquidation_price),
            "liquidator_id": liquidator_id,
            "liquidator_reward": str(liquidator_reward),
            "remaining_collateral": str(remaining_collateral),
            "timestamp": datetime.utcnow().isoformat()
        })
        
    async def _credit_user_balance(self, user_id: str, amount: Decimal, asset: str):
        """Credit user's balance"""
        balance_key = f"balance:{user_id}:{asset}"
        current_balance = await self.ignite.get(balance_key) or Decimal("0")
        await self.ignite.set(balance_key, str(Decimal(current_balance) + amount))
        
    async def _process_socialized_loss(self, loss: Decimal, market: Market):
        """Process socialized loss across all positions"""
        # In production, this would distribute loss across all profitable positions
        # proportionally to reduce systemic risk
        logger.warning(f"Socialized loss of {loss} {market.quote_asset} for market {market.id}")
        
    async def settle_expired_contract(self, market: Market):
        """Settle an expired futures/options contract"""
        if market.market_type.value not in ["futures", "options"]:
            return
            
        # Queue for processing
        await self.pending_expirations.put({
            "market": market,
            "expiry_price": market.mark_price,  # Would come from oracle
            "timestamp": datetime.utcnow()
        })
        
    async def _process_expiration(self, expiration_data: Dict):
        """Process contract expiration"""
        market = expiration_data["market"]
        expiry_price = expiration_data["expiry_price"]
        
        # Get all open positions
        positions = await self._get_all_positions_for_market(market.id)
        
        for position in positions:
            if position.status == PositionStatus.OPEN:
                # Calculate settlement PnL
                if position.side == PositionSide.LONG:
                    settlement_pnl = (expiry_price - position.entry_price) * position.size
                else:
                    settlement_pnl = (position.entry_price - expiry_price) * position.size
                    
                # Close position
                position.realized_pnl += settlement_pnl
                position.status = PositionStatus.CLOSED
                position.closed_at = datetime.utcnow()
                
                # Credit user balance
                final_balance = position.collateral + position.realized_pnl
                if final_balance > 0:
                    await self._credit_user_balance(
                        position.user_id,
                        final_balance,
                        market.quote_asset
                    )
                    
                await self._save_position(position)
                
        # Mark market as expired
        market.status = "expired"
        await self.ignite.set(f"market:{market.id}", market.to_dict())
        
    async def _get_all_positions_for_market(self, market_id: str) -> List[Position]:
        """Get all positions for a market"""
        positions_data = await self.ignite.get_pattern(f"position:*:{market_id}")
        positions = []
        
        for data in positions_data.values():
            positions.append(Position(**data))
            
        return positions
        
    async def get_settlement_metrics(self) -> Dict[str, Any]:
        """Get settlement engine metrics"""
        return {
            "settlements_processed": self.settlements_processed,
            "settlement_failures": self.settlement_failures,
            "pending_trades": self.pending_trades.qsize(),
            "pending_expirations": self.pending_expirations.qsize(),
            "success_rate": (
                self.settlements_processed / 
                (self.settlements_processed + self.settlement_failures)
                if (self.settlements_processed + self.settlement_failures) > 0
                else 0
            )
        } 