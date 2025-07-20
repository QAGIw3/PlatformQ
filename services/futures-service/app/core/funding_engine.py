"""Funding engine for perpetual futures contracts."""

import asyncio
from datetime import datetime, timedelta
from decimal import Decimal
from typing import Dict, List, Optional
import logging

from app.config import Settings
from app.models.futures import FundingRate, FuturesPosition, PositionSide
from app.cache.ignite_manager import FuturesCacheManager
from platformq_trading_common import publish_event, EventType


logger = logging.getLogger(__name__)


class FundingEngine:
    """Manages funding rate calculations and payments for perpetual futures."""
    
    def __init__(self, settings: Settings, cache_manager: FuturesCacheManager):
        self.settings = settings
        self.cache = cache_manager
        self._running = False
        self._funding_tasks: Dict[str, asyncio.Task] = {}
        
    async def start(self):
        """Start the funding engine."""
        self._running = True
        logger.info("Funding engine started")
        
    async def stop(self):
        """Stop the funding engine."""
        self._running = False
        
        # Cancel all funding tasks
        for task in self._funding_tasks.values():
            task.cancel()
            
        # Wait for tasks to complete
        if self._funding_tasks:
            await asyncio.gather(*self._funding_tasks.values(), return_exceptions=True)
            
        logger.info("Funding engine stopped")
        
    async def start_funding_cycle(self, symbol: str):
        """Start funding cycle for a perpetual contract."""
        if symbol in self._funding_tasks:
            logger.warning(f"Funding cycle already running for {symbol}")
            return
            
        task = asyncio.create_task(self._funding_cycle(symbol))
        self._funding_tasks[symbol] = task
        logger.info(f"Started funding cycle for {symbol}")
        
    async def _funding_cycle(self, symbol: str):
        """Run funding cycle for a symbol."""
        while self._running:
            try:
                # Calculate next funding time
                next_funding = self._get_next_funding_time(
                    self.settings.funding_interval_hours
                )
                
                # Wait until funding time
                wait_seconds = (next_funding - datetime.utcnow()).total_seconds()
                if wait_seconds > 0:
                    await asyncio.sleep(wait_seconds)
                    
                # Calculate and apply funding
                await self._process_funding(symbol)
                
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Error in funding cycle for {symbol}: {e}")
                await asyncio.sleep(60)  # Retry after 1 minute
                
    async def _process_funding(self, symbol: str):
        """Process funding for a symbol."""
        try:
            # Calculate funding rate
            funding_rate = await self.calculate_funding_rate(symbol)
            
            # Get mark price
            mark_price = await self._get_mark_price(symbol)
            if not mark_price:
                logger.error(f"No mark price available for {symbol}")
                return
                
            # Store funding rate
            await self.cache.store_funding_rate(symbol, funding_rate)
            
            # Apply funding to all positions
            positions = await self.cache.get_all_positions(symbol)
            
            funding_payments = []
            for position in positions:
                payment = await self._apply_funding_to_position(
                    position, funding_rate, mark_price
                )
                if payment:
                    funding_payments.append(payment)
                    
            # Publish funding event
            await publish_event(
                EventType.FUNDING_PAYMENT,
                {
                    "symbol": symbol,
                    "funding_rate": str(funding_rate.funding_rate),
                    "mark_price": str(mark_price),
                    "positions_affected": len(funding_payments),
                    "total_funding": str(sum(p["amount"] for p in funding_payments)),
                    "timestamp": datetime.utcnow().isoformat()
                }
            )
            
            logger.info(
                f"Processed funding for {symbol}: rate={funding_rate.funding_rate}, "
                f"positions={len(funding_payments)}"
            )
            
        except Exception as e:
            logger.error(f"Error processing funding for {symbol}: {e}")
            
    async def calculate_funding_rate(self, symbol: str) -> FundingRate:
        """Calculate funding rate for a perpetual contract."""
        # Get prices
        mark_price = await self._get_mark_price(symbol)
        index_price = await self._get_index_price(symbol)
        
        if not mark_price or not index_price:
            raise ValueError(f"Missing price data for {symbol}")
            
        # Calculate premium
        premium = (mark_price - index_price) / index_price
        
        # Apply smoothing over the window
        smoothed_premium = await self._get_smoothed_premium(
            symbol, premium, self.settings.funding_smoothing_window
        )
        
        # Calculate funding rate
        # Funding Rate = Premium Index + clamp(Interest Rate - Premium Index, -0.05%, 0.05%)
        interest_rate = Decimal("0.0001")  # 0.01% per interval
        
        funding_rate = smoothed_premium + self._clamp(
            interest_rate - smoothed_premium,
            -self.settings.max_funding_rate,
            self.settings.max_funding_rate
        )
        
        # Clamp final funding rate
        funding_rate = self._clamp(
            funding_rate,
            -self.settings.max_funding_rate,
            self.settings.max_funding_rate
        )
        
        return FundingRate(
            symbol=symbol,
            funding_rate=funding_rate,
            mark_price=mark_price,
            index_price=index_price,
            timestamp=datetime.utcnow(),
            next_funding_time=self._get_next_funding_time(
                self.settings.funding_interval_hours
            ),
            interest_rate=interest_rate
        )
        
    async def _apply_funding_to_position(
        self,
        position: FuturesPosition,
        funding_rate: FundingRate,
        mark_price: Decimal
    ) -> Optional[Dict]:
        """Apply funding payment to a position."""
        try:
            # Calculate funding payment
            # Long positions pay funding when rate is positive
            # Short positions receive funding when rate is positive
            position_value = position.size * mark_price
            
            if position.side == PositionSide.LONG:
                funding_payment = -position_value * funding_rate.funding_rate
            else:
                funding_payment = position_value * funding_rate.funding_rate
                
            # Update position
            position.funding_paid += funding_payment
            position.updated_at = datetime.utcnow()
            
            # Store updated position
            await self.cache.update_position(position)
            
            # Update user balance
            await self._update_user_balance(position.user_id, funding_payment)
            
            return {
                "user_id": position.user_id,
                "position_id": position.position_id,
                "symbol": position.symbol,
                "side": position.side,
                "amount": funding_payment,
                "timestamp": datetime.utcnow()
            }
            
        except Exception as e:
            logger.error(f"Error applying funding to position {position.position_id}: {e}")
            return None
            
    async def _get_mark_price(self, symbol: str) -> Optional[Decimal]:
        """Get mark price for a symbol."""
        # Get from cache or calculate
        price_data = await self.cache.get_latest_price(symbol)
        if price_data:
            return Decimal(str(price_data))
        return None
        
    async def _get_index_price(self, symbol: str) -> Optional[Decimal]:
        """Get index price for a symbol."""
        # In production, this would aggregate prices from multiple exchanges
        # For now, use mark price with small adjustment
        mark_price = await self._get_mark_price(symbol)
        if mark_price:
            # Simulate index price slightly different from mark
            return mark_price * Decimal("0.9995")
        return None
        
    async def _get_smoothed_premium(
        self,
        symbol: str,
        current_premium: Decimal,
        window_minutes: int
    ) -> Decimal:
        """Get smoothed premium over time window."""
        # In production, this would average premiums over the window
        # For now, return current premium with slight dampening
        return current_premium * Decimal("0.8")
        
    def _clamp(self, value: Decimal, min_val: Decimal, max_val: Decimal) -> Decimal:
        """Clamp a value between min and max."""
        return max(min_val, min(value, max_val))
        
    def _get_next_funding_time(self, interval_hours: int) -> datetime:
        """Calculate next funding time."""
        now = datetime.utcnow()
        
        # Find next funding time based on interval
        # Funding times are at 00:00, 08:00, 16:00 UTC for 8-hour intervals
        hour = now.hour
        next_hour = ((hour // interval_hours) + 1) * interval_hours
        
        if next_hour >= 24:
            # Next day
            next_funding = now.replace(
                hour=0, minute=0, second=0, microsecond=0
            ) + timedelta(days=1)
        else:
            next_funding = now.replace(
                hour=next_hour, minute=0, second=0, microsecond=0
            )
            
        return next_funding
        
    async def _update_user_balance(self, user_id: str, amount: Decimal):
        """Update user balance after funding payment."""
        # In production, this would update the user's wallet balance
        # For now, just log the update
        logger.info(f"Updated balance for user {user_id}: {amount}")
        
    async def get_funding_history(
        self,
        symbol: str,
        limit: int = 100
    ) -> List[FundingRate]:
        """Get funding rate history for a symbol."""
        return await self.cache.get_funding_history(symbol, limit) 