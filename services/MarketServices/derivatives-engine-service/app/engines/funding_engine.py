from decimal import Decimal
from typing import Dict, List, Optional, Tuple, Any
from datetime import datetime, timedelta
import asyncio
import logging
from collections import defaultdict
import statistics

from app.models.market import Market, MarketType
from app.models.position import Position, PositionSide
from app.integrations import OracleAggregatorClient, IgniteCache, PulsarEventPublisher

logger = logging.getLogger(__name__)


class FundingEngine:
    """
    Calculates and applies funding rates for perpetual contracts
    
    Funding Rate = Interest Rate Component + Premium/Discount Component
    - Interest Rate: Fixed borrowing cost (e.g., 0.01% per 8h)
    - Premium/Discount: Based on mark vs index price deviation
    """
    
    def __init__(
        self,
        oracle_client: OracleAggregatorClient,
        ignite_client: IgniteCache,
        pulsar_client: PulsarEventPublisher
    ):
        self.oracle = oracle_client
        self.ignite = ignite_client
        self.pulsar = pulsar_client
        
        # Configuration
        self.funding_interval = 8 * 3600  # 8 hours in seconds
        self.base_interest_rate = Decimal("0.0001")  # 0.01% per 8h = 0.03% daily
        self.max_funding_rate = Decimal("0.003")  # 0.3% per 8h max
        self.min_funding_rate = Decimal("-0.003")  # -0.3% per 8h min
        
        # TWAP configuration
        self.twap_window = 3600  # 1 hour TWAP for funding calculation
        self.sample_interval = 60  # Sample prices every minute
        
        # Price history for TWAP calculation
        self.price_history = defaultdict(lambda: defaultdict(list))  # market_id -> price_type -> [(timestamp, price)]
        
        # Current funding rates
        self.current_funding_rates = {}  # market_id -> funding_rate
        
        # Running state
        self.running = False
        self._calculation_task = None
        
    async def start_funding_calculation_loop(self):
        """Start the funding rate calculation loop"""
        self.running = True
        self._calculation_task = asyncio.create_task(self._funding_loop())
        logger.info("Funding engine started")
        
    async def stop(self):
        """Stop the funding engine"""
        self.running = False
        if self._calculation_task:
            self._calculation_task.cancel()
            try:
                await self._calculation_task
            except asyncio.CancelledError:
                pass
        logger.info("Funding engine stopped")
        
    async def _funding_loop(self):
        """Main funding calculation and application loop"""
        while self.running:
            try:
                # Get all active perpetual markets
                markets = await self._get_active_perpetual_markets()
                
                # Collect price samples
                await self._collect_price_samples(markets)
                
                # Check if any markets need funding
                for market in markets:
                    if await self._should_apply_funding(market):
                        await self._process_funding_cycle(market)
                        
                # Wait before next iteration
                await asyncio.sleep(60)  # Check every minute
                
            except Exception as e:
                logger.error(f"Error in funding loop: {e}")
                await asyncio.sleep(60)
                
    async def _get_active_perpetual_markets(self) -> List[Market]:
        """Get all active perpetual markets"""
        # In production, this would query from database
        markets_data = await self.ignite.get("markets:perpetual:*")
        markets = []
        
        for market_data in markets_data.values():
            market = Market(**market_data)
            if market.market_type == MarketType.PERPETUAL and market.status.value == "active":
                markets.append(market)
                
        return markets
        
    async def _collect_price_samples(self, markets: List[Market]):
        """Collect price samples for TWAP calculation"""
        timestamp = datetime.utcnow()
        
        for market in markets:
            # Get current mark price (from order book)
            mark_price = await self._get_mark_price(market.id)
            
            # Get index price from oracle
            index_price = await self.oracle.get_price(market.underlying_asset)
            
            if mark_price and index_price:
                # Store in history
                self._add_price_sample(market.id, "mark", timestamp, mark_price)
                self._add_price_sample(market.id, "index", timestamp, index_price)
                
                # Clean old samples
                self._clean_old_samples(market.id)
                
    def _add_price_sample(self, market_id: str, price_type: str, timestamp: datetime, price: Decimal):
        """Add a price sample to history"""
        self.price_history[market_id][price_type].append((timestamp, price))
        
    def _clean_old_samples(self, market_id: str):
        """Remove samples older than TWAP window"""
        cutoff_time = datetime.utcnow() - timedelta(seconds=self.twap_window)
        
        for price_type in ["mark", "index"]:
            self.price_history[market_id][price_type] = [
                (ts, price) for ts, price in self.price_history[market_id][price_type]
                if ts > cutoff_time
            ]
            
    async def _should_apply_funding(self, market: Market) -> bool:
        """Check if funding should be applied to a market"""
        if not market.funding_interval:
            return False
            
        if not market.last_funding_time:
            return True
            
        time_since_funding = (datetime.utcnow() - market.last_funding_time).total_seconds()
        return time_since_funding >= market.funding_interval
        
    async def _process_funding_cycle(self, market: Market):
        """Process a complete funding cycle for a market"""
        logger.info(f"Processing funding cycle for market {market.id}")
        
        # Calculate funding rate
        funding_rate = await self._calculate_funding_rate(market)
        
        # Store current funding rate
        self.current_funding_rates[market.id] = funding_rate
        await self.ignite.set(f"funding_rate:{market.id}", {
            "rate": str(funding_rate),
            "timestamp": datetime.utcnow().isoformat()
        })
        
        # Get all open positions for this market
        positions = await self._get_open_positions(market.id)
        
        # Apply funding to each position
        total_funding_paid = Decimal("0")
        funding_events = []
        
        for position in positions:
            funding_payment = await self._apply_funding_to_position(
                position,
                funding_rate,
                market
            )
            
            if funding_payment != 0:
                funding_events.append({
                    "position_id": position.id,
                    "user_id": position.user_id,
                    "funding_payment": str(funding_payment),
                    "funding_rate": str(funding_rate),
                    "position_size": str(position.size),
                    "mark_price": str(market.mark_price)
                })
                
                total_funding_paid += abs(funding_payment)
                
        # Update market's last funding time
        market.last_funding_time = datetime.utcnow()
        market.current_funding_rate = funding_rate
        await self._update_market(market)
        
        # Emit funding event
        await self.pulsar.publish("funding-events", {
            "market_id": market.id,
            "funding_rate": str(funding_rate),
            "timestamp": datetime.utcnow().isoformat(),
            "total_funding_paid": str(total_funding_paid),
            "positions_affected": len(funding_events),
            "events": funding_events
        })
        
        logger.info(f"Completed funding cycle for {market.id}: rate={funding_rate}, positions={len(positions)}")
        
    async def _calculate_funding_rate(self, market: Market) -> Decimal:
        """
        Calculate funding rate based on mark/index price deviation
        
        Funding Rate = clamp(Interest Rate + Premium, min_rate, max_rate)
        Premium = (TWAP Mark - TWAP Index) / TWAP Index
        """
        # Calculate TWAPs
        mark_twap = self._calculate_twap(market.id, "mark")
        index_twap = self._calculate_twap(market.id, "index")
        
        if not mark_twap or not index_twap or index_twap == 0:
            # Fallback to current prices
            mark_twap = await self._get_mark_price(market.id)
            index_twap = await self.oracle.get_price(market.underlying_asset)
            
        if not mark_twap or not index_twap or index_twap == 0:
            logger.warning(f"Could not calculate funding rate for {market.id}, using 0")
            return Decimal("0")
            
        # Calculate premium/discount
        premium = (mark_twap - index_twap) / index_twap
        
        # Apply dampener to reduce volatility
        dampener = Decimal("0.1")  # 10% of premium
        adjusted_premium = premium * dampener
        
        # Calculate final funding rate
        funding_rate = self.base_interest_rate + adjusted_premium
        
        # Clamp to max/min
        funding_rate = max(self.min_funding_rate, min(self.max_funding_rate, funding_rate))
        
        logger.debug(f"Funding calc for {market.id}: mark_twap={mark_twap}, index_twap={index_twap}, "
                    f"premium={premium}, rate={funding_rate}")
                    
        return funding_rate
        
    def _calculate_twap(self, market_id: str, price_type: str) -> Optional[Decimal]:
        """Calculate time-weighted average price"""
        samples = self.price_history[market_id][price_type]
        
        if len(samples) < 2:
            return None
            
        # Sort by timestamp
        samples.sort(key=lambda x: x[0])
        
        # Calculate TWAP
        weighted_sum = Decimal("0")
        total_weight = Decimal("0")
        
        for i in range(len(samples) - 1):
            time_diff = (samples[i + 1][0] - samples[i][0]).total_seconds()
            avg_price = (samples[i][1] + samples[i + 1][1]) / 2
            
            weighted_sum += avg_price * Decimal(str(time_diff))
            total_weight += Decimal(str(time_diff))
            
        if total_weight == 0:
            return None
            
        return weighted_sum / total_weight
        
    async def _get_mark_price(self, market_id: str) -> Optional[Decimal]:
        """Get current mark price from order book or last trade"""
        # Try to get from order book mid price
        order_book = await self.ignite.get(f"orderbook:{market_id}")
        
        if order_book and "best_bid" in order_book and "best_ask" in order_book:
            best_bid = Decimal(order_book["best_bid"])
            best_ask = Decimal(order_book["best_ask"])
            if best_bid > 0 and best_ask > 0:
                return (best_bid + best_ask) / 2
                
        # Fallback to last trade price
        market_data = await self.ignite.get(f"market:{market_id}")
        if market_data and "last_trade_price" in market_data:
            return Decimal(market_data["last_trade_price"])
            
        return None
        
    async def _get_open_positions(self, market_id: str) -> List[Position]:
        """Get all open positions for a market"""
        # In production, this would query from database
        positions_data = await self.ignite.get_pattern(f"position:*:{market_id}")
        positions = []
        
        for pos_data in positions_data.values():
            position = Position(**pos_data)
            if position.status.value == "open" and position.size > 0:
                positions.append(position)
                
        return positions
        
    async def _apply_funding_to_position(
        self,
        position: Position,
        funding_rate: Decimal,
        market: Market
    ) -> Decimal:
        """
        Apply funding to a position
        
        Longs pay shorts when funding is positive
        Shorts pay longs when funding is negative
        """
        # Calculate funding payment
        funding_payment = market.calculate_funding_payment(
            position.size,
            funding_rate,
            position.side == PositionSide.LONG
        )
        
        # Apply to position
        position.apply_funding(funding_rate, funding_payment)
        
        # Update position in storage
        await self.ignite.set(f"position:{position.id}:{market.id}", position.to_dict())
        
        # Update user balance
        await self._update_user_balance(position.user_id, funding_payment)
        
        return funding_payment
        
    async def _update_user_balance(self, user_id: str, funding_payment: Decimal):
        """Update user's balance based on funding payment"""
        # In production, this would update the user's collateral/margin balance
        user_data = await self.ignite.get(f"user:{user_id}")
        if user_data:
            current_balance = Decimal(user_data.get("balance", "0"))
            new_balance = current_balance + funding_payment  # Negative payment = deduction
            user_data["balance"] = str(new_balance)
            await self.ignite.set(f"user:{user_id}", user_data)
            
    async def _update_market(self, market: Market):
        """Update market data in storage"""
        await self.ignite.set(f"market:{market.id}", market.to_dict())
        
    async def get_funding_history(
        self,
        market_id: str,
        start_time: Optional[datetime] = None,
        end_time: Optional[datetime] = None,
        limit: int = 100
    ) -> List[Dict]:
        """Get historical funding rates for a market"""
        # In production, this would query from time-series database
        history = []
        
        # Get from cache
        cached_history = await self.ignite.get(f"funding_history:{market_id}")
        if cached_history:
            history = cached_history.get("rates", [])
            
        # Filter by time range if specified
        if start_time or end_time:
            filtered = []
            for rate_data in history:
                timestamp = datetime.fromisoformat(rate_data["timestamp"])
                if start_time and timestamp < start_time:
                    continue
                if end_time and timestamp > end_time:
                    continue
                filtered.append(rate_data)
            history = filtered
            
        # Apply limit
        return history[-limit:]
        
    async def get_current_funding_rates(self) -> Dict[str, Decimal]:
        """Get current funding rates for all markets"""
        return self.current_funding_rates.copy()
        
    async def estimate_funding_payment(
        self,
        market_id: str,
        position_size: Decimal,
        is_long: bool
    ) -> Dict[str, Any]:
        """Estimate funding payment for a position"""
        current_rate = self.current_funding_rates.get(market_id, Decimal("0"))
        
        # Get market data
        market_data = await self.ignite.get(f"market:{market_id}")
        if not market_data:
            return {
                "estimated_payment": "0",
                "funding_rate": "0",
                "error": "Market not found"
            }
            
        mark_price = Decimal(market_data.get("mark_price", "0"))
        
        # Calculate payment
        payment = position_size * mark_price * current_rate
        if is_long:
            payment = -payment  # Longs pay when rate is positive
            
        return {
            "estimated_payment": str(payment),
            "funding_rate": str(current_rate),
            "mark_price": str(mark_price),
            "position_size": str(position_size),
            "is_long": is_long,
            "next_funding_time": self._get_next_funding_time(market_id)
        }
        
    def _get_next_funding_time(self, market_id: str) -> Optional[str]:
        """Get the next funding time for a market"""
        # This would calculate based on market's funding interval
        # For now, return a simple estimate
        next_time = datetime.utcnow() + timedelta(hours=8)
        return next_time.isoformat() 