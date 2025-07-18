"""
Concentrated Liquidity AMM for Derivatives
Implements Uniswap V3-style concentrated liquidity with volatility adjustments
"""

import asyncio
from decimal import Decimal
from typing import Dict, List, Optional, Tuple
from dataclasses import dataclass
import numpy as np
from datetime import datetime, timedelta
import logging

from app.integrations import IgniteCache, PulsarEventPublisher, OracleAggregatorClient
from app.engines.pricing import BlackScholesEngine, VolatilitySurfaceEngine

logger = logging.getLogger(__name__)


@dataclass
class LiquidityPosition:
    """Represents a concentrated liquidity position"""
    id: str
    provider: str
    token0_amount: Decimal
    token1_amount: Decimal
    tick_lower: int
    tick_upper: int
    liquidity: Decimal
    fee_growth_inside_0: Decimal
    fee_growth_inside_1: Decimal
    tokens_owed_0: Decimal
    tokens_owed_1: Decimal
    created_at: datetime
    
    
@dataclass
class TickData:
    """Data for a single tick in the AMM"""
    liquidity_gross: Decimal
    liquidity_net: Decimal
    fee_growth_outside_0: Decimal
    fee_growth_outside_1: Decimal
    initialized: bool


class ConcentratedLiquidityAMM:
    """
    Revolutionary AMM design for options and perpetuals with concentrated liquidity
    """
    
    def __init__(
        self,
        ignite: IgniteCache,
        pulsar: PulsarEventPublisher,
        oracle: OracleAggregatorClient,
        pricing_engine: BlackScholesEngine,
        volatility_engine: VolatilitySurfaceEngine
    ):
        self.ignite = ignite
        self.pulsar = pulsar
        self.oracle = oracle
        self.pricing_engine = pricing_engine
        self.volatility_engine = volatility_engine
        
        # AMM parameters
        self.tick_spacing = 60  # 0.6% between ticks
        self.max_tick = 887272
        self.min_tick = -887272
        
        # Fee tiers with volatility adjustments
        self.base_fee_tiers = {
            'stable': Decimal('0.0005'),    # 0.05% for low volatility
            'normal': Decimal('0.003'),     # 0.3% for normal volatility
            'volatile': Decimal('0.01')     # 1% for high volatility
        }
        
        # Storage
        self.ticks: Dict[int, TickData] = {}
        self.positions: Dict[str, LiquidityPosition] = {}
        self.pools: Dict[str, Dict] = {}
        
    async def create_pool(
        self,
        market_id: str,
        initial_price: Decimal,
        fee_tier: str = 'normal'
    ) -> Dict:
        """Create a new concentrated liquidity pool"""
        
        # Calculate initial sqrt price
        sqrt_price = Decimal(float(initial_price) ** 0.5)
        tick = self._price_to_tick(initial_price)
        
        pool = {
            'market_id': market_id,
            'sqrt_price': sqrt_price,
            'tick': tick,
            'liquidity': Decimal('0'),
            'fee_tier': fee_tier,
            'fee_growth_global_0': Decimal('0'),
            'fee_growth_global_1': Decimal('0'),
            'protocol_fees_0': Decimal('0'),
            'protocol_fees_1': Decimal('0'),
            'created_at': datetime.utcnow()
        }
        
        # Store pool
        await self.ignite.put(f'cl_pool:{market_id}', pool)
        self.pools[market_id] = pool
        
        # Emit event
        await self.pulsar.publish('amm.pool.created', {
            'market_id': market_id,
            'initial_price': str(initial_price),
            'fee_tier': fee_tier
        })
        
        logger.info(f"Created concentrated liquidity pool for {market_id}")
        return pool
        
    async def add_liquidity(
        self,
        market_id: str,
        provider: str,
        amount0: Decimal,
        amount1: Decimal,
        tick_lower: int,
        tick_upper: int
    ) -> LiquidityPosition:
        """Add concentrated liquidity to a specific price range"""
        
        pool = self.pools.get(market_id)
        if not pool:
            raise ValueError(f"Pool not found for market {market_id}")
            
        # Validate ticks
        if tick_lower >= tick_upper:
            raise ValueError("tick_lower must be less than tick_upper")
        if tick_lower < self.min_tick or tick_upper > self.max_tick:
            raise ValueError("Ticks out of range")
            
        # Calculate liquidity amount
        liquidity = self._calculate_liquidity(
            amount0, amount1, 
            pool['sqrt_price'],
            self._tick_to_sqrt_price(tick_lower),
            self._tick_to_sqrt_price(tick_upper)
        )
        
        # Create position
        position_id = f"{provider}:{market_id}:{tick_lower}:{tick_upper}"
        position = LiquidityPosition(
            id=position_id,
            provider=provider,
            token0_amount=amount0,
            token1_amount=amount1,
            tick_lower=tick_lower,
            tick_upper=tick_upper,
            liquidity=liquidity,
            fee_growth_inside_0=Decimal('0'),
            fee_growth_inside_1=Decimal('0'),
            tokens_owed_0=Decimal('0'),
            tokens_owed_1=Decimal('0'),
            created_at=datetime.utcnow()
        )
        
        # Update ticks
        await self._update_tick(market_id, tick_lower, liquidity, True)
        await self._update_tick(market_id, tick_upper, liquidity, False)
        
        # Update pool liquidity if current price is in range
        current_tick = pool['tick']
        if tick_lower <= current_tick < tick_upper:
            pool['liquidity'] += liquidity
            
        # Store position
        self.positions[position_id] = position
        await self.ignite.put(f'cl_position:{position_id}', position)
        
        # Emit event
        await self.pulsar.publish('amm.liquidity.added', {
            'market_id': market_id,
            'provider': provider,
            'liquidity': str(liquidity),
            'tick_range': [tick_lower, tick_upper]
        })
        
        return position
        
    async def swap(
        self,
        market_id: str,
        amount_in: Decimal,
        token_in: str,  # 'token0' or 'token1'
        min_amount_out: Decimal,
        trader: str
    ) -> Dict:
        """Execute a swap through concentrated liquidity"""
        
        pool = self.pools.get(market_id)
        if not pool:
            raise ValueError(f"Pool not found for market {market_id}")
            
        # Get dynamic fee based on volatility
        fee = await self._get_dynamic_fee(market_id)
        
        # Track swap state
        amount_remaining = amount_in
        amount_out = Decimal('0')
        sqrt_price = pool['sqrt_price']
        tick = pool['tick']
        liquidity = pool['liquidity']
        
        # Determine swap direction
        zero_for_one = token_in == 'token0'
        
        # Execute swap through ticks
        while amount_remaining > 0:
            # Find next initialized tick
            next_tick = await self._get_next_initialized_tick(
                market_id, tick, zero_for_one
            )
            
            # Compute swap within current tick
            sqrt_price_next = self._tick_to_sqrt_price(next_tick)
            
            amount_in_tick, amount_out_tick, sqrt_price_new = \
                self._compute_swap_step(
                    sqrt_price,
                    sqrt_price_next,
                    liquidity,
                    amount_remaining,
                    fee,
                    zero_for_one
                )
            
            # Update amounts
            amount_remaining -= amount_in_tick
            amount_out += amount_out_tick
            sqrt_price = sqrt_price_new
            
            # Cross tick if necessary
            if sqrt_price == sqrt_price_next:
                tick = next_tick
                liquidity = await self._cross_tick(
                    market_id, tick, zero_for_one
                )
            else:
                tick = self._sqrt_price_to_tick(sqrt_price)
                break
                
        # Check slippage
        if amount_out < min_amount_out:
            raise ValueError("Insufficient output amount")
            
        # Update pool state
        pool['sqrt_price'] = sqrt_price
        pool['tick'] = tick
        pool['liquidity'] = liquidity
        
        # Update fee growth
        fee_amount = amount_in * fee
        if zero_for_one:
            pool['fee_growth_global_0'] += fee_amount / liquidity
        else:
            pool['fee_growth_global_1'] += fee_amount / liquidity
            
        # Store updated pool
        await self.ignite.put(f'cl_pool:{market_id}', pool)
        
        # Emit swap event
        await self.pulsar.publish('amm.swap.executed', {
            'market_id': market_id,
            'trader': trader,
            'token_in': token_in,
            'amount_in': str(amount_in),
            'amount_out': str(amount_out),
            'sqrt_price': str(sqrt_price),
            'fee': str(fee_amount)
        })
        
        return {
            'amount_out': amount_out,
            'fee_paid': fee_amount,
            'execution_price': amount_out / (amount_in - fee_amount),
            'price_impact': abs(float(sqrt_price**2 - pool['sqrt_price']**2)) / float(pool['sqrt_price']**2)
        }
        
    async def _get_dynamic_fee(self, market_id: str) -> Decimal:
        """Calculate dynamic fee based on volatility and volume"""
        
        # Get current volatility
        volatility = await self.volatility_engine.get_realized_volatility(
            market_id, window_hours=24
        )
        
        # Get recent volume
        volume_24h = await self.ignite.get(f'volume_24h:{market_id}', Decimal('0'))
        
        # Base fee tier
        base_fee = self.base_fee_tiers.get(
            self.pools[market_id]['fee_tier'], 
            self.base_fee_tiers['normal']
        )
        
        # Volatility adjustment (0.5x to 2x multiplier)
        vol_multiplier = min(max(volatility / Decimal('0.5'), Decimal('0.5')), Decimal('2'))
        
        # Volume discount (up to 50% reduction for high volume)
        volume_threshold = Decimal('10000000')  # $10M
        volume_discount = min(volume_24h / volume_threshold * Decimal('0.5'), Decimal('0.5'))
        
        # Time decay adjustment for options
        if await self._is_option_market(market_id):
            time_to_expiry = await self._get_time_to_expiry(market_id)
            if time_to_expiry < timedelta(days=1):
                vol_multiplier *= Decimal('1.5')  # Higher fees near expiry
                
        # Calculate final fee
        dynamic_fee = base_fee * vol_multiplier * (Decimal('1') - volume_discount)
        
        return dynamic_fee
        
    async def collect_fees(
        self,
        position_id: str,
        recipient: str
    ) -> Dict[str, Decimal]:
        """Collect accumulated fees for a liquidity position"""
        
        position = self.positions.get(position_id)
        if not position:
            raise ValueError(f"Position not found: {position_id}")
            
        # Calculate fees owed
        market_id = position_id.split(':')[1]
        pool = self.pools[market_id]
        
        # Get fee growth inside range
        fee_growth_inside_0, fee_growth_inside_1 = \
            await self._get_fee_growth_inside(
                market_id,
                position.tick_lower,
                position.tick_upper
            )
        
        # Calculate tokens owed
        tokens_owed_0 = position.liquidity * (
            fee_growth_inside_0 - position.fee_growth_inside_0
        )
        tokens_owed_1 = position.liquidity * (
            fee_growth_inside_1 - position.fee_growth_inside_1
        )
        
        # Add to existing owed amounts
        position.tokens_owed_0 += tokens_owed_0
        position.tokens_owed_1 += tokens_owed_1
        
        # Update fee growth checkpoint
        position.fee_growth_inside_0 = fee_growth_inside_0
        position.fee_growth_inside_1 = fee_growth_inside_1
        
        # Transfer fees (simplified - would integrate with token contracts)
        collected_0 = position.tokens_owed_0
        collected_1 = position.tokens_owed_1
        
        position.tokens_owed_0 = Decimal('0')
        position.tokens_owed_1 = Decimal('0')
        
        # Store updated position
        await self.ignite.put(f'cl_position:{position_id}', position)
        
        # Emit event
        await self.pulsar.publish('amm.fees.collected', {
            'position_id': position_id,
            'recipient': recipient,
            'amount_0': str(collected_0),
            'amount_1': str(collected_1)
        })
        
        return {
            'token0': collected_0,
            'token1': collected_1
        }
        
    def _calculate_liquidity(
        self,
        amount0: Decimal,
        amount1: Decimal,
        sqrt_price: Decimal,
        sqrt_price_lower: Decimal,
        sqrt_price_upper: Decimal
    ) -> Decimal:
        """Calculate liquidity from token amounts"""
        
        if sqrt_price <= sqrt_price_lower:
            # Current price below range
            return amount0 * (sqrt_price_lower * sqrt_price_upper) / \
                   (sqrt_price_upper - sqrt_price_lower)
        elif sqrt_price >= sqrt_price_upper:
            # Current price above range
            return amount1 / (sqrt_price_upper - sqrt_price_lower)
        else:
            # Current price in range
            liquidity0 = amount0 * (sqrt_price * sqrt_price_upper) / \
                        (sqrt_price_upper - sqrt_price)
            liquidity1 = amount1 / (sqrt_price - sqrt_price_lower)
            return min(liquidity0, liquidity1)
            
    def _price_to_tick(self, price: Decimal) -> int:
        """Convert price to tick"""
        return int(np.log(float(price)) / np.log(1.0001))
        
    def _tick_to_price(self, tick: int) -> Decimal:
        """Convert tick to price"""
        return Decimal(str(1.0001 ** tick))
        
    def _tick_to_sqrt_price(self, tick: int) -> Decimal:
        """Convert tick to sqrt price"""
        return Decimal(str(1.0001 ** (tick / 2)))
        
    def _sqrt_price_to_tick(self, sqrt_price: Decimal) -> int:
        """Convert sqrt price to tick"""
        return int(np.log(float(sqrt_price) ** 2) / np.log(1.0001))
        
    async def _update_tick(
        self,
        market_id: str,
        tick: int,
        liquidity_delta: Decimal,
        is_lower: bool
    ):
        """Update tick data when liquidity is added/removed"""
        
        tick_key = f'tick:{market_id}:{tick}'
        tick_data = self.ticks.get(tick, TickData(
            liquidity_gross=Decimal('0'),
            liquidity_net=Decimal('0'),
            fee_growth_outside_0=Decimal('0'),
            fee_growth_outside_1=Decimal('0'),
            initialized=False
        ))
        
        # Update liquidity
        tick_data.liquidity_gross += abs(liquidity_delta)
        if is_lower:
            tick_data.liquidity_net += liquidity_delta
        else:
            tick_data.liquidity_net -= liquidity_delta
            
        tick_data.initialized = tick_data.liquidity_gross > 0
        
        # Store tick
        self.ticks[tick] = tick_data
        await self.ignite.put(tick_key, tick_data)
        
    async def _is_option_market(self, market_id: str) -> bool:
        """Check if market is an option"""
        market_info = await self.ignite.get(f'market:{market_id}')
        return market_info and market_info.get('type') == 'option'
        
    async def _get_time_to_expiry(self, market_id: str) -> timedelta:
        """Get time until option expiry"""
        market_info = await self.ignite.get(f'market:{market_id}')
        if market_info and 'expiry' in market_info:
            expiry = datetime.fromisoformat(market_info['expiry'])
            return expiry - datetime.utcnow()
        return timedelta(days=30)  # Default 