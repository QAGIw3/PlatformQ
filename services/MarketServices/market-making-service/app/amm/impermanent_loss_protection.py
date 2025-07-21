"""
Impermanent Loss Protection for AMM Liquidity Providers
Implements automatic hedging and insurance mechanisms
"""

import asyncio
from decimal import Decimal
from typing import Dict, List, Optional, Tuple
from dataclasses import dataclass
from datetime import datetime, timedelta
import numpy as np
import logging

from app.integrations import IgniteCache, PulsarEventPublisher, OracleAggregatorClient
from app.engines.pricing import BlackScholesEngine
from app.engines.risk import GreeksCalculator
from app.amm.concentrated_liquidity import LiquidityPosition

logger = logging.getLogger(__name__)


@dataclass
class ILProtectionPosition:
    """Represents an IL protection position"""
    position_id: str
    liquidity_position_id: str
    hedge_positions: List[Dict]
    insurance_coverage: Decimal
    premium_paid: Decimal
    created_at: datetime
    expires_at: Optional[datetime]


@dataclass
class InsuranceFund:
    """Insurance fund for IL compensation"""
    total_capital: Decimal
    active_coverage: Decimal
    claims_paid: Decimal
    premiums_collected: Decimal
    reserve_ratio: Decimal


class ImpermanentLossProtector:
    """
    Automated impermanent loss protection system
    """
    
    def __init__(
        self,
        ignite: IgniteCache,
        pulsar: PulsarEventPublisher,
        oracle: OracleAggregatorClient,
        pricing_engine: BlackScholesEngine,
        greeks_calculator: GreeksCalculator
    ):
        self.ignite = ignite
        self.pulsar = pulsar
        self.oracle = oracle
        self.pricing_engine = pricing_engine
        self.greeks_calculator = greeks_calculator
        
        # Protection parameters
        self.min_protection_period = timedelta(days=7)
        self.max_protection_period = timedelta(days=90)
        self.base_premium_rate = Decimal('0.001')  # 0.1% per week
        self.max_coverage_ratio = Decimal('0.8')  # 80% IL coverage
        
        # Insurance fund
        self.insurance_fund = InsuranceFund(
            total_capital=Decimal('0'),
            active_coverage=Decimal('0'),
            claims_paid=Decimal('0'),
            premiums_collected=Decimal('0'),
            reserve_ratio=Decimal('3')  # 3x coverage ratio required
        )
        
        # Hedging parameters
        self.hedge_rebalance_threshold = Decimal('0.05')  # 5% IL threshold
        self.delta_hedge_enabled = True
        self.options_hedge_enabled = True
        
    async def purchase_protection(
        self,
        liquidity_position: LiquidityPosition,
        protection_period: timedelta,
        coverage_percent: Decimal = Decimal('0.8')
    ) -> ILProtectionPosition:
        """Purchase IL protection for a liquidity position"""
        
        # Validate parameters
        if protection_period < self.min_protection_period:
            raise ValueError(f"Minimum protection period is {self.min_protection_period}")
        if protection_period > self.max_protection_period:
            raise ValueError(f"Maximum protection period is {self.max_protection_period}")
        if coverage_percent > self.max_coverage_ratio:
            raise ValueError(f"Maximum coverage is {self.max_coverage_ratio * 100}%")
            
        # Calculate premium
        premium = await self._calculate_premium(
            liquidity_position,
            protection_period,
            coverage_percent
        )
        
        # Check insurance fund capacity
        position_value = await self._calculate_position_value(liquidity_position)
        max_payout = position_value * coverage_percent
        
        if not await self._check_fund_capacity(max_payout):
            raise ValueError("Insufficient insurance fund capacity")
            
        # Create initial hedge
        hedge_positions = await self._create_initial_hedge(
            liquidity_position,
            coverage_percent
        )
        
        # Create protection position
        protection_position = ILProtectionPosition(
            position_id=f"ilp:{liquidity_position.id}:{datetime.utcnow().timestamp()}",
            liquidity_position_id=liquidity_position.id,
            hedge_positions=hedge_positions,
            insurance_coverage=max_payout,
            premium_paid=premium,
            created_at=datetime.utcnow(),
            expires_at=datetime.utcnow() + protection_period
        )
        
        # Update insurance fund
        self.insurance_fund.premiums_collected += premium
        self.insurance_fund.active_coverage += max_payout
        self.insurance_fund.total_capital += premium * Decimal('0.8')  # 80% to fund
        
        # Store position
        await self.ignite.put(
            f'il_protection:{protection_position.position_id}',
            protection_position
        )
        
        # Emit event
        await self.pulsar.publish('il_protection.purchased', {
            'position_id': protection_position.position_id,
            'liquidity_position_id': liquidity_position.id,
            'coverage_amount': str(max_payout),
            'premium': str(premium),
            'expires_at': protection_position.expires_at.isoformat()
        })
        
        logger.info(f"IL protection purchased for position {liquidity_position.id}")
        return protection_position
        
    async def calculate_impermanent_loss(
        self,
        liquidity_position: LiquidityPosition,
        current_prices: Dict[str, Decimal]
    ) -> Decimal:
        """Calculate current impermanent loss for a position"""
        
        # Get initial prices when position was created
        initial_prices = await self._get_initial_prices(liquidity_position)
        
        # Calculate price ratios
        price_ratio_0 = current_prices['token0'] / initial_prices['token0']
        price_ratio_1 = current_prices['token1'] / initial_prices['token1']
        
        # IL formula: 2 * sqrt(price_ratio) / (1 + price_ratio) - 1
        price_ratio = price_ratio_0 / price_ratio_1
        sqrt_ratio = Decimal(float(price_ratio) ** 0.5)
        
        il_percent = 2 * sqrt_ratio / (1 + price_ratio) - 1
        
        # Calculate IL in value terms
        position_value = await self._calculate_position_value(liquidity_position)
        il_value = abs(il_percent) * position_value
        
        return il_value
        
    async def process_claim(
        self,
        protection_position_id: str,
        liquidity_position: LiquidityPosition
    ) -> Dict:
        """Process an IL protection claim"""
        
        # Load protection position
        protection_position = await self.ignite.get(
            f'il_protection:{protection_position_id}'
        )
        
        if not protection_position:
            raise ValueError("Protection position not found")
            
        # Check if protection is still active
        if datetime.utcnow() > protection_position.expires_at:
            raise ValueError("Protection has expired")
            
        # Calculate actual IL
        current_prices = {
            'token0': await self.oracle.get_price('token0'),
            'token1': await self.oracle.get_price('token1')
        }
        
        actual_il = await self.calculate_impermanent_loss(
            liquidity_position,
            current_prices
        )
        
        # Calculate payout (capped by coverage)
        coverage_ratio = protection_position.insurance_coverage / \
                        await self._calculate_position_value(liquidity_position)
        payout = min(actual_il * coverage_ratio, protection_position.insurance_coverage)
        
        # Process payout
        if payout > 0:
            # Update insurance fund
            self.insurance_fund.claims_paid += payout
            self.insurance_fund.total_capital -= payout
            self.insurance_fund.active_coverage -= protection_position.insurance_coverage
            
            # Close hedge positions
            await self._close_hedge_positions(protection_position.hedge_positions)
            
            # Mark position as claimed
            protection_position.claimed = True
            protection_position.claim_amount = payout
            protection_position.claim_date = datetime.utcnow()
            
            await self.ignite.put(
                f'il_protection:{protection_position_id}',
                protection_position
            )
            
            # Emit event
            await self.pulsar.publish('il_protection.claimed', {
                'position_id': protection_position_id,
                'payout': str(payout),
                'actual_il': str(actual_il),
                'coverage_used': str(payout / protection_position.insurance_coverage)
            })
            
        return {
            'payout': payout,
            'actual_il': actual_il,
            'coverage_ratio': coverage_ratio,
            'claim_processed': True
        }
        
    async def rebalance_hedges(self):
        """Rebalance all active hedge positions"""
        
        # Get all active protection positions
        active_positions = await self._get_active_protection_positions()
        
        for protection_position in active_positions:
            try:
                # Load liquidity position
                liquidity_position = await self._load_liquidity_position(
                    protection_position.liquidity_position_id
                )
                
                # Calculate current IL
                current_prices = {
                    'token0': await self.oracle.get_price('token0'),
                    'token1': await self.oracle.get_price('token1')
                }
                
                current_il = await self.calculate_impermanent_loss(
                    liquidity_position,
                    current_prices
                )
                
                # Check if rebalance needed
                hedge_value = await self._calculate_hedge_value(
                    protection_position.hedge_positions
                )
                
                hedge_effectiveness = hedge_value / current_il if current_il > 0 else Decimal('1')
                
                if abs(1 - hedge_effectiveness) > self.hedge_rebalance_threshold:
                    # Rebalance hedge
                    new_hedge_positions = await self._rebalance_hedge(
                        protection_position,
                        liquidity_position,
                        current_il
                    )
                    
                    protection_position.hedge_positions = new_hedge_positions
                    
                    await self.ignite.put(
                        f'il_protection:{protection_position.position_id}',
                        protection_position
                    )
                    
                    logger.info(f"Rebalanced hedge for position {protection_position.position_id}")
                    
            except Exception as e:
                logger.error(f"Error rebalancing hedge: {e}")
                
    async def _calculate_premium(
        self,
        liquidity_position: LiquidityPosition,
        protection_period: timedelta,
        coverage_percent: Decimal
    ) -> Decimal:
        """Calculate protection premium based on risk factors"""
        
        # Get volatility of underlying assets
        vol_token0 = await self.oracle.get_volatility('token0')
        vol_token1 = await self.oracle.get_volatility('token1')
        correlation = await self.oracle.get_correlation('token0', 'token1')
        
        # Calculate expected IL based on volatility and correlation
        # Higher volatility and lower correlation = higher IL risk
        weeks = protection_period.days / 7
        
        # Simplified IL expectation model
        vol_spread = abs(vol_token0 - vol_token1)
        il_risk_factor = vol_spread * (1 - correlation) * Decimal(weeks ** 0.5)
        
        # Base premium calculation
        position_value = await self._calculate_position_value(liquidity_position)
        base_premium = position_value * self.base_premium_rate * Decimal(weeks)
        
        # Risk-adjusted premium
        risk_multiplier = 1 + il_risk_factor
        risk_adjusted_premium = base_premium * risk_multiplier * coverage_percent
        
        # Add fund reserve requirement
        reserve_charge = (position_value * coverage_percent) / \
                        self.insurance_fund.reserve_ratio * Decimal('0.01')
        
        total_premium = risk_adjusted_premium + reserve_charge
        
        return total_premium
        
    async def _create_initial_hedge(
        self,
        liquidity_position: LiquidityPosition,
        coverage_percent: Decimal
    ) -> List[Dict]:
        """Create initial hedge positions for IL protection"""
        
        hedge_positions = []
        
        if self.delta_hedge_enabled:
            # Create delta hedge using spot positions
            delta_hedge = await self._create_delta_hedge(
                liquidity_position,
                coverage_percent
            )
            hedge_positions.append(delta_hedge)
            
        if self.options_hedge_enabled:
            # Create options hedge (straddle or strangle)
            options_hedge = await self._create_options_hedge(
                liquidity_position,
                coverage_percent
            )
            hedge_positions.extend(options_hedge)
            
        return hedge_positions
        
    async def _create_delta_hedge(
        self,
        liquidity_position: LiquidityPosition,
        coverage_percent: Decimal
    ) -> Dict:
        """Create delta-neutral hedge using spot positions"""
        
        # Calculate position deltas
        # For concentrated liquidity, delta changes with price
        current_tick = liquidity_position.tick_lower + \
                      (liquidity_position.tick_upper - liquidity_position.tick_lower) // 2
                      
        # Simplified delta calculation
        delta_token0 = liquidity_position.token0_amount * coverage_percent
        delta_token1 = liquidity_position.token1_amount * coverage_percent
        
        return {
            'type': 'delta_hedge',
            'positions': {
                'token0': -delta_token0,  # Short to hedge long exposure
                'token1': -delta_token1
            },
            'created_at': datetime.utcnow()
        }
        
    async def _create_options_hedge(
        self,
        liquidity_position: LiquidityPosition,
        coverage_percent: Decimal
    ) -> List[Dict]:
        """Create options hedge using straddle/strangle"""
        
        # Get current prices
        price_token0 = await self.oracle.get_price('token0')
        price_token1 = await self.oracle.get_price('token1')
        
        # Calculate notional to hedge
        position_value = await self._calculate_position_value(liquidity_position)
        hedge_notional = position_value * coverage_percent
        
        # Create straddle on the more volatile asset
        vol_token0 = await self.oracle.get_volatility('token0')
        vol_token1 = await self.oracle.get_volatility('token1')
        
        primary_asset = 'token0' if vol_token0 > vol_token1 else 'token1'
        primary_price = price_token0 if primary_asset == 'token0' else price_token1
        
        # Buy ATM straddle
        expiry = datetime.utcnow() + timedelta(days=30)
        
        options_positions = [
            {
                'type': 'option',
                'asset': primary_asset,
                'option_type': 'CALL',
                'strike': primary_price,
                'expiry': expiry,
                'amount': hedge_notional / (2 * primary_price),
                'position': 'long'
            },
            {
                'type': 'option',
                'asset': primary_asset,
                'option_type': 'PUT',
                'strike': primary_price,
                'expiry': expiry,
                'amount': hedge_notional / (2 * primary_price),
                'position': 'long'
            }
        ]
        
        return options_positions
        
    async def _calculate_position_value(
        self,
        liquidity_position: LiquidityPosition
    ) -> Decimal:
        """Calculate current value of liquidity position"""
        
        price_token0 = await self.oracle.get_price('token0')
        price_token1 = await self.oracle.get_price('token1')
        
        value_token0 = liquidity_position.token0_amount * price_token0
        value_token1 = liquidity_position.token1_amount * price_token1
        
        return value_token0 + value_token1
        
    async def _check_fund_capacity(self, required_coverage: Decimal) -> bool:
        """Check if insurance fund has capacity for new coverage"""
        
        # Calculate maximum allowed coverage
        max_coverage = self.insurance_fund.total_capital * self.insurance_fund.reserve_ratio
        
        # Check if adding this coverage would exceed limit
        total_coverage = self.insurance_fund.active_coverage + required_coverage
        
        return total_coverage <= max_coverage
        
    async def _get_initial_prices(
        self,
        liquidity_position: LiquidityPosition
    ) -> Dict[str, Decimal]:
        """Get prices when liquidity position was created"""
        
        # Fetch from historical data
        historical_key = f'historical_prices:{liquidity_position.created_at.date()}'
        historical_prices = await self.ignite.get(historical_key)
        
        if historical_prices:
            return historical_prices
            
        # Fallback to current prices (not ideal)
        return {
            'token0': await self.oracle.get_price('token0'),
            'token1': await self.oracle.get_price('token1')
        }
        
    async def _get_active_protection_positions(self) -> List[ILProtectionPosition]:
        """Get all active protection positions"""
        
        # In production, this would query a database
        # For now, simplified implementation
        positions = []
        
        # Scan through cached positions
        prefix = 'il_protection:'
        keys = await self.ignite.get_keys(prefix)
        
        for key in keys:
            position = await self.ignite.get(key)
            if position and datetime.utcnow() < position.expires_at:
                if not hasattr(position, 'claimed') or not position.claimed:
                    positions.append(position)
                    
        return positions
        
    async def _calculate_hedge_value(self, hedge_positions: List[Dict]) -> Decimal:
        """Calculate current value of hedge positions"""
        
        total_value = Decimal('0')
        
        for hedge in hedge_positions:
            if hedge['type'] == 'delta_hedge':
                # Value spot hedges
                for asset, amount in hedge['positions'].items():
                    price = await self.oracle.get_price(asset)
                    total_value += amount * price
                    
            elif hedge['type'] == 'option':
                # Value options
                option_value = await self.pricing_engine.calculate_option_price(
                    asset=hedge['asset'],
                    strike=hedge['strike'],
                    expiry=hedge['expiry'],
                    option_type=hedge['option_type']
                )
                total_value += hedge['amount'] * option_value
                
        return abs(total_value)  # Return absolute value
        
    async def _load_liquidity_position(
        self,
        position_id: str
    ) -> LiquidityPosition:
        """Load liquidity position from storage"""
        
        position = await self.ignite.get(f'cl_position:{position_id}')
        if not position:
            raise ValueError(f"Liquidity position {position_id} not found")
        return position
        
    async def _rebalance_hedge(
        self,
        protection_position: ILProtectionPosition,
        liquidity_position: LiquidityPosition,
        current_il: Decimal
    ) -> List[Dict]:
        """Rebalance hedge positions to maintain effectiveness"""
        
        # Close existing hedges
        await self._close_hedge_positions(protection_position.hedge_positions)
        
        # Calculate new hedge size based on current IL
        coverage_ratio = protection_position.insurance_coverage / \
                        await self._calculate_position_value(liquidity_position)
        
        # Create new hedges sized to current IL
        new_hedge_positions = []
        
        if self.delta_hedge_enabled:
            # Adjust delta hedge
            delta_hedge = await self._create_delta_hedge(
                liquidity_position,
                coverage_ratio
            )
            new_hedge_positions.append(delta_hedge)
            
        if self.options_hedge_enabled:
            # Adjust options hedge
            options_hedge = await self._create_options_hedge(
                liquidity_position,
                coverage_ratio
            )
            new_hedge_positions.extend(options_hedge)
            
        return new_hedge_positions
        
    async def _close_hedge_positions(self, hedge_positions: List[Dict]):
        """Close existing hedge positions"""
        
        for hedge in hedge_positions:
            if hedge['type'] == 'delta_hedge':
                # Close spot positions
                # In production, would execute trades
                pass
            elif hedge['type'] == 'option':
                # Close option positions
                # In production, would execute trades
                pass
                
        logger.info(f"Closed {len(hedge_positions)} hedge positions") 