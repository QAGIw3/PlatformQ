"""
Variance Swap Engine

Implements variance and volatility swaps for advanced volatility trading.
"""

import asyncio
from typing import Dict, List, Optional, Tuple, Any
from decimal import Decimal
from datetime import datetime, timedelta
from dataclasses import dataclass, field
import numpy as np
from collections import defaultdict
import logging

from app.integrations import IgniteCache, PulsarEventPublisher, OracleAggregatorClient
from app.models.market import Market

logger = logging.getLogger(__name__)


@dataclass
class VarianceSwap:
    """Variance swap contract"""
    swap_id: str
    underlying: str
    notional: Decimal  # Vega notional
    strike_volatility: Decimal  # Annualized vol
    strike_variance: Decimal  # Strike^2
    start_date: datetime
    maturity_date: datetime
    observation_frequency: str  # "daily", "hourly", "tick"
    
    # Realized variance tracking
    realized_variance: Decimal = Decimal("0")
    observations: List[Tuple[datetime, Decimal]] = field(default_factory=list)
    num_observations: int = 0
    
    # Parties
    buyer_id: str = ""  # Long volatility
    seller_id: str = ""  # Short volatility
    
    # Status
    is_active: bool = True
    final_settlement: Optional[Decimal] = None
    

@dataclass
class VolatilitySwap:
    """Volatility swap (square root of variance swap)"""
    swap_id: str
    underlying: str
    notional: Decimal  # Direct vol notional
    strike_volatility: Decimal
    start_date: datetime
    maturity_date: datetime
    
    # Convexity adjustment
    convexity_adjustment: Decimal = Decimal("0")
    
    # Link to variance swap
    variance_swap_id: Optional[str] = None


class VarianceSwapEngine:
    """
    Engine for variance and volatility swaps
    """
    
    def __init__(
        self,
        ignite: IgniteCache,
        pulsar: PulsarEventPublisher,
        oracle: OracleAggregatorClient
    ):
        self.ignite = ignite
        self.pulsar = pulsar
        self.oracle = oracle
        
        # Active swaps
        self.variance_swaps: Dict[str, VarianceSwap] = {}
        self.volatility_swaps: Dict[str, VolatilitySwap] = {}
        
        # Market data
        self.price_history: Dict[str, List[Tuple[datetime, Decimal]]] = defaultdict(list)
        self.implied_vols: Dict[str, Decimal] = {}
        
        # Risk limits
        self.max_notional_per_swap = Decimal("10000000")  # $10M
        self.max_total_exposure = Decimal("100000000")  # $100M
        
    async def start(self):
        """Start the variance swap engine"""
        asyncio.create_task(self._observation_loop())
        asyncio.create_task(self._settlement_loop())
        logger.info("Variance swap engine started")
        
    async def create_variance_swap(
        self,
        buyer_id: str,
        seller_id: str,
        underlying: str,
        notional: Decimal,
        strike_volatility: Decimal,
        maturity_days: int,
        observation_frequency: str = "daily"
    ) -> VarianceSwap:
        """Create a new variance swap"""
        # Validate inputs
        if notional > self.max_notional_per_swap:
            raise ValueError(f"Notional exceeds limit: {self.max_notional_per_swap}")
            
        # Check total exposure
        total_exposure = await self._calculate_total_exposure()
        if total_exposure + notional > self.max_total_exposure:
            raise ValueError("Total exposure limit exceeded")
            
        # Create swap
        swap = VarianceSwap(
            swap_id=f"var_{underlying}_{datetime.utcnow().timestamp()}",
            underlying=underlying,
            notional=notional,
            strike_volatility=strike_volatility,
            strike_variance=strike_volatility ** 2,
            start_date=datetime.utcnow(),
            maturity_date=datetime.utcnow() + timedelta(days=maturity_days),
            observation_frequency=observation_frequency,
            buyer_id=buyer_id,
            seller_id=seller_id
        )
        
        self.variance_swaps[swap.swap_id] = swap
        
        # Initial collateral requirements
        initial_margin = await self._calculate_initial_margin(swap)
        
        # Emit creation event
        await self.pulsar.publish(
            "variance-swap-created",
            {
                "swap_id": swap.swap_id,
                "underlying": underlying,
                "notional": str(notional),
                "strike_vol": str(strike_volatility),
                "maturity": swap.maturity_date.isoformat(),
                "buyer_margin": str(initial_margin["buyer"]),
                "seller_margin": str(initial_margin["seller"])
            }
        )
        
        return swap
        
    async def create_volatility_swap(
        self,
        buyer_id: str,
        seller_id: str,
        underlying: str,
        notional: Decimal,
        strike_volatility: Decimal,
        maturity_days: int
    ) -> VolatilitySwap:
        """Create a volatility swap with convexity adjustment"""
        # First create underlying variance swap
        # Vol swap notional needs adjustment for convexity
        var_notional = notional * strike_volatility * 2
        
        var_swap = await self.create_variance_swap(
            buyer_id=buyer_id,
            seller_id=seller_id,
            underlying=underlying,
            notional=var_notional,
            strike_volatility=strike_volatility,
            maturity_days=maturity_days
        )
        
        # Calculate convexity adjustment
        convexity_adj = await self._calculate_convexity_adjustment(
            underlying,
            strike_volatility,
            maturity_days
        )
        
        # Create vol swap
        vol_swap = VolatilitySwap(
            swap_id=f"vol_{underlying}_{datetime.utcnow().timestamp()}",
            underlying=underlying,
            notional=notional,
            strike_volatility=strike_volatility,
            start_date=datetime.utcnow(),
            maturity_date=datetime.utcnow() + timedelta(days=maturity_days),
            convexity_adjustment=convexity_adj,
            variance_swap_id=var_swap.swap_id
        )
        
        self.volatility_swaps[vol_swap.swap_id] = vol_swap
        
        return vol_swap
        
    async def _observation_loop(self):
        """Continuously observe and update realized variance"""
        while True:
            try:
                for swap_id, swap in self.variance_swaps.items():
                    if swap.is_active:
                        await self._update_realized_variance(swap)
                        
            except Exception as e:
                logger.error(f"Error in observation loop: {e}")
                
            # Frequency depends on observation requirements
            await asyncio.sleep(60)  # Check every minute
            
    async def _update_realized_variance(self, swap: VarianceSwap):
        """Update realized variance for a swap"""
        # Get latest price
        current_price = await self.oracle.get_price(swap.underlying)
        if not current_price:
            return
            
        current_time = datetime.utcnow()
        
        # Check if we need a new observation
        if self._should_observe(swap, current_time):
            # Add observation
            swap.observations.append((current_time, current_price))
            swap.num_observations += 1
            
            # Calculate realized variance
            if len(swap.observations) >= 2:
                returns = []
                for i in range(1, len(swap.observations)):
                    prev_price = swap.observations[i-1][1]
                    curr_price = swap.observations[i][1]
                    log_return = np.log(float(curr_price / prev_price))
                    returns.append(log_return)
                    
                # Annualized realized variance
                if swap.observation_frequency == "daily":
                    annualization_factor = 252
                elif swap.observation_frequency == "hourly":
                    annualization_factor = 252 * 24
                else:  # tick
                    annualization_factor = 252 * 24 * 60 * 60
                    
                realized_var = np.var(returns) * annualization_factor
                swap.realized_variance = Decimal(str(realized_var))
                
                # Calculate current P&L
                current_pnl = await self._calculate_swap_pnl(swap)
                
                # Emit update event
                await self.pulsar.publish(
                    "variance-swap-update",
                    {
                        "swap_id": swap.swap_id,
                        "realized_variance": str(swap.realized_variance),
                        "realized_volatility": str(np.sqrt(float(swap.realized_variance))),
                        "strike_variance": str(swap.strike_variance),
                        "current_pnl": str(current_pnl),
                        "observations": swap.num_observations
                    }
                )
                
    def _should_observe(self, swap: VarianceSwap, current_time: datetime) -> bool:
        """Check if we should take a new observation"""
        if not swap.observations:
            return True
            
        last_observation_time = swap.observations[-1][0]
        
        if swap.observation_frequency == "daily":
            # Observe at same time each day
            return (current_time - last_observation_time) >= timedelta(days=1)
        elif swap.observation_frequency == "hourly":
            return (current_time - last_observation_time) >= timedelta(hours=1)
        else:  # tick - every price update
            return True
            
    async def _calculate_swap_pnl(self, swap: VarianceSwap) -> Decimal:
        """Calculate current P&L for variance swap"""
        # P&L = Notional * (Realized Variance - Strike Variance)
        variance_diff = swap.realized_variance - swap.strike_variance
        
        # Convert to dollar P&L
        # Variance is in annual vol squared terms
        # Notional is in vega terms ($ per vol point)
        # So we need to scale appropriately
        pnl = swap.notional * variance_diff / (2 * swap.strike_volatility)
        
        return pnl
        
    async def _settlement_loop(self):
        """Check for swaps that need settlement"""
        while True:
            try:
                current_time = datetime.utcnow()
                
                for swap_id, swap in list(self.variance_swaps.items()):
                    if swap.is_active and current_time >= swap.maturity_date:
                        await self._settle_variance_swap(swap)
                        
            except Exception as e:
                logger.error(f"Error in settlement loop: {e}")
                
            await asyncio.sleep(3600)  # Check hourly
            
    async def _settle_variance_swap(self, swap: VarianceSwap):
        """Settle a matured variance swap"""
        # Calculate final realized variance
        if swap.observation_frequency == "daily":
            expected_observations = (swap.maturity_date - swap.start_date).days
        elif swap.observation_frequency == "hourly":
            expected_observations = int((swap.maturity_date - swap.start_date).total_seconds() / 3600)
        else:
            expected_observations = swap.num_observations  # Use actual for tick data
            
        # Adjust for missing observations (weekends, holidays)
        observation_ratio = swap.num_observations / expected_observations
        
        # Final settlement
        final_pnl = await self._calculate_swap_pnl(swap)
        
        # Buyer receives if realized > strike
        if final_pnl > 0:
            winner = swap.buyer_id
            loser = swap.seller_id
        else:
            winner = swap.seller_id
            loser = swap.buyer_id
            final_pnl = -final_pnl
            
        swap.is_active = False
        swap.final_settlement = final_pnl
        
        # Emit settlement event
        await self.pulsar.publish(
            "variance-swap-settled",
            {
                "swap_id": swap.swap_id,
                "realized_variance": str(swap.realized_variance),
                "realized_volatility": str(np.sqrt(float(swap.realized_variance))),
                "strike_variance": str(swap.strike_variance),
                "strike_volatility": str(swap.strike_volatility),
                "settlement_amount": str(final_pnl),
                "winner": winner,
                "loser": loser,
                "observations": swap.num_observations,
                "observation_ratio": float(observation_ratio)
            }
        )
        
        # Process settlement transfers
        await self._process_settlement_transfer(winner, loser, final_pnl)
        
        # Check and settle related vol swap
        for vol_swap_id, vol_swap in self.volatility_swaps.items():
            if vol_swap.variance_swap_id == swap.swap_id:
                await self._settle_volatility_swap(vol_swap, swap)
                
    async def _settle_volatility_swap(self, vol_swap: VolatilitySwap, var_swap: VarianceSwap):
        """Settle a volatility swap based on variance swap"""
        # Vol swap P&L = Notional * (Realized Vol - Strike Vol - Convexity Adjustment)
        realized_vol = Decimal(str(np.sqrt(float(var_swap.realized_variance))))
        
        vol_diff = realized_vol - vol_swap.strike_volatility - vol_swap.convexity_adjustment
        final_pnl = vol_swap.notional * vol_diff
        
        # Emit settlement
        await self.pulsar.publish(
            "volatility-swap-settled",
            {
                "swap_id": vol_swap.swap_id,
                "realized_volatility": str(realized_vol),
                "strike_volatility": str(vol_swap.strike_volatility),
                "convexity_adjustment": str(vol_swap.convexity_adjustment),
                "settlement_amount": str(final_pnl)
            }
        )
        
    async def _calculate_initial_margin(self, swap: VarianceSwap) -> Dict[str, Decimal]:
        """Calculate initial margin requirements"""
        # Use historical vol to estimate potential moves
        hist_vol = await self._get_historical_volatility(swap.underlying, 90)
        
        # Stress scenario: vol doubles
        stressed_vol = hist_vol * 2
        stressed_variance = stressed_vol ** 2
        
        # Maximum potential loss
        max_loss = swap.notional * abs(stressed_variance - swap.strike_variance) / (2 * swap.strike_volatility)
        
        # Margin is percentage of max loss
        # Seller needs more margin (unlimited risk)
        buyer_margin = max_loss * Decimal("0.2")  # 20%
        seller_margin = max_loss * Decimal("0.5")  # 50%
        
        return {
            "buyer": buyer_margin,
            "seller": seller_margin
        }
        
    async def _calculate_convexity_adjustment(
        self,
        underlying: str,
        strike_vol: Decimal,
        maturity_days: int
    ) -> Decimal:
        """Calculate convexity adjustment for vol swap"""
        # Simplified approach using historical vol of vol
        hist_vol = await self._get_historical_volatility(underlying, 90)
        vol_of_vol = await self._get_vol_of_vol(underlying, 90)
        
        # Convexity adjustment ≈ -0.5 * vol_of_vol^2 * T
        time_to_maturity = Decimal(str(maturity_days / 365))
        adjustment = Decimal("-0.5") * vol_of_vol ** 2 * time_to_maturity * hist_vol
        
        return adjustment
        
    async def _get_historical_volatility(self, underlying: str, lookback_days: int) -> Decimal:
        """Calculate historical volatility"""
        # Get price history
        end_date = datetime.utcnow()
        start_date = end_date - timedelta(days=lookback_days)
        
        # In practice, would query historical prices
        # For now, return a reasonable estimate
        if underlying.startswith("BTC"):
            return Decimal("0.7")  # 70% vol
        elif underlying.startswith("ETH"):
            return Decimal("0.8")  # 80% vol
        else:
            return Decimal("0.5")  # 50% vol
            
    async def _get_vol_of_vol(self, underlying: str, lookback_days: int) -> Decimal:
        """Calculate volatility of volatility"""
        # In practice, would calculate from rolling window vols
        # For now, use typical values
        base_vol = await self._get_historical_volatility(underlying, lookback_days)
        return base_vol * Decimal("0.5")  # Vol of vol typically 50% of vol
        
    async def _calculate_total_exposure(self) -> Decimal:
        """Calculate total notional exposure"""
        total = Decimal("0")
        
        for swap in self.variance_swaps.values():
            if swap.is_active:
                total += swap.notional
                
        return total
        
    async def _process_settlement_transfer(
        self,
        winner: str,
        loser: str,
        amount: Decimal
    ):
        """Process settlement payment"""
        # In practice, would integrate with settlement engine
        logger.info(f"Settlement: {loser} pays {amount} to {winner}")
        
    async def get_market_overview(self) -> Dict[str, Any]:
        """Get overview of variance swap market"""
        active_swaps = [s for s in self.variance_swaps.values() if s.is_active]
        
        total_notional = sum(s.notional for s in active_swaps)
        avg_strike_vol = sum(s.strike_volatility for s in active_swaps) / len(active_swaps) if active_swaps else Decimal("0")
        avg_realized_vol = sum(Decimal(str(np.sqrt(float(s.realized_variance)))) for s in active_swaps) / len(active_swaps) if active_swaps else Decimal("0")
        
        return {
            "active_swaps": len(active_swaps),
            "total_notional": str(total_notional),
            "average_strike_vol": f"{avg_strike_vol:.2%}",
            "average_realized_vol": f"{avg_realized_vol:.2%}",
            "vol_premium": f"{(avg_strike_vol - avg_realized_vol):.2%}"
        }
        
    async def get_swap_details(self, swap_id: str) -> Optional[Dict[str, Any]]:
        """Get detailed information about a swap"""
        swap = self.variance_swaps.get(swap_id)
        if not swap:
            return None
            
        current_pnl = await self._calculate_swap_pnl(swap)
        time_to_maturity = (swap.maturity_date - datetime.utcnow()).days
        
        return {
            "swap_id": swap.swap_id,
            "underlying": swap.underlying,
            "notional": str(swap.notional),
            "strike_volatility": f"{swap.strike_volatility:.2%}",
            "realized_volatility": f"{np.sqrt(float(swap.realized_variance)):.2%}",
            "current_pnl": str(current_pnl),
            "observations": swap.num_observations,
            "time_to_maturity": time_to_maturity,
            "is_active": swap.is_active
        } 