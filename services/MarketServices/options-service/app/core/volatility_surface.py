"""Volatility surface engine for options pricing."""

import asyncio
from datetime import datetime, timedelta
from decimal import Decimal
from typing import Dict, List, Optional, Tuple
import numpy as np
from scipy import interpolate
from scipy.optimize import minimize_scalar
import logging

from app.config import Settings
from app.models.options import (
    VolatilitySurface, OptionContract, OptionType,
    OptionPricing
)
from app.cache.ignite_manager import OptionsCacheManager
from platformq_trading_common import BlackScholesEngine


logger = logging.getLogger(__name__)


class VolatilitySurfaceEngine:
    """Manages volatility surface construction and implied volatility calculations."""
    
    def __init__(self, settings: Settings, cache_manager: OptionsCacheManager):
        self.settings = settings
        self.cache = cache_manager
        self.bs_engine = BlackScholesEngine()
        self._running = False
        self._update_tasks: Dict[str, asyncio.Task] = {}
        
    async def start(self):
        """Start the volatility surface engine."""
        self._running = True
        logger.info("Volatility surface engine started")
        
    async def stop(self):
        """Stop the volatility surface engine."""
        self._running = False
        
        # Cancel all update tasks
        for task in self._update_tasks.values():
            task.cancel()
            
        if self._update_tasks:
            await asyncio.gather(*self._update_tasks.values(), return_exceptions=True)
            
        logger.info("Volatility surface engine stopped")
        
    async def start_surface_updates(self, underlying_asset: str):
        """Start periodic volatility surface updates for an underlying asset."""
        if underlying_asset in self._update_tasks:
            logger.warning(f"Surface updates already running for {underlying_asset}")
            return
            
        task = asyncio.create_task(self._update_surface_loop(underlying_asset))
        self._update_tasks[underlying_asset] = task
        logger.info(f"Started volatility surface updates for {underlying_asset}")
        
    async def _update_surface_loop(self, underlying_asset: str):
        """Periodically update volatility surface."""
        while self._running:
            try:
                await self.update_volatility_surface(underlying_asset)
                await asyncio.sleep(self.settings.vol_surface_update_interval)
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Error updating surface for {underlying_asset}: {e}")
                await asyncio.sleep(60)
                
    async def update_volatility_surface(self, underlying_asset: str):
        """Update volatility surface for an underlying asset."""
        try:
            # Get all active options for the underlying
            options = await self.cache.get_options_by_underlying(underlying_asset)
            
            if not options:
                logger.warning(f"No options found for {underlying_asset}")
                return
                
            # Get current spot price
            spot_price = await self._get_spot_price(underlying_asset)
            if not spot_price:
                logger.error(f"No spot price for {underlying_asset}")
                return
                
            # Calculate implied volatilities
            surface_data = {}
            expiries = set()
            
            for option in options:
                # Get market price
                market_price = await self._get_option_market_price(option.symbol)
                if not market_price:
                    continue
                    
                # Calculate implied volatility
                iv = await self.calculate_implied_volatility(
                    option, spot_price, market_price
                )
                
                if iv:
                    expiry_key = option.expiry_date.strftime("%Y-%m-%d")
                    strike_key = str(option.strike_price)
                    
                    if expiry_key not in surface_data:
                        surface_data[expiry_key] = {}
                        
                    surface_data[expiry_key][strike_key] = float(iv)
                    expiries.add(option.expiry_date)
                    
            # Calculate surface parameters
            atm_vol = await self._calculate_atm_volatility(
                surface_data, spot_price
            )
            
            skew = await self._calculate_skew(surface_data, spot_price)
            term_structure = await self._calculate_term_structure(
                surface_data, spot_price
            )
            
            # Create surface object
            surface = VolatilitySurface(
                underlying_asset=underlying_asset,
                surface_data=surface_data,
                at_the_money_vol=atm_vol,
                skew=skew,
                term_structure=term_structure
            )
            
            # Store surface
            await self.cache.store_volatility_surface(underlying_asset, surface)
            
            logger.info(
                f"Updated volatility surface for {underlying_asset}: "
                f"ATM vol={atm_vol}, expiries={len(surface_data)}"
            )
            
        except Exception as e:
            logger.error(f"Error updating volatility surface: {e}")
            
    async def calculate_implied_volatility(
        self,
        option: OptionContract,
        spot_price: Decimal,
        market_price: Decimal
    ) -> Optional[Decimal]:
        """Calculate implied volatility from market price."""
        try:
            time_to_expiry = option.time_to_expiry()
            if time_to_expiry <= 0:
                return None
                
            # Convert to float for calculations
            S = float(spot_price)
            K = float(option.strike_price)
            T = time_to_expiry
            r = self.settings.risk_free_rate
            market = float(market_price)
            is_call = option.option_type == OptionType.CALL
            
            # Objective function to minimize
            def objective(sigma):
                try:
                    if is_call:
                        theoretical = self.bs_engine.calculate_call_price(
                            S, K, T, r, sigma
                        )
                    else:
                        theoretical = self.bs_engine.calculate_put_price(
                            S, K, T, r, sigma
                        )
                    return abs(theoretical - market)
                except:
                    return float('inf')
                    
            # Find implied volatility
            result = minimize_scalar(
                objective,
                bounds=(0.01, 5.0),
                method='bounded',
                options={
                    'maxiter': self.settings.implied_vol_iterations,
                    'xatol': self.settings.implied_vol_tolerance
                }
            )
            
            if result.success and result.fun < market * 0.01:  # Within 1% of market
                return Decimal(str(result.x))
                
            return None
            
        except Exception as e:
            logger.error(f"Error calculating IV for {option.symbol}: {e}")
            return None
            
    async def get_interpolated_volatility(
        self,
        underlying_asset: str,
        strike: Decimal,
        expiry: datetime
    ) -> Optional[Decimal]:
        """Get interpolated volatility from surface."""
        try:
            surface = await self.cache.get_volatility_surface(underlying_asset)
            if not surface:
                return None
                
            # Find nearest expiries
            expiry_key = expiry.strftime("%Y-%m-%d")
            
            if expiry_key in surface.surface_data:
                # Interpolate strikes for exact expiry
                strike_data = surface.surface_data[expiry_key]
                strikes = sorted([float(k) for k in strike_data.keys()])
                vols = [strike_data[str(k)] for k in strikes]
                
                if len(strikes) >= 2:
                    # Linear interpolation
                    f = interpolate.interp1d(
                        strikes, vols,
                        kind='linear',
                        fill_value='extrapolate'
                    )
                    return Decimal(str(f(float(strike))))
                    
            # Need to interpolate both strike and time
            # Simplified: use ATM vol with adjustments
            base_vol = surface.at_the_money_vol
            
            # Apply skew adjustment
            moneyness = float(strike) / float(await self._get_spot_price(underlying_asset))
            skew_adj = 0.1 * (1 - moneyness)  # Simple skew model
            
            return base_vol + Decimal(str(skew_adj))
            
        except Exception as e:
            logger.error(f"Error interpolating volatility: {e}")
            return None
            
    async def _get_spot_price(self, underlying_asset: str) -> Optional[Decimal]:
        """Get spot price for underlying asset."""
        # In production, get from market data service
        price = await self.cache.get_underlying_price(underlying_asset)
        return price
        
    async def _get_option_market_price(self, symbol: str) -> Optional[Decimal]:
        """Get market price for option."""
        stats = await self.cache.get_option_market_stats(symbol)
        if stats and stats.last_price:
            return stats.last_price
        elif stats and stats.bid_price and stats.ask_price:
            return (stats.bid_price + stats.ask_price) / 2
        return None
        
    async def _calculate_atm_volatility(
        self,
        surface_data: Dict[str, Dict[str, float]],
        spot_price: Decimal
    ) -> Decimal:
        """Calculate at-the-money volatility."""
        atm_vols = []
        spot = float(spot_price)
        
        for expiry, strikes in surface_data.items():
            # Find closest strike to spot
            strike_list = [(float(k), v) for k, v in strikes.items()]
            if strike_list:
                closest = min(strike_list, key=lambda x: abs(x[0] - spot))
                atm_vols.append(closest[1])
                
        if atm_vols:
            return Decimal(str(np.mean(atm_vols)))
        return Decimal("0.5")  # Default 50% vol
        
    async def _calculate_skew(
        self,
        surface_data: Dict[str, Dict[str, float]],
        spot_price: Decimal
    ) -> Dict[str, float]:
        """Calculate volatility skew for each expiry."""
        skew = {}
        spot = float(spot_price)
        
        for expiry, strikes in surface_data.items():
            strike_list = sorted([(float(k), v) for k, v in strikes.items()])
            
            if len(strike_list) >= 3:
                # Calculate 25-delta put vs 25-delta call skew
                otm_put_strikes = [s for s, _ in strike_list if s < spot * 0.9]
                otm_call_strikes = [s for s, _ in strike_list if s > spot * 1.1]
                
                if otm_put_strikes and otm_call_strikes:
                    put_vol = np.mean([v for s, v in strike_list if s in otm_put_strikes])
                    call_vol = np.mean([v for s, v in strike_list if s in otm_call_strikes])
                    skew[expiry] = put_vol - call_vol
                else:
                    skew[expiry] = 0.0
            else:
                skew[expiry] = 0.0
                
        return skew
        
    async def _calculate_term_structure(
        self,
        surface_data: Dict[str, Dict[str, float]],
        spot_price: Decimal
    ) -> Dict[str, float]:
        """Calculate term structure of ATM volatility."""
        term_structure = {}
        spot = float(spot_price)
        
        for expiry, strikes in surface_data.items():
            # Find ATM vol for this expiry
            strike_list = [(float(k), v) for k, v in strikes.items()]
            if strike_list:
                closest = min(strike_list, key=lambda x: abs(x[0] - spot))
                term_structure[expiry] = closest[1]
                
        return term_structure 