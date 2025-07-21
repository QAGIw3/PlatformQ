"""Volatility surface calculations for options."""

import numpy as np
from decimal import Decimal
from typing import Dict, List, Optional, Tuple
from scipy.interpolate import RectBivariateSpline, interp1d
from scipy.optimize import minimize_scalar
from datetime import datetime, timedelta
from dataclasses import dataclass
import logging

logger = logging.getLogger(__name__)


@dataclass
class VolatilitySurface:
    """Represents a volatility surface."""
    underlying_asset: str
    surface_data: Dict[str, Dict[str, float]]  # expiry -> strike -> IV
    at_the_money_vol: Decimal
    skew: Dict[str, float]  # expiry -> skew parameter
    term_structure: Dict[str, float]  # expiry -> ATM vol
    spot_price: Decimal
    updated_at: datetime
    
    def get_vol(self, strike: float, expiry: str) -> Optional[float]:
        """Get volatility for a specific strike and expiry."""
        if expiry in self.surface_data and str(strike) in self.surface_data[expiry]:
            return self.surface_data[expiry][str(strike)]
        return None


class VolatilitySurfaceEngine:
    """Engine for volatility surface construction and interpolation."""
    
    def __init__(self, min_moneyness: float = 0.5, max_moneyness: float = 2.0):
        """Initialize volatility surface engine.
        
        Args:
            min_moneyness: Minimum strike/spot ratio to consider
            max_moneyness: Maximum strike/spot ratio to consider
        """
        self.min_moneyness = min_moneyness
        self.max_moneyness = max_moneyness
        self._surfaces: Dict[str, VolatilitySurface] = {}
    
    def build_surface(
        self,
        underlying_asset: str,
        spot_price: Decimal,
        options_data: List[Dict[str, any]]
    ) -> VolatilitySurface:
        """Build volatility surface from market data.
        
        Args:
            underlying_asset: Underlying asset symbol
            spot_price: Current spot price
            options_data: List of dicts with keys: strike, expiry, implied_vol, option_type
            
        Returns:
            Constructed volatility surface
        """
        # Organize data by expiry and strike
        surface_data = {}
        
        for option in options_data:
            expiry = option['expiry']
            strike = float(option['strike'])
            iv = option['implied_vol']
            
            # Filter by moneyness
            moneyness = strike / float(spot_price)
            if moneyness < self.min_moneyness or moneyness > self.max_moneyness:
                continue
            
            if expiry not in surface_data:
                surface_data[expiry] = {}
            
            surface_data[expiry][str(strike)] = iv
        
        # Calculate ATM volatility and skew
        atm_vol = self._calculate_atm_volatility(surface_data, float(spot_price))
        skew = self._calculate_skew(surface_data, float(spot_price))
        term_structure = self._calculate_term_structure(surface_data, float(spot_price))
        
        surface = VolatilitySurface(
            underlying_asset=underlying_asset,
            surface_data=surface_data,
            at_the_money_vol=Decimal(str(atm_vol)),
            skew=skew,
            term_structure=term_structure,
            spot_price=spot_price,
            updated_at=datetime.utcnow()
        )
        
        # Cache the surface
        self._surfaces[underlying_asset] = surface
        
        return surface
    
    def interpolate_volatility(
        self,
        underlying_asset: str,
        strike: Decimal,
        expiry: datetime,
        spot_price: Optional[Decimal] = None
    ) -> Optional[Decimal]:
        """Interpolate implied volatility for any strike/expiry.
        
        Args:
            underlying_asset: Underlying asset symbol
            strike: Strike price
            expiry: Expiration date
            spot_price: Current spot price (uses cached if not provided)
            
        Returns:
            Interpolated implied volatility
        """
        if underlying_asset not in self._surfaces:
            logger.warning(f"No volatility surface found for {underlying_asset}")
            return None
        
        surface = self._surfaces[underlying_asset]
        
        if spot_price is None:
            spot_price = surface.spot_price
        
        # Convert expiry to days to expiration
        dte = (expiry - datetime.utcnow()).days
        if dte <= 0:
            return None
        
        # Use SABR or SVI model for interpolation
        return self._sabr_interpolation(
            surface,
            float(strike),
            dte,
            float(spot_price)
        )
    
    def _calculate_atm_volatility(
        self,
        surface_data: Dict[str, Dict[str, float]],
        spot_price: float
    ) -> float:
        """Calculate at-the-money volatility."""
        atm_vols = []
        
        for expiry, strikes in surface_data.items():
            # Find closest strike to spot
            strike_prices = [float(k) for k in strikes.keys()]
            closest_strike = min(strike_prices, key=lambda x: abs(x - spot_price))
            
            if abs(closest_strike - spot_price) / spot_price < 0.1:  # Within 10%
                atm_vols.append(strikes[str(closest_strike)])
        
        return np.mean(atm_vols) if atm_vols else 0.2  # Default 20% vol
    
    def _calculate_skew(
        self,
        surface_data: Dict[str, Dict[str, float]],
        spot_price: float
    ) -> Dict[str, float]:
        """Calculate volatility skew for each expiry."""
        skew = {}
        
        for expiry, strikes in surface_data.items():
            strike_prices = sorted([float(k) for k in strikes.keys()])
            vols = [strikes[str(k)] for k in strike_prices]
            
            if len(strike_prices) >= 3:
                # Calculate 90-110 skew
                k_90 = spot_price * 0.9
                k_110 = spot_price * 1.1
                
                # Interpolate volatilities
                f = interp1d(strike_prices, vols, kind='linear', fill_value='extrapolate')
                vol_90 = float(f(k_90))
                vol_110 = float(f(k_110))
                
                skew[expiry] = vol_90 - vol_110
            else:
                skew[expiry] = 0.0
        
        return skew
    
    def _calculate_term_structure(
        self,
        surface_data: Dict[str, Dict[str, float]],
        spot_price: float
    ) -> Dict[str, float]:
        """Calculate term structure of ATM volatility."""
        term_structure = {}
        
        for expiry, strikes in surface_data.items():
            # Find ATM vol for this expiry
            strike_prices = [float(k) for k in strikes.keys()]
            closest_strike = min(strike_prices, key=lambda x: abs(x - spot_price))
            
            if abs(closest_strike - spot_price) / spot_price < 0.1:
                term_structure[expiry] = strikes[str(closest_strike)]
        
        return term_structure
    
    def _sabr_interpolation(
        self,
        surface: VolatilitySurface,
        strike: float,
        dte: int,
        spot: float
    ) -> Optional[Decimal]:
        """SABR model interpolation for implied volatility.
        
        Simplified version - in production would use full SABR calibration.
        """
        # Find surrounding expiries
        expiries = sorted(surface.surface_data.keys())
        expiry_days = [(datetime.strptime(e, "%Y-%m-%d") - datetime.utcnow()).days 
                      for e in expiries]
        
        if not expiry_days or dte < expiry_days[0] or dte > expiry_days[-1]:
            # Extrapolation - use closest expiry
            if not expiry_days:
                return None
            closest_expiry = expiries[0] if dte < expiry_days[0] else expiries[-1]
            return self._interpolate_strike(surface, closest_expiry, strike, spot)
        
        # Find bracketing expiries
        lower_idx = 0
        for i, days in enumerate(expiry_days):
            if days <= dte:
                lower_idx = i
            else:
                break
        
        upper_idx = min(lower_idx + 1, len(expiries) - 1)
        
        # Interpolate for each expiry
        vol_lower = self._interpolate_strike(
            surface, expiries[lower_idx], strike, spot
        )
        vol_upper = self._interpolate_strike(
            surface, expiries[upper_idx], strike, spot
        )
        
        if vol_lower is None or vol_upper is None:
            return None
        
        # Time interpolation
        if expiry_days[lower_idx] == expiry_days[upper_idx]:
            return vol_lower
        
        w = (dte - expiry_days[lower_idx]) / (expiry_days[upper_idx] - expiry_days[lower_idx])
        vol = float(vol_lower) * (1 - w) + float(vol_upper) * w
        
        return Decimal(str(vol))
    
    def _interpolate_strike(
        self,
        surface: VolatilitySurface,
        expiry: str,
        strike: float,
        spot: float
    ) -> Optional[Decimal]:
        """Interpolate volatility across strikes for a given expiry."""
        if expiry not in surface.surface_data:
            return None
        
        strikes_data = surface.surface_data[expiry]
        strikes = sorted([float(k) for k in strikes_data.keys()])
        vols = [strikes_data[str(k)] for k in strikes]
        
        if not strikes:
            return None
        
        # Check if strike is in range
        if strike <= strikes[0]:
            # Extrapolate using volatility smile
            return self._extrapolate_low_strike(strikes, vols, strike, spot)
        elif strike >= strikes[-1]:
            # Extrapolate using volatility smile
            return self._extrapolate_high_strike(strikes, vols, strike, spot)
        else:
            # Interpolate
            f = interp1d(strikes, vols, kind='cubic')
            return Decimal(str(float(f(strike))))
    
    def _extrapolate_low_strike(
        self,
        strikes: List[float],
        vols: List[float],
        target_strike: float,
        spot: float
    ) -> Decimal:
        """Extrapolate volatility for low strikes using smile dynamics."""
        # Use power law extrapolation
        k1, k2 = strikes[0], strikes[1]
        v1, v2 = vols[0], vols[1]
        
        # Calculate local slope
        slope = (v2 - v1) / np.log(k2 / k1)
        
        # Extrapolate
        vol = v1 + slope * np.log(target_strike / k1)
        
        # Apply floor to prevent negative vols
        return Decimal(str(max(vol, 0.01)))
    
    def _extrapolate_high_strike(
        self,
        strikes: List[float],
        vols: List[float],
        target_strike: float,
        spot: float
    ) -> Decimal:
        """Extrapolate volatility for high strikes using smile dynamics."""
        # Use power law extrapolation
        k1, k2 = strikes[-2], strikes[-1]
        v1, v2 = vols[-2], vols[-1]
        
        # Calculate local slope
        slope = (v2 - v1) / np.log(k2 / k1)
        
        # Extrapolate with dampening for extreme strikes
        dampening = 0.5  # Reduce slope for far strikes
        vol = v2 + slope * dampening * np.log(target_strike / k2)
        
        # Apply floor
        return Decimal(str(max(vol, 0.01)))
    
    def get_surface(self, underlying_asset: str) -> Optional[VolatilitySurface]:
        """Get cached volatility surface."""
        return self._surfaces.get(underlying_asset)
    
    def calculate_forward_volatility(
        self,
        underlying_asset: str,
        t1: datetime,
        t2: datetime,
        strike: Decimal
    ) -> Optional[Decimal]:
        """Calculate forward implied volatility between two dates.
        
        Args:
            underlying_asset: Underlying asset symbol
            t1: Start date
            t2: End date (t2 > t1)
            strike: Strike price
            
        Returns:
            Forward implied volatility
        """
        if t2 <= t1:
            raise ValueError("End date must be after start date")
        
        # Get spot vols
        vol1 = self.interpolate_volatility(underlying_asset, strike, t1)
        vol2 = self.interpolate_volatility(underlying_asset, strike, t2)
        
        if vol1 is None or vol2 is None:
            return None
        
        # Calculate forward vol using variance
        T1 = (t1 - datetime.utcnow()).days / 365.0
        T2 = (t2 - datetime.utcnow()).days / 365.0
        
        if T1 <= 0:
            return vol2
        
        var1 = float(vol1) ** 2 * T1
        var2 = float(vol2) ** 2 * T2
        
        forward_var = (var2 - var1) / (T2 - T1)
        
        if forward_var <= 0:
            return vol2  # Fallback to spot vol
        
        return Decimal(str(np.sqrt(forward_var))) 