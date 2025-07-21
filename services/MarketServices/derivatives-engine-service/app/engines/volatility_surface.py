"""
Volatility Surface Engine with SABR Model and Real-time Updates

Provides sophisticated volatility modeling for compute options pricing
"""

from decimal import Decimal, getcontext
from typing import Dict, List, Optional, Tuple, Any
from dataclasses import dataclass, field
from datetime import datetime, timedelta
import numpy as np
import pandas as pd
from scipy import optimize, interpolate
from scipy.stats import norm
import logging
import asyncio
from collections import defaultdict
import json

# Use QuantLib for advanced volatility modeling if available
try:
    import QuantLib as ql
    HAS_QUANTLIB = True
except ImportError:
    HAS_QUANTLIB = False
    
# Set high precision
getcontext().prec = 28

logger = logging.getLogger(__name__)


@dataclass
class VolatilityPoint:
    """Single volatility observation"""
    strike: Decimal
    expiry: datetime
    implied_vol: Decimal
    spot_price: Decimal
    option_price: Decimal
    timestamp: datetime
    volume: Decimal = Decimal("0")
    is_call: bool = True
    bid_vol: Optional[Decimal] = None
    ask_vol: Optional[Decimal] = None
    confidence: Decimal = Decimal("1.0")  # 0-1 confidence score


@dataclass 
class SABRParameters:
    """SABR model parameters for a given expiry"""
    alpha: float  # Initial volatility
    beta: float   # CEV exponent (typically 0.5 for equities)
    rho: float    # Correlation between asset and volatility
    nu: float     # Volatility of volatility
    forward: float
    expiry: float
    calibration_error: float = 0.0
    

@dataclass
class VolatilitySurfaceData:
    """Complete volatility surface data"""
    underlying: str
    timestamp: datetime
    spot_price: Decimal
    risk_free_rate: Decimal
    dividend_yield: Decimal = Decimal("0")
    
    # Strike-expiry grid
    strikes: List[Decimal] = field(default_factory=list)
    expiries: List[datetime] = field(default_factory=list)
    implied_vols: Dict[Tuple[Decimal, datetime], Decimal] = field(default_factory=dict)
    
    # SABR parameters by expiry
    sabr_params: Dict[datetime, SABRParameters] = field(default_factory=dict)
    
    # Market data points
    market_points: List[VolatilityPoint] = field(default_factory=list)
    
    # Surface quality metrics
    calibration_rmse: float = 0.0
    arbitrage_violations: int = 0
    last_update: datetime = field(default_factory=datetime.utcnow)


class SABRModel:
    """SABR stochastic volatility model implementation"""
    
    @staticmethod
    def sabr_volatility(F: float, K: float, T: float, alpha: float, 
                       beta: float, rho: float, nu: float) -> float:
        """Calculate SABR implied volatility"""
        if K <= 0 or F <= 0:
            return 0.0
            
        # Handle ATM case
        if abs(F - K) < 1e-6:
            return SABRModel._sabr_atm_vol(F, T, alpha, beta, nu)
            
        # General SABR formula
        logFK = np.log(F / K)
        FK_beta = (F * K) ** ((1 - beta) / 2)
        
        # Calculate z and x(z)
        z = (nu / alpha) * FK_beta * logFK
        x_z = np.log((np.sqrt(1 - 2*rho*z + z**2) + z - rho) / (1 - rho))
        
        # Main formula components
        numerator = alpha
        denominator1 = FK_beta * (1 + ((1-beta)**2/24) * logFK**2 + 
                                  ((1-beta)**4/1920) * logFK**4)
        
        # Correction terms
        term1 = ((1-beta)**2/24) * (alpha**2 / FK_beta**2)
        term2 = (1/4) * (rho * beta * nu * alpha) / FK_beta
        term3 = (2 - 3*rho**2) * nu**2 / 24
        
        bracket = 1 + (term1 + term2 + term3) * T
        
        # Avoid division by zero
        if abs(x_z) < 1e-10:
            return numerator * bracket / denominator1
            
        return (numerator / denominator1) * (z / x_z) * bracket
        
    @staticmethod
    def _sabr_atm_vol(F: float, T: float, alpha: float, beta: float, nu: float) -> float:
        """SABR volatility at-the-money"""
        F_beta = F ** (1 - beta)
        term1 = ((1-beta)**2/24) * (alpha**2 / F_beta**2)
        term2 = nu**2 / 24
        
        return (alpha / F_beta) * (1 + (term1 + term2) * T)
        
    @staticmethod
    def calibrate_sabr(strikes: np.ndarray, vols: np.ndarray, forward: float,
                      expiry: float, beta: float = 0.5) -> SABRParameters:
        """Calibrate SABR parameters to market volatilities"""
        
        def objective(params):
            alpha, rho, nu = params
            
            # Ensure parameters are in valid ranges
            if alpha <= 0 or nu <= 0 or abs(rho) >= 1:
                return 1e10
                
            model_vols = np.array([
                SABRModel.sabr_volatility(forward, K, expiry, alpha, beta, rho, nu)
                for K in strikes
            ])
            
            # Weighted RMSE (weight by vega)
            weights = np.exp(-0.5 * ((strikes - forward) / forward) ** 2)
            return np.sqrt(np.mean(weights * (model_vols - vols) ** 2))
            
        # Initial guess
        atm_vol = np.interp(forward, strikes, vols)
        x0 = [atm_vol, 0.0, 0.3]
        
        # Bounds
        bounds = [(0.001, 2.0), (-0.999, 0.999), (0.001, 2.0)]
        
        # Optimize
        result = optimize.minimize(objective, x0, method='L-BFGS-B', bounds=bounds)
        
        return SABRParameters(
            alpha=result.x[0],
            beta=beta,
            rho=result.x[1],
            nu=result.x[2],
            forward=forward,
            expiry=expiry,
            calibration_error=result.fun
        )


class VolatilitySurfaceEngine:
    """Advanced volatility surface modeling with real-time updates"""
    
    def __init__(self, risk_free_rate: Decimal = Decimal("0.05")):
        self.surfaces: Dict[str, VolatilitySurfaceData] = {}
        self.risk_free_rate = risk_free_rate
        
        # Interpolation settings
        self.strike_interpolation = 'cubic'  # linear, cubic, sabr
        self.time_interpolation = 'linear'   # linear, sqrt_time
        
        # Arbitrage constraints
        self.min_volatility = Decimal("0.01")  # 1%
        self.max_volatility = Decimal("5.0")   # 500%
        self.butterfly_tolerance = Decimal("0.001")
        
        # Real-time update settings
        self.update_threshold = 100  # Update after N new points
        self.pending_updates: Dict[str, List[VolatilityPoint]] = defaultdict(list)
        
        # Quality metrics
        self.surface_quality: Dict[str, Dict] = {}
        
        # Background tasks
        self._update_task = None
        self._running = False
        
    async def start(self):
        """Start background tasks"""
        self._running = True
        self._update_task = asyncio.create_task(self._update_loop())
        logger.info("Volatility surface engine started")
        
    async def stop(self):
        """Stop background tasks"""
        self._running = False
        if self._update_task:
            await self._update_task
            
    async def add_market_point(self, point: VolatilityPoint):
        """Add a new market observation"""
        underlying = self._get_underlying_from_point(point)
        self.pending_updates[underlying].append(point)
        
        # Trigger immediate update if threshold reached
        if len(self.pending_updates[underlying]) >= self.update_threshold:
            await self.update_surface(underlying)
            
    async def update_surface(self, underlying: str):
        """Update volatility surface with new market data"""
        if underlying not in self.surfaces:
            self.surfaces[underlying] = VolatilitySurfaceData(
                underlying=underlying,
                timestamp=datetime.utcnow(),
                spot_price=Decimal("100"),  # Will be updated
                risk_free_rate=self.risk_free_rate
            )
            
        surface = self.surfaces[underlying]
        new_points = self.pending_updates.get(underlying, [])
        
        if not new_points:
            return
            
        # Add new points to surface
        surface.market_points.extend(new_points)
        
        # Keep only recent points (last 24 hours)
        cutoff = datetime.utcnow() - timedelta(hours=24)
        surface.market_points = [p for p in surface.market_points if p.timestamp > cutoff]
        
        # Update spot price
        surface.spot_price = new_points[-1].spot_price
        
        # Rebuild surface
        await self._rebuild_surface(surface)
        
        # Clear pending updates
        self.pending_updates[underlying] = []
        
        # Emit update event
        await self._emit_surface_update(underlying, surface)
        
    async def _rebuild_surface(self, surface: VolatilitySurfaceData):
        """Rebuild the entire volatility surface"""
        if not surface.market_points:
            return
            
        # Group points by expiry
        expiry_groups = defaultdict(list)
        for point in surface.market_points:
            expiry_groups[point.expiry].append(point)
            
        # Build strike-expiry grid
        all_strikes = sorted(set(p.strike for p in surface.market_points))
        all_expiries = sorted(expiry_groups.keys())
        
        surface.strikes = all_strikes
        surface.expiries = all_expiries
        
        # Calibrate SABR for each expiry
        for expiry, points in expiry_groups.items():
            if len(points) < 3:  # Need at least 3 points for SABR
                continue
                
            strikes = np.array([float(p.strike) for p in points])
            vols = np.array([float(p.implied_vol) for p in points])
            forward = float(surface.spot_price)  # Simplified - should use forward price
            time_to_expiry = (expiry - datetime.utcnow()).days / 365.25
            
            if time_to_expiry <= 0:
                continue
                
            # Calibrate SABR
            try:
                sabr_params = SABRModel.calibrate_sabr(
                    strikes, vols, forward, time_to_expiry
                )
                surface.sabr_params[expiry] = sabr_params
                
                # Fill implied vol grid using SABR
                for strike in surface.strikes:
                    iv = SABRModel.sabr_volatility(
                        forward, float(strike), time_to_expiry,
                        sabr_params.alpha, sabr_params.beta,
                        sabr_params.rho, sabr_params.nu
                    )
                    surface.implied_vols[(strike, expiry)] = Decimal(str(iv))
                    
            except Exception as e:
                logger.error(f"SABR calibration failed for {expiry}: {e}")
                # Fallback to interpolation
                self._interpolate_expiry_slice(surface, expiry, points)
                
        # Check arbitrage constraints
        self._check_arbitrage_constraints(surface)
        
        # Update quality metrics
        self._update_quality_metrics(surface)
        
        surface.last_update = datetime.utcnow()
        
    def get_implied_volatility(self, underlying: str, strike: Decimal,
                             expiry: datetime, spot: Optional[Decimal] = None) -> Decimal:
        """Get implied volatility for given strike and expiry"""
        if underlying not in self.surfaces:
            return self._get_default_volatility(strike, expiry, spot)
            
        surface = self.surfaces[underlying]
        
        # Direct lookup
        if (strike, expiry) in surface.implied_vols:
            return surface.implied_vols[(strike, expiry)]
            
        # Use SABR if available
        if expiry in surface.sabr_params:
            params = surface.sabr_params[expiry]
            time_to_expiry = (expiry - datetime.utcnow()).days / 365.25
            
            if time_to_expiry > 0:
                iv = SABRModel.sabr_volatility(
                    params.forward, float(strike), time_to_expiry,
                    params.alpha, params.beta, params.rho, params.nu
                )
                return Decimal(str(iv))
                
        # Interpolate
        return self._interpolate_volatility(surface, strike, expiry)
        
    def _interpolate_volatility(self, surface: VolatilitySurfaceData,
                               strike: Decimal, expiry: datetime) -> Decimal:
        """Interpolate volatility from surface"""
        # Find surrounding points
        strikes = sorted(surface.strikes)
        expiries = sorted(surface.expiries)
        
        if not strikes or not expiries:
            return self._get_default_volatility(strike, expiry, surface.spot_price)
            
        # Time interpolation first
        time_to_expiry = (expiry - datetime.utcnow()).days / 365.25
        
        # Get volatilities for this expiry across all strikes
        vol_slice = []
        for exp in expiries:
            if abs((exp - expiry).days) < 1:  # Same expiry
                vol_slice = [(float(k), float(surface.implied_vols.get((k, exp), 0)))
                           for k in strikes]
                break
                
        if not vol_slice:
            # Interpolate between expiries
            before_exp = None
            after_exp = None
            
            for exp in expiries:
                if exp <= expiry:
                    before_exp = exp
                elif after_exp is None:
                    after_exp = exp
                    break
                    
            if before_exp and after_exp:
                # Linear interpolation in time
                t1 = (before_exp - datetime.utcnow()).days / 365.25
                t2 = (after_exp - datetime.utcnow()).days / 365.25
                t = time_to_expiry
                
                w1 = (t2 - t) / (t2 - t1)
                w2 = (t - t1) / (t2 - t1)
                
                vol1 = self._interpolate_strike_slice(surface, strike, before_exp)
                vol2 = self._interpolate_strike_slice(surface, strike, after_exp)
                
                return vol1 * Decimal(str(w1)) + vol2 * Decimal(str(w2))
                
        # Strike interpolation
        return self._interpolate_strike_slice(surface, strike, expiry)
        
    def _interpolate_strike_slice(self, surface: VolatilitySurfaceData,
                                 strike: Decimal, expiry: datetime) -> Decimal:
        """Interpolate volatility across strikes for given expiry"""
        vols = []
        strikes_float = []
        
        for k in surface.strikes:
            if (k, expiry) in surface.implied_vols:
                strikes_float.append(float(k))
                vols.append(float(surface.implied_vols[(k, expiry)]))
                
        if len(vols) < 2:
            return self._get_default_volatility(strike, expiry, surface.spot_price)
            
        # Cubic interpolation with extrapolation
        f = interpolate.interp1d(strikes_float, vols, kind='cubic',
                               bounds_error=False, fill_value='extrapolate')
        
        iv = f(float(strike))
        
        # Bound check
        iv = max(float(self.min_volatility), min(float(self.max_volatility), iv))
        
        return Decimal(str(iv))
        
    def _get_default_volatility(self, strike: Decimal, expiry: datetime,
                               spot: Optional[Decimal] = None) -> Decimal:
        """Get default volatility when no surface available"""
        # Simple volatility smile
        moneyness = float(strike / spot) if spot else 1.0
        time_to_expiry = (expiry - datetime.utcnow()).days / 365.25
        
        # Base volatility with smile
        base_vol = 0.3  # 30%
        
        # Add smile effect
        smile = 0.1 * (moneyness - 1.0) ** 2
        
        # Term structure
        term_adj = 0.05 * np.sqrt(max(0.0, time_to_expiry))
        
        vol = base_vol + smile + term_adj
        
        return Decimal(str(max(0.01, min(5.0, vol))))
        
    def _check_arbitrage_constraints(self, surface: VolatilitySurfaceData):
        """Check and fix arbitrage violations"""
        violations = 0
        
        # Check butterfly arbitrage for each expiry
        for expiry in surface.expiries:
            strikes = sorted([k for k in surface.strikes
                           if (k, expiry) in surface.implied_vols])
            
            if len(strikes) < 3:
                continue
                
            for i in range(1, len(strikes) - 1):
                k1, k2, k3 = strikes[i-1], strikes[i], strikes[i+1]
                
                iv1 = surface.implied_vols.get((k1, expiry), Decimal("0"))
                iv2 = surface.implied_vols.get((k2, expiry), Decimal("0"))
                iv3 = surface.implied_vols.get((k3, expiry), Decimal("0"))
                
                # Butterfly condition: C(K1) + C(K3) >= 2*C(K2)
                # In terms of IV, this is approximately:
                # iv2 <= sqrt((iv1^2 + iv3^2) / 2)
                
                max_iv2 = (iv1**2 + iv3**2) / 2
                if iv2**2 > max_iv2 * (1 + self.butterfly_tolerance):
                    # Fix violation
                    new_iv2 = max_iv2.sqrt() * Decimal("0.99")
                    surface.implied_vols[(k2, expiry)] = new_iv2
                    violations += 1
                    
        surface.arbitrage_violations = violations
        
        if violations > 0:
            logger.warning(f"Fixed {violations} butterfly arbitrage violations")
            
    def _update_quality_metrics(self, surface: VolatilitySurfaceData):
        """Update surface quality metrics"""
        if not surface.market_points:
            return
            
        # Calculate fit quality
        errors = []
        
        for point in surface.market_points[-1000:]:  # Last 1000 points
            model_vol = self.get_implied_volatility(
                surface.underlying, point.strike, point.expiry, point.spot_price
            )
            error = float(model_vol - point.implied_vol)
            errors.append(error ** 2)
            
        surface.calibration_rmse = np.sqrt(np.mean(errors)) if errors else 0.0
        
        # Store quality metrics
        self.surface_quality[surface.underlying] = {
            'rmse': surface.calibration_rmse,
            'arbitrage_violations': surface.arbitrage_violations,
            'data_points': len(surface.market_points),
            'last_update': surface.last_update,
            'expiries': len(surface.expiries),
            'strikes': len(surface.strikes)
        }
        
    def _interpolate_expiry_slice(self, surface: VolatilitySurfaceData,
                                 expiry: datetime, points: List[VolatilityPoint]):
        """Fallback interpolation for expiry slice"""
        # Simple interpolation when SABR fails
        strikes = sorted(set(p.strike for p in points))
        
        for strike in strikes:
            # Find exact or interpolate
            exact_points = [p for p in points if p.strike == strike]
            
            if exact_points:
                # Use most recent
                vol = exact_points[-1].implied_vol
            else:
                # Linear interpolation
                below = [p for p in points if p.strike < strike]
                above = [p for p in points if p.strike > strike]
                
                if below and above:
                    p1 = max(below, key=lambda p: p.strike)
                    p2 = min(above, key=lambda p: p.strike)
                    
                    w1 = (p2.strike - strike) / (p2.strike - p1.strike)
                    w2 = (strike - p1.strike) / (p2.strike - p1.strike)
                    
                    vol = p1.implied_vol * w1 + p2.implied_vol * w2
                else:
                    vol = Decimal("0.3")  # Default
                    
            surface.implied_vols[(strike, expiry)] = vol
            
    async def _update_loop(self):
        """Background task to update surfaces periodically"""
        while self._running:
            try:
                # Update surfaces with pending data
                for underlying in list(self.pending_updates.keys()):
                    if self.pending_updates[underlying]:
                        await self.update_surface(underlying)
                        
                # Sleep
                await asyncio.sleep(30)  # Update every 30 seconds
                
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Error in volatility surface update loop: {e}")
                await asyncio.sleep(60)
                
    async def _emit_surface_update(self, underlying: str, surface: VolatilitySurfaceData):
        """Emit surface update event"""
        # This would publish to Pulsar in production
        update_data = {
            'underlying': underlying,
            'timestamp': surface.last_update.isoformat(),
            'spot_price': str(surface.spot_price),
            'quality_metrics': self.surface_quality.get(underlying, {}),
            'expiries': [e.isoformat() for e in surface.expiries[:10]],  # First 10
            'strikes': [str(s) for s in surface.strikes[:20]]  # First 20
        }
        
        logger.info(f"Volatility surface updated for {underlying}: {update_data}")
        
    def _get_underlying_from_point(self, point: VolatilityPoint) -> str:
        """Extract underlying from volatility point"""
        # In production, this would parse from option symbol
        return "compute"  # Default for now
        
    def get_surface_data(self, underlying: str) -> Optional[VolatilitySurfaceData]:
        """Get complete surface data"""
        return self.surfaces.get(underlying)
        
    def export_surface(self, underlying: str, format: str = 'json') -> Optional[str]:
        """Export surface data in various formats"""
        surface = self.surfaces.get(underlying)
        if not surface:
            return None
            
        if format == 'json':
            data = {
                'underlying': surface.underlying,
                'timestamp': surface.last_update.isoformat(),
                'spot_price': str(surface.spot_price),
                'risk_free_rate': str(surface.risk_free_rate),
                'strikes': [str(s) for s in surface.strikes],
                'expiries': [e.isoformat() for e in surface.expiries],
                'implied_vols': {
                    f"{k}_{e.isoformat()}": str(v)
                    for (k, e), v in surface.implied_vols.items()
                },
                'quality': self.surface_quality.get(underlying, {})
            }
            return json.dumps(data, indent=2)
            
        elif format == 'csv':
            # Export as CSV for analysis
            rows = ['Strike,Expiry,ImpliedVol,Spot,Timestamp']
            
            for (strike, expiry), vol in surface.implied_vols.items():
                rows.append(f"{strike},{expiry.isoformat()},{vol},{surface.spot_price},{surface.last_update.isoformat()}")
                
            return '\n'.join(rows)
            
        return None 