"""
Pricing engines for derivatives with full Black-Scholes implementation
"""

from decimal import Decimal, getcontext
from typing import Dict, Optional, List, Tuple
import math
from dataclasses import dataclass
from datetime import datetime
import numpy as np
from scipy import stats, optimize
from scipy.interpolate import RectBivariateSpline
import logging

# Set high precision for financial calculations
getcontext().prec = 28

logger = logging.getLogger(__name__)


@dataclass
class OptionParameters:
    """Parameters for option pricing"""
    spot: Decimal
    strike: Decimal
    time_to_expiry: Decimal  # in years
    volatility: Decimal
    risk_free_rate: Decimal
    dividend_yield: Decimal = Decimal("0")
    is_call: bool = True


@dataclass
class Greeks:
    """Option Greeks"""
    delta: Decimal
    gamma: Decimal
    theta: Decimal
    vega: Decimal
    rho: Decimal
    lambda_: Optional[Decimal] = None  # Elasticity
    vanna: Optional[Decimal] = None  # dDelta/dVol
    charm: Optional[Decimal] = None  # dDelta/dTime
    vomma: Optional[Decimal] = None  # dVega/dVol
    speed: Optional[Decimal] = None  # dGamma/dSpot


class BlackScholesEngine:
    """Black-Scholes option pricing engine with full Greeks calculation"""
    
    def __init__(self):
        self._cache = {}  # Cache for repeated calculations
    
    def _normal_cdf(self, x: float) -> float:
        """Cumulative distribution function for standard normal"""
        return stats.norm.cdf(x)
    
    def _normal_pdf(self, x: float) -> float:
        """Probability density function for standard normal"""
        return stats.norm.pdf(x)
    
    def _calculate_d1_d2(self, params: OptionParameters) -> Tuple[float, float]:
        """Calculate d1 and d2 for Black-Scholes formula"""
        S = float(params.spot)
        K = float(params.strike)
        T = float(params.time_to_expiry)
        r = float(params.risk_free_rate)
        q = float(params.dividend_yield)
        sigma = float(params.volatility)
        
        if T <= 0:
            # Handle expired options
            return (0.0, 0.0)
        
        d1 = (math.log(S / K) + (r - q + 0.5 * sigma ** 2) * T) / (sigma * math.sqrt(T))
        d2 = d1 - sigma * math.sqrt(T)
        
        return d1, d2
    
    def calculate_option_price(self, params: OptionParameters) -> Decimal:
        """Calculate option price using Black-Scholes formula"""
        S = float(params.spot)
        K = float(params.strike)
        T = float(params.time_to_expiry)
        r = float(params.risk_free_rate)
        q = float(params.dividend_yield)
        
        if T <= 0:
            # Handle expired options
            if params.is_call:
                return Decimal(str(max(S - K, 0)))
            else:
                return Decimal(str(max(K - S, 0)))
        
        d1, d2 = self._calculate_d1_d2(params)
        
        if params.is_call:
            price = (S * math.exp(-q * T) * self._normal_cdf(d1) - 
                    K * math.exp(-r * T) * self._normal_cdf(d2))
        else:
            price = (K * math.exp(-r * T) * self._normal_cdf(-d2) - 
                    S * math.exp(-q * T) * self._normal_cdf(-d1))
        
        return Decimal(str(price))
    
    def calculate_greeks(self, params: OptionParameters, calculate_second_order: bool = True) -> Greeks:
        """Calculate all option Greeks"""
        S = float(params.spot)
        K = float(params.strike)
        T = float(params.time_to_expiry)
        r = float(params.risk_free_rate)
        q = float(params.dividend_yield)
        sigma = float(params.volatility)
        
        if T <= 0:
            # Handle expired options
            if params.is_call:
                delta = Decimal("1") if S > K else Decimal("0")
            else:
                delta = Decimal("-1") if S < K else Decimal("0")
            
            return Greeks(
                delta=delta,
                gamma=Decimal("0"),
                theta=Decimal("0"),
                vega=Decimal("0"),
                rho=Decimal("0")
            )
        
        d1, d2 = self._calculate_d1_d2(params)
        sqrt_T = math.sqrt(T)
        
        # Delta
        if params.is_call:
            delta = math.exp(-q * T) * self._normal_cdf(d1)
        else:
            delta = math.exp(-q * T) * (self._normal_cdf(d1) - 1)
        
        # Gamma
        gamma = (math.exp(-q * T) * self._normal_pdf(d1)) / (S * sigma * sqrt_T)
        
        # Theta
        if params.is_call:
            theta = ((-S * self._normal_pdf(d1) * sigma * math.exp(-q * T)) / (2 * sqrt_T) -
                    r * K * math.exp(-r * T) * self._normal_cdf(d2) +
                    q * S * math.exp(-q * T) * self._normal_cdf(d1))
        else:
            theta = ((-S * self._normal_pdf(d1) * sigma * math.exp(-q * T)) / (2 * sqrt_T) +
                    r * K * math.exp(-r * T) * self._normal_cdf(-d2) -
                    q * S * math.exp(-q * T) * self._normal_cdf(-d1))
        
        theta = theta / 365  # Convert to daily theta
        
        # Vega
        vega = S * math.exp(-q * T) * self._normal_pdf(d1) * sqrt_T / 100  # Per 1% change
        
        # Rho
        if params.is_call:
            rho = K * T * math.exp(-r * T) * self._normal_cdf(d2) / 100  # Per 1% change
        else:
            rho = -K * T * math.exp(-r * T) * self._normal_cdf(-d2) / 100
        
        greeks = Greeks(
            delta=Decimal(str(delta)),
            gamma=Decimal(str(gamma)),
            theta=Decimal(str(theta)),
            vega=Decimal(str(vega)),
            rho=Decimal(str(rho))
        )
        
        if calculate_second_order:
            # Lambda (elasticity)
            option_price = float(self.calculate_option_price(params))
            if option_price > 0:
                greeks.lambda_ = Decimal(str(delta * S / option_price))
            
            # Vanna (dDelta/dVol)
            vanna = -math.exp(-q * T) * self._normal_pdf(d1) * d2 / sigma
            greeks.vanna = Decimal(str(vanna))
            
            # Charm (dDelta/dTime)
            if params.is_call:
                charm = -math.exp(-q * T) * (self._normal_pdf(d1) * 
                        (r - q - d2 * sigma / (2 * T)) / (sigma * sqrt_T) +
                        q * self._normal_cdf(d1))
            else:
                charm = -math.exp(-q * T) * (self._normal_pdf(d1) * 
                        (r - q - d2 * sigma / (2 * T)) / (sigma * sqrt_T) -
                        q * self._normal_cdf(-d1))
            
            greeks.charm = Decimal(str(charm / 365))  # Daily charm
            
            # Vomma (dVega/dVol)
            vomma = vega * d1 * d2 / sigma
            greeks.vomma = Decimal(str(vomma))
            
            # Speed (dGamma/dSpot)
            speed = -gamma / S * (1 + d1 / (sigma * sqrt_T))
            greeks.speed = Decimal(str(speed))
        
        return greeks
    
    def calculate_implied_volatility(
        self,
        option_price: Decimal,
        params: OptionParameters,
        max_iterations: int = 100,
        tolerance: float = 1e-6
    ) -> Optional[Decimal]:
        """Calculate implied volatility using Newton-Raphson method"""
        
        target_price = float(option_price)
        
        # Initial guess
        sigma = 0.3  # 30% volatility
        
        for _ in range(max_iterations):
            params.volatility = Decimal(str(sigma))
            
            # Calculate option price and vega
            calculated_price = float(self.calculate_option_price(params))
            greeks = self.calculate_greeks(params, calculate_second_order=False)
            vega = float(greeks.vega) * 100  # Convert back from per 1% to per unit
            
            # Check convergence
            price_diff = calculated_price - target_price
            if abs(price_diff) < tolerance:
                return Decimal(str(sigma))
            
            # Avoid division by zero
            if abs(vega) < 1e-10:
                break
            
            # Newton-Raphson update
            sigma = sigma - price_diff / vega
            
            # Ensure sigma stays positive
            sigma = max(0.001, min(sigma, 5.0))  # Cap between 0.1% and 500%
        
        # If no convergence, try bisection method
        return self._implied_vol_bisection(option_price, params)
    
    def _implied_vol_bisection(
        self,
        option_price: Decimal,
        params: OptionParameters,
        max_iterations: int = 50
    ) -> Optional[Decimal]:
        """Fallback bisection method for implied volatility"""
        low_vol = 0.001
        high_vol = 5.0
        target = float(option_price)
        
        for _ in range(max_iterations):
            mid_vol = (low_vol + high_vol) / 2
            params.volatility = Decimal(str(mid_vol))
            
            price = float(self.calculate_option_price(params))
            
            if abs(price - target) < 1e-6:
                return Decimal(str(mid_vol))
            
            if price < target:
                low_vol = mid_vol
            else:
                high_vol = mid_vol
        
        return None


class VolatilitySurfaceEngine:
    """Volatility surface modeling for options"""
    
    def __init__(self):
        self.surface_data = {}  # Strike -> TTM -> IV
        self.interpolator = None
        self.last_update = datetime.now()
        
    def update_surface(
        self,
        strikes: List[Decimal],
        maturities: List[Decimal],  # Time to maturity in years
        implied_vols: List[List[Decimal]]
    ):
        """Update volatility surface with new data"""
        self.surface_data = {
            "strikes": strikes,
            "maturities": maturities,
            "ivs": implied_vols
        }
        
        # Create interpolator
        strikes_float = [float(s) for s in strikes]
        maturities_float = [float(m) for m in maturities]
        ivs_float = [[float(iv) for iv in row] for row in implied_vols]
        
        self.interpolator = RectBivariateSpline(
            strikes_float,
            maturities_float,
            ivs_float,
            kx=2,  # Cubic spline
            ky=2
        )
        
        self.last_update = datetime.now()
        
    def get_implied_volatility(
        self,
        strike: Decimal,
        time_to_expiry: Decimal,
        spot: Optional[Decimal] = None
    ) -> Decimal:
        """Get implied volatility from surface"""
        
        if not self.interpolator:
            # Return default volatility if no surface data
            return Decimal("0.3")  # 30%
        
        strike_float = float(strike)
        ttm_float = float(time_to_expiry)
        
        # Apply bounds
        min_strike = float(min(self.surface_data["strikes"]))
        max_strike = float(max(self.surface_data["strikes"]))
        min_ttm = float(min(self.surface_data["maturities"]))
        max_ttm = float(max(self.surface_data["maturities"]))
        
        strike_float = max(min_strike, min(strike_float, max_strike))
        ttm_float = max(min_ttm, min(ttm_float, max_ttm))
        
        # Interpolate
        iv = float(self.interpolator(strike_float, ttm_float)[0, 0])
        
        # Apply smile/skew adjustments if spot provided
        if spot:
            moneyness = float(strike / spot)
            
            # Simple skew adjustment
            if moneyness < 0.9:  # OTM puts
                iv *= 1.1
            elif moneyness > 1.1:  # OTM calls
                iv *= 1.05
        
        return Decimal(str(max(0.01, min(iv, 5.0))))  # Cap between 1% and 500%
    
    def get_atm_volatility(self, time_to_expiry: Decimal, spot: Decimal) -> Decimal:
        """Get at-the-money implied volatility"""
        return self.get_implied_volatility(spot, time_to_expiry, spot)
    
    def calculate_volatility_smile(
        self,
        spot: Decimal,
        time_to_expiry: Decimal,
        num_strikes: int = 21
    ) -> Dict[Decimal, Decimal]:
        """Calculate volatility smile for given maturity"""
        
        # Generate strikes around spot (80% to 120% moneyness)
        strikes = []
        for i in range(num_strikes):
            moneyness = Decimal("0.8") + (Decimal("0.4") * i / (num_strikes - 1))
            strikes.append(spot * moneyness)
        
        smile = {}
        for strike in strikes:
            smile[strike] = self.get_implied_volatility(strike, time_to_expiry, spot)
        
        return smile
    
    def calculate_term_structure(
        self,
        strike: Decimal,
        max_maturity: Decimal = Decimal("2.0"),  # 2 years
        num_points: int = 10
    ) -> Dict[Decimal, Decimal]:
        """Calculate volatility term structure for given strike"""
        
        maturities = []
        for i in range(num_points):
            ttm = (max_maturity * (i + 1)) / num_points
            maturities.append(ttm)
        
        term_structure = {}
        for ttm in maturities:
            term_structure[ttm] = self.get_implied_volatility(strike, ttm)
        
        return term_structure


class OptionsChainManager:
    """Manages options chains for different underlying assets"""
    
    def __init__(self):
        self.chains = {}  # underlying -> expiry -> strikes -> option_data
        self.bs_engine = BlackScholesEngine()
        self.vol_surface = VolatilitySurfaceEngine()
        
    def create_option_chain(
        self,
        underlying: str,
        spot: Decimal,
        expiry: datetime,
        strike_interval: Decimal,
        num_strikes: int = 21,
        risk_free_rate: Decimal = Decimal("0.05")
    ) -> Dict:
        """Create a new option chain"""
        
        if underlying not in self.chains:
            self.chains[underlying] = {}
        
        # Calculate time to expiry
        time_to_expiry = Decimal(str((expiry - datetime.now()).days / 365.25))
        
        # Generate strikes centered around spot
        strikes = []
        center_strike = round(spot / strike_interval) * strike_interval
        
        for i in range(-(num_strikes // 2), (num_strikes // 2) + 1):
            strike = center_strike + (i * strike_interval)
            if strike > 0:
                strikes.append(strike)
        
        chain_data = {
            "spot": spot,
            "expiry": expiry,
            "time_to_expiry": time_to_expiry,
            "risk_free_rate": risk_free_rate,
            "strikes": {}
        }
        
        for strike in strikes:
            # Get implied volatility from surface
            iv = self.vol_surface.get_implied_volatility(strike, time_to_expiry, spot)
            
            # Create option parameters
            params = OptionParameters(
                spot=spot,
                strike=strike,
                time_to_expiry=time_to_expiry,
                volatility=iv,
                risk_free_rate=risk_free_rate
            )
            
            # Calculate call option
            params.is_call = True
            call_price = self.bs_engine.calculate_option_price(params)
            call_greeks = self.bs_engine.calculate_greeks(params)
            
            # Calculate put option
            params.is_call = False
            put_price = self.bs_engine.calculate_option_price(params)
            put_greeks = self.bs_engine.calculate_greeks(params)
            
            chain_data["strikes"][strike] = {
                "call": {
                    "price": call_price,
                    "iv": iv,
                    "greeks": call_greeks,
                    "open_interest": Decimal("0"),
                    "volume": Decimal("0")
                },
                "put": {
                    "price": put_price,
                    "iv": iv,
                    "greeks": put_greeks,
                    "open_interest": Decimal("0"),
                    "volume": Decimal("0")
                }
            }
        
        self.chains[underlying][expiry] = chain_data
        return chain_data
    
    def update_option_metrics(
        self,
        underlying: str,
        expiry: datetime,
        strike: Decimal,
        is_call: bool,
        volume: Decimal,
        open_interest: Decimal
    ):
        """Update volume and open interest for an option"""
        
        if (underlying in self.chains and 
            expiry in self.chains[underlying] and
            strike in self.chains[underlying][expiry]["strikes"]):
            
            option_type = "call" if is_call else "put"
            option_data = self.chains[underlying][expiry]["strikes"][strike][option_type]
            
            option_data["volume"] = volume
            option_data["open_interest"] = open_interest
    
    def get_option_chain(
        self,
        underlying: str,
        expiry: Optional[datetime] = None
    ) -> Optional[Dict]:
        """Get option chain data"""
        
        if underlying not in self.chains:
            return None
        
        if expiry:
            return self.chains[underlying].get(expiry)
        
        # Return all expiries
        return self.chains[underlying]
    
    def calculate_put_call_ratio(
        self,
        underlying: str,
        expiry: Optional[datetime] = None
    ) -> Optional[Decimal]:
        """Calculate put/call ratio for sentiment analysis"""
        
        total_put_oi = Decimal("0")
        total_call_oi = Decimal("0")
        
        chains_to_check = []
        if expiry and underlying in self.chains:
            chains_to_check = [self.chains[underlying].get(expiry, {})]
        elif underlying in self.chains:
            chains_to_check = self.chains[underlying].values()
        
        for chain in chains_to_check:
            for strike_data in chain.get("strikes", {}).values():
                total_put_oi += strike_data["put"]["open_interest"]
                total_call_oi += strike_data["call"]["open_interest"]
        
        if total_call_oi > 0:
            return total_put_oi / total_call_oi
        
        return None
    
    def find_max_pain(
        self,
        underlying: str,
        expiry: datetime
    ) -> Optional[Tuple[Decimal, Decimal]]:
        """Find max pain strike price"""
        
        if (underlying not in self.chains or 
            expiry not in self.chains[underlying]):
            return None
        
        chain = self.chains[underlying][expiry]
        spot = chain["spot"]
        
        min_pain_value = None
        max_pain_strike = None
        
        # Check each potential expiry price
        for potential_strike in chain["strikes"].keys():
            total_pain = Decimal("0")
            
            # Calculate pain for all strikes
            for strike, data in chain["strikes"].items():
                # Call pain (for call sellers)
                if potential_strike > strike:
                    call_pain = (potential_strike - strike) * data["call"]["open_interest"]
                    total_pain += call_pain
                
                # Put pain (for put sellers)
                if potential_strike < strike:
                    put_pain = (strike - potential_strike) * data["put"]["open_interest"]
                    total_pain += put_pain
            
            if min_pain_value is None or total_pain < min_pain_value:
                min_pain_value = total_pain
                max_pain_strike = potential_strike
        
        return (max_pain_strike, min_pain_value) if max_pain_strike else None 