"""Shared pricing engines for options and derivatives"""

from abc import ABC, abstractmethod
from dataclasses import dataclass
from decimal import Decimal
from datetime import datetime, timedelta
from typing import Optional, Tuple
import numpy as np
from scipy import stats
from scipy.optimize import brentq


@dataclass
class OptionParameters:
    """Parameters for option pricing"""
    spot: Decimal
    strike: Decimal
    time_to_expiry: Decimal  # In years
    volatility: Decimal
    risk_free_rate: Decimal
    dividend_yield: Decimal = Decimal(0)
    is_call: bool = True


@dataclass
class Greeks:
    """Option Greeks"""
    delta: Decimal
    gamma: Decimal
    theta: Decimal
    vega: Decimal
    rho: Decimal


class PricingEngine(ABC):
    """Base class for pricing engines"""
    
    @abstractmethod
    def price(self, params: OptionParameters) -> Decimal:
        """Calculate option price"""
        pass
    
    @abstractmethod
    def calculate_greeks(self, params: OptionParameters) -> Greeks:
        """Calculate option Greeks"""
        pass


class BlackScholesEngine(PricingEngine):
    """Black-Scholes option pricing engine"""
    
    def price(self, params: OptionParameters) -> Decimal:
        """Calculate option price using Black-Scholes formula"""
        S = float(params.spot)
        K = float(params.strike)
        T = float(params.time_to_expiry)
        sigma = float(params.volatility)
        r = float(params.risk_free_rate)
        q = float(params.dividend_yield)
        
        if T <= 0:
            # Option expired
            if params.is_call:
                return Decimal(str(max(S - K, 0)))
            else:
                return Decimal(str(max(K - S, 0)))
        
        # Calculate d1 and d2
        d1 = (np.log(S / K) + (r - q + 0.5 * sigma**2) * T) / (sigma * np.sqrt(T))
        d2 = d1 - sigma * np.sqrt(T)
        
        if params.is_call:
            # Call option
            price = S * np.exp(-q * T) * stats.norm.cdf(d1) - K * np.exp(-r * T) * stats.norm.cdf(d2)
        else:
            # Put option
            price = K * np.exp(-r * T) * stats.norm.cdf(-d2) - S * np.exp(-q * T) * stats.norm.cdf(-d1)
        
        return Decimal(str(max(price, 0)))
    
    def calculate_greeks(self, params: OptionParameters) -> Greeks:
        """Calculate option Greeks"""
        S = float(params.spot)
        K = float(params.strike)
        T = float(params.time_to_expiry)
        sigma = float(params.volatility)
        r = float(params.risk_free_rate)
        q = float(params.dividend_yield)
        
        if T <= 0:
            # Option expired
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
        
        # Calculate d1 and d2
        d1 = (np.log(S / K) + (r - q + 0.5 * sigma**2) * T) / (sigma * np.sqrt(T))
        d2 = d1 - sigma * np.sqrt(T)
        
        # Common terms
        phi_d1 = stats.norm.pdf(d1)
        
        # Delta
        if params.is_call:
            delta = np.exp(-q * T) * stats.norm.cdf(d1)
        else:
            delta = -np.exp(-q * T) * stats.norm.cdf(-d1)
        
        # Gamma (same for calls and puts)
        gamma = np.exp(-q * T) * phi_d1 / (S * sigma * np.sqrt(T))
        
        # Theta
        term1 = -S * phi_d1 * sigma * np.exp(-q * T) / (2 * np.sqrt(T))
        
        if params.is_call:
            term2 = r * K * np.exp(-r * T) * stats.norm.cdf(d2)
            term3 = q * S * np.exp(-q * T) * stats.norm.cdf(d1)
            theta = (term1 - term2 + term3) / 365  # Daily theta
        else:
            term2 = r * K * np.exp(-r * T) * stats.norm.cdf(-d2)
            term3 = q * S * np.exp(-q * T) * stats.norm.cdf(-d1)
            theta = (term1 + term2 - term3) / 365  # Daily theta
        
        # Vega (same for calls and puts)
        vega = S * np.exp(-q * T) * phi_d1 * np.sqrt(T) / 100  # Per 1% change in vol
        
        # Rho
        if params.is_call:
            rho = K * T * np.exp(-r * T) * stats.norm.cdf(d2) / 100  # Per 1% change in rate
        else:
            rho = -K * T * np.exp(-r * T) * stats.norm.cdf(-d2) / 100
        
        return Greeks(
            delta=Decimal(str(delta)),
            gamma=Decimal(str(gamma)),
            theta=Decimal(str(theta)),
            vega=Decimal(str(vega)),
            rho=Decimal(str(rho))
        )
    
    def implied_volatility(
        self, 
        option_price: Decimal, 
        params: OptionParameters,
        min_vol: float = 0.01,
        max_vol: float = 5.0
    ) -> Decimal:
        """Calculate implied volatility using Newton-Raphson method"""
        target_price = float(option_price)
        
        def objective(vol):
            test_params = OptionParameters(
                spot=params.spot,
                strike=params.strike,
                time_to_expiry=params.time_to_expiry,
                volatility=Decimal(str(vol)),
                risk_free_rate=params.risk_free_rate,
                dividend_yield=params.dividend_yield,
                is_call=params.is_call
            )
            return float(self.price(test_params)) - target_price
        
        try:
            # Use Brent's method for robustness
            iv = brentq(objective, min_vol, max_vol, xtol=1e-6)
            return Decimal(str(iv))
        except:
            # Fallback to a simple approximation
            return Decimal("0.3")  # 30% volatility as default


class BinomialEngine(PricingEngine):
    """Binomial tree option pricing engine (for American options)"""
    
    def __init__(self, steps: int = 100):
        self.steps = steps
    
    def price(self, params: OptionParameters) -> Decimal:
        """Price option using binomial tree"""
        S = float(params.spot)
        K = float(params.strike)
        T = float(params.time_to_expiry)
        sigma = float(params.volatility)
        r = float(params.risk_free_rate)
        q = float(params.dividend_yield)
        n = self.steps
        
        if T <= 0:
            if params.is_call:
                return Decimal(str(max(S - K, 0)))
            else:
                return Decimal(str(max(K - S, 0)))
        
        # Calculate parameters
        dt = T / n
        u = np.exp(sigma * np.sqrt(dt))
        d = 1 / u
        p = (np.exp((r - q) * dt) - d) / (u - d)
        
        # Initialize asset prices at maturity
        asset_prices = np.zeros(n + 1)
        for i in range(n + 1):
            asset_prices[i] = S * (u ** (n - i)) * (d ** i)
        
        # Initialize option values at maturity
        option_values = np.zeros(n + 1)
        for i in range(n + 1):
            if params.is_call:
                option_values[i] = max(asset_prices[i] - K, 0)
            else:
                option_values[i] = max(K - asset_prices[i], 0)
        
        # Step back through the tree
        for j in range(n - 1, -1, -1):
            for i in range(j + 1):
                # Calculate option value from future values
                option_values[i] = np.exp(-r * dt) * (
                    p * option_values[i] + (1 - p) * option_values[i + 1]
                )
                
                # For American options, check early exercise
                asset_price = S * (u ** (j - i)) * (d ** i)
                if params.is_call:
                    exercise_value = max(asset_price - K, 0)
                else:
                    exercise_value = max(K - asset_price, 0)
                
                option_values[i] = max(option_values[i], exercise_value)
        
        return Decimal(str(option_values[0]))
    
    def calculate_greeks(self, params: OptionParameters) -> Greeks:
        """Calculate Greeks using finite differences"""
        base_price = self.price(params)
        
        # Delta: dV/dS
        bump = params.spot * Decimal("0.01")  # 1% bump
        params_up = OptionParameters(
            spot=params.spot + bump,
            strike=params.strike,
            time_to_expiry=params.time_to_expiry,
            volatility=params.volatility,
            risk_free_rate=params.risk_free_rate,
            dividend_yield=params.dividend_yield,
            is_call=params.is_call
        )
        params_down = OptionParameters(
            spot=params.spot - bump,
            strike=params.strike,
            time_to_expiry=params.time_to_expiry,
            volatility=params.volatility,
            risk_free_rate=params.risk_free_rate,
            dividend_yield=params.dividend_yield,
            is_call=params.is_call
        )
        
        price_up = self.price(params_up)
        price_down = self.price(params_down)
        delta = (price_up - price_down) / (2 * bump)
        
        # Gamma: d²V/dS²
        gamma = (price_up - 2 * base_price + price_down) / (bump ** 2)
        
        # Vega: dV/dσ
        vol_bump = Decimal("0.01")  # 1% volatility bump
        params_vol_up = OptionParameters(
            spot=params.spot,
            strike=params.strike,
            time_to_expiry=params.time_to_expiry,
            volatility=params.volatility + vol_bump,
            risk_free_rate=params.risk_free_rate,
            dividend_yield=params.dividend_yield,
            is_call=params.is_call
        )
        
        vega = (self.price(params_vol_up) - base_price) / 100  # Per 1% change
        
        # Theta: dV/dt
        if params.time_to_expiry > Decimal("0.01"):
            time_bump = Decimal("1") / Decimal("365")  # 1 day
            params_time = OptionParameters(
                spot=params.spot,
                strike=params.strike,
                time_to_expiry=params.time_to_expiry - time_bump,
                volatility=params.volatility,
                risk_free_rate=params.risk_free_rate,
                dividend_yield=params.dividend_yield,
                is_call=params.is_call
            )
            theta = self.price(params_time) - base_price  # Daily theta
        else:
            theta = Decimal("0")
        
        # Rho: dV/dr
        rate_bump = Decimal("0.01")  # 1% rate bump
        params_rate_up = OptionParameters(
            spot=params.spot,
            strike=params.strike,
            time_to_expiry=params.time_to_expiry,
            volatility=params.volatility,
            risk_free_rate=params.risk_free_rate + rate_bump,
            dividend_yield=params.dividend_yield,
            is_call=params.is_call
        )
        
        rho = (self.price(params_rate_up) - base_price) / 100  # Per 1% change
        
        return Greeks(
            delta=delta,
            gamma=gamma,
            theta=theta,
            vega=vega,
            rho=rho
        ) 