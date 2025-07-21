"""Greeks calculations for options."""

import numpy as np
from decimal import Decimal
from typing import Dict, Optional, Tuple
from scipy.stats import norm
from dataclasses import dataclass
from enum import Enum


class OptionType(Enum):
    """Option types."""
    CALL = "call"
    PUT = "put"


@dataclass
class Greeks:
    """Option Greeks."""
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


class GreeksCalculator:
    """Calculate option Greeks using various models."""
    
    def __init__(self, precision: int = 6):
        """Initialize Greeks calculator.
        
        Args:
            precision: Decimal precision for calculations
        """
        self.precision = precision
    
    def calculate_black_scholes_greeks(
        self,
        spot: Decimal,
        strike: Decimal,
        time_to_expiry: Decimal,
        volatility: Decimal,
        risk_free_rate: Decimal,
        dividend_yield: Decimal,
        option_type: OptionType,
        calculate_second_order: bool = False
    ) -> Greeks:
        """Calculate Greeks using Black-Scholes model.
        
        Args:
            spot: Current spot price
            strike: Strike price
            time_to_expiry: Time to expiry in years
            volatility: Implied volatility (annualized)
            risk_free_rate: Risk-free rate
            dividend_yield: Dividend yield
            option_type: Call or Put
            calculate_second_order: Whether to calculate second-order Greeks
            
        Returns:
            Greeks object with calculated values
        """
        # Convert to float for calculations
        S = float(spot)
        K = float(strike)
        T = float(time_to_expiry)
        sigma = float(volatility)
        r = float(risk_free_rate)
        q = float(dividend_yield)
        
        # Handle edge cases
        if T <= 0:
            return self._calculate_expired_greeks(S, K, option_type)
        
        if sigma <= 0:
            return self._calculate_zero_vol_greeks(S, K, T, r, q, option_type)
        
        # Calculate d1 and d2
        d1 = (np.log(S / K) + (r - q + 0.5 * sigma ** 2) * T) / (sigma * np.sqrt(T))
        d2 = d1 - sigma * np.sqrt(T)
        
        # Calculate first-order Greeks
        if option_type == OptionType.CALL:
            delta = np.exp(-q * T) * norm.cdf(d1)
            theta = self._calculate_call_theta(S, K, T, r, q, sigma, d1, d2)
        else:  # PUT
            delta = -np.exp(-q * T) * norm.cdf(-d1)
            theta = self._calculate_put_theta(S, K, T, r, q, sigma, d1, d2)
        
        gamma = np.exp(-q * T) * norm.pdf(d1) / (S * sigma * np.sqrt(T))
        vega = S * np.exp(-q * T) * norm.pdf(d1) * np.sqrt(T) / 100  # Per 1% change
        
        if option_type == OptionType.CALL:
            rho = K * T * np.exp(-r * T) * norm.cdf(d2) / 100  # Per 1% change
        else:  # PUT
            rho = -K * T * np.exp(-r * T) * norm.cdf(-d2) / 100
        
        # Calculate option value for lambda
        option_value = self._calculate_option_value(S, K, T, r, q, sigma, d1, d2, option_type)
        lambda_ = delta * S / option_value if option_value > 0 else 0
        
        greeks = Greeks(
            delta=Decimal(str(delta)).quantize(Decimal(10) ** -self.precision),
            gamma=Decimal(str(gamma)).quantize(Decimal(10) ** -self.precision),
            theta=Decimal(str(theta)).quantize(Decimal(10) ** -self.precision),
            vega=Decimal(str(vega)).quantize(Decimal(10) ** -self.precision),
            rho=Decimal(str(rho)).quantize(Decimal(10) ** -self.precision),
            lambda_=Decimal(str(lambda_)).quantize(Decimal(10) ** -self.precision)
        )
        
        # Calculate second-order Greeks if requested
        if calculate_second_order:
            vanna = self._calculate_vanna(S, T, q, sigma, d1, d2)
            charm = self._calculate_charm(S, T, r, q, sigma, d1, d2, option_type)
            vomma = self._calculate_vomma(S, T, q, sigma, d1)
            speed = self._calculate_speed(S, T, q, sigma, d1)
            
            greeks.vanna = Decimal(str(vanna)).quantize(Decimal(10) ** -self.precision)
            greeks.charm = Decimal(str(charm)).quantize(Decimal(10) ** -self.precision)
            greeks.vomma = Decimal(str(vomma)).quantize(Decimal(10) ** -self.precision)
            greeks.speed = Decimal(str(speed)).quantize(Decimal(10) ** -self.precision)
        
        return greeks
    
    def calculate_portfolio_greeks(
        self,
        positions: Dict[str, Tuple[Decimal, Greeks]]
    ) -> Greeks:
        """Calculate aggregated Greeks for a portfolio.
        
        Args:
            positions: Dict of position_id -> (quantity, greeks)
            
        Returns:
            Aggregated Greeks for the portfolio
        """
        total_delta = Decimal("0")
        total_gamma = Decimal("0")
        total_theta = Decimal("0")
        total_vega = Decimal("0")
        total_rho = Decimal("0")
        
        for position_id, (quantity, greeks) in positions.items():
            total_delta += quantity * greeks.delta
            total_gamma += quantity * greeks.gamma
            total_theta += quantity * greeks.theta
            total_vega += quantity * greeks.vega
            total_rho += quantity * greeks.rho
        
        return Greeks(
            delta=total_delta,
            gamma=total_gamma,
            theta=total_theta,
            vega=total_vega,
            rho=total_rho
        )
    
    def _calculate_call_theta(
        self, S: float, K: float, T: float, r: float, q: float, 
        sigma: float, d1: float, d2: float
    ) -> float:
        """Calculate theta for call option."""
        term1 = -S * np.exp(-q * T) * norm.pdf(d1) * sigma / (2 * np.sqrt(T))
        term2 = q * S * np.exp(-q * T) * norm.cdf(d1)
        term3 = -r * K * np.exp(-r * T) * norm.cdf(d2)
        return (term1 + term2 + term3) / 365  # Per day
    
    def _calculate_put_theta(
        self, S: float, K: float, T: float, r: float, q: float,
        sigma: float, d1: float, d2: float
    ) -> float:
        """Calculate theta for put option."""
        term1 = -S * np.exp(-q * T) * norm.pdf(d1) * sigma / (2 * np.sqrt(T))
        term2 = -q * S * np.exp(-q * T) * norm.cdf(-d1)
        term3 = r * K * np.exp(-r * T) * norm.cdf(-d2)
        return (term1 + term2 + term3) / 365  # Per day
    
    def _calculate_option_value(
        self, S: float, K: float, T: float, r: float, q: float,
        sigma: float, d1: float, d2: float, option_type: OptionType
    ) -> float:
        """Calculate option value using Black-Scholes."""
        if option_type == OptionType.CALL:
            return S * np.exp(-q * T) * norm.cdf(d1) - K * np.exp(-r * T) * norm.cdf(d2)
        else:  # PUT
            return K * np.exp(-r * T) * norm.cdf(-d2) - S * np.exp(-q * T) * norm.cdf(-d1)
    
    def _calculate_vanna(
        self, S: float, T: float, q: float, sigma: float, d1: float, d2: float
    ) -> float:
        """Calculate vanna (dDelta/dVol)."""
        return -np.exp(-q * T) * norm.pdf(d1) * d2 / sigma
    
    def _calculate_charm(
        self, S: float, T: float, r: float, q: float, sigma: float,
        d1: float, d2: float, option_type: OptionType
    ) -> float:
        """Calculate charm (dDelta/dTime)."""
        term1 = -q * np.exp(-q * T) * norm.cdf(d1 if option_type == OptionType.CALL else -d1)
        term2 = np.exp(-q * T) * norm.pdf(d1) * (2 * (r - q) * T - d2 * sigma * np.sqrt(T)) / (2 * T * sigma * np.sqrt(T))
        sign = 1 if option_type == OptionType.CALL else -1
        return sign * (term1 + term2) / 365  # Per day
    
    def _calculate_vomma(
        self, S: float, T: float, q: float, sigma: float, d1: float
    ) -> float:
        """Calculate vomma (dVega/dVol)."""
        d2 = d1 - sigma * np.sqrt(T)
        return S * np.exp(-q * T) * norm.pdf(d1) * np.sqrt(T) * d1 * d2 / sigma
    
    def _calculate_speed(
        self, S: float, T: float, q: float, sigma: float, d1: float
    ) -> float:
        """Calculate speed (dGamma/dSpot)."""
        return -np.exp(-q * T) * norm.pdf(d1) * (d1 + sigma * np.sqrt(T)) / (S ** 2 * sigma * np.sqrt(T))
    
    def _calculate_expired_greeks(
        self, S: float, K: float, option_type: OptionType
    ) -> Greeks:
        """Calculate Greeks for expired option."""
        if option_type == OptionType.CALL:
            delta = Decimal("1") if S > K else Decimal("0")
        else:  # PUT
            delta = Decimal("-1") if S < K else Decimal("0")
        
        return Greeks(
            delta=delta,
            gamma=Decimal("0"),
            theta=Decimal("0"),
            vega=Decimal("0"),
            rho=Decimal("0"),
            lambda_=Decimal("0")
        )
    
    def _calculate_zero_vol_greeks(
        self, S: float, K: float, T: float, r: float, q: float, option_type: OptionType
    ) -> Greeks:
        """Calculate Greeks when volatility is zero."""
        forward = S * np.exp((r - q) * T)
        
        if option_type == OptionType.CALL:
            delta = Decimal("1") if forward > K else Decimal("0")
            value = max(0, forward - K) * np.exp(-r * T)
        else:  # PUT
            delta = Decimal("-1") if forward < K else Decimal("0")
            value = max(0, K - forward) * np.exp(-r * T)
        
        # Most Greeks are zero or undefined with zero volatility
        return Greeks(
            delta=delta,
            gamma=Decimal("0"),
            theta=Decimal(str(-r * value / 365)) if value > 0 else Decimal("0"),
            vega=Decimal("0"),
            rho=Decimal(str(T * value / 100)) if value > 0 else Decimal("0"),
            lambda_=Decimal(str(float(delta) * S / value)) if value > 0 else Decimal("0")
        ) 