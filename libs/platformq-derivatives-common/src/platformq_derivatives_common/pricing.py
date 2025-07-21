"""Pricing engines for derivatives."""

import numpy as np
from decimal import Decimal
from typing import Optional, Tuple
from scipy.stats import norm
from enum import Enum
import logging

logger = logging.getLogger(__name__)


class OptionType(Enum):
    """Option types."""
    CALL = "call"
    PUT = "put"


class OptionStyle(Enum):
    """Option exercise styles."""
    EUROPEAN = "european"
    AMERICAN = "american"
    BERMUDAN = "bermudan"


class BlackScholesEngine:
    """Black-Scholes pricing engine for European options."""
    
    def __init__(self, precision: int = 6):
        """Initialize Black-Scholes engine.
        
        Args:
            precision: Decimal precision for calculations
        """
        self.precision = precision
    
    def calculate_price(
        self,
        spot: Decimal,
        strike: Decimal,
        time_to_expiry: Decimal,
        volatility: Decimal,
        risk_free_rate: Decimal,
        dividend_yield: Decimal,
        option_type: OptionType
    ) -> Decimal:
        """Calculate option price using Black-Scholes formula.
        
        Args:
            spot: Current spot price
            strike: Strike price
            time_to_expiry: Time to expiry in years
            volatility: Implied volatility (annualized)
            risk_free_rate: Risk-free rate
            dividend_yield: Dividend yield
            option_type: Call or Put
            
        Returns:
            Option price
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
            if option_type == OptionType.CALL:
                return Decimal(str(max(0, S - K)))
            else:
                return Decimal(str(max(0, K - S)))
        
        if sigma <= 0:
            # No volatility - deterministic
            forward = S * np.exp((r - q) * T)
            if option_type == OptionType.CALL:
                return Decimal(str(max(0, forward - K) * np.exp(-r * T)))
            else:
                return Decimal(str(max(0, K - forward) * np.exp(-r * T)))
        
        # Calculate d1 and d2
        d1 = (np.log(S / K) + (r - q + 0.5 * sigma ** 2) * T) / (sigma * np.sqrt(T))
        d2 = d1 - sigma * np.sqrt(T)
        
        # Calculate option price
        if option_type == OptionType.CALL:
            price = S * np.exp(-q * T) * norm.cdf(d1) - K * np.exp(-r * T) * norm.cdf(d2)
        else:  # PUT
            price = K * np.exp(-r * T) * norm.cdf(-d2) - S * np.exp(-q * T) * norm.cdf(-d1)
        
        return Decimal(str(price)).quantize(Decimal(10) ** -self.precision)
    
    def calculate_implied_volatility(
        self,
        option_price: Decimal,
        spot: Decimal,
        strike: Decimal,
        time_to_expiry: Decimal,
        risk_free_rate: Decimal,
        dividend_yield: Decimal,
        option_type: OptionType,
        max_iterations: int = 100,
        tolerance: float = 1e-6
    ) -> Optional[Decimal]:
        """Calculate implied volatility using Newton-Raphson method.
        
        Args:
            option_price: Observed option price
            spot: Current spot price
            strike: Strike price
            time_to_expiry: Time to expiry in years
            risk_free_rate: Risk-free rate
            dividend_yield: Dividend yield
            option_type: Call or Put
            max_iterations: Maximum iterations for convergence
            tolerance: Convergence tolerance
            
        Returns:
            Implied volatility, or None if cannot converge
        """
        # Initial guess using Brenner-Subrahmanyam approximation
        S = float(spot)
        K = float(strike)
        T = float(time_to_expiry)
        C = float(option_price)
        
        # Starting volatility guess
        sigma = np.sqrt(2 * np.pi / T) * C / S
        
        # Ensure reasonable bounds
        sigma = max(0.01, min(5.0, sigma))
        
        for i in range(max_iterations):
            # Calculate option price and vega
            price = self.calculate_price(
                spot, strike, time_to_expiry, Decimal(str(sigma)),
                risk_free_rate, dividend_yield, option_type
            )
            
            # Calculate vega
            d1 = (np.log(S / K) + (float(risk_free_rate) - float(dividend_yield) + 0.5 * sigma ** 2) * T) / (sigma * np.sqrt(T))
            vega = S * np.exp(-float(dividend_yield) * T) * norm.pdf(d1) * np.sqrt(T)
            
            # Price difference
            price_diff = float(price) - C
            
            # Check convergence
            if abs(price_diff) < tolerance:
                return Decimal(str(sigma)).quantize(Decimal(10) ** -self.precision)
            
            # Newton-Raphson update
            if vega > 1e-10:  # Avoid division by zero
                sigma = sigma - price_diff / vega
                sigma = max(0.001, min(10.0, sigma))  # Keep in reasonable bounds
            else:
                # Vega too small, use bisection fallback
                return self._implied_vol_bisection(
                    option_price, spot, strike, time_to_expiry,
                    risk_free_rate, dividend_yield, option_type
                )
        
        # Failed to converge
        logger.warning(f"Implied volatility failed to converge after {max_iterations} iterations")
        return None
    
    def _implied_vol_bisection(
        self,
        option_price: Decimal,
        spot: Decimal,
        strike: Decimal,
        time_to_expiry: Decimal,
        risk_free_rate: Decimal,
        dividend_yield: Decimal,
        option_type: OptionType,
        tolerance: float = 1e-6
    ) -> Optional[Decimal]:
        """Bisection method fallback for implied volatility."""
        low_vol = 0.001
        high_vol = 5.0
        
        for _ in range(100):
            mid_vol = (low_vol + high_vol) / 2
            price = self.calculate_price(
                spot, strike, time_to_expiry, Decimal(str(mid_vol)),
                risk_free_rate, dividend_yield, option_type
            )
            
            if abs(float(price) - float(option_price)) < tolerance:
                return Decimal(str(mid_vol)).quantize(Decimal(10) ** -self.precision)
            
            if float(price) < float(option_price):
                low_vol = mid_vol
            else:
                high_vol = mid_vol
        
        return None


class BinomialEngine:
    """Binomial tree pricing engine for American and Bermudan options."""
    
    def __init__(self, steps: int = 100, precision: int = 6):
        """Initialize Binomial engine.
        
        Args:
            steps: Number of time steps in the tree
            precision: Decimal precision for calculations
        """
        self.steps = steps
        self.precision = precision
    
    def calculate_price(
        self,
        spot: Decimal,
        strike: Decimal,
        time_to_expiry: Decimal,
        volatility: Decimal,
        risk_free_rate: Decimal,
        dividend_yield: Decimal,
        option_type: OptionType,
        option_style: OptionStyle = OptionStyle.AMERICAN,
        exercise_dates: Optional[list] = None
    ) -> Decimal:
        """Calculate option price using binomial tree.
        
        Args:
            spot: Current spot price
            strike: Strike price
            time_to_expiry: Time to expiry in years
            volatility: Implied volatility (annualized)
            risk_free_rate: Risk-free rate
            dividend_yield: Dividend yield
            option_type: Call or Put
            option_style: European, American, or Bermudan
            exercise_dates: List of exercise dates for Bermudan options
            
        Returns:
            Option price
        """
        S = float(spot)
        K = float(strike)
        T = float(time_to_expiry)
        sigma = float(volatility)
        r = float(risk_free_rate)
        q = float(dividend_yield)
        
        # Calculate parameters
        dt = T / self.steps
        u = np.exp(sigma * np.sqrt(dt))  # Up move
        d = 1 / u  # Down move
        p = (np.exp((r - q) * dt) - d) / (u - d)  # Risk-neutral probability
        
        # Initialize asset prices at maturity
        asset_prices = np.zeros(self.steps + 1)
        for i in range(self.steps + 1):
            asset_prices[i] = S * (u ** (self.steps - i)) * (d ** i)
        
        # Initialize option values at maturity
        option_values = np.zeros(self.steps + 1)
        for i in range(self.steps + 1):
            if option_type == OptionType.CALL:
                option_values[i] = max(0, asset_prices[i] - K)
            else:  # PUT
                option_values[i] = max(0, K - asset_prices[i])
        
        # Backward induction
        for step in range(self.steps - 1, -1, -1):
            for i in range(step + 1):
                # Asset price at this node
                asset_price = S * (u ** (step - i)) * (d ** i)
                
                # Continuation value
                continuation = np.exp(-r * dt) * (p * option_values[i] + (1 - p) * option_values[i + 1])
                
                # Exercise value
                if option_type == OptionType.CALL:
                    exercise = max(0, asset_price - K)
                else:  # PUT
                    exercise = max(0, K - asset_price)
                
                # Option value depends on style
                if option_style == OptionStyle.EUROPEAN:
                    option_values[i] = continuation
                elif option_style == OptionStyle.AMERICAN:
                    option_values[i] = max(continuation, exercise)
                elif option_style == OptionStyle.BERMUDAN:
                    # Check if current time is an exercise date
                    current_time = step * dt
                    can_exercise = self._is_exercise_date(current_time, exercise_dates, T)
                    if can_exercise:
                        option_values[i] = max(continuation, exercise)
                    else:
                        option_values[i] = continuation
        
        return Decimal(str(option_values[0])).quantize(Decimal(10) ** -self.precision)
    
    def _is_exercise_date(
        self,
        current_time: float,
        exercise_dates: Optional[list],
        maturity: float
    ) -> bool:
        """Check if current time is an exercise date for Bermudan option."""
        if exercise_dates is None:
            return True  # Default to American if no dates specified
        
        # Convert exercise dates to time fractions
        for date_fraction in exercise_dates:
            if abs(current_time - date_fraction * maturity) < 1e-6:
                return True
        
        return current_time >= maturity - 1e-6  # Always can exercise at maturity


class MonteCarloEngine:
    """Monte Carlo pricing engine for exotic options."""
    
    def __init__(self, simulations: int = 10000, precision: int = 6, seed: Optional[int] = None):
        """Initialize Monte Carlo engine.
        
        Args:
            simulations: Number of Monte Carlo paths
            precision: Decimal precision for calculations
            seed: Random seed for reproducibility
        """
        self.simulations = simulations
        self.precision = precision
        if seed is not None:
            np.random.seed(seed)
    
    def calculate_price(
        self,
        spot: Decimal,
        strike: Decimal,
        time_to_expiry: Decimal,
        volatility: Decimal,
        risk_free_rate: Decimal,
        dividend_yield: Decimal,
        option_type: OptionType,
        option_style: OptionStyle = OptionStyle.EUROPEAN,
        path_dependent: bool = False,
        barrier: Optional[Decimal] = None,
        barrier_type: Optional[str] = None
    ) -> Tuple[Decimal, Decimal]:
        """Calculate option price using Monte Carlo simulation.
        
        Args:
            spot: Current spot price
            strike: Strike price
            time_to_expiry: Time to expiry in years
            volatility: Implied volatility (annualized)
            risk_free_rate: Risk-free rate
            dividend_yield: Dividend yield
            option_type: Call or Put
            option_style: European or American (American uses Longstaff-Schwartz)
            path_dependent: Whether option is path-dependent
            barrier: Barrier level for barrier options
            barrier_type: Type of barrier (up-in, up-out, down-in, down-out)
            
        Returns:
            Tuple of (option price, standard error)
        """
        S = float(spot)
        K = float(strike)
        T = float(time_to_expiry)
        sigma = float(volatility)
        r = float(risk_free_rate)
        q = float(dividend_yield)
        
        # Time steps for path simulation
        steps = max(1, int(252 * T)) if path_dependent else 1
        dt = T / steps
        
        # Generate random paths
        payoffs = np.zeros(self.simulations)
        
        for i in range(self.simulations):
            path = self._generate_path(S, r, q, sigma, T, steps)
            
            # Calculate payoff based on option type and features
            if barrier is not None:
                payoff = self._barrier_payoff(
                    path, K, float(barrier), barrier_type, option_type
                )
            elif option_style == OptionStyle.AMERICAN and steps > 1:
                payoff = self._american_payoff(
                    path, K, r, dt, option_type
                )
            else:
                # European payoff
                final_price = path[-1]
                if option_type == OptionType.CALL:
                    payoff = max(0, final_price - K)
                else:  # PUT
                    payoff = max(0, K - final_price)
            
            payoffs[i] = payoff
        
        # Discount payoffs
        discount_factor = np.exp(-r * T)
        discounted_payoffs = payoffs * discount_factor
        
        # Calculate price and standard error
        price = np.mean(discounted_payoffs)
        std_error = np.std(discounted_payoffs) / np.sqrt(self.simulations)
        
        return (
            Decimal(str(price)).quantize(Decimal(10) ** -self.precision),
            Decimal(str(std_error)).quantize(Decimal(10) ** -self.precision)
        )
    
    def _generate_path(
        self,
        S0: float,
        r: float,
        q: float,
        sigma: float,
        T: float,
        steps: int
    ) -> np.ndarray:
        """Generate a single asset price path."""
        dt = T / steps
        path = np.zeros(steps + 1)
        path[0] = S0
        
        # Generate Brownian motion increments
        z = np.random.standard_normal(steps)
        
        for i in range(1, steps + 1):
            path[i] = path[i-1] * np.exp(
                (r - q - 0.5 * sigma ** 2) * dt + sigma * np.sqrt(dt) * z[i-1]
            )
        
        return path
    
    def _barrier_payoff(
        self,
        path: np.ndarray,
        strike: float,
        barrier: float,
        barrier_type: str,
        option_type: OptionType
    ) -> float:
        """Calculate payoff for barrier option."""
        final_price = path[-1]
        
        # Check if barrier was hit
        if barrier_type == "up-out":
            if np.any(path >= barrier):
                return 0.0
        elif barrier_type == "down-out":
            if np.any(path <= barrier):
                return 0.0
        elif barrier_type == "up-in":
            if not np.any(path >= barrier):
                return 0.0
        elif barrier_type == "down-in":
            if not np.any(path <= barrier):
                return 0.0
        
        # Standard payoff if barrier conditions met
        if option_type == OptionType.CALL:
            return max(0, final_price - strike)
        else:  # PUT
            return max(0, strike - final_price)
    
    def _american_payoff(
        self,
        path: np.ndarray,
        strike: float,
        r: float,
        dt: float,
        option_type: OptionType
    ) -> float:
        """Calculate payoff for American option using regression approach."""
        # Simplified - for production use Longstaff-Schwartz algorithm
        steps = len(path) - 1
        
        # Calculate exercise value at each step
        exercise_values = np.zeros(steps + 1)
        for i in range(steps + 1):
            if option_type == OptionType.CALL:
                exercise_values[i] = max(0, path[i] - strike)
            else:  # PUT
                exercise_values[i] = max(0, strike - path[i])
        
        # Backward induction (simplified)
        option_value = exercise_values[-1]
        
        for i in range(steps - 1, -1, -1):
            continuation = option_value * np.exp(-r * dt)
            exercise = exercise_values[i]
            option_value = max(continuation, exercise)
        
        return option_value 