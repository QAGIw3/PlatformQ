"""Common risk calculation utilities."""

from decimal import Decimal
from typing import List, Optional, Tuple
import numpy as np
from scipy import stats


def calculate_var(
    returns: List[float],
    confidence_level: float = 0.95,
    method: str = "historical"
) -> float:
    """
    Calculate Value at Risk (VaR).
    
    Args:
        returns: List of returns
        confidence_level: Confidence level (e.g., 0.95 for 95% VaR)
        method: Method to use ('historical', 'parametric', 'montecarlo')
        
    Returns:
        VaR value
    """
    if not returns:
        return 0.0
        
    returns_array = np.array(returns)
    
    if method == "historical":
        # Historical simulation
        var_percentile = (1 - confidence_level) * 100
        return -np.percentile(returns_array, var_percentile)
        
    elif method == "parametric":
        # Parametric (variance-covariance) method
        mean = np.mean(returns_array)
        std = np.std(returns_array)
        z_score = stats.norm.ppf(1 - confidence_level)
        return -(mean + z_score * std)
        
    else:
        raise ValueError(f"Unknown VaR method: {method}")


def calculate_cvar(
    returns: List[float],
    confidence_level: float = 0.95,
    var_value: Optional[float] = None
) -> float:
    """
    Calculate Conditional Value at Risk (CVaR), also known as Expected Shortfall.
    
    Args:
        returns: List of returns
        confidence_level: Confidence level
        var_value: Pre-calculated VaR value (optional)
        
    Returns:
        CVaR value
    """
    if not returns:
        return 0.0
        
    returns_array = np.array(returns)
    
    if var_value is None:
        var_value = calculate_var(returns, confidence_level)
    
    # Get returns worse than VaR
    worse_returns = returns_array[returns_array <= -var_value]
    
    if len(worse_returns) == 0:
        return var_value
        
    return -np.mean(worse_returns)


def calculate_sharpe_ratio(
    returns: List[float],
    risk_free_rate: float = 0.0,
    periods_per_year: int = 252
) -> float:
    """
    Calculate Sharpe Ratio.
    
    Args:
        returns: List of returns
        risk_free_rate: Annual risk-free rate
        periods_per_year: Number of periods per year (252 for daily, 52 for weekly)
        
    Returns:
        Sharpe ratio
    """
    if not returns or len(returns) < 2:
        return 0.0
        
    returns_array = np.array(returns)
    
    # Calculate excess returns
    excess_returns = returns_array - (risk_free_rate / periods_per_year)
    
    # Annualized Sharpe ratio
    mean_excess = np.mean(excess_returns) * periods_per_year
    std_excess = np.std(excess_returns) * np.sqrt(periods_per_year)
    
    if std_excess == 0:
        return 0.0
        
    return mean_excess / std_excess


def calculate_portfolio_beta(
    portfolio_returns: List[float],
    market_returns: List[float]
) -> float:
    """
    Calculate portfolio beta relative to market.
    
    Args:
        portfolio_returns: List of portfolio returns
        market_returns: List of market returns
        
    Returns:
        Portfolio beta
    """
    if not portfolio_returns or not market_returns:
        return 0.0
        
    if len(portfolio_returns) != len(market_returns):
        raise ValueError("Portfolio and market returns must have same length")
        
    portfolio_array = np.array(portfolio_returns)
    market_array = np.array(market_returns)
    
    # Calculate covariance and market variance
    covariance = np.cov(portfolio_array, market_array)[0, 1]
    market_variance = np.var(market_array)
    
    if market_variance == 0:
        return 0.0
        
    return covariance / market_variance


def calculate_max_drawdown(values: List[float]) -> Tuple[float, int, int]:
    """
    Calculate maximum drawdown.
    
    Args:
        values: List of portfolio values or cumulative returns
        
    Returns:
        Tuple of (max_drawdown, peak_idx, trough_idx)
    """
    if not values:
        return 0.0, 0, 0
        
    values_array = np.array(values)
    cumulative = np.cumprod(1 + values_array / 100)
    running_max = np.maximum.accumulate(cumulative)
    drawdown = (cumulative - running_max) / running_max
    
    max_drawdown = np.min(drawdown)
    trough_idx = np.argmin(drawdown)
    
    # Find the peak before the trough
    peak_idx = np.argmax(cumulative[:trough_idx + 1])
    
    return -max_drawdown, peak_idx, trough_idx


def calculate_correlation_matrix(returns_dict: dict) -> np.ndarray:
    """
    Calculate correlation matrix for multiple assets.
    
    Args:
        returns_dict: Dictionary mapping asset names to return lists
        
    Returns:
        Correlation matrix
    """
    if not returns_dict:
        return np.array([])
        
    assets = list(returns_dict.keys())
    n_assets = len(assets)
    
    # Convert to numpy array
    returns_matrix = np.array([returns_dict[asset] for asset in assets])
    
    # Calculate correlation matrix
    correlation_matrix = np.corrcoef(returns_matrix)
    
    return correlation_matrix


def calculate_tracking_error(
    portfolio_returns: List[float],
    benchmark_returns: List[float],
    periods_per_year: int = 252
) -> float:
    """
    Calculate tracking error (standard deviation of excess returns).
    
    Args:
        portfolio_returns: List of portfolio returns
        benchmark_returns: List of benchmark returns
        periods_per_year: Number of periods per year
        
    Returns:
        Annualized tracking error
    """
    if not portfolio_returns or not benchmark_returns:
        return 0.0
        
    if len(portfolio_returns) != len(benchmark_returns):
        raise ValueError("Portfolio and benchmark returns must have same length")
        
    portfolio_array = np.array(portfolio_returns)
    benchmark_array = np.array(benchmark_returns)
    
    # Calculate excess returns
    excess_returns = portfolio_array - benchmark_array
    
    # Annualized tracking error
    return np.std(excess_returns) * np.sqrt(periods_per_year)


def calculate_information_ratio(
    portfolio_returns: List[float],
    benchmark_returns: List[float],
    periods_per_year: int = 252
) -> float:
    """
    Calculate information ratio.
    
    Args:
        portfolio_returns: List of portfolio returns
        benchmark_returns: List of benchmark returns
        periods_per_year: Number of periods per year
        
    Returns:
        Information ratio
    """
    if not portfolio_returns or not benchmark_returns:
        return 0.0
        
    tracking_error = calculate_tracking_error(
        portfolio_returns, benchmark_returns, periods_per_year
    )
    
    if tracking_error == 0:
        return 0.0
        
    portfolio_array = np.array(portfolio_returns)
    benchmark_array = np.array(benchmark_returns)
    
    # Calculate excess returns
    excess_returns = portfolio_array - benchmark_array
    mean_excess = np.mean(excess_returns) * periods_per_year
    
    return mean_excess / tracking_error 