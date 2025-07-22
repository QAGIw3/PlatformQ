"""SIMD-optimized operations for risk calculations using NumPy and Numba."""

import numpy as np
import numba
from numba import cuda, vectorize, guvectorize, float32, float64, int32
import logging
from typing import Tuple, Optional
from decimal import Decimal

logger = logging.getLogger(__name__)

# Enable parallel execution
numba.config.THREADING_LAYER = 'threadsafe'


@vectorize([float32(float32, float32), float64(float64, float64)], target='parallel')
def simd_pnl_calculation(position_size, price_diff):
    """
    SIMD vectorized P&L calculation.
    Calculates position_size * price_diff for arrays.
    """
    return position_size * price_diff


@vectorize([float32(float32, float32, float32), float64(float64, float64, float64)], target='parallel')
def simd_margin_ratio(equity, margin_used, min_margin):
    """
    SIMD vectorized margin ratio calculation.
    Returns (equity / margin_used) * 100 with floor at min_margin.
    """
    if margin_used > 0:
        ratio = (equity / margin_used) * 100.0
        return max(ratio, min_margin)
    return 999999.0  # Infinite margin


@numba.jit(nopython=True, parallel=True, fastmath=True)
def simd_portfolio_var(returns: np.ndarray, weights: np.ndarray, confidence_level: float = 0.95) -> float:
    """
    SIMD-optimized portfolio VaR calculation.
    
    Args:
        returns: Historical returns matrix (assets x time)
        weights: Portfolio weights
        confidence_level: VaR confidence level
        
    Returns:
        Portfolio VaR
    """
    # Calculate portfolio returns
    portfolio_returns = np.zeros(returns.shape[1])
    
    # Vectorized portfolio return calculation
    for i in numba.prange(returns.shape[1]):
        portfolio_returns[i] = np.dot(weights, returns[:, i])
    
    # Sort returns
    sorted_returns = np.sort(portfolio_returns)
    
    # Calculate VaR
    index = int((1 - confidence_level) * len(sorted_returns))
    return -sorted_returns[index]


@numba.jit(nopython=True, parallel=True, fastmath=True)
def simd_correlation_matrix(returns: np.ndarray) -> np.ndarray:
    """
    SIMD-optimized correlation matrix calculation.
    
    Args:
        returns: Returns matrix (assets x time)
        
    Returns:
        Correlation matrix
    """
    n_assets = returns.shape[0]
    n_periods = returns.shape[1]
    
    # Calculate means
    means = np.zeros(n_assets)
    for i in numba.prange(n_assets):
        means[i] = np.mean(returns[i, :])
    
    # Center the data
    centered = np.zeros_like(returns)
    for i in numba.prange(n_assets):
        centered[i, :] = returns[i, :] - means[i]
    
    # Calculate covariance matrix
    cov_matrix = np.zeros((n_assets, n_assets))
    for i in numba.prange(n_assets):
        for j in range(i, n_assets):
            cov = np.dot(centered[i, :], centered[j, :]) / (n_periods - 1)
            cov_matrix[i, j] = cov
            cov_matrix[j, i] = cov
    
    # Convert to correlation matrix
    corr_matrix = np.zeros((n_assets, n_assets))
    std_devs = np.sqrt(np.diag(cov_matrix))
    
    for i in numba.prange(n_assets):
        for j in range(n_assets):
            if std_devs[i] > 0 and std_devs[j] > 0:
                corr_matrix[i, j] = cov_matrix[i, j] / (std_devs[i] * std_devs[j])
            else:
                corr_matrix[i, j] = 0.0 if i != j else 1.0
    
    return corr_matrix


@guvectorize([(float64[:], float64[:], float64[:])], '(n),(n)->()', target='parallel')
def simd_weighted_sum(values, weights, result):
    """
    SIMD generalized ufunc for weighted sum.
    Useful for portfolio calculations.
    """
    total = 0.0
    for i in range(values.shape[0]):
        total += values[i] * weights[i]
    result[0] = total


@numba.jit(nopython=True, parallel=True, fastmath=True)
def simd_stress_test_batch(
    positions: np.ndarray,
    prices: np.ndarray,
    shock_scenarios: np.ndarray
) -> np.ndarray:
    """
    SIMD-optimized batch stress testing.
    
    Args:
        positions: Position sizes (n_positions)
        prices: Current prices (n_positions)
        shock_scenarios: Price shock scenarios (n_scenarios x n_positions)
        
    Returns:
        P&L for each scenario (n_scenarios)
    """
    n_scenarios = shock_scenarios.shape[0]
    n_positions = positions.shape[0]
    
    results = np.zeros(n_scenarios)
    
    for i in numba.prange(n_scenarios):
        pnl = 0.0
        for j in range(n_positions):
            shocked_price = prices[j] * (1 + shock_scenarios[i, j])
            pnl += positions[j] * (shocked_price - prices[j])
        results[i] = pnl
    
    return results


@numba.jit(nopython=True, parallel=True, fastmath=True)
def simd_liquidation_scan(
    margin_levels: np.ndarray,
    thresholds: np.ndarray,
    positions: np.ndarray,
    priorities: np.ndarray
) -> Tuple[np.ndarray, int]:
    """
    SIMD-optimized liquidation scanning.
    
    Args:
        margin_levels: Current margin levels
        thresholds: Liquidation thresholds
        positions: Position IDs
        priorities: Liquidation priorities (higher = liquidate first)
        
    Returns:
        Tuple of (positions to liquidate, count)
    """
    n = len(margin_levels)
    
    # Find positions below threshold
    liquidation_flags = np.zeros(n, dtype=np.bool_)
    
    for i in numba.prange(n):
        if margin_levels[i] < thresholds[i]:
            liquidation_flags[i] = True
    
    # Count liquidations
    count = np.sum(liquidation_flags)
    
    if count == 0:
        return np.array([], dtype=np.int32), 0
    
    # Extract positions to liquidate
    to_liquidate = np.zeros(count, dtype=np.int32)
    priorities_to_sort = np.zeros(count, dtype=np.float32)
    
    idx = 0
    for i in range(n):
        if liquidation_flags[i]:
            to_liquidate[idx] = positions[i]
            priorities_to_sort[idx] = priorities[i]
            idx += 1
    
    # Sort by priority (higher priority first)
    sort_indices = np.argsort(-priorities_to_sort)
    sorted_positions = to_liquidate[sort_indices]
    
    return sorted_positions, count


# GPU-accelerated functions (if CUDA available)
try:
    @cuda.jit
    def cuda_portfolio_risk(positions, prices, volatilities, correlations, output):
        """
        GPU-accelerated portfolio risk calculation.
        
        Args:
            positions: Position sizes
            prices: Current prices
            volatilities: Asset volatilities
            correlations: Correlation matrix
            output: Output array for risk metrics
        """
        idx = cuda.grid(1)
        
        if idx < positions.shape[0]:
            # Calculate position values
            position_value = positions[idx] * prices[idx]
            
            # Calculate individual VaR
            individual_var = position_value * volatilities[idx] * 2.33  # 99% VaR
            
            # Store in output
            output[idx, 0] = position_value
            output[idx, 1] = individual_var
            
    @cuda.jit
    def cuda_margin_check_batch(equity, margin_used, threshold, results):
        """
        GPU-accelerated batch margin checking.
        
        Args:
            equity: Equity values
            margin_used: Margin used values
            threshold: Margin threshold
            results: Output array (1 = margin call, 0 = ok)
        """
        idx = cuda.grid(1)
        
        if idx < equity.shape[0]:
            if margin_used[idx] > 0:
                margin_ratio = equity[idx] / margin_used[idx]
                results[idx] = 1 if margin_ratio < threshold else 0
            else:
                results[idx] = 0
                
    # Helper functions for GPU operations
    def gpu_calculate_portfolio_risk(positions, prices, volatilities, correlations):
        """
        Calculate portfolio risk on GPU.
        
        Returns:
            Array with [position_value, individual_var] for each position
        """
        n = len(positions)
        
        # Allocate device memory
        d_positions = cuda.to_device(positions)
        d_prices = cuda.to_device(prices)
        d_volatilities = cuda.to_device(volatilities)
        d_correlations = cuda.to_device(correlations)
        d_output = cuda.device_array((n, 2), dtype=np.float32)
        
        # Configure kernel
        threads_per_block = 256
        blocks = (n + threads_per_block - 1) // threads_per_block
        
        # Launch kernel
        cuda_portfolio_risk[blocks, threads_per_block](
            d_positions, d_prices, d_volatilities, d_correlations, d_output
        )
        
        # Copy result back
        return d_output.copy_to_host()
        
    def gpu_batch_margin_check(equity_array, margin_array, threshold=1.0):
        """
        Check margins for multiple accounts on GPU.
        
        Returns:
            Boolean array indicating margin calls
        """
        n = len(equity_array)
        
        # Allocate device memory
        d_equity = cuda.to_device(equity_array)
        d_margin = cuda.to_device(margin_array)
        d_results = cuda.device_array(n, dtype=np.int32)
        
        # Configure kernel
        threads_per_block = 256
        blocks = (n + threads_per_block - 1) // threads_per_block
        
        # Launch kernel
        cuda_margin_check_batch[blocks, threads_per_block](
            d_equity, d_margin, threshold, d_results
        )
        
        # Copy result back
        return d_results.copy_to_host().astype(bool)
        
    GPU_AVAILABLE = True
    logger.info("GPU acceleration available for risk calculations")
    
except:
    GPU_AVAILABLE = False
    logger.info("GPU acceleration not available, using CPU SIMD only")
    
    # Dummy functions if GPU not available
    def gpu_calculate_portfolio_risk(positions, prices, volatilities, correlations):
        raise NotImplementedError("GPU not available")
        
    def gpu_batch_margin_check(equity_array, margin_array, threshold=1.0):
        raise NotImplementedError("GPU not available")


class SIMDRiskCalculator:
    """
    High-level interface for SIMD-optimized risk calculations.
    """
    
    def __init__(self, use_gpu: bool = True):
        self.use_gpu = use_gpu and GPU_AVAILABLE
        
    def calculate_portfolio_var(self, 
                               returns: np.ndarray,
                               weights: np.ndarray,
                               confidence_level: float = 0.95) -> float:
        """Calculate portfolio VaR using SIMD operations."""
        return simd_portfolio_var(returns, weights, confidence_level)
        
    def calculate_pnl_batch(self,
                           positions: np.ndarray,
                           price_changes: np.ndarray) -> np.ndarray:
        """Calculate P&L for multiple positions using SIMD."""
        return simd_pnl_calculation(positions, price_changes)
        
    def check_margin_batch(self,
                          equity: np.ndarray,
                          margin_used: np.ndarray,
                          min_margin: float = 100.0) -> np.ndarray:
        """Check margin levels for multiple accounts."""
        if self.use_gpu and len(equity) > 1000:
            # Use GPU for large batches
            return gpu_batch_margin_check(equity, margin_used, min_margin / 100)
        else:
            # Use CPU SIMD
            min_margin_array = np.full_like(equity, min_margin)
            return simd_margin_ratio(equity, margin_used, min_margin_array)
            
    def stress_test_portfolio(self,
                            positions: np.ndarray,
                            prices: np.ndarray,
                            scenarios: np.ndarray) -> np.ndarray:
        """Run stress tests on portfolio."""
        return simd_stress_test_batch(positions, prices, scenarios)
        
    def scan_for_liquidations(self,
                            margin_levels: np.ndarray,
                            thresholds: np.ndarray,
                            position_ids: np.ndarray,
                            priorities: Optional[np.ndarray] = None) -> Tuple[np.ndarray, int]:
        """Scan for positions requiring liquidation."""
        if priorities is None:
            # Default priority based on margin level (lower margin = higher priority)
            priorities = 1.0 / (margin_levels + 0.01)
            
        return simd_liquidation_scan(margin_levels, thresholds, position_ids, priorities)
        
    def calculate_correlations(self, returns: np.ndarray) -> np.ndarray:
        """Calculate correlation matrix using SIMD."""
        return simd_correlation_matrix(returns) 