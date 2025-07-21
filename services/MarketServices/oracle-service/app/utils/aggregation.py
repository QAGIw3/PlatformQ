"""
Measurement Aggregation Utilities
"""
import numpy as np
from typing import List, Union, Tuple
from scipy import stats


def aggregate_measurements(
    values: List[float],
    method: str = "median"
) -> float:
    """
    Aggregate multiple measurements using specified method
    
    Args:
        values: List of measurement values
        method: Aggregation method (median, mean, weighted)
    
    Returns:
        Aggregated value
    """
    if not values:
        return 0.0
    
    if method == "median":
        return float(np.median(values))
    elif method == "mean":
        return float(np.mean(values))
    elif method == "weighted":
        # Weight recent measurements more heavily
        weights = np.linspace(0.5, 1.0, len(values))
        weights = weights / weights.sum()
        return float(np.average(values, weights=weights))
    else:
        # Default to median
        return float(np.median(values))


def detect_outliers(
    values: List[float],
    z_threshold: float = 3.0
) -> List[float]:
    """
    Detect and remove outliers using z-score method
    
    Args:
        values: List of measurement values
        z_threshold: Z-score threshold for outlier detection
    
    Returns:
        List of values with outliers removed
    """
    if len(values) < 3:
        return values
    
    # Calculate z-scores
    z_scores = np.abs(stats.zscore(values))
    
    # Filter outliers
    return [v for v, z in zip(values, z_scores) if z < z_threshold]


def calculate_confidence_interval(
    values: List[float],
    confidence_level: float = 0.95
) -> Tuple[float, float]:
    """
    Calculate confidence interval for measurements
    
    Args:
        values: List of measurement values
        confidence_level: Confidence level (e.g., 0.95 for 95%)
    
    Returns:
        Tuple of (lower_bound, upper_bound)
    """
    if not values:
        return (0.0, 0.0)
    
    if len(values) == 1:
        return (values[0], values[0])
    
    # Calculate mean and standard error
    mean = np.mean(values)
    std_err = stats.sem(values)
    
    # Calculate confidence interval
    interval = stats.t.interval(
        confidence_level,
        len(values) - 1,
        loc=mean,
        scale=std_err
    )
    
    return (float(interval[0]), float(interval[1]))


def calculate_trend(
    values: List[float],
    timestamps: List[float] = None
) -> str:
    """
    Calculate trend direction from time series data
    
    Args:
        values: List of measurement values
        timestamps: Optional list of timestamps (uses indices if not provided)
    
    Returns:
        Trend direction: "improving", "degrading", "stable"
    """
    if len(values) < 3:
        return "stable"
    
    # Use indices if timestamps not provided
    if timestamps is None:
        timestamps = list(range(len(values)))
    
    # Fit linear regression
    slope, _, r_value, _, _ = stats.linregress(timestamps, values)
    
    # Determine trend based on slope and correlation
    if abs(r_value) < 0.5:
        return "stable"
    elif slope > 0:
        return "degrading"  # Higher values are worse for most metrics
    else:
        return "improving"


def exponential_moving_average(
    values: List[float],
    alpha: float = 0.3
) -> List[float]:
    """
    Calculate exponential moving average
    
    Args:
        values: List of measurement values
        alpha: Smoothing factor (0 < alpha < 1)
    
    Returns:
        List of EMA values
    """
    if not values:
        return []
    
    ema = [values[0]]
    for i in range(1, len(values)):
        ema.append(alpha * values[i] + (1 - alpha) * ema[i-1])
    
    return ema


def weighted_quality_score(
    component_scores: dict,
    weights: dict = None
) -> float:
    """
    Calculate weighted quality score from components
    
    Args:
        component_scores: Dictionary of component scores
        weights: Optional dictionary of weights (must sum to 1)
    
    Returns:
        Weighted overall score
    """
    if not component_scores:
        return 0.0
    
    # Use equal weights if not provided
    if weights is None:
        weights = {k: 1.0 / len(component_scores) for k in component_scores}
    
    # Normalize weights
    total_weight = sum(weights.values())
    weights = {k: v / total_weight for k, v in weights.items()}
    
    # Calculate weighted score
    score = 0.0
    for component, value in component_scores.items():
        weight = weights.get(component, 0.0)
        score += value * weight
    
    return score


def calculate_percentile_score(
    value: float,
    reference_values: List[float],
    inverse: bool = False
) -> float:
    """
    Calculate percentile-based score
    
    Args:
        value: Value to score
        reference_values: Reference distribution
        inverse: If True, lower values are better
    
    Returns:
        Percentile score (0-100)
    """
    if not reference_values:
        return 50.0
    
    percentile = stats.percentileofscore(reference_values, value)
    
    if inverse:
        return 100 - percentile
    else:
        return percentile 