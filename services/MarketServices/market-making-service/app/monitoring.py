"""Monitoring and metrics for Market Making Service"""

from prometheus_client import Counter, Histogram, Gauge, CollectorRegistry
import time
from functools import wraps
from typing import Callable, Any

# Create registry
metrics_registry = CollectorRegistry()

# Define metrics
pool_operations = Counter(
    'market_making_pool_operations_total',
    'Total number of pool operations',
    ['operation', 'pool_type', 'status'],
    registry=metrics_registry
)

swap_volume = Counter(
    'market_making_swap_volume_total',
    'Total swap volume in USD',
    ['pool_id', 'token_in', 'token_out'],
    registry=metrics_registry
)

liquidity_gauge = Gauge(
    'market_making_liquidity_usd',
    'Current liquidity in USD',
    ['pool_id', 'pool_type'],
    registry=metrics_registry
)

strategy_pnl = Gauge(
    'market_making_strategy_pnl',
    'Strategy P&L in USD',
    ['strategy_id', 'strategy_type'],
    registry=metrics_registry
)

order_latency = Histogram(
    'market_making_order_latency_seconds',
    'Order execution latency',
    ['market', 'order_type'],
    registry=metrics_registry
)

active_strategies = Gauge(
    'market_making_active_strategies',
    'Number of active strategies',
    ['strategy_type'],
    registry=metrics_registry
)

mining_rewards = Counter(
    'market_making_mining_rewards_total',
    'Total mining rewards distributed',
    ['program_id', 'token'],
    registry=metrics_registry
)

fee_revenue = Counter(
    'market_making_fee_revenue_total',
    'Total fee revenue in USD',
    ['pool_id', 'fee_tier'],
    registry=metrics_registry
)


def setup_monitoring():
    """Initialize monitoring"""
    # Set initial values
    active_strategies.labels(strategy_type='grid').set(0)
    active_strategies.labels(strategy_type='arbitrage').set(0)
    active_strategies.labels(strategy_type='delta_neutral').set(0)
    active_strategies.labels(strategy_type='volatility').set(0)


def track_execution_time(metric: Histogram):
    """Decorator to track execution time"""
    def decorator(func: Callable) -> Callable:
        @wraps(func)
        async def async_wrapper(*args, **kwargs) -> Any:
            start_time = time.time()
            try:
                result = await func(*args, **kwargs)
                return result
            finally:
                execution_time = time.time() - start_time
                # Extract labels from kwargs if available
                labels = {}
                for label in ['market', 'order_type']:
                    if label in kwargs:
                        labels[label] = kwargs[label]
                metric.labels(**labels).observe(execution_time)
        
        @wraps(func)
        def sync_wrapper(*args, **kwargs) -> Any:
            start_time = time.time()
            try:
                result = func(*args, **kwargs)
                return result
            finally:
                execution_time = time.time() - start_time
                # Extract labels from kwargs if available
                labels = {}
                for label in ['market', 'order_type']:
                    if label in kwargs:
                        labels[label] = kwargs[label]
                metric.labels(**labels).observe(execution_time)
        
        # Return appropriate wrapper based on function type
        import asyncio
        if asyncio.iscoroutinefunction(func):
            return async_wrapper
        else:
            return sync_wrapper
    
    return decorator 