"""
Resilience patterns for DataIntelligenceSuite services.

This module re-exports resilience patterns from the consolidated core.patterns.resilience module
for backward compatibility.
"""

# Re-export everything from the consolidated resilience module
from ..core.patterns.resilience import (
    # States and Errors
    CircuitState,
    CircuitBreakerError,
    
    # Configurations
    RetryConfig,
    CircuitBreakerConfig,
    BulkheadConfig,
    TimeoutConfig,
    FallbackConfig,
    RateLimitConfig,
    ResilienceConfig,
    
    # Patterns
    RetryPattern,
    CircuitBreakerPattern,
    BulkheadPattern,
    TimeoutPattern,
    FallbackPattern,
    RateLimiter,
    ResiliencePolicy,
    
    # Decorators
    retry,
    circuit_breaker,
    bulkhead,
    timeout,
    fallback,
    rate_limit,
    resilient
)

# For backward compatibility, create aliases for old class names
CircuitBreaker = CircuitBreakerPattern
RetryPolicy = RetryPattern
Bulkhead = BulkheadPattern
Timeout = TimeoutPattern
Fallback = FallbackPattern

# Re-export ResilienceManager for backward compatibility
from typing import Dict, Optional, Union
from datetime import timedelta


class ResilienceManager:
    """
    Manager for resilience patterns.
    
    This is a compatibility wrapper around the new resilience patterns.
    """
    
    def __init__(self, service_name: str):
        self.service_name = service_name
        self.circuit_breakers: Dict[str, CircuitBreakerPattern] = {}
        self.rate_limiters: Dict[str, RateLimiter] = {}
        self.bulkheads: Dict[str, BulkheadPattern] = {}
        
    def get_circuit_breaker(self, name: str, config: Optional[CircuitBreakerConfig] = None) -> CircuitBreakerPattern:
        """Get or create circuit breaker"""
        if name not in self.circuit_breakers:
            self.circuit_breakers[name] = CircuitBreakerPattern(config or CircuitBreakerConfig())
        return self.circuit_breakers[name]
        
    def get_rate_limiter(self, name: str, max_calls: int = 100, period: timedelta = timedelta(minutes=1)) -> RateLimiter:
        """Get or create rate limiter"""
        if name not in self.rate_limiters:
            config = RateLimitConfig(max_calls=max_calls, period=period)
            self.rate_limiters[name] = RateLimiter(config)
        return self.rate_limiters[name]
        
    def get_bulkhead(self, name: str, max_concurrent: int = 10) -> BulkheadPattern:
        """Get or create bulkhead"""
        if name not in self.bulkheads:
            config = BulkheadConfig(max_concurrent=max_concurrent)
            self.bulkheads[name] = BulkheadPattern(config)
        return self.bulkheads[name]
        
    def with_resilience(self,
                       circuit_breaker: Optional[str] = None,
                       rate_limiter: Optional[str] = None,
                       retry: Optional[RetryConfig] = None,
                       bulkhead: Optional[str] = None,
                       timeout: Optional[Union[float, timedelta]] = None):
        """Decorator to apply multiple resilience patterns"""
        def decorator(func):
            wrapped = func
            
            # Apply patterns in order
            if timeout:
                if isinstance(timeout, (int, float)):
                    timeout_td = timedelta(seconds=timeout)
                else:
                    timeout_td = timeout
                wrapped = TimeoutPattern(TimeoutConfig(timeout=timeout_td)).execute_async
                
            if retry:
                wrapped = RetryPattern(retry).execute_async
                
            if bulkhead:
                bh = self.get_bulkhead(bulkhead)
                wrapped = bh.execute_async
                
            if rate_limiter:
                rl = self.get_rate_limiter(rate_limiter)
                # Note: This is simplified - real implementation would need to handle rate limiting properly
                wrapped = wrapped
                
            if circuit_breaker:
                cb = self.get_circuit_breaker(circuit_breaker)
                wrapped = cb.execute_async
                
            return wrapped
            
        return decorator
        
    def get_health_status(self) -> Dict[str, any]:
        """Get health status of all resilience components"""
        return {
            "circuit_breakers": {
                name: cb.get_state() 
                for name, cb in self.circuit_breakers.items()
            },
            "bulkheads": {
                name: bh.get_metrics()
                for name, bh in self.bulkheads.items()
            }
        }


# Export all for backward compatibility
__all__ = [
    # States and Errors
    "CircuitState",
    "CircuitBreakerError",
    
    # Configurations
    "RetryConfig",
    "CircuitBreakerConfig",
    "BulkheadConfig", 
    "TimeoutConfig",
    "FallbackConfig",
    "RateLimitConfig",
    "ResilienceConfig",
    
    # Patterns (with aliases)
    "RetryPattern",
    "RetryPolicy",
    "CircuitBreakerPattern",
    "CircuitBreaker",
    "BulkheadPattern",
    "Bulkhead",
    "TimeoutPattern",
    "Timeout",
    "FallbackPattern",
    "Fallback",
    "RateLimiter",
    "ResiliencePolicy",
    
    # Decorators
    "retry",
    "circuit_breaker",
    "bulkhead",
    "timeout",
    "fallback",
    "rate_limit",
    "resilient",
    
    # Manager
    "ResilienceManager"
] 