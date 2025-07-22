"""Circuit breaker pattern for fault tolerance."""

import time
import logging
from enum import Enum
from typing import Callable, Any, Optional
from dataclasses import dataclass, field
from datetime import datetime, timedelta
import asyncio

logger = logging.getLogger(__name__)


class CircuitState(Enum):
    """Circuit breaker states."""
    CLOSED = "closed"  # Normal operation
    OPEN = "open"      # Failing, reject calls
    HALF_OPEN = "half_open"  # Testing recovery


@dataclass
class CircuitBreakerConfig:
    """Circuit breaker configuration."""
    failure_threshold: int = 5  # Failures before opening
    recovery_timeout: float = 60.0  # Seconds before trying half-open
    expected_exception: type = Exception  # Exception type to catch
    name: str = "CircuitBreaker"
    
    # Advanced settings
    success_threshold: int = 2  # Successes needed to close from half-open
    failure_rate_threshold: float = 0.5  # Failure rate to open circuit
    min_throughput: int = 10  # Minimum calls before checking failure rate


@dataclass
class CircuitBreakerStats:
    """Circuit breaker statistics."""
    total_calls: int = 0
    successful_calls: int = 0
    failed_calls: int = 0
    rejected_calls: int = 0
    last_failure_time: Optional[float] = None
    consecutive_failures: int = 0
    consecutive_successes: int = 0
    state_changes: list = field(default_factory=list)
    
    @property
    def failure_rate(self) -> float:
        """Calculate failure rate."""
        if self.total_calls == 0:
            return 0.0
        return self.failed_calls / self.total_calls


class CircuitBreaker:
    """
    Circuit breaker for fault tolerance.
    
    Prevents cascading failures by temporarily blocking calls to failing services.
    """
    
    def __init__(self, config: CircuitBreakerConfig):
        self.config = config
        self.state = CircuitState.CLOSED
        self.stats = CircuitBreakerStats()
        self._last_attempt_time = None
        self._state_lock = asyncio.Lock()
        
    async def call(self, func: Callable, *args, **kwargs) -> Any:
        """
        Execute function with circuit breaker protection.
        
        Args:
            func: Async function to call
            *args: Function arguments
            **kwargs: Function keyword arguments
            
        Returns:
            Function result
            
        Raises:
            Exception: If circuit is open or function fails
        """
        async with self._state_lock:
            # Check if we should attempt the call
            if not await self._should_attempt_call():
                self.stats.rejected_calls += 1
                raise Exception(f"Circuit breaker {self.config.name} is OPEN")
            
        # Attempt the call
        try:
            result = await func(*args, **kwargs)
            await self._on_success()
            return result
            
        except self.config.expected_exception as e:
            await self._on_failure()
            raise e
            
    async def _should_attempt_call(self) -> bool:
        """Check if we should attempt a call."""
        current_time = time.time()
        
        if self.state == CircuitState.CLOSED:
            return True
            
        elif self.state == CircuitState.OPEN:
            # Check if we should transition to half-open
            if self._last_attempt_time and \
               current_time - self._last_attempt_time >= self.config.recovery_timeout:
                await self._transition_to_half_open()
                return True
            return False
            
        else:  # HALF_OPEN
            return True
            
    async def _on_success(self):
        """Handle successful call."""
        async with self._state_lock:
            self.stats.total_calls += 1
            self.stats.successful_calls += 1
            self.stats.consecutive_successes += 1
            self.stats.consecutive_failures = 0
            
            if self.state == CircuitState.HALF_OPEN:
                if self.stats.consecutive_successes >= self.config.success_threshold:
                    await self._transition_to_closed()
                    
    async def _on_failure(self):
        """Handle failed call."""
        async with self._state_lock:
            self.stats.total_calls += 1
            self.stats.failed_calls += 1
            self.stats.consecutive_failures += 1
            self.stats.consecutive_successes = 0
            self.stats.last_failure_time = time.time()
            self._last_attempt_time = time.time()
            
            if self.state == CircuitState.CLOSED:
                # Check failure threshold
                if self.stats.consecutive_failures >= self.config.failure_threshold:
                    await self._transition_to_open()
                # Check failure rate
                elif self.stats.total_calls >= self.config.min_throughput:
                    if self.stats.failure_rate >= self.config.failure_rate_threshold:
                        await self._transition_to_open()
                        
            elif self.state == CircuitState.HALF_OPEN:
                # Single failure in half-open state reopens circuit
                await self._transition_to_open()
                
    async def _transition_to_open(self):
        """Transition to OPEN state."""
        logger.warning(f"Circuit breaker {self.config.name} transitioning to OPEN")
        self.state = CircuitState.OPEN
        self.stats.state_changes.append({
            "from": self.state.value,
            "to": CircuitState.OPEN.value,
            "timestamp": datetime.utcnow().isoformat(),
            "reason": f"Failures: {self.stats.consecutive_failures}"
        })
        
    async def _transition_to_closed(self):
        """Transition to CLOSED state."""
        logger.info(f"Circuit breaker {self.config.name} transitioning to CLOSED")
        self.state = CircuitState.CLOSED
        self.stats.consecutive_failures = 0
        self.stats.consecutive_successes = 0
        self.stats.state_changes.append({
            "from": self.state.value,
            "to": CircuitState.CLOSED.value,
            "timestamp": datetime.utcnow().isoformat(),
            "reason": "Recovery successful"
        })
        
    async def _transition_to_half_open(self):
        """Transition to HALF_OPEN state."""
        logger.info(f"Circuit breaker {self.config.name} transitioning to HALF_OPEN")
        self.state = CircuitState.HALF_OPEN
        self.stats.consecutive_failures = 0
        self.stats.consecutive_successes = 0
        self.stats.state_changes.append({
            "from": self.state.value,
            "to": CircuitState.HALF_OPEN.value,
            "timestamp": datetime.utcnow().isoformat(),
            "reason": "Testing recovery"
        })
        
    def get_stats(self) -> dict:
        """Get circuit breaker statistics."""
        return {
            "name": self.config.name,
            "state": self.state.value,
            "total_calls": self.stats.total_calls,
            "successful_calls": self.stats.successful_calls,
            "failed_calls": self.stats.failed_calls,
            "rejected_calls": self.stats.rejected_calls,
            "failure_rate": round(self.stats.failure_rate, 3),
            "consecutive_failures": self.stats.consecutive_failures,
            "consecutive_successes": self.stats.consecutive_successes,
            "last_failure": self.stats.last_failure_time,
            "state_changes": self.stats.state_changes[-10:]  # Last 10 changes
        }
        
    async def reset(self):
        """Reset circuit breaker to closed state."""
        async with self._state_lock:
            self.state = CircuitState.CLOSED
            self.stats = CircuitBreakerStats()
            self._last_attempt_time = None
            logger.info(f"Circuit breaker {self.config.name} reset") 