"""
Tests for resilience patterns
"""

import pytest
import time
import threading
from unittest.mock import Mock, patch
from platformq_shared.resilience import (
    CircuitBreaker, CircuitState, CircuitBreakerError,
    RateLimiter, RateLimitExceeded,
    AdaptiveRetry, ConnectionPoolManager,
    MetricsCollector, with_resilience,
    circuit_breaker, rate_limit, adaptive_retry,
    get_connection_pool_manager, get_metrics_collector
)


class TestCircuitBreaker:
    """Test circuit breaker pattern"""
    
    def test_circuit_breaker_closed_state(self):
        """Test circuit breaker in closed state"""
        breaker = CircuitBreaker(failure_threshold=3, recovery_timeout=1)
        
        # Successful calls should work
        result = breaker.call(lambda: "success")
        assert result == "success"
        assert breaker.state == CircuitState.CLOSED
        
    def test_circuit_breaker_opens_after_failures(self):
        """Test circuit breaker opens after threshold failures"""
        breaker = CircuitBreaker(failure_threshold=3, recovery_timeout=1)
        
        def failing_func():
            raise Exception("Test failure")
        
        # Fail 3 times to open the circuit
        for i in range(3):
            with pytest.raises(Exception):
                breaker.call(failing_func)
                
        assert breaker.state == CircuitState.OPEN
        
        # Circuit should be open, rejecting calls
        with pytest.raises(CircuitBreakerError):
            breaker.call(lambda: "success")
            
    def test_circuit_breaker_half_open_recovery(self):
        """Test circuit breaker recovery to half-open state"""
        breaker = CircuitBreaker(failure_threshold=2, recovery_timeout=0.5)
        
        def failing_func():
            raise Exception("Test failure")
        
        # Open the circuit
        for i in range(2):
            with pytest.raises(Exception):
                breaker.call(failing_func)
                
        assert breaker.state == CircuitState.OPEN
        
        # Wait for recovery timeout
        time.sleep(0.6)
        
        # Should be half-open now
        assert breaker.state == CircuitState.HALF_OPEN
        
        # Successful call should close the circuit
        result = breaker.call(lambda: "recovered")
        assert result == "recovered"
        assert breaker.state == CircuitState.CLOSED
        
    def test_circuit_breaker_decorator(self):
        """Test circuit breaker as decorator"""
        call_count = 0
        
        @circuit_breaker(failure_threshold=2, recovery_timeout=0.5)
        def test_func(should_fail=False):
            nonlocal call_count
            call_count += 1
            if should_fail:
                raise Exception("Test failure")
            return "success"
            
        # Successful calls
        assert test_func() == "success"
        assert test_func() == "success"
        
        # Fail to open circuit
        for i in range(2):
            with pytest.raises(Exception):
                test_func(should_fail=True)
                
        # Circuit should be open
        with pytest.raises(CircuitBreakerError):
            test_func()


class TestRateLimiter:
    """Test rate limiter"""
    
    def test_rate_limiter_allows_bursts(self):
        """Test rate limiter allows burst requests"""
        limiter = RateLimiter(rate=10, burst=5)  # 10/s with burst of 5
        
        # Should allow 5 requests immediately
        for i in range(5):
            assert limiter.acquire() is True
            
    def test_rate_limiter_blocks_excess(self):
        """Test rate limiter blocks when exceeded"""
        limiter = RateLimiter(rate=2, burst=2)  # 2/s with burst of 2
        
        # Use up burst
        assert limiter.acquire() is True
        assert limiter.acquire() is True
        
        # Next request should fail if non-blocking
        assert limiter.acquire(blocking=False) is False
        
    def test_rate_limiter_refills(self):
        """Test rate limiter refills over time"""
        limiter = RateLimiter(rate=10, burst=2)  # 10/s
        
        # Use up burst
        assert limiter.acquire() is True
        assert limiter.acquire() is True
        
        # Wait for refill
        time.sleep(0.2)  # Should refill 2 tokens
        
        # Should be able to acquire again
        assert limiter.acquire() is True
        
    def test_rate_limit_decorator(self):
        """Test rate limit as decorator"""
        call_times = []
        
        @rate_limit(rate=5, burst=2)
        def test_func():
            call_times.append(time.time())
            return "success"
            
        # Should allow burst
        assert test_func() == "success"
        assert test_func() == "success"
        
        # Next should raise if we're too fast
        with pytest.raises(RateLimitExceeded):
            test_func()


class TestAdaptiveRetry:
    """Test adaptive retry logic"""
    
    def test_retry_succeeds_after_failures(self):
        """Test retry succeeds after transient failures"""
        retry_logic = AdaptiveRetry(max_retries=3, initial_delay=0.1)
        attempts = 0
        
        @adaptive_retry(max_retries=3, initial_delay=0.1)
        def flaky_func():
            nonlocal attempts
            attempts += 1
            if attempts < 3:
                raise ConnectionError("Transient error")
            return "success"
            
        result = flaky_func()
        assert result == "success"
        assert attempts == 3
        
    def test_retry_gives_up_after_max_attempts(self):
        """Test retry gives up after max attempts"""
        attempts = 0
        
        @adaptive_retry(max_retries=2, initial_delay=0.1)
        def always_fails():
            nonlocal attempts
            attempts += 1
            raise ConnectionError("Persistent error")
            
        with pytest.raises(ConnectionError):
            always_fails()
            
        assert attempts == 3  # initial + 2 retries
        
    def test_retry_with_jitter(self):
        """Test retry delay includes jitter"""
        retry = AdaptiveRetry(initial_delay=0.1, jitter=True)
        
        delays = []
        for i in range(5):
            delay = retry.calculate_delay(i)
            delays.append(delay)
            
        # Delays should vary due to jitter
        assert len(set(delays)) > 1
        
    def test_retry_non_retryable_errors(self):
        """Test retry doesn't retry certain errors"""
        @adaptive_retry(max_retries=3, retry_on=(ConnectionError,))
        def test_func():
            raise ValueError("Non-retryable error")
            
        # Should not retry ValueError
        with pytest.raises(ValueError):
            test_func()


class TestConnectionPoolManager:
    """Test connection pool manager"""
    
    def test_get_session_creates_pool(self):
        """Test getting session creates connection pool"""
        manager = ConnectionPoolManager()
        
        session = manager.get_session(
            "http://test.example.com",
            headers={"X-Test": "value"}
        )
        
        assert session is not None
        assert session.headers["X-Test"] == "value"
        
    def test_get_session_reuses_pool(self):
        """Test getting session reuses existing pool"""
        manager = ConnectionPoolManager()
        
        session1 = manager.get_session("http://test.example.com")
        session2 = manager.get_session("http://test.example.com")
        
        assert session1 is session2
        
    def test_close_all_pools(self):
        """Test closing all connection pools"""
        manager = ConnectionPoolManager()
        
        session1 = manager.get_session("http://test1.example.com")
        session2 = manager.get_session("http://test2.example.com")
        
        manager.close_all()
        
        # Should create new sessions after close
        session3 = manager.get_session("http://test1.example.com")
        assert session3 is not session1


class TestMetricsCollector:
    """Test metrics collection"""
    
    def test_record_successful_request(self):
        """Test recording successful request metrics"""
        collector = MetricsCollector()
        
        collector.record_request(
            service="test-service",
            endpoint="test-endpoint",
            duration=0.123,
            status_code=200
        )
        
        stats = collector.get_stats("test-service", "test-endpoint")
        assert len(stats) == 1
        
        key = "test-service:test-endpoint"
        assert stats[key]["total_requests"] == 1
        assert stats[key]["successful_requests"] == 1
        assert stats[key]["failed_requests"] == 0
        assert stats[key]["avg_duration"] == 0.123
        
    def test_record_failed_request(self):
        """Test recording failed request metrics"""
        collector = MetricsCollector()
        
        collector.record_request(
            service="test-service",
            endpoint="test-endpoint",
            duration=0.5,
            status_code=500,
            error="Internal server error"
        )
        
        stats = collector.get_stats()
        key = "test-service:test-endpoint"
        assert stats[key]["failed_requests"] == 1
        assert stats[key]["success_rate"] == 0
        assert len(stats[key]["errors"]) == 1
        
    def test_metrics_window_cleanup(self):
        """Test old metrics are cleaned up"""
        collector = MetricsCollector(window_size=1)  # 1 second window
        
        collector.record_request("service", "endpoint", 0.1, 200)
        time.sleep(1.5)
        
        # Record new metric to trigger cleanup
        collector.record_request("service", "endpoint", 0.2, 200)
        
        stats = collector.get_stats()
        # Should only have 1 recent request
        assert stats["service:endpoint"]["total_requests"] == 1


class TestWithResilience:
    """Test combined resilience decorator"""
    
    def test_with_resilience_decorator(self):
        """Test the combined resilience decorator"""
        call_count = 0
        
        @with_resilience(
            service_name="test",
            circuit_breaker_threshold=5,
            rate_limit=10.0,
            max_retries=2
        )
        def test_func(should_fail=False):
            nonlocal call_count
            call_count += 1
            if should_fail:
                raise ConnectionError("Test error")
            return "success"
            
        # Should succeed
        assert test_func() == "success"
        
        # Should retry on failure
        call_count = 0
        with pytest.raises(ConnectionError):
            test_func(should_fail=True)
            
        assert call_count == 3  # initial + 2 retries
        
        # Should have circuit breaker
        assert hasattr(test_func, 'circuit_breaker')
        assert hasattr(test_func, 'rate_limiter')
        assert hasattr(test_func, 'retry_logic')


class TestGlobalInstances:
    """Test global singleton instances"""
    
    def test_global_connection_pool_manager(self):
        """Test global connection pool manager is singleton"""
        manager1 = get_connection_pool_manager()
        manager2 = get_connection_pool_manager()
        assert manager1 is manager2
        
    def test_global_metrics_collector(self):
        """Test global metrics collector is singleton"""
        collector1 = get_metrics_collector()
        collector2 = get_metrics_collector()
        assert collector1 is collector2 