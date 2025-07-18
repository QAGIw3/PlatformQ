"""
Monitoring Module

Prometheus metrics for dataset marketplace
"""

from prometheus_client import Counter, Histogram, Gauge, generate_latest
import time
from functools import wraps
import logging

logger = logging.getLogger(__name__)

# Metrics
dataset_uploads = Counter(
    'dataset_uploads_total',
    'Total number of dataset uploads',
    ['dataset_type', 'status']
)

quality_assessments = Counter(
    'quality_assessments_total',
    'Total number of quality assessments',
    ['quality_tier', 'automated']
)

data_access_requests = Counter(
    'data_access_requests_total',
    'Total number of data access requests',
    ['access_level', 'granted']
)

trust_scores = Histogram(
    'trust_scores',
    'Distribution of trust scores',
    ['entity_type']
)

quality_scores = Histogram(
    'quality_scores',
    'Distribution of quality scores',
    ['dataset_type']
)

active_datasets = Gauge(
    'active_datasets',
    'Number of active datasets',
    ['dataset_type']
)

revenue_total = Counter(
    'revenue_total',
    'Total revenue from dataset sales',
    ['currency']
)

processing_duration = Histogram(
    'processing_duration_seconds',
    'Duration of various processing operations',
    ['operation']
)


class PrometheusMetrics:
    """Prometheus metrics handler"""
    
    @staticmethod
    def record_dataset_upload(dataset_type: str, status: str):
        """Record dataset upload"""
        dataset_uploads.labels(dataset_type=dataset_type, status=status).inc()
    
    @staticmethod
    def record_quality_assessment(quality_tier: str, automated: bool):
        """Record quality assessment"""
        quality_assessments.labels(
            quality_tier=quality_tier,
            automated=str(automated)
        ).inc()
    
    @staticmethod
    def record_access_request(access_level: str, granted: bool):
        """Record data access request"""
        data_access_requests.labels(
            access_level=access_level,
            granted=str(granted)
        ).inc()
    
    @staticmethod
    def record_trust_score(entity_type: str, score: float):
        """Record trust score"""
        trust_scores.labels(entity_type=entity_type).observe(score)
    
    @staticmethod
    def record_quality_score(dataset_type: str, score: float):
        """Record quality score"""
        quality_scores.labels(dataset_type=dataset_type).observe(score)
    
    @staticmethod
    def update_active_datasets(dataset_type: str, count: int):
        """Update active datasets gauge"""
        active_datasets.labels(dataset_type=dataset_type).set(count)
    
    @staticmethod
    def record_revenue(amount: float, currency: str = "USD"):
        """Record revenue"""
        revenue_total.labels(currency=currency).inc(amount)
    
    @staticmethod
    def time_operation(operation: str):
        """Decorator to time operations"""
        def decorator(func):
            @wraps(func)
            async def async_wrapper(*args, **kwargs):
                start = time.time()
                try:
                    result = await func(*args, **kwargs)
                    return result
                finally:
                    duration = time.time() - start
                    processing_duration.labels(operation=operation).observe(duration)
            
            @wraps(func)
            def sync_wrapper(*args, **kwargs):
                start = time.time()
                try:
                    result = func(*args, **kwargs)
                    return result
                finally:
                    duration = time.time() - start
                    processing_duration.labels(operation=operation).observe(duration)
            
            # Return appropriate wrapper based on function type
            import asyncio
            if asyncio.iscoroutinefunction(func):
                return async_wrapper
            else:
                return sync_wrapper
        
        return decorator
    
    @staticmethod
    def generate_metrics():
        """Generate metrics in Prometheus format"""
        return generate_latest()


# Export metrics instance
metrics = PrometheusMetrics() 