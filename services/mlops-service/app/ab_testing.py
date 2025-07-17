"""
A/B Testing for ML Models

Implements gradual rollout and performance comparison
"""

import logging
from typing import Dict, Any, List, Optional, Tuple
from datetime import datetime, timedelta
from dataclasses import dataclass
import asyncio
import random
import httpx
from enum import Enum
import numpy as np
from scipy import stats

logger = logging.getLogger(__name__)


class RolloutStrategy(Enum):
    FIXED = "fixed"  # Fixed percentage split
    PROGRESSIVE = "progressive"  # Gradually increase traffic
    CANARY = "canary"  # Start small, increase if successful
    BLUE_GREEN = "blue_green"  # All or nothing switch
    MULTI_ARMED_BANDIT = "multi_armed_bandit"  # Adaptive based on performance


@dataclass
class ABTestConfig:
    test_id: str
    model_name: str
    version_a: str
    version_b: str
    tenant_id: str
    strategy: RolloutStrategy
    initial_split: float  # Initial traffic to version B
    target_split: float  # Target traffic to version B
    duration_hours: int
    success_criteria: Dict[str, Any]
    rollback_criteria: Dict[str, Any]
    created_at: datetime
    updated_at: datetime
    status: str  # active, completed, rolled_back
    

@dataclass
class VersionMetrics:
    version: str
    request_count: int
    success_count: int
    error_count: int
    average_latency: float
    p95_latency: float
    accuracy: float
    custom_metrics: Dict[str, float]
    

class ABTestManager:
    """Manages A/B testing for ML models"""
    
    def __init__(self,
                 performance_tracker,
                 deployment_manager,
                 notification_service_url: Optional[str] = None):
        self.performance_tracker = performance_tracker
        self.deployment_manager = deployment_manager
        self.notification_service_url = notification_service_url
        self.active_tests: Dict[str, ABTestConfig] = {}
        self.test_metrics: Dict[str, Dict[str, VersionMetrics]] = {}
        self._monitoring_tasks: Dict[str, asyncio.Task] = {}
        
    async def create_ab_test(self,
                           model_name: str,
                           version_a: str,
                           version_b: str,
                           tenant_id: str,
                           strategy: RolloutStrategy = RolloutStrategy.PROGRESSIVE,
                           initial_split: float = 0.1,
                           target_split: float = 0.5,
                           duration_hours: int = 24,
                           success_criteria: Optional[Dict[str, Any]] = None,
                           rollback_criteria: Optional[Dict[str, Any]] = None) -> str:
        """Create a new A/B test"""
        test_id = f"ab_test_{model_name}_{datetime.utcnow().strftime('%Y%m%d_%H%M%S')}"
        
        # Default criteria
        if not success_criteria:
            success_criteria = {
                "min_accuracy_improvement": 0.01,
                "max_latency_increase": 1.1,  # 10% increase allowed
                "min_requests": 1000
            }
            
        if not rollback_criteria:
            rollback_criteria = {
                "max_error_rate": 0.05,
                "max_latency_degradation": 2.0,  # 2x latency triggers rollback
                "accuracy_drop": 0.05
            }
        
        config = ABTestConfig(
            test_id=test_id,
            model_name=model_name,
            version_a=version_a,
            version_b=version_b,
            tenant_id=tenant_id,
            strategy=strategy,
            initial_split=initial_split,
            target_split=target_split,
            duration_hours=duration_hours,
            success_criteria=success_criteria,
            rollback_criteria=rollback_criteria,
            created_at=datetime.utcnow(),
            updated_at=datetime.utcnow(),
            status="active"
        )
        
        self.active_tests[test_id] = config
        
        # Initialize metrics
        self.test_metrics[test_id] = {
            version_a: VersionMetrics(
                version=version_a,
                request_count=0,
                success_count=0,
                error_count=0,
                average_latency=0,
                p95_latency=0,
                accuracy=0,
                custom_metrics={}
            ),
            version_b: VersionMetrics(
                version=version_b,
                request_count=0,
                success_count=0,
                error_count=0,
                average_latency=0,
                p95_latency=0,
                accuracy=0,
                custom_metrics={}
            )
        }
        
        # Configure routing
        await self._configure_routing(config)
        
        # Start monitoring
        task = asyncio.create_task(self._monitor_test(test_id))
        self._monitoring_tasks[test_id] = task
        
        logger.info(f"Created A/B test {test_id} for {model_name}")
        return test_id
        
    async def _configure_routing(self, config: ABTestConfig):
        """Configure traffic routing for A/B test"""
        current_split = config.initial_split if config.strategy != RolloutStrategy.BLUE_GREEN else 0
        
        routing_config = {
            "test_id": config.test_id,
            "model_name": config.model_name,
            "routes": [
                {
                    "version": config.version_a,
                    "weight": 1 - current_split
                },
                {
                    "version": config.version_b,
                    "weight": current_split
                }
            ]
        }
        
        await self.deployment_manager.update_routing(
            tenant_id=config.tenant_id,
            model_name=config.model_name,
            routing_config=routing_config
        )
        
    async def _monitor_test(self, test_id: str):
        """Monitor A/B test progress"""
        config = self.active_tests[test_id]
        end_time = config.created_at + timedelta(hours=config.duration_hours)
        
        while datetime.utcnow() < end_time and config.status == "active":
            try:
                await asyncio.sleep(60)  # Check every minute
                
                # Update metrics
                await self._update_metrics(test_id)
                
                # Check rollback criteria
                if await self._should_rollback(test_id):
                    await self.rollback_test(test_id)
                    break
                    
                # Update traffic split if progressive
                if config.strategy in [RolloutStrategy.PROGRESSIVE, RolloutStrategy.CANARY]:
                    await self._update_traffic_split(test_id)
                    
                # Multi-armed bandit optimization
                if config.strategy == RolloutStrategy.MULTI_ARMED_BANDIT:
                    await self._optimize_traffic_split(test_id)
                    
            except Exception as e:
                logger.error(f"Error monitoring A/B test {test_id}: {e}")
                
        # Test completed
        if config.status == "active":
            await self._complete_test(test_id)
            
    async def _update_metrics(self, test_id: str):
        """Update metrics for both versions"""
        config = self.active_tests[test_id]
        
        for version in [config.version_a, config.version_b]:
            # Get performance metrics
            perf_report = await self.performance_tracker.get_performance_report(
                model_name=config.model_name,
                version=version,
                tenant_id=config.tenant_id
            )
            
            if perf_report.get("status") != "no_data":
                metrics = self.test_metrics[test_id][version]
                current = perf_report["current_metrics"]
                latency = perf_report["latency_metrics"]
                
                metrics.accuracy = current.get("accuracy", 0)
                metrics.average_latency = latency.get("p50", 0)
                metrics.p95_latency = latency.get("p95", 0)
                metrics.request_count = perf_report.get("prediction_count", 0)
                
    async def _should_rollback(self, test_id: str) -> bool:
        """Check if test should be rolled back"""
        config = self.active_tests[test_id]
        criteria = config.rollback_criteria
        
        metrics_a = self.test_metrics[test_id][config.version_a]
        metrics_b = self.test_metrics[test_id][config.version_b]
        
        # Check error rate
        if metrics_b.request_count > 0:
            error_rate_b = metrics_b.error_count / metrics_b.request_count
            if error_rate_b > criteria["max_error_rate"]:
                logger.warning(f"High error rate in version B: {error_rate_b}")
                return True
                
        # Check latency degradation
        if metrics_a.average_latency > 0:
            latency_ratio = metrics_b.average_latency / metrics_a.average_latency
            if latency_ratio > criteria["max_latency_degradation"]:
                logger.warning(f"High latency degradation: {latency_ratio}x")
                return True
                
        # Check accuracy drop
        if metrics_a.accuracy > 0:
            accuracy_drop = metrics_a.accuracy - metrics_b.accuracy
            if accuracy_drop > criteria["accuracy_drop"]:
                logger.warning(f"Accuracy drop: {accuracy_drop}")
                return True
                
        return False
        
    async def _update_traffic_split(self, test_id: str):
        """Update traffic split for progressive rollout"""
        config = self.active_tests[test_id]
        
        # Calculate progress
        elapsed = (datetime.utcnow() - config.created_at).total_seconds()
        total_duration = config.duration_hours * 3600
        progress = min(elapsed / total_duration, 1.0)
        
        # Calculate new split
        if config.strategy == RolloutStrategy.PROGRESSIVE:
            new_split = config.initial_split + (config.target_split - config.initial_split) * progress
        elif config.strategy == RolloutStrategy.CANARY:
            # Canary: step function
            if progress < 0.25:
                new_split = config.initial_split
            elif progress < 0.5:
                new_split = min(config.initial_split * 2, 0.25)
            elif progress < 0.75:
                new_split = min(config.initial_split * 4, 0.5)
            else:
                new_split = config.target_split
        else:
            return
            
        # Update routing
        await self._configure_routing(config)
        config.updated_at = datetime.utcnow()
        
    async def _optimize_traffic_split(self, test_id: str):
        """Optimize traffic split using multi-armed bandit"""
        config = self.active_tests[test_id]
        metrics_a = self.test_metrics[test_id][config.version_a]
        metrics_b = self.test_metrics[test_id][config.version_b]
        
        # Use Thompson sampling
        if metrics_a.request_count > 10 and metrics_b.request_count > 10:
            # Calculate success rates
            success_a = metrics_a.success_count / max(metrics_a.request_count, 1)
            success_b = metrics_b.success_count / max(metrics_b.request_count, 1)
            
            # Thompson sampling
            sample_a = np.random.beta(metrics_a.success_count + 1, 
                                    metrics_a.request_count - metrics_a.success_count + 1)
            sample_b = np.random.beta(metrics_b.success_count + 1,
                                    metrics_b.request_count - metrics_b.success_count + 1)
            
            # Update split based on samples
            new_split = sample_b / (sample_a + sample_b)
            new_split = max(0.05, min(0.95, new_split))  # Keep between 5% and 95%
            
            config.initial_split = new_split
            await self._configure_routing(config)
            
    async def _complete_test(self, test_id: str):
        """Complete A/B test and determine winner"""
        config = self.active_tests[test_id]
        metrics_a = self.test_metrics[test_id][config.version_a]
        metrics_b = self.test_metrics[test_id][config.version_b]
        
        # Statistical significance test
        significance, p_value = await self._test_significance(metrics_a, metrics_b)
        
        # Determine winner
        winner = None
        if significance and metrics_b.accuracy > metrics_a.accuracy:
            winner = config.version_b
        else:
            winner = config.version_a
            
        # Update status
        config.status = "completed"
        
        # Send completion notification
        await self._send_completion_notification(test_id, winner, p_value)
        
        # Clean up monitoring task
        if test_id in self._monitoring_tasks:
            self._monitoring_tasks[test_id].cancel()
            del self._monitoring_tasks[test_id]
            
        logger.info(f"A/B test {test_id} completed. Winner: {winner}")
        
    async def _test_significance(self, metrics_a: VersionMetrics, metrics_b: VersionMetrics) -> Tuple[bool, float]:
        """Test statistical significance between versions"""
        # Simple z-test for proportions
        n_a = metrics_a.request_count
        n_b = metrics_b.request_count
        
        if n_a < 100 or n_b < 100:
            return False, 1.0
            
        p_a = metrics_a.accuracy
        p_b = metrics_b.accuracy
        
        # Pooled proportion
        p_pool = (p_a * n_a + p_b * n_b) / (n_a + n_b)
        
        # Standard error
        se = np.sqrt(p_pool * (1 - p_pool) * (1/n_a + 1/n_b))
        
        # Z-score
        z = (p_b - p_a) / se
        
        # P-value (two-tailed)
        p_value = 2 * (1 - stats.norm.cdf(abs(z)))
        
        return p_value < 0.05, p_value
        
    async def rollback_test(self, test_id: str):
        """Rollback A/B test to version A"""
        config = self.active_tests[test_id]
        
        # Route all traffic to version A
        routing_config = {
            "test_id": config.test_id,
            "model_name": config.model_name,
            "routes": [
                {
                    "version": config.version_a,
                    "weight": 1.0
                },
                {
                    "version": config.version_b,
                    "weight": 0.0
                }
            ]
        }
        
        await self.deployment_manager.update_routing(
            tenant_id=config.tenant_id,
            model_name=config.model_name,
            routing_config=routing_config
        )
        
        config.status = "rolled_back"
        
        # Send rollback notification
        await self._send_rollback_notification(test_id)
        
        logger.info(f"A/B test {test_id} rolled back to version {config.version_a}")
        
    async def get_test_results(self, test_id: str) -> Dict[str, Any]:
        """Get current A/B test results"""
        if test_id not in self.active_tests:
            return {"error": "Test not found"}
            
        config = self.active_tests[test_id]
        metrics = self.test_metrics[test_id]
        
        return {
            "test_id": test_id,
            "status": config.status,
            "model_name": config.model_name,
            "strategy": config.strategy.value,
            "duration": {
                "total_hours": config.duration_hours,
                "elapsed_hours": (datetime.utcnow() - config.created_at).total_seconds() / 3600
            },
            "versions": {
                "a": {
                    "version": config.version_a,
                    "metrics": {
                        "accuracy": metrics[config.version_a].accuracy,
                        "latency_avg": metrics[config.version_a].average_latency,
                        "latency_p95": metrics[config.version_a].p95_latency,
                        "request_count": metrics[config.version_a].request_count,
                        "error_rate": metrics[config.version_a].error_count / max(metrics[config.version_a].request_count, 1)
                    }
                },
                "b": {
                    "version": config.version_b,
                    "metrics": {
                        "accuracy": metrics[config.version_b].accuracy,
                        "latency_avg": metrics[config.version_b].average_latency,
                        "latency_p95": metrics[config.version_b].p95_latency,
                        "request_count": metrics[config.version_b].request_count,
                        "error_rate": metrics[config.version_b].error_count / max(metrics[config.version_b].request_count, 1)
                    }
                }
            },
            "current_split": config.initial_split,
            "created_at": config.created_at.isoformat(),
            "updated_at": config.updated_at.isoformat()
        }
        
    async def _send_completion_notification(self, test_id: str, winner: str, p_value: float):
        """Send notification when test completes"""
        if not self.notification_service_url:
            return
            
        # In production, integrate with notification service
        logger.info(f"A/B test {test_id} completed. Winner: {winner}, p-value: {p_value}")
        
    async def _send_rollback_notification(self, test_id: str):
        """Send notification when test is rolled back"""
        if not self.notification_service_url:
            return
            
        # In production, integrate with notification service
        logger.info(f"A/B test {test_id} was rolled back") 