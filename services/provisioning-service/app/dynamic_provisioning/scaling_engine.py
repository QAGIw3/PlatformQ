"""Scaling Engine for Dynamic Resource Provisioning

Makes intelligent scaling decisions based on metrics, policies, and predictions.
"""

import logging
import asyncio
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Tuple
from dataclasses import dataclass
from enum import Enum
import json

from kubernetes import client as k8s_client, config as k8s_config
from pyignite import Client as IgniteClient
import pulsar
from pulsar.schema import AvroSchema

from .resource_monitor import ResourceMonitor, ResourceMetrics, ClusterMetrics
from .predictive_scaler import PredictiveScaler
from .cost_optimizer import CostOptimizer
from platformq_shared.events import BaseEvent

logger = logging.getLogger(__name__)


class ScalingAction(Enum):
    """Types of scaling actions"""
    SCALE_UP = "scale_up"
    SCALE_DOWN = "scale_down"
    SCALE_OUT = "scale_out"  # Add more pods
    SCALE_IN = "scale_in"    # Remove pods
    VERTICAL_SCALE = "vertical_scale"  # Change resource limits
    NO_ACTION = "no_action"


@dataclass
class ScalingDecision:
    """Container for scaling decisions"""
    service_name: str
    namespace: str
    action: ScalingAction
    current_replicas: int
    target_replicas: Optional[int] = None
    current_cpu_limit: Optional[str] = None
    target_cpu_limit: Optional[str] = None
    current_memory_limit: Optional[str] = None
    target_memory_limit: Optional[str] = None
    reason: str = ""
    confidence: float = 1.0
    estimated_cost_impact: float = 0.0
    timestamp: datetime = None
    
    def __post_init__(self):
        if self.timestamp is None:
            self.timestamp = datetime.utcnow()


@dataclass
class ScalingPolicy:
    """Scaling policy for a service"""
    service_name: str
    min_replicas: int = 1
    max_replicas: int = 10
    target_cpu_utilization: float = 70.0
    target_memory_utilization: float = 80.0
    scale_up_threshold: float = 80.0
    scale_down_threshold: float = 30.0
    scale_up_rate: float = 1.5  # Multiply replicas by this
    scale_down_rate: float = 0.8  # Multiply replicas by this
    cooldown_seconds: int = 300  # 5 minutes
    enable_vertical_scaling: bool = True
    enable_predictive_scaling: bool = True
    cost_aware: bool = True
    business_hours_only: bool = False  # Scale down after hours
    priority: int = 1  # Higher priority services scale first


class ScalingEngine:
    """Intelligent scaling engine for dynamic resource provisioning
    
    Features:
    - Horizontal and vertical scaling
    - Predictive scaling based on ML models
    - Cost-aware scaling decisions
    - Multi-tenant resource management
    - Business rules and policies
    """
    
    def __init__(
        self,
        resource_monitor: ResourceMonitor,
        ignite_client: IgniteClient,
        pulsar_client: pulsar.Client,
        kubernetes_namespace: str = "platformq"
    ):
        self.resource_monitor = resource_monitor
        self.ignite_client = ignite_client
        self.pulsar_client = pulsar_client
        self.kubernetes_namespace = kubernetes_namespace
        
        # Initialize Kubernetes client
        try:
            k8s_config.load_incluster_config()
        except:
            k8s_config.load_kube_config()
        
        self.k8s_apps = k8s_client.AppsV1Api()
        self.k8s_core = k8s_client.CoreV1Api()
        self.k8s_autoscaling = k8s_client.AutoscalingV2Api()
        
        # Create caches
        self.policies_cache = ignite_client.get_or_create_cache('scaling_policies')
        self.decisions_cache = ignite_client.get_or_create_cache('scaling_decisions')
        self.cooldown_cache = ignite_client.get_or_create_cache('scaling_cooldowns')
        
        # Initialize components
        self.predictive_scaler = PredictiveScaler(ignite_client)
        self.cost_optimizer = CostOptimizer(ignite_client)
        
        # Event publishers
        self.scaling_event_publisher = pulsar_client.create_producer(
            'persistent://public/default/scaling-events',
            schema=AvroSchema(ScalingEvent)
        )
        
        self._running = False
        self._tasks = []
        
        # Load default policies
        self._load_default_policies()
    
    def _load_default_policies(self):
        """Load default scaling policies"""
        default_policies = [
            ScalingPolicy(
                service_name="auth-service",
                min_replicas=2,
                max_replicas=20,
                priority=10  # High priority
            ),
            ScalingPolicy(
                service_name="digital-asset-service",
                min_replicas=2,
                max_replicas=15,
                enable_vertical_scaling=True
            ),
            ScalingPolicy(
                service_name="simulation-service",
                min_replicas=1,
                max_replicas=50,
                enable_vertical_scaling=True,
                target_cpu_utilization=60.0  # Lower threshold for compute-intensive
            ),
            ScalingPolicy(
                service_name="federated-learning-service",
                min_replicas=1,
                max_replicas=30,
                enable_predictive_scaling=True,
                business_hours_only=True
            ),
            ScalingPolicy(
                service_name="workflow-service",
                min_replicas=2,
                max_replicas=10
            ),
            ScalingPolicy(
                service_name="notification-service",
                min_replicas=1,
                max_replicas=5,
                scale_up_threshold=90.0  # Less sensitive
            )
        ]
        
        for policy in default_policies:
            self.policies_cache.put(policy.service_name, policy)
    
    async def start(self):
        """Start the scaling engine"""
        self._running = True
        logger.info("Starting scaling engine")
        
        # Start background tasks
        self._tasks = [
            asyncio.create_task(self._scaling_loop()),
            asyncio.create_task(self._policy_sync_loop()),
            asyncio.create_task(self._cost_optimization_loop())
        ]
    
    async def stop(self):
        """Stop the scaling engine"""
        self._running = False
        for task in self._tasks:
            task.cancel()
        await asyncio.gather(*self._tasks, return_exceptions=True)
        self.scaling_event_publisher.close()
        logger.info("Scaling engine stopped")
    
    async def _scaling_loop(self):
        """Main scaling decision loop"""
        while self._running:
            try:
                # Get all services with policies
                services = []
                for key in self.policies_cache.keys():
                    policy = self.policies_cache.get(key)
                    services.append(policy)
                
                # Sort by priority
                services.sort(key=lambda p: p.priority, reverse=True)
                
                # Make scaling decisions
                decisions = []
                for policy in services:
                    decision = await self._evaluate_service(policy)
                    if decision and decision.action != ScalingAction.NO_ACTION:
                        decisions.append(decision)
                
                # Apply decisions
                for decision in decisions:
                    await self._apply_scaling_decision(decision)
                
            except Exception as e:
                logger.error(f"Error in scaling loop: {e}")
            
            await asyncio.sleep(30)  # Evaluate every 30 seconds
    
    async def _evaluate_service(self, policy: ScalingPolicy) -> Optional[ScalingDecision]:
        """Evaluate a service for scaling"""
        # Check cooldown
        if self._is_in_cooldown(policy.service_name):
            return None
        
        # Get current metrics
        metrics = self.resource_monitor.get_current_metrics(
            policy.service_name,
            self.kubernetes_namespace
        )
        
        if not metrics:
            return None
        
        # Get current deployment info
        deployment = await self._get_deployment(policy.service_name)
        if not deployment:
            return None
        
        current_replicas = deployment.spec.replicas
        
        # Initialize decision
        decision = ScalingDecision(
            service_name=policy.service_name,
            namespace=self.kubernetes_namespace,
            action=ScalingAction.NO_ACTION,
            current_replicas=current_replicas
        )
        
        # Check if we should use predictive scaling
        if policy.enable_predictive_scaling:
            predicted_load = await self.predictive_scaler.predict_load(
                policy.service_name,
                horizon_minutes=30
            )
            
            if predicted_load:
                # Use predicted metrics
                metrics.cpu_usage = max(metrics.cpu_usage, predicted_load.cpu_usage)
                metrics.memory_usage = max(metrics.memory_usage, predicted_load.memory_usage)
                decision.confidence = predicted_load.confidence
        
        # Check business hours policy
        if policy.business_hours_only:
            if not self._is_business_hours():
                # Scale down after hours
                if current_replicas > policy.min_replicas:
                    decision.action = ScalingAction.SCALE_IN
                    decision.target_replicas = policy.min_replicas
                    decision.reason = "After business hours scale down"
                    return decision
        
        # Evaluate horizontal scaling
        if metrics.cpu_usage > policy.scale_up_threshold or metrics.memory_usage > policy.scale_up_threshold:
            # Scale out
            new_replicas = min(
                int(current_replicas * policy.scale_up_rate),
                policy.max_replicas
            )
            
            if new_replicas > current_replicas:
                decision.action = ScalingAction.SCALE_OUT
                decision.target_replicas = new_replicas
                decision.reason = f"High resource usage: CPU {metrics.cpu_usage:.1f}%, Memory {metrics.memory_usage:.1f}%"
        
        elif metrics.cpu_usage < policy.scale_down_threshold and metrics.memory_usage < policy.scale_down_threshold:
            # Scale in
            new_replicas = max(
                int(current_replicas * policy.scale_down_rate),
                policy.min_replicas
            )
            
            if new_replicas < current_replicas:
                decision.action = ScalingAction.SCALE_IN
                decision.target_replicas = new_replicas
                decision.reason = f"Low resource usage: CPU {metrics.cpu_usage:.1f}%, Memory {metrics.memory_usage:.1f}%"
        
        # Evaluate vertical scaling if enabled
        if policy.enable_vertical_scaling and decision.action == ScalingAction.NO_ACTION:
            vertical_decision = await self._evaluate_vertical_scaling(
                policy,
                deployment,
                metrics
            )
            if vertical_decision:
                decision = vertical_decision
        
        # Cost optimization
        if policy.cost_aware and decision.action != ScalingAction.NO_ACTION:
            optimized_decision = await self.cost_optimizer.optimize_decision(
                decision,
                metrics,
                self.resource_monitor.get_cluster_metrics()
            )
            if optimized_decision:
                decision = optimized_decision
        
        # Store decision
        if decision.action != ScalingAction.NO_ACTION:
            self.decisions_cache.put(
                f"{policy.service_name}:{decision.timestamp.isoformat()}",
                decision
            )
        
        return decision
    
    async def _evaluate_vertical_scaling(
        self,
        policy: ScalingPolicy,
        deployment: k8s_client.V1Deployment,
        metrics: ResourceMetrics
    ) -> Optional[ScalingDecision]:
        """Evaluate vertical scaling needs"""
        container = deployment.spec.template.spec.containers[0]
        
        current_cpu_limit = container.resources.limits.get('cpu', '1')
        current_memory_limit = container.resources.limits.get('memory', '1Gi')
        
        decision = ScalingDecision(
            service_name=policy.service_name,
            namespace=self.kubernetes_namespace,
            action=ScalingAction.NO_ACTION,
            current_replicas=deployment.spec.replicas,
            current_cpu_limit=current_cpu_limit,
            current_memory_limit=current_memory_limit
        )
        
        # Check if we need more resources per pod
        if metrics.cpu_usage > 90 and deployment.spec.replicas >= policy.max_replicas:
            # Can't scale out more, need bigger pods
            new_cpu = self._increase_resource(current_cpu_limit)
            decision.action = ScalingAction.VERTICAL_SCALE
            decision.target_cpu_limit = new_cpu
            decision.reason = "CPU constrained at max replicas"
        
        elif metrics.memory_usage > 90 and deployment.spec.replicas >= policy.max_replicas:
            # Need more memory per pod
            new_memory = self._increase_resource(current_memory_limit)
            decision.action = ScalingAction.VERTICAL_SCALE
            decision.target_memory_limit = new_memory
            decision.reason = "Memory constrained at max replicas"
        
        return decision if decision.action != ScalingAction.NO_ACTION else None
    
    def _increase_resource(self, current: str) -> str:
        """Increase a resource specification"""
        # Simple logic to increase resources
        if current.endswith('m'):  # millicores
            value = int(current[:-1])
            return f"{int(value * 1.5)}m"
        elif current.endswith('Gi'):
            value = float(current[:-2])
            return f"{value * 1.5:.1f}Gi"
        elif current.endswith('Mi'):
            value = int(current[:-2])
            return f"{int(value * 1.5)}Mi"
        else:
            # Assume it's a plain number (cores)
            return str(float(current) * 1.5)
    
    async def _apply_scaling_decision(self, decision: ScalingDecision):
        """Apply a scaling decision"""
        try:
            logger.info(
                f"Applying scaling decision for {decision.service_name}: "
                f"{decision.action.value} - {decision.reason}"
            )
            
            if decision.action in [ScalingAction.SCALE_OUT, ScalingAction.SCALE_IN]:
                # Horizontal scaling
                await self._scale_deployment(
                    decision.service_name,
                    decision.target_replicas
                )
            
            elif decision.action == ScalingAction.VERTICAL_SCALE:
                # Vertical scaling
                await self._update_resources(
                    decision.service_name,
                    decision.target_cpu_limit,
                    decision.target_memory_limit
                )
            
            # Set cooldown
            self._set_cooldown(decision.service_name)
            
            # Publish event
            event = ScalingEvent(
                service_name=decision.service_name,
                namespace=decision.namespace,
                action=decision.action.value,
                old_replicas=decision.current_replicas,
                new_replicas=decision.target_replicas,
                old_cpu_limit=decision.current_cpu_limit,
                new_cpu_limit=decision.target_cpu_limit,
                old_memory_limit=decision.current_memory_limit,
                new_memory_limit=decision.target_memory_limit,
                reason=decision.reason,
                timestamp=decision.timestamp.isoformat()
            )
            self.scaling_event_publisher.send(event)
            
        except Exception as e:
            logger.error(f"Failed to apply scaling decision: {e}")
    
    async def _scale_deployment(self, deployment_name: str, replicas: int):
        """Scale a deployment horizontally"""
        body = {
            'spec': {
                'replicas': replicas
            }
        }
        
        self.k8s_apps.patch_namespaced_deployment_scale(
            name=deployment_name,
            namespace=self.kubernetes_namespace,
            body=body
        )
    
    async def _update_resources(
        self,
        deployment_name: str,
        cpu_limit: Optional[str],
        memory_limit: Optional[str]
    ):
        """Update deployment resource limits"""
        deployment = await self._get_deployment(deployment_name)
        if not deployment:
            return
        
        # Update container resources
        container = deployment.spec.template.spec.containers[0]
        
        if not container.resources:
            container.resources = k8s_client.V1ResourceRequirements()
        if not container.resources.limits:
            container.resources.limits = {}
        if not container.resources.requests:
            container.resources.requests = {}
        
        if cpu_limit:
            container.resources.limits['cpu'] = cpu_limit
            # Set request to 50% of limit
            container.resources.requests['cpu'] = self._calculate_request(cpu_limit)
        
        if memory_limit:
            container.resources.limits['memory'] = memory_limit
            # Set request to 80% of limit
            container.resources.requests['memory'] = self._calculate_request(memory_limit, 0.8)
        
        # Update deployment
        self.k8s_apps.patch_namespaced_deployment(
            name=deployment_name,
            namespace=self.kubernetes_namespace,
            body=deployment
        )
    
    def _calculate_request(self, limit: str, factor: float = 0.5) -> str:
        """Calculate resource request from limit"""
        if limit.endswith('m'):
            value = int(limit[:-1])
            return f"{int(value * factor)}m"
        elif limit.endswith('Gi'):
            value = float(limit[:-2])
            return f"{value * factor:.1f}Gi"
        elif limit.endswith('Mi'):
            value = int(limit[:-2])
            return f"{int(value * factor)}Mi"
        else:
            return str(float(limit) * factor)
    
    async def _get_deployment(self, name: str) -> Optional[k8s_client.V1Deployment]:
        """Get deployment object"""
        try:
            return self.k8s_apps.read_namespaced_deployment(
                name=name,
                namespace=self.kubernetes_namespace
            )
        except Exception as e:
            logger.error(f"Failed to get deployment {name}: {e}")
            return None
    
    def _is_in_cooldown(self, service_name: str) -> bool:
        """Check if service is in cooldown period"""
        key = f"cooldown:{service_name}"
        if self.cooldown_cache.contains_key(key):
            cooldown_until = self.cooldown_cache.get(key)
            return datetime.utcnow() < cooldown_until
        return False
    
    def _set_cooldown(self, service_name: str):
        """Set cooldown for a service"""
        policy = self.policies_cache.get(service_name)
        if policy:
            cooldown_until = datetime.utcnow() + timedelta(seconds=policy.cooldown_seconds)
            key = f"cooldown:{service_name}"
            self.cooldown_cache.put(key, cooldown_until)
    
    def _is_business_hours(self) -> bool:
        """Check if current time is within business hours"""
        now = datetime.utcnow()
        # Simple logic: Monday-Friday, 8 AM - 6 PM UTC
        if now.weekday() >= 5:  # Weekend
            return False
        return 8 <= now.hour < 18
    
    async def _policy_sync_loop(self):
        """Sync policies from external sources"""
        while self._running:
            try:
                # This would sync policies from a configuration service
                # or database in a real implementation
                pass
            except Exception as e:
                logger.error(f"Error syncing policies: {e}")
            
            await asyncio.sleep(300)  # Every 5 minutes
    
    async def _cost_optimization_loop(self):
        """Periodic cost optimization"""
        while self._running:
            try:
                # Analyze cost trends and optimize
                await self.cost_optimizer.analyze_and_optimize(
                    self.resource_monitor,
                    self.policies_cache,
                    self.decisions_cache
                )
            except Exception as e:
                logger.error(f"Error in cost optimization: {e}")
            
            await asyncio.sleep(3600)  # Every hour
    
    def get_policy(self, service_name: str) -> Optional[ScalingPolicy]:
        """Get scaling policy for a service"""
        if self.policies_cache.contains_key(service_name):
            return self.policies_cache.get(service_name)
        return None
    
    def update_policy(self, policy: ScalingPolicy):
        """Update scaling policy"""
        self.policies_cache.put(policy.service_name, policy)
        logger.info(f"Updated scaling policy for {policy.service_name}")
    
    def get_recent_decisions(
        self,
        service_name: Optional[str] = None,
        hours: int = 24
    ) -> List[ScalingDecision]:
        """Get recent scaling decisions"""
        decisions = []
        cutoff = datetime.utcnow() - timedelta(hours=hours)
        
        for key in self.decisions_cache.keys():
            parts = key.split(':')
            if len(parts) == 2:
                svc_name, timestamp_str = parts
                
                if service_name and svc_name != service_name:
                    continue
                
                timestamp = datetime.fromisoformat(timestamp_str)
                if timestamp >= cutoff:
                    decisions.append(self.decisions_cache.get(key))
        
        return sorted(decisions, key=lambda d: d.timestamp, reverse=True)


class ScalingEvent(BaseEvent):
    """Event for scaling actions"""
    service_name: str
    namespace: str
    action: str
    old_replicas: Optional[int]
    new_replicas: Optional[int]
    old_cpu_limit: Optional[str]
    new_cpu_limit: Optional[str]
    old_memory_limit: Optional[str]
    new_memory_limit: Optional[str]
    reason: str
    timestamp: str 