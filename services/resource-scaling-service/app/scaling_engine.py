"""Scaling Engine Implementation

Handles auto-scaling decisions with predictive capabilities.
"""

import logging
import asyncio
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Any
import json

import httpx
from pyignite import Client as IgniteClient
import consul
from kubernetes import client as k8s_client, config as k8s_config

from platformq_resource_common import (
    IScalingEngine,
    ScalingDecision,
    ScalingPolicy,
    ScalingAction,
    ResourceMetrics
)

from .config import Settings
from .predictive_scaler import PredictiveScaler

logger = logging.getLogger(__name__)


class ScalingEngine(IScalingEngine):
    """Auto-scaling engine with predictive capabilities"""
    
    def __init__(self, settings: Settings):
        self.settings = settings
        self.ignite_client = None
        self.consul_client = None
        self.k8s_apps_v1 = None
        self.k8s_v1 = None
        self.http_client = None
        self.predictive_scaler = None
        
        self._running = False
        self._tasks = []
        self._cooldown_tracker = {}  # Track last scaling time per service
    
    async def initialize(self):
        """Initialize connections"""
        # Initialize Ignite client
        self.ignite_client = IgniteClient()
        self.ignite_client.connect([
            (self.settings.ignite_host, self.settings.ignite_port)
        ])
        
        # Create caches
        self.policies_cache = self.ignite_client.get_or_create_cache('scaling_policies')
        self.decisions_cache = self.ignite_client.get_or_create_cache('scaling_decisions')
        self.history_cache = self.ignite_client.get_or_create_cache('scaling_history')
        
        # Initialize Consul client
        self.consul_client = consul.Consul(
            host=self.settings.consul_host,
            port=self.settings.consul_port
        )
        
        # Initialize Kubernetes client
        try:
            k8s_config.load_incluster_config()
        except:
            k8s_config.load_kube_config()
        
        self.k8s_apps_v1 = k8s_client.AppsV1Api()
        self.k8s_v1 = k8s_client.CoreV1Api()
        
        # Initialize HTTP client
        self.http_client = httpx.AsyncClient()
        
        # Initialize predictive scaler
        if self.settings.enable_predictive_scaling:
            self.predictive_scaler = PredictiveScaler()
            await self.predictive_scaler.initialize()
        
        # Load default policies from Consul
        await self._load_default_policies()
        
        logger.info("Scaling engine initialized")
    
    async def start(self):
        """Start scaling engine"""
        self._running = True
        logger.info("Starting scaling engine")
        
        # Start evaluation loop
        self._tasks = [
            asyncio.create_task(self._scaling_evaluation_loop()),
            asyncio.create_task(self._model_training_loop()) if self.settings.enable_predictive_scaling else None
        ]
        self._tasks = [t for t in self._tasks if t is not None]
    
    async def stop(self):
        """Stop scaling engine"""
        self._running = False
        for task in self._tasks:
            task.cancel()
        await asyncio.gather(*self._tasks, return_exceptions=True)
        
        # Close connections
        await self.http_client.aclose()
        self.ignite_client.close()
        
        logger.info("Scaling engine stopped")
    
    async def evaluate_scaling(
        self,
        service_name: str,
        policy: ScalingPolicy,
        metrics: ResourceMetrics
    ) -> Optional[ScalingDecision]:
        """Evaluate if scaling is needed"""
        # Check cooldown
        if not await self._check_cooldown(service_name, policy):
            return None
        
        # Get current deployment info
        deployment = await self._get_deployment(service_name, metrics.namespace)
        if not deployment:
            logger.warning(f"Deployment not found for {service_name}")
            return None
        
        current_replicas = deployment.spec.replicas
        
        # Initialize decision
        decision = ScalingDecision(
            service_name=service_name,
            namespace=metrics.namespace,
            action=ScalingAction.NO_ACTION,
            current_replicas=current_replicas
        )
        
        # Evaluate horizontal scaling
        horizontal_decision = await self._evaluate_horizontal_scaling(
            metrics, policy, current_replicas
        )
        
        # Evaluate vertical scaling if enabled
        vertical_decision = None
        if policy.enable_vertical_scaling:
            vertical_decision = await self._evaluate_vertical_scaling(
                metrics, policy, deployment
            )
        
        # Evaluate predictive scaling if enabled
        predictive_decision = None
        if policy.enable_predictive_scaling and self.predictive_scaler:
            predictive_decision = await self._evaluate_predictive_scaling(
                service_name, metrics, policy, current_replicas
            )
        
        # Choose the best decision
        final_decision = self._choose_best_decision(
            horizontal_decision,
            vertical_decision,
            predictive_decision,
            policy
        )
        
        if final_decision and final_decision.action != ScalingAction.NO_ACTION:
            decision = final_decision
            
            # Estimate cost impact if cost-aware
            if policy.cost_aware:
                cost_impact = await self._estimate_cost_impact(decision)
                decision.estimated_cost_impact = cost_impact
        
        return decision if decision.action != ScalingAction.NO_ACTION else None
    
    async def apply_scaling_decision(self, decision: ScalingDecision) -> bool:
        """Apply a scaling decision"""
        if self.settings.dry_run_mode:
            logger.info(f"DRY RUN: Would apply scaling decision: {decision}")
            decision.applied = True
            decision.applied_at = datetime.utcnow()
            await self._store_decision(decision)
            return True
        
        try:
            # Apply horizontal scaling
            if decision.target_replicas is not None:
                success = await self._apply_horizontal_scaling(
                    decision.service_name,
                    decision.namespace,
                    decision.target_replicas
                )
                if not success:
                    return False
            
            # Apply vertical scaling
            if decision.target_cpu_limit or decision.target_memory_limit:
                success = await self._apply_vertical_scaling(
                    decision.service_name,
                    decision.namespace,
                    decision.target_cpu_limit,
                    decision.target_memory_limit
                )
                if not success:
                    return False
            
            # Mark as applied
            decision.applied = True
            decision.applied_at = datetime.utcnow()
            
            # Update cooldown tracker
            self._cooldown_tracker[decision.service_name] = datetime.utcnow()
            
            # Store decision
            await self._store_decision(decision)
            
            logger.info(f"Applied scaling decision for {decision.service_name}: {decision.action}")
            return True
            
        except Exception as e:
            logger.error(f"Failed to apply scaling decision: {e}")
            return False
    
    async def get_scaling_policy(self, service_name: str) -> Optional[ScalingPolicy]:
        """Get scaling policy for a service"""
        # Check cache first
        if self.policies_cache.contains_key(service_name):
            policy_dict = self.policies_cache.get(service_name)
            return ScalingPolicy(**policy_dict)
        
        # Try to load from Consul
        key = f"platformq/scaling-policies/{service_name}"
        _, data = self.consul_client.kv.get(key)
        
        if data:
            policy_dict = json.loads(data['Value'].decode('utf-8'))
            policy = ScalingPolicy(**policy_dict)
            self.policies_cache.put(service_name, policy.dict())
            return policy
        
        # Return default policy
        return ScalingPolicy(service_name=service_name)
    
    async def update_scaling_policy(self, policy: ScalingPolicy) -> bool:
        """Update scaling policy for a service"""
        try:
            # Store in Consul
            key = f"platformq/scaling-policies/{policy.service_name}"
            self.consul_client.kv.put(key, json.dumps(policy.dict()))
            
            # Update cache
            self.policies_cache.put(policy.service_name, policy.dict())
            
            # Update timestamp
            policy.updated_at = datetime.utcnow()
            
            logger.info(f"Updated scaling policy for {policy.service_name}")
            return True
            
        except Exception as e:
            logger.error(f"Failed to update scaling policy: {e}")
            return False
    
    async def get_recent_decisions(
        self,
        service_name: Optional[str] = None,
        hours: int = 24
    ) -> List[ScalingDecision]:
        """Get recent scaling decisions"""
        decisions = []
        cutoff = datetime.utcnow() - timedelta(hours=hours)
        
        for key in self.decisions_cache.keys():
            decision_dict = self.decisions_cache.get(key)
            decision = ScalingDecision(**decision_dict)
            
            if decision.timestamp >= cutoff:
                if service_name is None or decision.service_name == service_name:
                    decisions.append(decision)
        
        return sorted(decisions, key=lambda d: d.timestamp, reverse=True)
    
    async def _scaling_evaluation_loop(self):
        """Main scaling evaluation loop"""
        while self._running:
            try:
                # Get all services with scaling policies
                services = await self._get_services_to_scale()
                
                # Evaluate each service
                for service_name, namespace in services:
                    try:
                        await self._evaluate_service_scaling(service_name, namespace)
                    except Exception as e:
                        logger.error(f"Error evaluating {service_name}: {e}")
                        
            except Exception as e:
                logger.error(f"Error in scaling evaluation loop: {e}")
            
            await asyncio.sleep(self.settings.evaluation_interval)
    
    async def _evaluate_service_scaling(self, service_name: str, namespace: str):
        """Evaluate scaling for a specific service"""
        # Get scaling policy
        policy = await self.get_scaling_policy(service_name)
        if not policy:
            return
        
        # Get current metrics from monitoring service
        metrics = await self._get_service_metrics(service_name, namespace)
        if not metrics:
            logger.warning(f"No metrics available for {service_name}")
            return
        
        # Evaluate scaling
        decision = await self.evaluate_scaling(service_name, policy, metrics)
        
        if decision:
            # Apply decision
            success = await self.apply_scaling_decision(decision)
            if success:
                logger.info(f"Scaled {service_name}: {decision.action}")
    
    async def _get_service_metrics(
        self,
        service_name: str,
        namespace: str
    ) -> Optional[ResourceMetrics]:
        """Get metrics from monitoring service"""
        try:
            url = f"{self.settings.monitoring_service_url}/api/v1/metrics/service/{service_name}"
            params = {"namespace": namespace}
            
            response = await self.http_client.get(url, params=params)
            if response.status_code == 200:
                return ResourceMetrics(**response.json())
                
        except Exception as e:
            logger.error(f"Failed to get metrics for {service_name}: {e}")
        
        return None
    
    async def _get_deployment(self, service_name: str, namespace: str):
        """Get Kubernetes deployment"""
        try:
            return self.k8s_apps_v1.read_namespaced_deployment(
                name=service_name,
                namespace=namespace
            )
        except Exception as e:
            logger.error(f"Failed to get deployment {service_name}: {e}")
            return None
    
    async def _check_cooldown(
        self,
        service_name: str,
        policy: ScalingPolicy
    ) -> bool:
        """Check if service is in cooldown period"""
        last_scaling = self._cooldown_tracker.get(service_name)
        if not last_scaling:
            return True
        
        elapsed = (datetime.utcnow() - last_scaling).total_seconds()
        return elapsed >= policy.cooldown_seconds
    
    async def _evaluate_horizontal_scaling(
        self,
        metrics: ResourceMetrics,
        policy: ScalingPolicy,
        current_replicas: int
    ) -> Optional[ScalingDecision]:
        """Evaluate horizontal scaling needs"""
        decision = ScalingDecision(
            service_name=metrics.service_name,
            namespace=metrics.namespace,
            action=ScalingAction.NO_ACTION,
            current_replicas=current_replicas
        )
        
        # Check CPU threshold
        if metrics.cpu_usage > policy.scale_up_threshold:
            # Scale up
            target_replicas = min(
                int(current_replicas * policy.scale_up_rate),
                policy.max_replicas
            )
            
            if target_replicas > current_replicas:
                decision.action = ScalingAction.SCALE_OUT
                decision.target_replicas = target_replicas
                decision.reason = f"CPU usage {metrics.cpu_usage:.1f}% > threshold {policy.scale_up_threshold}%"
                
        elif metrics.cpu_usage < policy.scale_down_threshold:
            # Scale down
            target_replicas = max(
                int(current_replicas * policy.scale_down_rate),
                policy.min_replicas
            )
            
            if target_replicas < current_replicas:
                decision.action = ScalingAction.SCALE_IN
                decision.target_replicas = target_replicas
                decision.reason = f"CPU usage {metrics.cpu_usage:.1f}% < threshold {policy.scale_down_threshold}%"
        
        # Check memory threshold
        if metrics.memory_usage > policy.target_memory_utilization:
            # Memory-based scaling
            if decision.action == ScalingAction.NO_ACTION:
                target_replicas = min(
                    current_replicas + 1,
                    policy.max_replicas
                )
                
                if target_replicas > current_replicas:
                    decision.action = ScalingAction.SCALE_OUT
                    decision.target_replicas = target_replicas
                    decision.reason = f"Memory usage {metrics.memory_usage:.1f}% > target {policy.target_memory_utilization}%"
        
        return decision if decision.action != ScalingAction.NO_ACTION else None
    
    async def _evaluate_vertical_scaling(
        self,
        metrics: ResourceMetrics,
        policy: ScalingPolicy,
        deployment
    ) -> Optional[ScalingDecision]:
        """Evaluate vertical scaling needs"""
        # TODO: Implement vertical scaling logic
        return None
    
    async def _evaluate_predictive_scaling(
        self,
        service_name: str,
        metrics: ResourceMetrics,
        policy: ScalingPolicy,
        current_replicas: int
    ) -> Optional[ScalingDecision]:
        """Evaluate predictive scaling needs"""
        if not self.predictive_scaler:
            return None
        
        # Get prediction
        predicted_load = await self.predictive_scaler.predict_load(
            service_name,
            horizon_minutes=self.settings.prediction_horizon_minutes
        )
        
        if predicted_load is None:
            return None
        
        # Calculate needed replicas based on prediction
        # Assuming linear relationship between load and replicas
        current_load = metrics.cpu_usage
        if current_load > 0:
            load_ratio = predicted_load / current_load
            predicted_replicas = int(current_replicas * load_ratio)
            
            # Apply policy limits
            predicted_replicas = max(policy.min_replicas, min(predicted_replicas, policy.max_replicas))
            
            if predicted_replicas != current_replicas:
                decision = ScalingDecision(
                    service_name=service_name,
                    namespace=metrics.namespace,
                    action=ScalingAction.SCALE_OUT if predicted_replicas > current_replicas else ScalingAction.SCALE_IN,
                    current_replicas=current_replicas,
                    target_replicas=predicted_replicas,
                    reason=f"Predictive scaling: expected load {predicted_load:.1f}% in {self.settings.prediction_horizon_minutes} minutes",
                    confidence=0.8  # TODO: Get confidence from model
                )
                return decision
        
        return None
    
    def _choose_best_decision(
        self,
        horizontal: Optional[ScalingDecision],
        vertical: Optional[ScalingDecision],
        predictive: Optional[ScalingDecision],
        policy: ScalingPolicy
    ) -> Optional[ScalingDecision]:
        """Choose the best scaling decision from multiple options"""
        decisions = [d for d in [horizontal, vertical, predictive] if d is not None]
        
        if not decisions:
            return None
        
        # If only one decision, return it
        if len(decisions) == 1:
            return decisions[0]
        
        # Priority: Immediate need > Predictive
        # For now, prefer horizontal scaling for immediate needs
        if horizontal and horizontal.action != ScalingAction.NO_ACTION:
            return horizontal
        
        # Use predictive if no immediate need
        if predictive and predictive.confidence > 0.7:
            return predictive
        
        return vertical
    
    async def _apply_horizontal_scaling(
        self,
        service_name: str,
        namespace: str,
        target_replicas: int
    ) -> bool:
        """Apply horizontal scaling to deployment"""
        try:
            # Update deployment replicas
            self.k8s_apps_v1.patch_namespaced_deployment_scale(
                name=service_name,
                namespace=namespace,
                body={"spec": {"replicas": target_replicas}}
            )
            return True
        except Exception as e:
            logger.error(f"Failed to scale {service_name} to {target_replicas} replicas: {e}")
            return False
    
    async def _apply_vertical_scaling(
        self,
        service_name: str,
        namespace: str,
        cpu_limit: Optional[str],
        memory_limit: Optional[str]
    ) -> bool:
        """Apply vertical scaling to deployment"""
        # TODO: Implement vertical scaling
        return True
    
    async def _estimate_cost_impact(self, decision: ScalingDecision) -> float:
        """Estimate the cost impact of a scaling decision"""
        # TODO: Implement cost estimation
        # For now, return a dummy value
        if decision.action == ScalingAction.SCALE_OUT:
            return 50.0 * (decision.target_replicas - decision.current_replicas)
        elif decision.action == ScalingAction.SCALE_IN:
            return -50.0 * (decision.current_replicas - decision.target_replicas)
        return 0.0
    
    async def _store_decision(self, decision: ScalingDecision):
        """Store scaling decision in cache"""
        key = f"{decision.service_name}:{decision.decision_id}"
        self.decisions_cache.put(key, decision.dict())
        
        # Also store in history
        history_key = f"{decision.service_name}:{decision.timestamp.isoformat()}"
        self.history_cache.put(history_key, decision.dict())
    
    async def _get_services_to_scale(self) -> List[tuple]:
        """Get list of services that have scaling policies"""
        services = []
        
        # Get all deployments in namespace
        try:
            deployments = self.k8s_apps_v1.list_namespaced_deployment(
                namespace=self.settings.kubernetes_namespace
            )
            
            for deployment in deployments.items:
                # Check if service has scaling policy
                service_name = deployment.metadata.name
                if self.policies_cache.contains_key(service_name):
                    services.append((service_name, deployment.metadata.namespace))
                    
        except Exception as e:
            logger.error(f"Failed to list deployments: {e}")
        
        return services
    
    async def _load_default_policies(self):
        """Load default scaling policies from Consul"""
        try:
            # Get all policies from Consul
            _, policies = self.consul_client.kv.get("platformq/scaling-policies/", recurse=True)
            
            if policies:
                for item in policies:
                    if item['Value']:
                        policy_dict = json.loads(item['Value'].decode('utf-8'))
                        policy = ScalingPolicy(**policy_dict)
                        self.policies_cache.put(policy.service_name, policy.dict())
                        
                logger.info(f"Loaded {len(policies)} scaling policies from Consul")
                
        except Exception as e:
            logger.error(f"Failed to load default policies: {e}")
    
    async def _model_training_loop(self):
        """Periodically train predictive models"""
        while self._running:
            try:
                if self.predictive_scaler:
                    await self.predictive_scaler.train_models()
            except Exception as e:
                logger.error(f"Error in model training: {e}")
            
            await asyncio.sleep(self.settings.model_training_interval) 