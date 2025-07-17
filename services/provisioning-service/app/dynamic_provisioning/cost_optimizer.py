"""Cost Optimizer for Dynamic Resource Provisioning

Optimizes resource allocation decisions based on cost considerations.
"""

import logging
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Tuple
from dataclasses import dataclass
import numpy as np

from pyignite import Client as IgniteClient

from .scaling_engine import ScalingDecision, ScalingAction
from .resource_monitor import ResourceMetrics, ClusterMetrics

logger = logging.getLogger(__name__)


@dataclass
class ResourceCost:
    """Container for resource cost information"""
    cpu_core_hour: float = 0.05  # Cost per CPU core per hour
    memory_gb_hour: float = 0.01  # Cost per GB memory per hour
    storage_gb_month: float = 0.10  # Cost per GB storage per month
    network_gb: float = 0.09  # Cost per GB network transfer
    gpu_hour: float = 0.90  # Cost per GPU per hour
    
    # Spot/preemptible pricing (usually 60-80% cheaper)
    spot_discount: float = 0.3  # 30% of regular price
    
    # Reserved instance discount
    reserved_discount: float = 0.6  # 60% of regular price for 1-year commitment
    
    # Multi-region costs
    cross_region_transfer_gb: float = 0.02


@dataclass
class CostAnalysis:
    """Container for cost analysis results"""
    service_name: str
    current_monthly_cost: float
    projected_monthly_cost: float
    cost_savings: float
    optimization_recommendations: List[str]
    confidence: float
    timestamp: datetime = None
    
    def __post_init__(self):
        if self.timestamp is None:
            self.timestamp = datetime.utcnow()


class CostOptimizer:
    """Cost optimizer for resource provisioning
    
    Features:
    - Real-time cost calculation
    - Cost-aware scaling decisions
    - Spot instance recommendations
    - Reserved capacity planning
    - Budget enforcement
    - Cost anomaly detection
    """
    
    def __init__(self, ignite_client: IgniteClient):
        self.ignite_client = ignite_client
        
        # Create caches
        self.cost_cache = ignite_client.get_or_create_cache('resource_costs')
        self.budget_cache = ignite_client.get_or_create_cache('budgets')
        self.cost_history_cache = ignite_client.get_or_create_cache('cost_history')
        self.recommendations_cache = ignite_client.get_or_create_cache('cost_recommendations')
        
        # Load cost configuration
        self.resource_cost = ResourceCost()
        self._load_cost_config()
    
    def _load_cost_config(self):
        """Load cost configuration from cache or use defaults"""
        if self.cost_cache.contains_key('config'):
            config = self.cost_cache.get('config')
            self.resource_cost = ResourceCost(**config)
        else:
            # Save default config
            self.cost_cache.put('config', self.resource_cost.__dict__)
    
    async def optimize_decision(
        self,
        decision: ScalingDecision,
        metrics: ResourceMetrics,
        cluster_metrics: Optional[ClusterMetrics] = None
    ) -> Optional[ScalingDecision]:
        """Optimize a scaling decision based on cost"""
        try:
            # Calculate current cost
            current_cost = self._calculate_service_cost(
                decision.service_name,
                decision.current_replicas,
                decision.current_cpu_limit,
                decision.current_memory_limit,
                metrics
            )
            
            # Calculate projected cost
            projected_cost = self._calculate_service_cost(
                decision.service_name,
                decision.target_replicas or decision.current_replicas,
                decision.target_cpu_limit or decision.current_cpu_limit,
                decision.target_memory_limit or decision.current_memory_limit,
                metrics
            )
            
            # Check budget constraints
            if not self._check_budget_constraint(decision.service_name, projected_cost):
                logger.warning(f"Budget constraint violated for {decision.service_name}")
                # Reduce scaling to stay within budget
                decision = self._adjust_for_budget(decision, current_cost)
            
            # Optimize for cost efficiency
            if decision.action in [ScalingAction.SCALE_OUT, ScalingAction.VERTICAL_SCALE]:
                # Check if spot instances are available
                if cluster_metrics and self._should_use_spot_instances(metrics, cluster_metrics):
                    decision.reason += " (using spot instances for cost savings)"
                    projected_cost *= self.resource_cost.spot_discount
            
            # Set cost impact
            decision.estimated_cost_impact = projected_cost - current_cost
            
            # Log cost analysis
            self._log_cost_analysis(
                decision.service_name,
                current_cost,
                projected_cost,
                decision
            )
            
            return decision
            
        except Exception as e:
            logger.error(f"Failed to optimize decision for cost: {e}")
            return decision
    
    def _calculate_service_cost(
        self,
        service_name: str,
        replicas: int,
        cpu_limit: str,
        memory_limit: str,
        metrics: ResourceMetrics
    ) -> float:
        """Calculate hourly cost for a service"""
        # Parse resource limits
        cpu_cores = self._parse_cpu_limit(cpu_limit)
        memory_gb = self._parse_memory_limit(memory_limit)
        
        # Base compute cost
        compute_cost = (
            cpu_cores * self.resource_cost.cpu_core_hour +
            memory_gb * self.resource_cost.memory_gb_hour
        ) * replicas
        
        # Add GPU cost if applicable
        if metrics.gpu_usage is not None and metrics.gpu_usage > 0:
            # Assume 1 GPU per replica if GPU is used
            compute_cost += self.resource_cost.gpu_hour * replicas
        
        # Estimate network cost based on request rate
        # Assume 1KB per request average
        network_gb_hour = (metrics.request_rate * 3600 * 1024) / (1024**3)
        network_cost = network_gb_hour * self.resource_cost.network_gb
        
        # Storage cost (estimated)
        if metrics.storage_usage_bytes:
            storage_gb = metrics.storage_usage_bytes / (1024**3)
            storage_cost_hour = (storage_gb * self.resource_cost.storage_gb_month) / (30 * 24)
        else:
            storage_cost_hour = 0
        
        total_hourly_cost = compute_cost + network_cost + storage_cost_hour
        
        return total_hourly_cost
    
    def _parse_cpu_limit(self, cpu_limit: str) -> float:
        """Parse CPU limit string to cores"""
        if not cpu_limit:
            return 1.0
        
        if cpu_limit.endswith('m'):
            return float(cpu_limit[:-1]) / 1000
        else:
            return float(cpu_limit)
    
    def _parse_memory_limit(self, memory_limit: str) -> float:
        """Parse memory limit string to GB"""
        if not memory_limit:
            return 1.0
        
        if memory_limit.endswith('Gi'):
            return float(memory_limit[:-2])
        elif memory_limit.endswith('Mi'):
            return float(memory_limit[:-2]) / 1024
        elif memory_limit.endswith('Ki'):
            return float(memory_limit[:-2]) / (1024**2)
        else:
            return float(memory_limit)
    
    def _check_budget_constraint(self, service_name: str, hourly_cost: float) -> bool:
        """Check if cost is within budget"""
        budget_key = f"budget:{service_name}"
        
        if self.budget_cache.contains_key(budget_key):
            budget = self.budget_cache.get(budget_key)
            monthly_cost = hourly_cost * 24 * 30
            
            return monthly_cost <= budget['monthly_limit']
        
        # No budget constraint
        return True
    
    def _adjust_for_budget(
        self,
        decision: ScalingDecision,
        current_cost: float
    ) -> ScalingDecision:
        """Adjust scaling decision to stay within budget"""
        budget_key = f"budget:{decision.service_name}"
        
        if not self.budget_cache.contains_key(budget_key):
            return decision
        
        budget = self.budget_cache.get(budget_key)
        monthly_budget = budget['monthly_limit']
        max_hourly_cost = monthly_budget / (30 * 24)
        
        if decision.action == ScalingAction.SCALE_OUT:
            # Calculate max replicas within budget
            cost_per_replica = current_cost / decision.current_replicas
            max_replicas = int(max_hourly_cost / cost_per_replica)
            
            if max_replicas > decision.current_replicas:
                decision.target_replicas = min(decision.target_replicas, max_replicas)
            else:
                # Can't scale out within budget
                decision.action = ScalingAction.NO_ACTION
                decision.reason = "Budget constraint prevents scaling"
        
        elif decision.action == ScalingAction.VERTICAL_SCALE:
            # Reduce resource increase to stay within budget
            decision.reason += " (reduced due to budget constraint)"
            # Simple approach: scale back by 50%
            if decision.target_cpu_limit:
                current = self._parse_cpu_limit(decision.current_cpu_limit)
                target = self._parse_cpu_limit(decision.target_cpu_limit)
                new_target = current + (target - current) * 0.5
                decision.target_cpu_limit = f"{new_target:.2f}"
            
            if decision.target_memory_limit:
                current = self._parse_memory_limit(decision.current_memory_limit)
                target = self._parse_memory_limit(decision.target_memory_limit)
                new_target = current + (target - current) * 0.5
                decision.target_memory_limit = f"{new_target:.1f}Gi"
        
        return decision
    
    def _should_use_spot_instances(
        self,
        metrics: ResourceMetrics,
        cluster_metrics: ClusterMetrics
    ) -> bool:
        """Determine if spot instances should be used"""
        # Use spot instances for:
        # 1. Non-critical services (low error rate tolerance)
        # 2. Stateless workloads
        # 3. When cluster has capacity
        
        if metrics.error_rate > 5:  # Critical service
            return False
        
        if cluster_metrics.used_cpu_cores / cluster_metrics.total_cpu_cores > 0.8:
            return False  # Cluster is too busy
        
        # Check if service is stateless (simplified check)
        stateless_services = [
            'simulation-service',
            'federated-learning-service',
            'workflow-service'
        ]
        
        return metrics.service_name in stateless_services
    
    def _log_cost_analysis(
        self,
        service_name: str,
        current_cost: float,
        projected_cost: float,
        decision: ScalingDecision
    ):
        """Log cost analysis for tracking"""
        analysis = {
            'service_name': service_name,
            'timestamp': datetime.utcnow().isoformat(),
            'current_hourly_cost': current_cost,
            'projected_hourly_cost': projected_cost,
            'cost_delta': projected_cost - current_cost,
            'scaling_action': decision.action.value,
            'scaling_reason': decision.reason
        }
        
        # Store in history
        history_key = f"{service_name}:{datetime.utcnow().isoformat()}"
        self.cost_history_cache.put(history_key, analysis)
    
    async def analyze_and_optimize(
        self,
        resource_monitor,
        policies_cache,
        decisions_cache
    ):
        """Periodic cost analysis and optimization"""
        try:
            all_services = []
            total_current_cost = 0
            total_optimized_cost = 0
            recommendations = []
            
            # Analyze each service
            for service_key in policies_cache.keys():
                policy = policies_cache.get(service_key)
                service_name = policy.service_name
                
                # Get current metrics
                metrics = resource_monitor.get_current_metrics(service_name)
                if not metrics:
                    continue
                
                # Calculate current cost
                current_cost = self._calculate_service_cost(
                    service_name,
                    metrics.pod_count,
                    "1",  # Default if not available
                    "1Gi",  # Default if not available
                    metrics
                )
                
                total_current_cost += current_cost
                
                # Generate optimization recommendations
                service_recommendations = self._generate_recommendations(
                    service_name,
                    metrics,
                    current_cost
                )
                
                recommendations.extend(service_recommendations)
                
                # Calculate optimized cost
                optimized_cost = self._calculate_optimized_cost(
                    service_name,
                    metrics,
                    service_recommendations
                )
                
                total_optimized_cost += optimized_cost
                
                # Create cost analysis
                analysis = CostAnalysis(
                    service_name=service_name,
                    current_monthly_cost=current_cost * 24 * 30,
                    projected_monthly_cost=optimized_cost * 24 * 30,
                    cost_savings=(current_cost - optimized_cost) * 24 * 30,
                    optimization_recommendations=[r['description'] for r in service_recommendations],
                    confidence=0.85
                )
                
                # Store analysis
                self.recommendations_cache.put(service_name, analysis)
            
            # Log summary
            total_monthly_current = total_current_cost * 24 * 30
            total_monthly_optimized = total_optimized_cost * 24 * 30
            total_savings = total_monthly_current - total_monthly_optimized
            
            logger.info(
                f"Cost optimization analysis complete: "
                f"Current: ${total_monthly_current:.2f}/month, "
                f"Optimized: ${total_monthly_optimized:.2f}/month, "
                f"Potential savings: ${total_savings:.2f}/month ({total_savings/total_monthly_current*100:.1f}%)"
            )
            
        except Exception as e:
            logger.error(f"Failed to analyze and optimize costs: {e}")
    
    def _generate_recommendations(
        self,
        service_name: str,
        metrics: ResourceMetrics,
        current_cost: float
    ) -> List[Dict]:
        """Generate cost optimization recommendations"""
        recommendations = []
        
        # Check CPU utilization
        if metrics.cpu_usage < 30:
            recommendations.append({
                'type': 'downsize',
                'description': f"CPU usage is only {metrics.cpu_usage:.1f}%. Consider reducing CPU allocation.",
                'estimated_savings': current_cost * 0.2
            })
        
        # Check memory utilization
        if metrics.memory_usage < 40:
            recommendations.append({
                'type': 'downsize',
                'description': f"Memory usage is only {metrics.memory_usage:.1f}%. Consider reducing memory allocation.",
                'estimated_savings': current_cost * 0.1
            })
        
        # Check for spot instance eligibility
        if service_name in ['simulation-service', 'federated-learning-service']:
            recommendations.append({
                'type': 'spot_instances',
                'description': "Consider using spot instances for this stateless workload.",
                'estimated_savings': current_cost * (1 - self.resource_cost.spot_discount)
            })
        
        # Check for reserved instance eligibility
        if metrics.pod_count >= 3:  # Stable workload
            recommendations.append({
                'type': 'reserved_instances',
                'description': "Consider reserved instances for predictable workload.",
                'estimated_savings': current_cost * (1 - self.resource_cost.reserved_discount)
            })
        
        # Check response time vs resource usage
        if metrics.response_time_p99 < 100 and metrics.cpu_usage < 50:
            recommendations.append({
                'type': 'consolidation',
                'description': "Low latency with low resource usage. Consider consolidating workloads.",
                'estimated_savings': current_cost * 0.15
            })
        
        return recommendations
    
    def _calculate_optimized_cost(
        self,
        service_name: str,
        metrics: ResourceMetrics,
        recommendations: List[Dict]
    ) -> float:
        """Calculate cost after applying recommendations"""
        # Start with current cost (simplified)
        base_cost = self._calculate_service_cost(
            service_name,
            metrics.pod_count,
            "1",
            "1Gi",
            metrics
        )
        
        # Apply recommendation savings
        total_savings_rate = 0
        for rec in recommendations:
            if rec['type'] == 'spot_instances':
                total_savings_rate = max(total_savings_rate, 1 - self.resource_cost.spot_discount)
            elif rec['type'] == 'reserved_instances':
                total_savings_rate = max(total_savings_rate, 1 - self.resource_cost.reserved_discount)
            elif rec['type'] in ['downsize', 'consolidation']:
                total_savings_rate += 0.1  # 10% additional savings
        
        # Cap total savings at 80%
        total_savings_rate = min(total_savings_rate, 0.8)
        
        return base_cost * (1 - total_savings_rate)
    
    def set_budget(self, service_name: str, monthly_limit: float):
        """Set budget for a service"""
        budget = {
            'service_name': service_name,
            'monthly_limit': monthly_limit,
            'created_at': datetime.utcnow().isoformat(),
            'currency': 'USD'
        }
        
        self.budget_cache.put(f"budget:{service_name}", budget)
        logger.info(f"Set budget for {service_name}: ${monthly_limit}/month")
    
    def get_cost_report(
        self,
        start_date: datetime,
        end_date: datetime,
        service_name: Optional[str] = None
    ) -> Dict:
        """Generate cost report for date range"""
        report = {
            'start_date': start_date.isoformat(),
            'end_date': end_date.isoformat(),
            'services': {},
            'total_cost': 0,
            'recommendations': []
        }
        
        # Get cost history
        for key in self.cost_history_cache.keys():
            parts = key.split(':')
            if len(parts) == 2:
                svc_name, timestamp_str = parts
                
                if service_name and svc_name != service_name:
                    continue
                
                timestamp = datetime.fromisoformat(timestamp_str)
                if start_date <= timestamp <= end_date:
                    analysis = self.cost_history_cache.get(key)
                    
                    if svc_name not in report['services']:
                        report['services'][svc_name] = {
                            'entries': [],
                            'total_cost': 0
                        }
                    
                    report['services'][svc_name]['entries'].append(analysis)
                    
                    # Calculate cost for the period
                    hours = (end_date - start_date).total_seconds() / 3600
                    cost = analysis['current_hourly_cost'] * hours
                    report['services'][svc_name]['total_cost'] += cost
                    report['total_cost'] += cost
        
        # Add recommendations
        for key in self.recommendations_cache.keys():
            if not service_name or key == service_name:
                recommendations = self.recommendations_cache.get(key)
                report['recommendations'].append(recommendations)
        
        return report