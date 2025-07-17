"""Tenant Resource Manager for Dynamic Provisioning

Manages resource allocation and limits for multi-tenant environments.
"""

import logging
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Set, Tuple
from dataclasses import dataclass
from enum import Enum
import asyncio

from pyignite import Client as IgniteClient
from kubernetes import client as k8s_client, config as k8s_config

logger = logging.getLogger(__name__)


class TenantTier(Enum):
    """Tenant subscription tiers"""
    FREE = "free"
    STARTER = "starter"
    PROFESSIONAL = "professional"
    ENTERPRISE = "enterprise"
    CUSTOM = "custom"


@dataclass
class TenantResourceQuota:
    """Resource quota for a tenant"""
    tenant_id: str
    tier: TenantTier
    max_cpu_cores: int
    max_memory_gb: int
    max_storage_gb: int
    max_pods: int
    max_services: int
    max_gpu_count: int = 0
    max_monthly_cost: float = 0.0  # 0 means unlimited
    priority: int = 1  # Higher priority gets resources first
    
    # Burst limits (temporary resource increases)
    burst_cpu_cores: Optional[int] = None
    burst_memory_gb: Optional[int] = None
    burst_duration_hours: int = 24
    
    # Fair share weights
    cpu_weight: float = 1.0
    memory_weight: float = 1.0


@dataclass
class TenantResourceUsage:
    """Current resource usage for a tenant"""
    tenant_id: str
    timestamp: datetime
    cpu_cores_used: float
    memory_gb_used: float
    storage_gb_used: float
    pod_count: int
    service_count: int
    gpu_count_used: int
    monthly_cost: float
    
    # Usage trends
    cpu_usage_trend: float = 0.0  # Percentage change over last hour
    memory_usage_trend: float = 0.0
    cost_trend: float = 0.0


class TenantResourceManager:
    """Manages resource allocation across tenants
    
    Features:
    - Tenant quota enforcement
    - Fair resource sharing
    - Burst capacity management
    - Resource isolation
    - Usage tracking and billing
    - Priority-based allocation
    """
    
    def __init__(self, ignite_client: IgniteClient):
        self.ignite_client = ignite_client
        
        # Create caches
        self.quotas_cache = ignite_client.get_or_create_cache('tenant_quotas')
        self.usage_cache = ignite_client.get_or_create_cache('tenant_usage')
        self.burst_cache = ignite_client.get_or_create_cache('tenant_bursts')
        self.allocations_cache = ignite_client.get_or_create_cache('resource_allocations')
        
        # Initialize Kubernetes client
        try:
            k8s_config.load_incluster_config()
        except:
            k8s_config.load_kube_config()
        
        self.k8s_core = k8s_client.CoreV1Api()
        self.k8s_apps = k8s_client.AppsV1Api()
        
        # Default tier quotas
        self.tier_quotas = self._get_default_tier_quotas()
        
        self._running = False
        self._tasks = []
    
    def _get_default_tier_quotas(self) -> Dict[TenantTier, Dict]:
        """Get default resource quotas for each tier"""
        return {
            TenantTier.FREE: {
                'max_cpu_cores': 2,
                'max_memory_gb': 4,
                'max_storage_gb': 10,
                'max_pods': 5,
                'max_services': 3,
                'max_gpu_count': 0,
                'max_monthly_cost': 0,  # Free tier
                'priority': 1
            },
            TenantTier.STARTER: {
                'max_cpu_cores': 8,
                'max_memory_gb': 16,
                'max_storage_gb': 100,
                'max_pods': 20,
                'max_services': 10,
                'max_gpu_count': 0,
                'max_monthly_cost': 100,
                'priority': 2,
                'burst_cpu_cores': 12,
                'burst_memory_gb': 24
            },
            TenantTier.PROFESSIONAL: {
                'max_cpu_cores': 32,
                'max_memory_gb': 64,
                'max_storage_gb': 500,
                'max_pods': 50,
                'max_services': 25,
                'max_gpu_count': 2,
                'max_monthly_cost': 500,
                'priority': 3,
                'burst_cpu_cores': 48,
                'burst_memory_gb': 96
            },
            TenantTier.ENTERPRISE: {
                'max_cpu_cores': 128,
                'max_memory_gb': 256,
                'max_storage_gb': 2000,
                'max_pods': 200,
                'max_services': 100,
                'max_gpu_count': 8,
                'max_monthly_cost': 0,  # Unlimited
                'priority': 5,
                'burst_cpu_cores': 256,
                'burst_memory_gb': 512
            }
        }
    
    async def start(self):
        """Start the tenant resource manager"""
        self._running = True
        logger.info("Starting tenant resource manager")
        
        # Start background tasks
        self._tasks = [
            asyncio.create_task(self._usage_tracking_loop()),
            asyncio.create_task(self._quota_enforcement_loop()),
            asyncio.create_task(self._burst_management_loop())
        ]
    
    async def stop(self):
        """Stop the tenant resource manager"""
        self._running = False
        for task in self._tasks:
            task.cancel()
        await asyncio.gather(*self._tasks, return_exceptions=True)
        logger.info("Tenant resource manager stopped")
    
    def create_tenant_quota(
        self,
        tenant_id: str,
        tier: TenantTier,
        custom_limits: Optional[Dict] = None
    ) -> TenantResourceQuota:
        """Create resource quota for a tenant"""
        # Get base quota from tier
        base_quota = self.tier_quotas[tier].copy()
        
        # Apply custom limits if provided
        if custom_limits:
            base_quota.update(custom_limits)
        
        # Create quota object
        quota = TenantResourceQuota(
            tenant_id=tenant_id,
            tier=tier,
            **base_quota
        )
        
        # Store in cache
        self.quotas_cache.put(tenant_id, quota)
        
        # Create Kubernetes namespace and resource quota
        self._create_kubernetes_resources(tenant_id, quota)
        
        logger.info(f"Created resource quota for tenant {tenant_id} with tier {tier.value}")
        
        return quota
    
    def _create_kubernetes_resources(self, tenant_id: str, quota: TenantResourceQuota):
        """Create Kubernetes namespace and resource quota"""
        namespace_name = f"tenant-{tenant_id}"
        
        try:
            # Create namespace
            namespace = k8s_client.V1Namespace(
                metadata=k8s_client.V1ObjectMeta(
                    name=namespace_name,
                    labels={
                        'tenant-id': tenant_id,
                        'tenant-tier': quota.tier.value,
                        'managed-by': 'platformq'
                    }
                )
            )
            self.k8s_core.create_namespace(namespace)
            
            # Create resource quota
            resource_quota = k8s_client.V1ResourceQuota(
                metadata=k8s_client.V1ObjectMeta(
                    name=f"{tenant_id}-quota",
                    namespace=namespace_name
                ),
                spec=k8s_client.V1ResourceQuotaSpec(
                    hard={
                        'requests.cpu': str(quota.max_cpu_cores),
                        'requests.memory': f"{quota.max_memory_gb}Gi",
                        'requests.storage': f"{quota.max_storage_gb}Gi",
                        'persistentvolumeclaims': str(quota.max_services * 2),  # Allow 2 PVCs per service
                        'pods': str(quota.max_pods),
                        'services': str(quota.max_services)
                    }
                )
            )
            self.k8s_core.create_namespaced_resource_quota(
                namespace=namespace_name,
                body=resource_quota
            )
            
            # Create limit range
            limit_range = k8s_client.V1LimitRange(
                metadata=k8s_client.V1ObjectMeta(
                    name=f"{tenant_id}-limits",
                    namespace=namespace_name
                ),
                spec=k8s_client.V1LimitRangeSpec(
                    limits=[
                        {
                            'type': 'Pod',
                            'max': {
                                'cpu': f"{quota.max_cpu_cores // max(quota.max_pods, 1)}",
                                'memory': f"{quota.max_memory_gb // max(quota.max_pods, 1)}Gi"
                            },
                            'min': {
                                'cpu': '100m',
                                'memory': '128Mi'
                            }
                        },
                        {
                            'type': 'Container',
                            'default': {
                                'cpu': '500m',
                                'memory': '512Mi'
                            },
                            'defaultRequest': {
                                'cpu': '100m',
                                'memory': '128Mi'
                            }
                        }
                    ]
                )
            )
            self.k8s_core.create_namespaced_limit_range(
                namespace=namespace_name,
                body=limit_range
            )
            
            logger.info(f"Created Kubernetes resources for tenant {tenant_id}")
            
        except k8s_client.ApiException as e:
            if e.status != 409:  # Not already exists
                logger.error(f"Failed to create Kubernetes resources for tenant {tenant_id}: {e}")
    
    async def get_tenant_usage(self, tenant_id: str) -> Optional[TenantResourceUsage]:
        """Get current resource usage for a tenant"""
        try:
            namespace_name = f"tenant-{tenant_id}"
            
            # Get pods in namespace
            pods = self.k8s_core.list_namespaced_pod(namespace=namespace_name)
            
            # Calculate resource usage
            cpu_used = 0
            memory_used = 0
            gpu_used = 0
            
            for pod in pods.items:
                if pod.status.phase != 'Running':
                    continue
                
                for container in pod.spec.containers:
                    if container.resources and container.resources.requests:
                        # CPU
                        cpu_req = container.resources.requests.get('cpu', '0')
                        if cpu_req.endswith('m'):
                            cpu_used += float(cpu_req[:-1]) / 1000
                        else:
                            cpu_used += float(cpu_req)
                        
                        # Memory
                        mem_req = container.resources.requests.get('memory', '0')
                        if mem_req.endswith('Gi'):
                            memory_used += float(mem_req[:-2])
                        elif mem_req.endswith('Mi'):
                            memory_used += float(mem_req[:-2]) / 1024
                        
                        # GPU
                        gpu_req = container.resources.requests.get('nvidia.com/gpu', '0')
                        gpu_used += int(gpu_req)
            
            # Get services
            services = self.k8s_core.list_namespaced_service(namespace=namespace_name)
            service_count = len(services.items)
            
            # Get storage (PVCs)
            pvcs = self.k8s_core.list_namespaced_persistent_volume_claim(namespace=namespace_name)
            storage_used = 0
            for pvc in pvcs.items:
                if pvc.spec.resources and pvc.spec.resources.requests:
                    storage_req = pvc.spec.resources.requests.get('storage', '0')
                    if storage_req.endswith('Gi'):
                        storage_used += float(storage_req[:-2])
            
            # Calculate monthly cost (simplified)
            from .cost_optimizer import ResourceCost
            cost = ResourceCost()
            monthly_cost = (
                cpu_used * cost.cpu_core_hour * 24 * 30 +
                memory_used * cost.memory_gb_hour * 24 * 30 +
                storage_used * cost.storage_gb_month +
                gpu_used * cost.gpu_hour * 24 * 30
            )
            
            # Get historical usage for trends
            usage_history_key = f"{tenant_id}:usage_history"
            if self.usage_cache.contains_key(usage_history_key):
                history = self.usage_cache.get(usage_history_key)
                if len(history) > 0:
                    last_hour_usage = history[-1]
                    cpu_trend = ((cpu_used - last_hour_usage.cpu_cores_used) / 
                                max(last_hour_usage.cpu_cores_used, 1)) * 100
                    memory_trend = ((memory_used - last_hour_usage.memory_gb_used) / 
                                   max(last_hour_usage.memory_gb_used, 1)) * 100
                    cost_trend = ((monthly_cost - last_hour_usage.monthly_cost) / 
                                 max(last_hour_usage.monthly_cost, 1)) * 100
                else:
                    cpu_trend = memory_trend = cost_trend = 0
            else:
                cpu_trend = memory_trend = cost_trend = 0
            
            # Create usage object
            usage = TenantResourceUsage(
                tenant_id=tenant_id,
                timestamp=datetime.utcnow(),
                cpu_cores_used=cpu_used,
                memory_gb_used=memory_used,
                storage_gb_used=storage_used,
                pod_count=len(pods.items),
                service_count=service_count,
                gpu_count_used=gpu_used,
                monthly_cost=monthly_cost,
                cpu_usage_trend=cpu_trend,
                memory_usage_trend=memory_trend,
                cost_trend=cost_trend
            )
            
            # Store current usage
            self.usage_cache.put(tenant_id, usage)
            
            # Update history
            if self.usage_cache.contains_key(usage_history_key):
                history = self.usage_cache.get(usage_history_key)
                history.append(usage)
                # Keep last 24 hours
                if len(history) > 24:
                    history = history[-24:]
                self.usage_cache.put(usage_history_key, history)
            else:
                self.usage_cache.put(usage_history_key, [usage])
            
            return usage
            
        except Exception as e:
            logger.error(f"Failed to get usage for tenant {tenant_id}: {e}")
            return None
    
    async def check_resource_availability(
        self,
        tenant_id: str,
        requested_cpu: float,
        requested_memory: float,
        requested_pods: int = 1
    ) -> Tuple[bool, Optional[str]]:
        """Check if tenant can allocate requested resources"""
        # Get tenant quota
        if not self.quotas_cache.contains_key(tenant_id):
            return False, "Tenant quota not found"
        
        quota = self.quotas_cache.get(tenant_id)
        
        # Get current usage
        usage = await self.get_tenant_usage(tenant_id)
        if not usage:
            return False, "Unable to determine current usage"
        
        # Check burst availability
        in_burst = self._is_in_burst(tenant_id)
        max_cpu = quota.burst_cpu_cores if in_burst and quota.burst_cpu_cores else quota.max_cpu_cores
        max_memory = quota.burst_memory_gb if in_burst and quota.burst_memory_gb else quota.max_memory_gb
        
        # Check limits
        if usage.cpu_cores_used + requested_cpu > max_cpu:
            return False, f"CPU limit exceeded: {usage.cpu_cores_used + requested_cpu} > {max_cpu}"
        
        if usage.memory_gb_used + requested_memory > max_memory:
            return False, f"Memory limit exceeded: {usage.memory_gb_used + requested_memory} > {max_memory}"
        
        if usage.pod_count + requested_pods > quota.max_pods:
            return False, f"Pod limit exceeded: {usage.pod_count + requested_pods} > {quota.max_pods}"
        
        # Check cost limit if applicable
        if quota.max_monthly_cost > 0:
            from .cost_optimizer import ResourceCost
            cost = ResourceCost()
            additional_monthly_cost = (
                requested_cpu * cost.cpu_core_hour * 24 * 30 +
                requested_memory * cost.memory_gb_hour * 24 * 30
            )
            
            if usage.monthly_cost + additional_monthly_cost > quota.max_monthly_cost:
                return False, f"Monthly cost limit exceeded: ${usage.monthly_cost + additional_monthly_cost:.2f} > ${quota.max_monthly_cost}"
        
        return True, None
    
    def enable_burst_mode(self, tenant_id: str, duration_hours: Optional[int] = None):
        """Enable burst mode for a tenant"""
        if not self.quotas_cache.contains_key(tenant_id):
            logger.error(f"Tenant {tenant_id} not found")
            return
        
        quota = self.quotas_cache.get(tenant_id)
        
        if not quota.burst_cpu_cores and not quota.burst_memory_gb:
            logger.warning(f"Tenant {tenant_id} does not have burst capacity")
            return
        
        burst_duration = duration_hours or quota.burst_duration_hours
        burst_end = datetime.utcnow() + timedelta(hours=burst_duration)
        
        self.burst_cache.put(tenant_id, {
            'enabled': True,
            'start_time': datetime.utcnow().isoformat(),
            'end_time': burst_end.isoformat(),
            'original_quota': quota
        })
        
        logger.info(f"Enabled burst mode for tenant {tenant_id} until {burst_end}")
    
    def _is_in_burst(self, tenant_id: str) -> bool:
        """Check if tenant is in burst mode"""
        if not self.burst_cache.contains_key(tenant_id):
            return False
        
        burst_info = self.burst_cache.get(tenant_id)
        if not burst_info['enabled']:
            return False
        
        end_time = datetime.fromisoformat(burst_info['end_time'])
        return datetime.utcnow() < end_time
    
    async def _usage_tracking_loop(self):
        """Track resource usage for all tenants"""
        while self._running:
            try:
                # Get all tenants
                for tenant_id in self.quotas_cache.keys():
                    await self.get_tenant_usage(tenant_id)
                
            except Exception as e:
                logger.error(f"Error in usage tracking: {e}")
            
            await asyncio.sleep(300)  # Every 5 minutes
    
    async def _quota_enforcement_loop(self):
        """Enforce resource quotas"""
        while self._running:
            try:
                for tenant_id in self.quotas_cache.keys():
                    quota = self.quotas_cache.get(tenant_id)
                    usage = await self.get_tenant_usage(tenant_id)
                    
                    if not usage:
                        continue
                    
                    # Check for violations
                    violations = []
                    
                    if usage.cpu_cores_used > quota.max_cpu_cores:
                        violations.append(f"CPU: {usage.cpu_cores_used} > {quota.max_cpu_cores}")
                    
                    if usage.memory_gb_used > quota.max_memory_gb:
                        violations.append(f"Memory: {usage.memory_gb_used} > {quota.max_memory_gb}")
                    
                    if usage.pod_count > quota.max_pods:
                        violations.append(f"Pods: {usage.pod_count} > {quota.max_pods}")
                    
                    if violations:
                        logger.warning(f"Quota violations for tenant {tenant_id}: {', '.join(violations)}")
                        # In a real system, we would take action here (e.g., prevent new deployments)
                
            except Exception as e:
                logger.error(f"Error in quota enforcement: {e}")
            
            await asyncio.sleep(60)  # Every minute
    
    async def _burst_management_loop(self):
        """Manage burst mode expirations"""
        while self._running:
            try:
                for tenant_id in list(self.burst_cache.keys()):
                    burst_info = self.burst_cache.get(tenant_id)
                    
                    if burst_info['enabled']:
                        end_time = datetime.fromisoformat(burst_info['end_time'])
                        
                        if datetime.utcnow() >= end_time:
                            # Disable burst mode
                            burst_info['enabled'] = False
                            self.burst_cache.put(tenant_id, burst_info)
                            logger.info(f"Burst mode expired for tenant {tenant_id}")
                
            except Exception as e:
                logger.error(f"Error in burst management: {e}")
            
            await asyncio.sleep(60)  # Every minute
    
    def get_tenant_quota(self, tenant_id: str) -> Optional[TenantResourceQuota]:
        """Get quota for a tenant"""
        if self.quotas_cache.contains_key(tenant_id):
            return self.quotas_cache.get(tenant_id)
        return None
    
    def update_tenant_tier(self, tenant_id: str, new_tier: TenantTier):
        """Update tenant tier"""
        if not self.quotas_cache.contains_key(tenant_id):
            logger.error(f"Tenant {tenant_id} not found")
            return
        
        # Create new quota with new tier
        new_quota = self.create_tenant_quota(tenant_id, new_tier)
        
        logger.info(f"Updated tenant {tenant_id} to tier {new_tier.value}")
    
    def get_all_tenant_usage(self) -> Dict[str, TenantResourceUsage]:
        """Get usage for all tenants"""
        usage_dict = {}
        
        for tenant_id in self.quotas_cache.keys():
            if self.usage_cache.contains_key(tenant_id):
                usage_dict[tenant_id] = self.usage_cache.get(tenant_id)
        
        return usage_dict
    
    async def allocate_resources_fairly(
        self,
        total_cpu: float,
        total_memory: float,
        tenant_requests: Dict[str, Dict[str, float]]
    ) -> Dict[str, Dict[str, float]]:
        """Allocate resources fairly among tenants based on priority and weights"""
        allocations = {}
        
        # Get tenant priorities and weights
        tenant_info = []
        for tenant_id, requests in tenant_requests.items():
            if self.quotas_cache.contains_key(tenant_id):
                quota = self.quotas_cache.get(tenant_id)
                tenant_info.append({
                    'tenant_id': tenant_id,
                    'priority': quota.priority,
                    'cpu_weight': quota.cpu_weight,
                    'memory_weight': quota.memory_weight,
                    'requested_cpu': requests.get('cpu', 0),
                    'requested_memory': requests.get('memory', 0),
                    'quota': quota
                })
        
        # Sort by priority (descending)
        tenant_info.sort(key=lambda x: x['priority'], reverse=True)
        
        # Allocate resources
        remaining_cpu = total_cpu
        remaining_memory = total_memory
        
        for info in tenant_info:
            tenant_id = info['tenant_id']
            quota = info['quota']
            
            # Calculate allocation based on weights and availability
            cpu_allocation = min(
                info['requested_cpu'],
                remaining_cpu * info['cpu_weight'] / sum(t['cpu_weight'] for t in tenant_info),
                quota.max_cpu_cores
            )
            
            memory_allocation = min(
                info['requested_memory'],
                remaining_memory * info['memory_weight'] / sum(t['memory_weight'] for t in tenant_info),
                quota.max_memory_gb
            )
            
            allocations[tenant_id] = {
                'cpu': cpu_allocation,
                'memory': memory_allocation
            }
            
            remaining_cpu -= cpu_allocation
            remaining_memory -= memory_allocation
        
        return allocations 