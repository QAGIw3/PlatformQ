"""Common resource management interfaces"""

from abc import ABC, abstractmethod
from typing import List, Optional, Dict, Any, Tuple
from datetime import datetime
from .models import (
    ResourceMetrics,
    ClusterMetrics,
    ScalingDecision,
    ScalingPolicy,
    ResourceQuota,
    ResourceUsage,
    ResourceAllocation,
    ResourceAnomalyEvent
)


class IResourceMonitor(ABC):
    """Interface for resource monitoring"""
    
    @abstractmethod
    async def get_service_metrics(self, service_name: str, 
                                namespace: str = "platformq") -> Optional[ResourceMetrics]:
        """Get current metrics for a service"""
        pass
    
    @abstractmethod
    async def get_cluster_metrics(self) -> Optional[ClusterMetrics]:
        """Get current cluster-wide metrics"""
        pass
    
    @abstractmethod
    async def get_historical_metrics(self, service_name: str, namespace: str,
                                   start_time: datetime, end_time: datetime) -> List[ResourceMetrics]:
        """Get historical metrics for a service"""
        pass
    
    @abstractmethod
    async def detect_anomalies(self, metrics: ResourceMetrics) -> List[ResourceAnomalyEvent]:
        """Detect anomalies in resource metrics"""
        pass


class IScalingEngine(ABC):
    """Interface for scaling operations"""
    
    @abstractmethod
    async def evaluate_scaling(self, service_name: str, 
                             policy: ScalingPolicy,
                             metrics: ResourceMetrics) -> Optional[ScalingDecision]:
        """Evaluate if scaling is needed"""
        pass
    
    @abstractmethod
    async def apply_scaling_decision(self, decision: ScalingDecision) -> bool:
        """Apply a scaling decision"""
        pass
    
    @abstractmethod
    async def get_scaling_policy(self, service_name: str) -> Optional[ScalingPolicy]:
        """Get scaling policy for a service"""
        pass
    
    @abstractmethod
    async def update_scaling_policy(self, policy: ScalingPolicy) -> bool:
        """Update scaling policy for a service"""
        pass
    
    @abstractmethod
    async def get_recent_decisions(self, service_name: Optional[str] = None,
                                 hours: int = 24) -> List[ScalingDecision]:
        """Get recent scaling decisions"""
        pass


class IQuotaManager(ABC):
    """Interface for quota management"""
    
    @abstractmethod
    async def create_quota(self, tenant_id: str, tier: str, 
                         custom_limits: Optional[Dict[str, Any]] = None) -> ResourceQuota:
        """Create resource quota for a tenant"""
        pass
    
    @abstractmethod
    async def get_quota(self, tenant_id: str) -> Optional[ResourceQuota]:
        """Get quota for a tenant"""
        pass
    
    @abstractmethod
    async def update_quota(self, tenant_id: str, updates: Dict[str, Any]) -> Optional[ResourceQuota]:
        """Update quota for a tenant"""
        pass
    
    @abstractmethod
    async def get_usage(self, tenant_id: str) -> Optional[ResourceUsage]:
        """Get current resource usage for a tenant"""
        pass
    
    @abstractmethod
    async def check_availability(self, tenant_id: str, requested_cpu: float,
                               requested_memory: float, requested_pods: int = 1) -> Tuple[bool, Optional[str]]:
        """Check if requested resources are available within quota"""
        pass
    
    @abstractmethod
    async def allocate_resources(self, allocation: ResourceAllocation) -> bool:
        """Allocate resources to a tenant"""
        pass
    
    @abstractmethod
    async def release_resources(self, allocation_id: str) -> bool:
        """Release allocated resources"""
        pass


class IResourceRepository(ABC):
    """Interface for resource data persistence"""
    
    @abstractmethod
    async def store_metrics(self, metrics: ResourceMetrics) -> bool:
        """Store resource metrics"""
        pass
    
    @abstractmethod
    async def store_scaling_decision(self, decision: ScalingDecision) -> str:
        """Store a scaling decision"""
        pass
    
    @abstractmethod
    async def update_scaling_decision_status(self, decision_id: str, 
                                           applied: bool, error: Optional[str] = None) -> bool:
        """Update the status of a scaling decision"""
        pass
    
    @abstractmethod
    async def store_resource_usage(self, usage: ResourceUsage) -> bool:
        """Store resource usage data"""
        pass
    
    @abstractmethod
    async def get_tenant_allocations(self, tenant_id: str) -> List[ResourceAllocation]:
        """Get all active allocations for a tenant"""
        pass 