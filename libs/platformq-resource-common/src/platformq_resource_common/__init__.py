"""
Platform Q Resource Common Library

Common models, interfaces, and utilities for resource management across Platform Q services.
"""

# Export all models
from .models import (
    # Enums
    ResourceType,
    ComputeResourceType,
    ProviderType,
    ResourceStatus,
    AllocationStrategy,
    PricingModel,
    TenantTier,
    QuotaStatus,
    ScalingAction,
    ProvisioningStatus,
    
    # Core Models
    ResourceSpec,
    ResourceRequirements,
    ResourceAllocation,
    ResourceMetrics,
    ClusterMetrics,
    ResourceQuota,
    ScalingPolicy,
    ScalingDecision,
    InfrastructureResource,
    ProvisioningRequest,
    ProvisioningResult,
    ProviderCapabilities,
    
    # Event Models
    BaseEvent,
    ResourceAnomalyEvent,
    ScalingEvent,
    QuotaExceededEvent,
    AllocationEvent,
    TenantCreatedEvent,
    TenantDeletedEvent,
    TenantUpgradedEvent,
    UserCreatedEvent,
    
    # Request/Response Models
    AllocationRequest,
    AllocationResponse,
)

# Export all interfaces
from .interfaces import (
    # Provider Interfaces
    IResourceProvider,
    
    # Service Interfaces
    IAllocationService,
    IResourceMonitor,
    IScalingEngine,
    IPredictiveScaler,
    IQuotaManager,
    IResourceProvisioner,
    IProvisioningOrchestrator,
    
    # Repository Interfaces
    IProvisioningRepository,
    IResourceRepository,
    
    # Service Client Interfaces
    IServiceClient,
    IComputeAllocationClient,
    IQuotaServiceClient,
    IMonitoringServiceClient,
    IScalingServiceClient,
)

__version__ = "0.1.0"

__all__ = [
    # Version
    "__version__",
    
    # Enums
    "ResourceType",
    "ComputeResourceType",
    "ProviderType",
    "ResourceStatus",
    "AllocationStrategy",
    "PricingModel",
    "TenantTier",
    "QuotaStatus",
    "ScalingAction",
    "ProvisioningStatus",
    
    # Core Models
    "ResourceSpec",
    "ResourceRequirements",
    "ResourceAllocation",
    "ResourceMetrics",
    "ClusterMetrics",
    "ResourceQuota",
    "ScalingPolicy",
    "ScalingDecision",
    "InfrastructureResource",
    "ProvisioningRequest",
    "ProvisioningResult",
    "ProviderCapabilities",
    
    # Event Models
    "BaseEvent",
    "ResourceAnomalyEvent",
    "ScalingEvent",
    "QuotaExceededEvent",
    "AllocationEvent",
    "TenantCreatedEvent",
    "TenantDeletedEvent",
    "TenantUpgradedEvent",
    "UserCreatedEvent",
    
    # Request/Response Models
    "AllocationRequest",
    "AllocationResponse",
    
    # Provider Interfaces
    "IResourceProvider",
    
    # Service Interfaces
    "IAllocationService",
    "IResourceMonitor",
    "IScalingEngine",
    "IPredictiveScaler",
    "IQuotaManager",
    "IResourceProvisioner",
    "IProvisioningOrchestrator",
    
    # Repository Interfaces
    "IProvisioningRepository",
    "IResourceRepository",
    
    # Service Client Interfaces
    "IServiceClient",
    "IComputeAllocationClient",
    "IQuotaServiceClient",
    "IMonitoringServiceClient",
    "IScalingServiceClient",
] 