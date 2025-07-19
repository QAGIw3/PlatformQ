"""Tests for compute resource models"""

import pytest
from decimal import Decimal
from datetime import datetime, timedelta

from platformq_compute_common.models import (
    ComputeResourceType,
    ProviderType,
    AllocationStatus,
    PricingModel,
    AllocationStrategy,
    ResourceRequirements,
    ResourceAllocation,
    AllocationRequest,
    AllocationResponse
)


class TestResourceRequirements:
    """Test ResourceRequirements model"""
    
    def test_basic_requirements(self):
        """Test creating basic requirements"""
        req = ResourceRequirements(
            cpu_cores=4,
            memory_gb=16,
            storage_gb=100
        )
        
        assert req.cpu_cores == 4
        assert req.memory_gb == 16
        assert req.storage_gb == 100
        assert req.gpu_count == 0
        assert req.gpu_type is None
    
    def test_gpu_requirements(self):
        """Test GPU requirements"""
        req = ResourceRequirements(
            cpu_cores=8,
            memory_gb=32,
            gpu_count=2,
            gpu_type="nvidia-v100"
        )
        
        assert req.gpu_count == 2
        assert req.gpu_type == "nvidia-v100"
    
    def test_validation(self):
        """Test requirements validation"""
        # Valid requirements
        req = ResourceRequirements(cpu_cores=2, memory_gb=4)
        errors = req.validate()
        assert len(errors) == 0
        
        # Invalid - negative values
        req = ResourceRequirements(cpu_cores=-1, memory_gb=4)
        errors = req.validate()
        assert len(errors) > 0
        assert "CPU cores must be positive" in errors[0]
        
        # Invalid - GPU type without count
        req = ResourceRequirements(
            cpu_cores=2,
            memory_gb=4,
            gpu_type="nvidia-v100"
        )
        errors = req.validate()
        assert "GPU type specified without GPU count" in errors
    
    def test_regions_validation(self):
        """Test regions validation"""
        req = ResourceRequirements(
            cpu_cores=2,
            memory_gb=4,
            regions=["us-east-1", "eu-west-1"]
        )
        
        errors = req.validate()
        assert len(errors) == 0
        
        # Empty regions is valid
        req.regions = []
        errors = req.validate()
        assert len(errors) == 0


class TestResourceAllocation:
    """Test ResourceAllocation model"""
    
    def test_basic_allocation(self):
        """Test creating basic allocation"""
        alloc = ResourceAllocation(
            tenant_id="tenant-123",
            workload_id="workload-456",
            workload_type="ml-training",
            provider=ProviderType.AWS,
            region="us-east-1",
            cpu_cores=8,
            memory_gb=32,
            storage_gb=200
        )
        
        assert alloc.allocation_id is not None
        assert alloc.tenant_id == "tenant-123"
        assert alloc.status == AllocationStatus.PENDING
        assert alloc.created_at is not None
    
    def test_cost_calculation(self):
        """Test cost calculation"""
        alloc = ResourceAllocation(
            tenant_id="tenant-123",
            workload_id="workload-456",
            workload_type="simulation",
            provider=ProviderType.AWS,
            region="us-east-1",
            cpu_cores=4,
            memory_gb=16,
            storage_gb=100,
            cost_per_hour=Decimal("2.50"),
            activated_at=datetime.utcnow() - timedelta(hours=5)
        )
        
        cost = alloc.calculate_cost()
        assert cost == Decimal("12.50")  # 5 hours * 2.50
    
    def test_is_active(self):
        """Test active status checking"""
        alloc = ResourceAllocation(
            tenant_id="tenant-123",
            workload_id="workload-456",
            workload_type="batch",
            provider=ProviderType.KUBERNETES,
            region="local",
            cpu_cores=2,
            memory_gb=8,
            storage_gb=50,
            status=AllocationStatus.ACTIVE
        )
        
        assert alloc.is_active() is True
        
        alloc.status = AllocationStatus.TERMINATED
        assert alloc.is_active() is False
    
    def test_is_expired(self):
        """Test expiration checking"""
        # Not expired
        alloc = ResourceAllocation(
            tenant_id="tenant-123",
            workload_id="workload-456",
            workload_type="compute",
            provider=ProviderType.CLOUDSTACK,
            region="default",
            cpu_cores=4,
            memory_gb=16,
            storage_gb=100,
            expires_at=datetime.utcnow() + timedelta(hours=1)
        )
        
        assert alloc.is_expired() is False
        
        # Expired
        alloc.expires_at = datetime.utcnow() - timedelta(hours=1)
        assert alloc.is_expired() is True
    
    def test_to_dict(self):
        """Test dictionary conversion"""
        alloc = ResourceAllocation(
            tenant_id="tenant-123",
            workload_id="workload-456",
            workload_type="ml-training",
            provider=ProviderType.AWS,
            region="us-east-1",
            cpu_cores=8,
            memory_gb=32,
            storage_gb=200,
            gpu_count=1,
            gpu_type="nvidia-v100",
            tags={"project": "research"}
        )
        
        data = alloc.to_dict()
        
        assert data["allocation_id"] == alloc.allocation_id
        assert data["tenant_id"] == "tenant-123"
        assert data["provider"] == "aws"
        assert data["gpu_count"] == 1
        assert data["gpu_type"] == "nvidia-v100"
        assert data["tags"] == {"project": "research"}


class TestAllocationRequest:
    """Test AllocationRequest model"""
    
    def test_basic_request(self):
        """Test creating allocation request"""
        req = AllocationRequest(
            tenant_id="tenant-123",
            workload_id="job-789",
            workload_type="simulation",
            requirements=ResourceRequirements(
                cpu_cores=16,
                memory_gb=64,
                storage_gb=500
            )
        )
        
        assert req.tenant_id == "tenant-123"
        assert req.strategy == AllocationStrategy.BALANCED
        assert req.duration_hours == 1.0
        assert req.pricing_preferences == []
    
    def test_with_preferences(self):
        """Test request with preferences"""
        req = AllocationRequest(
            tenant_id="tenant-123",
            workload_id="job-789",
            workload_type="batch-processing",
            requirements=ResourceRequirements(
                cpu_cores=32,
                memory_gb=128
            ),
            strategy=AllocationStrategy.COST_OPTIMIZED,
            duration_hours=24,
            pricing_preferences=[PricingModel.SPOT, PricingModel.ON_DEMAND],
            tags={"team": "analytics", "priority": "low"}
        )
        
        assert req.strategy == AllocationStrategy.COST_OPTIMIZED
        assert req.duration_hours == 24
        assert PricingModel.SPOT in req.pricing_preferences
        assert req.tags["team"] == "analytics"


class TestAllocationResponse:
    """Test AllocationResponse model"""
    
    def test_success_response(self):
        """Test successful allocation response"""
        allocation = ResourceAllocation(
            tenant_id="tenant-123",
            workload_id="workload-456",
            workload_type="compute",
            provider=ProviderType.AWS,
            region="us-east-1",
            cpu_cores=4,
            memory_gb=16,
            storage_gb=100
        )
        
        response = AllocationResponse(
            success=True,
            allocation=allocation,
            message="Resources allocated successfully"
        )
        
        assert response.success is True
        assert response.allocation is not None
        assert response.allocation.allocation_id == allocation.allocation_id
        assert response.message == "Resources allocated successfully"
    
    def test_failure_response(self):
        """Test failed allocation response"""
        response = AllocationResponse(
            success=False,
            message="No resources available",
            alternative_options=[
                {
                    "provider": "aws",
                    "region": "us-west-2",
                    "available_in_minutes": 30
                }
            ]
        )
        
        assert response.success is False
        assert response.allocation is None
        assert response.message == "No resources available"
        assert len(response.alternative_options) == 1 