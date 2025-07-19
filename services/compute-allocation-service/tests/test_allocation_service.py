"""Tests for compute allocation service"""

import pytest
from unittest.mock import Mock, AsyncMock, patch
from datetime import datetime, timedelta
from decimal import Decimal

from app.allocation_service import AllocationService
from platformq_compute_common.models import (
    ResourceRequirements,
    ResourceAllocation,
    AllocationRequest,
    AllocationResponse,
    AllocationStatus,
    ProviderType,
    PricingModel,
    AllocationStrategy
)
from platformq_compute_common.providers import ResourceProvider, ProviderCapabilities


class MockProvider(ResourceProvider):
    """Mock provider for testing"""
    
    def __init__(self, provider_type: ProviderType = ProviderType.AWS):
        super().__init__({"type": provider_type.value})
        self.provider_type = provider_type
        self.allocate_success = True
        self.deallocate_success = True
    
    async def get_capabilities(self) -> ProviderCapabilities:
        return ProviderCapabilities(
            provider_type=self.provider_type,
            supported_regions=["us-east-1", "us-west-2"],
            supported_instance_types={"m5.large": {"cpu": 2, "memory": 8}},
            supported_gpu_types=["nvidia-v100"],
            supported_pricing_models=[PricingModel.ON_DEMAND, PricingModel.SPOT],
            max_instances=100,
            features={"spot_instances": True},
            sla_guarantees={"availability": 0.99}
        )
    
    async def check_availability(self, requirements, region):
        return True, "m5.large", {"available_count": 10}
    
    async def get_pricing(self, requirements, region, instance_type, pricing_model):
        return {"hourly_cost": 0.1}
    
    async def allocate(self, allocation):
        if self.allocate_success:
            return True, {"instance_id": "i-123456", "public_ip": "1.2.3.4"}
        return False, {"error": "Allocation failed"}
    
    async def deallocate(self, allocation):
        if self.deallocate_success:
            return True, "Success"
        return False, "Failed"
    
    async def get_status(self, allocation):
        return {"status": "running", "health": "healthy"}
    
    async def resize(self, allocation, new_requirements):
        return True, {"resized": True}


class TestAllocationService:
    """Test AllocationService functionality"""
    
    @pytest.fixture
    def mock_config_manager(self):
        """Create mock config manager"""
        config_manager = Mock()
        config_manager.get_config = AsyncMock(return_value={})
        config_manager.set_config = AsyncMock(return_value=True)
        config_manager.watch_config = AsyncMock()
        config_manager.get_provider_credentials = AsyncMock(return_value={})
        return config_manager
    
    @pytest.fixture
    async def allocation_service(self, mock_config_manager):
        """Create allocation service with mocked dependencies"""
        service = AllocationService(mock_config_manager)
        
        # Mock provider registry methods
        service.provider_registry.start_health_monitoring = AsyncMock()
        service.provider_registry.stop_health_monitoring = AsyncMock()
        
        # Register a mock provider
        mock_provider = MockProvider(ProviderType.AWS)
        service.provider_registry.register("aws", mock_provider)
        
        # Set a test budget
        service.budget_manager.set_budget("tenant-123", Decimal("1000"))
        
        return service
    
    @pytest.mark.asyncio
    async def test_allocate_resources_success(self, allocation_service):
        """Test successful resource allocation"""
        request = AllocationRequest(
            tenant_id="tenant-123",
            workload_id="workload-456",
            workload_type="ml-training",
            requirements=ResourceRequirements(
                cpu_cores=4,
                memory_gb=16,
                storage_gb=100
            ),
            strategy=AllocationStrategy.BALANCED,
            duration_hours=24
        )
        
        response = await allocation_service.allocate_resources(request)
        
        assert response.success is True
        assert response.allocation is not None
        assert response.allocation.status == AllocationStatus.ACTIVE
        assert response.allocation.provider == ProviderType.AWS
        assert response.message == "Resources allocated successfully"
        
        # Check allocation is stored
        assert response.allocation.allocation_id in allocation_service.allocations
    
    @pytest.mark.asyncio
    async def test_allocate_resources_invalid_requirements(self, allocation_service):
        """Test allocation with invalid requirements"""
        request = AllocationRequest(
            tenant_id="tenant-123",
            workload_id="workload-456",
            workload_type="compute",
            requirements=ResourceRequirements(
                cpu_cores=-1,  # Invalid
                memory_gb=16
            )
        )
        
        response = await allocation_service.allocate_resources(request)
        
        assert response.success is False
        assert "Invalid requirements" in response.message
    
    @pytest.mark.asyncio
    async def test_allocate_resources_budget_exceeded(self, allocation_service):
        """Test allocation when budget is exceeded"""
        # Use up most of the budget
        allocation_service.budget_manager.update_usage("tenant-123", Decimal("990"))
        
        request = AllocationRequest(
            tenant_id="tenant-123",
            workload_id="workload-456",
            workload_type="compute",
            requirements=ResourceRequirements(
                cpu_cores=32,  # Expensive
                memory_gb=128
            ),
            duration_hours=24
        )
        
        response = await allocation_service.allocate_resources(request)
        
        assert response.success is False
        assert "budget" in response.message.lower()
    
    @pytest.mark.asyncio
    async def test_allocate_resources_no_provider_available(self, allocation_service):
        """Test allocation when no provider is available"""
        # Unregister all providers
        allocation_service.provider_registry.providers.clear()
        
        request = AllocationRequest(
            tenant_id="tenant-123",
            workload_id="workload-456",
            workload_type="compute",
            requirements=ResourceRequirements(
                cpu_cores=4,
                memory_gb=16
            )
        )
        
        response = await allocation_service.allocate_resources(request)
        
        assert response.success is False
        assert "No single provider can fulfill" in response.message
    
    @pytest.mark.asyncio
    async def test_get_allocation(self, allocation_service):
        """Test getting allocation details"""
        # Create an allocation first
        allocation = ResourceAllocation(
            allocation_id="test-alloc-123",
            tenant_id="tenant-123",
            workload_id="workload-456",
            workload_type="compute",
            provider=ProviderType.AWS,
            region="us-east-1",
            cpu_cores=4,
            memory_gb=16,
            storage_gb=100,
            status=AllocationStatus.ACTIVE
        )
        
        allocation_service.allocations["test-alloc-123"] = allocation
        
        # Get allocation
        result = await allocation_service.get_allocation("test-alloc-123")
        
        assert result is not None
        assert result.allocation_id == "test-alloc-123"
        assert result.tenant_id == "tenant-123"
    
    @pytest.mark.asyncio
    async def test_get_allocation_from_consul(self, allocation_service, mock_config_manager):
        """Test getting allocation from Consul when not in cache"""
        allocation_data = {
            "allocation_id": "consul-alloc-123",
            "tenant_id": "tenant-123",
            "workload_id": "workload-456",
            "workload_type": "compute",
            "provider": "aws",
            "region": "us-east-1",
            "cpu_cores": 4,
            "memory_gb": 16,
            "storage_gb": 100,
            "status": "active",
            "cost_per_hour": "0.5",
            "created_at": datetime.utcnow().isoformat()
        }
        
        mock_config_manager.get_config.return_value = allocation_data
        
        result = await allocation_service.get_allocation("consul-alloc-123")
        
        assert result is not None
        assert result.allocation_id == "consul-alloc-123"
        assert result in allocation_service.allocations.values()
    
    @pytest.mark.asyncio
    async def test_deallocate_resources_success(self, allocation_service):
        """Test successful resource deallocation"""
        # Create an allocation
        allocation = ResourceAllocation(
            allocation_id="test-alloc-123",
            tenant_id="tenant-123",
            workload_id="workload-456",
            workload_type="compute",
            provider=ProviderType.AWS,
            region="us-east-1",
            cpu_cores=4,
            memory_gb=16,
            storage_gb=100,
            status=AllocationStatus.ACTIVE
        )
        
        allocation_service.allocations["test-alloc-123"] = allocation
        
        # Deallocate
        success = await allocation_service.deallocate_resources("test-alloc-123")
        
        assert success is True
        assert "test-alloc-123" not in allocation_service.allocations
    
    @pytest.mark.asyncio
    async def test_modify_allocation_extend_hours(self, allocation_service):
        """Test extending allocation duration"""
        # Create an allocation
        allocation = ResourceAllocation(
            allocation_id="test-alloc-123",
            tenant_id="tenant-123",
            workload_id="workload-456",
            workload_type="compute",
            provider=ProviderType.AWS,
            region="us-east-1",
            cpu_cores=4,
            memory_gb=16,
            storage_gb=100,
            cost_per_hour=Decimal("0.5"),
            expires_at=datetime.utcnow() + timedelta(hours=24),
            status=AllocationStatus.ACTIVE
        )
        
        allocation_service.allocations["test-alloc-123"] = allocation
        original_expiry = allocation.expires_at
        
        # Extend by 12 hours
        success = await allocation_service.modify_allocation(
            "test-alloc-123",
            {"extend_hours": 12}
        )
        
        assert success is True
        assert allocation.expires_at > original_expiry
        assert allocation.expires_at == original_expiry + timedelta(hours=12)
    
    @pytest.mark.asyncio
    async def test_modify_allocation_scale_to(self, allocation_service):
        """Test scaling allocation resources"""
        # Create an allocation
        allocation = ResourceAllocation(
            allocation_id="test-alloc-123",
            tenant_id="tenant-123",
            workload_id="workload-456",
            workload_type="compute",
            provider=ProviderType.AWS,
            region="us-east-1",
            cpu_cores=4,
            memory_gb=16,
            storage_gb=100,
            status=AllocationStatus.ACTIVE
        )
        
        allocation_service.allocations["test-alloc-123"] = allocation
        
        # Scale up
        new_requirements = {
            "cpu_cores": 8,
            "memory_gb": 32,
            "storage_gb": 200
        }
        
        success = await allocation_service.modify_allocation(
            "test-alloc-123",
            {"scale_to": new_requirements}
        )
        
        assert success is True
        assert allocation.cpu_cores == 8
        assert allocation.memory_gb == 32
        assert allocation.storage_gb == 200
    
    @pytest.mark.asyncio
    async def test_get_allocation_metrics(self, allocation_service):
        """Test getting allocation metrics"""
        # Create some allocations
        for i in range(3):
            allocation = ResourceAllocation(
                allocation_id=f"test-alloc-{i}",
                tenant_id="tenant-123",
                workload_id=f"workload-{i}",
                workload_type="compute",
                provider=ProviderType.AWS,
                region="us-east-1",
                cpu_cores=4,
                memory_gb=16,
                storage_gb=100,
                cost_per_hour=Decimal("0.5"),
                status=AllocationStatus.ACTIVE if i < 2 else AllocationStatus.TERMINATED,
                activated_at=datetime.utcnow() - timedelta(hours=1)
            )
            allocation_service.allocations[f"test-alloc-{i}"] = allocation
        
        metrics = await allocation_service.get_allocation_metrics()
        
        assert metrics["total_allocations"] == 3
        assert metrics["active_allocations"] == 2
        assert metrics["total_cost_usd"] == 1.0  # 2 active * 0.5 per hour * 1 hour
        assert "aws" in metrics["by_provider"]
        assert metrics["by_provider"]["aws"]["count"] == 2
        assert metrics["by_provider"]["aws"]["cpu_cores"] == 8 