"""Tests for cost calculation and budget management"""

import pytest
from decimal import Decimal
from datetime import datetime, timedelta

from platformq_compute_common.cost import (
    ResourceCost,
    CostCalculator,
    BudgetManager,
    CostAnalysis
)
from platformq_compute_common.models import (
    ResourceRequirements,
    ProviderType,
    PricingModel
)


class TestResourceCost:
    """Test ResourceCost configuration"""
    
    def test_default_costs(self):
        """Test default cost values"""
        cost = ResourceCost()
        
        assert cost.cpu_core_hour == Decimal("0.05")
        assert cost.memory_gb_hour == Decimal("0.01")
        assert cost.storage_gb_month == Decimal("0.10")
        assert cost.gpu_hour == Decimal("0.90")
        assert cost.network_gb == Decimal("0.09")
    
    def test_custom_costs(self):
        """Test custom cost values"""
        cost = ResourceCost(
            cpu_core_hour=Decimal("0.10"),
            gpu_hour=Decimal("2.00")
        )
        
        assert cost.cpu_core_hour == Decimal("0.10")
        assert cost.gpu_hour == Decimal("2.00")


class TestCostCalculator:
    """Test CostCalculator functionality"""
    
    def test_calculate_requirements_cost(self):
        """Test calculating cost for requirements"""
        calculator = CostCalculator()
        
        requirements = ResourceRequirements(
            cpu_cores=4,
            memory_gb=16,
            storage_gb=100
        )
        
        analysis = calculator.calculate_requirements_cost(
            requirements,
            ProviderType.AWS,
            "us-east-1",
            PricingModel.ON_DEMAND,
            duration_hours=24
        )
        
        assert isinstance(analysis, CostAnalysis)
        assert analysis.cpu_cost > 0
        assert analysis.memory_cost > 0
        assert analysis.storage_cost > 0
        assert analysis.gpu_cost == 0
        assert analysis.total_hourly_cost > 0
        assert analysis.provider == ProviderType.AWS
        assert analysis.region == "us-east-1"
    
    def test_gpu_cost_calculation(self):
        """Test GPU cost calculation"""
        calculator = CostCalculator()
        
        # Standard GPU
        requirements = ResourceRequirements(
            cpu_cores=8,
            memory_gb=32,
            gpu_count=1,
            gpu_type="nvidia-v100"
        )
        
        analysis = calculator.calculate_requirements_cost(
            requirements,
            ProviderType.AWS,
            "us-east-1",
            PricingModel.ON_DEMAND,
            duration_hours=1
        )
        
        assert analysis.gpu_cost > 0
        assert analysis.gpu_type == "nvidia-v100"
        
        # Premium GPU
        requirements.gpu_type = "nvidia-a100"
        analysis = calculator.calculate_requirements_cost(
            requirements,
            ProviderType.AWS,
            "us-east-1",
            PricingModel.ON_DEMAND,
            duration_hours=1
        )
        
        # A100 should be more expensive
        assert analysis.gpu_cost > Decimal("0.90")
    
    def test_spot_pricing_discount(self):
        """Test spot pricing discount"""
        calculator = CostCalculator()
        
        requirements = ResourceRequirements(
            cpu_cores=4,
            memory_gb=16
        )
        
        # On-demand pricing
        on_demand = calculator.calculate_requirements_cost(
            requirements,
            ProviderType.AWS,
            "us-east-1",
            PricingModel.ON_DEMAND,
            duration_hours=1
        )
        
        # Spot pricing
        spot = calculator.calculate_requirements_cost(
            requirements,
            ProviderType.AWS,
            "us-east-1",
            PricingModel.SPOT,
            duration_hours=1
        )
        
        # Spot should be cheaper
        assert spot.total_hourly_cost < on_demand.total_hourly_cost
        assert spot.total_hourly_cost == on_demand.total_hourly_cost * Decimal("0.3")
    
    def test_reserved_pricing_discount(self):
        """Test reserved pricing discount"""
        calculator = CostCalculator()
        
        requirements = ResourceRequirements(
            cpu_cores=8,
            memory_gb=32
        )
        
        # On-demand pricing
        on_demand = calculator.calculate_requirements_cost(
            requirements,
            ProviderType.AWS,
            "us-east-1",
            PricingModel.ON_DEMAND,
            duration_hours=1
        )
        
        # Reserved pricing
        reserved = calculator.calculate_requirements_cost(
            requirements,
            ProviderType.AWS,
            "us-east-1",
            PricingModel.RESERVED,
            duration_hours=1
        )
        
        # Reserved should be cheaper
        assert reserved.total_hourly_cost < on_demand.total_hourly_cost
        assert reserved.total_hourly_cost == on_demand.total_hourly_cost * Decimal("0.6")
    
    def test_provider_rate_adjustments(self):
        """Test provider-specific rate adjustments"""
        calculator = CostCalculator()
        
        requirements = ResourceRequirements(
            cpu_cores=4,
            memory_gb=16
        )
        
        # AWS rates
        aws_cost = calculator.calculate_requirements_cost(
            requirements,
            ProviderType.AWS,
            "us-east-1",
            PricingModel.ON_DEMAND,
            duration_hours=1
        )
        
        # Kubernetes (internal) rates
        k8s_cost = calculator.calculate_requirements_cost(
            requirements,
            ProviderType.KUBERNETES,
            "local",
            PricingModel.ON_DEMAND,
            duration_hours=1
        )
        
        # Kubernetes should be cheaper (50% of cloud)
        assert k8s_cost.total_hourly_cost < aws_cost.total_hourly_cost
        assert k8s_cost.total_hourly_cost == aws_cost.total_hourly_cost * Decimal("0.5")
    
    def test_cost_analysis_to_dict(self):
        """Test CostAnalysis dictionary conversion"""
        analysis = CostAnalysis(
            cpu_cost=Decimal("0.20"),
            memory_cost=Decimal("0.16"),
            storage_cost=Decimal("0.01"),
            gpu_cost=Decimal("0"),
            network_cost=Decimal("0"),
            total_hourly_cost=Decimal("0.37"),
            provider=ProviderType.AWS,
            region="us-east-1",
            pricing_model=PricingModel.ON_DEMAND
        )
        
        data = analysis.to_dict()
        
        assert data["cpu_cost"] == 0.20
        assert data["memory_cost"] == 0.16
        assert data["total_hourly_cost"] == 0.37
        assert data["provider"] == "AWS"
        assert data["pricing_model"] == "ON_DEMAND"


class TestBudgetManager:
    """Test BudgetManager functionality"""
    
    def test_set_budget(self):
        """Test setting tenant budget"""
        manager = BudgetManager()
        
        manager.set_budget(
            tenant_id="tenant-123",
            monthly_limit=Decimal("1000"),
            alert_thresholds=[0.5, 0.75, 0.9]
        )
        
        budget = manager.tenant_budgets.get("tenant-123")
        assert budget is not None
        assert budget["monthly_limit"] == Decimal("1000")
        assert budget["alert_thresholds"] == [0.5, 0.75, 0.9]
    
    def test_check_budget_within_limit(self):
        """Test budget check within limit"""
        manager = BudgetManager()
        
        manager.set_budget("tenant-123", Decimal("1000"))
        manager.update_usage("tenant-123", Decimal("100"))
        
        # Check $50 allocation
        ok, message = manager.check_budget("tenant-123", Decimal("50"))
        
        assert ok is True
        assert message == "Budget check passed"
    
    def test_check_budget_exceeds_limit(self):
        """Test budget check exceeding limit"""
        manager = BudgetManager()
        
        manager.set_budget("tenant-123", Decimal("1000"))
        manager.update_usage("tenant-123", Decimal("950"))
        
        # Check $100 allocation (would exceed)
        ok, message = manager.check_budget("tenant-123", Decimal("100"))
        
        assert ok is False
        assert "would exceed monthly budget" in message
    
    def test_get_budget_status(self):
        """Test getting budget status"""
        manager = BudgetManager()
        
        manager.set_budget("tenant-123", Decimal("1000"))
        manager.update_usage("tenant-123", Decimal("250"))
        
        status = manager.get_budget_status("tenant-123")
        
        assert status["monthly_limit"] == Decimal("1000")
        assert status["current_usage"] == Decimal("250")
        assert status["remaining"] == Decimal("750")
        assert status["percentage_used"] == 25.0
    
    def test_alert_thresholds(self):
        """Test alert threshold detection"""
        manager = BudgetManager()
        
        manager.set_budget(
            "tenant-123",
            Decimal("1000"),
            alert_thresholds=[0.5, 0.75, 0.9]
        )
        
        # Below threshold
        manager.update_usage("tenant-123", Decimal("400"))
        status = manager.get_budget_status("tenant-123")
        assert len(status.get("alerts_triggered", [])) == 0
        
        # 50% threshold
        manager.update_usage("tenant-123", Decimal("500"))
        status = manager.get_budget_status("tenant-123")
        assert 0.5 in status.get("alerts_triggered", [])
        
        # 75% threshold
        manager.update_usage("tenant-123", Decimal("750"))
        status = manager.get_budget_status("tenant-123")
        assert 0.75 in status.get("alerts_triggered", [])
    
    def test_update_usage(self):
        """Test usage updates"""
        manager = BudgetManager()
        
        manager.set_budget("tenant-123", Decimal("1000"))
        
        # Initial usage
        manager.update_usage("tenant-123", Decimal("100"))
        assert manager.current_usage["tenant-123"] == Decimal("100")
        
        # Additional usage
        manager.update_usage("tenant-123", Decimal("50"))
        assert manager.current_usage["tenant-123"] == Decimal("150")
    
    def test_reset_usage(self):
        """Test usage reset"""
        manager = BudgetManager()
        
        manager.set_budget("tenant-123", Decimal("1000"))
        manager.update_usage("tenant-123", Decimal("500"))
        
        # Reset usage
        manager.reset_usage("tenant-123")
        
        assert manager.current_usage.get("tenant-123", 0) == 0
        
        status = manager.get_budget_status("tenant-123")
        assert status["current_usage"] == Decimal("0")
        assert status["remaining"] == Decimal("1000") 