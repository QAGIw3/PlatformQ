"""Provisioning utility functions"""

import re
from typing import Dict, Any
from .models import TenantTier


def generate_resource_name(tenant_id: str, resource_type: str) -> str:
    """Generate a consistent resource name for a tenant"""
    # Ensure tenant_id is valid
    clean_tenant_id = re.sub(r'[^a-zA-Z0-9-]', '-', tenant_id)
    return f"{resource_type}-{clean_tenant_id}"


def validate_tenant_id(tenant_id: str) -> bool:
    """Validate tenant ID format"""
    # UUID format or alphanumeric with hyphens
    pattern = r'^[a-fA-F0-9]{8}-[a-fA-F0-9]{4}-[a-fA-F0-9]{4}-[a-fA-F0-9]{4}-[a-fA-F0-9]{12}$|^[a-zA-Z0-9][a-zA-Z0-9-]{0,61}[a-zA-Z0-9]$'
    return bool(re.match(pattern, tenant_id))


def get_tier_defaults(tier: TenantTier) -> Dict[str, Any]:
    """Get default resource limits for a tier"""
    tier_configs = {
        TenantTier.FREE: {
            "max_cpu_cores": 2,
            "max_memory_gb": 4,
            "max_storage_gb": 10,
            "max_pods": 5,
            "max_services": 3,
            "max_gpu_count": 0,
            "max_monthly_cost": 0.0,
            "burst_allowed": False,
            "priority": 1
        },
        TenantTier.STARTER: {
            "max_cpu_cores": 8,
            "max_memory_gb": 16,
            "max_storage_gb": 100,
            "max_pods": 20,
            "max_services": 10,
            "max_gpu_count": 0,
            "max_monthly_cost": 500.0,
            "burst_allowed": True,
            "priority": 3
        },
        TenantTier.PROFESSIONAL: {
            "max_cpu_cores": 32,
            "max_memory_gb": 64,
            "max_storage_gb": 500,
            "max_pods": 50,
            "max_services": 25,
            "max_gpu_count": 2,
            "max_monthly_cost": 2000.0,
            "burst_allowed": True,
            "priority": 5
        },
        TenantTier.ENTERPRISE: {
            "max_cpu_cores": 128,
            "max_memory_gb": 256,
            "max_storage_gb": 2000,
            "max_pods": 200,
            "max_services": 100,
            "max_gpu_count": 8,
            "max_monthly_cost": 10000.0,
            "burst_allowed": True,
            "priority": 7
        },
        TenantTier.CUSTOM: {
            "max_cpu_cores": 256,
            "max_memory_gb": 512,
            "max_storage_gb": 5000,
            "max_pods": 500,
            "max_services": 250,
            "max_gpu_count": 16,
            "max_monthly_cost": 0.0,  # Unlimited
            "burst_allowed": True,
            "priority": 10
        }
    }
    
    return tier_configs.get(tier, tier_configs[TenantTier.STARTER]) 