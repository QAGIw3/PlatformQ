"""
Vault and Consul Integration for DataIntelligenceSuite

Provides secure secret management and service discovery capabilities.
"""

from .base import (
    BaseIntegration,
    CacheableMixin,
    LeaseManagerMixin,
    ConfigWatcherMixin
)

from .vault_integration import (
    VaultIntegration,
    VaultConfig
)

from .consul_integration import (
    ConsulIntegration,
    ConsulConfig
)

from .unified_integration import (
    VaultConsulIntegration
)

__all__ = [
    # Base
    "BaseIntegration",
    "CacheableMixin", 
    "LeaseManagerMixin",
    "ConfigWatcherMixin",
    
    # Vault
    "VaultIntegration",
    "VaultConfig",
    
    # Consul
    "ConsulIntegration",
    "ConsulConfig",
    
    # Unified
    "VaultConsulIntegration"
] 