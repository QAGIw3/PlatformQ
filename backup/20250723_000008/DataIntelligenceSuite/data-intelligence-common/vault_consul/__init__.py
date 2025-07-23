"""
Vault and Consul Integration for DataIntelligenceSuite

Provides secure secret management and service discovery capabilities.
"""

from .vault_integration import (
    VaultIntegration,
    VaultConfig,
    SecretEngine,
    AuthMethod,
    VaultSecret
)

from .consul_integration import (
    ConsulIntegration,
    ConsulConfig,
    ServiceRegistration,
    HealthCheck,
    ConsulKV
)

from .unified_integration import (
    VaultConsulIntegration,
    DataServiceConfig,
    ServiceCredentials,
    DynamicConfig,
    SecretRotation
)

__all__ = [
    # Vault
    "VaultIntegration",
    "VaultConfig",
    "SecretEngine",
    "AuthMethod",
    "VaultSecret",
    
    # Consul
    "ConsulIntegration",
    "ConsulConfig",
    "ServiceRegistration",
    "HealthCheck",
    "ConsulKV",
    
    # Unified
    "VaultConsulIntegration",
    "DataServiceConfig",
    "ServiceCredentials",
    "DynamicConfig",
    "SecretRotation"
] 