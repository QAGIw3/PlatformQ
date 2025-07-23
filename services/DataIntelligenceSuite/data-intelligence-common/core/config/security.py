"""Security configurations."""

from dataclasses import dataclass
from .base import SecurityConfig, BaseConfig


@dataclass
class VaultConfig(BaseConfig):
    """HashiCorp Vault configuration"""
    url: str = "http://localhost:8200"
    token: str = ""


@dataclass
class ConsulConfig(BaseConfig):
    """HashiCorp Consul configuration"""
    url: str = "http://localhost:8500"
    token: str = ""


@dataclass
class AuthConfig(SecurityConfig):
    """Authentication configuration"""
    pass


@dataclass
class EncryptionConfig(BaseConfig):
    """Encryption configuration"""
    algorithm: str = "AES256" 