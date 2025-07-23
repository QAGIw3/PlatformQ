"""Environment and deployment configurations."""

from dataclasses import dataclass
from .base import BaseConfig


@dataclass
class EnvironmentConfig(BaseConfig):
    """Environment configuration"""
    name: str = "development"
    region: str = "us-east-1"


@dataclass
class DeploymentConfig(BaseConfig):
    """Deployment configuration"""
    namespace: str = "default"
    replicas: int = 1


@dataclass
class ResourceLimits(BaseConfig):
    """Resource limits configuration"""
    cpu: str = "1000m"
    memory: str = "2Gi"


@dataclass
class ScalingConfig(BaseConfig):
    """Auto-scaling configuration"""
    enabled: bool = True
    min_replicas: int = 1
    max_replicas: int = 10 