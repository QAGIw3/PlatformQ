"""Processing engine configurations."""

from dataclasses import dataclass
from .base import BaseConfig


@dataclass
class SparkConfig(BaseConfig):
    """Apache Spark configuration"""
    master: str = "local[*]"
    app_name: str = "PlatformQ"


@dataclass
class FlinkConfig(BaseConfig):
    """Apache Flink configuration"""
    job_manager: str = "localhost:8081"


@dataclass
class TrinoConfig(BaseConfig):
    """Trino configuration"""
    coordinator: str = "localhost:8080"


@dataclass
class SeaTunnelConfig(BaseConfig):
    """Apache SeaTunnel configuration"""
    config_path: str = "/config/seatunnel" 