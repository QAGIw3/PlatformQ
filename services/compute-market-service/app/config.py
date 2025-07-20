"""Configuration for Compute Market Service."""

from pydantic_settings import BaseSettings
from typing import List, Dict, Any
from decimal import Decimal


class Settings(BaseSettings):
    """Compute Market Service configuration."""
    
    # Service info
    service_name: str = "compute-market-service"
    service_version: str = "1.0.0"
    
    # API Configuration
    api_prefix: str = "/api/v1"
    host: str = "0.0.0.0"
    port: int = 8023
    
    # Apache Ignite
    ignite_host: str = "localhost"
    ignite_port: int = 10800
    ignite_cache_prefix: str = "compute_market"
    
    # Apache Pulsar
    pulsar_url: str = "pulsar://localhost:6650"
    pulsar_compute_events_topic: str = "persistent://public/default/compute-events"
    pulsar_allocation_events_topic: str = "persistent://public/default/allocation-events"
    
    # Compute resource types
    resource_types: List[str] = ["cpu", "gpu", "tpu", "memory", "storage", "bandwidth"]
    
    # Market configuration
    spot_market_enabled: bool = True
    futures_market_enabled: bool = True
    options_market_enabled: bool = True
    
    # Pricing parameters
    base_pricing: Dict[str, Decimal] = {
        "cpu": Decimal("0.10"),  # per vCPU hour
        "gpu": Decimal("1.50"),  # per GPU hour
        "tpu": Decimal("5.00"),  # per TPU hour
        "memory": Decimal("0.01"),  # per GB hour
        "storage": Decimal("0.05"),  # per TB day
        "bandwidth": Decimal("0.10")  # per GB
    }
    
    # Dynamic pricing
    dynamic_pricing_enabled: bool = True
    price_update_interval_seconds: int = 60
    max_price_change_percent: Decimal = Decimal("0.10")  # 10% max change
    
    # Allocation parameters
    min_allocation_duration_hours: int = 1
    max_allocation_duration_hours: int = 168  # 7 days
    allocation_granularity_minutes: int = 60
    
    # Provider management
    min_provider_stake: Decimal = Decimal("1000")
    provider_reputation_enabled: bool = True
    provider_slashing_enabled: bool = True
    
    # Quality of Service (QoS)
    qos_levels: List[str] = ["bronze", "silver", "gold", "platinum"]
    qos_multipliers: Dict[str, Decimal] = {
        "bronze": Decimal("1.0"),
        "silver": Decimal("1.2"),
        "gold": Decimal("1.5"),
        "platinum": Decimal("2.0")
    }
    
    # Burst capacity
    burst_enabled: bool = True
    burst_multiplier: Decimal = Decimal("2.0")
    max_burst_duration_minutes: int = 60
    
    # External services
    trading_core_url: str = "http://localhost:8020"
    provisioning_url: str = "http://localhost:8030"
    
    # Monitoring
    metrics_enabled: bool = True
    metrics_port: int = 9023
    
    class Config:
        env_prefix = "COMPUTE_MARKET_"
        case_sensitive = False 