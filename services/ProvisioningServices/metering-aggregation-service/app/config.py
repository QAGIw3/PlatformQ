"""Configuration for Metering Aggregation Service"""

from pydantic_settings import BaseSettings
from typing import Optional


class Settings(BaseSettings):
    """Service configuration"""
    
    # Service info
    service_name: str = "metering-aggregation-service"
    service_port: int = 8091
    
    # CloudKitty configuration
    cloudkitty_enabled: bool = True
    cloudkitty_url: str = "http://cloudkitty:8889"
    
    # OpenMeter configuration
    openmeter_enabled: bool = True
    openmeter_url: str = "http://openmeter:8080"
    openmeter_api_key: Optional[str] = None
    
    # Cassandra configuration for budget storage
    cassandra_hosts: str = "cassandra:9042"
    cassandra_keyspace: str = "platformq"
    cassandra_username: Optional[str] = None
    cassandra_password: Optional[str] = None
    
    # Ignite configuration for caching
    ignite_host: str = "ignite"
    ignite_port: int = 10800
    ignite_cache_name: str = "metering-cache"
    
    # Pulsar configuration for events
    pulsar_url: str = "pulsar://pulsar:6650"
    pulsar_topic_prefix: str = "persistent://public/default/"
    pulsar_budget_alerts_topic: str = "budget-alerts"
    pulsar_cost_events_topic: str = "cost-events"
    
    # Consul configuration
    consul_host: str = "consul"
    consul_port: int = 8500
    consul_service_name: str = "metering-aggregation-service"
    
    # Analysis configuration
    cost_analysis_cache_ttl: int = 300  # 5 minutes
    budget_check_interval_minutes: int = 15
    recommendation_min_savings_percent: float = 5.0
    anomaly_detection_threshold: float = 2.0  # Standard deviations
    
    class Config:
        env_file = ".env"
        case_sensitive = False 