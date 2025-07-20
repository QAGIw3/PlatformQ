"""Configuration for Quota Management Service"""

from pydantic_settings import BaseSettings
from typing import Optional, Dict, Any


class Settings(BaseSettings):
    """Service configuration"""
    
    # Service info
    service_name: str = "quota-management-service"
    service_port: int = 8091
    
    # Cassandra configuration
    cassandra_hosts: str = "cassandra:9042"
    cassandra_keyspace: str = "platformq"
    cassandra_username: Optional[str] = None
    cassandra_password: Optional[str] = None
    
    # Ignite configuration
    ignite_host: str = "ignite"
    ignite_port: int = 10800
    ignite_cache_name: str = "quota-cache"
    
    # Pulsar configuration
    pulsar_url: str = "pulsar://pulsar:6650"
    pulsar_topic_prefix: str = "persistent://public/default/"
    pulsar_subscription: str = "quota-management-service"
    pulsar_quota_events_topic: str = "quota-events"
    pulsar_resource_events_topic: str = "resource-events"
    
    # Consul configuration
    consul_host: str = "consul"
    consul_port: int = 8500
    consul_service_name: str = "quota-management-service"
    consul_service_id: str = "quota-management-service-1"
    consul_health_check_interval: str = "10s"
    consul_deregister_critical_after: str = "30s"
    
    # Quota management
    quota_check_interval_seconds: int = 60  # Check quotas every minute
    quota_enforcement_enabled: bool = True
    quota_soft_limit_threshold: float = 0.8  # Warn at 80%
    quota_hard_limit_threshold: float = 1.0  # Block at 100%
    
    # Default quotas (per tenant)
    default_quota_cpu_cores: int = 100
    default_quota_memory_gb: int = 256
    default_quota_storage_gb: int = 1000
    default_quota_instances: int = 50
    default_quota_networks: int = 10
    default_quota_databases: int = 5
    
    # Resource tracking
    usage_tracking_enabled: bool = True
    usage_cache_ttl_seconds: int = 300  # 5 minutes
    usage_history_retention_days: int = 90
    
    # Alerts and notifications
    quota_alert_enabled: bool = True
    quota_alert_thresholds: str = "50,75,90,95"  # Comma-separated percentages
    
    # Rate limiting
    rate_limit_enabled: bool = True
    rate_limit_requests_per_minute: int = 100
    
    # Logging
    log_level: str = "INFO"
    
    class Config:
        env_file = ".env"
        env_file_encoding = "utf-8"


# Create settings instance
settings = Settings() 