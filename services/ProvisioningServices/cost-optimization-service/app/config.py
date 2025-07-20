"""Configuration for Cost Optimization Service"""

from pydantic_settings import BaseSettings
from typing import Optional, Dict, Any


class Settings(BaseSettings):
    """Service configuration"""
    
    # Service info
    service_name: str = "cost-optimization-service"
    service_port: int = 8090
    
    # AWS configuration
    aws_access_key_id: Optional[str] = None
    aws_secret_access_key: Optional[str] = None
    aws_region: str = "us-east-1"
    aws_cost_explorer_enabled: bool = True
    
    # CloudStack configuration
    cloudstack_api_url: Optional[str] = None
    cloudstack_api_key: Optional[str] = None
    cloudstack_secret_key: Optional[str] = None
    
    # Kubernetes configuration
    kubernetes_config_type: str = "incluster"  # incluster or kubeconfig
    kubernetes_metrics_enabled: bool = True
    
    # Cassandra configuration
    cassandra_hosts: str = "cassandra:9042"
    cassandra_keyspace: str = "platformq"
    cassandra_username: Optional[str] = None
    cassandra_password: Optional[str] = None
    
    # Ignite configuration
    ignite_host: str = "ignite"
    ignite_port: int = 10800
    ignite_cache_name: str = "cost-optimization-cache"
    
    # Pulsar configuration
    pulsar_url: str = "pulsar://pulsar:6650"
    pulsar_topic_prefix: str = "persistent://public/default/"
    pulsar_subscription: str = "cost-optimization-service"
    pulsar_cost_events_topic: str = "cost-events"
    pulsar_budget_alerts_topic: str = "budget-alerts"
    
    # Consul configuration
    consul_host: str = "consul"
    consul_port: int = 8500
    consul_service_name: str = "cost-optimization-service"
    consul_service_id: str = "cost-optimization-service-1"
    consul_health_check_interval: str = "10s"
    consul_deregister_critical_after: str = "30s"
    
    # Cost analysis configuration
    cost_analysis_interval_hours: int = 24
    cost_anomaly_threshold_percent: float = 20.0
    cost_optimization_min_savings_percent: float = 5.0
    
    # Budget management
    budget_check_interval_hours: int = 6
    budget_alert_thresholds: str = "50,75,90,100"  # Comma-separated percentages
    
    # Recommendation engine
    recommendation_lookback_days: int = 30
    recommendation_confidence_threshold: float = 0.7
    
    # Resource rightsizing
    cpu_utilization_low_threshold: float = 20.0
    cpu_utilization_high_threshold: float = 80.0
    memory_utilization_low_threshold: float = 30.0
    memory_utilization_high_threshold: float = 85.0
    
    # Reserved instance recommendations
    ri_recommendation_min_savings: float = 100.0  # Minimum monthly savings in USD
    ri_recommendation_min_usage_days: int = 20  # Minimum days of consistent usage
    
    # Logging
    log_level: str = "INFO"
    
    class Config:
        env_file = ".env"
        env_file_encoding = "utf-8"


# Create settings instance
settings = Settings() 