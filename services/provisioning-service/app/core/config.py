"""Configuration settings for the Provisioning Service."""

from pydantic import BaseSettings, Field, validator
from typing import Dict, List, Optional
import os


class Settings(BaseSettings):
    """Configuration settings for the Provisioning Service."""
    
    # Service info
    service_name: str = "provisioning-service"
    environment: str = Field(default="development", env="ENVIRONMENT")
    debug: bool = Field(default=False, env="DEBUG")
    
    # API Configuration
    api_version: str = "v1"
    api_prefix: str = "/api"
    cors_origins: List[str] = Field(default=["*"], env="CORS_ORIGINS")
    
    # Database Configuration
    cassandra_config: Dict = Field(default_factory=lambda: {
        "hosts": os.getenv("CASSANDRA_HOSTS", "cassandra").split(","),
        "port": int(os.getenv("CASSANDRA_PORT", "9042")),
        "keyspace": os.getenv("CASSANDRA_KEYSPACE", "platformq"),
        "replication_factor": int(os.getenv("CASSANDRA_REPLICATION_FACTOR", "3")),
        "consistency_level": os.getenv("CASSANDRA_CONSISTENCY_LEVEL", "QUORUM")
    })
    
    # Storage Configuration
    minio_config: Dict = Field(default_factory=lambda: {
        "endpoint": os.getenv("MINIO_ENDPOINT", "minio:9000"),
        "access_key": os.getenv("MINIO_ACCESS_KEY", "minioadmin"),
        "secret_key": os.getenv("MINIO_SECRET_KEY", "minioadmin"),
        "secure": os.getenv("MINIO_SECURE", "false").lower() == "true",
        "region": os.getenv("MINIO_REGION", "us-east-1")
    })
    
    # Messaging Configuration
    pulsar_config: Dict = Field(default_factory=lambda: {
        "service_url": os.getenv("PULSAR_URL", "pulsar://pulsar:6650"),
        "admin_url": os.getenv("PULSAR_ADMIN_URL", "http://pulsar:8080"),
        "namespace": os.getenv("PULSAR_NAMESPACE", "platformq"),
        "max_pending_messages": int(os.getenv("PULSAR_MAX_PENDING_MESSAGES", "1000")),
        "batch_size": int(os.getenv("PULSAR_BATCH_SIZE", "100"))
    })
    
    # Compute Provider Configuration
    compute_providers: Dict = Field(default_factory=lambda: {
        "aws": {
            "enabled": os.getenv("AWS_ENABLED", "false").lower() == "true",
            "access_key": os.getenv("AWS_ACCESS_KEY", ""),
            "secret_key": os.getenv("AWS_SECRET_KEY", ""),
            "regions": os.getenv("AWS_REGIONS", "us-east-1,us-west-2").split(",")
        },
        "cloudstack": {
            "enabled": os.getenv("CLOUDSTACK_ENABLED", "true").lower() == "true",
            "api_url": os.getenv("CLOUDSTACK_API_URL", "http://cloudstack-management:8080/client/api"),
            "api_key": os.getenv("CLOUDSTACK_API_KEY", ""),
            "secret_key": os.getenv("CLOUDSTACK_SECRET_KEY", "")
        },
        "kubernetes": {
            "enabled": True,
            "namespace": os.getenv("K8S_NAMESPACE", "platformq"),
            "max_pods_per_node": int(os.getenv("K8S_MAX_PODS_PER_NODE", "110"))
        }
    })
    
    # Scaling Configuration
    scaling_config: Dict = Field(default_factory=lambda: {
        "enabled": os.getenv("SCALING_ENABLED", "true").lower() == "true",
        "evaluation_interval": int(os.getenv("SCALING_EVALUATION_INTERVAL", "30")),
        "cooldown_period": int(os.getenv("SCALING_COOLDOWN_PERIOD", "300")),
        "metrics_window": int(os.getenv("SCALING_METRICS_WINDOW", "300")),
        "predictive_enabled": os.getenv("PREDICTIVE_SCALING_ENABLED", "true").lower() == "true"
    })
    
    # Cost Management
    cost_config: Dict = Field(default_factory=lambda: {
        "optimization_enabled": os.getenv("COST_OPTIMIZATION_ENABLED", "true").lower() == "true",
        "budget_enforcement": os.getenv("BUDGET_ENFORCEMENT_ENABLED", "true").lower() == "true",
        "currency": os.getenv("DEFAULT_CURRENCY", "USD"),
        "alert_thresholds": [0.5, 0.75, 0.9, 1.0]
    })
    
    # Integration URLs
    derivatives_engine_url: str = Field(
        default="http://derivatives-engine-service:8000",
        env="DERIVATIVES_ENGINE_URL"
    )
    auth_service_url: str = Field(
        default="http://auth-service:8000",
        env="AUTH_SERVICE_URL"
    )
    
    # Vault Configuration
    vault_config: Dict = Field(default_factory=lambda: {
        "enabled": os.getenv("VAULT_ENABLED", "true").lower() == "true",
        "address": os.getenv("VAULT_ADDR", "http://vault:8200"),
        "token": os.getenv("VAULT_TOKEN", ""),
        "mount_path": os.getenv("VAULT_MOUNT_PATH", "secret"),
        "role": os.getenv("VAULT_ROLE", "provisioning-service")
    })
    
    # Monitoring
    monitoring_config: Dict = Field(default_factory=lambda: {
        "prometheus_enabled": os.getenv("PROMETHEUS_ENABLED", "true").lower() == "true",
        "jaeger_enabled": os.getenv("JAEGER_ENABLED", "true").lower() == "true",
        "jaeger_agent_host": os.getenv("JAEGER_AGENT_HOST", "jaeger"),
        "jaeger_agent_port": int(os.getenv("JAEGER_AGENT_PORT", "6831"))
    })
    
    @validator("environment")
    def validate_environment(cls, v):
        """Validate environment value"""
        allowed = ["development", "staging", "production"]
        if v not in allowed:
            raise ValueError(f"Environment must be one of {allowed}")
        return v
    
    @validator("compute_providers")
    def validate_providers(cls, v):
        """Ensure at least one provider is enabled"""
        enabled = [p for p, config in v.items() if config.get("enabled", False)]
        if not enabled:
            raise ValueError("At least one compute provider must be enabled")
        return v

    class Config:
        env_file = ".env"
        case_sensitive = False
        
    def get_enabled_providers(self) -> List[str]:
        """Get list of enabled compute providers"""
        return [
            name for name, config in self.compute_providers.items()
            if config.get("enabled", False)
        ]
        
    def is_production(self) -> bool:
        """Check if running in production"""
        return self.environment == "production"


# Singleton instance
settings = Settings() 