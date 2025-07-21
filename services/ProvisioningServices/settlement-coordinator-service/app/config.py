"""Configuration settings for Settlement Coordinator Service"""

from pydantic_settings import BaseSettings
from typing import Optional, Dict, Any
import os


class Settings(BaseSettings):
    # Service Configuration
    service_name: str = "settlement-coordinator-service"
    service_version: str = "1.0.0"
    environment: str = "development"
    
    # API Configuration
    http_port: int = 8092
    grpc_port: int = 50051
    api_prefix: str = "/api/v1"
    
    # CloudKitty Configuration
    cloudkitty_url: str = os.getenv("CLOUDKITTY_URL", "http://localhost:8889")
    cloudkitty_api_version: str = "v1"
    cloudkitty_auth_url: str = os.getenv("CLOUDKITTY_AUTH_URL", "http://localhost:5000/v3")
    cloudkitty_username: str = os.getenv("CLOUDKITTY_USERNAME", "admin")
    cloudkitty_password: str = os.getenv("CLOUDKITTY_PASSWORD", "admin")
    cloudkitty_project_id: str = os.getenv("CLOUDKITTY_PROJECT_ID", "admin")
    
    # OpenMeter Configuration
    openmeter_url: str = os.getenv("OPENMETER_URL", "http://localhost:8888")
    openmeter_api_key: Optional[str] = os.getenv("OPENMETER_API_KEY", None)
    openmeter_namespace: str = "compute-resources"
    
    # Apache Ignite Configuration
    ignite_host: str = os.getenv("IGNITE_HOST", "ignite")
    ignite_port: int = 10800
    ignite_username: Optional[str] = os.getenv("IGNITE_USERNAME", None)
    ignite_password: Optional[str] = os.getenv("IGNITE_PASSWORD", None)
    
    # Blockchain Configuration for Tokenization
    enable_tokenization: bool = os.getenv("ENABLE_TOKENIZATION", "true").lower() == "true"
    blockchain_chain_id: int = int(os.getenv("BLOCKCHAIN_CHAIN_ID", "1"))
    blockchain_rpc_url: str = os.getenv("BLOCKCHAIN_RPC_URL", "http://localhost:8545")
    resource_token_contract: str = os.getenv("RESOURCE_TOKEN_CONTRACT", "")
    tokenizer_private_key: str = os.getenv("TOKENIZER_PRIVATE_KEY", "")
    
    # Risk Calculation Parameters
    risk_alpha: float = 1.4  # SA-CCR alpha parameter
    risk_confidence_level: float = 0.95  # 95% confidence for Monte Carlo
    risk_downtime_penalty_factor: float = 2.0  # Penalty multiplier for downtime
    risk_volatility_window_days: int = 30  # Days to calculate volatility
    risk_monte_carlo_simulations: int = 10000  # Number of MC simulations
    risk_cache_ttl_seconds: int = 300  # 5 minutes cache for risk scores
    
    # Risk Thresholds
    risk_threshold_low: float = 0.1
    risk_threshold_medium: float = 0.3  # 10-30% - medium risk
    risk_threshold_high: float = 0.5  # 30-50% - high risk
    # Above 50% - critical risk
    
    # Pulsar Configuration
    pulsar_url: str = os.getenv("PULSAR_URL", "pulsar://pulsar:6650")
    pulsar_topic_settlements: str = "persistent://public/default/settlements"
    pulsar_topic_risk_events: str = "persistent://public/default/risk-events"
    pulsar_topic_billing_events: str = "persistent://public/default/billing-events"
    pulsar_subscription: str = "settlement-coordinator"
    
    # Prometheus Configuration
    prometheus_url: str = os.getenv("PROMETHEUS_URL", "http://prometheus:9090")
    prometheus_sla_query_template: str = 'avg_over_time(up{job="compute-provider",provider_id="%s"}[%s])'
    
    # Consul Configuration
    consul_host: str = os.getenv("CONSUL_HOST", "consul")
    consul_port: int = 8500
    consul_service_name: str = "settlement-coordinator"
    consul_health_check_interval: str = "10s"
    consul_deregister_critical_after: str = "30s"
    
    # Vault Configuration
    vault_url: str = os.getenv("VAULT_URL", "http://vault:8200")
    vault_token: Optional[str] = os.getenv("VAULT_TOKEN", None)
    vault_mount_path: str = "secret"
    vault_secret_path: str = "platformq/settlement"
    
    # Settlement Configuration
    settlement_batch_size: int = 100
    settlement_worker_threads: int = 4
    settlement_timeout_seconds: int = 300
    settlement_retry_attempts: int = 3
    settlement_retry_delay_seconds: int = 10
    
    # Escrow Configuration
    escrow_buffer_percentage: float = 0.1  # 10% pre-provisioning buffer
    escrow_release_delay_hours: int = 24  # Hold escrow for 24 hours
    high_risk_escrow_hours: int = 72  # 3 days for high risk settlements
    
    # Cache Configuration
    cache_backend: str = "ignite"  # or "redis" for local dev
    cache_default_ttl: int = 3600
    cache_key_prefix: str = "settlement:"
    
    class Config:
        env_file = ".env"
        case_sensitive = False


settings = Settings() 