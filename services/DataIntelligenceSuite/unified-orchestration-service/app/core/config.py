"""
Configuration settings for Unified Orchestration Service
"""

from typing import Dict, Any, List, Optional
from pydantic_settings import BaseSettings
from pydantic import Field


class Settings(BaseSettings):
    """Service configuration settings"""
    
    # Service Information
    service_name: str = Field(default="unified-orchestration-service", env="SERVICE_NAME")
    service_port: int = Field(default=8019, env="SERVICE_PORT")
    environment: str = Field(default="development", env="ENVIRONMENT")
    
    # Apache Airflow Configuration
    airflow_enabled: bool = Field(default=True, env="AIRFLOW_ENABLED")
    airflow_api_url: str = Field(default="http://airflow-webserver:8080", env="AIRFLOW_API_URL")
    airflow_username: str = Field(default="airflow", env="AIRFLOW_USERNAME")
    airflow_password: str = Field(default="airflow", env="AIRFLOW_PASSWORD")
    airflow_dags_folder: str = Field(default="/opt/airflow/dags", env="AIRFLOW_DAGS_FOLDER")
    
    # SeaTunnel Configuration
    seatunnel_api_url: str = Field(default="http://seatunnel-api:8080", env="SEATUNNEL_API_URL")
    seatunnel_orchestration_templates: str = Field(
        default="/config/orchestration-templates",
        env="SEATUNNEL_ORCHESTRATION_TEMPLATES"
    )
    seatunnel_job_timeout: int = Field(default=3600, env="SEATUNNEL_JOB_TIMEOUT")
    
    # ML Optimization Configuration
    ml_optimization_enabled: bool = Field(default=True, env="ML_OPTIMIZATION_ENABLED")
    optimization_interval: int = Field(default=300, env="OPTIMIZATION_INTERVAL")  # 5 minutes
    learning_rate: float = Field(default=0.001, env="LEARNING_RATE")
    model_update_threshold: float = Field(default=0.05, env="MODEL_UPDATE_THRESHOLD")
    optimization_lookback_days: int = Field(default=30, env="OPTIMIZATION_LOOKBACK_DAYS")
    
    # Apache Ignite Configuration
    ignite_host: str = Field(default="ignite", env="IGNITE_HOST")
    ignite_port: int = Field(default=10800, env="IGNITE_PORT")
    ignite_cache_name: str = Field(default="orchestration_cache", env="IGNITE_CACHE_NAME")
    
    # Apache Pulsar Configuration
    pulsar_service_url: str = Field(default="pulsar://pulsar:6650", env="PULSAR_SERVICE_URL")
    pulsar_topic_prefix: str = Field(default="orchestration", env="PULSAR_TOPIC_PREFIX")
    pulsar_subscription_name: str = Field(default="orchestration-service", env="PULSAR_SUBSCRIPTION_NAME")
    
    # Service Discovery
    consul_host: str = Field(default="consul", env="CONSUL_HOST")
    consul_port: int = Field(default=8500, env="CONSUL_PORT")
    vault_addr: str = Field(default="http://vault:8200", env="VAULT_ADDR")
    vault_token: Optional[str] = Field(default=None, env="VAULT_TOKEN")
    
    # Resource Limits
    max_concurrent_workflows: int = Field(default=100, env="MAX_CONCURRENT_WORKFLOWS")
    max_pipeline_retries: int = Field(default=3, env="MAX_PIPELINE_RETRIES")
    default_timeout: int = Field(default=3600, env="DEFAULT_TIMEOUT")  # 1 hour
    max_dag_file_size: int = Field(default=1048576, env="MAX_DAG_FILE_SIZE")  # 1MB
    
    # Performance Configuration
    cache_ttl: int = Field(default=300, env="CACHE_TTL")  # 5 minutes
    batch_size: int = Field(default=100, env="BATCH_SIZE")
    worker_pool_size: int = Field(default=10, env="WORKER_POOL_SIZE")
    
    # Monitoring
    metrics_enabled: bool = Field(default=True, env="METRICS_ENABLED")
    log_level: str = Field(default="INFO", env="LOG_LEVEL")
    
    # Workflow Configuration
    workflow_templates_path: str = Field(default="/config/workflow-templates", env="WORKFLOW_TEMPLATES_PATH")
    enable_dynamic_dags: bool = Field(default=True, env="ENABLE_DYNAMIC_DAGS")
    dag_refresh_interval: int = Field(default=60, env="DAG_REFRESH_INTERVAL")  # seconds
    
    # Pipeline Configuration
    pipeline_storage_backend: str = Field(default="ignite", env="PIPELINE_STORAGE_BACKEND")
    pipeline_execution_mode: str = Field(default="async", env="PIPELINE_EXECUTION_MODE")
    enable_pipeline_caching: bool = Field(default=True, env="ENABLE_PIPELINE_CACHING")
    
    # Event-Driven Configuration
    event_mapping_enabled: bool = Field(default=True, env="EVENT_MAPPING_ENABLED")
    event_correlation_window: int = Field(default=300, env="EVENT_CORRELATION_WINDOW")  # seconds
    max_event_batch_size: int = Field(default=1000, env="MAX_EVENT_BATCH_SIZE")
    
    # Verifiable Credentials
    enable_workflow_attestations: bool = Field(default=True, env="ENABLE_WORKFLOW_ATTESTATIONS")
    attestation_issuer_did: Optional[str] = Field(default=None, env="ATTESTATION_ISSUER_DID")
    attestation_key_path: Optional[str] = Field(default=None, env="ATTESTATION_KEY_PATH")
    
    class Config:
        env_file = ".env"
        case_sensitive = False


# Create global settings instance
settings = Settings() 