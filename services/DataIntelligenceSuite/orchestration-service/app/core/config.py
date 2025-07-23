"""
Orchestration Service Configuration
"""
from typing import List, Optional, Dict, Any
from pydantic import Field

# Import from common library
from data_intelligence_common.base_service.config import ServiceConfig
from data_intelligence_common.core.config.config_manager import ConfigManager


class OrchestrationConfig(ServiceConfig):
    """Orchestration Service configuration extending common config"""
    
    # Service identification
    service_name: str = Field(default="orchestration-service", env="SERVICE_NAME")
    service_version: str = Field(default="2.0.0", env="SERVICE_VERSION")
    
    # Apache Airflow Configuration
    airflow_enabled: bool = Field(default=True, env="AIRFLOW_ENABLED")
    airflow_api_url: str = Field(default="http://airflow-webserver:8080", env="AIRFLOW_API_URL")
    airflow_username: str = Field(default="airflow", env="AIRFLOW_USERNAME")
    airflow_password: str = Field(default="airflow", env="AIRFLOW_PASSWORD")
    airflow_dags_folder: str = Field(default="/opt/airflow/dags", env="AIRFLOW_DAGS_FOLDER")
    
    # SeaTunnel Configuration
    seatunnel_enabled: bool = Field(default=True, env="SEATUNNEL_ENABLED")
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
    
    # Kubernetes Configuration
    k8s_enabled: bool = Field(default=True, env="K8S_ENABLED")
    k8s_namespace: str = Field(default="default", env="K8S_NAMESPACE")
    k8s_in_cluster: bool = Field(default=True, env="K8S_IN_CLUSTER")
    k8s_config_file: Optional[str] = Field(default=None, env="K8S_CONFIG_FILE")
    
    # Workflow Configuration
    workflow_templates_path: str = Field(default="/config/workflow-templates", env="WORKFLOW_TEMPLATES_PATH")
    enable_dynamic_dags: bool = Field(default=True, env="ENABLE_DYNAMIC_DAGS")
    dag_refresh_interval: int = Field(default=60, env="DAG_REFRESH_INTERVAL")  # seconds
    max_concurrent_workflows: int = Field(default=100, env="MAX_CONCURRENT_WORKFLOWS")
    
    # Pipeline Configuration
    pipeline_storage_backend: str = Field(default="ignite", env="PIPELINE_STORAGE_BACKEND")
    pipeline_execution_mode: str = Field(default="async", env="PIPELINE_EXECUTION_MODE")
    max_pipeline_retries: int = Field(default=3, env="MAX_PIPELINE_RETRIES")
    default_timeout: int = Field(default=3600, env="DEFAULT_TIMEOUT")  # 1 hour
    
    # Event-Driven Configuration
    event_mapping_enabled: bool = Field(default=True, env="EVENT_MAPPING_ENABLED")
    event_correlation_window: int = Field(default=300, env="EVENT_CORRELATION_WINDOW")  # seconds
    event_buffer_size: int = Field(default=1000, env="EVENT_BUFFER_SIZE")
    
    # Verifiable Credentials
    vc_enabled: bool = Field(default=True, env="VC_ENABLED")
    vc_issuer: str = Field(default="orchestration-service", env="VC_ISSUER")
    vc_signing_key_path: Optional[str] = Field(default=None, env="VC_SIGNING_KEY_PATH")
    
    # Resource Management
    resource_quota_enabled: bool = Field(default=True, env="RESOURCE_QUOTA_ENABLED")
    max_cpu_per_workflow: float = Field(default=4.0, env="MAX_CPU_PER_WORKFLOW")
    max_memory_per_workflow: str = Field(default="8Gi", env="MAX_MEMORY_PER_WORKFLOW")
    
    # Performance Configuration
    cache_ttl: int = Field(default=300, env="CACHE_TTL")  # 5 minutes
    batch_size: int = Field(default=100, env="BATCH_SIZE")
    worker_pool_size: int = Field(default=10, env="WORKER_POOL_SIZE")
    
    # Integration Services
    ml_platform_service_url: str = Field(default="http://ml-platform-service:8000", env="ML_PLATFORM_SERVICE_URL")
    data_platform_service_url: str = Field(default="http://data-platform-service:8000", env="DATA_PLATFORM_SERVICE_URL")
    governance_service_url: str = Field(default="http://data-governance-service:8000", env="GOVERNANCE_SERVICE_URL")
    
    # Monitoring
    enable_workflow_metrics: bool = Field(default=True, env="ENABLE_WORKFLOW_METRICS")
    metrics_collection_interval: int = Field(default=60, env="METRICS_COLLECTION_INTERVAL")
    
    class Config:
        env_file = ".env"
        env_file_encoding = "utf-8"
        case_sensitive = False


def get_config() -> OrchestrationConfig:
    """Get service configuration with Vault/Consul integration"""
    config_manager = ConfigManager()
    
    # Load base configuration
    config = OrchestrationConfig()
    
    # Override with Consul config if available
    consul_config = config_manager.get_consul_config("orchestration-service")
    if consul_config:
        config = OrchestrationConfig(**consul_config)
    
    # Load secrets from Vault
    vault_secrets = config_manager.get_vault_secrets("orchestration-service")
    if vault_secrets:
        # Update sensitive fields
        for key, value in vault_secrets.items():
            if hasattr(config, key):
                setattr(config, key, value)
    
    return config 