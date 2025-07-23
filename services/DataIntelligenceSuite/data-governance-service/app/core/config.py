"""
Data Governance Service Configuration
"""
from typing import List, Optional, Dict, Any
from pydantic import Field

# Import from common library
from data_intelligence_common.base_service.config import ServiceConfig
from data_intelligence_common.core.config.config_manager import ConfigManager


class DataGovernanceConfig(ServiceConfig):
    """Data Governance Service configuration extending common config"""
    
    # Service identification
    service_name: str = Field(default="data-governance-service", env="SERVICE_NAME")
    service_version: str = Field(default="2.0.0", env="SERVICE_VERSION")
    
    # Quality Engine Configuration
    quality_engine_enabled: bool = Field(default=True, env="QUALITY_ENGINE_ENABLED")
    quality_check_parallelism: int = Field(default=4, env="QUALITY_CHECK_PARALLELISM")
    quality_cache_ttl_seconds: int = Field(default=3600, env="QUALITY_CACHE_TTL_SECONDS")
    
    # ML-based quality
    ml_quality_enabled: bool = Field(default=True, env="ML_QUALITY_ENABLED")
    ml_model_cache_size: int = Field(default=10, env="ML_MODEL_CACHE_SIZE")
    anomaly_detection_threshold: float = Field(default=0.95, env="ANOMALY_DETECTION_THRESHOLD")
    
    # Profiling Configuration
    profiling_sample_size: int = Field(default=10000, env="PROFILING_SAMPLE_SIZE")
    profiling_timeout_seconds: int = Field(default=300, env="PROFILING_TIMEOUT_SECONDS")
    enable_advanced_profiling: bool = Field(default=True, env="ENABLE_ADVANCED_PROFILING")
    
    # Remediation Configuration
    auto_remediation_enabled: bool = Field(default=False, env="AUTO_REMEDIATION_ENABLED")
    remediation_approval_required: bool = Field(default=True, env="REMEDIATION_APPROVAL_REQUIRED")
    max_remediation_retries: int = Field(default=3, env="MAX_REMEDIATION_RETRIES")
    
    # Policy Engine Configuration
    policy_engine_enabled: bool = Field(default=True, env="POLICY_ENGINE_ENABLED")
    policy_evaluation_cache_ttl: int = Field(default=300, env="POLICY_EVALUATION_CACHE_TTL")
    policy_enforcement_mode: str = Field(default="monitor", env="POLICY_ENFORCEMENT_MODE")  # monitor, enforce
    
    # Compliance Configuration
    compliance_frameworks: List[str] = Field(
        default_factory=lambda: ["GDPR", "CCPA", "HIPAA"],
        env="COMPLIANCE_FRAMEWORKS"
    )
    compliance_scan_interval_hours: int = Field(default=24, env="COMPLIANCE_SCAN_INTERVAL_HOURS")
    compliance_report_retention_days: int = Field(default=365, env="COMPLIANCE_REPORT_RETENTION_DAYS")
    
    # Privacy Configuration
    privacy_request_sla_hours: int = Field(default=72, env="PRIVACY_REQUEST_SLA_HOURS")
    pii_detection_enabled: bool = Field(default=True, env="PII_DETECTION_ENABLED")
    data_masking_enabled: bool = Field(default=True, env="DATA_MASKING_ENABLED")
    
    # Catalog Integration
    catalog_sync_enabled: bool = Field(default=True, env="CATALOG_SYNC_ENABLED")
    catalog_sync_interval_minutes: int = Field(default=15, env="CATALOG_SYNC_INTERVAL_MINUTES")
    
    # Lineage Configuration
    lineage_tracking_enabled: bool = Field(default=True, env="LINEAGE_TRACKING_ENABLED")
    lineage_depth_limit: int = Field(default=10, env="LINEAGE_DEPTH_LIMIT")
    
    # Storage Configuration (using common MinIO config)
    quality_reports_bucket: str = Field(default="quality-reports", env="QUALITY_REPORTS_BUCKET")
    compliance_reports_bucket: str = Field(default="compliance-reports", env="COMPLIANCE_REPORTS_BUCKET")
    
    # Integration Services (from common)
    ml_platform_service_url: str = Field(default="http://ml-platform-service:8000", env="ML_PLATFORM_SERVICE_URL")
    data_platform_service_url: str = Field(default="http://data-platform-service:8000", env="DATA_PLATFORM_SERVICE_URL")
    
    # Advanced Features
    enable_data_contracts: bool = Field(default=True, env="ENABLE_DATA_CONTRACTS")
    enable_cost_governance: bool = Field(default=True, env="ENABLE_COST_GOVERNANCE")
    enable_access_reviews: bool = Field(default=True, env="ENABLE_ACCESS_REVIEWS")
    access_review_interval_days: int = Field(default=90, env="ACCESS_REVIEW_INTERVAL_DAYS")
    
    # Performance Tuning
    max_concurrent_quality_checks: int = Field(default=10, env="MAX_CONCURRENT_QUALITY_CHECKS")
    quality_check_queue_size: int = Field(default=1000, env="QUALITY_CHECK_QUEUE_SIZE")
    
    # Notification Configuration
    notification_channels: List[str] = Field(
        default_factory=lambda: ["email", "slack", "webhook"],
        env="NOTIFICATION_CHANNELS"
    )
    critical_alert_recipients: List[str] = Field(
        default_factory=list,
        env="CRITICAL_ALERT_RECIPIENTS"
    )
    
    class Config:
        env_file = ".env"
        env_file_encoding = "utf-8"
        case_sensitive = False


def get_config() -> DataGovernanceConfig:
    """Get service configuration with Vault/Consul integration"""
    config_manager = ConfigManager()
    
    # Load base configuration
    config = DataGovernanceConfig()
    
    # Override with Consul config if available
    consul_config = config_manager.get_consul_config("data-governance-service")
    if consul_config:
        config = DataGovernanceConfig(**consul_config)
    
    # Load secrets from Vault
    vault_secrets = config_manager.get_vault_secrets("data-governance-service")
    if vault_secrets:
        # Update sensitive fields
        for key, value in vault_secrets.items():
            if hasattr(config, key):
                setattr(config, key, value)
    
    return config 