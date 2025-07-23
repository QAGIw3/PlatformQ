"""
Service-specific configuration classes.
"""

from dataclasses import dataclass, field
from typing import Dict, List, Optional
from datetime import timedelta

from .base import ServiceConfig


@dataclass
class AnalyticsConfig(ServiceConfig):
    """Analytics Engine Service configuration"""
    name: str = "analytics-engine-service"
    
    # Query settings
    default_query_engine: str = "spark"
    max_query_results: int = 10000
    query_timeout: timedelta = timedelta(minutes=5)
    
    # Processing
    enable_spark: bool = True
    enable_flink: bool = True
    enable_trino: bool = True
    
    # Caching
    cache_query_results: bool = True
    cache_aggregations: bool = True


@dataclass
class MLPlatformConfig(ServiceConfig):
    """ML Platform Service configuration"""
    name: str = "ml-platform-service"
    
    # Model settings
    model_registry_path: str = "/models"
    max_model_size: int = 1024  # MB
    model_cache_size: int = 10
    
    # Training
    default_framework: str = "tensorflow"
    enable_gpu: bool = True
    max_training_time: timedelta = timedelta(hours=24)
    
    # Serving
    batch_inference_enabled: bool = True
    max_batch_size: int = 1000
    inference_timeout: timedelta = timedelta(seconds=30)


@dataclass
class DataPlatformConfig(ServiceConfig):
    """Data Platform Service configuration"""
    name: str = "data-platform-service"
    
    # Storage
    default_storage: str = "minio"
    data_retention_days: int = 90
    
    # Processing
    enable_streaming: bool = True
    enable_batch: bool = True
    
    # Catalog
    enable_data_catalog: bool = True
    auto_discovery: bool = True


@dataclass
class IntegrationHubConfig(ServiceConfig):
    """Integration Hub Service configuration"""
    name: str = "integration-hub-service"
    
    # Connectors
    enabled_connectors: List[str] = field(default_factory=list)
    connector_timeout: timedelta = timedelta(seconds=60)
    
    # Sync settings
    sync_interval: timedelta = timedelta(minutes=5)
    max_sync_records: int = 10000
    
    # SeaTunnel
    seatunnel_enabled: bool = True
    seatunnel_config_path: str = "/config/seatunnel"


@dataclass
class OrchestrationConfig(ServiceConfig):
    """Orchestration Service configuration"""
    name: str = "orchestration-service"
    
    # Workflow settings
    max_workflow_duration: timedelta = timedelta(hours=24)
    default_retry_count: int = 3
    
    # Scheduling
    enable_scheduler: bool = True
    scheduler_interval: timedelta = timedelta(seconds=60)
    
    # Airflow
    airflow_enabled: bool = True
    airflow_dag_path: str = "/dags"


@dataclass
class GovernanceConfig(ServiceConfig):
    """Data Governance Service configuration"""
    name: str = "data-governance-service"
    
    # Policies
    enable_data_policies: bool = True
    policy_evaluation_mode: str = "enforce"  # enforce, audit, disabled
    
    # Lineage
    enable_lineage_tracking: bool = True
    lineage_retention_days: int = 365
    
    # Quality
    enable_quality_checks: bool = True
    quality_threshold: float = 0.95 