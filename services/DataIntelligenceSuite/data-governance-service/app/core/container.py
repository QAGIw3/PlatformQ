"""
Dependency injection container for Data Governance Service
"""
from dependency_injector import containers, providers

# Import from common library
from data_intelligence_common.core.caching.cache_manager import CacheManager
from data_intelligence_common.core.events.event_bus import EventBus
from data_intelligence_common.integrations.ignite_client import IgniteClient
from data_intelligence_common.integrations.elasticsearch_client import ElasticsearchClient
from data_intelligence_common.integrations.cassandra_client import CassandraClient
from data_intelligence_common.integrations.minio_client import MinIOClient
from data_intelligence_common.integrations.pulsar_client import PulsarClient
from data_intelligence_common.integrations.atlas_client import AtlasClient
from data_intelligence_common.integrations.seatunnel_client import SeaTunnelClient
from data_intelligence_common.integrations.great_expectations_client import GreatExpectationsClient
from data_intelligence_common.integrations.soda_core_client import SodaCoreClient
from data_intelligence_common.clients.ml_client import MLServiceClient
from data_intelligence_common.clients.catalog_client import CatalogServiceClient
from data_intelligence_common.clients.processing_client import ProcessingServiceClient
from data_intelligence_common.vault_consul.unified_integration import VaultConsulIntegration
from data_intelligence_common.monitoring.metrics import MetricsCollector
from data_intelligence_common.core.processing.quality_processor import QualityProcessor
from data_intelligence_common.core.catalog.discovery_engine import DiscoveryEngine
from data_intelligence_common.core.catalog.lineage_tracker import LineageTracker
from data_intelligence_common.core.governance.policy_engine import PolicyEngine

# Import service-specific components
from .config import DataGovernanceConfig, get_config
from .quality_engine import EnhancedQualityEngine
from .profiler import AdvancedProfiler
from .remediation_orchestrator import RemediationOrchestrator
from .anomaly_detector import MLAnomalyDetector
from .ml_optimizer import MLQualityOptimizer
from .policy_manager import GovernancePolicyManager
from .compliance_manager import ComplianceManager
from .privacy_manager import PrivacyManager
from .contract_manager import DataContractManager
from .access_manager import AccessControlManager


class Container(containers.DeclarativeContainer):
    """DI Container for Data Governance Service"""
    
    # Configuration
    config = providers.Singleton(get_config)
    
    # Vault/Consul Integration
    vault_consul = providers.Singleton(
        VaultConsulIntegration,
        service_name="data-governance-service",
        vault_url=config.provided.vault_url,
        consul_url=config.provided.consul_url,
        vault_token=config.provided.vault_token
    )
    
    # Metrics Collector
    metrics_collector = providers.Singleton(
        MetricsCollector,
        service_name="data-governance-service",
        pushgateway_url=config.provided.prometheus_pushgateway_url
    )
    
    # Event Bus
    event_bus = providers.Singleton(
        EventBus,
        backend="pulsar",
        config={
            "url": config.provided.pulsar_url,
            "topic_prefix": "data-governance"
        }
    )
    
    # Cache Manager
    cache_manager = providers.Singleton(
        CacheManager,
        ignite_nodes=[(config.provided.ignite_host, config.provided.ignite_port)],
        service_name="data-governance-service",
        vault_client=vault_consul.provided.vault_client,
        consul_client=vault_consul.provided.consul_client,
        metrics_collector=metrics_collector,
        enable_encryption=True
    )
    
    # Infrastructure Clients
    ignite_client = providers.Singleton(
        IgniteClient,
        host=config.provided.ignite_host,
        port=config.provided.ignite_port,
        cache_name="governance_cache"
    )
    
    elasticsearch_client = providers.Singleton(
        ElasticsearchClient,
        hosts=config.provided.elasticsearch_hosts,
        username=config.provided.elasticsearch_username,
        password=config.provided.elasticsearch_password
    )
    
    cassandra_client = providers.Singleton(
        CassandraClient,
        hosts=config.provided.cassandra_hosts,
        port=config.provided.cassandra_port,
        keyspace=config.provided.cassandra_keyspace
    )
    
    minio_client = providers.Singleton(
        MinIOClient,
        endpoint=config.provided.minio_endpoint,
        access_key=config.provided.minio_access_key,
        secret_key=config.provided.minio_secret_key,
        secure=config.provided.minio_secure
    )
    
    pulsar_client = providers.Singleton(
        PulsarClient,
        url=config.provided.pulsar_url,
        topic_prefix="data-governance"
    )
    
    atlas_client = providers.Singleton(
        AtlasClient,
        base_url=config.provided.atlas_url,
        username=config.provided.atlas_username,
        password=config.provided.atlas_password
    )
    
    seatunnel_client = providers.Singleton(
        SeaTunnelClient,
        api_url=config.provided.seatunnel_api_url,
        home_path=config.provided.seatunnel_home
    )
    
    great_expectations_client = providers.Singleton(
        GreatExpectationsClient,
        config_path="/etc/great_expectations"
    )
    
    soda_core_client = providers.Singleton(
        SodaCoreClient,
        config_path="/etc/soda"
    )
    
    # Service Clients
    ml_service_client = providers.Singleton(
        MLServiceClient,
        base_url=config.provided.ml_platform_service_url,
        vault_client=vault_consul.provided.vault_client,
        consul_client=vault_consul.provided.consul_client
    )
    
    catalog_service_client = providers.Singleton(
        CatalogServiceClient,
        base_url=config.provided.data_platform_service_url,
        vault_client=vault_consul.provided.vault_client,
        consul_client=vault_consul.provided.consul_client
    )
    
    processing_service_client = providers.Singleton(
        ProcessingServiceClient,
        base_url=config.provided.data_platform_service_url,
        vault_client=vault_consul.provided.vault_client,
        consul_client=vault_consul.provided.consul_client
    )
    
    # Common Components
    quality_processor = providers.Singleton(
        QualityProcessor,
        cache_manager=cache_manager,
        event_bus=event_bus,
        metrics_collector=metrics_collector
    )
    
    discovery_engine = providers.Singleton(
        DiscoveryEngine,
        cache_manager=cache_manager,
        event_bus=event_bus,
        vault_client=vault_consul.provided.vault_client,
        consul_client=vault_consul.provided.consul_client
    )
    
    lineage_tracker = providers.Singleton(
        LineageTracker,
        atlas_client=atlas_client,
        cache_manager=cache_manager,
        event_bus=event_bus
    )
    
    policy_engine = providers.Singleton(
        PolicyEngine,
        cache_manager=cache_manager,
        event_bus=event_bus,
        vault_client=vault_consul.provided.vault_client
    )
    
    # Service-Specific Managers
    quality_engine = providers.Singleton(
        EnhancedQualityEngine,
        config=config,
        quality_processor=quality_processor,
        cache_manager=cache_manager,
        event_bus=event_bus,
        ignite_client=ignite_client,
        elasticsearch_client=elasticsearch_client,
        minio_client=minio_client,
        seatunnel_client=seatunnel_client,
        great_expectations_client=great_expectations_client,
        soda_core_client=soda_core_client,
        ml_service_client=ml_service_client,
        metrics_collector=metrics_collector
    )
    
    profiler = providers.Singleton(
        AdvancedProfiler,
        config=config,
        cache_manager=cache_manager,
        elasticsearch_client=elasticsearch_client,
        ml_service_client=ml_service_client,
        metrics_collector=metrics_collector
    )
    
    remediation_orchestrator = providers.Singleton(
        RemediationOrchestrator,
        config=config,
        quality_engine=quality_engine,
        cache_manager=cache_manager,
        event_bus=event_bus,
        ml_service_client=ml_service_client,
        processing_service_client=processing_service_client,
        metrics_collector=metrics_collector
    )
    
    anomaly_detector = providers.Singleton(
        MLAnomalyDetector,
        config=config,
        ml_service_client=ml_service_client,
        cache_manager=cache_manager,
        event_bus=event_bus,
        metrics_collector=metrics_collector
    )
    
    ml_optimizer = providers.Singleton(
        MLQualityOptimizer,
        config=config,
        quality_engine=quality_engine,
        ml_service_client=ml_service_client,
        cache_manager=cache_manager,
        metrics_collector=metrics_collector
    )
    
    policy_manager = providers.Singleton(
        GovernancePolicyManager,
        config=config,
        policy_engine=policy_engine,
        cache_manager=cache_manager,
        event_bus=event_bus,
        cassandra_client=cassandra_client,
        metrics_collector=metrics_collector
    )
    
    compliance_manager = providers.Singleton(
        ComplianceManager,
        config=config,
        policy_manager=policy_manager,
        quality_engine=quality_engine,
        catalog_service_client=catalog_service_client,
        minio_client=minio_client,
        event_bus=event_bus,
        metrics_collector=metrics_collector
    )
    
    privacy_manager = providers.Singleton(
        PrivacyManager,
        config=config,
        catalog_service_client=catalog_service_client,
        policy_engine=policy_engine,
        cassandra_client=cassandra_client,
        event_bus=event_bus,
        metrics_collector=metrics_collector
    )
    
    contract_manager = providers.Singleton(
        DataContractManager,
        config=config,
        quality_engine=quality_engine,
        catalog_service_client=catalog_service_client,
        cassandra_client=cassandra_client,
        event_bus=event_bus,
        metrics_collector=metrics_collector
    )
    
    access_manager = providers.Singleton(
        AccessControlManager,
        config=config,
        policy_engine=policy_engine,
        catalog_service_client=catalog_service_client,
        cassandra_client=cassandra_client,
        event_bus=event_bus,
        metrics_collector=metrics_collector
    ) 