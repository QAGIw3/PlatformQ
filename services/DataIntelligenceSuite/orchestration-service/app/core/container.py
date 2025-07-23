"""
Dependency injection container for Orchestration Service
"""
from dependency_injector import containers, providers

# Import from common library
from data_intelligence_common.core.caching.cache_manager import CacheManager
from data_intelligence_common.core.events.event_bus import EventBus
from data_intelligence_common.integrations.ignite_client import IgniteClient
from data_intelligence_common.integrations.pulsar_client import PulsarClient
from data_intelligence_common.integrations.airflow_client import AirflowClient
from data_intelligence_common.integrations.seatunnel_client import SeaTunnelClient
from data_intelligence_common.clients.ml_client import MLServiceClient
from data_intelligence_common.clients.catalog_client import CatalogServiceClient
from data_intelligence_common.clients.processing_client import ProcessingServiceClient
from data_intelligence_common.vault_consul.unified_integration import VaultConsulIntegration
from data_intelligence_common.monitoring.metrics import MetricsCollector
from data_intelligence_common.core.orchestration.workflow_orchestrator import WorkflowOrchestrator
from data_intelligence_common.core.orchestration.pipeline_orchestrator import PipelineOrchestrator
from data_intelligence_common.core.orchestration.event_orchestrator import EventOrchestrator

# Import service-specific components
from .config import OrchestrationConfig, get_config
from .airflow_bridge import EnhancedAirflowBridge
from .pipeline_manager import AdvancedPipelineManager
from .ml_optimizer import MLPipelineOptimizer
from .seatunnel_orchestrator import EnhancedSeaTunnelOrchestrator
from .event_coordinator import EventCoordinator
from .k8s_manager import K8sJobManager
from .credential_attestor import WorkflowCredentialAttestor


class Container(containers.DeclarativeContainer):
    """DI Container for Orchestration Service"""
    
    # Configuration
    config = providers.Singleton(get_config)
    
    # Vault/Consul Integration
    vault_consul = providers.Singleton(
        VaultConsulIntegration,
        service_name="orchestration-service",
        vault_url=config.provided.vault_url,
        consul_url=config.provided.consul_url,
        vault_token=config.provided.vault_token
    )
    
    # Metrics Collector
    metrics_collector = providers.Singleton(
        MetricsCollector,
        service_name="orchestration-service",
        pushgateway_url=config.provided.prometheus_pushgateway_url
    )
    
    # Event Bus
    event_bus = providers.Singleton(
        EventBus,
        backend="pulsar",
        config={
            "url": config.provided.pulsar_url,
            "topic_prefix": "orchestration"
        }
    )
    
    # Cache Manager
    cache_manager = providers.Singleton(
        CacheManager,
        ignite_nodes=[(config.provided.ignite_host, config.provided.ignite_port)],
        service_name="orchestration-service",
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
        cache_name="orchestration_cache"
    )
    
    pulsar_client = providers.Singleton(
        PulsarClient,
        url=config.provided.pulsar_url,
        topic_prefix="orchestration"
    )
    
    airflow_client = providers.Singleton(
        AirflowClient,
        base_url=config.provided.airflow_api_url,
        username=config.provided.airflow_username,
        password=config.provided.airflow_password
    )
    
    seatunnel_client = providers.Singleton(
        SeaTunnelClient,
        api_url=config.provided.seatunnel_api_url,
        home_path=config.provided.seatunnel_home
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
    
    governance_service_client = providers.Singleton(
        CatalogServiceClient,  # Using catalog client for governance
        base_url=config.provided.governance_service_url,
        vault_client=vault_consul.provided.vault_client,
        consul_client=vault_consul.provided.consul_client
    )
    
    # Common Orchestrators
    workflow_orchestrator = providers.Singleton(
        WorkflowOrchestrator,
        cache_manager=cache_manager,
        event_bus=event_bus,
        metrics_collector=metrics_collector
    )
    
    pipeline_orchestrator = providers.Singleton(
        PipelineOrchestrator,
        cache_manager=cache_manager,
        event_bus=event_bus,
        metrics_collector=metrics_collector
    )
    
    event_orchestrator = providers.Singleton(
        EventOrchestrator,
        event_bus=event_bus,
        cache_manager=cache_manager,
        metrics_collector=metrics_collector
    )
    
    # Service-Specific Components
    airflow_bridge = providers.Singleton(
        EnhancedAirflowBridge,
        config=config,
        airflow_client=airflow_client,
        workflow_orchestrator=workflow_orchestrator,
        cache_manager=cache_manager,
        event_bus=event_bus,
        metrics_collector=metrics_collector
    )
    
    pipeline_manager = providers.Singleton(
        AdvancedPipelineManager,
        config=config,
        pipeline_orchestrator=pipeline_orchestrator,
        cache_manager=cache_manager,
        event_bus=event_bus,
        ignite_client=ignite_client,
        processing_service_client=processing_service_client,
        metrics_collector=metrics_collector
    )
    
    ml_optimizer = providers.Singleton(
        MLPipelineOptimizer,
        config=config,
        ml_service_client=ml_service_client,
        pipeline_manager=pipeline_manager,
        cache_manager=cache_manager,
        metrics_collector=metrics_collector
    )
    
    seatunnel_orchestrator = providers.Singleton(
        EnhancedSeaTunnelOrchestrator,
        config=config,
        seatunnel_client=seatunnel_client,
        pipeline_orchestrator=pipeline_orchestrator,
        cache_manager=cache_manager,
        event_bus=event_bus,
        metrics_collector=metrics_collector
    )
    
    event_coordinator = providers.Singleton(
        EventCoordinator,
        config=config,
        event_orchestrator=event_orchestrator,
        workflow_orchestrator=workflow_orchestrator,
        cache_manager=cache_manager,
        event_bus=event_bus,
        metrics_collector=metrics_collector
    )
    
    k8s_manager = providers.Singleton(
        K8sJobManager,
        config=config,
        cache_manager=cache_manager,
        event_bus=event_bus,
        metrics_collector=metrics_collector
    )
    
    credential_attestor = providers.Singleton(
        WorkflowCredentialAttestor,
        config=config,
        vault_client=vault_consul.provided.vault_client,
        cache_manager=cache_manager,
        event_bus=event_bus,
        metrics_collector=metrics_collector
    ) 