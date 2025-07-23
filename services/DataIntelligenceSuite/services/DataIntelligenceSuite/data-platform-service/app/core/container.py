"""
Dependency injection container
"""

from dependency_injector import containers, providers
from data_intelligence_common.core.caching import CacheManager
from data_intelligence_common.monitoring import MetricsCollector
from data_intelligence_common.core.events import EventBus

from .config import settings
from ..services.health import HealthChecker


class Container(containers.DeclarativeContainer):
    """DI container for service dependencies"""
    
    config = providers.Singleton(lambda: settings)
    
    # Infrastructure
    cache_manager = providers.Singleton(
        CacheManager,
        config=config
    )
    
    metrics_collector = providers.Singleton(
        MetricsCollector,
        service_name=config().SERVICE_NAME
    )
    
    event_bus = providers.Singleton(
        EventBus,
        config=config
    )
    
    # Services
    health_checker = providers.Singleton(
        HealthChecker,
        cache_manager=cache_manager,
        event_bus=event_bus
    )
    
    async def init_resources(self):
        """Initialize resources"""
        await self.cache_manager().initialize()
        await self.event_bus().initialize()
        
    async def shutdown_resources(self):
        """Shutdown resources"""
        await self.event_bus().close()
        await self.cache_manager().close()
