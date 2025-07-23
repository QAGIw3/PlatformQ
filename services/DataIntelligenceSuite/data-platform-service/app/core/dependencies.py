"""
Dependency injection for Data Platform Service.
"""

import os
from typing import Optional

from data_intelligence_common.core.events import EventBus
from data_intelligence_common.core.caching import CacheManager
from data_intelligence_common.integrations import IgniteClient

# Type imports for type hints
from app.engines.feature import (
    FeatureStore,
    FeatureRegistry,
    FeatureServer,
    FeatureCompute
)
from app.engines.storage import (
    StorageManager,
    DocumentConverter,
    PreviewGenerator,
    ContentIndexer,
    QuotaManager
)

# Common Dependencies
_event_bus: Optional[EventBus] = None
_cache_manager: Optional[CacheManager] = None
_ignite_client: Optional[IgniteClient] = None


async def get_event_bus() -> EventBus:
    """Get event bus instance."""
    global _event_bus
    if not _event_bus:
        _event_bus = EventBus(
            pulsar_url=os.getenv("PULSAR_URL", "pulsar://pulsar:6650")
        )
        await _event_bus.initialize()
    return _event_bus


async def get_cache_manager() -> CacheManager:
    """Get cache manager instance."""
    global _cache_manager
    if not _cache_manager:
        _cache_manager = CacheManager(
            redis_url=os.getenv("REDIS_URL", "redis://redis:6379")
        )
    return _cache_manager


async def get_ignite_client() -> IgniteClient:
    """Get Ignite client instance."""
    global _ignite_client
    if not _ignite_client:
        _ignite_client = IgniteClient(
            host=os.getenv("IGNITE_HOST", "ignite"),
            port=int(os.getenv("IGNITE_PORT", "10800"))
        )
        await _ignite_client.connect()
    return _ignite_client


# Feature Store Dependencies
_feature_store: Optional[FeatureStore] = None
_feature_registry: Optional[FeatureRegistry] = None
_feature_server: Optional[FeatureServer] = None
_feature_compute: Optional[FeatureCompute] = None

# Storage Dependencies
_storage_manager: Optional[StorageManager] = None
_document_converter: Optional[DocumentConverter] = None
_preview_generator: Optional[PreviewGenerator] = None
_content_indexer: Optional[ContentIndexer] = None
_quota_manager: Optional[QuotaManager] = None


async def get_feature_store() -> FeatureStore:
    """Get feature store instance."""
    global _feature_store
    if not _feature_store:
        from app.engines.feature import FeatureStore
        _feature_store = FeatureStore(
            event_bus=await get_event_bus(),
            cache_manager=await get_cache_manager(),
            ignite_client=await get_ignite_client()
        )
        await _feature_store.initialize()
    return _feature_store


async def get_feature_registry() -> FeatureRegistry:
    """Get feature registry instance."""
    global _feature_registry
    if not _feature_registry:
        from app.engines.feature import FeatureRegistry
        _feature_registry = FeatureRegistry(
            event_bus=await get_event_bus(),
            cache_manager=await get_cache_manager()
        )
        await _feature_registry.initialize()
    return _feature_registry


async def get_feature_server() -> FeatureServer:
    """Get feature server instance."""
    global _feature_server
    if not _feature_server:
        from app.engines.feature import FeatureServer
        _feature_server = FeatureServer(
            event_bus=await get_event_bus(),
            cache_manager=await get_cache_manager(),
            feature_store=await get_feature_store()
        )
        await _feature_server.initialize()
    return _feature_server


async def get_feature_compute() -> FeatureCompute:
    """Get feature compute instance."""
    global _feature_compute
    if not _feature_compute:
        from app.engines.feature import FeatureCompute
        _feature_compute = FeatureCompute(
            event_bus=await get_event_bus(),
            cache_manager=await get_cache_manager()
        )
        await _feature_compute.initialize()
    return _feature_compute 


async def get_storage_manager() -> StorageManager:
    """Get storage manager instance."""
    global _storage_manager
    if not _storage_manager:
        from app.engines.storage import StorageManager, StorageBackend
        _storage_manager = StorageManager(
            event_bus=await get_event_bus(),
            cache_manager=await get_cache_manager(),
            backend=StorageBackend.MINIO,
            config={
                "endpoint": os.getenv("MINIO_ENDPOINT", "minio:9000"),
                "access_key": os.getenv("MINIO_ACCESS_KEY", "minioadmin"),
                "secret_key": os.getenv("MINIO_SECRET_KEY", "minioadmin"),
                "secure": os.getenv("MINIO_SECURE", "false").lower() == "true"
            }
        )
        await _storage_manager.initialize()
    return _storage_manager


async def get_document_converter() -> DocumentConverter:
    """Get document converter instance."""
    global _document_converter
    if not _document_converter:
        from app.engines.storage import DocumentConverter
        _document_converter = DocumentConverter(
            event_bus=await get_event_bus(),
            cache_manager=await get_cache_manager(),
            storage_manager=await get_storage_manager()
        )
        await _document_converter.initialize()
    return _document_converter


async def get_preview_generator() -> PreviewGenerator:
    """Get preview generator instance."""
    global _preview_generator
    if not _preview_generator:
        from app.engines.storage import PreviewGenerator
        _preview_generator = PreviewGenerator(
            event_bus=await get_event_bus(),
            cache_manager=await get_cache_manager(),
            storage_manager=await get_storage_manager()
        )
        await _preview_generator.initialize()
    return _preview_generator


async def get_content_indexer() -> ContentIndexer:
    """Get content indexer instance."""
    global _content_indexer
    if not _content_indexer:
        from app.engines.storage import ContentIndexer
        _content_indexer = ContentIndexer(
            event_bus=await get_event_bus(),
            cache_manager=await get_cache_manager(),
            elasticsearch_url=os.getenv("ELASTICSEARCH_URL", "http://elasticsearch:9200")
        )
        await _content_indexer.initialize()
    return _content_indexer


async def get_quota_manager() -> QuotaManager:
    """Get quota manager instance."""
    global _quota_manager
    if not _quota_manager:
        from app.engines.storage import QuotaManager
        _quota_manager = QuotaManager(
            event_bus=await get_event_bus(),
            cache_manager=await get_cache_manager()
        )
        await _quota_manager.initialize()
    return _quota_manager


async def get_current_tenant_id() -> str:
    """Get current tenant ID from context."""
    # This would normally come from authentication/authorization
    # For now, return a default
    return os.getenv("DEFAULT_TENANT_ID", "default") 