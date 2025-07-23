"""
DataLoader Registry

Manages DataLoaders for efficient batching and caching of data fetches.
"""

from typing import Dict, List, Any, Optional, Callable
from dataclasses import dataclass
import asyncio

from strawberry.dataloader import DataLoader

from data_intelligence_common import StructuredLogger

logger = StructuredLogger.get_logger(__name__)


@dataclass
class LoaderConfig:
    """Configuration for a DataLoader"""
    batch_fn: Callable
    max_batch_size: int = 100
    cache: bool = True
    cache_ttl: int = 300  # seconds


class DataLoaderRegistry:
    """
    Registry for managing DataLoaders across the GraphQL gateway
    """
    
    def __init__(self):
        self.loaders: Dict[str, DataLoader] = {}
        self.loader_configs: Dict[str, LoaderConfig] = {}
        self._initialize_loaders()
    
    def _initialize_loaders(self):
        """Initialize standard data loaders"""
        # Entity loader
        self.register_loader(
            "entity",
            LoaderConfig(
                batch_fn=self._batch_load_entities,
                max_batch_size=100,
                cache=True
            )
        )
        
        # Pipeline loader
        self.register_loader(
            "pipeline",
            LoaderConfig(
                batch_fn=self._batch_load_pipelines,
                max_batch_size=50,
                cache=True
            )
        )
        
        # Model loader
        self.register_loader(
            "model",
            LoaderConfig(
                batch_fn=self._batch_load_models,
                max_batch_size=50,
                cache=True
            )
        )
        
        # User loader
        self.register_loader(
            "user",
            LoaderConfig(
                batch_fn=self._batch_load_users,
                max_batch_size=100,
                cache=True
            )
        )
        
        # Quality profile loader
        self.register_loader(
            "quality_profile",
            LoaderConfig(
                batch_fn=self._batch_load_quality_profiles,
                max_batch_size=20,
                cache=True,
                cache_ttl=60  # Shorter TTL for quality data
            )
        )
    
    def register_loader(self, name: str, config: LoaderConfig):
        """Register a new data loader"""
        loader = DataLoader(load_fn=config.batch_fn)
        self.loaders[name] = loader
        self.loader_configs[name] = config
        logger.info(f"Registered data loader: {name}")
    
    def get_loader(self, name: str) -> Optional[DataLoader]:
        """Get a data loader by name"""
        return self.loaders.get(name)
    
    def clear_cache(self, loader_name: Optional[str] = None):
        """Clear cache for specific loader or all loaders"""
        if loader_name:
            if loader_name in self.loaders:
                self.loaders[loader_name].clear_all()
                logger.info(f"Cleared cache for loader: {loader_name}")
        else:
            for name, loader in self.loaders.items():
                loader.clear_all()
            logger.info("Cleared cache for all loaders")
    
    # Batch loading functions
    async def _batch_load_entities(self, entity_ids: List[str]) -> List[Optional[Dict[str, Any]]]:
        """Batch load catalog entities"""
        logger.debug(f"Batch loading {len(entity_ids)} entities")
        
        # This would make a batch request to the catalog service
        # For now, return placeholder data
        results = []
        for entity_id in entity_ids:
            results.append({
                "id": entity_id,
                "name": f"Entity {entity_id}",
                "type": "dataset",
                "metadata": {}
            })
        
        return results
    
    async def _batch_load_pipelines(self, pipeline_ids: List[str]) -> List[Optional[Dict[str, Any]]]:
        """Batch load pipelines"""
        logger.debug(f"Batch loading {len(pipeline_ids)} pipelines")
        
        # Placeholder implementation
        results = []
        for pipeline_id in pipeline_ids:
            results.append({
                "id": pipeline_id,
                "name": f"Pipeline {pipeline_id}",
                "status": "active",
                "schedule": "0 * * * *"
            })
        
        return results
    
    async def _batch_load_models(self, model_ids: List[str]) -> List[Optional[Dict[str, Any]]]:
        """Batch load ML models"""
        logger.debug(f"Batch loading {len(model_ids)} models")
        
        # Placeholder implementation
        results = []
        for model_id in model_ids:
            results.append({
                "id": model_id,
                "name": f"Model {model_id}",
                "version": "1.0.0",
                "status": "deployed"
            })
        
        return results
    
    async def _batch_load_users(self, user_ids: List[str]) -> List[Optional[Dict[str, Any]]]:
        """Batch load users"""
        logger.debug(f"Batch loading {len(user_ids)} users")
        
        # Placeholder implementation
        results = []
        for user_id in user_ids:
            results.append({
                "id": user_id,
                "username": f"user_{user_id}",
                "email": f"user_{user_id}@example.com",
                "roles": ["data_scientist"]
            })
        
        return results
    
    async def _batch_load_quality_profiles(self, dataset_ids: List[str]) -> List[Optional[Dict[str, Any]]]:
        """Batch load quality profiles"""
        logger.debug(f"Batch loading {len(dataset_ids)} quality profiles")
        
        # Placeholder implementation
        results = []
        for dataset_id in dataset_ids:
            results.append({
                "dataset_id": dataset_id,
                "completeness": 0.98,
                "accuracy": 0.95,
                "consistency": 0.97,
                "last_checked": "2024-01-01T00:00:00Z"
            })
        
        return results
    
    def create_custom_loader(self, batch_fn: Callable, 
                           max_batch_size: int = 100,
                           cache: bool = True) -> DataLoader:
        """Create a custom data loader"""
        return DataLoader(load_fn=batch_fn)
    
    async def prime_cache(self, loader_name: str, data: Dict[str, Any]):
        """Prime the cache with pre-loaded data"""
        loader = self.get_loader(loader_name)
        if loader:
            for key, value in data.items():
                loader.prime(key, value)
            logger.debug(f"Primed cache for {loader_name} with {len(data)} items")
    
    def get_statistics(self) -> Dict[str, Any]:
        """Get loader statistics"""
        stats = {}
        
        for name, loader in self.loaders.items():
            # DataLoader doesn't expose internal stats, so we track basic info
            stats[name] = {
                "name": name,
                "cache_enabled": self.loader_configs[name].cache,
                "max_batch_size": self.loader_configs[name].max_batch_size,
                "cache_ttl": self.loader_configs[name].cache_ttl
            }
        
        return stats 