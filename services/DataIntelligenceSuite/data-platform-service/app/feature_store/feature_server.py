"""
Feature Server

High-performance feature serving for online inference
"""

import logging
from typing import Dict, List, Optional, Any, Tuple
from datetime import datetime, timedelta
import asyncio
from concurrent.futures import ThreadPoolExecutor
import numpy as np

import grpc
from grpc import aio

from .models import FeatureRequest, FeatureResponse, FeatureGroup
from .feature_registry import FeatureRegistry
from .ignite_feature_cache import IgniteFeatureCache

logger = logging.getLogger(__name__)


class FeatureServer:
    """High-performance feature server for online serving"""
    
    def __init__(self,
                 registry: FeatureRegistry,
                 cache_ttl: int = 300,
                 max_batch_size: int = 1000,
                 enable_grpc: bool = False):
        self.registry = registry
        self.cache = IgniteFeatureCache(
            cache_name="feature_store",
            ttl_seconds=cache_ttl,
            enable_sql=True,
            enable_persistence=False
        )
        self.cache_ttl = cache_ttl
        self.max_batch_size = max_batch_size
        self.enable_grpc = enable_grpc
        self._connected = False
        
        # Performance optimizations
        self.feature_cache = {}  # Local in-memory cache
        self.cache_stats = {
            "hits": 0,
            "misses": 0,
            "errors": 0
        }
        
        # Thread pool for parallel feature fetching
        self.executor = ThreadPoolExecutor(max_workers=10)
        
        # gRPC server if enabled
        self.grpc_server = None
        if enable_grpc:
            self._setup_grpc_server()
    
    async def initialize(self) -> None:
        """Initialize feature server and connect to Ignite"""
        if not self._connected:
            await self.cache.connect()
            self._connected = True
            logger.info("Feature server initialized with Ignite cache")
            
    async def get_online_features(self,
                                feature_request: FeatureRequest) -> FeatureResponse:
        """
        Get online features with low latency
        
        Args:
            feature_request: Feature request with entities and features
            
        Returns:
            Feature response with values
        """
        start_time = datetime.utcnow()
        
        try:
            # Parse requested features by group
            features_by_group = self._parse_features_by_group(feature_request.features)
            
            # Fetch features in parallel
            feature_futures = []
            for group_name, features in features_by_group.items():
                future = asyncio.create_task(
                    self._fetch_group_features(
                        group_name=group_name,
                        features=features,
                        entities=feature_request.entities
                    )
                )
                feature_futures.append(future)
                
            # Wait for all features
            results = await asyncio.gather(*feature_futures)
            
            # Combine results
            combined_features = {}
            for result in results:
                combined_features.update(result)
                
            # Apply defaults for missing features
            final_features = await self._apply_defaults(
                features=combined_features,
                requested=feature_request.features
            )
            
            # Calculate latency
            latency_ms = (datetime.utcnow() - start_time).total_seconds() * 1000
            
            # Build response
            response = FeatureResponse(
                features=final_features,
                metadata={
                    "cache_hit_rate": self._calculate_cache_hit_rate(),
                    "features_served": len(final_features),
                    "timestamp": datetime.utcnow()
                } if feature_request.include_metadata else None,
                latency_ms=latency_ms
            )
            
            return response
            
        except Exception as e:
            logger.error(f"Failed to get online features: {e}")
            self.cache_stats["errors"] += 1
            raise
            
    async def get_batch_features(self,
                               entities_list: List[Dict[str, Any]],
                               features: List[str]) -> List[Dict[str, Any]]:
        """
        Get features for multiple entities (batch serving)
        
        Args:
            entities_list: List of entity dictionaries
            features: Features to retrieve
            
        Returns:
            List of feature dictionaries
        """
        # Process in batches to avoid overwhelming the system
        results = []
        
        for i in range(0, len(entities_list), self.max_batch_size):
            batch = entities_list[i:i + self.max_batch_size]
            
            # Get features for batch
            batch_futures = []
            for entities in batch:
                request = FeatureRequest(
                    entities=entities,
                    features=features
                )
                batch_futures.append(
                    asyncio.create_task(self.get_online_features(request))
                )
                
            # Wait for batch completion
            batch_results = await asyncio.gather(*batch_futures)
            
            # Extract feature values
            for response in batch_results:
                results.append(response.features)
                
        return results
        
    async def preload_features(self,
                             feature_groups: List[str],
                             entity_keys: Optional[List[Dict[str, Any]]] = None):
        """
        Preload features into cache for better performance
        
        Args:
            feature_groups: Feature groups to preload
            entity_keys: Optional specific entities to preload
        """
        try:
            for group_name in feature_groups:
                # Get feature group
                group = await self.registry.get_feature_group(group_name)
                if not group:
                    continue
                    
                # Load from offline store if no specific entities
                if not entity_keys:
                    await self._preload_recent_features(group)
                else:
                    # Load specific entities
                    for entities in entity_keys:
                        cache_key = self._build_cache_key(group_name, entities)
                        
                        # Check if already cached
                        if cache_key in self.feature_cache:
                            continue
                            
                        # Fetch and cache
                        features = await self._fetch_features_from_store(
                            group_name, entities
                        )
                        
                        if features:
                            self.feature_cache[cache_key] = {
                                "features": features,
                                "timestamp": datetime.utcnow()
                            }
                            
            logger.info(f"Preloaded features for {len(feature_groups)} groups")
            
        except Exception as e:
            logger.error(f"Failed to preload features: {e}")
            
    async def invalidate_cache(self,
                             feature_group: Optional[str] = None,
                             entities: Optional[Dict[str, Any]] = None):
        """
        Invalidate cached features
        
        Args:
            feature_group: Optional specific group to invalidate
            entities: Optional specific entities to invalidate
        """
        if not feature_group:
            # Clear all cache
            self.feature_cache.clear()
            await self.cache.clear_pattern("features:*")
            logger.info("Cleared all feature cache")
        else:
            if entities:
                # Clear specific entity
                cache_key = self._build_cache_key(feature_group, entities)
                self.feature_cache.pop(cache_key, None)
                await self.cache.delete(cache_key)
            else:
                # Clear all entities for group
                keys_to_remove = [
                    k for k in self.feature_cache.keys() 
                    if k.startswith(f"features:{feature_group}:")
                ]
                for key in keys_to_remove:
                    self.feature_cache.pop(key)
                    
                await self.cache.clear_pattern(f"features:{feature_group}:*")
                
    async def get_serving_stats(self) -> Dict[str, Any]:
        """Get feature serving statistics"""
        total_requests = sum([
            self.cache_stats["hits"],
            self.cache_stats["misses"]
        ])
        
        return {
            "total_requests": total_requests,
            "cache_hit_rate": self._calculate_cache_hit_rate(),
            "cache_hits": self.cache_stats["hits"],
            "cache_misses": self.cache_stats["misses"],
            "errors": self.cache_stats["errors"],
            "cache_size": len(self.feature_cache),
            "status": "healthy" if self.cache_stats["errors"] < 10 else "degraded"
        }
        
    # Helper methods
    
    def _parse_features_by_group(self, features: List[str]) -> Dict[str, List[str]]:
        """Parse features by group name"""
        features_by_group = {}
        
        for feature in features:
            if "." in feature:
                group, feature_name = feature.split(".", 1)
                if group not in features_by_group:
                    features_by_group[group] = []
                features_by_group[group].append(feature_name)
            else:
                # Assume default group
                if "_default" not in features_by_group:
                    features_by_group["_default"] = []
                features_by_group["_default"].append(feature)
                
        return features_by_group
        
    async def _fetch_group_features(self,
                                  group_name: str,
                                  features: List[str],
                                  entities: Dict[str, Any]) -> Dict[str, Any]:
        """Fetch features from a specific group"""
        result = {}
        
        try:
            # Get feature group metadata
            feature_group = await self.registry.get_feature_group(group_name)
            if not feature_group:
                logger.warning(f"Feature group not found: {group_name}")
                return result
            
            # Build feature keys
            feature_keys = [f"{group_name}.{feature}" for feature in features]
            
            # Extract entity IDs
            entity_ids = []
            if feature_group.entity_keys:
                # Build composite entity ID from multiple keys
                entity_values = []
                for key in feature_group.entity_keys:
                    if key in entities:
                        entity_values.append(str(entities[key]))
                if entity_values:
                    entity_ids = ["_".join(entity_values)]
            else:
                # Use first entity value as ID
                entity_ids = [str(list(entities.values())[0])]
            
            # Try cache first
            cached_features = await self.cache.get_features_batch(
                feature_keys=feature_keys,
                entity_ids=entity_ids
            )
            
            # Process cached results
            for (feature_key, entity_id), value in cached_features.items():
                feature_name = feature_key.split(".", 1)[1]
                result[feature_key] = value
                self.cache_stats["hits"] += 1
            
            # Identify missing features
            missing_features = []
            for feature_key in feature_keys:
                if feature_key not in result:
                    missing_features.append(feature_key)
                    self.cache_stats["misses"] += 1
            
            # If features missing, try to fetch from offline store
            if missing_features:
                # This would typically involve querying the data lake
                # For now, we'll use default values
                for feature_key in missing_features:
                    feature_name = feature_key.split(".", 1)[1]
                    # Get feature definition for default value
                    feature_def = next((f for f in feature_group.features if f.name == feature_name), None)
                    if feature_def and feature_def.default_value is not None:
                        result[feature_key] = feature_def.default_value
                        # Cache the default value
                        await self.cache.put_feature(
                            feature_key=feature_key,
                            entity_id=entity_ids[0],
                            value=feature_def.default_value
                        )
            
            return result
            
        except Exception as e:
            self.cache_stats["errors"] += 1
            logger.error(f"Error fetching features from group {group_name}: {e}")
            return result
        
    async def _fetch_features_from_store(self,
                                       group_name: str,
                                       entities: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """Fetch features from underlying store"""
        # This would connect to the actual feature store
        # For now, return mock data
        return {
            f"{feature}": np.random.random()
            for feature in ["feature1", "feature2", "feature3"]
        }
        
    async def _apply_defaults(self,
                            features: Dict[str, Any],
                            requested: List[str]) -> Dict[str, Any]:
        """Apply default values for missing features"""
        result = features.copy()
        
        for feature_name in requested:
            if feature_name not in result:
                # Get default from registry
                default_value = await self._get_feature_default(feature_name)
                result[feature_name] = default_value
                
        return result
        
    async def _get_feature_default(self, feature_name: str) -> Any:
        """Get default value for a feature"""
        # This would look up the feature definition
        # For now, return 0
        return 0
        
    def _build_cache_key(self, group_name: str, entities: Dict[str, Any]) -> str:
        """Build cache key from group and entities"""
        entity_str = "_".join([f"{k}:{v}" for k, v in sorted(entities.items())])
        return f"features:{group_name}:{entity_str}"
        
    def _is_cache_valid(self, cached: Dict[str, Any]) -> bool:
        """Check if cached data is still valid"""
        if "timestamp" not in cached:
            return False
            
        age = datetime.utcnow() - cached["timestamp"]
        return age.total_seconds() < self.cache_ttl
        
    def _extract_features(self,
                         features_data: Dict[str, Any],
                         requested: List[str]) -> Dict[str, Any]:
        """Extract requested features from data"""
        return {
            feature: features_data.get(feature)
            for feature in requested
            if feature in features_data
        }
        
    def _calculate_cache_hit_rate(self) -> float:
        """Calculate cache hit rate"""
        total = self.cache_stats["hits"] + self.cache_stats["misses"]
        if total == 0:
            return 0.0
        return self.cache_stats["hits"] / total
        
    async def _preload_recent_features(self, group: FeatureGroup):
        """Preload recent features for a group"""
        # This would load recent/popular features
        # Implementation depends on storage backend
        pass
        
    def _setup_grpc_server(self):
        """Set up gRPC server for high-performance serving"""
        # This would set up a gRPC server
        # Skipping for now as it requires proto definitions
        pass
        
    async def close(self):
        """Cleanup resources"""
        self.executor.shutdown(wait=True)
        if self.grpc_server:
            await self.grpc_server.stop(0) 