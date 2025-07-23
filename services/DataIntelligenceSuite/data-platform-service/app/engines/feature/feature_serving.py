"""
Feature Serving for online and batch feature retrieval.
"""

import asyncio
from typing import Dict, List, Optional, Any, Union, Tuple
from datetime import datetime, timedelta
from dataclasses import dataclass, field
from enum import Enum
import json
import pandas as pd
import numpy as np
from collections import defaultdict
import aiohttp

from data_intelligence_common.core.events import EventBus
from data_intelligence_common.core.caching import CacheManager

from platformq_shared.logging_config import get_logger

logger = get_logger(__name__)


class ServingMode(str, Enum):
    """Feature serving modes."""
    ONLINE = "online"  # Low-latency, single entity
    BATCH = "batch"    # High-throughput, multiple entities
    STREAM = "stream"  # Real-time streaming


@dataclass
class FeatureVector:
    """Represents a feature vector."""
    entity_id: str
    features: Dict[str, Any]
    timestamp: datetime = field(default_factory=datetime.utcnow)
    metadata: Dict[str, Any] = field(default_factory=dict)
    
    def to_array(self, feature_names: List[str]) -> np.ndarray:
        """Convert to numpy array."""
        values = []
        for name in feature_names:
            value = self.features.get(name, 0)
            if isinstance(value, (list, np.ndarray)):
                values.extend(value)
            else:
                values.append(value)
        return np.array(values)


@dataclass
class BatchRequest:
    """Batch feature request."""
    entity_ids: List[str]
    feature_names: List[str]
    feature_view: Optional[str] = None
    filters: Optional[Dict[str, Any]] = None
    point_in_time: Optional[datetime] = None
    metadata: Dict[str, Any] = field(default_factory=dict)


@dataclass
class StreamRequest:
    """Stream feature request."""
    entity_pattern: str  # Pattern to match entities
    feature_names: List[str]
    window_size: int = 100  # Number of events to buffer
    timeout: int = 30  # Seconds to wait for events
    metadata: Dict[str, Any] = field(default_factory=dict)


class FeatureServer:
    """
    High-performance feature serving layer.
    """
    
    def __init__(
        self,
        event_bus: EventBus,
        cache_manager: CacheManager,
        feature_store: Any  # Avoid circular import
    ):
        self.event_bus = event_bus
        self.cache_manager = cache_manager
        self.feature_store = feature_store
        
        # Serving caches
        self.hot_cache: Dict[str, FeatureVector] = {}  # In-memory cache
        self.request_cache: Dict[str, Any] = {}  # Request result cache
        
        # Serving statistics
        self.serving_stats = defaultdict(lambda: defaultdict(int))
        
        # Stream handlers
        self.stream_handlers: Dict[str, asyncio.Task] = {}
        
        # Background tasks
        self._cache_refresh_task: Optional[asyncio.Task] = None
        self._stats_report_task: Optional[asyncio.Task] = None
        
        logger.info("Feature Server initialized")
        
    async def initialize(self):
        """Initialize feature server."""
        # Subscribe to events
        await self.event_bus.subscribe("feature.values.written", self._handle_feature_update)
        await self.event_bus.subscribe("serving.request", self._handle_serving_request)
        
        # Start background tasks
        self._cache_refresh_task = asyncio.create_task(self._refresh_hot_cache())
        self._stats_report_task = asyncio.create_task(self._report_statistics())
        
        logger.info("Feature Server ready")
        
    async def cleanup(self):
        """Cleanup feature server resources."""
        # Cancel background tasks
        if self._cache_refresh_task:
            self._cache_refresh_task.cancel()
        if self._stats_report_task:
            self._stats_report_task.cancel()
        
        # Cancel stream handlers
        for handler in self.stream_handlers.values():
            handler.cancel()
        
        logger.info("Feature Server cleaned up")
        
    async def get_online_features(
        self,
        entity_id: str,
        feature_names: List[str],
        use_cache: bool = True
    ) -> Optional[FeatureVector]:
        """Get features for a single entity with low latency."""
        start_time = datetime.utcnow()
        
        # Check hot cache first
        if use_cache and entity_id in self.hot_cache:
            vector = self.hot_cache[entity_id]
            # Check if all requested features are present
            if all(name in vector.features for name in feature_names):
                self._update_stats("online", "cache_hit")
                return vector
        
        # Get from feature store
        try:
            df = await self.feature_store.get_online_features(
                [entity_id],
                feature_names
            )
            
            if df.empty:
                self._update_stats("online", "not_found")
                return None
            
            # Convert to FeatureVector
            row = df.iloc[0]
            features = {
                name: row[name]
                for name in feature_names
                if name in row
            }
            
            vector = FeatureVector(
                entity_id=entity_id,
                features=features
            )
            
            # Update hot cache
            if use_cache:
                self.hot_cache[entity_id] = vector
            
            # Update statistics
            latency = (datetime.utcnow() - start_time).total_seconds() * 1000
            self._update_stats("online", "success", latency=latency)
            
            return vector
            
        except Exception as e:
            logger.error(f"Error getting online features: {e}")
            self._update_stats("online", "error")
            return None
            
    async def get_batch_features(
        self,
        request: BatchRequest
    ) -> pd.DataFrame:
        """Get features for multiple entities."""
        start_time = datetime.utcnow()
        
        # Check request cache
        cache_key = self._get_request_cache_key(request)
        if cache_key in self.request_cache:
            cached = self.request_cache[cache_key]
            if (datetime.utcnow() - cached["timestamp"]).seconds < 300:  # 5 min cache
                self._update_stats("batch", "cache_hit")
                return cached["data"]
        
        try:
            # Get features based on request type
            if request.feature_view:
                df = await self.feature_store.get_batch_features(
                    request.entity_ids,
                    request.feature_view
                )
            else:
                df = await self.feature_store.get_online_features(
                    request.entity_ids,
                    request.feature_names
                )
            
            # Apply filters if specified
            if request.filters:
                for column, value in request.filters.items():
                    if column in df.columns:
                        df = df[df[column] == value]
            
            # Apply point-in-time correction if specified
            if request.point_in_time:
                # This would filter features based on their timestamps
                pass
            
            # Cache result
            self.request_cache[cache_key] = {
                "data": df,
                "timestamp": datetime.utcnow()
            }
            
            # Update statistics
            latency = (datetime.utcnow() - start_time).total_seconds() * 1000
            self._update_stats("batch", "success", 
                             latency=latency,
                             entities=len(request.entity_ids),
                             features=len(request.feature_names))
            
            return df
            
        except Exception as e:
            logger.error(f"Error getting batch features: {e}")
            self._update_stats("batch", "error")
            return pd.DataFrame()
            
    async def stream_features(
        self,
        request: StreamRequest,
        callback: Any
    ) -> str:
        """Stream features in real-time."""
        stream_id = f"stream_{len(self.stream_handlers)}"
        
        # Create stream handler
        handler = asyncio.create_task(
            self._handle_stream(stream_id, request, callback)
        )
        self.stream_handlers[stream_id] = handler
        
        logger.info(f"Started feature stream: {stream_id}")
        return stream_id
        
    async def stop_stream(self, stream_id: str):
        """Stop a feature stream."""
        if stream_id in self.stream_handlers:
            self.stream_handlers[stream_id].cancel()
            del self.stream_handlers[stream_id]
            logger.info(f"Stopped feature stream: {stream_id}")
            
    async def get_feature_statistics(
        self,
        feature_names: List[str]
    ) -> Dict[str, Any]:
        """Get serving statistics for features."""
        stats = {}
        
        for feature_name in feature_names:
            # Get from feature store
            feature_stats = self.feature_store.feature_stats.get(feature_name, {})
            
            # Add serving stats
            serving_stats = {
                "online_requests": self.serving_stats[feature_name]["online_requests"],
                "batch_requests": self.serving_stats[feature_name]["batch_requests"],
                "avg_latency_ms": self.serving_stats[feature_name].get("avg_latency", 0),
                "cache_hit_rate": self._calculate_cache_hit_rate(feature_name)
            }
            
            stats[feature_name] = {
                **feature_stats,
                **serving_stats
            }
        
        return stats
        
    async def preload_features(
        self,
        entity_ids: List[str],
        feature_names: List[str]
    ):
        """Preload features into hot cache."""
        logger.info(f"Preloading {len(entity_ids)} entities with {len(feature_names)} features")
        
        # Get features in batches
        batch_size = 1000
        for i in range(0, len(entity_ids), batch_size):
            batch_ids = entity_ids[i:i + batch_size]
            
            df = await self.feature_store.get_online_features(
                batch_ids,
                feature_names
            )
            
            # Load into hot cache
            for _, row in df.iterrows():
                entity_id = row["entity_id"]
                features = {
                    name: row[name]
                    for name in feature_names
                    if name in row
                }
                
                self.hot_cache[entity_id] = FeatureVector(
                    entity_id=entity_id,
                    features=features
                )
        
        logger.info(f"Preloaded {len(entity_ids)} entities into hot cache")
        
    async def export_features(
        self,
        entity_ids: List[str],
        feature_names: List[str],
        format: str = "parquet",
        path: Optional[str] = None
    ) -> Union[str, bytes]:
        """Export features to file or bytes."""
        # Get features
        df = await self.feature_store.get_online_features(
            entity_ids,
            feature_names
        )
        
        # Export based on format
        if format == "parquet":
            if path:
                df.to_parquet(path)
                return path
            else:
                return df.to_parquet()
        elif format == "csv":
            if path:
                df.to_csv(path, index=False)
                return path
            else:
                return df.to_csv(index=False).encode()
        elif format == "json":
            if path:
                df.to_json(path, orient="records")
                return path
            else:
                return df.to_json(orient="records")
        else:
            raise ValueError(f"Unsupported format: {format}")
            
    def _get_request_cache_key(self, request: BatchRequest) -> str:
        """Generate cache key for request."""
        key_parts = [
            ",".join(sorted(request.entity_ids[:10])),  # First 10 entities
            ",".join(sorted(request.feature_names)),
            request.feature_view or "",
            json.dumps(request.filters or {}, sort_keys=True)
        ]
        return ":".join(key_parts)
        
    def _update_stats(
        self,
        mode: str,
        status: str,
        latency: Optional[float] = None,
        **kwargs
    ):
        """Update serving statistics."""
        self.serving_stats[mode][f"{status}_count"] += 1
        
        if latency:
            # Update average latency
            current_avg = self.serving_stats[mode].get("avg_latency", 0)
            count = self.serving_stats[mode].get("total_count", 0)
            new_avg = (current_avg * count + latency) / (count + 1)
            self.serving_stats[mode]["avg_latency"] = new_avg
            self.serving_stats[mode]["total_count"] = count + 1
        
        # Update additional metrics
        for key, value in kwargs.items():
            self.serving_stats[mode][key] = value
            
    def _calculate_cache_hit_rate(self, feature_name: str) -> float:
        """Calculate cache hit rate for a feature."""
        hits = self.serving_stats[feature_name].get("cache_hits", 0)
        total = self.serving_stats[feature_name].get("online_requests", 0)
        
        if total == 0:
            return 0.0
        
        return hits / total
        
    async def _handle_stream(
        self,
        stream_id: str,
        request: StreamRequest,
        callback: Any
    ):
        """Handle feature streaming."""
        buffer = []
        
        try:
            while True:
                # Subscribe to feature updates matching pattern
                async def handle_update(event_data):
                    entity_id = event_data.get("entity_id")
                    if entity_id and entity_id.startswith(request.entity_pattern):
                        # Get features
                        vector = await self.get_online_features(
                            entity_id,
                            request.feature_names
                        )
                        
                        if vector:
                            buffer.append(vector)
                            
                            # Send when buffer is full
                            if len(buffer) >= request.window_size:
                                await callback(buffer.copy())
                                buffer.clear()
                
                # Subscribe to updates
                await self.event_bus.subscribe(
                    "feature.values.written",
                    handle_update
                )
                
                # Send buffer periodically
                await asyncio.sleep(request.timeout)
                if buffer:
                    await callback(buffer.copy())
                    buffer.clear()
                    
        except asyncio.CancelledError:
            logger.info(f"Stream {stream_id} cancelled")
        except Exception as e:
            logger.error(f"Error in stream {stream_id}: {e}")
            
    async def _refresh_hot_cache(self):
        """Refresh hot cache periodically."""
        while True:
            try:
                # Remove old entries
                now = datetime.utcnow()
                to_remove = []
                
                for entity_id, vector in self.hot_cache.items():
                    age = (now - vector.timestamp).seconds
                    if age > 3600:  # 1 hour
                        to_remove.append(entity_id)
                
                for entity_id in to_remove:
                    del self.hot_cache[entity_id]
                
                if to_remove:
                    logger.info(f"Removed {len(to_remove)} stale entries from hot cache")
                
                # Sleep for 5 minutes
                await asyncio.sleep(300)
                
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Error refreshing hot cache: {e}")
                await asyncio.sleep(300)
                
    async def _report_statistics(self):
        """Report serving statistics periodically."""
        while True:
            try:
                # Aggregate statistics
                stats = {
                    "online": dict(self.serving_stats["online"]),
                    "batch": dict(self.serving_stats["batch"]),
                    "hot_cache_size": len(self.hot_cache),
                    "request_cache_size": len(self.request_cache),
                    "active_streams": len(self.stream_handlers)
                }
                
                # Publish metrics
                await self.event_bus.publish("serving.metrics", stats)
                
                # Sleep for 1 minute
                await asyncio.sleep(60)
                
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Error reporting statistics: {e}")
                await asyncio.sleep(60)
                
    async def _handle_feature_update(self, event_data: Dict[str, Any]):
        """Handle feature update events."""
        try:
            # Invalidate relevant caches
            features = event_data.get("features", [])
            
            # Clear request cache entries containing these features
            to_remove = []
            for key, cached in self.request_cache.items():
                if any(f in key for f in features):
                    to_remove.append(key)
            
            for key in to_remove:
                del self.request_cache[key]
                
        except Exception as e:
            logger.error(f"Error handling feature update: {e}")
            
    async def _handle_serving_request(self, event_data: Dict[str, Any]):
        """Handle serving request event."""
        try:
            request_type = event_data.get("type")
            
            if request_type == "online":
                entity_id = event_data.get("entity_id")
                features = event_data.get("features", [])
                
                vector = await self.get_online_features(entity_id, features)
                
                # Publish response
                await self.event_bus.publish("serving.response", {
                    "request_id": event_data.get("request_id"),
                    "vector": vector.to_array(features) if vector else None
                })
                
            elif request_type == "batch":
                request = BatchRequest(
                    entity_ids=event_data.get("entity_ids", []),
                    feature_names=event_data.get("features", []),
                    feature_view=event_data.get("feature_view")
                )
                
                df = await self.get_batch_features(request)
                
                # Publish response
                await self.event_bus.publish("serving.response", {
                    "request_id": event_data.get("request_id"),
                    "data": df.to_dict("records")
                })
                
        except Exception as e:
            logger.error(f"Error handling serving request: {e}")
            
    def get_statistics(self) -> Dict[str, Any]:
        """Get server statistics."""
        return {
            "serving_modes": {
                "online": dict(self.serving_stats["online"]),
                "batch": dict(self.serving_stats["batch"]),
                "stream": {
                    "active_streams": len(self.stream_handlers)
                }
            },
            "cache": {
                "hot_cache_size": len(self.hot_cache),
                "request_cache_size": len(self.request_cache)
            }
        } 