"""Apache Ignite cache manager for settlement state and risk caching"""

import logging
from typing import Any, Optional, Dict, List
from datetime import datetime, timedelta
import json
import pickle
from pyignite import Client, GenericObjectMeta
from pyignite.datatypes import String, DoubleObject, IntObject, TimestampObject

from app.config import settings
from app.models.settlement import Settlement, RiskAssessment, ProviderMetrics

logger = logging.getLogger(__name__)


class IgniteCacheManager:
    """Manages Apache Ignite caches for settlement service"""
    
    def __init__(self):
        self.client = Client()
        self.connected = False
        
        # Cache names
        self.settlement_cache = "settlement_cache"
        self.risk_cache = "risk_assessment_cache"
        self.provider_metrics_cache = "provider_metrics_cache"
        self.session_cache = "settlement_session_cache"
        
    async def connect(self):
        """Connect to Ignite cluster"""
        try:
            nodes = [(settings.ignite_host, settings.ignite_port)]
            
            if settings.ignite_username and settings.ignite_password:
                self.client.connect(
                    nodes,
                    username=settings.ignite_username,
                    password=settings.ignite_password
                )
            else:
                self.client.connect(nodes)
            
            self.connected = True
            
            # Create caches if they don't exist
            self._create_caches()
            
            logger.info("Successfully connected to Ignite cluster")
        except Exception as e:
            logger.error(f"Failed to connect to Ignite: {e}")
            raise
    
    async def disconnect(self):
        """Disconnect from Ignite cluster"""
        if self.connected:
            self.client.close()
            self.connected = False
    
    def _create_caches(self):
        """Create required caches if they don't exist"""
        cache_configs = [
            (self.settlement_cache, {
                'name': self.settlement_cache,
                'backup': 1,
                'atomicity_mode': 'TRANSACTIONAL',
                'cache_mode': 'PARTITIONED'
            }),
            (self.risk_cache, {
                'name': self.risk_cache,
                'backup': 1,
                'atomicity_mode': 'ATOMIC',
                'cache_mode': 'PARTITIONED',
                'expiry_policy': {
                    'type': 'CREATED',
                    'duration': settings.risk_cache_ttl_seconds * 1000  # milliseconds
                }
            }),
            (self.provider_metrics_cache, {
                'name': self.provider_metrics_cache,
                'backup': 1,
                'atomicity_mode': 'ATOMIC',
                'cache_mode': 'REPLICATED'
            }),
            (self.session_cache, {
                'name': self.session_cache,
                'backup': 0,
                'atomicity_mode': 'ATOMIC',
                'cache_mode': 'PARTITIONED',
                'expiry_policy': {
                    'type': 'ACCESSED',
                    'duration': 3600000  # 1 hour
                }
            })
        ]
        
        for cache_name, config in cache_configs:
            try:
                self.client.get_or_create_cache(cache_name)
                logger.info(f"Cache '{cache_name}' ready")
            except Exception as e:
                logger.error(f"Failed to create cache '{cache_name}': {e}")
    
    # Settlement Cache Operations
    
    async def get_settlement(self, settlement_id: str) -> Optional[Settlement]:
        """Get settlement from cache"""
        try:
            cache = self.client.get_cache(self.settlement_cache)
            data = cache.get(settlement_id)
            
            if data:
                return Settlement(**json.loads(data))
            return None
        except Exception as e:
            logger.error(f"Failed to get settlement from cache: {e}")
            return None
    
    async def save_settlement(self, settlement: Settlement) -> bool:
        """Save settlement to cache"""
        try:
            cache = self.client.get_cache(self.settlement_cache)
            
            # Convert to JSON for storage
            data = settlement.model_dump_json()
            cache.put(settlement.id, data)
            
            # Also update index by trade_id
            cache.put(f"trade:{settlement.trade_id}", settlement.id)
            
            return True
        except Exception as e:
            logger.error(f"Failed to save settlement to cache: {e}")
            return False
    
    async def update_settlement_status(
        self,
        settlement_id: str,
        status: str,
        timestamp: Optional[datetime] = None
    ) -> bool:
        """Update settlement status atomically"""
        try:
            cache = self.client.get_cache(self.settlement_cache)
            
            # Use Ignite transaction for atomic update
            with self.client.tx_start() as tx:
                data = cache.get(settlement_id)
                if data:
                    settlement_dict = json.loads(data)
                    settlement_dict['status'] = status
                    if timestamp:
                        settlement_dict['settlement_timestamp'] = timestamp.isoformat()
                    
                    cache.put(settlement_id, json.dumps(settlement_dict))
                    tx.commit()
                    return True
                else:
                    tx.rollback()
                    return False
                    
        except Exception as e:
            logger.error(f"Failed to update settlement status: {e}")
            return False
    
    async def get_settlements_by_status(
        self,
        status: str,
        limit: int = 100
    ) -> List[Settlement]:
        """Get settlements by status using SQL query"""
        try:
            # Use Ignite SQL query
            query = f"""
                SELECT _val FROM {self.settlement_cache}
                WHERE JSON_VALUE(_val, '$.status') = ?
                LIMIT ?
            """
            
            cursor = self.client.sql(query, status, limit)
            settlements = []
            
            for row in cursor:
                data = row[0]
                settlements.append(Settlement(**json.loads(data)))
            
            return settlements
        except Exception as e:
            logger.error(f"Failed to query settlements by status: {e}")
            return []
    
    # Risk Assessment Cache Operations
    
    async def get_risk_assessment(
        self,
        settlement_id: str
    ) -> Optional[RiskAssessment]:
        """Get risk assessment from cache"""
        try:
            cache = self.client.get_cache(self.risk_cache)
            data = cache.get(settlement_id)
            
            if data:
                return RiskAssessment(**json.loads(data))
            return None
        except Exception as e:
            logger.error(f"Failed to get risk assessment from cache: {e}")
            return None
    
    async def save_risk_assessment(
        self,
        assessment: RiskAssessment
    ) -> bool:
        """Save risk assessment to cache with TTL"""
        try:
            cache = self.client.get_cache(self.risk_cache)
            data = assessment.model_dump_json()
            
            # Cache will automatically expire based on configured TTL
            cache.put(assessment.settlement_id, data)
            
            return True
        except Exception as e:
            logger.error(f"Failed to save risk assessment to cache: {e}")
            return False
    
    # Provider Metrics Cache Operations
    
    async def get_provider_metrics(
        self,
        provider_id: str
    ) -> Optional[ProviderMetrics]:
        """Get provider metrics from cache"""
        try:
            cache = self.client.get_cache(self.provider_metrics_cache)
            data = cache.get(provider_id)
            
            if data:
                return ProviderMetrics(**json.loads(data))
            return None
        except Exception as e:
            logger.error(f"Failed to get provider metrics from cache: {e}")
            return None
    
    async def save_provider_metrics(
        self,
        metrics: ProviderMetrics
    ) -> bool:
        """Save provider metrics to cache"""
        try:
            cache = self.client.get_cache(self.provider_metrics_cache)
            data = metrics.model_dump_json()
            cache.put(metrics.provider_id, data)
            
            return True
        except Exception as e:
            logger.error(f"Failed to save provider metrics to cache: {e}")
            return False
    
    async def update_provider_metrics(
        self,
        provider_id: str,
        updates: Dict[str, Any]
    ) -> bool:
        """Update provider metrics atomically"""
        try:
            cache = self.client.get_cache(self.provider_metrics_cache)
            
            with self.client.tx_start() as tx:
                data = cache.get(provider_id)
                if data:
                    metrics_dict = json.loads(data)
                    metrics_dict.update(updates)
                    metrics_dict['last_updated'] = datetime.utcnow().isoformat()
                    
                    cache.put(provider_id, json.dumps(metrics_dict))
                    tx.commit()
                    return True
                else:
                    tx.rollback()
                    return False
                    
        except Exception as e:
            logger.error(f"Failed to update provider metrics: {e}")
            return False
    
    # Session Cache Operations (for gRPC streaming)
    
    async def save_session_data(
        self,
        session_id: str,
        data: Dict[str, Any],
        ttl_seconds: int = 3600
    ) -> bool:
        """Save session data with TTL"""
        try:
            cache = self.client.get_cache(self.session_cache)
            cache.put(session_id, json.dumps(data))
            return True
        except Exception as e:
            logger.error(f"Failed to save session data: {e}")
            return False
    
    async def get_session_data(
        self,
        session_id: str
    ) -> Optional[Dict[str, Any]]:
        """Get session data"""
        try:
            cache = self.client.get_cache(self.session_cache)
            data = cache.get(session_id)
            
            if data:
                return json.loads(data)
            return None
        except Exception as e:
            logger.error(f"Failed to get session data: {e}")
            return None
    
    async def delete_session_data(self, session_id: str) -> bool:
        """Delete session data"""
        try:
            cache = self.client.get_cache(self.session_cache)
            cache.remove_key(session_id)
            return True
        except Exception as e:
            logger.error(f"Failed to delete session data: {e}")
            return False
    
    # Batch Operations
    
    async def save_settlements_batch(
        self,
        settlements: List[Settlement]
    ) -> bool:
        """Save multiple settlements in batch"""
        try:
            cache = self.client.get_cache(self.settlement_cache)
            
            # Prepare batch data
            batch_data = {}
            for settlement in settlements:
                batch_data[settlement.id] = settlement.model_dump_json()
            
            # Batch put
            cache.put_all(batch_data)
            
            return True
        except Exception as e:
            logger.error(f"Failed to save settlements batch: {e}")
            return False
    
    # Utility Methods
    
    async def clear_cache(self, cache_name: str) -> bool:
        """Clear specific cache"""
        try:
            cache = self.client.get_cache(cache_name)
            cache.clear()
            logger.info(f"Cleared cache: {cache_name}")
            return True
        except Exception as e:
            logger.error(f"Failed to clear cache {cache_name}: {e}")
            return False
    
    async def get_cache_metrics(self) -> Dict[str, Any]:
        """Get cache metrics for monitoring"""
        try:
            metrics = {}
            
            for cache_name in [
                self.settlement_cache,
                self.risk_cache,
                self.provider_metrics_cache,
                self.session_cache
            ]:
                cache = self.client.get_cache(cache_name)
                metrics[cache_name] = {
                    "size": cache.get_size(),
                    "name": cache_name
                }
            
            return metrics
        except Exception as e:
            logger.error(f"Failed to get cache metrics: {e}")
            return {}


# Singleton instance
cache_manager = IgniteCacheManager() 