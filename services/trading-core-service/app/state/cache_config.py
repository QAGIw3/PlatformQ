"""Cache configuration for Apache Ignite."""

from enum import Enum
from typing import Dict, Any, Optional
from dataclasses import dataclass


class CacheType(str, Enum):
    """Cache types."""
    ORDERBOOK = "orderbook"
    ORDER = "order"
    POSITION = "position"
    TRADE = "trade"
    MARKET = "market"
    USER_STATE = "user_state"
    RISK_STATE = "risk_state"


@dataclass
class CacheConfig:
    """Configuration for an Ignite cache."""
    name: str
    cache_type: CacheType
    
    # Cache mode
    cache_mode: str = "PARTITIONED"  # PARTITIONED, REPLICATED, LOCAL
    atomicity_mode: str = "TRANSACTIONAL"  # ATOMIC, TRANSACTIONAL
    
    # Partitioning
    backups: int = 1
    affinity_key: Optional[str] = None
    
    # Memory configuration
    on_heap_enabled: bool = True
    eviction_enabled: bool = False
    max_memory_size: Optional[int] = None  # bytes
    
    # Persistence
    persistence_enabled: bool = True
    wal_mode: str = "LOG_ONLY"  # FSYNC, LOG_ONLY, BACKGROUND, NONE
    
    # Expiry policies
    default_ttl: Optional[int] = None  # seconds
    
    # Query configuration
    sql_index_enabled: bool = True
    query_entities: Optional[Dict[str, Any]] = None
    
    # Near cache (client-side caching)
    near_cache_enabled: bool = False
    near_cache_size: int = 1000
    
    def to_ignite_config(self) -> Dict[str, Any]:
        """Convert to Ignite cache configuration."""
        config = {
            "name": self.name,
            "cacheMode": self.cache_mode,
            "atomicityMode": self.atomicity_mode,
            "backups": self.backups,
            "onheapCacheEnabled": self.on_heap_enabled,
            "evictionPolicy": {
                "maxSize": self.max_memory_size
            } if self.eviction_enabled and self.max_memory_size else None,
            "writeSynchronizationMode": "FULL_SYNC",
            "partitionLossPolicy": "READ_WRITE_SAFE"
        }
        
        if self.affinity_key:
            config["affinity"] = {
                "affinityBackupFilter": self.affinity_key
            }
        
        if self.default_ttl:
            config["expiryPolicyFactory"] = {
                "expiryDuration": {
                    "timeUnit": "SECONDS",
                    "value": self.default_ttl
                }
            }
        
        if self.sql_index_enabled and self.query_entities:
            config["queryEntities"] = self.query_entities
        
        return config


# Predefined cache configurations
CACHE_CONFIGS = {
    CacheType.ORDERBOOK: CacheConfig(
        name="orderbook_cache",
        cache_type=CacheType.ORDERBOOK,
        cache_mode="REPLICATED",
        atomicity_mode="ATOMIC",
        persistence_enabled=False,
        near_cache_enabled=True,
        near_cache_size=5000
    ),
    
    CacheType.ORDER: CacheConfig(
        name="order_cache",
        cache_type=CacheType.ORDER,
        cache_mode="PARTITIONED",
        atomicity_mode="TRANSACTIONAL",
        backups=2,
        affinity_key="user_id",
        persistence_enabled=True,
        default_ttl=86400  # 24 hours
    ),
    
    CacheType.POSITION: CacheConfig(
        name="position_cache",
        cache_type=CacheType.POSITION,
        cache_mode="PARTITIONED",
        atomicity_mode="TRANSACTIONAL",
        backups=2,
        affinity_key="user_id",
        persistence_enabled=True,
        near_cache_enabled=True
    ),
    
    CacheType.TRADE: CacheConfig(
        name="trade_cache",
        cache_type=CacheType.TRADE,
        cache_mode="PARTITIONED",
        atomicity_mode="ATOMIC",
        backups=1,
        persistence_enabled=True,
        default_ttl=604800  # 7 days
    ),
    
    CacheType.MARKET: CacheConfig(
        name="market_cache",
        cache_type=CacheType.MARKET,
        cache_mode="REPLICATED",
        atomicity_mode="ATOMIC",
        persistence_enabled=True,
        near_cache_enabled=True,
        near_cache_size=1000
    ),
    
    CacheType.USER_STATE: CacheConfig(
        name="user_state_cache",
        cache_type=CacheType.USER_STATE,
        cache_mode="PARTITIONED",
        atomicity_mode="TRANSACTIONAL",
        backups=2,
        affinity_key="user_id",
        persistence_enabled=True
    ),
    
    CacheType.RISK_STATE: CacheConfig(
        name="risk_state_cache",
        cache_type=CacheType.RISK_STATE,
        cache_mode="PARTITIONED",
        atomicity_mode="ATOMIC",
        backups=1,
        affinity_key="user_id",
        persistence_enabled=False,
        default_ttl=300  # 5 minutes
    )
} 