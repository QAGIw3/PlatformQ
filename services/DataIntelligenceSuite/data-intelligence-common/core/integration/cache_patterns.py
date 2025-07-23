"""
Cache patterns for data integration.

Provides cache warming, optimization, and management patterns.
"""

from typing import Dict, List, Any, Optional, Callable, Set, Tuple
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from enum import Enum
from abc import ABC, abstractmethod
import asyncio
import statistics

from ...monitoring import StructuredLogger, MetricsCollector
from ..events import EventBus, Event

logger = StructuredLogger.get_logger(__name__)


class WarmingStrategy(str, Enum):
    """Cache warming strategies"""
    FULL = "full"                    # Load all data
    INCREMENTAL = "incremental"      # Load only changes
    POPULAR = "popular"              # Load frequently accessed
    PREDICTIVE = "predictive"        # Load based on predictions
    SCHEDULED = "scheduled"          # Load at specific times


class OptimizationGoal(str, Enum):
    """Cache optimization goals"""
    HIT_RATE = "hit_rate"           # Maximize cache hits
    MEMORY = "memory"               # Minimize memory usage
    LATENCY = "latency"             # Minimize access latency
    COST = "cost"                   # Minimize operational cost
    BALANCED = "balanced"           # Balance all factors


@dataclass
class CacheStatistics:
    """Detailed cache statistics"""
    hits: int = 0
    misses: int = 0
    evictions: int = 0
    
    # Size metrics
    entry_count: int = 0
    memory_bytes: int = 0
    
    # Performance metrics
    avg_hit_latency_ms: float = 0.0
    avg_miss_latency_ms: float = 0.0
    
    # Access patterns
    access_frequency: Dict[str, int] = field(default_factory=dict)
    last_access_time: Dict[str, datetime] = field(default_factory=dict)
    
    # Time-based metrics
    hourly_hits: List[int] = field(default_factory=lambda: [0] * 24)
    hourly_misses: List[int] = field(default_factory=lambda: [0] * 24)
    
    @property
    def hit_rate(self) -> float:
        """Calculate hit rate"""
        total = self.hits + self.misses
        return self.hits / total if total > 0 else 0.0
        
    @property
    def miss_rate(self) -> float:
        """Calculate miss rate"""
        return 1.0 - self.hit_rate
        
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary"""
        return {
            "hits": self.hits,
            "misses": self.misses,
            "evictions": self.evictions,
            "hit_rate": self.hit_rate,
            "miss_rate": self.miss_rate,
            "entry_count": self.entry_count,
            "memory_bytes": self.memory_bytes,
            "avg_hit_latency_ms": self.avg_hit_latency_ms,
            "avg_miss_latency_ms": self.avg_miss_latency_ms,
            "hourly_hits": self.hourly_hits,
            "hourly_misses": self.hourly_misses
        }


@dataclass
class WarmingTask:
    """Cache warming task configuration"""
    task_id: str
    region_name: str
    data_source: str
    query: str
    strategy: WarmingStrategy = WarmingStrategy.FULL
    
    # Scheduling
    schedule: Optional[str] = None  # Cron expression
    interval_seconds: Optional[int] = None
    
    # Filters
    key_pattern: Optional[str] = None
    priority_keys: List[str] = field(default_factory=list)
    
    # Limits
    max_entries: Optional[int] = None
    max_duration_seconds: Optional[int] = None
    
    # State
    last_run: Optional[datetime] = None
    next_run: Optional[datetime] = None
    is_active: bool = True


@dataclass
class OptimizationRecommendation:
    """Cache optimization recommendation"""
    recommendation_type: str
    description: str
    expected_improvement: Dict[str, float]
    priority: int  # 1-10, higher is more important
    
    # Specific parameters
    parameters: Dict[str, Any] = field(default_factory=dict)
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary"""
        return {
            "type": self.recommendation_type,
            "description": self.description,
            "expected_improvement": self.expected_improvement,
            "priority": self.priority,
            "parameters": self.parameters
        }


class CacheWarmer:
    """
    Cache warming manager.
    
    Features:
    - Multiple warming strategies
    - Scheduled warming
    - Priority-based loading
    - Progress tracking
    """
    
    def __init__(
        self,
        event_bus: Optional[EventBus] = None,
        metrics_collector: Optional[MetricsCollector] = None
    ):
        self.event_bus = event_bus
        self.metrics = metrics_collector
        
        # Tasks
        self._warming_tasks: Dict[str, WarmingTask] = {}
        self._active_tasks: Dict[str, asyncio.Task] = {}
        
        # Statistics
        self._warming_stats: Dict[str, Dict[str, Any]] = {}
        
    async def create_warming_task(self, task: WarmingTask) -> str:
        """
        Create a new warming task.
        
        Args:
            task: Warming task configuration
            
        Returns:
            Task ID
        """
        logger.info(f"Creating warming task: {task.task_id}")
        
        # Store task
        self._warming_tasks[task.task_id] = task
        
        # Start task if scheduled
        if task.schedule or task.interval_seconds:
            asyncio.create_task(self._schedule_task(task))
            
        # Publish event
        if self.event_bus:
            await self.event_bus.publish(Event(
                type="cache.warming.task.created",
                source="cache_warmer",
                data={
                    "task_id": task.task_id,
                    "region": task.region_name,
                    "strategy": task.strategy.value
                }
            ))
            
        return task.task_id
        
    async def warm_cache(
        self,
        region_name: str,
        data_loader: Callable,
        strategy: WarmingStrategy = WarmingStrategy.FULL,
        **kwargs
    ) -> Dict[str, Any]:
        """
        Warm cache with data.
        
        Args:
            region_name: Cache region to warm
            data_loader: Function to load data
            strategy: Warming strategy
            **kwargs: Strategy-specific parameters
            
        Returns:
            Warming results
        """
        start_time = datetime.utcnow()
        loaded_count = 0
        error_count = 0
        
        logger.info(f"Starting cache warming for {region_name} with strategy {strategy.value}")
        
        try:
            if strategy == WarmingStrategy.FULL:
                loaded_count = await self._warm_full(region_name, data_loader, **kwargs)
                
            elif strategy == WarmingStrategy.INCREMENTAL:
                loaded_count = await self._warm_incremental(region_name, data_loader, **kwargs)
                
            elif strategy == WarmingStrategy.POPULAR:
                loaded_count = await self._warm_popular(region_name, data_loader, **kwargs)
                
            elif strategy == WarmingStrategy.PREDICTIVE:
                loaded_count = await self._warm_predictive(region_name, data_loader, **kwargs)
                
            else:
                raise ValueError(f"Unknown warming strategy: {strategy}")
                
        except Exception as e:
            logger.error(f"Error warming cache: {e}")
            error_count += 1
            
        duration = (datetime.utcnow() - start_time).total_seconds()
        
        # Record stats
        stats = {
            "loaded_count": loaded_count,
            "error_count": error_count,
            "duration_seconds": duration,
            "throughput": loaded_count / duration if duration > 0 else 0
        }
        
        self._warming_stats[region_name] = stats
        
        # Record metrics
        if self.metrics:
            self.metrics.increment("cache_warming_total", {"region": region_name, "strategy": strategy.value})
            self.metrics.observe("cache_warming_duration", duration, {"region": region_name})
            self.metrics.observe("cache_warming_entries", loaded_count, {"region": region_name})
            
        # Publish event
        if self.event_bus:
            await self.event_bus.publish(Event(
                type="cache.warming.completed",
                source="cache_warmer",
                data={
                    "region": region_name,
                    "strategy": strategy.value,
                    "stats": stats
                }
            ))
            
        return stats
        
    async def _warm_full(
        self,
        region_name: str,
        data_loader: Callable,
        batch_size: int = 1000,
        **kwargs
    ) -> int:
        """Full cache warming"""
        loaded_count = 0
        offset = 0
        
        while True:
            # Load batch
            batch = await data_loader(
                offset=offset,
                limit=batch_size,
                **kwargs
            )
            
            if not batch:
                break
                
            loaded_count += len(batch)
            offset += batch_size
            
            # Progress update
            if loaded_count % 10000 == 0:
                logger.debug(f"Loaded {loaded_count} entries into {region_name}")
                
        return loaded_count
        
    async def _warm_incremental(
        self,
        region_name: str,
        data_loader: Callable,
        last_update: Optional[datetime] = None,
        **kwargs
    ) -> int:
        """Incremental cache warming"""
        # Load only changed data since last update
        if not last_update:
            last_update = datetime.utcnow() - timedelta(hours=1)
            
        data = await data_loader(
            since=last_update,
            **kwargs
        )
        
        return len(data) if data else 0
        
    async def _warm_popular(
        self,
        region_name: str,
        data_loader: Callable,
        top_n: int = 1000,
        **kwargs
    ) -> int:
        """Popular items cache warming"""
        # Get access statistics
        stats = self._warming_stats.get(region_name, {})
        access_frequency = stats.get("access_frequency", {})
        
        # Sort by frequency
        popular_keys = sorted(
            access_frequency.items(),
            key=lambda x: x[1],
            reverse=True
        )[:top_n]
        
        # Load popular items
        if popular_keys:
            keys = [k for k, _ in popular_keys]
            data = await data_loader(keys=keys, **kwargs)
            return len(data) if data else 0
            
        return 0
        
    async def _warm_predictive(
        self,
        region_name: str,
        data_loader: Callable,
        **kwargs
    ) -> int:
        """Predictive cache warming"""
        # This would use ML models to predict what will be accessed
        # For now, use time-based patterns
        
        current_hour = datetime.utcnow().hour
        stats = self._warming_stats.get(region_name, {})
        
        # Find peak hours
        hourly_access = stats.get("hourly_hits", [0] * 24)
        avg_access = statistics.mean(hourly_access) if hourly_access else 0
        
        # If current hour is approaching peak, warm more aggressively
        next_hour = (current_hour + 1) % 24
        if hourly_access[next_hour] > avg_access * 1.5:
            # Load more data for peak hour
            return await self._warm_popular(
                region_name,
                data_loader,
                top_n=2000,
                **kwargs
            )
            
        return 0
        
    async def _schedule_task(self, task: WarmingTask):
        """Schedule warming task"""
        while task.is_active:
            try:
                # Wait for next run
                if task.interval_seconds:
                    await asyncio.sleep(task.interval_seconds)
                else:
                    # Calculate next run from cron expression
                    # Placeholder - would use croniter or similar
                    await asyncio.sleep(3600)  # Default 1 hour
                    
                # Execute warming
                if task.task_id in self._warming_tasks:
                    await self._execute_task(task)
                    
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Error in scheduled warming task: {e}")
                await asyncio.sleep(60)  # Back off on error


class CacheOptimizer:
    """
    Cache optimization analyzer.
    
    Features:
    - Performance analysis
    - Configuration recommendations
    - Eviction policy tuning
    - Size optimization
    """
    
    def __init__(
        self,
        event_bus: Optional[EventBus] = None,
        metrics_collector: Optional[MetricsCollector] = None
    ):
        self.event_bus = event_bus
        self.metrics = metrics_collector
        
        # Analysis history
        self._analysis_history: Dict[str, List[Dict[str, Any]]] = {}
        
    async def analyze_cache(
        self,
        region_name: str,
        stats: CacheStatistics,
        config: Dict[str, Any],
        goal: OptimizationGoal = OptimizationGoal.BALANCED
    ) -> List[OptimizationRecommendation]:
        """
        Analyze cache and provide recommendations.
        
        Args:
            region_name: Cache region name
            stats: Current cache statistics
            config: Current cache configuration
            goal: Optimization goal
            
        Returns:
            List of recommendations
        """
        logger.info(f"Analyzing cache {region_name} for {goal.value} optimization")
        
        recommendations = []
        
        # Analyze based on goal
        if goal == OptimizationGoal.HIT_RATE:
            recommendations.extend(await self._optimize_hit_rate(stats, config))
            
        elif goal == OptimizationGoal.MEMORY:
            recommendations.extend(await self._optimize_memory(stats, config))
            
        elif goal == OptimizationGoal.LATENCY:
            recommendations.extend(await self._optimize_latency(stats, config))
            
        elif goal == OptimizationGoal.BALANCED:
            # Run all optimizations
            recommendations.extend(await self._optimize_hit_rate(stats, config))
            recommendations.extend(await self._optimize_memory(stats, config))
            recommendations.extend(await self._optimize_latency(stats, config))
            
        # Sort by priority
        recommendations.sort(key=lambda x: x.priority, reverse=True)
        
        # Store analysis
        analysis = {
            "timestamp": datetime.utcnow().isoformat(),
            "stats": stats.to_dict(),
            "recommendations": [r.to_dict() for r in recommendations]
        }
        
        if region_name not in self._analysis_history:
            self._analysis_history[region_name] = []
        self._analysis_history[region_name].append(analysis)
        
        # Publish event
        if self.event_bus:
            await self.event_bus.publish(Event(
                type="cache.optimization.analyzed",
                source="cache_optimizer",
                data={
                    "region": region_name,
                    "goal": goal.value,
                    "recommendation_count": len(recommendations)
                }
            ))
            
        return recommendations
        
    async def _optimize_hit_rate(
        self,
        stats: CacheStatistics,
        config: Dict[str, Any]
    ) -> List[OptimizationRecommendation]:
        """Optimize for hit rate"""
        recommendations = []
        
        # Check hit rate
        if stats.hit_rate < 0.7:
            # Low hit rate
            if config.get("eviction_policy") == "FIFO":
                recommendations.append(OptimizationRecommendation(
                    recommendation_type="change_eviction_policy",
                    description="Change eviction policy from FIFO to LRU for better hit rate",
                    expected_improvement={"hit_rate": 0.1},
                    priority=8,
                    parameters={"eviction_policy": "LRU"}
                ))
                
            if config.get("max_size", 0) < stats.entry_count * 1.5:
                recommendations.append(OptimizationRecommendation(
                    recommendation_type="increase_cache_size",
                    description="Increase cache size to reduce evictions",
                    expected_improvement={"hit_rate": 0.15},
                    priority=9,
                    parameters={"max_size": int(stats.entry_count * 2)}
                ))
                
        # Check access patterns
        if stats.access_frequency:
            # Calculate skew
            frequencies = list(stats.access_frequency.values())
            if frequencies:
                top_20_percent = sorted(frequencies, reverse=True)[:len(frequencies)//5]
                if sum(top_20_percent) > sum(frequencies) * 0.8:
                    # High skew - 80/20 rule applies
                    recommendations.append(OptimizationRecommendation(
                        recommendation_type="implement_tiered_cache",
                        description="Implement tiered caching for hot/cold data separation",
                        expected_improvement={"hit_rate": 0.2, "memory": -0.3},
                        priority=7,
                        parameters={"hot_tier_size": len(top_20_percent)}
                    ))
                    
        return recommendations
        
    async def _optimize_memory(
        self,
        stats: CacheStatistics,
        config: Dict[str, Any]
    ) -> List[OptimizationRecommendation]:
        """Optimize for memory usage"""
        recommendations = []
        
        # Check memory efficiency
        if stats.entry_count > 0:
            avg_entry_size = stats.memory_bytes / stats.entry_count
            
            if avg_entry_size > 10240:  # 10KB per entry
                recommendations.append(OptimizationRecommendation(
                    recommendation_type="enable_compression",
                    description="Enable compression for large cache entries",
                    expected_improvement={"memory": -0.4},
                    priority=7,
                    parameters={"compression": "snappy"}
                ))
                
        # Check for stale data
        if stats.last_access_time:
            now = datetime.utcnow()
            stale_count = sum(
                1 for last_access in stats.last_access_time.values()
                if (now - last_access).total_seconds() > 3600  # 1 hour
            )
            
            if stale_count > stats.entry_count * 0.3:
                recommendations.append(OptimizationRecommendation(
                    recommendation_type="reduce_ttl",
                    description="Reduce TTL to remove stale data faster",
                    expected_improvement={"memory": -0.2},
                    priority=6,
                    parameters={"ttl_seconds": 1800}  # 30 minutes
                ))
                
        return recommendations
        
    async def _optimize_latency(
        self,
        stats: CacheStatistics,
        config: Dict[str, Any]
    ) -> List[OptimizationRecommendation]:
        """Optimize for latency"""
        recommendations = []
        
        # Check latency
        if stats.avg_hit_latency_ms > 5:
            recommendations.append(OptimizationRecommendation(
                recommendation_type="enable_local_cache",
                description="Enable local L1 cache for frequently accessed items",
                expected_improvement={"latency": -0.7},
                priority=8,
                parameters={"l1_cache_size": 1000}
            ))
            
        if stats.avg_miss_latency_ms > 100:
            recommendations.append(OptimizationRecommendation(
                recommendation_type="enable_async_loading",
                description="Enable asynchronous cache loading to reduce miss penalty",
                expected_improvement={"latency": -0.3},
                priority=7,
                parameters={"async_loading": True}
            ))
            
        return recommendations
        
    async def apply_recommendations(
        self,
        region_name: str,
        recommendations: List[OptimizationRecommendation],
        cache_configurator: Callable
    ) -> Dict[str, Any]:
        """
        Apply optimization recommendations.
        
        Args:
            region_name: Cache region name
            recommendations: Recommendations to apply
            cache_configurator: Function to update cache configuration
            
        Returns:
            Results of applied recommendations
        """
        results = {
            "applied": [],
            "failed": [],
            "skipped": []
        }
        
        for rec in recommendations:
            try:
                # Apply recommendation
                await cache_configurator(
                    region_name,
                    rec.recommendation_type,
                    rec.parameters
                )
                
                results["applied"].append(rec.recommendation_type)
                
                logger.info(f"Applied recommendation: {rec.recommendation_type}")
                
            except Exception as e:
                logger.error(f"Failed to apply recommendation {rec.recommendation_type}: {e}")
                results["failed"].append({
                    "type": rec.recommendation_type,
                    "error": str(e)
                })
                
        return results 