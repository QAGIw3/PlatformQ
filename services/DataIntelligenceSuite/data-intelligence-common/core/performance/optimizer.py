"""
Performance Optimizer

Provides automatic performance optimization capabilities.
"""

import asyncio
import functools
import time
import statistics
from typing import Any, Dict, List, Optional, Callable, Union, Tuple, TypeVar
from dataclasses import dataclass, field
from enum import Enum
from collections import defaultdict, deque
import numpy as np
from datetime import datetime, timedelta
import threading
import multiprocessing as mp
from concurrent.futures import ThreadPoolExecutor

from .benchmarks import PerformanceBenchmark, BenchmarkConfig, BenchmarkResult
from ...monitoring import StructuredLogger, MetricsCollector
from ...caching import CacheManager

logger = StructuredLogger.get_logger(__name__)

T = TypeVar('T')


class OptimizationType(str, Enum):
    """Types of optimizations"""
    CACHING = "caching"
    PARALLELIZATION = "parallelization"
    BATCHING = "batching"
    VECTORIZATION = "vectorization"
    MEMORY = "memory"
    IO = "io"
    ALGORITHM = "algorithm"


@dataclass
class OptimizationResult:
    """Result of an optimization"""
    optimization_type: OptimizationType
    original_performance: float  # ops/sec
    optimized_performance: float  # ops/sec
    improvement_percent: float
    recommendations: List[str] = field(default_factory=list)
    applied: bool = False
    metadata: Dict[str, Any] = field(default_factory=dict)


@dataclass
class OptimizationConfig:
    """Configuration for optimization"""
    enable_auto_optimization: bool = True
    optimization_threshold: float = 0.1  # 10% improvement threshold
    
    # Optimization types
    enable_caching: bool = True
    enable_parallelization: bool = True
    enable_batching: bool = True
    enable_vectorization: bool = True
    
    # Resource limits
    max_cache_size_mb: float = 1024
    max_parallel_workers: int = mp.cpu_count()
    max_batch_size: int = 1000
    
    # Monitoring
    profile_interval_seconds: int = 60
    optimization_interval_seconds: int = 300


class PerformanceOptimizer:
    """Automatic performance optimizer"""
    
    def __init__(
        self,
        config: OptimizationConfig,
        cache_manager: Optional[CacheManager] = None,
        metrics: Optional[MetricsCollector] = None
    ):
        self.config = config
        self.cache = cache_manager
        self.metrics = metrics or MetricsCollector()
        
        # Performance history
        self._performance_history: Dict[str, deque] = defaultdict(
            lambda: deque(maxlen=100)
        )
        self._optimization_history: List[OptimizationResult] = []
        
        # Active optimizations
        self._active_optimizations: Dict[str, OptimizationType] = {}
        
    def optimize_function(
        self,
        func: Callable[..., T],
        optimization_types: Optional[List[OptimizationType]] = None
    ) -> Callable[..., T]:
        """Optimize a function with automatic performance improvements"""
        
        if optimization_types is None:
            optimization_types = []
            if self.config.enable_caching:
                optimization_types.append(OptimizationType.CACHING)
            if self.config.enable_batching:
                optimization_types.append(OptimizationType.BATCHING)
                
        # Apply optimizations
        optimized_func = func
        
        if OptimizationType.CACHING in optimization_types:
            optimized_func = self._add_caching(optimized_func)
            
        if OptimizationType.BATCHING in optimization_types:
            optimized_func = self._add_batching(optimized_func)
            
        if OptimizationType.PARALLELIZATION in optimization_types:
            optimized_func = self._add_parallelization(optimized_func)
            
        # Add performance monitoring
        optimized_func = self._add_monitoring(optimized_func, func.__name__)
        
        return optimized_func
        
    def _add_caching(self, func: Callable[..., T]) -> Callable[..., T]:
        """Add caching to a function"""
        
        @functools.wraps(func)
        async def cached_wrapper(*args, **kwargs):
            # Generate cache key
            cache_key = self._generate_cache_key(func.__name__, args, kwargs)
            
            # Check cache
            if self.cache:
                cached_result = await self.cache.get(cache_key)
                if cached_result is not None:
                    return cached_result
                    
            # Execute function
            result = await func(*args, **kwargs) if asyncio.iscoroutinefunction(func) else func(*args, **kwargs)
            
            # Store in cache
            if self.cache:
                await self.cache.set(cache_key, result, ttl=3600)
                
            return result
            
        return cached_wrapper
        
    def _add_batching(self, func: Callable[..., T]) -> Callable[..., T]:
        """Add batching to a function"""
        batch_queue = []
        batch_lock = threading.Lock()
        batch_event = threading.Event()
        
        @functools.wraps(func)
        async def batched_wrapper(item):
            # Add to batch
            with batch_lock:
                batch_queue.append(item)
                
                # Process batch if full
                if len(batch_queue) >= self.config.max_batch_size:
                    batch = batch_queue[:]
                    batch_queue.clear()
                else:
                    # Wait for batch to fill or timeout
                    batch_event.wait(timeout=0.1)
                    with batch_lock:
                        if len(batch_queue) > 0:
                            batch = batch_queue[:]
                            batch_queue.clear()
                        else:
                            batch = [item]
                            
            # Process batch
            if asyncio.iscoroutinefunction(func):
                results = await func(batch)
            else:
                results = func(batch)
                
            # Return individual result
            if isinstance(results, list) and len(results) == len(batch):
                return results[batch.index(item)]
            else:
                return results
                
        return batched_wrapper
        
    def _add_parallelization(self, func: Callable[..., T]) -> Callable[..., T]:
        """Add parallelization to a function"""
        
        @functools.wraps(func)
        async def parallel_wrapper(items):
            if not isinstance(items, list):
                items = [items]
                
            # Create worker pool
            loop = asyncio.get_event_loop()
            
            if asyncio.iscoroutinefunction(func):
                # Async parallelization
                tasks = [func(item) for item in items]
                results = await asyncio.gather(*tasks)
            else:
                # Thread pool parallelization
                with ThreadPoolExecutor(max_workers=self.config.max_parallel_workers) as executor:
                    futures = [
                        loop.run_in_executor(executor, func, item)
                        for item in items
                    ]
                    results = await asyncio.gather(*futures)
                    
            return results
            
        return parallel_wrapper
        
    def _add_monitoring(self, func: Callable[..., T], name: str) -> Callable[..., T]:
        """Add performance monitoring to a function"""
        
        @functools.wraps(func)
        async def monitored_wrapper(*args, **kwargs):
            start_time = time.perf_counter()
            
            try:
                if asyncio.iscoroutinefunction(func):
                    result = await func(*args, **kwargs)
                else:
                    result = func(*args, **kwargs)
                    
                duration = time.perf_counter() - start_time
                
                # Record performance
                self._performance_history[name].append({
                    "timestamp": datetime.utcnow(),
                    "duration": duration,
                    "success": True
                })
                
                # Check if optimization needed
                if self.config.enable_auto_optimization:
                    await self._check_optimization_needed(name)
                    
                return result
                
            except Exception as e:
                duration = time.perf_counter() - start_time
                
                self._performance_history[name].append({
                    "timestamp": datetime.utcnow(),
                    "duration": duration,
                    "success": False
                })
                
                raise
                
        return monitored_wrapper
        
    async def _check_optimization_needed(self, function_name: str):
        """Check if optimization is needed for a function"""
        history = self._performance_history[function_name]
        
        if len(history) < 10:
            return  # Not enough data
            
        # Calculate performance trend
        recent_durations = [h["duration"] for h in list(history)[-10:]]
        avg_duration = statistics.mean(recent_durations)
        
        # Check if performance is degrading
        older_durations = [h["duration"] for h in list(history)[-20:-10]]
        if older_durations:
            old_avg = statistics.mean(older_durations)
            
            if avg_duration > old_avg * 1.2:  # 20% slower
                logger.warning(
                    f"Performance degradation detected for {function_name}: "
                    f"{old_avg:.3f}s -> {avg_duration:.3f}s"
                )
                
                # Trigger optimization
                await self._optimize_function(function_name)
                
    async def _optimize_function(self, function_name: str):
        """Apply optimizations to a function"""
        logger.info(f"Optimizing function: {function_name}")
        
        # Analyze performance characteristics
        analysis = await self._analyze_performance(function_name)
        
        # Determine best optimization
        if analysis.get("cache_hit_potential", 0) > 0.3:
            self._apply_optimization(
                function_name,
                OptimizationType.CACHING,
                analysis
            )
            
        elif analysis.get("parallelizable", False):
            self._apply_optimization(
                function_name,
                OptimizationType.PARALLELIZATION,
                analysis
            )
            
        elif analysis.get("batchable", False):
            self._apply_optimization(
                function_name,
                OptimizationType.BATCHING,
                analysis
            )
            
    async def _analyze_performance(self, function_name: str) -> Dict[str, Any]:
        """Analyze performance characteristics"""
        history = self._performance_history[function_name]
        
        analysis = {
            "function_name": function_name,
            "total_calls": len(history),
            "avg_duration": statistics.mean([h["duration"] for h in history]),
            "error_rate": sum(1 for h in history if not h["success"]) / len(history)
        }
        
        # Check for patterns
        durations = [h["duration"] for h in history]
        
        # Cache hit potential (repeated similar durations)
        unique_durations = len(set(round(d, 3) for d in durations))
        analysis["cache_hit_potential"] = 1 - (unique_durations / len(durations))
        
        # Parallelization potential (consistent execution time)
        if len(durations) > 10:
            cv = statistics.stdev(durations) / statistics.mean(durations)
            analysis["parallelizable"] = cv < 0.3
            
        # Batching potential (many quick calls)
        analysis["batchable"] = analysis["avg_duration"] < 0.01 and len(history) > 100
        
        return analysis
        
    def _apply_optimization(
        self,
        function_name: str,
        optimization_type: OptimizationType,
        analysis: Dict[str, Any]
    ):
        """Apply an optimization to a function"""
        if function_name in self._active_optimizations:
            logger.info(
                f"Function {function_name} already optimized with "
                f"{self._active_optimizations[function_name]}"
            )
            return
            
        self._active_optimizations[function_name] = optimization_type
        
        result = OptimizationResult(
            optimization_type=optimization_type,
            original_performance=1 / analysis["avg_duration"],  # ops/sec
            optimized_performance=0,  # Will be measured
            improvement_percent=0,
            applied=True,
            metadata=analysis
        )
        
        self._optimization_history.append(result)
        
        logger.info(
            f"Applied {optimization_type.value} optimization to {function_name}"
        )
        
    def _generate_cache_key(self, func_name: str, args: tuple, kwargs: dict) -> str:
        """Generate cache key for function call"""
        import hashlib
        import json
        
        key_parts = [
            func_name,
            json.dumps(args, sort_keys=True, default=str),
            json.dumps(kwargs, sort_keys=True, default=str)
        ]
        
        key_string = "|".join(key_parts)
        return hashlib.sha256(key_string.encode()).hexdigest()
        
    async def profile_system(self) -> Dict[str, Any]:
        """Profile system performance"""
        profile = {
            "timestamp": datetime.utcnow(),
            "functions": {}
        }
        
        # Analyze each function
        for func_name, history in self._performance_history.items():
            if history:
                recent = list(history)[-100:]
                profile["functions"][func_name] = {
                    "calls": len(recent),
                    "avg_duration": statistics.mean([h["duration"] for h in recent]),
                    "error_rate": sum(1 for h in recent if not h["success"]) / len(recent),
                    "optimization": self._active_optimizations.get(func_name)
                }
                
        # System metrics
        import psutil
        profile["system"] = {
            "cpu_percent": psutil.cpu_percent(interval=1),
            "memory_percent": psutil.virtual_memory().percent,
            "disk_io": psutil.disk_io_counters()._asdict() if psutil.disk_io_counters() else {}
        }
        
        return profile
        
    def get_optimization_report(self) -> str:
        """Generate optimization report"""
        report = "Performance Optimization Report\n"
        report += "=" * 50 + "\n\n"
        
        # Active optimizations
        report += f"Active Optimizations: {len(self._active_optimizations)}\n"
        for func_name, opt_type in self._active_optimizations.items():
            report += f"  - {func_name}: {opt_type.value}\n"
            
        report += "\n"
        
        # Optimization history
        if self._optimization_history:
            report += "Optimization History:\n"
            for opt in self._optimization_history[-10:]:  # Last 10
                report += f"  - {opt.optimization_type.value}: "
                report += f"{opt.improvement_percent:.1f}% improvement\n"
                
        report += "\n"
        
        # Performance summary
        report += "Function Performance Summary:\n"
        for func_name, history in self._performance_history.items():
            if history:
                recent = list(history)[-100:]
                avg_duration = statistics.mean([h["duration"] for h in recent])
                report += f"  - {func_name}: {avg_duration:.3f}s avg, {len(recent)} calls\n"
                
        return report


class AdaptiveOptimizer:
    """Adaptive optimizer that learns from performance patterns"""
    
    def __init__(
        self,
        optimizer: PerformanceOptimizer,
        learning_rate: float = 0.1
    ):
        self.optimizer = optimizer
        self.learning_rate = learning_rate
        
        # Optimization effectiveness history
        self._effectiveness: Dict[Tuple[str, OptimizationType], float] = defaultdict(float)
        
    async def optimize_adaptively(
        self,
        function: Callable,
        workload: List[Any]
    ) -> Tuple[Callable, OptimizationResult]:
        """Optimize function based on workload characteristics"""
        
        # Analyze workload
        workload_profile = self._analyze_workload(workload)
        
        # Select best optimization
        best_optimization = await self._select_optimization(
            function.__name__,
            workload_profile
        )
        
        # Apply optimization
        optimized_func = self.optimizer.optimize_function(
            function,
            [best_optimization]
        )
        
        # Measure improvement
        improvement = await self._measure_improvement(
            function,
            optimized_func,
            workload
        )
        
        # Update effectiveness
        self._update_effectiveness(
            function.__name__,
            best_optimization,
            improvement
        )
        
        result = OptimizationResult(
            optimization_type=best_optimization,
            original_performance=improvement["original"],
            optimized_performance=improvement["optimized"],
            improvement_percent=improvement["percent"],
            applied=True
        )
        
        return optimized_func, result
        
    def _analyze_workload(self, workload: List[Any]) -> Dict[str, Any]:
        """Analyze workload characteristics"""
        return {
            "size": len(workload),
            "homogeneous": len(set(type(item) for item in workload)) == 1,
            "data_size": sum(
                len(str(item)) if hasattr(item, '__len__') else 1
                for item in workload
            ),
            "unique_ratio": len(set(str(item) for item in workload)) / len(workload)
        }
        
    async def _select_optimization(
        self,
        function_name: str,
        workload_profile: Dict[str, Any]
    ) -> OptimizationType:
        """Select best optimization based on history and workload"""
        
        candidates = []
        
        # Caching is good for low unique ratio
        if workload_profile["unique_ratio"] < 0.5:
            candidates.append((
                OptimizationType.CACHING,
                self._effectiveness.get((function_name, OptimizationType.CACHING), 0.5)
            ))
            
        # Batching is good for small items
        if workload_profile["data_size"] / workload_profile["size"] < 1000:
            candidates.append((
                OptimizationType.BATCHING,
                self._effectiveness.get((function_name, OptimizationType.BATCHING), 0.5)
            ))
            
        # Parallelization is good for homogeneous workloads
        if workload_profile["homogeneous"]:
            candidates.append((
                OptimizationType.PARALLELIZATION,
                self._effectiveness.get((function_name, OptimizationType.PARALLELIZATION), 0.5)
            ))
            
        # Select best based on effectiveness
        if candidates:
            candidates.sort(key=lambda x: x[1], reverse=True)
            return candidates[0][0]
        else:
            return OptimizationType.CACHING  # Default
            
    async def _measure_improvement(
        self,
        original_func: Callable,
        optimized_func: Callable,
        workload: List[Any]
    ) -> Dict[str, float]:
        """Measure performance improvement"""
        
        # Benchmark original
        original_start = time.perf_counter()
        for item in workload[:100]:  # Sample
            if asyncio.iscoroutinefunction(original_func):
                await original_func(item)
            else:
                original_func(item)
        original_duration = time.perf_counter() - original_start
        
        # Benchmark optimized
        optimized_start = time.perf_counter()
        for item in workload[:100]:
            if asyncio.iscoroutinefunction(optimized_func):
                await optimized_func(item)
            else:
                optimized_func(item)
        optimized_duration = time.perf_counter() - optimized_start
        
        return {
            "original": 100 / original_duration,  # ops/sec
            "optimized": 100 / optimized_duration,
            "percent": ((original_duration - optimized_duration) / original_duration) * 100
        }
        
    def _update_effectiveness(
        self,
        function_name: str,
        optimization_type: OptimizationType,
        improvement: Dict[str, float]
    ):
        """Update optimization effectiveness using exponential moving average"""
        key = (function_name, optimization_type)
        current = self._effectiveness.get(key, 0.5)
        
        # Normalize improvement to [0, 1]
        effectiveness = min(1.0, max(0.0, improvement["percent"] / 100))
        
        # Update with learning rate
        self._effectiveness[key] = (
            current * (1 - self.learning_rate) +
            effectiveness * self.learning_rate
        ) 