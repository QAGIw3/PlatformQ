"""
Performance Profiler

Provides profiling capabilities for identifying performance bottlenecks.
"""

from typing import Dict, Any, List, Optional, Callable, Union
from datetime import datetime, timedelta
from dataclasses import dataclass, field
from enum import Enum
from contextlib import contextmanager, asynccontextmanager
import asyncio
import time
import cProfile
import pstats
import io
import tracemalloc
import psutil
import functools
from collections import defaultdict
import threading

from ..monitoring import StructuredLogger

logger = StructuredLogger.get_logger(__name__)


class ProfileType(str, Enum):
    """Types of profiling"""
    CPU = "cpu"
    MEMORY = "memory"
    ASYNC = "async"
    COMBINED = "combined"


@dataclass
class ProfileResult:
    """Result of a profiling session"""
    profile_type: ProfileType
    duration: float
    start_time: datetime
    end_time: datetime
    
    # CPU profiling
    cpu_stats: Optional[pstats.Stats] = None
    top_functions: List[Dict[str, Any]] = field(default_factory=list)
    
    # Memory profiling
    memory_peak: Optional[int] = None
    memory_allocations: List[Dict[str, Any]] = field(default_factory=list)
    
    # Async profiling
    async_stats: Dict[str, Any] = field(default_factory=dict)
    
    # Hot paths
    hot_paths: List[Dict[str, Any]] = field(default_factory=list)
    
    # Recommendations
    recommendations: List[str] = field(default_factory=list)


class PerformanceProfiler:
    """
    Performance profiler for identifying bottlenecks.
    
    Features:
    - CPU profiling with hot path detection
    - Memory profiling with allocation tracking
    - Async task profiling
    - Automatic recommendation generation
    - Low-overhead sampling
    """
    
    def __init__(self):
        self._cpu_profiler = None
        self._memory_tracking = False
        self._async_tasks: Dict[int, Dict[str, Any]] = {}
        self._function_times: Dict[str, List[float]] = defaultdict(list)
        self._lock = threading.Lock()
        
    @contextmanager
    def profile(
        self,
        profile_type: ProfileType = ProfileType.CPU,
        enabled: bool = True
    ) -> ProfileResult:
        """Profile a code block"""
        if not enabled:
            yield None
            return
            
        result = ProfileResult(
            profile_type=profile_type,
            start_time=datetime.utcnow(),
            duration=0,
            end_time=datetime.utcnow()
        )
        
        start_time = time.time()
        
        try:
            if profile_type == ProfileType.CPU:
                with self._profile_cpu(result):
                    yield result
            elif profile_type == ProfileType.MEMORY:
                with self._profile_memory(result):
                    yield result
            elif profile_type == ProfileType.ASYNC:
                with self._profile_async(result):
                    yield result
            elif profile_type == ProfileType.COMBINED:
                with self._profile_combined(result):
                    yield result
        finally:
            result.duration = time.time() - start_time
            result.end_time = datetime.utcnow()
            
            # Generate recommendations
            self._generate_recommendations(result)
            
    @asynccontextmanager
    async def async_profile(
        self,
        profile_type: ProfileType = ProfileType.ASYNC,
        enabled: bool = True
    ) -> ProfileResult:
        """Profile an async code block"""
        if not enabled:
            yield None
            return
            
        result = ProfileResult(
            profile_type=profile_type,
            start_time=datetime.utcnow(),
            duration=0,
            end_time=datetime.utcnow()
        )
        
        start_time = time.time()
        
        try:
            if profile_type == ProfileType.ASYNC:
                async with self._async_profile_async(result):
                    yield result
            else:
                # Use sync profiling for other types
                with self.profile(profile_type, enabled) as sync_result:
                    result = sync_result
                    yield result
        finally:
            result.duration = time.time() - start_time
            result.end_time = datetime.utcnow()
            
            # Generate recommendations
            self._generate_recommendations(result)
            
    @contextmanager
    def _profile_cpu(self, result: ProfileResult):
        """CPU profiling"""
        profiler = cProfile.Profile()
        profiler.enable()
        
        try:
            yield
        finally:
            profiler.disable()
            
            # Get stats
            s = io.StringIO()
            stats = pstats.Stats(profiler, stream=s)
            stats.sort_stats(pstats.SortKey.CUMULATIVE)
            
            result.cpu_stats = stats
            
            # Extract top functions
            stats_dict = stats.stats
            top_functions = []
            
            for func, (cc, nc, tt, ct, callers) in stats_dict.items():
                top_functions.append({
                    'function': f"{func[0]}:{func[1]}:{func[2]}",
                    'calls': nc,
                    'total_time': tt,
                    'cumulative_time': ct,
                    'average_time': tt / nc if nc > 0 else 0
                })
                
            # Sort by cumulative time
            top_functions.sort(key=lambda x: x['cumulative_time'], reverse=True)
            result.top_functions = top_functions[:20]
            
            # Identify hot paths
            self._identify_hot_paths(result, stats_dict)
            
    @contextmanager
    def _profile_memory(self, result: ProfileResult):
        """Memory profiling"""
        tracemalloc.start()
        
        # Get initial memory
        process = psutil.Process()
        initial_memory = process.memory_info().rss
        
        try:
            yield
        finally:
            # Get memory peak
            current, peak = tracemalloc.get_traced_memory()
            result.memory_peak = peak
            
            # Get top allocations
            snapshot = tracemalloc.take_snapshot()
            top_stats = snapshot.statistics('lineno')
            
            allocations = []
            for stat in top_stats[:20]:
                allocations.append({
                    'file': stat.traceback.format()[0] if stat.traceback else 'unknown',
                    'size': stat.size,
                    'count': stat.count,
                    'average': stat.size // stat.count if stat.count > 0 else 0
                })
                
            result.memory_allocations = allocations
            
            # Calculate memory growth
            final_memory = process.memory_info().rss
            memory_growth = final_memory - initial_memory
            
            result.async_stats['memory_growth'] = memory_growth
            result.async_stats['memory_peak'] = peak
            
            tracemalloc.stop()
            
    @contextmanager
    def _profile_async(self, result: ProfileResult):
        """Async task profiling"""
        # Track all tasks
        initial_tasks = asyncio.all_tasks()
        task_start_times = {}
        
        for task in initial_tasks:
            task_id = id(task)
            task_start_times[task_id] = time.time()
            
        try:
            yield
        finally:
            # Analyze task execution
            final_tasks = asyncio.all_tasks()
            
            task_stats = {
                'initial_count': len(initial_tasks),
                'final_count': len(final_tasks),
                'created': 0,
                'completed': 0,
                'long_running': []
            }
            
            # Find new tasks
            for task in final_tasks:
                if task not in initial_tasks:
                    task_stats['created'] += 1
                    
            # Find completed tasks
            for task in initial_tasks:
                if task not in final_tasks:
                    task_stats['completed'] += 1
                    
            # Find long-running tasks
            current_time = time.time()
            for task in final_tasks:
                task_id = id(task)
                if task_id in task_start_times:
                    duration = current_time - task_start_times[task_id]
                    if duration > 1.0:  # Tasks running > 1 second
                        task_stats['long_running'].append({
                            'name': task.get_name(),
                            'duration': duration,
                            'stack': task.get_stack()
                        })
                        
            result.async_stats = task_stats
            
    @contextmanager
    def _profile_combined(self, result: ProfileResult):
        """Combined CPU and memory profiling"""
        # Start both profilers
        cpu_profiler = cProfile.Profile()
        cpu_profiler.enable()
        tracemalloc.start()
        
        try:
            yield
        finally:
            # Stop profilers
            cpu_profiler.disable()
            current, peak = tracemalloc.get_traced_memory()
            tracemalloc.stop()
            
            # Get CPU stats
            stats = pstats.Stats(cpu_profiler)
            result.cpu_stats = stats
            
            # Get memory stats
            result.memory_peak = peak
            
            # Combine analysis
            self._analyze_combined(result, stats)
            
    @asynccontextmanager
    async def _async_profile_async(self, result: ProfileResult):
        """Async-specific profiling"""
        # Track task creation and completion
        created_tasks = []
        completed_tasks = []
        
        def task_created_callback(task):
            created_tasks.append({
                'task': task,
                'created_at': time.time(),
                'name': task.get_name()
            })
            
        def task_done_callback(task):
            completed_tasks.append({
                'task': task,
                'completed_at': time.time(),
                'name': task.get_name()
            })
            
        # Monitor event loop
        loop = asyncio.get_event_loop()
        loop_start_time = loop.time()
        
        try:
            yield
        finally:
            # Calculate event loop metrics
            loop_end_time = loop.time()
            loop_duration = loop_end_time - loop_start_time
            
            result.async_stats = {
                'loop_duration': loop_duration,
                'tasks_created': len(created_tasks),
                'tasks_completed': len(completed_tasks),
                'pending_tasks': len(asyncio.all_tasks()),
                'task_details': []
            }
            
            # Analyze task execution times
            for created in created_tasks:
                for completed in completed_tasks:
                    if created['task'] == completed['task']:
                        duration = completed['completed_at'] - created['created_at']
                        result.async_stats['task_details'].append({
                            'name': created['name'],
                            'duration': duration
                        })
                        
    def _identify_hot_paths(self, result: ProfileResult, stats_dict: Dict):
        """Identify hot paths in CPU profile"""
        hot_paths = []
        
        # Find functions that consume > 10% of total time
        total_time = sum(stat[2] for stat in stats_dict.values())
        
        for func, (cc, nc, tt, ct, callers) in stats_dict.items():
            if ct / total_time > 0.1:  # > 10% of time
                hot_paths.append({
                    'function': f"{func[0]}:{func[1]}:{func[2]}",
                    'percentage': (ct / total_time) * 100,
                    'cumulative_time': ct,
                    'calls': nc,
                    'callers': list(callers.keys()) if callers else []
                })
                
        result.hot_paths = sorted(hot_paths, key=lambda x: x['percentage'], reverse=True)
        
    def _analyze_combined(self, result: ProfileResult, stats: pstats.Stats):
        """Analyze combined CPU and memory profile"""
        # Extract top CPU consumers
        stats_dict = stats.stats
        top_cpu = []
        
        for func, (cc, nc, tt, ct, callers) in list(stats_dict.items())[:10]:
            top_cpu.append({
                'function': f"{func[0]}:{func[1]}:{func[2]}",
                'cpu_time': ct,
                'calls': nc
            })
            
        # Combine with memory data
        result.async_stats['top_cpu_consumers'] = top_cpu
        result.async_stats['memory_cpu_correlation'] = self._correlate_memory_cpu(result)
        
    def _correlate_memory_cpu(self, result: ProfileResult) -> List[Dict[str, Any]]:
        """Correlate memory allocations with CPU usage"""
        correlations = []
        
        # This is a simplified correlation
        # In practice, would need more sophisticated analysis
        if result.memory_allocations and result.top_functions:
            for alloc in result.memory_allocations[:5]:
                for func in result.top_functions[:5]:
                    # Check if allocation is from same file as hot function
                    if alloc['file'] in func['function']:
                        correlations.append({
                            'function': func['function'],
                            'memory_allocated': alloc['size'],
                            'cpu_time': func['cumulative_time']
                        })
                        
        return correlations
        
    def _generate_recommendations(self, result: ProfileResult):
        """Generate performance recommendations"""
        recommendations = []
        
        # CPU recommendations
        if result.top_functions:
            # Check for functions consuming > 20% time
            for func in result.top_functions[:3]:
                if func['cumulative_time'] > result.duration * 0.2:
                    recommendations.append(
                        f"Function {func['function']} consumes {func['cumulative_time']:.2f}s "
                        f"({(func['cumulative_time']/result.duration)*100:.1f}% of total time). "
                        "Consider optimization."
                    )
                    
            # Check for functions called many times
            for func in result.top_functions:
                if func['calls'] > 10000 and func['average_time'] > 0.0001:
                    recommendations.append(
                        f"Function {func['function']} called {func['calls']} times. "
                        "Consider caching or batching."
                    )
                    
        # Memory recommendations
        if result.memory_peak:
            if result.memory_peak > 100 * 1024 * 1024:  # > 100MB
                recommendations.append(
                    f"High memory usage detected: {result.memory_peak / 1024 / 1024:.1f}MB peak. "
                    "Consider streaming or chunking data."
                )
                
        # Async recommendations
        if result.async_stats.get('long_running'):
            for task in result.async_stats['long_running']:
                recommendations.append(
                    f"Long-running async task '{task['name']}' ({task['duration']:.1f}s). "
                    "Consider breaking into smaller tasks."
                )
                
        if result.async_stats.get('pending_tasks', 0) > 100:
            recommendations.append(
                f"High number of pending tasks: {result.async_stats['pending_tasks']}. "
                "Consider task throttling or pooling."
            )
            
        result.recommendations = recommendations
        
    def profile_function(
        self,
        profile_type: ProfileType = ProfileType.CPU
    ) -> Callable:
        """Decorator for profiling functions"""
        def decorator(func):
            if asyncio.iscoroutinefunction(func):
                @functools.wraps(func)
                async def async_wrapper(*args, **kwargs):
                    async with self.async_profile(profile_type) as result:
                        value = await func(*args, **kwargs)
                        
                        # Log results
                        if result and result.recommendations:
                            logger.warning(
                                f"Performance issues in {func.__name__}",
                                recommendations=result.recommendations
                            )
                            
                        return value
                return async_wrapper
            else:
                @functools.wraps(func)
                def sync_wrapper(*args, **kwargs):
                    with self.profile(profile_type) as result:
                        value = func(*args, **kwargs)
                        
                        # Log results
                        if result and result.recommendations:
                            logger.warning(
                                f"Performance issues in {func.__name__}",
                                recommendations=result.recommendations
                            )
                            
                        return value
                return sync_wrapper
        return decorator
        
    def get_hot_paths(self, result: ProfileResult, threshold: float = 0.1) -> List[Dict[str, Any]]:
        """Get functions that consume more than threshold of total time"""
        if not result.hot_paths:
            return []
            
        return [
            path for path in result.hot_paths
            if path['percentage'] > threshold * 100
        ]
        
    def format_report(self, result: ProfileResult) -> str:
        """Format a human-readable performance report"""
        lines = [
            f"Performance Profile Report",
            f"=" * 50,
            f"Type: {result.profile_type.value}",
            f"Duration: {result.duration:.3f}s",
            f"Time: {result.start_time} - {result.end_time}",
            ""
        ]
        
        # CPU section
        if result.top_functions:
            lines.extend([
                "Top CPU Consumers:",
                "-" * 30
            ])
            
            for i, func in enumerate(result.top_functions[:10]):
                lines.append(
                    f"{i+1}. {func['function']}: "
                    f"{func['cumulative_time']:.3f}s "
                    f"({func['calls']} calls)"
                )
                
            lines.append("")
            
        # Memory section
        if result.memory_peak:
            lines.extend([
                "Memory Usage:",
                "-" * 30,
                f"Peak: {result.memory_peak / 1024 / 1024:.1f}MB"
            ])
            
            if result.memory_allocations:
                lines.append("\nTop Allocations:")
                for i, alloc in enumerate(result.memory_allocations[:5]):
                    lines.append(
                        f"{i+1}. {alloc['file']}: "
                        f"{alloc['size'] / 1024:.1f}KB "
                        f"({alloc['count']} allocations)"
                    )
                    
            lines.append("")
            
        # Async section
        if result.async_stats:
            lines.extend([
                "Async Statistics:",
                "-" * 30
            ])
            
            for key, value in result.async_stats.items():
                if key != 'long_running' and key != 'task_details':
                    lines.append(f"{key}: {value}")
                    
            lines.append("")
            
        # Hot paths
        if result.hot_paths:
            lines.extend([
                "Hot Paths (>10% time):",
                "-" * 30
            ])
            
            for path in result.hot_paths:
                lines.append(
                    f"- {path['function']}: {path['percentage']:.1f}%"
                )
                
            lines.append("")
            
        # Recommendations
        if result.recommendations:
            lines.extend([
                "Recommendations:",
                "-" * 30
            ])
            
            for i, rec in enumerate(result.recommendations):
                lines.append(f"{i+1}. {rec}")
                
        return "\n".join(lines)


# Global profiler instance
_profiler = PerformanceProfiler()


def profile(profile_type: ProfileType = ProfileType.CPU, enabled: bool = True):
    """Context manager for profiling"""
    return _profiler.profile(profile_type, enabled)


def async_profile(profile_type: ProfileType = ProfileType.ASYNC, enabled: bool = True):
    """Async context manager for profiling"""
    return _profiler.async_profile(profile_type, enabled)


def profile_function(profile_type: ProfileType = ProfileType.CPU):
    """Decorator for profiling functions"""
    return _profiler.profile_function(profile_type)


# Export main components
__all__ = [
    'PerformanceProfiler',
    'ProfileType',
    'ProfileResult',
    'profile',
    'async_profile',
    'profile_function'
] 