"""
Performance Benchmarks

Provides comprehensive benchmarking capabilities for the data intelligence platform.
"""

import asyncio
import time
import psutil
import gc
import tracemalloc
from typing import Any, Dict, List, Optional, Callable, Union, Tuple
from datetime import datetime, timedelta
from dataclasses import dataclass, field
from enum import Enum
import statistics
import json
import numpy as np
from concurrent.futures import ProcessPoolExecutor, ThreadPoolExecutor

from ...monitoring import StructuredLogger, MetricsCollector

logger = StructuredLogger.get_logger(__name__)


class BenchmarkType(str, Enum):
    """Types of benchmarks"""
    THROUGHPUT = "throughput"
    LATENCY = "latency"
    MEMORY = "memory"
    CPU = "cpu"
    IO = "io"
    CONCURRENCY = "concurrency"
    SCALABILITY = "scalability"


@dataclass
class BenchmarkResult:
    """Result of a benchmark run"""
    benchmark_name: str
    benchmark_type: BenchmarkType
    started_at: datetime
    completed_at: datetime
    duration_seconds: float
    
    # Performance metrics
    throughput: Optional[float] = None  # ops/sec
    latency_p50: Optional[float] = None  # milliseconds
    latency_p95: Optional[float] = None
    latency_p99: Optional[float] = None
    
    # Resource metrics
    memory_used_mb: Optional[float] = None
    peak_memory_mb: Optional[float] = None
    cpu_percent: Optional[float] = None
    
    # Additional metrics
    operations_completed: int = 0
    errors: int = 0
    error_rate: float = 0.0
    
    # Raw measurements
    measurements: List[float] = field(default_factory=list)
    metadata: Dict[str, Any] = field(default_factory=dict)


@dataclass
class BenchmarkConfig:
    """Configuration for benchmarks"""
    name: str
    iterations: int = 100
    warmup_iterations: int = 10
    duration_seconds: Optional[float] = None
    concurrent_workers: int = 1
    
    # Resource limits
    max_memory_mb: Optional[float] = None
    max_cpu_percent: Optional[float] = None
    
    # Measurement settings
    measure_memory: bool = True
    measure_cpu: bool = True
    measure_io: bool = False
    
    # Output settings
    save_results: bool = True
    results_file: Optional[str] = None
    verbose: bool = False


class PerformanceBenchmark:
    """Base class for performance benchmarks"""
    
    def __init__(
        self,
        config: BenchmarkConfig,
        metrics: Optional[MetricsCollector] = None
    ):
        self.config = config
        self.metrics = metrics or MetricsCollector()
        self._process = psutil.Process()
        
    async def run(self) -> BenchmarkResult:
        """Run the benchmark"""
        logger.info(f"Starting benchmark: {self.config.name}")
        
        # Initialize result
        result = BenchmarkResult(
            benchmark_name=self.config.name,
            benchmark_type=BenchmarkType.THROUGHPUT,
            started_at=datetime.utcnow(),
            completed_at=datetime.utcnow(),
            duration_seconds=0
        )
        
        # Warmup
        if self.config.warmup_iterations > 0:
            logger.info(f"Running {self.config.warmup_iterations} warmup iterations")
            await self._warmup()
            
        # Reset metrics
        gc.collect()
        if self.config.measure_memory:
            tracemalloc.start()
            
        # Record initial state
        initial_memory = self._get_memory_usage()
        initial_cpu = self._process.cpu_percent(interval=0.1)
        
        # Run benchmark
        measurements = []
        errors = 0
        start_time = time.perf_counter()
        
        try:
            if self.config.duration_seconds:
                # Time-based benchmark
                end_time = start_time + self.config.duration_seconds
                operations = 0
                
                while time.perf_counter() < end_time:
                    try:
                        op_start = time.perf_counter()
                        await self._run_single_operation()
                        op_duration = (time.perf_counter() - op_start) * 1000  # ms
                        measurements.append(op_duration)
                        operations += 1
                    except Exception as e:
                        errors += 1
                        if self.config.verbose:
                            logger.error(f"Operation failed: {e}")
                            
            else:
                # Iteration-based benchmark
                for i in range(self.config.iterations):
                    try:
                        op_start = time.perf_counter()
                        await self._run_single_operation()
                        op_duration = (time.perf_counter() - op_start) * 1000  # ms
                        measurements.append(op_duration)
                    except Exception as e:
                        errors += 1
                        if self.config.verbose:
                            logger.error(f"Operation {i} failed: {e}")
                            
        finally:
            end_time = time.perf_counter()
            duration = end_time - start_time
            
            # Collect final metrics
            final_memory = self._get_memory_usage()
            avg_cpu = self._process.cpu_percent(interval=0.1)
            
            if self.config.measure_memory and tracemalloc.is_tracing():
                current, peak = tracemalloc.get_traced_memory()
                tracemalloc.stop()
                result.peak_memory_mb = peak / 1024 / 1024
                
        # Calculate results
        result.completed_at = datetime.utcnow()
        result.duration_seconds = duration
        result.operations_completed = len(measurements)
        result.errors = errors
        result.error_rate = errors / (len(measurements) + errors) if measurements else 1.0
        
        if measurements:
            result.throughput = len(measurements) / duration
            result.latency_p50 = statistics.median(measurements)
            result.latency_p95 = np.percentile(measurements, 95)
            result.latency_p99 = np.percentile(measurements, 99)
            result.measurements = measurements
            
        result.memory_used_mb = final_memory - initial_memory
        result.cpu_percent = avg_cpu
        
        # Save results
        if self.config.save_results:
            await self._save_results(result)
            
        logger.info(
            f"Benchmark completed: {result.operations_completed} ops, "
            f"{result.throughput:.2f} ops/sec, "
            f"p50={result.latency_p50:.2f}ms"
        )
        
        return result
        
    async def _warmup(self):
        """Run warmup iterations"""
        for _ in range(self.config.warmup_iterations):
            try:
                await self._run_single_operation()
            except:
                pass  # Ignore errors during warmup
                
    async def _run_single_operation(self):
        """Run a single benchmark operation - override in subclasses"""
        raise NotImplementedError("Subclasses must implement _run_single_operation")
        
    def _get_memory_usage(self) -> float:
        """Get current memory usage in MB"""
        return self._process.memory_info().rss / 1024 / 1024
        
    async def _save_results(self, result: BenchmarkResult):
        """Save benchmark results"""
        filename = self.config.results_file or f"benchmark_{self.config.name}_{datetime.utcnow().isoformat()}.json"
        
        data = {
            "benchmark_name": result.benchmark_name,
            "benchmark_type": result.benchmark_type.value,
            "started_at": result.started_at.isoformat(),
            "completed_at": result.completed_at.isoformat(),
            "duration_seconds": result.duration_seconds,
            "throughput": result.throughput,
            "latency_p50": result.latency_p50,
            "latency_p95": result.latency_p95,
            "latency_p99": result.latency_p99,
            "memory_used_mb": result.memory_used_mb,
            "peak_memory_mb": result.peak_memory_mb,
            "cpu_percent": result.cpu_percent,
            "operations_completed": result.operations_completed,
            "errors": result.errors,
            "error_rate": result.error_rate,
            "metadata": result.metadata
        }
        
        with open(filename, 'w') as f:
            json.dump(data, f, indent=2)


class ThroughputBenchmark(PerformanceBenchmark):
    """Benchmark for measuring throughput"""
    
    def __init__(
        self,
        config: BenchmarkConfig,
        operation: Callable,
        data_generator: Optional[Callable] = None,
        metrics: Optional[MetricsCollector] = None
    ):
        super().__init__(config, metrics)
        self.operation = operation
        self.data_generator = data_generator or (lambda: None)
        
    async def _run_single_operation(self):
        """Run the operation being benchmarked"""
        data = self.data_generator()
        
        if asyncio.iscoroutinefunction(self.operation):
            await self.operation(data)
        else:
            await asyncio.get_event_loop().run_in_executor(
                None, self.operation, data
            )


class ConcurrencyBenchmark(PerformanceBenchmark):
    """Benchmark for measuring concurrent performance"""
    
    def __init__(
        self,
        config: BenchmarkConfig,
        operation: Callable,
        data_generator: Optional[Callable] = None,
        metrics: Optional[MetricsCollector] = None
    ):
        super().__init__(config, metrics)
        self.operation = operation
        self.data_generator = data_generator or (lambda: None)
        self.config.benchmark_type = BenchmarkType.CONCURRENCY
        
    async def run(self) -> BenchmarkResult:
        """Run concurrent benchmark"""
        logger.info(
            f"Starting concurrency benchmark: {self.config.name} "
            f"with {self.config.concurrent_workers} workers"
        )
        
        result = BenchmarkResult(
            benchmark_name=self.config.name,
            benchmark_type=BenchmarkType.CONCURRENCY,
            started_at=datetime.utcnow(),
            completed_at=datetime.utcnow(),
            duration_seconds=0
        )
        
        # Warmup
        if self.config.warmup_iterations > 0:
            await self._warmup()
            
        # Run concurrent operations
        start_time = time.perf_counter()
        
        tasks = []
        for _ in range(self.config.concurrent_workers):
            task = asyncio.create_task(self._run_worker())
            tasks.append(task)
            
        # Wait for all workers
        worker_results = await asyncio.gather(*tasks, return_exceptions=True)
        
        end_time = time.perf_counter()
        duration = end_time - start_time
        
        # Aggregate results
        all_measurements = []
        total_operations = 0
        total_errors = 0
        
        for worker_result in worker_results:
            if isinstance(worker_result, Exception):
                logger.error(f"Worker failed: {worker_result}")
            else:
                measurements, errors = worker_result
                all_measurements.extend(measurements)
                total_operations += len(measurements)
                total_errors += errors
                
        # Calculate results
        result.completed_at = datetime.utcnow()
        result.duration_seconds = duration
        result.operations_completed = total_operations
        result.errors = total_errors
        result.error_rate = total_errors / (total_operations + total_errors) if total_operations else 1.0
        
        if all_measurements:
            result.throughput = total_operations / duration
            result.latency_p50 = statistics.median(all_measurements)
            result.latency_p95 = np.percentile(all_measurements, 95)
            result.latency_p99 = np.percentile(all_measurements, 99)
            result.measurements = all_measurements
            
        result.metadata["concurrent_workers"] = self.config.concurrent_workers
        
        return result
        
    async def _run_worker(self) -> Tuple[List[float], int]:
        """Run operations in a single worker"""
        measurements = []
        errors = 0
        
        operations_per_worker = self.config.iterations // self.config.concurrent_workers
        
        for _ in range(operations_per_worker):
            try:
                op_start = time.perf_counter()
                await self._run_single_operation()
                op_duration = (time.perf_counter() - op_start) * 1000
                measurements.append(op_duration)
            except Exception as e:
                errors += 1
                
        return measurements, errors


class ScalabilityBenchmark:
    """Benchmark for measuring scalability"""
    
    def __init__(
        self,
        base_config: BenchmarkConfig,
        operation: Callable,
        data_generator: Optional[Callable] = None,
        metrics: Optional[MetricsCollector] = None
    ):
        self.base_config = base_config
        self.operation = operation
        self.data_generator = data_generator
        self.metrics = metrics
        
    async def run(
        self,
        worker_counts: List[int] = [1, 2, 4, 8, 16]
    ) -> Dict[int, BenchmarkResult]:
        """Run scalability benchmark with different worker counts"""
        results = {}
        
        for worker_count in worker_counts:
            config = BenchmarkConfig(
                name=f"{self.base_config.name}_workers_{worker_count}",
                iterations=self.base_config.iterations,
                warmup_iterations=self.base_config.warmup_iterations,
                concurrent_workers=worker_count
            )
            
            benchmark = ConcurrencyBenchmark(
                config=config,
                operation=self.operation,
                data_generator=self.data_generator,
                metrics=self.metrics
            )
            
            result = await benchmark.run()
            results[worker_count] = result
            
            logger.info(
                f"Scalability test with {worker_count} workers: "
                f"{result.throughput:.2f} ops/sec"
            )
            
        # Calculate scalability metrics
        baseline = results.get(1)
        if baseline:
            for worker_count, result in results.items():
                if worker_count > 1:
                    speedup = result.throughput / baseline.throughput
                    efficiency = speedup / worker_count
                    result.metadata["speedup"] = speedup
                    result.metadata["efficiency"] = efficiency
                    
        return results


class BenchmarkSuite:
    """Suite of benchmarks"""
    
    def __init__(self, name: str, metrics: Optional[MetricsCollector] = None):
        self.name = name
        self.metrics = metrics
        self.benchmarks: List[PerformanceBenchmark] = []
        
    def add_benchmark(self, benchmark: PerformanceBenchmark):
        """Add a benchmark to the suite"""
        self.benchmarks.append(benchmark)
        
    async def run(self) -> List[BenchmarkResult]:
        """Run all benchmarks in the suite"""
        logger.info(f"Starting benchmark suite: {self.name}")
        
        results = []
        for benchmark in self.benchmarks:
            try:
                result = await benchmark.run()
                results.append(result)
            except Exception as e:
                logger.error(f"Benchmark {benchmark.config.name} failed: {e}")
                
        return results
        
    def generate_report(self, results: List[BenchmarkResult]) -> str:
        """Generate a report from benchmark results"""
        report = f"Benchmark Suite: {self.name}\n"
        report += "=" * 50 + "\n\n"
        
        for result in results:
            report += f"Benchmark: {result.benchmark_name}\n"
            report += f"Type: {result.benchmark_type.value}\n"
            report += f"Duration: {result.duration_seconds:.2f} seconds\n"
            report += f"Operations: {result.operations_completed}\n"
            report += f"Errors: {result.errors} ({result.error_rate:.2%})\n"
            
            if result.throughput:
                report += f"Throughput: {result.throughput:.2f} ops/sec\n"
                
            if result.latency_p50:
                report += f"Latency (p50): {result.latency_p50:.2f} ms\n"
                report += f"Latency (p95): {result.latency_p95:.2f} ms\n"
                report += f"Latency (p99): {result.latency_p99:.2f} ms\n"
                
            if result.memory_used_mb:
                report += f"Memory Used: {result.memory_used_mb:.2f} MB\n"
                
            if result.cpu_percent:
                report += f"CPU Usage: {result.cpu_percent:.1f}%\n"
                
            report += "\n"
            
        return report 