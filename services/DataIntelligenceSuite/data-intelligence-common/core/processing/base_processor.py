"""
Base Processor for DataIntelligenceSuite v2.0

Enhanced with enterprise-scale processing capabilities, automatic optimization,
and intelligent resource management.
"""

import asyncio
import logging
from typing import Any, Dict, Optional, List, Callable, Union, AsyncIterator, TypeVar, Generic
from datetime import datetime, timedelta
from dataclasses import dataclass, field
from enum import Enum
from abc import ABC, abstractmethod
import uuid
import json
from concurrent.futures import ThreadPoolExecutor
import psutil
import numpy as np

from platformq_shared.vault.vault_client import VaultClient
from platformq_shared.consul.consul_client import ConsulClient
from ...monitoring import MetricsCollector, StructuredLogger
from ...core.caching import CacheManager
from ..events import EventBus
from ..orchestration import DistributedOrchestrator

logger = StructuredLogger.get_logger(__name__)

T = TypeVar('T')


class ProcessingStatus(Enum):
    """Processing status"""
    PENDING = "pending"
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"
    CANCELLED = "cancelled"
    RETRYING = "retrying"
    PAUSED = "paused"
    OPTIMIZING = "optimizing"


class ProcessingMode(Enum):
    """Processing mode"""
    BATCH = "batch"
    STREAM = "stream"
    MICRO_BATCH = "micro_batch"
    HYBRID = "hybrid"  # Lambda architecture
    ADAPTIVE = "adaptive"  # Auto-select based on data


class PartitionStrategy(Enum):
    """Data partitioning strategies"""
    HASH = "hash"
    RANGE = "range"
    ROUND_ROBIN = "round_robin"
    KEY_BASED = "key_based"
    SIZE_BASED = "size_based"
    ADAPTIVE = "adaptive"


class BackpressureStrategy(Enum):
    """Backpressure handling strategies"""
    BUFFER = "buffer"
    DROP_NEWEST = "drop_newest"
    DROP_OLDEST = "drop_oldest"
    THROTTLE = "throttle"
    ADAPTIVE = "adaptive"


@dataclass
class ResourceLimits:
    """Resource limits for processing"""
    max_memory_mb: Optional[int] = None
    max_cpu_cores: Optional[float] = None
    max_disk_io_mbps: Optional[float] = None
    max_network_io_mbps: Optional[float] = None
    max_concurrent_tasks: Optional[int] = None
    
    def __post_init__(self):
        # Set intelligent defaults based on system resources
        if self.max_memory_mb is None:
            # Use 70% of available memory
            self.max_memory_mb = int(psutil.virtual_memory().available / 1024 / 1024 * 0.7)
        if self.max_cpu_cores is None:
            # Use 80% of CPU cores
            self.max_cpu_cores = psutil.cpu_count() * 0.8
        if self.max_concurrent_tasks is None:
            # 2x CPU cores for I/O bound tasks
            self.max_concurrent_tasks = psutil.cpu_count() * 2


@dataclass
class ProcessorConfig:
    """Enhanced configuration for processors v2.0"""
    name: str
    mode: ProcessingMode = ProcessingMode.ADAPTIVE
    
    # Parallelism and partitioning
    parallelism: int = -1  # -1 for auto-detect
    partition_strategy: PartitionStrategy = PartitionStrategy.ADAPTIVE
    partition_size_mb: int = 128  # Target partition size
    
    # Backpressure and flow control
    backpressure_strategy: BackpressureStrategy = BackpressureStrategy.ADAPTIVE
    buffer_size: int = 10000
    buffer_timeout: timedelta = timedelta(seconds=5)
    
    # Resource management
    resource_limits: ResourceLimits = field(default_factory=ResourceLimits)
    adaptive_resource_management: bool = True
    resource_check_interval: timedelta = timedelta(seconds=30)
    
    # Optimization
    enable_auto_optimization: bool = True
    optimization_interval: timedelta = timedelta(minutes=5)
    cost_optimization: bool = True
    
    # Retry and fault tolerance
    retry_attempts: int = 3
    retry_delay: timedelta = timedelta(seconds=5)
    retry_backoff_multiplier: float = 2.0
    timeout: Optional[timedelta] = None
    checkpoint_interval: Optional[timedelta] = timedelta(minutes=5)
    
    # Monitoring and observability
    enable_metrics: bool = True
    enable_tracing: bool = True
    enable_profiling: bool = False
    metrics_interval: timedelta = timedelta(seconds=10)
    
    # Caching
    enable_caching: bool = True
    cache_results: bool = True
    cache_ttl: Optional[timedelta] = timedelta(hours=1)
    cache_warming: bool = True
    
    # Security
    enable_encryption: bool = True
    encryption_key: str = "processor-data"
    required_roles: List[str] = field(default_factory=list)
    enable_audit: bool = True
    
    # Vault/Consul
    use_dynamic_credentials: bool = True
    credential_refresh_interval: timedelta = timedelta(hours=1)
    
    # Advanced features
    enable_lineage_tracking: bool = True
    enable_quality_checks: bool = True
    enable_cost_tracking: bool = True
    
    tags: List[str] = field(default_factory=list)
    metadata: Dict[str, Any] = field(default_factory=dict)
    
    def __post_init__(self):
        # Auto-detect parallelism if not set
        if self.parallelism == -1:
            self.parallelism = psutil.cpu_count()


@dataclass
class ProcessingMetrics:
    """Detailed processing metrics"""
    # Performance metrics
    records_processed: int = 0
    records_failed: int = 0
    records_skipped: int = 0
    bytes_processed: int = 0
    
    # Timing metrics
    processing_time_ms: float = 0
    queue_time_ms: float = 0
    serialization_time_ms: float = 0
    
    # Throughput metrics
    throughput_records_per_sec: float = 0
    throughput_mb_per_sec: float = 0
    
    # Resource metrics
    peak_memory_mb: float = 0
    avg_cpu_percent: float = 0
    peak_cpu_percent: float = 0
    
    # Cost metrics
    estimated_cost_usd: float = 0
    cost_per_record: float = 0
    
    # Quality metrics
    data_quality_score: float = 1.0
    validation_errors: int = 0
    
    # Partition metrics
    partitions_created: int = 0
    partition_skew: float = 0  # 0 = perfect balance, 1 = maximum skew
    
    def to_dict(self) -> Dict[str, Any]:
        return {k: v for k, v in self.__dict__.items() if not k.startswith('_')}


@dataclass
class ProcessingResult:
    """Enhanced result of processing operation"""
    job_id: str
    status: ProcessingStatus
    started_at: datetime
    completed_at: Optional[datetime] = None
    
    # Detailed metrics
    metrics: ProcessingMetrics = field(default_factory=ProcessingMetrics)
    
    # Output
    output_location: Optional[str] = None
    output_format: Optional[str] = None
    output_partitions: List[str] = field(default_factory=list)
    
    # Lineage
    input_datasets: List[str] = field(default_factory=list)
    output_datasets: List[str] = field(default_factory=list)
    transformations: List[str] = field(default_factory=list)
    
    # Errors and warnings
    errors: List[Dict[str, Any]] = field(default_factory=list)
    warnings: List[Dict[str, Any]] = field(default_factory=list)
    
    # Optimization suggestions
    optimization_suggestions: List[str] = field(default_factory=list)
    
    # Additional metadata
    metadata: Dict[str, Any] = field(default_factory=dict)


class BaseProcessor(ABC, Generic[T]):
    """
    Enhanced base processor for all data processing operations.
    
    New v2.0 Features:
    - Automatic data partitioning with multiple strategies
    - Parallel processing with backpressure control
    - Adaptive resource management
    - Cross-engine optimization
    - Intelligent caching with warming
    - Cost optimization
    - Advanced monitoring and profiling
    - Lineage tracking
    - Quality integration
    """
    
    def __init__(
        self,
        config: ProcessorConfig,
        metrics_collector: Optional[MetricsCollector] = None,
        cache_manager: Optional[CacheManager] = None,
        vault_client: Optional[VaultClient] = None,
        consul_client: Optional[ConsulClient] = None,
        event_bus: Optional[EventBus] = None,
        orchestrator: Optional[DistributedOrchestrator] = None
    ):
        self.config = config
        self.metrics = metrics_collector or MetricsCollector(config.name)
        self.cache = cache_manager
        self.vault_client = vault_client
        self.consul_client = consul_client
        self.event_bus = event_bus
        self.orchestrator = orchestrator
        
        # Processing state
        self._jobs: Dict[str, ProcessingResult] = {}
        self._active_jobs: Dict[str, asyncio.Task] = {}
        self._checkpoints: Dict[str, Any] = {}
        
        # Resource management
        self._resource_monitor: Optional[asyncio.Task] = None
        self._optimizer_task: Optional[asyncio.Task] = None
        self._current_resources = {
            "memory_mb": 0,
            "cpu_percent": 0,
            "active_tasks": 0
        }
        
        # Parallel processing
        self._executor = ThreadPoolExecutor(max_workers=config.parallelism)
        self._semaphore = asyncio.Semaphore(config.resource_limits.max_concurrent_tasks or config.parallelism)
        
        # Backpressure control
        self._buffer: asyncio.Queue = asyncio.Queue(maxsize=config.buffer_size)
        self._backpressure_state = {
            "dropped_items": 0,
            "throttle_rate": 1.0
        }
        
        # Callbacks
        self._progress_callbacks: List[Callable] = []
        self._completion_callbacks: List[Callable] = []
        
        # Security
        self._credentials: Dict[str, Any] = {}
        self._credential_task: Optional[asyncio.Task] = None
        self._processor_config: Dict[str, Any] = {}
        self._config_task: Optional[asyncio.Task] = None
        
    async def initialize(self):
        """Initialize processor with enhanced features"""
        # Load configuration from Consul
        if self.consul_client:
            await self._load_processor_config()
            self._config_task = asyncio.create_task(self._watch_config_changes())
            
        # Start credential renewal if using Vault
        if self.vault_client and self.config.use_dynamic_credentials:
            await self._refresh_credentials()
            self._credential_task = asyncio.create_task(self._credential_renewal_loop())
            
        # Start resource monitoring
        if self.config.adaptive_resource_management:
            self._resource_monitor = asyncio.create_task(self._monitor_resources())
            
        # Start optimizer
        if self.config.enable_auto_optimization:
            self._optimizer_task = asyncio.create_task(self._optimization_loop())
            
        # Warm cache if enabled
        if self.config.cache_warming and self.cache:
            await self._warm_cache()
            
        logger.info(f"Initialized processor v2.0: {self.config.name}")
        
    async def shutdown(self):
        """Enhanced shutdown with cleanup"""
        # Cancel background tasks
        tasks = [
            self._credential_task,
            self._config_task,
            self._resource_monitor,
            self._optimizer_task
        ]
        
        for task in tasks:
            if task:
                task.cancel()
            try:
                    await task
            except asyncio.CancelledError:
                pass
                
        # Cancel active jobs
        for job_id, task in self._active_jobs.items():
            task.cancel()
            
        # Shutdown executor
        self._executor.shutdown(wait=True)
            
        logger.info(f"Shutdown processor: {self.config.name}")
        
    async def _load_processor_config(self):
        """Load processor configuration from Consul"""
        try:
            config_key = f"data-intelligence/processors/{self.config.name}/config"
            config_data = await self.consul_client.kv_get(config_key)
            if config_data:
                self._processor_config = json.loads(config_data)
                # Update config with Consul values
                for key, value in self._processor_config.items():
                    if hasattr(self.config, key):
                        setattr(self.config, key, value)
                logger.info(f"Loaded processor config from Consul")
        except Exception as e:
            logger.error(f"Failed to load processor config: {e}")
            
    async def _watch_config_changes(self):
        """Watch for configuration changes in Consul"""
        while True:
            try:
                await asyncio.sleep(60)  # Check every minute
                await self._load_processor_config()
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Error watching config: {e}")
                
    async def _refresh_credentials(self):
        """Refresh credentials from Vault"""
        try:
            # Get database credentials if needed
            if self._processor_config.get("database_enabled"):
                db_mount = self._processor_config.get("database_mount", "database")
                db_role = self._processor_config.get("database_role", f"{self.config.name}-readonly")
                creds = await self.vault_client.get_database_credentials(db_mount, db_role)
                self._credentials["database"] = creds
                
            # Get API keys if needed
            if self._processor_config.get("api_keys"):
                for key_name in self._processor_config["api_keys"]:
                    secret_path = f"secret/data/processors/{self.config.name}/{key_name}"
                    secret = await self.vault_client.read_secret(secret_path)
                    if secret:
                        self._credentials[key_name] = secret.get("value")
                        
            logger.info("Refreshed processor credentials from Vault")
        except Exception as e:
            logger.error(f"Failed to refresh credentials: {e}")
            
    async def _credential_renewal_loop(self):
        """Background task to renew credentials"""
        while True:
            try:
                await asyncio.sleep(self.config.credential_refresh_interval.total_seconds())
                await self._refresh_credentials()
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Error in credential renewal: {e}")
        
    async def process(self, data: T, job_id: Optional[str] = None) -> ProcessingResult:
        """
        Process data with automatic optimization.
        Must be implemented by subclasses.
        """
        # This will be overridden, but we provide a template
        result = ProcessingResult(
            job_id=job_id or str(uuid.uuid4()),
            status=ProcessingStatus.RUNNING,
            started_at=datetime.utcnow()
        )
        
        try:
            # Partition data
            partitions = await self.partition_data(data)
            result.metrics.partitions_created = len(partitions)
            
            # Process partitions in parallel
            partition_results = await self.parallel_process(
                partitions,
                lambda p: self._process_partition(p, result.job_id)
            )
            
            # Merge results
            final_result = await self.merge_results(partition_results)
            
            # Update metrics
            result.status = ProcessingStatus.COMPLETED
            result.completed_at = datetime.utcnow()
            self._update_metrics(result)
            
            return result
            
        except Exception as e:
            result.status = ProcessingStatus.FAILED
            result.errors.append({
                "type": type(e).__name__,
                "message": str(e),
                "timestamp": datetime.utcnow().isoformat()
            })
            raise
            
    async def partition_data(
        self,
        data: T,
        strategy: Optional[PartitionStrategy] = None
    ) -> List[T]:
        """Intelligent data partitioning"""
        strategy = strategy or self.config.partition_strategy
        
        # Estimate data size
        data_size = self._estimate_data_size(data)
        optimal_partitions = max(
            1,
            min(
                int(data_size / (self.config.partition_size_mb * 1024 * 1024)),
                self.config.parallelism
            )
        )
        
        if strategy == PartitionStrategy.ADAPTIVE:
            # Choose strategy based on data characteristics
            strategy = self._select_partition_strategy(data)
            
        logger.debug(f"Partitioning data using {strategy.value} into {optimal_partitions} partitions")
        
        if strategy == PartitionStrategy.HASH:
            return self._hash_partition(data, optimal_partitions)
        elif strategy == PartitionStrategy.RANGE:
            return self._range_partition(data, optimal_partitions)
        elif strategy == PartitionStrategy.SIZE_BASED:
            return self._size_based_partition(data, self.config.partition_size_mb)
        else:
            return self._round_robin_partition(data, optimal_partitions)
            
    async def parallel_process(
        self,
        items: List[T],
        process_func: Callable[[T], AsyncIterator[Any]],
        max_concurrency: Optional[int] = None
    ) -> List[Any]:
        """Process items in parallel with backpressure control"""
        max_concurrency = max_concurrency or self.config.parallelism
        results = []
        
        # Create processing tasks with semaphore for concurrency control
        async def process_with_backpressure(item: T) -> Any:
            async with self._semaphore:
                # Check resource limits
                await self._wait_for_resources()
                
                # Apply backpressure if needed
                if self._buffer.full():
                    await self._apply_backpressure()
                    
                try:
                    return await process_func(item)
                finally:
                    # Update resource usage
                    self._current_resources["active_tasks"] -= 1
                    
        # Process all items
        tasks = [
            asyncio.create_task(process_with_backpressure(item))
            for item in items
        ]
        
        # Wait with progress tracking
        for i, task in enumerate(asyncio.as_completed(tasks)):
            result = await task
            results.append(result)
            
            # Report progress
            progress = (i + 1) / len(tasks)
            await self._notify_progress(
                "parallel_processing",
                progress,
                f"Processed {i + 1}/{len(tasks)} partitions"
            )
            
        return results
        
    async def _monitor_resources(self):
        """Monitor and adapt resource usage"""
        while True:
            try:
                # Get current resource usage
                process = psutil.Process()
                memory_mb = process.memory_info().rss / 1024 / 1024
                cpu_percent = process.cpu_percent(interval=1)
                
                self._current_resources.update({
                    "memory_mb": memory_mb,
                    "cpu_percent": cpu_percent
                })
                
                # Check limits and adapt
                if self.config.adaptive_resource_management:
                    await self._adapt_resources()
                    
                # Record metrics
                self.metrics.set_gauge("processor_memory_mb", memory_mb, {"processor": self.config.name})
                self.metrics.set_gauge("processor_cpu_percent", cpu_percent, {"processor": self.config.name})
                
                await asyncio.sleep(self.config.resource_check_interval.total_seconds())
                
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Resource monitoring error: {e}")
                
    async def _adapt_resources(self):
        """Adapt processing based on resource usage"""
        memory_usage_percent = (
            self._current_resources["memory_mb"] / 
            self.config.resource_limits.max_memory_mb * 100
        )
        
        if memory_usage_percent > 90:
            # Reduce parallelism
            new_limit = max(1, self._semaphore._value - 1)
            self._semaphore = asyncio.Semaphore(new_limit)
            logger.warning(f"High memory usage ({memory_usage_percent:.1f}%), reducing parallelism to {new_limit}")
            
        elif memory_usage_percent < 50 and self._semaphore._value < self.config.parallelism:
            # Increase parallelism
            new_limit = min(self.config.parallelism, self._semaphore._value + 1)
            self._semaphore = asyncio.Semaphore(new_limit)
            logger.info(f"Low memory usage ({memory_usage_percent:.1f}%), increasing parallelism to {new_limit}")
            
    async def _optimization_loop(self):
        """Continuously optimize processing"""
        while True:
            try:
                await asyncio.sleep(self.config.optimization_interval.total_seconds())
                
                # Analyze recent job metrics
                suggestions = await self._analyze_performance()
                
                # Apply optimizations
                if suggestions:
                    await self._apply_optimizations(suggestions)
                    
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Optimization error: {e}")
                
    async def _analyze_performance(self) -> List[str]:
        """Analyze performance and generate optimization suggestions"""
        suggestions = []
        
        # Analyze recent jobs
        recent_jobs = [
            job for job in self._jobs.values()
            if job.completed_at and 
            (datetime.utcnow() - job.completed_at).total_seconds() < 3600
        ]
        
        if not recent_jobs:
            return suggestions
            
        # Calculate averages
        avg_throughput = np.mean([j.metrics.throughput_records_per_sec for j in recent_jobs])
        avg_partition_skew = np.mean([j.metrics.partition_skew for j in recent_jobs])
        avg_cpu = np.mean([j.metrics.avg_cpu_percent for j in recent_jobs])
        
        # Generate suggestions
        if avg_partition_skew > 0.3:
            suggestions.append("High partition skew detected. Consider changing partition strategy.")
            
        if avg_cpu < 50:
            suggestions.append("Low CPU utilization. Consider increasing parallelism.")
            
        if avg_throughput < 1000:  # Records per second
            suggestions.append("Low throughput. Consider optimizing data serialization or increasing buffer size.")
            
        return suggestions
        
    async def _apply_optimizations(self, suggestions: List[str]):
        """Apply optimization suggestions"""
        for suggestion in suggestions:
            logger.info(f"Optimization suggestion: {suggestion}")
            
            if "partition strategy" in suggestion:
                # Switch to adaptive partitioning
                self.config.partition_strategy = PartitionStrategy.ADAPTIVE
                
            elif "increasing parallelism" in suggestion:
                # Increase parallelism by 20%
                self.config.parallelism = int(self.config.parallelism * 1.2)
                self._semaphore = asyncio.Semaphore(self.config.parallelism)
                
            elif "buffer size" in suggestion:
                # Increase buffer size
                self.config.buffer_size = int(self.config.buffer_size * 1.5)
                
    def _estimate_data_size(self, data: Any) -> int:
        """Estimate data size in bytes"""
        # Override in subclasses for accurate estimation
        import sys
        return sys.getsizeof(data)
        
    def _select_partition_strategy(self, data: Any) -> PartitionStrategy:
        """Select optimal partition strategy based on data"""
        # Override in subclasses
        return PartitionStrategy.HASH
        
    def _hash_partition(self, data: Any, num_partitions: int) -> List[Any]:
        """Hash-based partitioning"""
        # Override in subclasses
        return [data]  # Default: no partitioning
        
    def _range_partition(self, data: Any, num_partitions: int) -> List[Any]:
        """Range-based partitioning"""
        # Override in subclasses
        return [data]  # Default: no partitioning
        
    def _size_based_partition(self, data: Any, target_size_mb: int) -> List[Any]:
        """Size-based partitioning"""
        # Override in subclasses
        return [data]  # Default: no partitioning
        
    def _round_robin_partition(self, data: Any, num_partitions: int) -> List[Any]:
        """Round-robin partitioning"""
        # Override in subclasses
        return [data]  # Default: no partitioning
        
    async def merge_results(self, results: List[Any]) -> Any:
        """Merge results from parallel processing"""
        # Override in subclasses
        return results
        
    async def _wait_for_resources(self):
        """Wait until resources are available"""
        while True:
            memory_usage_percent = (
                self._current_resources["memory_mb"] / 
                self.config.resource_limits.max_memory_mb * 100
            )
            
            if memory_usage_percent < 85:  # 85% threshold
                break
                
            logger.debug("Waiting for resources to become available...")
            await asyncio.sleep(1)
            
    async def _apply_backpressure(self):
        """Apply backpressure strategy"""
        strategy = self.config.backpressure_strategy
        
        if strategy == BackpressureStrategy.THROTTLE:
            # Slow down processing
            await asyncio.sleep(self._backpressure_state["throttle_rate"])
            self._backpressure_state["throttle_rate"] *= 1.1  # Increase throttle
            
        elif strategy == BackpressureStrategy.DROP_NEWEST:
            # Drop newest items
            if not self._buffer.empty():
                self._buffer.get_nowait()
                self._backpressure_state["dropped_items"] += 1
                
        elif strategy == BackpressureStrategy.DROP_OLDEST:
            # Drop oldest items (clear buffer)
            while not self._buffer.empty():
                self._buffer.get_nowait()
                self._backpressure_state["dropped_items"] += 1
                
    async def _warm_cache(self):
        """Warm cache with frequently accessed data"""
        # Override in subclasses
        logger.info("Cache warming completed")
        
    def _update_metrics(self, result: ProcessingResult):
        """Update detailed metrics"""
        if result.completed_at and result.started_at:
            result.metrics.processing_time_ms = (
                result.completed_at - result.started_at
            ).total_seconds() * 1000
            
            if result.metrics.records_processed > 0:
                result.metrics.throughput_records_per_sec = (
                    result.metrics.records_processed / 
                    (result.metrics.processing_time_ms / 1000)
                )
                
                if result.metrics.bytes_processed > 0:
                    result.metrics.throughput_mb_per_sec = (
                        result.metrics.bytes_processed / 1024 / 1024 /
                        (result.metrics.processing_time_ms / 1000)
                    )
                    
        # Calculate cost if enabled
        if self.config.enable_cost_tracking:
            result.metrics.estimated_cost_usd = self._calculate_cost(result.metrics)
            if result.metrics.records_processed > 0:
                result.metrics.cost_per_record = (
                    result.metrics.estimated_cost_usd / 
                    result.metrics.records_processed
                )
                
    def _calculate_cost(self, metrics: ProcessingMetrics) -> float:
        """Calculate estimated processing cost"""
        # Simple cost model - override for accurate calculation
        cpu_hour_cost = 0.05  # $0.05 per CPU hour
        memory_gb_hour_cost = 0.01  # $0.01 per GB hour
        
        cpu_hours = (metrics.avg_cpu_percent / 100) * (metrics.processing_time_ms / 3600000)
        memory_gb_hours = (metrics.peak_memory_mb / 1024) * (metrics.processing_time_ms / 3600000)
        
        return (cpu_hours * cpu_hour_cost) + (memory_gb_hours * memory_gb_hour_cost)
        
    async def _process_partition(self, partition: Any, job_id: str) -> Any:
        """Process a single partition - override in subclasses"""
        raise NotImplementedError("Subclasses must implement _process_partition")
        
    async def submit_job(
        self,
        data: Any,
        job_id: Optional[str] = None,
        priority: int = 0,
        user_context: Optional[Dict[str, Any]] = None
    ) -> str:
        """Submit a processing job with access control"""
        if job_id is None:
            job_id = str(uuid.uuid4())
            
        # Check role requirements
        if self.config.required_roles and user_context:
            user_roles = user_context.get("roles", [])
            if not any(role in self.config.required_roles for role in user_roles):
                raise PermissionError(f"User lacks required roles: {self.config.required_roles}")
            
        # Check if job already exists
        if job_id in self._jobs:
            raise ValueError(f"Job {job_id} already exists")
            
        # Encrypt sensitive data if enabled
        if self.config.enable_encryption and self.vault_client:
            data = await self._encrypt_job_data(data)
            
        # Create job result
        result = ProcessingResult(
            job_id=job_id,
            status=ProcessingStatus.PENDING,
            started_at=datetime.utcnow(),
            metadata={"priority": priority, "user": user_context.get("user_id") if user_context else None}
        )
        self._jobs[job_id] = result
        
        # Start processing task
        task = asyncio.create_task(self._process_with_retry(data, job_id))
        self._active_jobs[job_id] = task
        
        logger.info(f"Submitted job {job_id} for processing")
        return job_id
        
    async def _encrypt_job_data(self, data: Any) -> Dict[str, Any]:
        """Encrypt sensitive job data"""
        import json
        plaintext = json.dumps(data, default=str)
        encrypted = await self.vault_client.transit_encrypt(
            self.config.encryption_key,
            plaintext
        )
        return {
            "encrypted": encrypted["ciphertext"],
            "metadata": {
                "encrypted_at": datetime.utcnow().isoformat(),
                "key_version": encrypted.get("key_version", 1)
            }
        }
        
    async def _decrypt_job_data(self, data: Dict[str, Any]) -> Any:
        """Decrypt job data"""
        if "encrypted" in data:
            decrypted = await self.vault_client.transit_decrypt(
                self.config.encryption_key,
                data["encrypted"]
            )
            import json
            return json.loads(decrypted)
        return data
        
    async def get_job_status(self, job_id: str) -> Optional[ProcessingResult]:
        """Get status of a processing job"""
        return self._jobs.get(job_id)
        
    async def cancel_job(self, job_id: str) -> bool:
        """Cancel a processing job"""
        if job_id not in self._active_jobs:
            return False
            
        task = self._active_jobs[job_id]
        task.cancel()
        
        # Update status
        if job_id in self._jobs:
            self._jobs[job_id].status = ProcessingStatus.CANCELLED
            self._jobs[job_id].completed_at = datetime.utcnow()
            
        logger.info(f"Cancelled job {job_id}")
        return True
        
    def add_progress_callback(self, callback: Callable):
        """Add progress callback"""
        self._progress_callbacks.append(callback)
        
    def add_completion_callback(self, callback: Callable):
        """Add completion callback"""
        self._completion_callbacks.append(callback)
        
    async def _process_with_retry(self, data: Any, job_id: str):
        """Process with retry logic"""
        result = self._jobs[job_id]
        result.status = ProcessingStatus.RUNNING
        result.started_at = datetime.utcnow()
        
        # Decrypt data if needed
        if isinstance(data, dict) and "encrypted" in data:
            data = await self._decrypt_job_data(data)
        
        attempts = 0
        last_error = None
        
        while attempts < self.config.retry_attempts:
            try:
                # Check cache if enabled
                if self.config.enable_caching and self.cache:
                    cache_key = self._generate_cache_key(data)
                    cached_result = await self.cache.get(f"{self.config.name}_results", cache_key)
                    if cached_result:
                        logger.info(f"Job {job_id} found in cache")
                        result.status = ProcessingStatus.COMPLETED
                        result.completed_at = datetime.utcnow()
                        result.metadata["cached"] = True
                        await self._notify_completion(result)
                        return
                        
                # Process data
                processing_result = await self._process_with_timeout(data, job_id)
                
                # Update result
                result.status = processing_result.status
                result.records_processed = processing_result.records_processed
                result.records_failed = processing_result.records_failed
                result.output_location = processing_result.output_location
                result.errors = processing_result.errors
                result.warnings = processing_result.warnings
                
                # Calculate metrics
                result.completed_at = datetime.utcnow()
                result.processing_time_ms = (
                    result.completed_at - result.started_at
                ).total_seconds() * 1000
                
                if result.records_processed > 0 and result.processing_time_ms > 0:
                    result.throughput_records_per_sec = (
                        result.records_processed / (result.processing_time_ms / 1000)
                    )
                    
                # Cache result if enabled
                if self.config.cache_results and self.cache and result.status == ProcessingStatus.COMPLETED:
                    await self.cache.put(
                        f"{self.config.name}_results",
                        cache_key,
                        processing_result,
                        self.config.cache_ttl
                    )
                    
                # Record metrics
                self._record_metrics(result)
                
                # Notify completion
                await self._notify_completion(result)
                
                # Success - exit retry loop
                break
                
            except asyncio.TimeoutError:
                logger.warning(f"Job {job_id} timed out (attempt {attempts + 1})")
                last_error = "Processing timeout"
                result.warnings.append({
                    "type": "timeout",
                    "attempt": attempts + 1,
                    "message": f"Timed out after {self.config.timeout}"
                })
                
            except Exception as e:
                logger.error(f"Job {job_id} failed (attempt {attempts + 1}): {e}")
                last_error = e
                result.errors.append({
                    "type": type(e).__name__,
                    "attempt": attempts + 1,
                    "message": str(e)
                })
                
            attempts += 1
            
            if attempts < self.config.retry_attempts:
                result.status = ProcessingStatus.RETRYING
                await asyncio.sleep(self.config.retry_delay.total_seconds())
                
        # All retries failed
        result.status = ProcessingStatus.FAILED
        result.completed_at = datetime.utcnow()
        result.metadata["final_error"] = str(last_error)
        
        await self._notify_completion(result)
        del self._active_jobs[job_id]
        
    async def _process_with_timeout(self, data: Any, job_id: str) -> ProcessingResult:
        """Process with timeout"""
        if self.config.timeout:
            return await asyncio.wait_for(
                self.process(data, job_id),
                self.config.timeout.total_seconds()
            )
        else:
            return await self.process(data, job_id)
            
    def _generate_cache_key(self, data: Any) -> str:
        """Generate cache key for data"""
        # Simple implementation - can be overridden
        import hashlib
        import json
        
        data_str = json.dumps(data, sort_keys=True, default=str)
        return hashlib.md5(data_str.encode()).hexdigest()
        
    async def _notify_progress(self, job_id: str, progress: float, message: Optional[str] = None):
        """Notify progress callbacks"""
        for callback in self._progress_callbacks:
            try:
                if asyncio.iscoroutinefunction(callback):
                    await callback(job_id, progress, message)
                else:
                    callback(job_id, progress, message)
            except Exception as e:
                logger.error(f"Progress callback error: {e}")
                
    async def _notify_completion(self, result: ProcessingResult):
        """Notify completion callbacks"""
        for callback in self._completion_callbacks:
            try:
                if asyncio.iscoroutinefunction(callback):
                    await callback(result)
                else:
                    callback(result)
            except Exception as e:
                logger.error(f"Completion callback error: {e}")
                
    def _record_metrics(self, result: ProcessingResult):
        """Record processing metrics"""
        if not self.metrics:
            return
            
        labels = {
            "processor": self.config.name,
            "mode": self.config.mode.value,
            "status": result.status.value
        }
        
        # Record counters
        self.metrics.increment_counter("processing_jobs_total", labels)
        self.metrics.increment_counter("processing_records_total", labels, result.records_processed)
        
        if result.records_failed > 0:
            self.metrics.increment_counter("processing_failures_total", labels, result.records_failed)
            
        # Record timing
        if result.processing_time_ms:
            self.metrics.observe_histogram(
                "processing_duration_ms",
                result.processing_time_ms,
                labels
            )
            
        # Record throughput
        if result.throughput_records_per_sec:
            self.metrics.set_gauge(
                "processing_throughput",
                result.throughput_records_per_sec,
                labels
            )
            
    async def checkpoint(self, job_id: str, state: Any):
        """Save checkpoint for job"""
        checkpoint_data = {
            "state": state,
            "timestamp": datetime.utcnow()
        }
        
        # Encrypt checkpoint if enabled
        if self.config.enable_encryption and self.vault_client:
            import json
            plaintext = json.dumps(checkpoint_data, default=str)
            encrypted = await self.vault_client.transit_encrypt(
                f"{self.config.encryption_key}-checkpoint",
                plaintext
            )
            checkpoint_data = {"encrypted": encrypted["ciphertext"]}
            
        self._checkpoints[job_id] = checkpoint_data
        
        # Also save to cache if available
        if self.cache and self.config.checkpoint_interval:
            await self.cache.put(
                f"{self.config.name}_checkpoints",
                job_id,
                checkpoint_data,
                self.config.checkpoint_interval * 2  # Keep for 2x checkpoint interval
            )
            
        logger.debug(f"Saved checkpoint for job {job_id}")
        
    async def restore_checkpoint(self, job_id: str) -> Optional[Any]:
        """Restore checkpoint for job"""
        checkpoint_data = self._checkpoints.get(job_id)
        
        if not checkpoint_data and self.cache:
            # Try to load from cache
            checkpoint_data = await self.cache.get(
                f"{self.config.name}_checkpoints",
                job_id
            )
            
        if checkpoint_data:
            # Decrypt if needed
            if isinstance(checkpoint_data, dict) and "encrypted" in checkpoint_data:
                decrypted = await self.vault_client.transit_decrypt(
                    f"{self.config.encryption_key}-checkpoint",
                    checkpoint_data["encrypted"]
                )
                import json
                checkpoint_data = json.loads(decrypted)
                
            return checkpoint_data.get("state")
            
        return None
        
    def get_credentials(self, key: str) -> Optional[Any]:
        """Get credentials for a specific key"""
        return self._credentials.get(key)
        
    def get_processor_config(self, key: str, default: Any = None) -> Any:
        """Get processor configuration value"""
        return self._processor_config.get(key, default) 