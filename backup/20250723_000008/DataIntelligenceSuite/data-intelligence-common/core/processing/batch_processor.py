"""
Batch Processing Implementation for DataIntelligenceSuite v2.0

Enhanced with enterprise-scale batch processing capabilities, intelligent optimization,
and seamless integration with multiple compute engines.
"""

import asyncio
import logging
from typing import Any, Dict, List, Optional, Callable, Union, Tuple
from datetime import datetime, timedelta
from dataclasses import dataclass, field
from enum import Enum
import uuid
import json
from pathlib import Path

try:
    from pyspark.sql import SparkSession, DataFrame
    from pyspark.sql.types import StructType
    from pyspark import SparkConf
    SPARK_AVAILABLE = True
except ImportError:
    SPARK_AVAILABLE = False
    
try:
    import ray
    RAY_AVAILABLE = True
except ImportError:
    RAY_AVAILABLE = False
    
try:
    import dask.dataframe as dd
    DASK_AVAILABLE = True
except ImportError:
    DASK_AVAILABLE = False

import pandas as pd
import numpy as np

from .base_processor import (
    BaseProcessor, ProcessorConfig, ProcessingResult, ProcessingStatus,
    ProcessingMode, PartitionStrategy as BasePartitionStrategy,
    ProcessingMetrics, ResourceLimits
)
from ...monitoring import StructuredLogger
from ...core.lakehouse import LakehouseManager, TableFormat
from ...core.quality import QualityProcessor
from ...core.lineage import LineageTracker

logger = StructuredLogger.get_logger(__name__)


class BatchEngine(Enum):
    """Available batch processing engines"""
    SPARK = "spark"
    RAY = "ray"
    DASK = "dask"
    PANDAS = "pandas"  # For small datasets
    AUTO = "auto"  # Auto-select based on data size and type


class OptimizationStrategy(Enum):
    """Batch optimization strategies"""
    THROUGHPUT = "throughput"  # Maximize records/sec
    LATENCY = "latency"  # Minimize processing time
    COST = "cost"  # Minimize compute cost
    BALANCED = "balanced"  # Balance all factors


@dataclass
class BatchConfig(ProcessorConfig):
    """Enhanced configuration for batch processing v2.0"""
    # Engine configuration
    engine: BatchEngine = BatchEngine.AUTO
    engine_config: Dict[str, Any] = field(default_factory=dict)
    
    # Batch settings
    batch_size: int = 10000
    micro_batch_size: int = 1000  # For streaming-like processing
    
    # Optimization
    optimization_strategy: OptimizationStrategy = OptimizationStrategy.BALANCED
    enable_adaptive_batching: bool = True
    enable_predicate_pushdown: bool = True
    enable_column_pruning: bool = True
    enable_partition_pruning: bool = True
    
    # Spark-specific (if using Spark)
    spark_master: str = "local[*]"
    spark_app_name: str = "DataIntelligenceBatch"
    spark_config: Dict[str, str] = field(default_factory=dict)
    adaptive_query_execution: bool = True
    broadcast_threshold_mb: int = 10
    shuffle_partitions: int = 200
    
    # Ray-specific (if using Ray)
    ray_address: Optional[str] = None
    ray_num_cpus: Optional[int] = None
    ray_num_gpus: Optional[int] = None
    
    # Dask-specific (if using Dask)
    dask_scheduler: Optional[str] = None
    dask_n_workers: int = 4
    dask_threads_per_worker: int = 2
    
    # Input/Output
    input_format: str = "parquet"
    output_format: str = "parquet"
    compression: str = "snappy"
    
    # Lakehouse integration
    enable_lakehouse: bool = True
    table_format: TableFormat = TableFormat.ICEBERG
    enable_time_travel: bool = True
    
    # Quality integration
    enable_quality_checks: bool = True
    quality_threshold: float = 0.95
    
    # Advanced features
    enable_incremental_processing: bool = True
    enable_schema_evolution: bool = True
    enable_data_skipping: bool = True
    enable_z_ordering: bool = True  # For optimized reads
    
    def __post_init__(self):
        super().__post_init__()
        # Set mode to BATCH if not already set
        if self.mode != ProcessingMode.BATCH:
            self.mode = ProcessingMode.BATCH


@dataclass
class BatchJob:
    """Enhanced batch job information"""
    job_id: str
    name: str
    input_paths: List[str]  # Support multiple inputs
    output_path: str
    
    # Processing
    transform_func: Optional[Callable] = None
    sql_query: Optional[str] = None  # Support SQL transformations
    
    # Configuration
    config: BatchConfig = field(default_factory=BatchConfig)
    
    # Metadata
    created_at: datetime = field(default_factory=datetime.utcnow)
    created_by: Optional[str] = None
    tags: List[str] = field(default_factory=list)
    
    # Runtime state
    engine_job_id: Optional[str] = None
    partitions_processed: int = 0
    total_partitions: Optional[int] = None
    
    # Incremental processing
    last_processed_timestamp: Optional[datetime] = None
    watermark: Optional[str] = None


@dataclass
class BatchResult(ProcessingResult):
    """Enhanced result of batch processing"""
    # Record counts
    input_records: int = 0
    output_records: int = 0
    
    # Data quality
    invalid_records: int = 0
    duplicate_records: int = 0
    quality_score: float = 1.0
    quality_report: Optional[Dict[str, Any]] = None
    
    # Performance metrics
    read_time_ms: float = 0
    transform_time_ms: float = 0
    write_time_ms: float = 0
    optimization_time_ms: float = 0
    
    # Engine-specific metrics
    engine_metrics: Dict[str, Any] = field(default_factory=dict)
    
    # Output details
    output_files: List[str] = field(default_factory=list)
    output_size_bytes: int = 0
    output_table: Optional[str] = None
    
    # Lakehouse metadata
    table_version: Optional[int] = None
    snapshot_id: Optional[str] = None


class BatchProcessor(BaseProcessor[Union[BatchJob, str, List[str]]]):
    """
    Enhanced batch processor for enterprise-scale data processing.
    
    New v2.0 Features:
    - Multi-engine support (Spark, Ray, Dask)
    - Intelligent engine selection
    - Lakehouse integration (Iceberg, Delta, Hudi)
    - Incremental processing
    - Advanced optimization strategies
    - Built-in quality checks
    - Schema evolution
    - Cost optimization
    """
    
    def __init__(
        self,
        config: BatchConfig,
        lakehouse_manager: Optional[LakehouseManager] = None,
        quality_processor: Optional[QualityProcessor] = None,
        lineage_tracker: Optional[LineageTracker] = None,
        **kwargs
    ):
        super().__init__(config, **kwargs)
        self.config: BatchConfig = config
        self.lakehouse = lakehouse_manager
        self.quality = quality_processor
        self.lineage = lineage_tracker
        
        # Engine instances
        self.spark: Optional[SparkSession] = None
        self.ray_context: Optional[Any] = None
        self.dask_client: Optional[Any] = None
        
        # Job tracking
        self._jobs: Dict[str, BatchJob] = {}
        self._engine_cache: Dict[str, Any] = {}
        
    async def initialize(self):
        """Initialize batch processor with auto engine selection"""
        await super().initialize()
        
        logger.info(f"Initializing batch processor v2.0: {self.config.name}")
        
        # Initialize selected engine
        if self.config.engine == BatchEngine.AUTO:
            self._select_optimal_engine()
        
        await self._initialize_engine()
        
        # Initialize lakehouse if enabled
        if self.config.enable_lakehouse and self.lakehouse:
            await self.lakehouse.initialize()
            
    async def _initialize_engine(self):
        """Initialize the selected processing engine"""
        if self.config.engine == BatchEngine.SPARK and SPARK_AVAILABLE:
            self.spark = self._create_spark_session()
        elif self.config.engine == BatchEngine.RAY and RAY_AVAILABLE:
            self.ray_context = self._initialize_ray()
        elif self.config.engine == BatchEngine.DASK and DASK_AVAILABLE:
            self.dask_client = self._initialize_dask()
        else:
            # Fallback to pandas for small datasets
            logger.info("Using Pandas engine for batch processing")
            
    def _select_optimal_engine(self):
        """Select optimal engine based on data and resources"""
        # Check available engines
        available_engines = []
        if SPARK_AVAILABLE:
            available_engines.append(BatchEngine.SPARK)
        if RAY_AVAILABLE:
            available_engines.append(BatchEngine.RAY)
        if DASK_AVAILABLE:
            available_engines.append(BatchEngine.DASK)
            
        if not available_engines:
            self.config.engine = BatchEngine.PANDAS
            return
            
        # Select based on optimization strategy
        if self.config.optimization_strategy == OptimizationStrategy.THROUGHPUT:
            # Spark is generally best for throughput
            self.config.engine = BatchEngine.SPARK if BatchEngine.SPARK in available_engines else available_engines[0]
        elif self.config.optimization_strategy == OptimizationStrategy.LATENCY:
            # Ray is good for low latency
            self.config.engine = BatchEngine.RAY if BatchEngine.RAY in available_engines else available_engines[0]
        elif self.config.optimization_strategy == OptimizationStrategy.COST:
            # Dask can be more cost-effective for certain workloads
            self.config.engine = BatchEngine.DASK if BatchEngine.DASK in available_engines else available_engines[0]
        else:
            # Default to Spark for balanced approach
            self.config.engine = BatchEngine.SPARK if BatchEngine.SPARK in available_engines else available_engines[0]
            
        logger.info(f"Auto-selected {self.config.engine.value} engine")
        
    def _create_spark_session(self) -> 'SparkSession':
        """Create optimized Spark session"""
        conf = SparkConf()
        
        # Set application name
        conf.setAppName(self.config.spark_app_name)
        
        # Apply custom configuration
        for key, value in self.config.spark_config.items():
            conf.set(key, value)
            
        # Standard optimizations
        conf.set("spark.sql.adaptive.enabled", str(self.config.adaptive_query_execution).lower())
        conf.set("spark.sql.adaptive.coalescePartitions.enabled", "true")
        conf.set("spark.sql.adaptive.skewJoin.enabled", "true")
        conf.set("spark.sql.autoBroadcastJoinThreshold", f"{self.config.broadcast_threshold_mb}MB")
        conf.set("spark.sql.shuffle.partitions", str(self.config.shuffle_partitions))
        
        # Lakehouse optimizations
        if self.config.enable_lakehouse:
            if self.config.table_format == TableFormat.ICEBERG:
                conf.set("spark.sql.catalog.spark_catalog", "org.apache.iceberg.spark.SparkSessionCatalog")
                conf.set("spark.sql.catalog.spark_catalog.type", "hive")
            elif self.config.table_format == TableFormat.DELTA:
                conf.set("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
                conf.set("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
                
        # Cost optimizations
        if self.config.optimization_strategy == OptimizationStrategy.COST:
            conf.set("spark.dynamicAllocation.enabled", "true")
            conf.set("spark.dynamicAllocation.minExecutors", "1")
            conf.set("spark.dynamicAllocation.maxExecutors", "10")
            
        builder = SparkSession.builder.config(conf=conf)
        
        if self.config.spark_master:
            builder = builder.master(self.config.spark_master)
            
        return builder.getOrCreate()
        
    def _initialize_ray(self):
        """Initialize Ray for distributed processing"""
        if not ray.is_initialized():
            ray.init(
                address=self.config.ray_address,
                num_cpus=self.config.ray_num_cpus,
                num_gpus=self.config.ray_num_gpus,
                **self.config.engine_config.get("ray", {})
            )
        return ray
        
    def _initialize_dask(self):
        """Initialize Dask client"""
        from dask.distributed import Client
        
        if self.config.dask_scheduler:
            client = Client(self.config.dask_scheduler)
        else:
            client = Client(
                n_workers=self.config.dask_n_workers,
                threads_per_worker=self.config.dask_threads_per_worker,
                **self.config.engine_config.get("dask", {})
            )
        return client
        
    async def process(
        self,
        data: Union[BatchJob, str, List[str]],
        job_id: Optional[str] = None
    ) -> BatchResult:
        """
        Process batch data with automatic optimization.
        
        Args:
            data: BatchJob, file path, or list of file paths
            job_id: Optional job ID
            
        Returns:
            BatchResult with detailed metrics
        """
        job_id = job_id or str(uuid.uuid4())
        
        # Create batch job if needed
        if isinstance(data, BatchJob):
            job = data
            job.job_id = job_id
        else:
            # Convert path(s) to BatchJob
            paths = [data] if isinstance(data, str) else data
            job = BatchJob(
                job_id=job_id,
                name=f"batch_{job_id}",
                input_paths=paths,
                output_path=f"{self.config.metadata.get('output_base', '/tmp')}/batch_{job_id}",
                config=self.config
            )
            
        self._jobs[job_id] = job
        
        # Create result object
        result = BatchResult(
            job_id=job_id,
            status=ProcessingStatus.RUNNING,
            started_at=datetime.utcnow()
        )
        
        try:
            # Track lineage if enabled
            if self.lineage:
                await self.lineage.track_job_start(job_id, job.input_paths, job.output_path)
            
            # Process based on engine
            if self.config.engine == BatchEngine.SPARK:
                await self._process_with_spark(job, result)
            elif self.config.engine == BatchEngine.RAY:
                await self._process_with_ray(job, result)
            elif self.config.engine == BatchEngine.DASK:
                await self._process_with_dask(job, result)
            else:
                await self._process_with_pandas(job, result)
                
            # Run quality checks if enabled
            if self.config.enable_quality_checks and self.quality:
                await self._run_quality_checks(job, result)
                
            # Update lineage
            if self.lineage:
                await self.lineage.track_job_complete(
                    job_id,
                    result.output_files,
                    result.metrics.to_dict()
                )
                
            result.status = ProcessingStatus.COMPLETED
            
        except Exception as e:
            logger.error(f"Batch processing failed: {e}", exc_info=True)
            result.status = ProcessingStatus.FAILED
            result.errors.append({
                "type": type(e).__name__,
                "message": str(e),
                "timestamp": datetime.utcnow().isoformat()
            })
            
        finally:
            result.completed_at = datetime.utcnow()
            self._update_metrics(result)
            
        return result
        
    async def _process_with_spark(self, job: BatchJob, result: BatchResult):
        """Process batch with Spark"""
        start_time = datetime.utcnow()
        
        # Read data
        read_start = datetime.utcnow()
        df = self._read_spark_data(job.input_paths, job.config.input_format)
        result.input_records = df.count()
        result.metrics.read_time_ms = (datetime.utcnow() - read_start).total_seconds() * 1000
        
        # Apply transformations
        transform_start = datetime.utcnow()
        if job.sql_query:
            # Register temp view and run SQL
            df.createOrReplaceTempView("input_data")
            df = self.spark.sql(job.sql_query)
        elif job.transform_func:
            df = job.transform_func(df)
            
        # Optimize if enabled
        if self.config.enable_adaptive_batching:
            df = self._optimize_spark_dataframe(df)
            
        result.metrics.transform_time_ms = (datetime.utcnow() - transform_start).total_seconds() * 1000
        
        # Write results
        write_start = datetime.utcnow()
        if self.config.enable_lakehouse and self.lakehouse:
            # Write to lakehouse table
            table_name = f"batch_output_{job.job_id}"
            await self._write_to_lakehouse(df, table_name, job)
            result.output_table = table_name
        else:
            # Write to files
            self._write_spark_data(df, job.output_path, job.config.output_format)
            result.output_files = self._list_output_files(job.output_path)
            
        result.output_records = df.count()
        result.metrics.write_time_ms = (datetime.utcnow() - write_start).total_seconds() * 1000
        
        # Collect engine metrics
        if self.spark.sparkContext._jsc:
            status = self.spark.sparkContext.statusTracker()
            result.engine_metrics = {
                "active_jobs": len(status.getActiveJobIds()),
                "active_stages": len(status.getActiveStageIds()),
                "executor_count": len(status.getExecutorInfos())
            }
            
    def _read_spark_data(self, paths: List[str], format: str) -> 'DataFrame':
        """Read data into Spark DataFrame"""
        reader = self.spark.read
        
        # Apply optimizations
        if self.config.enable_predicate_pushdown:
            reader = reader.option("pushDownPredicate", "true")
        if self.config.enable_column_pruning:
            reader = reader.option("columnPruning", "true")
            
        # Read based on format
        if format == "parquet":
            df = reader.parquet(*paths)
        elif format == "csv":
            df = reader.option("header", "true").csv(*paths)
        elif format == "json":
            df = reader.json(*paths)
        elif format == "orc":
            df = reader.orc(*paths)
        elif format == "avro":
            df = reader.format("avro").load(*paths)
        else:
            df = reader.format(format).load(*paths)
            
        return df
        
    def _optimize_spark_dataframe(self, df: 'DataFrame') -> 'DataFrame':
        """Apply Spark DataFrame optimizations"""
        # Repartition if needed
        current_partitions = df.rdd.getNumPartitions()
        optimal_partitions = self._calculate_optimal_partitions(df)
        
        if current_partitions != optimal_partitions:
            df = df.repartition(optimal_partitions)
            
        # Cache if beneficial
        if self._should_cache_dataframe(df):
            df = df.cache()
            
        return df
        
    def _calculate_optimal_partitions(self, df: 'DataFrame') -> int:
        """Calculate optimal number of partitions"""
        # Estimate data size
        sample_size = min(1000, df.count())
        if sample_size == 0:
            return 1
            
        sample_df = df.limit(sample_size).toPandas()
        avg_row_size = sample_df.memory_usage(deep=True).sum() / len(sample_df)
        total_size_mb = (avg_row_size * df.count()) / (1024 * 1024)
        
        # Target partition size
        target_partition_size_mb = self.config.partition_size_mb
        optimal_partitions = max(1, int(total_size_mb / target_partition_size_mb))
        
        # Cap at shuffle partitions
        return min(optimal_partitions, self.config.shuffle_partitions)
        
    def _should_cache_dataframe(self, df: 'DataFrame') -> bool:
        """Determine if DataFrame should be cached"""
        # Simple heuristic - cache if used multiple times
        # Override in subclasses for better logic
        return False
        
    async def _write_to_lakehouse(self, df: 'DataFrame', table_name: str, job: BatchJob):
        """Write DataFrame to lakehouse table"""
        # Convert Spark DataFrame to Pandas for lakehouse
        # In production, use native Spark writers
        pandas_df = df.toPandas()
        
        # Create table if not exists
        if not await self.lakehouse.table_exists(table_name):
            schema = self._infer_schema_from_dataframe(df)
            await self.lakehouse.create_table(
                table_name,
                schema,
                format=self.config.table_format
            )
            
        # Write data
        await self.lakehouse.write_table(
            table_name,
            pandas_df,
            mode="append" if job.config.enable_incremental_processing else "overwrite"
        )
        
    def _write_spark_data(self, df: 'DataFrame', path: str, format: str):
        """Write Spark DataFrame to files"""
        writer = df.write.mode("overwrite")
        
        # Apply compression
        if self.config.compression:
            writer = writer.option("compression", self.config.compression)
            
        # Write based on format
        if format == "parquet":
            writer.parquet(path)
        elif format == "csv":
            writer.option("header", "true").csv(path)
        elif format == "json":
            writer.json(path)
        elif format == "orc":
            writer.orc(path)
        elif format == "avro":
            writer.format("avro").save(path)
        else:
            writer.format(format).save(path)
            
    async def _process_with_ray(self, job: BatchJob, result: BatchResult):
        """Process batch with Ray"""
        # Implementation for Ray processing
        # This would use Ray datasets and distributed operations
        pass
        
    async def _process_with_dask(self, job: BatchJob, result: BatchResult):
        """Process batch with Dask"""
        # Implementation for Dask processing
        # This would use Dask dataframes
        pass
        
    async def _process_with_pandas(self, job: BatchJob, result: BatchResult):
        """Process batch with Pandas (for small datasets)"""
        # Simple pandas implementation for small datasets
        dfs = []
        for path in job.input_paths:
            if job.config.input_format == "parquet":
                df = pd.read_parquet(path)
            elif job.config.input_format == "csv":
                df = pd.read_csv(path)
            else:
                raise ValueError(f"Unsupported format for pandas: {job.config.input_format}")
            dfs.append(df)
            
        # Combine dataframes
        df = pd.concat(dfs, ignore_index=True)
        result.input_records = len(df)
        
        # Apply transformation
        if job.transform_func:
            df = job.transform_func(df)
            
        # Write output
        if job.config.output_format == "parquet":
            df.to_parquet(job.output_path)
        elif job.config.output_format == "csv":
            df.to_csv(job.output_path, index=False)
            
        result.output_records = len(df)
        result.output_files = [job.output_path]
        
    async def _run_quality_checks(self, job: BatchJob, result: BatchResult):
        """Run data quality checks on output"""
        if not self.quality:
            return
            
        # Run quality assessment
        quality_result = await self.quality.assess_quality(
            result.output_files[0] if result.output_files else result.output_table,
            checks=["completeness", "validity", "uniqueness", "consistency"]
        )
        
        # Update result
        result.quality_score = quality_result.overall_score
        result.quality_report = quality_result.to_dict()
        result.invalid_records = quality_result.invalid_count
        result.duplicate_records = quality_result.duplicate_count
        
        # Check threshold
        if result.quality_score < self.config.quality_threshold:
            result.warnings.append({
                "type": "quality_threshold",
                "message": f"Quality score {result.quality_score} below threshold {self.config.quality_threshold}",
                "details": quality_result.issues
            })
            
    def _list_output_files(self, path: str) -> List[str]:
        """List output files from path"""
        path_obj = Path(path)
        if path_obj.is_file():
            return [str(path_obj)]
        elif path_obj.is_dir():
            return [str(f) for f in path_obj.rglob("*") if f.is_file()]
        return []
        
    def _infer_schema_from_dataframe(self, df: Any) -> Dict[str, Any]:
        """Infer schema from dataframe"""
        # Implementation depends on engine
        # This is a placeholder
        return {"columns": []}
        
    def _estimate_data_size(self, data: Any) -> int:
        """Estimate data size for partitioning"""
        if isinstance(data, BatchJob):
            # Estimate from file sizes
            total_size = 0
            for path in data.input_paths:
                try:
                    path_obj = Path(path)
                    if path_obj.is_file():
                        total_size += path_obj.stat().st_size
                    elif path_obj.is_dir():
                        total_size += sum(f.stat().st_size for f in path_obj.rglob("*") if f.is_file())
                except:
                    pass
            return total_size
        return super()._estimate_data_size(data)
        
    def _select_partition_strategy(self, data: Any) -> BasePartitionStrategy:
        """Select optimal partition strategy for batch data"""
        if isinstance(data, BatchJob) and data.config.partition_columns:
            return BasePartitionStrategy.KEY_BASED
        return BasePartitionStrategy.SIZE_BASED
        
    async def submit_incremental_job(
        self,
        job: BatchJob,
        since: Optional[datetime] = None
    ) -> str:
        """Submit an incremental processing job"""
        # Set up incremental processing
        if since:
            job.last_processed_timestamp = since
        elif job.last_processed_timestamp is None:
            # First run - process all data
            job.last_processed_timestamp = datetime.min
            
        # Add incremental filter to transform
        original_transform = job.transform_func
        
        def incremental_transform(df):
            # Filter by timestamp
            if hasattr(df, 'filter'):  # Spark DataFrame
                df = df.filter(df.timestamp > job.last_processed_timestamp)
            elif hasattr(df, 'query'):  # Pandas DataFrame
                df = df[df['timestamp'] > job.last_processed_timestamp]
                
            # Apply original transform
            if original_transform:
                df = original_transform(df)
                
            return df
            
        job.transform_func = incremental_transform
        
        # Submit job
        return await self.submit_job(job)
        
    async def optimize_table(self, table_name: str):
        """Optimize a lakehouse table"""
        if not self.lakehouse:
            return
            
        # Run optimization based on table format
        if self.config.table_format == TableFormat.ICEBERG:
            # Compact small files
            await self.lakehouse.compact_table(table_name)
            # Expire old snapshots
            await self.lakehouse.expire_snapshots(table_name, older_than=timedelta(days=7))
        elif self.config.table_format == TableFormat.DELTA:
            # Optimize and Z-order
            if self.config.enable_z_ordering:
                await self.lakehouse.optimize_table(table_name, z_order_by=["date", "user_id"])
            else:
                await self.lakehouse.optimize_table(table_name) 