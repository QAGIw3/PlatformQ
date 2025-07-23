"""
Batch Processing Implementation for DataIntelligenceSuite

Provides scalable batch processing capabilities with Spark integration.
"""

import asyncio
import logging
from typing import Any, Dict, List, Optional, Callable, Union
from datetime import datetime, timedelta
from dataclasses import dataclass, field
from enum import Enum
import uuid

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import StructType
import pandas as pd

from .base_processor import BaseProcessor, ProcessorConfig, ProcessingResult, ProcessingStatus
from ...monitoring import MetricsCollector

logger = logging.getLogger(__name__)


class PartitionStrategy(Enum):
    """Partition strategies for batch processing"""
    HASH = "hash"
    RANGE = "range"
    ROUND_ROBIN = "round_robin"
    CUSTOM = "custom"


@dataclass
class BatchConfig(ProcessorConfig):
    """Configuration for batch processing"""
    batch_size: int = 1000
    partition_strategy: PartitionStrategy = PartitionStrategy.HASH
    partition_columns: List[str] = field(default_factory=list)
    num_partitions: Optional[int] = None
    
    # Spark configuration
    spark_master: str = "local[*]"
    spark_app_name: str = "DataIntelligenceBatch"
    spark_config: Dict[str, str] = field(default_factory=dict)
    
    # Input/Output
    input_format: str = "parquet"
    output_format: str = "parquet"
    compression: Optional[str] = "snappy"
    
    # Performance
    adaptive_query_execution: bool = True
    broadcast_threshold_mb: int = 10
    shuffle_partitions: int = 200
    
    # Checkpointing
    checkpoint_location: Optional[str] = None
    checkpoint_interval: int = 10


@dataclass
class BatchJob:
    """Batch job information"""
    job_id: str
    name: str
    input_path: str
    output_path: str
    transform_func: Optional[Callable] = None
    config: BatchConfig = field(default_factory=BatchConfig)
    created_at: datetime = field(default_factory=datetime.utcnow)
    
    # Runtime state
    spark_job_id: Optional[str] = None
    partitions_processed: int = 0
    total_partitions: Optional[int] = None


@dataclass
class BatchResult(ProcessingResult):
    """Result of batch processing"""
    input_records: int = 0
    output_records: int = 0
    partitions_processed: int = 0
    
    # Data quality
    invalid_records: int = 0
    duplicate_records: int = 0
    
    # Performance metrics
    read_time_ms: Optional[float] = None
    transform_time_ms: Optional[float] = None
    write_time_ms: Optional[float] = None
    
    # Output details
    output_files: List[str] = field(default_factory=list)
    output_size_bytes: Optional[int] = None


class BatchProcessor(BaseProcessor):
    """
    Batch processor for large-scale data processing.
    
    Features:
    - Spark-based processing
    - Multiple partition strategies
    - Automatic checkpointing
    - Data quality validation
    - Performance optimization
    """
    
    def __init__(
        self,
        config: BatchConfig,
        spark_session: Optional[SparkSession] = None,
        **kwargs
    ):
        super().__init__(config, **kwargs)
        self.config: BatchConfig = config
        self.spark = spark_session
        self._jobs: Dict[str, BatchJob] = {}
        
    async def initialize(self):
        """Initialize batch processor"""
        logger.info(f"Initializing batch processor: {self.config.name}")
        
        # Create Spark session if not provided
        if not self.spark:
            self.spark = self._create_spark_session()
            
        # Configure Spark optimizations
        self._configure_spark_optimizations()
        
    def _create_spark_session(self) -> SparkSession:
        """Create Spark session with configuration"""
        builder = SparkSession.builder \
            .master(self.config.spark_master) \
            .appName(self.config.spark_app_name)
            
        # Apply custom Spark configuration
        for key, value in self.config.spark_config.items():
            builder = builder.config(key, value)
            
        # Standard optimizations
        builder = builder \
            .config("spark.sql.adaptive.enabled", str(self.config.adaptive_query_execution).lower()) \
            .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
            .config("spark.sql.autoBroadcastJoinThreshold", f"{self.config.broadcast_threshold_mb}MB") \
            .config("spark.sql.shuffle.partitions", str(self.config.shuffle_partitions))
            
        return builder.getOrCreate()
        
    def _configure_spark_optimizations(self):
        """Configure Spark optimizations"""
        if self.config.compression:
            self.spark.conf.set("spark.sql.parquet.compression.codec", self.config.compression)
            
        # Enable dynamic allocation if configured
        if self.config.spark_config.get("spark.dynamicAllocation.enabled", "false") == "true":
            self.spark.sparkContext.setLogLevel("INFO")
            
    async def process(self, data: Any, job_id: Optional[str] = None) -> ProcessingResult:
        """Process batch data"""
        job_id = job_id or str(uuid.uuid4())
        start_time = datetime.utcnow()
        
        try:
            # Create batch job
            if isinstance(data, BatchJob):
                job = data
                job.job_id = job_id
            else:
                job = BatchJob(
                    job_id=job_id,
                    name=f"batch_{job_id}",
                    input_path=str(data),
                    output_path=f"/tmp/output_{job_id}",
                    config=self.config
                )
                
            self._jobs[job_id] = job
            
            # Process the batch
            result = await self._process_batch(job)
            
            # Record metrics
            self._record_metrics(result)
            
            return result
            
        except Exception as e:
            logger.error(f"Batch processing failed: {e}")
            return BatchResult(
                job_id=job_id,
                status=ProcessingStatus.FAILED,
                started_at=start_time,
                completed_at=datetime.utcnow(),
                errors=[{"error": str(e), "type": type(e).__name__}]
            )
            
    async def _process_batch(self, job: BatchJob) -> BatchResult:
        """Process a batch job"""
        result = BatchResult(
            job_id=job.job_id,
            status=ProcessingStatus.RUNNING,
            started_at=datetime.utcnow()
        )
        
        try:
            # Read input data
            read_start = datetime.utcnow()
            df = await self._read_input(job)
            result.read_time_ms = (datetime.utcnow() - read_start).total_seconds() * 1000
            result.input_records = df.count()
            
            # Apply transformations
            transform_start = datetime.utcnow()
            df = await self._apply_transformations(df, job)
            result.transform_time_ms = (datetime.utcnow() - transform_start).total_seconds() * 1000
            
            # Validate data quality
            quality_result = await self._validate_quality(df)
            result.invalid_records = quality_result.get("invalid_count", 0)
            result.duplicate_records = quality_result.get("duplicate_count", 0)
            
            # Write output
            write_start = datetime.utcnow()
            output_info = await self._write_output(df, job)
            result.write_time_ms = (datetime.utcnow() - write_start).total_seconds() * 1000
            result.output_records = output_info["record_count"]
            result.output_files = output_info["files"]
            result.output_size_bytes = output_info.get("size_bytes")
            
            # Update result
            result.status = ProcessingStatus.COMPLETED
            result.completed_at = datetime.utcnow()
            result.records_processed = result.input_records
            result.processing_time_ms = (result.completed_at - result.started_at).total_seconds() * 1000
            
            if result.processing_time_ms > 0:
                result.throughput_records_per_sec = result.records_processed / (result.processing_time_ms / 1000)
                
            return result
            
        except Exception as e:
            logger.error(f"Batch processing error: {e}")
            result.status = ProcessingStatus.FAILED
            result.completed_at = datetime.utcnow()
            result.errors.append({"error": str(e), "type": type(e).__name__})
            return result
            
    async def _read_input(self, job: BatchJob) -> DataFrame:
        """Read input data"""
        reader = self.spark.read
        
        # Configure reader based on format
        if job.config.input_format == "parquet":
            df = reader.parquet(job.input_path)
        elif job.config.input_format == "json":
            df = reader.json(job.input_path)
        elif job.config.input_format == "csv":
            df = reader.csv(job.input_path, header=True, inferSchema=True)
        elif job.config.input_format == "avro":
            df = reader.format("avro").load(job.input_path)
        else:
            raise ValueError(f"Unsupported input format: {job.config.input_format}")
            
        # Apply partitioning if configured
        if job.config.num_partitions:
            df = df.repartition(job.config.num_partitions)
            
        return df
        
    async def _apply_transformations(self, df: DataFrame, job: BatchJob) -> DataFrame:
        """Apply transformations to data"""
        # Apply custom transformation function if provided
        if job.transform_func:
            if asyncio.iscoroutinefunction(job.transform_func):
                df = await job.transform_func(df)
            else:
                df = job.transform_func(df)
                
        # Apply partitioning strategy
        if job.config.partition_columns:
            if job.config.partition_strategy == PartitionStrategy.HASH:
                df = df.repartition(*job.config.partition_columns)
            elif job.config.partition_strategy == PartitionStrategy.RANGE:
                df = df.repartitionByRange(*job.config.partition_columns)
                
        # Cache if needed for multiple operations
        if job.config.cache_results:
            df = df.cache()
            
        return df
        
    async def _validate_quality(self, df: DataFrame) -> Dict[str, Any]:
        """Validate data quality"""
        quality_result = {}
        
        # Check for nulls in required columns
        # This is a simplified version - actual implementation would be more comprehensive
        null_counts = {}
        for col in df.columns:
            null_count = df.filter(df[col].isNull()).count()
            if null_count > 0:
                null_counts[col] = null_count
                
        quality_result["null_counts"] = null_counts
        quality_result["invalid_count"] = sum(null_counts.values())
        
        # Check for duplicates (simplified)
        total_count = df.count()
        distinct_count = df.distinct().count()
        quality_result["duplicate_count"] = total_count - distinct_count
        
        return quality_result
        
    async def _write_output(self, df: DataFrame, job: BatchJob) -> Dict[str, Any]:
        """Write output data"""
        writer = df.write.mode("overwrite")
        
        # Configure writer based on format
        if job.config.output_format == "parquet":
            if job.config.compression:
                writer = writer.option("compression", job.config.compression)
            writer.parquet(job.output_path)
        elif job.config.output_format == "json":
            writer.json(job.output_path)
        elif job.config.output_format == "csv":
            writer.csv(job.output_path, header=True)
        elif job.config.output_format == "avro":
            writer.format("avro").save(job.output_path)
        else:
            raise ValueError(f"Unsupported output format: {job.config.output_format}")
            
        # Get output information
        output_info = {
            "record_count": df.count(),
            "files": self._get_output_files(job.output_path),
            "size_bytes": self._get_output_size(job.output_path)
        }
        
        return output_info
        
    def _get_output_files(self, path: str) -> List[str]:
        """Get list of output files"""
        # Simplified - actual implementation would use Hadoop FileSystem API
        return [path]
        
    def _get_output_size(self, path: str) -> Optional[int]:
        """Get total size of output files"""
        # Simplified - actual implementation would calculate actual size
        return None
        
    async def submit_batch_job(
        self,
        input_path: str,
        output_path: str,
        transform_func: Optional[Callable] = None,
        job_name: Optional[str] = None
    ) -> str:
        """Submit a batch job for processing"""
        job = BatchJob(
            job_id=str(uuid.uuid4()),
            name=job_name or f"batch_job_{datetime.utcnow().strftime('%Y%m%d_%H%M%S')}",
            input_path=input_path,
            output_path=output_path,
            transform_func=transform_func,
            config=self.config
        )
        
        # Submit job asynchronously
        job_id = await self.submit_job(job)
        return job_id
        
    async def get_batch_status(self, job_id: str) -> Optional[Dict[str, Any]]:
        """Get status of a batch job"""
        job = self._jobs.get(job_id)
        if not job:
            return None
            
        result = await self.get_job_status(job_id)
        if not result:
            return None
            
        return {
            "job_id": job_id,
            "name": job.name,
            "status": result.status.value,
            "progress": {
                "input_records": result.input_records,
                "output_records": result.output_records,
                "partitions_processed": result.partitions_processed
            },
            "performance": {
                "read_time_ms": result.read_time_ms,
                "transform_time_ms": result.transform_time_ms,
                "write_time_ms": result.write_time_ms,
                "throughput_records_per_sec": result.throughput_records_per_sec
            }
        }
        
    async def shutdown(self):
        """Shutdown batch processor"""
        logger.info("Shutting down batch processor")
        
        # Stop Spark session
        if self.spark:
            self.spark.stop()
            
        # Clear jobs
        self._jobs.clear() 