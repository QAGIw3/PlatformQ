"""
Apache Spark Client Integration

Provides high-level client for Apache Spark operations.
"""

import logging
from typing import Any, Dict, List, Optional, Union, Callable
from dataclasses import dataclass, field
from datetime import datetime
import os

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import StructType, StructField
from pyspark.sql.functions import col, lit, when, count, sum as spark_sum, avg, max as spark_max, min as spark_min
from pyspark.conf import SparkConf
from pyspark.sql.streaming import StreamingQuery

logger = logging.getLogger(__name__)


@dataclass
class SparkConfig:
    """Configuration for Spark client"""
    app_name: str = "DataIntelligenceSuite"
    master: str = "local[*]"
    
    # Memory settings
    driver_memory: str = "2g"
    executor_memory: str = "2g"
    executor_instances: int = 2
    executor_cores: int = 2
    
    # Spark settings
    spark_home: Optional[str] = None
    hadoop_home: Optional[str] = None
    
    # Additional configs
    configs: Dict[str, str] = field(default_factory=dict)
    
    # Common configurations
    enable_hive_support: bool = False
    enable_delta_lake: bool = False
    enable_iceberg: bool = False
    
    # Checkpoint directory
    checkpoint_dir: str = "/tmp/spark-checkpoints"
    
    # UI settings
    ui_enabled: bool = True
    ui_port: int = 4040


@dataclass
class JobResult:
    """Result of a Spark job"""
    job_id: str
    status: str
    start_time: datetime
    end_time: Optional[datetime] = None
    duration_seconds: Optional[float] = None
    rows_processed: Optional[int] = None
    error: Optional[str] = None
    metrics: Dict[str, Any] = field(default_factory=dict)


class SparkClient:
    """
    High-level client for Apache Spark operations.
    
    Features:
    - Batch processing
    - Stream processing
    - SQL queries
    - DataFrame operations
    - ML pipeline integration
    - Delta Lake support
    """
    
    def __init__(self, config: SparkConfig):
        self.config = config
        self._spark: Optional[SparkSession] = None
        self._streaming_queries: Dict[str, StreamingQuery] = {}
        
    def connect(self):
        """Create Spark session"""
        try:
            # Build Spark configuration
            conf = SparkConf()
            conf.setAppName(self.config.app_name)
            conf.setMaster(self.config.master)
            
            # Set memory configurations
            conf.set("spark.driver.memory", self.config.driver_memory)
            conf.set("spark.executor.memory", self.config.executor_memory)
            conf.set("spark.executor.instances", str(self.config.executor_instances))
            conf.set("spark.executor.cores", str(self.config.executor_cores))
            
            # Set UI settings
            if self.config.ui_enabled:
                conf.set("spark.ui.enabled", "true")
                conf.set("spark.ui.port", str(self.config.ui_port))
            else:
                conf.set("spark.ui.enabled", "false")
                
            # Set checkpoint directory
            conf.set("spark.sql.streaming.checkpointLocation", self.config.checkpoint_dir)
            
            # Apply additional configurations
            for key, value in self.config.configs.items():
                conf.set(key, value)
                
            # Set environment variables if provided
            if self.config.spark_home:
                os.environ["SPARK_HOME"] = self.config.spark_home
            if self.config.hadoop_home:
                os.environ["HADOOP_HOME"] = self.config.hadoop_home
                
            # Build SparkSession
            builder = SparkSession.builder.config(conf=conf)
            
            # Enable Hive support if requested
            if self.config.enable_hive_support:
                builder = builder.enableHiveSupport()
                
            # Configure Delta Lake if requested
            if self.config.enable_delta_lake:
                builder = builder.config(
                    "spark.sql.extensions",
                    "io.delta.sql.DeltaSparkSessionExtension"
                ).config(
                    "spark.sql.catalog.spark_catalog",
                    "org.apache.spark.sql.delta.catalog.DeltaCatalog"
                )
                
            # Configure Iceberg if requested
            if self.config.enable_iceberg:
                builder = builder.config(
                    "spark.sql.extensions",
                    "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions"
                ).config(
                    "spark.sql.catalog.spark_catalog",
                    "org.apache.iceberg.spark.SparkSessionCatalog"
                ).config(
                    "spark.sql.catalog.spark_catalog.type",
                    "hive"
                )
                
            # Create session
            self._spark = builder.getOrCreate()
            
            # Set log level
            self._spark.sparkContext.setLogLevel("WARN")
            
            logger.info(f"Connected to Spark: {self.config.master}")
            
        except Exception as e:
            logger.error(f"Failed to create Spark session: {e}")
            raise
            
    def disconnect(self):
        """Stop Spark session"""
        if self._spark:
            # Stop all streaming queries
            for query in self._streaming_queries.values():
                if query.isActive:
                    query.stop()
            self._streaming_queries.clear()
            
            # Stop Spark session
            self._spark.stop()
            self._spark = None
            logger.info("Disconnected from Spark")
            
    @property
    def spark(self) -> SparkSession:
        """Get Spark session"""
        if not self._spark:
            raise RuntimeError("Not connected to Spark")
        return self._spark
        
    # DataFrame operations
    
    def read_csv(
        self,
        path: str,
        header: bool = True,
        inferSchema: bool = True,
        delimiter: str = ",",
        quote: str = '"',
        escape: str = "\\",
        **options
    ) -> DataFrame:
        """Read CSV file(s)"""
        reader = self.spark.read.format("csv") \
            .option("header", header) \
            .option("inferSchema", inferSchema) \
            .option("delimiter", delimiter) \
            .option("quote", quote) \
            .option("escape", escape)
            
        for key, value in options.items():
            reader = reader.option(key, value)
            
        return reader.load(path)
        
    def read_json(
        self,
        path: str,
        multiLine: bool = False,
        **options
    ) -> DataFrame:
        """Read JSON file(s)"""
        reader = self.spark.read.format("json") \
            .option("multiLine", multiLine)
            
        for key, value in options.items():
            reader = reader.option(key, value)
            
        return reader.load(path)
        
    def read_parquet(self, path: str, **options) -> DataFrame:
        """Read Parquet file(s)"""
        reader = self.spark.read.format("parquet")
        
        for key, value in options.items():
            reader = reader.option(key, value)
            
        return reader.load(path)
        
    def read_delta(self, path: str, version: Optional[int] = None) -> DataFrame:
        """Read Delta table"""
        if not self.config.enable_delta_lake:
            raise RuntimeError("Delta Lake not enabled")
            
        if version is not None:
            return self.spark.read.format("delta") \
                .option("versionAsOf", version) \
                .load(path)
        else:
            return self.spark.read.format("delta").load(path)
            
    def read_table(self, table_name: str) -> DataFrame:
        """Read Hive/Spark table"""
        return self.spark.table(table_name)
        
    def write_csv(
        self,
        df: DataFrame,
        path: str,
        mode: str = "overwrite",
        header: bool = True,
        delimiter: str = ",",
        **options
    ):
        """Write DataFrame to CSV"""
        writer = df.write.mode(mode) \
            .option("header", header) \
            .option("delimiter", delimiter)
            
        for key, value in options.items():
            writer = writer.option(key, value)
            
        writer.csv(path)
        
    def write_parquet(
        self,
        df: DataFrame,
        path: str,
        mode: str = "overwrite",
        compression: str = "snappy",
        **options
    ):
        """Write DataFrame to Parquet"""
        writer = df.write.mode(mode) \
            .option("compression", compression)
            
        for key, value in options.items():
            writer = writer.option(key, value)
            
        writer.parquet(path)
        
    def write_delta(
        self,
        df: DataFrame,
        path: str,
        mode: str = "overwrite",
        partitionBy: Optional[List[str]] = None,
        **options
    ):
        """Write DataFrame to Delta table"""
        if not self.config.enable_delta_lake:
            raise RuntimeError("Delta Lake not enabled")
            
        writer = df.write.mode(mode)
        
        if partitionBy:
            writer = writer.partitionBy(*partitionBy)
            
        for key, value in options.items():
            writer = writer.option(key, value)
            
        writer.format("delta").save(path)
        
    def create_table(
        self,
        df: DataFrame,
        table_name: str,
        mode: str = "overwrite",
        partitionBy: Optional[List[str]] = None
    ):
        """Create table from DataFrame"""
        writer = df.write.mode(mode)
        
        if partitionBy:
            writer = writer.partitionBy(*partitionBy)
            
        writer.saveAsTable(table_name)
        
    # SQL operations
    
    def sql(self, query: str) -> DataFrame:
        """Execute SQL query"""
        return self.spark.sql(query)
        
    def register_temp_view(self, df: DataFrame, view_name: str):
        """Register DataFrame as temporary view"""
        df.createOrReplaceTempView(view_name)
        
    def register_global_temp_view(self, df: DataFrame, view_name: str):
        """Register DataFrame as global temporary view"""
        df.createOrReplaceGlobalTempView(view_name)
        
    # Stream processing
    
    def read_stream_kafka(
        self,
        bootstrap_servers: str,
        topics: Union[str, List[str]],
        starting_offsets: str = "latest",
        **options
    ) -> DataFrame:
        """Read stream from Kafka"""
        reader = self.spark.readStream.format("kafka") \
            .option("kafka.bootstrap.servers", bootstrap_servers) \
            .option("startingOffsets", starting_offsets)
            
        if isinstance(topics, str):
            reader = reader.option("subscribe", topics)
        else:
            reader = reader.option("subscribe", ",".join(topics))
            
        for key, value in options.items():
            reader = reader.option(key, value)
            
        return reader.load()
        
    def read_stream_file(
        self,
        path: str,
        format: str = "json",
        schema: Optional[StructType] = None,
        **options
    ) -> DataFrame:
        """Read stream from files"""
        reader = self.spark.readStream.format(format)
        
        if schema:
            reader = reader.schema(schema)
            
        for key, value in options.items():
            reader = reader.option(key, value)
            
        return reader.load(path)
        
    def write_stream(
        self,
        df: DataFrame,
        output_mode: str = "append",
        format: str = "console",
        query_name: Optional[str] = None,
        trigger_interval: Optional[str] = None,
        checkpoint_location: Optional[str] = None,
        **options
    ) -> StreamingQuery:
        """Write streaming DataFrame"""
        writer = df.writeStream.outputMode(output_mode).format(format)
        
        if query_name:
            writer = writer.queryName(query_name)
            
        if trigger_interval:
            from pyspark.sql.streaming import Trigger
            writer = writer.trigger(processingTime=trigger_interval)
            
        if checkpoint_location:
            writer = writer.option("checkpointLocation", checkpoint_location)
            
        for key, value in options.items():
            writer = writer.option(key, value)
            
        query = writer.start()
        
        # Track query
        if query_name:
            self._streaming_queries[query_name] = query
            
        return query
        
    def stop_stream(self, query_name: str):
        """Stop streaming query"""
        if query_name in self._streaming_queries:
            query = self._streaming_queries[query_name]
            if query.isActive:
                query.stop()
            del self._streaming_queries[query_name]
            
    # DataFrame transformations
    
    def transform(
        self,
        df: DataFrame,
        transformations: List[Callable[[DataFrame], DataFrame]]
    ) -> DataFrame:
        """Apply series of transformations"""
        result = df
        for transform in transformations:
            result = transform(result)
        return result
        
    def aggregate(
        self,
        df: DataFrame,
        group_by: List[str],
        aggregations: Dict[str, str]
    ) -> DataFrame:
        """Perform aggregations"""
        agg_exprs = []
        
        for col_name, agg_func in aggregations.items():
            if agg_func == "count":
                agg_exprs.append(count(col_name).alias(f"{col_name}_{agg_func}"))
            elif agg_func == "sum":
                agg_exprs.append(spark_sum(col_name).alias(f"{col_name}_{agg_func}"))
            elif agg_func == "avg":
                agg_exprs.append(avg(col_name).alias(f"{col_name}_{agg_func}"))
            elif agg_func == "max":
                agg_exprs.append(spark_max(col_name).alias(f"{col_name}_{agg_func}"))
            elif agg_func == "min":
                agg_exprs.append(spark_min(col_name).alias(f"{col_name}_{agg_func}"))
                
        return df.groupBy(*group_by).agg(*agg_exprs)
        
    def join(
        self,
        left: DataFrame,
        right: DataFrame,
        on: Union[str, List[str]],
        how: str = "inner"
    ) -> DataFrame:
        """Join two DataFrames"""
        return left.join(right, on=on, how=how)
        
    # Job execution
    
    def execute_job(
        self,
        job_func: Callable[[SparkSession], Any],
        job_id: Optional[str] = None
    ) -> JobResult:
        """Execute a Spark job"""
        if not job_id:
            import uuid
            job_id = str(uuid.uuid4())
            
        result = JobResult(
            job_id=job_id,
            status="running",
            start_time=datetime.utcnow()
        )
        
        try:
            # Execute job
            output = job_func(self.spark)
            
            # Update result
            result.status = "completed"
            result.end_time = datetime.utcnow()
            result.duration_seconds = (
                result.end_time - result.start_time
            ).total_seconds()
            
            # Try to get row count if output is DataFrame
            if isinstance(output, DataFrame):
                result.rows_processed = output.count()
                
        except Exception as e:
            result.status = "failed"
            result.end_time = datetime.utcnow()
            result.duration_seconds = (
                result.end_time - result.start_time
            ).total_seconds()
            result.error = str(e)
            logger.error(f"Job {job_id} failed: {e}")
            
        return result
        
    # Utilities
    
    def get_spark_conf(self) -> Dict[str, str]:
        """Get current Spark configuration"""
        return dict(self.spark.sparkContext.getConf().getAll())
        
    def set_spark_conf(self, key: str, value: str):
        """Set Spark configuration"""
        self.spark.conf.set(key, value)
        
    def get_active_streams(self) -> List[str]:
        """Get active streaming queries"""
        return [
            name for name, query in self._streaming_queries.items()
            if query.isActive
        ]
        
    def cache_dataframe(self, df: DataFrame) -> DataFrame:
        """Cache DataFrame in memory"""
        return df.cache()
        
    def unpersist_dataframe(self, df: DataFrame):
        """Remove DataFrame from cache"""
        df.unpersist()
        
    def checkpoint_dataframe(self, df: DataFrame) -> DataFrame:
        """Checkpoint DataFrame to disk"""
        return df.checkpoint()
        
    def repartition(
        self,
        df: DataFrame,
        num_partitions: Optional[int] = None,
        columns: Optional[List[str]] = None
    ) -> DataFrame:
        """Repartition DataFrame"""
        if columns:
            return df.repartition(*columns) if not num_partitions else df.repartition(num_partitions, *columns)
        else:
            return df.repartition(num_partitions) if num_partitions else df
            
    def coalesce(self, df: DataFrame, num_partitions: int) -> DataFrame:
        """Coalesce DataFrame partitions"""
        return df.coalesce(num_partitions) 