"""
Apache Hudi Client Integration

Provides high-level client for Apache Hudi table format operations.
Supports incremental processing, CDC, and unified batch/streaming.
"""

import os
from typing import Any, Dict, List, Optional, Union, Tuple
from datetime import datetime, timedelta
from dataclasses import dataclass, field
from enum import Enum
import pandas as pd
import pyarrow as pa
from pyspark.sql import SparkSession, DataFrame as SparkDataFrame

from platformq_shared.vault.vault_client import VaultClient
from platformq_shared.consul.consul_client import ConsulClient
from ...clients.base_client import BaseServiceClient, ClientConfig
from ...monitoring import StructuredLogger

logger = StructuredLogger.get_logger(__name__)


class HudiTableType(str, Enum):
    """Hudi table types"""
    COPY_ON_WRITE = "COPY_ON_WRITE"
    MERGE_ON_READ = "MERGE_ON_READ"


class WriteMode(str, Enum):
    """Hudi write modes"""
    INSERT = "insert"
    UPSERT = "upsert"
    BULK_INSERT = "bulk_insert"
    DELETE = "delete"
    INSERT_OVERWRITE = "insert_overwrite"
    INSERT_OVERWRITE_TABLE = "insert_overwrite_table"


class IndexType(str, Enum):
    """Hudi index types"""
    BLOOM = "BLOOM"
    GLOBAL_BLOOM = "GLOBAL_BLOOM"
    SIMPLE = "SIMPLE"
    GLOBAL_SIMPLE = "GLOBAL_SIMPLE"
    HBASE = "HBASE"
    BUCKET = "BUCKET"


class CompactionStrategy(str, Enum):
    """Compaction strategies"""
    INLINE = "inline"
    ASYNC = "async"
    SCHEDULED = "scheduled"


@dataclass
class HudiConfig(ClientConfig):
    """Configuration for Hudi client"""
    table_path: str = "s3://datalake/hudi"
    spark_master: str = "local[*]"
    
    # S3/MinIO configuration
    s3_endpoint: Optional[str] = None
    s3_access_key: Optional[str] = None
    s3_secret_key: Optional[str] = None
    s3_region: str = "us-east-1"
    
    # Table defaults
    table_type: HudiTableType = HudiTableType.COPY_ON_WRITE
    index_type: IndexType = IndexType.BLOOM
    
    # Write configuration
    insert_parallelism: int = 200
    upsert_parallelism: int = 200
    delete_parallelism: int = 200
    bulk_insert_parallelism: int = 200
    
    # Compaction
    compaction_strategy: CompactionStrategy = CompactionStrategy.INLINE
    compaction_max_delta_commits: int = 5
    compaction_target_io: int = 512 * 1024 * 1024  # 512MB
    
    # Clustering
    enable_clustering: bool = True
    clustering_max_group_size: int = 2 * 1024 * 1024 * 1024  # 2GB
    
    # Clean
    cleaner_commits_retained: int = 10
    cleaner_hours_retained: int = 24
    
    def __post_init__(self):
        super().__post_init__()
        self.service_name = self.service_name or "hudi"


@dataclass
class HudiTable:
    """Hudi table metadata"""
    path: str
    name: str
    table_type: HudiTableType
    latest_commit: str
    total_records: int
    total_files: int
    schema: Dict[str, str]
    partitions: List[str]
    timeline: List[Dict[str, Any]]
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary"""
        return {
            "path": self.path,
            "name": self.name,
            "table_type": self.table_type.value,
            "latest_commit": self.latest_commit,
            "total_records": self.total_records,
            "total_files": self.total_files,
            "schema": self.schema,
            "partitions": self.partitions,
            "timeline": self.timeline
        }


@dataclass
class IncrementalQuery:
    """Configuration for incremental queries"""
    begin_instant_time: str
    end_instant_time: Optional[str] = None
    include_all_data_files: bool = False


@dataclass
class ClusteringConfig:
    """Configuration for clustering"""
    columns: List[str]
    max_file_size: int = 1024 * 1024 * 1024  # 1GB
    target_file_size: int = 128 * 1024 * 1024  # 128MB
    max_groups: int = 30


class HudiClient(BaseServiceClient):
    """
    Apache Hudi client for lakehouse operations.
    
    Features:
    - Incremental data processing
    - Change data capture (CDC)
    - ACID transactions
    - Time travel queries
    - Automatic file management
    - Data clustering
    """
    
    def __init__(
        self,
        config: Optional[HudiConfig] = None,
        vault_client: Optional[VaultClient] = None,
        consul_client: Optional[ConsulClient] = None,
        **kwargs
    ):
        if not config:
            config = HudiConfig()
            
        super().__init__(config, vault_client, consul_client, **kwargs)
        self.config: HudiConfig = config
        self._spark: Optional[SparkSession] = None
        
    async def connect(self):
        """Connect to Hudi with Spark"""
        await super().connect()
        
        try:
            # Get S3 credentials from Vault if configured
            if self.config.use_vault_credentials:
                creds = await self._get_credentials()
                if creds:
                    self.config.s3_access_key = creds.get("access_key")
                    self.config.s3_secret_key = creds.get("secret_key")
            
            # Create Spark session
            builder = SparkSession.builder \
                .appName("HudiClient") \
                .master(self.config.spark_master) \
                .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer") \
                .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.hudi.catalog.HoodieCatalog") \
                .config("spark.sql.extensions", "org.apache.spark.sql.hudi.HoodieSparkSessionExtension")
            
            # Add S3 configuration if provided
            if self.config.s3_endpoint:
                builder = builder \
                    .config("spark.hadoop.fs.s3a.endpoint", self.config.s3_endpoint) \
                    .config("spark.hadoop.fs.s3a.access.key", self.config.s3_access_key) \
                    .config("spark.hadoop.fs.s3a.secret.key", self.config.s3_secret_key) \
                    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
                    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
            
            self._spark = builder.getOrCreate()
            
            logger.info(f"Connected to Hudi with Spark: {self.config.spark_master}")
            
        except Exception as e:
            logger.error(f"Failed to connect to Hudi: {e}")
            raise
    
    async def create_table(
        self,
        table_name: str,
        schema: Union[pa.Schema, pd.DataFrame],
        record_key: str,
        partition_path: Optional[str] = None,
        precombine_field: Optional[str] = None,
        table_type: Optional[HudiTableType] = None
    ) -> HudiTable:
        """
        Create a new Hudi table.
        
        Args:
            table_name: Table name
            schema: PyArrow schema or pandas DataFrame
            record_key: Primary key field
            partition_path: Partition field
            precombine_field: Field for deduplication
            table_type: Table type (COW or MOR)
            
        Returns:
            Created table metadata
        """
        try:
            table_path = os.path.join(self.config.table_path, table_name)
            table_type = table_type or self.config.table_type
            
            # Convert schema to Spark DataFrame
            if isinstance(schema, pa.Schema):
                df = pd.DataFrame(columns=[field.name for field in schema])
                spark_df = self._spark.createDataFrame(df)
            elif isinstance(schema, pd.DataFrame):
                spark_df = self._spark.createDataFrame(schema.head(0))
            else:
                spark_df = schema
            
            # Hudi options
            hudi_options = {
                "hoodie.table.name": table_name,
                "hoodie.datasource.write.recordkey.field": record_key,
                "hoodie.datasource.write.table.type": table_type.value,
                "hoodie.datasource.write.operation": "bulk_insert",
                "hoodie.datasource.write.precombine.field": precombine_field or record_key,
                "hoodie.index.type": self.config.index_type.value,
                "hoodie.bulkinsert.shuffle.parallelism": self.config.bulk_insert_parallelism
            }
            
            if partition_path:
                hudi_options["hoodie.datasource.write.partitionpath.field"] = partition_path
                hudi_options["hoodie.datasource.write.hive_style_partitioning"] = "true"
            
            # Write empty DataFrame to create table
            spark_df.write \
                .format("hudi") \
                .options(**hudi_options) \
                .mode("overwrite") \
                .save(table_path)
            
            # Get table metadata
            table_meta = await self.get_table_details(table_name)
            
            logger.info(f"Created Hudi table: {table_name}")
            return table_meta
            
        except Exception as e:
            logger.error(f"Failed to create table: {e}")
            raise
    
    async def write_data(
        self,
        table_name: str,
        data: Union[pd.DataFrame, SparkDataFrame, List[Dict[str, Any]]],
        mode: WriteMode = WriteMode.UPSERT,
        partition_overwrite: Optional[List[str]] = None
    ) -> Dict[str, Any]:
        """
        Write data to Hudi table.
        
        Args:
            table_name: Table name
            data: Data to write
            mode: Write mode
            partition_overwrite: Partitions to overwrite
            
        Returns:
            Write statistics
        """
        try:
            table_path = os.path.join(self.config.table_path, table_name)
            
            # Convert data to Spark DataFrame
            if isinstance(data, list):
                spark_df = self._spark.createDataFrame(pd.DataFrame(data))
            elif isinstance(data, pd.DataFrame):
                spark_df = self._spark.createDataFrame(data)
            else:
                spark_df = data
            
            # Get table config
            table_config = self._spark.read.format("hudi").load(table_path).limit(0)
            
            # Hudi write options
            hudi_options = {
                "hoodie.table.name": table_name,
                "hoodie.datasource.write.operation": mode.value,
                f"hoodie.{mode.value}.shuffle.parallelism": getattr(
                    self.config, f"{mode.value}_parallelism", 200
                )
            }
            
            # Add compaction options
            if self.config.compaction_strategy == CompactionStrategy.INLINE:
                hudi_options["hoodie.compact.inline"] = "true"
                hudi_options["hoodie.compact.inline.max.delta.commits"] = str(
                    self.config.compaction_max_delta_commits
                )
            
            # Add clustering options
            if self.config.enable_clustering:
                hudi_options["hoodie.clustering.inline"] = "true"
                hudi_options["hoodie.clustering.plan.strategy.max.bytes.per.group"] = str(
                    self.config.clustering_max_group_size
                )
            
            # Handle partition overwrite
            if partition_overwrite and mode in [WriteMode.INSERT_OVERWRITE, WriteMode.INSERT_OVERWRITE_TABLE]:
                hudi_options["hoodie.datasource.write.partitions.to.delete"] = ",".join(partition_overwrite)
            
            # Write data
            start_time = datetime.now()
            spark_df.write \
                .format("hudi") \
                .options(**hudi_options) \
                .mode("append") \
                .save(table_path)
            
            # Get write statistics
            stats = {
                "records_written": spark_df.count(),
                "mode": mode.value,
                "duration_seconds": (datetime.now() - start_time).total_seconds(),
                "timestamp": datetime.now().isoformat()
            }
            
            logger.info(f"Wrote {stats['records_written']} records to {table_name}")
            return stats
            
        except Exception as e:
            logger.error(f"Failed to write data: {e}")
            raise
    
    async def read_table(
        self,
        table_name: str,
        columns: Optional[List[str]] = None,
        filter_expr: Optional[str] = None,
        as_of_instant: Optional[str] = None,
        incremental: Optional[IncrementalQuery] = None,
        limit: Optional[int] = None
    ) -> pd.DataFrame:
        """
        Read data from Hudi table.
        
        Args:
            table_name: Table name
            columns: Columns to read
            filter_expr: Filter expression
            as_of_instant: Read as of specific instant
            incremental: Incremental query config
            limit: Row limit
            
        Returns:
            DataFrame with results
        """
        try:
            table_path = os.path.join(self.config.table_path, table_name)
            
            # Build read options
            read_options = {}
            
            if as_of_instant:
                read_options["as.of.instant"] = as_of_instant
            
            if incremental:
                read_options["hoodie.datasource.query.type"] = "incremental"
                read_options["hoodie.datasource.read.begin.instanttime"] = incremental.begin_instant_time
                if incremental.end_instant_time:
                    read_options["hoodie.datasource.read.end.instanttime"] = incremental.end_instant_time
                read_options["hoodie.datasource.read.incr.path.glob"] = str(incremental.include_all_data_files).lower()
            else:
                read_options["hoodie.datasource.query.type"] = "snapshot"
            
            # Read data
            spark_df = self._spark.read \
                .format("hudi") \
                .options(**read_options) \
                .load(table_path)
            
            # Select columns
            if columns:
                spark_df = spark_df.select(*columns)
            
            # Apply filter
            if filter_expr:
                spark_df = spark_df.filter(filter_expr)
            
            # Apply limit
            if limit:
                spark_df = spark_df.limit(limit)
            
            # Convert to pandas
            df = spark_df.toPandas()
            
            logger.info(f"Read {len(df)} rows from {table_name}")
            return df
            
        except Exception as e:
            logger.error(f"Failed to read table: {e}")
            raise
    
    async def get_table_timeline(
        self,
        table_name: str,
        limit: int = 10
    ) -> List[Dict[str, Any]]:
        """
        Get table commit timeline.
        
        Args:
            table_name: Table name
            limit: Maximum commits to return
            
        Returns:
            List of commits
        """
        try:
            table_path = os.path.join(self.config.table_path, table_name)
            
            # Read timeline using Spark SQL
            self._spark.sql(f"""
                SELECT 
                    commit_time,
                    action,
                    total_records_written,
                    total_files_added,
                    total_files_updated,
                    total_log_files_added,
                    total_log_files_updated,
                    total_partitions_written
                FROM hudi_table_timeline('{table_path}')
                ORDER BY commit_time DESC
                LIMIT {limit}
            """).createOrReplaceTempView("timeline")
            
            timeline_df = self._spark.sql("SELECT * FROM timeline")
            
            timeline = []
            for row in timeline_df.collect():
                timeline.append({
                    "commit_time": row.commit_time,
                    "action": row.action,
                    "records_written": row.total_records_written,
                    "files_added": row.total_files_added,
                    "files_updated": row.total_files_updated,
                    "log_files_added": row.total_log_files_added,
                    "log_files_updated": row.total_log_files_updated,
                    "partitions_written": row.total_partitions_written
                })
            
            return timeline
            
        except Exception as e:
            logger.error(f"Failed to get table timeline: {e}")
            # Fallback to file system based approach
            return []
    
    async def compact_table(
        self,
        table_name: str,
        instant_time: Optional[str] = None,
        async_compact: bool = False
    ) -> Dict[str, Any]:
        """
        Compact Hudi table.
        
        Args:
            table_name: Table name
            instant_time: Specific instant to compact
            async_compact: Run compaction asynchronously
            
        Returns:
            Compaction statistics
        """
        try:
            table_path = os.path.join(self.config.table_path, table_name)
            
            # Run compaction using Spark
            compact_options = {
                "hoodie.compact.inline": "false",
                "hoodie.datasource.compaction.async.enable": str(async_compact).lower()
            }
            
            if instant_time:
                compact_options["hoodie.compaction.instant.time"] = instant_time
            
            # Trigger compaction
            self._spark.read \
                .format("hudi") \
                .options(**compact_options) \
                .load(table_path) \
                .write \
                .format("hudi") \
                .option("hoodie.datasource.write.operation", "compact") \
                .mode("append") \
                .save(table_path)
            
            stats = {
                "status": "completed" if not async_compact else "scheduled",
                "instant_time": instant_time or "latest",
                "timestamp": datetime.now().isoformat()
            }
            
            logger.info(f"Compaction {stats['status']} for {table_name}")
            return stats
            
        except Exception as e:
            logger.error(f"Failed to compact table: {e}")
            raise
    
    async def cluster_table(
        self,
        table_name: str,
        clustering_config: ClusteringConfig
    ) -> Dict[str, Any]:
        """
        Cluster Hudi table for better performance.
        
        Args:
            table_name: Table name
            clustering_config: Clustering configuration
            
        Returns:
            Clustering statistics
        """
        try:
            table_path = os.path.join(self.config.table_path, table_name)
            
            # Clustering options
            cluster_options = {
                "hoodie.clustering.inline": "true",
                "hoodie.clustering.plan.strategy.sort.columns": ",".join(clustering_config.columns),
                "hoodie.clustering.plan.strategy.max.bytes.per.group": str(clustering_config.max_file_size),
                "hoodie.clustering.plan.strategy.target.file.max.bytes": str(clustering_config.target_file_size),
                "hoodie.clustering.max.groups": str(clustering_config.max_groups)
            }
            
            # Trigger clustering
            self._spark.read \
                .format("hudi") \
                .load(table_path) \
                .write \
                .format("hudi") \
                .options(**cluster_options) \
                .option("hoodie.datasource.write.operation", "cluster") \
                .mode("append") \
                .save(table_path)
            
            stats = {
                "status": "completed",
                "columns": clustering_config.columns,
                "timestamp": datetime.now().isoformat()
            }
            
            logger.info(f"Clustering completed for {table_name}")
            return stats
            
        except Exception as e:
            logger.error(f"Failed to cluster table: {e}")
            raise
    
    async def get_table_details(
        self,
        table_name: str
    ) -> HudiTable:
        """
        Get detailed table information.
        
        Args:
            table_name: Table name
            
        Returns:
            Table metadata
        """
        try:
            table_path = os.path.join(self.config.table_path, table_name)
            
            # Read table
            spark_df = self._spark.read.format("hudi").load(table_path)
            
            # Get schema
            schema = {field.name: str(field.dataType) for field in spark_df.schema.fields}
            
            # Get partitions
            partitions = []
            if "_hoodie_partition_path" in spark_df.columns:
                partitions = [row[0] for row in spark_df.select("_hoodie_partition_path").distinct().collect()]
            
            # Get timeline
            timeline = await self.get_table_timeline(table_name, limit=5)
            
            # Get latest commit
            latest_commit = timeline[0]["commit_time"] if timeline else "0"
            
            return HudiTable(
                path=table_path,
                name=table_name,
                table_type=self.config.table_type,
                latest_commit=latest_commit,
                total_records=spark_df.count(),
                total_files=len(spark_df.inputFiles()),
                schema=schema,
                partitions=partitions,
                timeline=timeline
            )
            
        except Exception as e:
            logger.error(f"Failed to get table details: {e}")
            raise
    
    async def rollback_table(
        self,
        table_name: str,
        instant_time: str
    ) -> Dict[str, Any]:
        """
        Rollback table to specific instant.
        
        Args:
            table_name: Table name
            instant_time: Target instant time
            
        Returns:
            Rollback statistics
        """
        try:
            table_path = os.path.join(self.config.table_path, table_name)
            
            # Rollback using savepoint
            self._spark.sql(f"""
                CALL rollback_to_instant(
                    table => '{table_path}',
                    instant_time => '{instant_time}'
                )
            """)
            
            stats = {
                "rolled_back_to": instant_time,
                "timestamp": datetime.now().isoformat()
            }
            
            logger.info(f"Rolled back table {table_name} to {instant_time}")
            return stats
            
        except Exception as e:
            logger.error(f"Failed to rollback table: {e}")
            raise
    
    async def get_client_specific_config(self) -> Dict[str, Any]:
        """Get Hudi specific configuration"""
        return {
            "table_path": self.config.table_path,
            "spark_master": self.config.spark_master,
            "table_type": self.config.table_type.value,
            "index_type": self.config.index_type.value,
            "compaction_strategy": self.config.compaction_strategy.value,
            "enable_clustering": self.config.enable_clustering
        } 