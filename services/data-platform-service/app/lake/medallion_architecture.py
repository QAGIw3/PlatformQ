"""
Medallion Architecture Data Lake Manager with Iceberg and Delta Support
"""
import os
from typing import Dict, Any, List, Optional, Union, Tuple
from datetime import datetime, timedelta
from enum import Enum
import json

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import StructType, StructField
from pyspark.sql.functions import col, current_timestamp, hash, lit
from delta import DeltaTable, configure_spark_with_delta_pip
from pyiceberg.catalog import load_catalog
from pyiceberg.table import Table
from pyiceberg.schema import Schema
from pyiceberg.types import *
from minio import Minio
from minio.error import S3Error

from platformq_shared.utils.logger import get_logger
from platformq_shared.errors import ValidationError, ServiceError
from ..core.config import settings

logger = get_logger(__name__)


class DataZone(str, Enum):
    """Data lake zones in medallion architecture"""
    BRONZE = "bronze"  # Raw data
    SILVER = "silver"  # Cleaned/validated data
    GOLD = "gold"      # Business-ready aggregates
    

class DataFormat(str, Enum):
    """Supported data formats"""
    DELTA = "delta"
    ICEBERG = "iceberg"
    PARQUET = "parquet"
    AVRO = "avro"
    JSON = "json"
    CSV = "csv"
    ORC = "orc"


class TableFormat(str, Enum):
    """Table formats for lakehouse"""
    DELTA = "delta"
    ICEBERG = "iceberg"


class MedallionLakeManager:
    """
    Manages the medallion architecture data lake with Iceberg and Delta support.
    
    Features:
    - Bronze layer: Raw data ingestion
    - Silver layer: Data cleaning and validation
    - Gold layer: Business aggregates and marts
    - Apache Iceberg for advanced table management
    - Delta Lake for ACID transactions
    - Time travel and versioning
    - Schema evolution
    - Data quality enforcement
    - Partition evolution (Iceberg)
    - Hidden partitioning (Iceberg)
    """
    
    def __init__(self,
                 spark_config: Optional[Dict[str, str]] = None,
                 storage_config: Optional[Dict[str, str]] = None,
                 default_table_format: TableFormat = TableFormat.ICEBERG):
        # Parse configs
        spark_config = spark_config or {}
        storage_config = storage_config or {}
        
        self.spark_master = spark_config.get("spark.master", "local[*]")
        self.minio_endpoint = storage_config.get("endpoint", "http://minio:9000").replace("http://", "")
        self.minio_access_key = storage_config.get("access_key", "minioadmin")
        self.minio_secret_key = storage_config.get("secret_key", "minioadmin")
        self.lake_bucket = storage_config.get("bucket", "platformq-lake")
        self.default_table_format = default_table_format
        
        # Store full spark config for session creation
        self.spark_config = spark_config
        
        # Spark session (to be initialized)
        self.spark: Optional[SparkSession] = None
        
        # MinIO client
        self.minio_client: Optional[Minio] = None
        
        # Iceberg catalog
        self.iceberg_catalog = None
        
        # Zone paths
        self.zone_paths = {
            DataZone.BRONZE: f"s3a://{lake_bucket}/bronze",
            DataZone.SILVER: f"s3a://{lake_bucket}/silver",
            DataZone.GOLD: f"s3a://{lake_bucket}/gold"
        }
        
        # Iceberg warehouse path
        self.iceberg_warehouse = f"s3a://{lake_bucket}/iceberg/warehouse"
        
        # Checkpoint paths for streaming
        self.checkpoint_paths = {
            DataZone.BRONZE: f"s3a://{lake_bucket}/checkpoints/bronze",
            DataZone.SILVER: f"s3a://{lake_bucket}/checkpoints/silver",
            DataZone.GOLD: f"s3a://{lake_bucket}/checkpoints/gold"
        }
    
    async def initialize(self) -> None:
        """Initialize the lake manager"""
        try:
            # Initialize Spark session
            await self._init_spark()
            
            # Initialize MinIO client
            await self._init_minio()
            
            # Initialize Iceberg catalog
            await self._init_iceberg_catalog()
            
            # Create bucket structure
            await self._create_lake_structure()
            
            logger.info("Medallion lake manager initialized with Iceberg and Delta support")
            
        except Exception as e:
            logger.error(f"Failed to initialize lake manager: {e}")
            raise
    
    async def _init_spark(self) -> None:
        """Initialize Spark session with Delta Lake and Iceberg"""
        try:
            # Configure Spark with Delta and Iceberg
            builder = SparkSession.builder
            
            # Apply all spark configs from initialization
            for key, value in self.spark_config.items():
                builder = builder.config(key, value)
            
            # Ensure Iceberg and Delta extensions are included
            extensions = self.spark_config.get("spark.sql.extensions", "")
            if "IcebergSparkSessionExtensions" not in extensions:
                extensions = f"{extensions},org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions" if extensions else "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions"
            if "DeltaSparkSessionExtension" not in extensions:
                extensions = f"{extensions},io.delta.sql.DeltaSparkSessionExtension" if extensions else "io.delta.sql.DeltaSparkSessionExtension"
            builder = builder.config("spark.sql.extensions", extensions)
            
            # S3/MinIO configuration
            builder = builder \
                .config("spark.hadoop.fs.s3a.endpoint", self.minio_endpoint) \
                .config("spark.hadoop.fs.s3a.access.key", self.minio_access_key) \
                .config("spark.hadoop.fs.s3a.secret.key", self.minio_secret_key) \
                .config("spark.hadoop.fs.s3a.path.style.access", "true") \
                .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
                .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
            
            # Iceberg-specific configurations
            builder = builder \
                .config("spark.sql.catalog.iceberg.io-impl", "org.apache.iceberg.aws.s3.S3FileIO") \
                .config("spark.sql.catalog.iceberg.s3.endpoint", f"http://{self.minio_endpoint}") \
                .config("spark.sql.catalog.iceberg.s3.access-key-id", self.minio_access_key) \
                .config("spark.sql.catalog.iceberg.s3.secret-access-key", self.minio_secret_key) \
                .config("spark.sql.catalog.iceberg.s3.path-style-access", "true")
            
            # Performance optimizations
            builder = builder \
                .config("spark.sql.adaptive.enabled", "true") \
                .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
                .config("spark.sql.adaptive.skewJoin.enabled", "true") \
                .config("spark.sql.shuffle.partitions", "200") \
                .config("spark.sql.iceberg.vectorization.enabled", "true")
            
            # Create session
            self.spark = configure_spark_with_delta_pip(builder).getOrCreate()
            
            # Set log level
            self.spark.sparkContext.setLogLevel("WARN")
            
            logger.info("Spark session initialized with Delta Lake support")
            
        except Exception as e:
            logger.error(f"Failed to initialize Spark: {e}")
            raise
    
    async def _init_minio(self) -> None:
        """Initialize MinIO client"""
        try:
            self.minio_client = Minio(
                self.minio_endpoint,
                access_key=self.minio_access_key,
                secret_key=self.minio_secret_key,
                secure=False
            )
            
            # Test connection
            buckets = self.minio_client.list_buckets()
            logger.info(f"Connected to MinIO, found {len(buckets)} buckets")
            
        except Exception as e:
            logger.error(f"Failed to initialize MinIO: {e}")
            raise
    
    async def _init_iceberg_catalog(self) -> None:
        """Initialize Iceberg catalog"""
        try:
            self.iceberg_catalog = load_catalog("iceberg")
            logger.info(f"Initialized Iceberg catalog: {self.iceberg_catalog.get_current_location()}")
        except Exception as e:
            logger.error(f"Failed to initialize Iceberg catalog: {e}")
            raise
    
    async def _create_lake_structure(self) -> None:
        """Create lake bucket and directory structure"""
        try:
            # Create bucket if not exists
            if not self.minio_client.bucket_exists(self.lake_bucket):
                self.minio_client.make_bucket(self.lake_bucket)
                logger.info(f"Created lake bucket: {self.lake_bucket}")
            
            # Create zone directories (MinIO creates them implicitly)
            # Just log the structure
            logger.info(f"Lake structure: {list(self.zone_paths.values())}")
            
        except S3Error as e:
            logger.error(f"Failed to create lake structure: {e}")
            raise
    
    async def ingest_to_bronze(self,
                             source_data: Union[str, DataFrame],
                             dataset_name: str,
                             tenant_id: str,
                             source_format: DataFormat = DataFormat.JSON,
                             schema: Optional[StructType] = None,
                             partition_by: Optional[List[str]] = None,
                             table_format: Optional[TableFormat] = None) -> Dict[str, Any]:
        """Ingest raw data into Bronze zone with support for Delta and Iceberg"""
        try:
            start_time = datetime.utcnow()
            
            # Use default table format if not specified
            table_format = table_format or self.default_table_format
            
            # Load data
            if isinstance(source_data, str):
                df = await self._load_data(source_data, source_format, schema)
            else:
                df = source_data
            
            # Add metadata columns
            df = df.withColumn("_ingestion_timestamp", current_timestamp()) \
                   .withColumn("_tenant_id", lit(tenant_id)) \
                   .withColumn("_source_file", lit(source_data if isinstance(source_data, str) else "dataframe")) \
                   .withColumn("_record_hash", hash(*[col(c) for c in df.columns]))
            
            # Bronze path/table
            if table_format == TableFormat.ICEBERG:
                table_name = f"iceberg.bronze.{tenant_id}_{dataset_name}"
                
                # Create table if not exists
                if not self._iceberg_table_exists(table_name):
                    await self._create_iceberg_table(
                        table_name=table_name,
                        df=df,
                        partition_by=partition_by or ["_ingestion_timestamp"]
                    )
                
                # Write to Iceberg table
                df.writeTo(table_name).using("iceberg").mode("append").save()
                
                result_path = table_name
            else:
                # Delta format
                bronze_path = f"{self.zone_paths[DataZone.BRONZE]}/{tenant_id}/{dataset_name}"
                
                # Write to Delta format
                writer = df.write.mode("append").format("delta")
            
            # Add partitioning if specified
            if partition_by:
                writer = writer.partitionBy(partition_by)
            
            # Write data
            writer.save(bronze_path)
            
            # Get statistics
            row_count = df.count()
            
            # Log ingestion
            end_time = datetime.utcnow()
            duration = (end_time - start_time).total_seconds()
            
            result = {
                "dataset_name": dataset_name,
                "zone": DataZone.BRONZE,
                "path": bronze_path,
                "row_count": row_count,
                "ingestion_timestamp": start_time.isoformat(),
                "duration_seconds": duration,
                "status": "success"
            }
            
            logger.info(f"Ingested {row_count} rows to Bronze: {dataset_name}")
            
            return result
            
        except Exception as e:
            logger.error(f"Failed to ingest to Bronze: {e}")
            raise ServiceError(f"Bronze ingestion failed: {str(e)}")
    
    async def process_to_silver(self,
                              bronze_dataset: str,
                              silver_dataset: str,
                              tenant_id: str,
                              quality_rules: Optional[List[Dict[str, Any]]] = None,
                              transformations: Optional[List[Dict[str, Any]]] = None) -> Dict[str, Any]:
        """Process Bronze data to Silver with quality checks"""
        try:
            start_time = datetime.utcnow()
            
            # Read from Bronze
            bronze_path = f"{self.zone_paths[DataZone.BRONZE]}/{tenant_id}/{bronze_dataset}"
            df = self.spark.read.format("delta").load(bronze_path)
            
            # Apply quality rules
            if quality_rules:
                df = await self._apply_quality_rules(df, quality_rules)
            
            # Apply transformations
            if transformations:
                df = await self._apply_transformations(df, transformations)
            
            # Add Silver metadata
            df = df.withColumn("_processing_timestamp", current_timestamp()) \
                   .withColumn("_quality_score", lit(1.0))  # Placeholder
            
            # Silver path
            silver_path = f"{self.zone_paths[DataZone.SILVER]}/{tenant_id}/{silver_dataset}"
            
            # Write to Silver
            df.write.mode("overwrite").format("delta").save(silver_path)
            
            # Enable Delta features
            delta_table = DeltaTable.forPath(self.spark, silver_path)
            
            # Optimize table
            delta_table.optimize().executeCompaction()
            
            # Get statistics
            row_count = df.count()
            
            result = {
                "dataset_name": silver_dataset,
                "zone": DataZone.SILVER,
                "path": silver_path,
                "row_count": row_count,
                "processing_timestamp": start_time.isoformat(),
                "duration_seconds": (datetime.utcnow() - start_time).total_seconds(),
                "status": "success"
            }
            
            logger.info(f"Processed {row_count} rows to Silver: {silver_dataset}")
            
            return result
            
        except Exception as e:
            logger.error(f"Failed to process to Silver: {e}")
            raise ServiceError(f"Silver processing failed: {str(e)}")
    
    async def aggregate_to_gold(self,
                              silver_datasets: List[str],
                              gold_dataset: str,
                              tenant_id: str,
                              aggregations: List[Dict[str, Any]],
                              join_conditions: Optional[List[Dict[str, Any]]] = None) -> Dict[str, Any]:
        """Create Gold layer aggregates from Silver data"""
        try:
            start_time = datetime.utcnow()
            
            # Load Silver datasets
            dataframes = []
            for dataset in silver_datasets:
                silver_path = f"{self.zone_paths[DataZone.SILVER]}/{tenant_id}/{dataset}"
                df = self.spark.read.format("delta").load(silver_path)
                dataframes.append((dataset, df))
            
            # Join datasets if multiple
            if len(dataframes) > 1 and join_conditions:
                result_df = await self._join_dataframes(dataframes, join_conditions)
            else:
                result_df = dataframes[0][1]
            
            # Apply aggregations
            for agg in aggregations:
                result_df = await self._apply_aggregation(result_df, agg)
            
            # Add Gold metadata
            result_df = result_df.withColumn("_aggregation_timestamp", current_timestamp()) \
                                 .withColumn("_source_datasets", lit(",".join(silver_datasets)))
            
            # Gold path
            gold_path = f"{self.zone_paths[DataZone.GOLD]}/{tenant_id}/{gold_dataset}"
            
            # Write to Gold
            result_df.write.mode("overwrite").format("delta").save(gold_path)
            
            # Create/update table for SQL access
            result_df.write.mode("overwrite").saveAsTable(f"{tenant_id}_{gold_dataset}")
            
            # Get statistics
            row_count = result_df.count()
            
            result = {
                "dataset_name": gold_dataset,
                "zone": DataZone.GOLD,
                "path": gold_path,
                "row_count": row_count,
                "aggregation_timestamp": start_time.isoformat(),
                "duration_seconds": (datetime.utcnow() - start_time).total_seconds(),
                "source_datasets": silver_datasets,
                "status": "success"
            }
            
            logger.info(f"Created Gold aggregate: {gold_dataset} with {row_count} rows")
            
            return result
            
        except Exception as e:
            logger.error(f"Failed to aggregate to Gold: {e}")
            raise ServiceError(f"Gold aggregation failed: {str(e)}")
    
    async def _load_data(self, 
                        source_path: str,
                        format: DataFormat,
                        schema: Optional[StructType] = None) -> DataFrame:
        """Load data from various formats"""
        reader = self.spark.read
        
        if schema:
            reader = reader.schema(schema)
        
        if format == DataFormat.JSON:
            return reader.json(source_path)
        elif format == DataFormat.CSV:
            return reader.option("header", "true").csv(source_path)
        elif format == DataFormat.PARQUET:
            return reader.parquet(source_path)
        elif format == DataFormat.AVRO:
            return reader.format("avro").load(source_path)
        elif format == DataFormat.ORC:
            return reader.orc(source_path)
        else:
            raise ValueError(f"Unsupported format: {format}")
    
    async def _apply_quality_rules(self, 
                                 df: DataFrame,
                                 rules: List[Dict[str, Any]]) -> DataFrame:
        """Apply data quality rules"""
        for rule in rules:
            rule_type = rule.get("type")
            
            if rule_type == "not_null":
                columns = rule.get("columns", [])
                for col_name in columns:
                    df = df.filter(col(col_name).isNotNull())
                    
            elif rule_type == "range":
                column = rule.get("column")
                min_val = rule.get("min")
                max_val = rule.get("max")
                if min_val is not None and max_val is not None:
                    df = df.filter((col(column) >= min_val) & (col(column) <= max_val))
                    
            elif rule_type == "pattern":
                column = rule.get("column")
                pattern = rule.get("pattern")
                df = df.filter(col(column).rlike(pattern))
                
            elif rule_type == "dedup":
                columns = rule.get("columns", df.columns)
                df = df.dropDuplicates(columns)
        
        return df
    
    async def _apply_transformations(self,
                                   df: DataFrame,
                                   transformations: List[Dict[str, Any]]) -> DataFrame:
        """Apply data transformations"""
        for transform in transformations:
            transform_type = transform.get("type")
            
            if transform_type == "rename":
                mappings = transform.get("mappings", {})
                for old_name, new_name in mappings.items():
                    df = df.withColumnRenamed(old_name, new_name)
                    
            elif transform_type == "cast":
                mappings = transform.get("mappings", {})
                for col_name, data_type in mappings.items():
                    df = df.withColumn(col_name, col(col_name).cast(data_type))
                    
            elif transform_type == "derive":
                column = transform.get("column")
                expression = transform.get("expression")
                df = df.withColumn(column, eval(expression))
                
            elif transform_type == "filter":
                condition = transform.get("condition")
                df = df.filter(condition)
        
        return df
    
    async def _join_dataframes(self,
                             dataframes: List[Tuple[str, DataFrame]],
                             join_conditions: List[Dict[str, Any]]) -> DataFrame:
        """Join multiple dataframes"""
        result = dataframes[0][1]
        
        for i in range(1, len(dataframes)):
            join_info = join_conditions[i-1] if i-1 < len(join_conditions) else {}
            
            left_keys = join_info.get("left_keys", [])
            right_keys = join_info.get("right_keys", left_keys)
            join_type = join_info.get("type", "inner")
            
            if len(left_keys) != len(right_keys):
                raise ValueError("Mismatched join keys")
            
            # Build join condition
            if left_keys:
                conditions = [result[lk] == dataframes[i][1][rk] 
                            for lk, rk in zip(left_keys, right_keys)]
                join_expr = conditions[0]
                for cond in conditions[1:]:
                    join_expr = join_expr & cond
                
                result = result.join(dataframes[i][1], join_expr, join_type)
            else:
                result = result.join(dataframes[i][1], how=join_type)
        
        return result
    
    async def _apply_aggregation(self,
                               df: DataFrame,
                               aggregation: Dict[str, Any]) -> DataFrame:
        """Apply aggregation to dataframe"""
        group_by = aggregation.get("group_by", [])
        agg_functions = aggregation.get("functions", {})
        
        if group_by:
            grouped = df.groupBy(*group_by)
            
            # Build aggregation expressions
            agg_exprs = []
            for col_name, func_list in agg_functions.items():
                if isinstance(func_list, str):
                    func_list = [func_list]
                
                for func in func_list:
                    if func == "sum":
                        agg_exprs.append(F.sum(col_name).alias(f"{col_name}_sum"))
                    elif func == "avg":
                        agg_exprs.append(F.avg(col_name).alias(f"{col_name}_avg"))
                    elif func == "count":
                        agg_exprs.append(F.count(col_name).alias(f"{col_name}_count"))
                    elif func == "max":
                        agg_exprs.append(F.max(col_name).alias(f"{col_name}_max"))
                    elif func == "min":
                        agg_exprs.append(F.min(col_name).alias(f"{col_name}_min"))
            
            if agg_exprs:
                df = grouped.agg(*agg_exprs)
        
        return df
    
    async def get_dataset_info(self,
                             dataset_name: str,
                             zone: DataZone,
                             tenant_id: str) -> Dict[str, Any]:
        """Get information about a dataset"""
        try:
            dataset_path = f"{self.zone_paths[zone]}/{tenant_id}/{dataset_name}"
            
            # Check if dataset exists
            try:
                delta_table = DeltaTable.forPath(self.spark, dataset_path)
            except:
                raise ValueError(f"Dataset not found: {dataset_name}")
            
            # Get table details
            detail_df = delta_table.detail()
            details = detail_df.collect()[0].asDict()
            
            # Get history
            history_df = delta_table.history(10)
            history = [row.asDict() for row in history_df.collect()]
            
            # Get schema
            schema = self.spark.read.format("delta").load(dataset_path).schema.json()
            
            return {
                "dataset_name": dataset_name,
                "zone": zone.value,
                "path": dataset_path,
                "format": details.get("format", "delta"),
                "size_bytes": details.get("sizeInBytes", 0),
                "num_files": details.get("numFiles", 0),
                "created_time": details.get("createdTime"),
                "last_modified": details.get("lastModified"),
                "schema": json.loads(schema),
                "partitions": details.get("partitionColumns", []),
                "history": history
            }
            
        except Exception as e:
            logger.error(f"Failed to get dataset info: {e}")
            raise
    
    async def time_travel(self,
                        dataset_name: str,
                        zone: DataZone,
                        tenant_id: str,
                        version: Optional[int] = None,
                        timestamp: Optional[str] = None) -> DataFrame:
        """Read dataset at specific version or timestamp"""
        try:
            dataset_path = f"{self.zone_paths[zone]}/{tenant_id}/{dataset_name}"
            
            reader = self.spark.read.format("delta")
            
            if version is not None:
                reader = reader.option("versionAsOf", version)
            elif timestamp:
                reader = reader.option("timestampAsOf", timestamp)
            else:
                raise ValueError("Either version or timestamp must be specified")
            
            return reader.load(dataset_path)
            
        except Exception as e:
            logger.error(f"Failed to time travel: {e}")
            raise
    
    async def optimize_delta_tables(self) -> Dict[str, Any]:
        """Optimize all Delta tables in the lake"""
        optimized_tables = []
        
        for zone in DataZone:
            zone_path = self.zone_paths[zone]
            
            # List all Delta tables in zone
            # This is simplified - in production, would scan directories
            
            logger.info(f"Optimizing tables in {zone.value} zone")
            
        return {
            "optimized_tables": optimized_tables,
            "timestamp": datetime.utcnow().isoformat()
        }
    
    async def get_lake_statistics(self) -> Dict[str, Any]:
        """Get overall lake statistics"""
        stats = {
            "zones": {},
            "total_size_bytes": 0,
            "total_datasets": 0,
            "last_updated": datetime.utcnow().isoformat()
        }
        
        for zone in DataZone:
            # Get zone statistics
            # In production, would query MinIO for actual stats
            zone_stats = {
                "datasets": 0,
                "size_bytes": 0,
                "last_ingestion": None
            }
            
            stats["zones"][zone.value] = zone_stats
        
        return stats
    
    async def health_check(self) -> None:
        """Check lake health"""
        # Test Spark
        self.spark.sql("SELECT 1").collect()
        
        # Test MinIO
        self.minio_client.list_buckets()
        
        logger.info("Lake health check passed")
    
    async def shutdown(self) -> None:
        """Shutdown lake manager"""
        if self.spark:
            self.spark.stop()
            logger.info("Spark session stopped")


# Import pyspark.sql.functions as F for aggregations
    # Iceberg-specific methods
    
    def _iceberg_table_exists(self, table_name: str) -> bool:
        """Check if Iceberg table exists"""
        try:
            self.spark.table(table_name)
            return True
        except Exception:
            return False
    
    async def _create_iceberg_table(self,
                                   table_name: str,
                                   df: DataFrame,
                                   partition_by: List[str],
                                   properties: Optional[Dict[str, str]] = None) -> None:
        """Create a new Iceberg table"""
        try:
            # Create table with partitioning
            create_table_sql = f"""
            CREATE TABLE IF NOT EXISTS {table_name}
            USING iceberg
            """
            
            if partition_by:
                create_table_sql += f"\nPARTITIONED BY ({', '.join(partition_by)})"
            
            if properties:
                props_str = ", ".join([f"'{k}'='{v}'" for k, v in properties.items()])
                create_table_sql += f"\nTBLPROPERTIES ({props_str})"
            
            # Create empty table with schema
            df.createOrReplaceTempView("temp_table_schema")
            create_table_sql += "\nAS SELECT * FROM temp_table_schema WHERE 1=0"
            
            self.spark.sql(create_table_sql)
            logger.info(f"Created Iceberg table: {table_name}")
            
        except Exception as e:
            logger.error(f"Failed to create Iceberg table: {e}")
            raise
    
    async def evolve_iceberg_schema(self,
                                   table_name: str,
                                   add_columns: Optional[Dict[str, str]] = None,
                                   rename_columns: Optional[Dict[str, str]] = None,
                                   drop_columns: Optional[List[str]] = None) -> None:
        """Evolve schema of an Iceberg table"""
        try:
            if add_columns:
                for col_name, col_type in add_columns.items():
                    self.spark.sql(f"ALTER TABLE {table_name} ADD COLUMN {col_name} {col_type}")
                    logger.info(f"Added column {col_name} to {table_name}")
            
            if rename_columns:
                for old_name, new_name in rename_columns.items():
                    self.spark.sql(f"ALTER TABLE {table_name} RENAME COLUMN {old_name} TO {new_name}")
                    logger.info(f"Renamed column {old_name} to {new_name} in {table_name}")
            
            if drop_columns:
                for col_name in drop_columns:
                    self.spark.sql(f"ALTER TABLE {table_name} DROP COLUMN {col_name}")
                    logger.info(f"Dropped column {col_name} from {table_name}")
                    
        except Exception as e:
            logger.error(f"Failed to evolve schema: {e}")
            raise
    
    async def iceberg_time_travel(self,
                                 table_name: str,
                                 snapshot_id: Optional[int] = None,
                                 as_of_timestamp: Optional[str] = None) -> DataFrame:
        """Read Iceberg table at specific snapshot or timestamp"""
        try:
            if snapshot_id:
                return self.spark.read.option("snapshot-id", snapshot_id).table(table_name)
            elif as_of_timestamp:
                return self.spark.read.option("as-of-timestamp", as_of_timestamp).table(table_name)
            else:
                return self.spark.table(table_name)
                
        except Exception as e:
            logger.error(f"Failed to read Iceberg table snapshot: {e}")
            raise
    
    async def get_iceberg_snapshots(self, table_name: str) -> List[Dict[str, Any]]:
        """Get all snapshots of an Iceberg table"""
        try:
            snapshots_df = self.spark.sql(f"SELECT * FROM {table_name}.snapshots")
            snapshots = []
            
            for row in snapshots_df.collect():
                snapshots.append({
                    "snapshot_id": row.snapshot_id,
                    "parent_id": row.parent_id,
                    "operation": row.operation,
                    "manifest_list": row.manifest_list,
                    "summary": row.summary,
                    "committed_at": datetime.fromtimestamp(row.committed_at / 1000).isoformat()
                })
                
            return snapshots
            
        except Exception as e:
            logger.error(f"Failed to get snapshots: {e}")
            raise
    
    async def optimize_iceberg_tables(self,
                                     compact_small_files: bool = True,
                                     expire_snapshots: bool = True,
                                     rewrite_manifests: bool = True) -> Dict[str, Any]:
        """Optimize Iceberg tables for better performance"""
        try:
            results = {
                "optimized_tables": [],
                "errors": []
            }
            
            # Get all Iceberg tables
            tables_df = self.spark.sql("SHOW TABLES IN iceberg")
            
            for row in tables_df.collect():
                table_name = f"iceberg.{row.namespace}.{row.tableName}"
                
                try:
                    # Compact small files
                    if compact_small_files:
                        self.spark.sql(f"""
                        CALL iceberg.system.rewrite_data_files(
                            table => '{table_name}',
                            strategy => 'binpack',
                            options => map('target-file-size-bytes', '134217728')
                        )
                        """)
                    
                    # Expire old snapshots
                    if expire_snapshots:
                        retention_days = 7
                        expire_timestamp = (datetime.utcnow() - timedelta(days=retention_days)).isoformat()
                        self.spark.sql(f"""
                        CALL iceberg.system.expire_snapshots(
                            table => '{table_name}',
                            older_than => TIMESTAMP '{expire_timestamp}',
                            retain_last => 3
                        )
                        """)
                    
                    # Rewrite manifests
                    if rewrite_manifests:
                        self.spark.sql(f"""
                        CALL iceberg.system.rewrite_manifests(
                            table => '{table_name}'
                        )
                        """)
                    
                    results["optimized_tables"].append(table_name)
                    logger.info(f"Optimized Iceberg table: {table_name}")
                    
                except Exception as e:
                    results["errors"].append({
                        "table": table_name,
                        "error": str(e)
                    })
                    logger.error(f"Failed to optimize {table_name}: {e}")
            
            return results
            
        except Exception as e:
            logger.error(f"Failed to optimize Iceberg tables: {e}")
            raise
    
    async def convert_delta_to_iceberg(self,
                                      delta_path: str,
                                      iceberg_table: str,
                                      partition_by: Optional[List[str]] = None) -> None:
        """Convert existing Delta table to Iceberg format"""
        try:
            # Read Delta table
            delta_df = self.spark.read.format("delta").load(delta_path)
            
            # Create Iceberg table
            await self._create_iceberg_table(
                table_name=iceberg_table,
                df=delta_df,
                partition_by=partition_by or [],
                properties={
                    "write.format.default": "parquet",
                    "write.parquet.compression-codec": "snappy"
                }
            )
            
            # Write data to Iceberg
            delta_df.writeTo(iceberg_table).using("iceberg").mode("overwrite").save()
            
            logger.info(f"Converted Delta table {delta_path} to Iceberg {iceberg_table}")
            
        except Exception as e:
            logger.error(f"Failed to convert Delta to Iceberg: {e}")
            raise
            
    def close(self) -> None:
        """Close resources"""
        if self.spark:
            self.spark.stop()
            logger.info("Spark session closed") 