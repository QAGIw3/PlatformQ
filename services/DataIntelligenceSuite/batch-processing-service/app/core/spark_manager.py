"""Spark Manager for Batch Processing Service

Manages Spark session initialization, configuration, and SQL execution.
"""

import logging
from typing import Dict, Any, Optional, List
from datetime import datetime
import json

from pyspark.sql import SparkSession, DataFrame
from pyspark import SparkConf
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
import mlflow
import mlflow.spark

from app.core.config import Settings


logger = logging.getLogger(__name__)


class SparkManager:
    """Manages Spark sessions and operations"""
    
    def __init__(self, settings: Settings):
        self.settings = settings
        self.spark: Optional[SparkSession] = None
        self.initialized = False
        
    async def initialize(self):
        """Initialize Spark session"""
        logger.info("Initializing SparkManager")
        
        try:
            # Create Spark configuration
            conf = SparkConf()
            conf.setAppName(self.settings.spark_app_name)
            
            # Basic configurations
            conf.set("spark.sql.adaptive.enabled", "true")
            conf.set("spark.sql.adaptive.coalescePartitions.enabled", "true")
            conf.set("spark.sql.shuffle.partitions", str(self.settings.spark_sql_shuffle_partitions))
            
            # Set executor configurations
            if self.settings.spark_master != "local[*]":
                conf.set("spark.executor.memory", self.settings.spark_executor_memory)
                conf.set("spark.executor.cores", str(self.settings.spark_executor_cores))
                conf.set("spark.dynamicAllocation.enabled", "true")
                conf.set("spark.dynamicAllocation.maxExecutors", str(self.settings.spark_max_executors))
            
            # MinIO configurations for S3 compatibility
            conf.set("spark.hadoop.fs.s3a.endpoint", f"http://{self.settings.minio_endpoint}")
            conf.set("spark.hadoop.fs.s3a.access.key", self.settings.minio_access_key)
            conf.set("spark.hadoop.fs.s3a.secret.key", self.settings.minio_secret_key)
            conf.set("spark.hadoop.fs.s3a.path.style.access", "true")
            conf.set("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
            
            # Cassandra configurations
            conf.set("spark.cassandra.connection.host", ",".join(self.settings.cassandra_hosts))
            conf.set("spark.cassandra.connection.port", str(self.settings.cassandra_port))
            if self.settings.cassandra_username:
                conf.set("spark.cassandra.auth.username", self.settings.cassandra_username)
                conf.set("spark.cassandra.auth.password", self.settings.cassandra_password)
                
            # Elasticsearch configurations
            conf.set("es.nodes", ",".join(self.settings.elasticsearch_hosts))
            conf.set("es.nodes.wan.only", "true")
            if self.settings.elasticsearch_username:
                conf.set("es.net.http.auth.user", self.settings.elasticsearch_username)
                conf.set("es.net.http.auth.pass", self.settings.elasticsearch_password)
            
            # Create Spark session
            self.spark = SparkSession.builder \
                .master(self.settings.spark_master) \
                .config(conf=conf) \
                .enableHiveSupport() \
                .getOrCreate()
            
            # Set log level
            self.spark.sparkContext.setLogLevel(self.settings.log_level)
            
            # Configure MLflow
            mlflow.set_tracking_uri(self.settings.mlflow_tracking_uri)
            
            # Register UDFs
            self._register_udfs()
            
            # Create common temp views
            await self._create_temp_views()
            
            self.initialized = True
            logger.info("SparkManager initialized successfully")
            
        except Exception as e:
            logger.error(f"Failed to initialize SparkManager: {e}")
            raise
            
    async def cleanup(self):
        """Cleanup Spark session"""
        logger.info("Cleaning up SparkManager")
        
        if self.spark:
            try:
                self.spark.stop()
                self.spark = None
                self.initialized = False
                logger.info("SparkManager cleaned up")
            except Exception as e:
                logger.error(f"Error during SparkManager cleanup: {e}")
                
    def get_spark(self) -> SparkSession:
        """Get Spark session"""
        if not self.initialized or not self.spark:
            raise RuntimeError("SparkManager not initialized")
        return self.spark
        
    async def execute_sql(self, query: str, output_format: str = "json", 
                         limit: Optional[int] = None) -> Dict[str, Any]:
        """Execute Spark SQL query"""
        logger.info(f"Executing SQL query: {query[:100]}...")
        
        try:
            spark = self.get_spark()
            
            # Execute query
            df = spark.sql(query)
            
            # Apply limit if specified
            if limit:
                df = df.limit(limit)
                
            # Get schema
            schema = [{"name": field.name, "type": str(field.dataType)} 
                     for field in df.schema.fields]
            
            # Collect results based on format
            if output_format == "json":
                data = df.toJSON().collect()
                data = [json.loads(row) for row in data]
            elif output_format == "dict":
                data = [row.asDict() for row in df.collect()]
            else:
                data = df.collect()
                
            return {
                "schema": schema,
                "data": data,
                "count": len(data)
            }
            
        except Exception as e:
            logger.error(f"Failed to execute SQL: {e}")
            raise
            
    async def read_data(self, path: str, format: str = "parquet", 
                       options: Optional[Dict[str, Any]] = None) -> DataFrame:
        """Read data from various sources"""
        spark = self.get_spark()
        reader = spark.read.format(format)
        
        if options:
            for key, value in options.items():
                reader = reader.option(key, value)
                
        return reader.load(path)
        
    async def write_data(self, df: DataFrame, path: str, format: str = "parquet",
                        mode: str = "overwrite", partition_by: Optional[List[str]] = None,
                        options: Optional[Dict[str, Any]] = None):
        """Write data to various sinks"""
        writer = df.write.format(format).mode(mode)
        
        if partition_by:
            writer = writer.partitionBy(*partition_by)
            
        if options:
            for key, value in options.items():
                writer = writer.option(key, value)
                
        writer.save(path)
        
    async def cache_dataframe(self, df: DataFrame, name: str) -> DataFrame:
        """Cache DataFrame with a name"""
        df.createOrReplaceTempView(name)
        cached_df = df.cache()
        logger.info(f"Cached DataFrame as {name}")
        return cached_df
        
    async def uncache_dataframe(self, name: str):
        """Uncache DataFrame"""
        spark = self.get_spark()
        spark.catalog.dropTempView(name)
        spark.catalog.clearCache()
        logger.info(f"Uncached DataFrame {name}")
        
    def _register_udfs(self):
        """Register user-defined functions"""
        spark = self.get_spark()
        
        # Example UDF for hash generation
        def generate_hash(value: str) -> str:
            import hashlib
            return hashlib.sha256(value.encode()).hexdigest()
            
        spark.udf.register("generate_hash", generate_hash, StringType())
        
        # Example UDF for JSON parsing
        def parse_json_field(json_str: str, field: str) -> str:
            try:
                data = json.loads(json_str)
                return str(data.get(field, ""))
            except:
                return ""
                
        spark.udf.register("parse_json_field", parse_json_field, StringType())
        
        logger.info("Registered UDFs")
        
    async def _create_temp_views(self):
        """Create common temporary views"""
        spark = self.get_spark()
        
        # Create a sample metadata view
        metadata_schema = StructType([
            StructField("key", StringType(), True),
            StructField("value", StringType(), True),
            StructField("timestamp", StringType(), True)
        ])
        
        metadata_df = spark.createDataFrame([], metadata_schema)
        metadata_df.createOrReplaceTempView("metadata")
        
        logger.info("Created temporary views")
        
    async def optimize_table(self, table_name: str):
        """Optimize table for better performance"""
        spark = self.get_spark()
        
        try:
            # Analyze table to update statistics
            spark.sql(f"ANALYZE TABLE {table_name} COMPUTE STATISTICS")
            
            # If Delta table, run optimize
            try:
                spark.sql(f"OPTIMIZE {table_name}")
                logger.info(f"Optimized table {table_name}")
            except:
                logger.debug(f"Table {table_name} is not a Delta table")
                
        except Exception as e:
            logger.error(f"Failed to optimize table {table_name}: {e}")
            
    def get_spark_ui_url(self) -> str:
        """Get Spark UI URL"""
        spark = self.get_spark()
        return spark.sparkContext.uiWebUrl or "http://localhost:4040" 