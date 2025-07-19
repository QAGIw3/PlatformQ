"""
Data Ingestion Engine for various data sources
"""
import asyncio
from typing import Dict, Any, List, Optional, Union, AsyncIterator
from datetime import datetime
from enum import Enum
import json
import pandas as pd

from pyspark.sql import DataFrame
from pyspark.sql.types import StructType
import pulsar
from minio import Minio

from platformq_shared.utils.logger import get_logger
from platformq_shared.errors import ValidationError, ServiceError
from .medallion_architecture import MedallionLakeManager, DataFormat

logger = get_logger(__name__)


class IngestionMode(str, Enum):
    """Data ingestion modes"""
    BATCH = "batch"
    STREAMING = "streaming"
    CDC = "cdc"  # Change Data Capture
    INCREMENTAL = "incremental"


class SourceType(str, Enum):
    """Supported data sources"""
    FILE = "file"
    DATABASE = "database"
    API = "api"
    STREAM = "stream"
    S3 = "s3"
    

class DataIngestionEngine:
    """
    Engine for ingesting data from various sources into the data lake.
    
    Features:
    - Multiple source connectors
    - Batch and streaming ingestion
    - Schema inference and validation
    - Incremental data loading
    - Error handling and recovery
    - Progress monitoring
    """
    
    def __init__(self,
                 lake_manager: MedallionLakeManager,
                 pulsar_url: str = "pulsar://pulsar:6650"):
        self.lake_manager = lake_manager
        self.pulsar_url = pulsar_url
        
        # Pulsar client for streaming
        self.pulsar_client: Optional[pulsar.Client] = None
        
        # Active ingestion jobs
        self.active_jobs: Dict[str, Dict[str, Any]] = {}
        
        # Ingestion statistics
        self.stats = {
            "total_jobs": 0,
            "successful_jobs": 0,
            "failed_jobs": 0,
            "total_rows_ingested": 0,
            "total_bytes_ingested": 0
        }
    
    async def initialize(self) -> None:
        """Initialize the ingestion engine"""
        try:
            # Initialize Pulsar client for streaming
            self.pulsar_client = pulsar.Client(self.pulsar_url)
            
            logger.info("Data ingestion engine initialized")
            
        except Exception as e:
            logger.error(f"Failed to initialize ingestion engine: {e}")
            raise
    
    async def ingest_data(self,
                         dataset_id: str,
                         source_config: Dict[str, Any],
                         tenant_id: str,
                         mode: IngestionMode = IngestionMode.BATCH) -> Dict[str, Any]:
        """Main entry point for data ingestion"""
        job_id = f"ingest_{dataset_id}_{datetime.utcnow().timestamp()}"
        
        try:
            # Track job
            self.active_jobs[job_id] = {
                "dataset_id": dataset_id,
                "status": "running",
                "started_at": datetime.utcnow(),
                "mode": mode
            }
            self.stats["total_jobs"] += 1
            
            # Route to appropriate ingestion method
            source_type = SourceType(source_config.get("type", "file"))
            
            if mode == IngestionMode.BATCH:
                result = await self._ingest_batch(
                    dataset_id, source_type, source_config, tenant_id
                )
            elif mode == IngestionMode.STREAMING:
                result = await self._ingest_streaming(
                    dataset_id, source_type, source_config, tenant_id
                )
            elif mode == IngestionMode.CDC:
                result = await self._ingest_cdc(
                    dataset_id, source_type, source_config, tenant_id
                )
            elif mode == IngestionMode.INCREMENTAL:
                result = await self._ingest_incremental(
                    dataset_id, source_type, source_config, tenant_id
                )
            else:
                raise ValueError(f"Unsupported ingestion mode: {mode}")
            
            # Update job status
            self.active_jobs[job_id]["status"] = "completed"
            self.active_jobs[job_id]["completed_at"] = datetime.utcnow()
            self.active_jobs[job_id]["result"] = result
            
            self.stats["successful_jobs"] += 1
            self.stats["total_rows_ingested"] += result.get("row_count", 0)
            self.stats["total_bytes_ingested"] += result.get("bytes_ingested", 0)
            
            logger.info(f"Ingestion job {job_id} completed successfully")
            
            return {
                "job_id": job_id,
                "status": "completed",
                **result
            }
            
        except Exception as e:
            logger.error(f"Ingestion job {job_id} failed: {e}")
            
            self.active_jobs[job_id]["status"] = "failed"
            self.active_jobs[job_id]["error"] = str(e)
            self.stats["failed_jobs"] += 1
            
            raise ServiceError(f"Ingestion failed: {str(e)}")
            
        finally:
            # Cleanup completed job after some time
            asyncio.create_task(self._cleanup_job(job_id, delay=3600))
    
    async def _ingest_batch(self,
                          dataset_id: str,
                          source_type: SourceType,
                          source_config: Dict[str, Any],
                          tenant_id: str) -> Dict[str, Any]:
        """Batch ingestion from various sources"""
        start_time = datetime.utcnow()
        
        # Load data based on source type
        if source_type == SourceType.FILE:
            df = await self._load_from_file(source_config)
        elif source_type == SourceType.DATABASE:
            df = await self._load_from_database(source_config)
        elif source_type == SourceType.API:
            df = await self._load_from_api(source_config)
        elif source_type == SourceType.S3:
            df = await self._load_from_s3(source_config)
        else:
            raise ValueError(f"Unsupported source type for batch: {source_type}")
        
        # Get row count before ingestion
        row_count = df.count()
        
        # Ingest to Bronze layer
        result = await self.lake_manager.ingest_to_bronze(
            source_data=df,
            dataset_name=dataset_id,
            tenant_id=tenant_id,
            source_format=DataFormat.PARQUET,
            partition_by=source_config.get("partition_by")
        )
        
        # Calculate bytes (approximate)
        bytes_ingested = self._estimate_dataframe_size(df)
        
        return {
            "mode": "batch",
            "source_type": source_type.value,
            "row_count": row_count,
            "bytes_ingested": bytes_ingested,
            "duration_seconds": (datetime.utcnow() - start_time).total_seconds(),
            **result
        }
    
    async def _ingest_streaming(self,
                              dataset_id: str,
                              source_type: SourceType,
                              source_config: Dict[str, Any],
                              tenant_id: str) -> Dict[str, Any]:
        """Streaming ingestion"""
        if source_type != SourceType.STREAM:
            raise ValueError(f"Streaming only supports STREAM source type")
        
        # Create Pulsar consumer
        topic = source_config.get("topic", f"persistent://platformq/{tenant_id}/{dataset_id}")
        subscription = source_config.get("subscription", f"{dataset_id}_ingestion")
        
        consumer = self.pulsar_client.subscribe(
            topic,
            subscription,
            consumer_type=pulsar.ConsumerType.Shared
        )
        
        # Create streaming task
        streaming_task = asyncio.create_task(
            self._stream_to_bronze(consumer, dataset_id, tenant_id, source_config)
        )
        
        # Store task reference
        self.active_jobs[f"stream_{dataset_id}"] = {
            "task": streaming_task,
            "consumer": consumer,
            "started_at": datetime.utcnow()
        }
        
        return {
            "mode": "streaming",
            "source_type": source_type.value,
            "status": "streaming_started",
            "topic": topic,
            "subscription": subscription
        }
    
    async def _ingest_cdc(self,
                        dataset_id: str,
                        source_type: SourceType,
                        source_config: Dict[str, Any],
                        tenant_id: str) -> Dict[str, Any]:
        """Change Data Capture ingestion"""
        if source_type != SourceType.DATABASE:
            raise ValueError("CDC only supports DATABASE source type")
        
        # CDC configuration
        cdc_config = {
            "connector": source_config.get("cdc_connector", "debezium"),
            "database": source_config.get("database"),
            "tables": source_config.get("tables", []),
            "start_position": source_config.get("start_position", "current")
        }
        
        # In production, this would set up CDC connector
        # For now, simulate with periodic polling
        
        poll_interval = source_config.get("poll_interval", 60)  # seconds
        
        # Create CDC task
        cdc_task = asyncio.create_task(
            self._poll_cdc_changes(dataset_id, source_config, tenant_id, poll_interval)
        )
        
        # Store task reference
        self.active_jobs[f"cdc_{dataset_id}"] = {
            "task": cdc_task,
            "config": cdc_config,
            "started_at": datetime.utcnow()
        }
        
        return {
            "mode": "cdc",
            "source_type": source_type.value,
            "status": "cdc_started",
            "config": cdc_config
        }
    
    async def _ingest_incremental(self,
                                dataset_id: str,
                                source_type: SourceType,
                                source_config: Dict[str, Any],
                                tenant_id: str) -> Dict[str, Any]:
        """Incremental data loading"""
        # Get last ingestion timestamp
        last_timestamp = await self._get_last_ingestion_timestamp(dataset_id, tenant_id)
        
        # Update source config with timestamp filter
        if last_timestamp:
            timestamp_column = source_config.get("timestamp_column", "updated_at")
            source_config["filter"] = f"{timestamp_column} > '{last_timestamp}'"
        
        # Perform batch ingestion with filter
        result = await self._ingest_batch(dataset_id, source_type, source_config, tenant_id)
        
        # Store new timestamp
        await self._update_ingestion_timestamp(dataset_id, tenant_id)
        
        return {
            "mode": "incremental",
            "last_timestamp": last_timestamp,
            **result
        }
    
    async def _load_from_file(self, config: Dict[str, Any]) -> DataFrame:
        """Load data from file system"""
        file_path = config.get("path")
        file_format = config.get("format", "csv")
        schema = config.get("schema")
        
        if not file_path:
            raise ValueError("File path is required")
        
        # Use lake manager's spark session
        spark = self.lake_manager.spark
        
        reader = spark.read
        
        if schema:
            reader = reader.schema(schema)
        
        if file_format == "csv":
            reader = reader.option("header", config.get("header", "true"))
            reader = reader.option("inferSchema", config.get("infer_schema", "true"))
            return reader.csv(file_path)
        elif file_format == "json":
            return reader.json(file_path)
        elif file_format == "parquet":
            return reader.parquet(file_path)
        elif file_format == "avro":
            return reader.format("avro").load(file_path)
        else:
            raise ValueError(f"Unsupported file format: {file_format}")
    
    async def _load_from_database(self, config: Dict[str, Any]) -> DataFrame:
        """Load data from database using JDBC"""
        jdbc_url = config.get("jdbc_url")
        table = config.get("table")
        query = config.get("query")
        user = config.get("user")
        password = config.get("password")
        
        if not jdbc_url:
            raise ValueError("JDBC URL is required")
        
        spark = self.lake_manager.spark
        
        reader = spark.read.format("jdbc") \
            .option("url", jdbc_url) \
            .option("driver", config.get("driver", "org.postgresql.Driver"))
        
        if user:
            reader = reader.option("user", user)
        if password:
            reader = reader.option("password", password)
        
        if query:
            reader = reader.option("dbtable", f"({query}) as tmp")
        elif table:
            reader = reader.option("dbtable", table)
        else:
            raise ValueError("Either table or query must be specified")
        
        # Add partitioning for parallel reads
        if config.get("partition_column"):
            reader = reader.option("partitionColumn", config["partition_column"]) \
                          .option("lowerBound", config.get("lower_bound", 0)) \
                          .option("upperBound", config.get("upper_bound", 1000000)) \
                          .option("numPartitions", config.get("num_partitions", 10))
        
        return reader.load()
    
    async def _load_from_api(self, config: Dict[str, Any]) -> DataFrame:
        """Load data from REST API"""
        url = config.get("url")
        method = config.get("method", "GET")
        headers = config.get("headers", {})
        params = config.get("params", {})
        
        if not url:
            raise ValueError("API URL is required")
        
        # Use httpx for async HTTP calls
        import httpx
        
        async with httpx.AsyncClient() as client:
            response = await client.request(
                method=method,
                url=url,
                headers=headers,
                params=params
            )
            response.raise_for_status()
            
            data = response.json()
            
            # Convert to DataFrame
            spark = self.lake_manager.spark
            
            if isinstance(data, list):
                # List of records
                df = spark.createDataFrame(data)
            elif isinstance(data, dict) and "data" in data:
                # Wrapped response
                df = spark.createDataFrame(data["data"])
            else:
                # Single record
                df = spark.createDataFrame([data])
            
            return df
    
    async def _load_from_s3(self, config: Dict[str, Any]) -> DataFrame:
        """Load data from S3/MinIO"""
        bucket = config.get("bucket")
        prefix = config.get("prefix", "")
        file_format = config.get("format", "parquet")
        
        if not bucket:
            raise ValueError("Bucket name is required")
        
        # Construct S3 path
        s3_path = f"s3a://{bucket}/{prefix}"
        
        # Use appropriate reader based on format
        config["path"] = s3_path
        config["format"] = file_format
        
        return await self._load_from_file(config)
    
    async def _stream_to_bronze(self,
                              consumer: pulsar.Consumer,
                              dataset_id: str,
                              tenant_id: str,
                              config: Dict[str, Any]) -> None:
        """Stream data to Bronze layer"""
        batch_size = config.get("batch_size", 1000)
        batch_timeout = config.get("batch_timeout", 60)  # seconds
        
        batch = []
        last_flush = datetime.utcnow()
        
        try:
            while True:
                try:
                    # Receive message with timeout
                    msg = consumer.receive(timeout_millis=1000)
                    
                    # Parse message
                    data = json.loads(msg.data().decode('utf-8'))
                    batch.append(data)
                    
                    # Acknowledge message
                    consumer.acknowledge(msg)
                    
                    # Check if batch is ready
                    if len(batch) >= batch_size or \
                       (datetime.utcnow() - last_flush).seconds >= batch_timeout:
                        
                        # Convert to DataFrame and ingest
                        df = self.lake_manager.spark.createDataFrame(batch)
                        
                        await self.lake_manager.ingest_to_bronze(
                            source_data=df,
                            dataset_name=dataset_id,
                            tenant_id=tenant_id
                        )
                        
                        logger.info(f"Streamed {len(batch)} records to Bronze")
                        
                        # Reset batch
                        batch = []
                        last_flush = datetime.utcnow()
                        
                except Exception as e:
                    if "timeout" not in str(e).lower():
                        logger.error(f"Stream processing error: {e}")
                    
                    # Check for timeout and flush if needed
                    if batch and (datetime.utcnow() - last_flush).seconds >= batch_timeout:
                        df = self.lake_manager.spark.createDataFrame(batch)
                        await self.lake_manager.ingest_to_bronze(
                            source_data=df,
                            dataset_name=dataset_id,
                            tenant_id=tenant_id
                        )
                        batch = []
                        last_flush = datetime.utcnow()
                        
        except asyncio.CancelledError:
            # Final flush on cancellation
            if batch:
                df = self.lake_manager.spark.createDataFrame(batch)
                await self.lake_manager.ingest_to_bronze(
                    source_data=df,
                    dataset_name=dataset_id,
                    tenant_id=tenant_id
                )
            raise
        finally:
            consumer.close()
    
    async def _poll_cdc_changes(self,
                              dataset_id: str,
                              config: Dict[str, Any],
                              tenant_id: str,
                              poll_interval: int) -> None:
        """Poll for CDC changes"""
        last_poll_time = datetime.utcnow()
        
        while True:
            try:
                # Sleep until next poll
                await asyncio.sleep(poll_interval)
                
                # Query for changes since last poll
                config["filter"] = f"modified_at > '{last_poll_time}'"
                
                # Load changes
                df = await self._load_from_database(config)
                
                if df.count() > 0:
                    # Ingest changes
                    await self.lake_manager.ingest_to_bronze(
                        source_data=df,
                        dataset_name=f"{dataset_id}_cdc",
                        tenant_id=tenant_id
                    )
                    
                    logger.info(f"CDC: Ingested {df.count()} changes")
                
                last_poll_time = datetime.utcnow()
                
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"CDC polling error: {e}")
                await asyncio.sleep(poll_interval)
    
    async def _get_last_ingestion_timestamp(self, 
                                          dataset_id: str,
                                          tenant_id: str) -> Optional[str]:
        """Get last successful ingestion timestamp"""
        # In production, would query metadata store
        # For now, return None to ingest all data
        return None
    
    async def _update_ingestion_timestamp(self,
                                        dataset_id: str,
                                        tenant_id: str) -> None:
        """Update last ingestion timestamp"""
        # In production, would update metadata store
        pass
    
    def _estimate_dataframe_size(self, df: DataFrame) -> int:
        """Estimate DataFrame size in bytes"""
        # Rough estimation based on row count and schema
        row_count = df.count()
        
        # Estimate bytes per row based on schema
        bytes_per_row = 0
        for field in df.schema.fields:
            if "int" in str(field.dataType).lower():
                bytes_per_row += 8
            elif "string" in str(field.dataType).lower():
                bytes_per_row += 50  # Average string length
            elif "double" in str(field.dataType).lower():
                bytes_per_row += 8
            elif "float" in str(field.dataType).lower():
                bytes_per_row += 4
            elif "boolean" in str(field.dataType).lower():
                bytes_per_row += 1
            else:
                bytes_per_row += 20  # Default
        
        return row_count * bytes_per_row
    
    async def _cleanup_job(self, job_id: str, delay: int = 3600) -> None:
        """Cleanup completed job after delay"""
        await asyncio.sleep(delay)
        
        if job_id in self.active_jobs:
            del self.active_jobs[job_id]
    
    async def stop_streaming_job(self, dataset_id: str) -> bool:
        """Stop a streaming ingestion job"""
        job_key = f"stream_{dataset_id}"
        
        if job_key in self.active_jobs:
            job = self.active_jobs[job_key]
            
            # Cancel task
            task = job.get("task")
            if task and not task.done():
                task.cancel()
            
            # Close consumer
            consumer = job.get("consumer")
            if consumer:
                consumer.close()
            
            del self.active_jobs[job_key]
            
            logger.info(f"Stopped streaming job for {dataset_id}")
            return True
        
        return False
    
    async def get_job_status(self, job_id: str) -> Optional[Dict[str, Any]]:
        """Get status of an ingestion job"""
        if job_id in self.active_jobs:
            job = self.active_jobs[job_id]
            
            return {
                "job_id": job_id,
                "status": job.get("status", "running"),
                "started_at": job.get("started_at"),
                "completed_at": job.get("completed_at"),
                "error": job.get("error"),
                "result": job.get("result")
            }
        
        return None
    
    def get_statistics(self) -> Dict[str, Any]:
        """Get ingestion engine statistics"""
        return {
            "active_jobs": len(self.active_jobs),
            **self.stats
        }
    
    async def shutdown(self) -> None:
        """Shutdown the ingestion engine"""
        # Cancel all active streaming/CDC jobs
        for job_id, job in list(self.active_jobs.items()):
            task = job.get("task")
            if task and not task.done():
                task.cancel()
                
            consumer = job.get("consumer")
            if consumer:
                consumer.close()
        
        # Close Pulsar client
        if self.pulsar_client:
            self.pulsar_client.close()
        
        logger.info("Data ingestion engine shut down") 