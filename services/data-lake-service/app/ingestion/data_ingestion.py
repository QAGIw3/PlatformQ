"""
Data Ingestion Pipeline

Handles ingestion from multiple sources:
- Apache Pulsar streaming
- APIs
- Databases  
- Files (S3, MinIO)
- IoT devices
"""

import asyncio
import json
import logging
from typing import Dict, List, Any, Optional, Union, AsyncIterator
from datetime import datetime, timezone
from dataclasses import dataclass
import hashlib
from abc import ABC, abstractmethod

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import StructType
import pulsar
from pulsar.schema import AvroSchema, JsonSchema
from minio import Minio
import httpx
import asyncpg
from delta import DeltaTable

from ..models.medallion_architecture import MedallionArchitecture, DataLayer
from ..jobs import JobRepository

logger = logging.getLogger(__name__)


@dataclass
class IngestionConfig:
    """Configuration for data ingestion"""
    source_type: str  # pulsar, api, database, file, iot
    source_config: Dict[str, Any]
    target_dataset: str
    ingestion_mode: str  # batch, streaming, incremental
    schedule: Optional[str] = None  # cron expression for batch
    schema: Optional[StructType] = None
    transformations: List[Dict[str, Any]] = None
    quality_checks: List[str] = None


@dataclass
class IngestionJob:
    """Ingestion job tracking"""
    job_id: str
    config: IngestionConfig
    status: str  # running, completed, failed
    started_at: datetime
    completed_at: Optional[datetime] = None
    records_processed: int = 0
    errors: List[str] = None
    bronze_path: Optional[str] = None


class DataSource(ABC):
    """Abstract base class for data sources"""
    
    @abstractmethod
    async def connect(self):
        """Connect to the data source"""
        pass
        
    @abstractmethod
    async def read(self) -> AsyncIterator[Dict[str, Any]]:
        """Read data from the source"""
        pass
        
    @abstractmethod
    async def close(self):
        """Close the connection"""
        pass


class PulsarSource(DataSource):
    """Apache Pulsar data source"""
    
    def __init__(self, config: Dict[str, Any]):
        self.config = config
        self.client = None
        self.consumer = None
        
    async def connect(self):
        """Connect to Pulsar"""
        self.client = pulsar.Client(
            self.config.get("service_url", "pulsar://localhost:6650")
        )
        
        schema = None
        if self.config.get("schema_type") == "avro":
            schema = AvroSchema(self.config.get("schema_definition"))
        elif self.config.get("schema_type") == "json":
            schema = JsonSchema(self.config.get("schema_definition"))
            
        self.consumer = self.client.subscribe(
            self.config["topics"],
            self.config.get("subscription_name", "data-lake-ingestion"),
            consumer_type=pulsar.ConsumerType.Shared,
            schema=schema
        )
        
    async def read(self) -> AsyncIterator[Dict[str, Any]]:
        """Read messages from Pulsar"""
        while True:
            try:
                msg = self.consumer.receive(timeout_millis=1000)
                
                # Parse message
                data = msg.value() if hasattr(msg, 'value') else json.loads(msg.data())
                
                # Add metadata
                yield {
                    "_pulsar_message_id": str(msg.message_id()),
                    "_pulsar_topic": msg.topic_name(),
                    "_pulsar_publish_time": msg.publish_timestamp(),
                    "_pulsar_event_time": msg.event_timestamp(),
                    **data
                }
                
                # Acknowledge message
                self.consumer.acknowledge(msg)
                
            except Exception as e:
                if "timeout" not in str(e).lower():
                    logger.error(f"Error reading from Pulsar: {e}")
                await asyncio.sleep(0.1)
                
    async def close(self):
        """Close Pulsar connection"""
        if self.consumer:
            self.consumer.close()
        if self.client:
            self.client.close()


class APISource(DataSource):
    """REST API data source"""
    
    def __init__(self, config: Dict[str, Any]):
        self.config = config
        self.client = None
        self.headers = config.get("headers", {})
        self.auth = None
        
        if "auth" in config:
            auth_config = config["auth"]
            if auth_config["type"] == "basic":
                self.auth = (auth_config["username"], auth_config["password"])
            elif auth_config["type"] == "bearer":
                self.headers["Authorization"] = f"Bearer {auth_config['token']}"
                
    async def connect(self):
        """Initialize HTTP client"""
        self.client = httpx.AsyncClient(
            timeout=self.config.get("timeout", 30.0),
            headers=self.headers
        )
        
    async def read(self) -> AsyncIterator[Dict[str, Any]]:
        """Read data from API"""
        url = self.config["url"]
        method = self.config.get("method", "GET")
        params = self.config.get("params", {})
        
        # Handle pagination
        page = 1
        has_more = True
        
        while has_more:
            if self.config.get("pagination_type") == "page":
                params[self.config.get("page_param", "page")] = page
                
            response = await self.client.request(
                method=method,
                url=url,
                params=params,
                auth=self.auth
            )
            
            response.raise_for_status()
            data = response.json()
            
            # Extract records based on response structure
            records = data
            if self.config.get("data_path"):
                for key in self.config["data_path"].split("."):
                    records = records.get(key, [])
                    
            if not isinstance(records, list):
                records = [records]
                
            for record in records:
                yield {
                    "_api_source": url,
                    "_api_timestamp": datetime.now(timezone.utc).isoformat(),
                    **record
                }
                
            # Check for more pages
            if self.config.get("pagination_type") == "page":
                total_pages = data.get(self.config.get("total_pages_field", "total_pages"), 1)
                has_more = page < total_pages
                page += 1
            elif self.config.get("pagination_type") == "cursor":
                cursor = data.get(self.config.get("cursor_field", "next_cursor"))
                has_more = cursor is not None
                params[self.config.get("cursor_param", "cursor")] = cursor
            else:
                has_more = False
                
    async def close(self):
        """Close HTTP client"""
        if self.client:
            await self.client.aclose()


class DatabaseSource(DataSource):
    """Database data source (PostgreSQL, MySQL, etc.)"""
    
    def __init__(self, config: Dict[str, Any]):
        self.config = config
        self.connection = None
        
    async def connect(self):
        """Connect to database"""
        db_type = self.config.get("type", "postgresql")
        
        if db_type == "postgresql":
            self.connection = await asyncpg.connect(
                host=self.config["host"],
                port=self.config.get("port", 5432),
                user=self.config["user"],
                password=self.config["password"],
                database=self.config["database"]
            )
        else:
            raise ValueError(f"Unsupported database type: {db_type}")
            
    async def read(self) -> AsyncIterator[Dict[str, Any]]:
        """Read data from database"""
        query = self.config["query"]
        
        # Handle incremental loads
        if self.config.get("incremental_column"):
            # Get last processed value from state store
            last_value = await self._get_last_processed_value()
            
            if last_value:
                query += f" WHERE {self.config['incremental_column']} > '{last_value}'"
                
            query += f" ORDER BY {self.config['incremental_column']}"
            
        # Execute query
        if self.config.get("type") == "postgresql":
            async with self.connection.transaction():
                async for record in self.connection.cursor(query):
                    yield {
                        "_db_source": f"{self.config['host']}/{self.config['database']}",
                        "_db_extracted_at": datetime.now(timezone.utc).isoformat(),
                        **dict(record)
                    }
                    
    async def _get_last_processed_value(self) -> Optional[str]:
        """Get last processed value for incremental loads"""
        # In production, this would read from a state store
        return None
        
    async def close(self):
        """Close database connection"""
        if self.connection:
            await self.connection.close()


class FileSource(DataSource):
    """File-based data source (MinIO, S3, local)"""
    
    def __init__(self, config: Dict[str, Any]):
        self.config = config
        self.minio_client = None
        
    async def connect(self):
        """Connect to file storage"""
        storage_type = self.config.get("storage_type", "minio")
        
        if storage_type == "minio":
            self.minio_client = Minio(
                self.config["endpoint"],
                access_key=self.config["access_key"],
                secret_key=self.config["secret_key"],
                secure=self.config.get("secure", True)
            )
            
    async def read(self) -> AsyncIterator[Dict[str, Any]]:
        """Read files from storage"""
        if self.config.get("storage_type") == "minio":
            bucket = self.config["bucket"]
            prefix = self.config.get("prefix", "")
            
            # List objects
            objects = self.minio_client.list_objects(
                bucket,
                prefix=prefix,
                recursive=True
            )
            
            for obj in objects:
                if obj.is_dir:
                    continue
                    
                # Check file extension
                if not obj.object_name.endswith(tuple(self.config.get("file_extensions", [".json", ".csv", ".parquet"]))):
                    continue
                    
                # Download and parse file
                response = self.minio_client.get_object(bucket, obj.object_name)
                
                if obj.object_name.endswith(".json"):
                    data = json.loads(response.read())
                    if isinstance(data, dict):
                        data = [data]
                        
                    for record in data:
                        yield {
                            "_file_source": f"minio://{bucket}/{obj.object_name}",
                            "_file_modified": obj.last_modified.isoformat(),
                            "_file_size": obj.size,
                            **record
                        }
                        
                elif obj.object_name.endswith(".csv"):
                    df = pd.read_csv(response)
                    for _, row in df.iterrows():
                        yield {
                            "_file_source": f"minio://{bucket}/{obj.object_name}",
                            "_file_modified": obj.last_modified.isoformat(),
                            **row.to_dict()
                        }
                        
                elif obj.object_name.endswith(".parquet"):
                    df = pd.read_parquet(response)
                    for _, row in df.iterrows():
                        yield {
                            "_file_source": f"minio://{bucket}/{obj.object_name}",
                            "_file_modified": obj.last_modified.isoformat(),
                            **row.to_dict()
                        }
                        
                response.close()
                response.release_conn()
                
    async def close(self):
        """Close file storage connection"""
        pass  # MinIO client doesn't need explicit closing


class IoTSource(DataSource):
    """IoT device data source (MQTT, CoAP)"""
    
    def __init__(self, config: Dict[str, Any]):
        self.config = config
        self.client = None
        
    async def connect(self):
        """Connect to IoT gateway"""
        protocol = self.config.get("protocol", "mqtt")
        
        if protocol == "mqtt":
            import asyncio_mqtt
            
            self.client = asyncio_mqtt.Client(
                hostname=self.config["broker"],
                port=self.config.get("port", 1883),
                username=self.config.get("username"),
                password=self.config.get("password")
            )
            await self.client.connect()
            
    async def read(self) -> AsyncIterator[Dict[str, Any]]:
        """Read IoT device data"""
        if self.config.get("protocol") == "mqtt":
            topics = self.config["topics"]
            
            async with self.client.subscribe(topics) as messages:
                async for message in messages:
                    try:
                        payload = json.loads(message.payload.decode())
                        
                        yield {
                            "_iot_topic": str(message.topic),
                            "_iot_timestamp": datetime.now(timezone.utc).isoformat(),
                            "_iot_qos": message.qos,
                            **payload
                        }
                    except Exception as e:
                        logger.error(f"Error parsing IoT message: {e}")
                        
    async def close(self):
        """Close IoT connection"""
        if self.client:
            await self.client.disconnect()


class DataIngestionPipeline:
    """Main data ingestion pipeline"""
    
    def __init__(self,
                 spark: SparkSession,
                 medallion: MedallionArchitecture,
                 job_repository: JobRepository):
        self.spark = spark
        self.medallion = medallion
        self.job_repository = job_repository
        self.source_registry = {
            "pulsar": PulsarSource,
            "api": APISource,
            "database": DatabaseSource,
            "file": FileSource,
            "iot": IoTSource
        }
        
    async def start_ingestion(self, config: IngestionConfig) -> str:
        """Start a new ingestion job"""
        job_id = hashlib.md5(
            f"{config.source_type}_{config.target_dataset}_{datetime.now()}".encode()
        ).hexdigest()
        
        job = IngestionJob(
            job_id=job_id,
            config=config,
            status="running",
            started_at=datetime.now(timezone.utc),
            errors=[]
        )
        
        self.job_repository.add_ingestion_job(job)
        
        # Start ingestion based on mode
        if config.ingestion_mode == "streaming":
            asyncio.create_task(self._run_streaming_ingestion(job))
        elif config.ingestion_mode == "batch":
            asyncio.create_task(self._run_batch_ingestion(job))
        elif config.ingestion_mode == "incremental":
            asyncio.create_task(self._run_incremental_ingestion(job))
        else:
            raise ValueError(f"Unknown ingestion mode: {config.ingestion_mode}")
            
        logger.info(f"Started ingestion job {job_id} for {config.target_dataset}")
        return job_id
        
    async def _run_streaming_ingestion(self, job: IngestionJob):
        """Run streaming ingestion"""
        source_class = self.source_registry.get(job.config.source_type)
        if not source_class:
            job.status = "failed"
            job.errors.append(f"Unknown source type: {job.config.source_type}")
            return
            
        source = source_class(job.config.source_config)
        
        try:
            await source.connect()
            
            # Buffer for micro-batching
            buffer = []
            buffer_size = job.config.source_config.get("buffer_size", 1000)
            buffer_timeout = job.config.source_config.get("buffer_timeout", 60)
            last_flush = datetime.now()
            
            async for record in source.read():
                buffer.append(record)
                job.records_processed += 1
                
                # Flush buffer
                if len(buffer) >= buffer_size or \
                   (datetime.now() - last_flush).seconds >= buffer_timeout:
                    
                    await self._flush_to_bronze(job, buffer)
                    buffer = []
                    last_flush = datetime.now()
                    
            # Final flush
            if buffer:
                await self._flush_to_bronze(job, buffer)
                
        except Exception as e:
            logger.error(f"Streaming ingestion error: {e}")
            job.status = "failed"
            job.errors.append(str(e))
        finally:
            await source.close()
            job.completed_at = datetime.now(timezone.utc)
            
    async def _run_batch_ingestion(self, job: IngestionJob):
        """Run batch ingestion"""
        source_class = self.source_registry.get(job.config.source_type)
        if not source_class:
            job.status = "failed"
            job.errors.append(f"Unknown source type: {job.config.source_type}")
            return
            
        source = source_class(job.config.source_config)
        
        try:
            await source.connect()
            
            # Collect all records
            records = []
            async for record in source.read():
                records.append(record)
                job.records_processed += 1
                
            # Write to Bronze
            if records:
                await self._flush_to_bronze(job, records)
                
            job.status = "completed"
            
        except Exception as e:
            logger.error(f"Batch ingestion error: {e}")
            job.status = "failed"
            job.errors.append(str(e))
        finally:
            await source.close()
            job.completed_at = datetime.now(timezone.utc)
            
    async def _run_incremental_ingestion(self, job: IngestionJob):
        """Run incremental ingestion"""
        # Similar to batch but tracks state for incremental loads
        await self._run_batch_ingestion(job)
        
        # Update state store with last processed value
        if job.status == "completed":
            await self._update_incremental_state(job)
            
    async def _flush_to_bronze(self, job: IngestionJob, records: List[Dict[str, Any]]):
        """Flush records to Bronze layer"""
        try:
            # Apply transformations if configured
            if job.config.transformations:
                records = self._apply_transformations(records, job.config.transformations)
                
            # Convert to Pandas DataFrame for easier handling
            df = pd.DataFrame(records)
            
            # Add ingestion metadata
            df["_ingestion_job_id"] = job.job_id
            df["_ingestion_timestamp"] = datetime.now(timezone.utc).isoformat()
            
            # Write to Bronze layer
            bronze_path = self.medallion.ingest_to_bronze(
                data=df,
                dataset_name=job.config.target_dataset,
                format="parquet" if not self.medallion.delta_enabled else "delta",
                metadata={
                    "source_type": job.config.source_type,
                    "ingestion_mode": job.config.ingestion_mode,
                    "job_id": job.job_id
                }
            )
            
            job.bronze_path = bronze_path
            logger.info(f"Flushed {len(records)} records to Bronze: {bronze_path}")
            
        except Exception as e:
            logger.error(f"Error flushing to Bronze: {e}")
            job.errors.append(f"Bronze flush error: {str(e)}")
            
    def _apply_transformations(self,
                             records: List[Dict[str, Any]],
                             transformations: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """Apply transformations to records"""
        for transform in transformations:
            transform_type = transform["type"]
            
            if transform_type == "rename":
                # Rename fields
                for record in records:
                    for old_name, new_name in transform["mappings"].items():
                        if old_name in record:
                            record[new_name] = record.pop(old_name)
                            
            elif transform_type == "filter":
                # Filter records
                condition = transform["condition"]
                records = [r for r in records if eval(condition, {"record": r})]
                
            elif transform_type == "enrich":
                # Add computed fields
                for record in records:
                    for field_name, expression in transform["fields"].items():
                        record[field_name] = eval(expression, {"record": record})
                        
            elif transform_type == "flatten":
                # Flatten nested structures
                flattened = []
                for record in records:
                    flattened.append(self._flatten_dict(record, transform.get("separator", "_")))
                records = flattened
                
        return records
        
    def _flatten_dict(self, d: Dict, separator: str = "_", parent_key: str = "") -> Dict:
        """Flatten nested dictionary"""
        items = []
        for k, v in d.items():
            new_key = f"{parent_key}{separator}{k}" if parent_key else k
            if isinstance(v, dict):
                items.extend(self._flatten_dict(v, separator, new_key).items())
            elif isinstance(v, list):
                items.append((new_key, json.dumps(v)))
            else:
                items.append((new_key, v))
        return dict(items)
        
    async def _update_incremental_state(self, job: IngestionJob):
        """Update state for incremental ingestion"""
        # In production, this would persist to a state store
        logger.info(f"Updated incremental state for job {job.job_id}")
        
    async def stop_ingestion(self, job_id: str):
        """Stop an active ingestion job"""
        job = self.job_repository.get_ingestion_job(job_id)
        if job:
            job.status = "stopped"
            job.completed_at = datetime.now(timezone.utc)
            logger.info(f"Stopped ingestion job {job_id}")
            
    def get_job_status(self, job_id: str) -> Optional[IngestionJob]:
        """Get status of an ingestion job"""
        return self.job_repository.get_ingestion_job(job_id)
        
    def list_active_jobs(self) -> List[IngestionJob]:
        """List all active ingestion jobs"""
        return self.job_repository.list_active_ingestion_jobs() 