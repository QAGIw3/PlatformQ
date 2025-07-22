"""
Data Compaction Service for Data Lake
Handles file compaction, format optimization, and storage efficiency
"""

from typing import Dict, Any, List, Optional, Tuple, Set
from datetime import datetime, timedelta
from dataclasses import dataclass, field
from enum import Enum
import asyncio
import logging
import json
import os
import tempfile
from pathlib import Path
import pyarrow as pa
import pyarrow.parquet as pq
import pyarrow.dataset as ds
import pandas as pd
import fastavro
from minio import Minio
from minio.error import S3Error
import aiofiles
import hashlib
from collections import defaultdict

from platformq_shared.consul.consul_client import ConsulClient

logger = logging.getLogger(__name__)


class CompactionStrategy(Enum):
    """Compaction strategies"""
    TIME_BASED = "time_based"
    SIZE_BASED = "size_based"
    FILE_COUNT_BASED = "file_count_based"
    HYBRID = "hybrid"


class FileFormat(Enum):
    """Supported file formats"""
    PARQUET = "parquet"
    AVRO = "avro"
    ORC = "orc"
    JSON = "json"
    CSV = "csv"


class CompactionStatus(Enum):
    """Compaction job status"""
    PENDING = "pending"
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"
    CANCELLED = "cancelled"


@dataclass
class CompactionConfig:
    """Compaction configuration"""
    strategy: CompactionStrategy = CompactionStrategy.HYBRID
    min_file_size_mb: float = 128  # Min size for compaction
    max_file_size_mb: float = 1024  # Target compacted file size
    min_files_to_compact: int = 5
    time_threshold_hours: int = 24
    target_format: FileFormat = FileFormat.PARQUET
    compression: str = "snappy"
    partition_cols: List[str] = field(default_factory=list)
    sort_cols: List[str] = field(default_factory=list)
    enable_auto_compaction: bool = True
    compaction_schedule: str = "0 2 * * *"  # 2 AM daily
    retention_days: int = 7  # Keep original files for N days
    use_avro_for_schemas: bool = True  # Use Avro for complex schemas


@dataclass
class CompactionJob:
    """Represents a compaction job"""
    job_id: str
    dataset: str
    source_path: str
    target_path: str
    status: CompactionStatus
    strategy: CompactionStrategy
    started_at: Optional[datetime] = None
    completed_at: Optional[datetime] = None
    files_processed: int = 0
    bytes_processed: int = 0
    bytes_saved: int = 0
    error_message: Optional[str] = None
    metadata: Dict[str, Any] = field(default_factory=dict)


@dataclass
class FileMetadata:
    """File metadata for compaction decisions"""
    path: str
    size_bytes: int
    created_at: datetime
    format: FileFormat
    partition_values: Dict[str, str] = field(default_factory=dict)
    row_count: Optional[int] = None
    schema_hash: Optional[str] = None


class DataCompactionService:
    """
    Service for managing data compaction in the data lake
    """
    
    def __init__(
        self,
        minio_client: Minio,
        consul_client: ConsulClient,
        config: Optional[CompactionConfig] = None
    ):
        self.minio = minio_client
        self.consul = consul_client
        self.config = config or CompactionConfig()
        self._active_jobs: Dict[str, CompactionJob] = {}
        self._job_queue: asyncio.Queue = asyncio.Queue()
        self._workers: List[asyncio.Task] = []
        self._running = False
        self._file_cache: Dict[str, List[FileMetadata]] = {}
        self._schema_registry: Dict[str, pa.Schema] = {}
        
    async def initialize(self):
        """Initialize the compaction service"""
        logger.info("Initializing data compaction service")
        
        # Load configuration from Consul
        await self._load_configuration()
        
        # Register service in Consul
        await self._register_service()
        
        # Start background workers
        await self._start_workers()
        
        # Schedule auto-compaction if enabled
        if self.config.enable_auto_compaction:
            asyncio.create_task(self._auto_compaction_scheduler())
            
        logger.info("Data compaction service initialized")
        
    async def shutdown(self):
        """Shutdown the compaction service"""
        logger.info("Shutting down data compaction service")
        
        self._running = False
        
        # Cancel active jobs
        for job_id in list(self._active_jobs.keys()):
            await self.cancel_job(job_id)
            
        # Stop workers
        for worker in self._workers:
            worker.cancel()
            
        # Deregister from Consul
        await self.consul.deregister_service("data-compaction-service")
        
        logger.info("Data compaction service shutdown complete")
        
    async def submit_compaction_job(
        self,
        dataset: str,
        source_path: str,
        target_path: Optional[str] = None,
        strategy: Optional[CompactionStrategy] = None,
        config_overrides: Optional[Dict[str, Any]] = None
    ) -> str:
        """Submit a new compaction job"""
        job_id = f"compact-{dataset}-{datetime.utcnow().timestamp()}"
        
        if not target_path:
            target_path = f"{source_path}/compacted"
            
        job = CompactionJob(
            job_id=job_id,
            dataset=dataset,
            source_path=source_path,
            target_path=target_path,
            status=CompactionStatus.PENDING,
            strategy=strategy or self.config.strategy,
            metadata=config_overrides or {}
        )
        
        # Store job in Consul
        await self._store_job(job)
        
        # Add to queue
        await self._job_queue.put(job)
        
        logger.info(f"Compaction job submitted: {job_id}")
        return job_id
        
    async def get_job_status(self, job_id: str) -> Optional[CompactionJob]:
        """Get compaction job status"""
        # Check active jobs first
        if job_id in self._active_jobs:
            return self._active_jobs[job_id]
            
        # Check Consul for completed jobs
        return await self._load_job(job_id)
        
    async def list_jobs(
        self,
        dataset: Optional[str] = None,
        status: Optional[CompactionStatus] = None,
        limit: int = 100
    ) -> List[CompactionJob]:
        """List compaction jobs"""
        jobs = []
        
        # Get jobs from Consul
        prefix = "compaction/jobs/"
        if dataset:
            prefix += f"{dataset}/"
            
        keys = await self.consul.kv_list(prefix)
        
        for key in keys[:limit]:
            job_data = await self.consul.kv_get(key)
            if job_data:
                job = self._deserialize_job(json.loads(job_data))
                if not status or job.status == status:
                    jobs.append(job)
                    
        return jobs
        
    async def cancel_job(self, job_id: str) -> bool:
        """Cancel a compaction job"""
        if job_id in self._active_jobs:
            job = self._active_jobs[job_id]
            job.status = CompactionStatus.CANCELLED
            await self._store_job(job)
            del self._active_jobs[job_id]
            logger.info(f"Cancelled compaction job: {job_id}")
            return True
        return False
        
    async def analyze_dataset(
        self,
        dataset: str,
        path: str
    ) -> Dict[str, Any]:
        """Analyze dataset for compaction opportunities"""
        logger.info(f"Analyzing dataset {dataset} at {path}")
        
        # List files in the path
        files = await self._list_files(path)
        
        # Group files by partition
        partitions = defaultdict(list)
        total_size = 0
        small_files = 0
        
        for file in files:
            total_size += file.size_bytes
            if file.size_bytes < self.config.min_file_size_mb * 1024 * 1024:
                small_files += 1
                
            partition_key = tuple(sorted(file.partition_values.items()))
            partitions[partition_key].append(file)
            
        # Calculate compaction opportunities
        compaction_candidates = []
        potential_savings = 0
        
        for partition, files in partitions.items():
            if len(files) >= self.config.min_files_to_compact:
                partition_size = sum(f.size_bytes for f in files)
                if partition_size > self.config.max_file_size_mb * 1024 * 1024:
                    # Estimate savings from compaction
                    estimated_size = partition_size * 0.8  # 20% compression estimate
                    savings = partition_size - estimated_size
                    potential_savings += savings
                    
                    compaction_candidates.append({
                        "partition": dict(partition),
                        "file_count": len(files),
                        "total_size_mb": partition_size / 1024 / 1024,
                        "estimated_savings_mb": savings / 1024 / 1024
                    })
                    
        analysis = {
            "dataset": dataset,
            "path": path,
            "total_files": len(files),
            "total_size_gb": total_size / 1024 / 1024 / 1024,
            "small_files": small_files,
            "partitions": len(partitions),
            "compaction_candidates": compaction_candidates,
            "potential_savings_gb": potential_savings / 1024 / 1024 / 1024,
            "recommended_strategy": self._recommend_strategy(files, partitions)
        }
        
        return analysis
        
    async def compact_files(
        self,
        job: CompactionJob,
        files: List[FileMetadata],
        output_path: str
    ) -> Tuple[str, int]:
        """Compact a list of files into a single file"""
        logger.info(f"Compacting {len(files)} files for job {job.job_id}")
        
        # Determine output format
        output_format = self.config.target_format
        
        # Use Avro for complex schemas if configured
        if self.config.use_avro_for_schemas and self._has_complex_schema(files):
            output_format = FileFormat.AVRO
            
        with tempfile.TemporaryDirectory() as tmpdir:
            # Download and merge files
            if output_format == FileFormat.PARQUET:
                output_file = await self._compact_to_parquet(
                    files, tmpdir, job
                )
            elif output_format == FileFormat.AVRO:
                output_file = await self._compact_to_avro(
                    files, tmpdir, job
                )
            else:
                raise ValueError(f"Unsupported output format: {output_format}")
                
            # Upload compacted file to MinIO
            bucket, key = self._parse_s3_path(output_path)
            
            file_size = os.path.getsize(output_file)
            
            await self._upload_file(
                output_file,
                bucket,
                key,
                metadata={
                    "compaction_job": job.job_id,
                    "source_files": str(len(files)),
                    "compression": self.config.compression
                }
            )
            
            logger.info(f"Uploaded compacted file: {output_path} ({file_size} bytes)")
            
            return output_path, file_size
            
    async def _compact_to_parquet(
        self,
        files: List[FileMetadata],
        tmpdir: str,
        job: CompactionJob
    ) -> str:
        """Compact files to Parquet format"""
        output_file = os.path.join(tmpdir, "compacted.parquet")
        
        # Download files and create dataset
        file_paths = []
        for file in files:
            local_path = await self._download_file(file.path, tmpdir)
            file_paths.append(local_path)
            
        # Read all files into a single dataset
        dataset = ds.dataset(file_paths, format="parquet")
        
        # Apply sorting if configured
        if self.config.sort_cols:
            # Read into table and sort
            table = dataset.to_table()
            table = table.sort_by(self.config.sort_cols)
        else:
            table = dataset.to_table()
            
        # Write compacted file
        pq.write_table(
            table,
            output_file,
            compression=self.config.compression,
            use_dictionary=True,
            version='2.6'
        )
        
        return output_file
        
    async def _compact_to_avro(
        self,
        files: List[FileMetadata],
        tmpdir: str,
        job: CompactionJob
    ) -> str:
        """Compact files to Avro format"""
        output_file = os.path.join(tmpdir, "compacted.avro")
        
        # Get or infer schema
        schema = await self._get_avro_schema(files)
        
        records = []
        
        # Read all files
        for file in files:
            local_path = await self._download_file(file.path, tmpdir)
            
            if file.format == FileFormat.AVRO:
                with open(local_path, 'rb') as f:
                    reader = fastavro.reader(f)
                    records.extend(reader)
            else:
                # Convert to Avro-compatible format
                df = self._read_file(local_path, file.format)
                records.extend(df.to_dict('records'))
                
        # Sort if configured
        if self.config.sort_cols:
            records.sort(key=lambda r: tuple(r.get(col) for col in self.config.sort_cols))
            
        # Write Avro file
        with open(output_file, 'wb') as f:
            fastavro.writer(f, schema, records)
            
        return output_file
        
    async def _worker(self, worker_id: int):
        """Background worker for processing compaction jobs"""
        logger.info(f"Compaction worker {worker_id} started")
        
        while self._running:
            try:
                # Get job from queue with timeout
                job = await asyncio.wait_for(
                    self._job_queue.get(),
                    timeout=5.0
                )
                
                # Process job
                await self._process_job(job)
                
            except asyncio.TimeoutError:
                continue
            except Exception as e:
                logger.error(f"Worker {worker_id} error: {e}")
                
        logger.info(f"Compaction worker {worker_id} stopped")
        
    async def _process_job(self, job: CompactionJob):
        """Process a compaction job"""
        logger.info(f"Processing compaction job: {job.job_id}")
        
        job.status = CompactionStatus.RUNNING
        job.started_at = datetime.utcnow()
        self._active_jobs[job.job_id] = job
        await self._store_job(job)
        
        try:
            # List files to compact
            files = await self._list_files(job.source_path)
            
            # Filter files based on strategy
            files_to_compact = await self._select_files_for_compaction(
                files, job.strategy
            )
            
            if not files_to_compact:
                logger.info(f"No files to compact for job {job.job_id}")
                job.status = CompactionStatus.COMPLETED
                job.completed_at = datetime.utcnow()
                await self._store_job(job)
                return
                
            # Group files by partition
            partitioned_files = self._partition_files(files_to_compact)
            
            total_bytes_processed = 0
            total_bytes_saved = 0
            
            # Compact each partition
            for partition, partition_files in partitioned_files.items():
                # Generate output path
                output_path = self._generate_output_path(
                    job.target_path,
                    partition
                )
                
                # Compact files
                compacted_path, compacted_size = await self.compact_files(
                    job, partition_files, output_path
                )
                
                # Calculate savings
                original_size = sum(f.size_bytes for f in partition_files)
                saved = original_size - compacted_size
                
                total_bytes_processed += original_size
                total_bytes_saved += saved
                
                # Mark original files for deletion (after retention period)
                await self._mark_files_for_deletion(
                    partition_files,
                    job.job_id
                )
                
            # Update job status
            job.status = CompactionStatus.COMPLETED
            job.completed_at = datetime.utcnow()
            job.files_processed = len(files_to_compact)
            job.bytes_processed = total_bytes_processed
            job.bytes_saved = total_bytes_saved
            
            logger.info(
                f"Compaction job {job.job_id} completed: "
                f"processed {job.files_processed} files, "
                f"saved {job.bytes_saved / 1024 / 1024:.2f} MB"
            )
            
        except Exception as e:
            logger.error(f"Compaction job {job.job_id} failed: {e}")
            job.status = CompactionStatus.FAILED
            job.error_message = str(e)
            job.completed_at = datetime.utcnow()
            
        finally:
            # Remove from active jobs
            self._active_jobs.pop(job.job_id, None)
            await self._store_job(job)
            
    async def _list_files(self, path: str) -> List[FileMetadata]:
        """List files in a given path"""
        # Check cache first
        if path in self._file_cache:
            return self._file_cache[path]
            
        files = []
        bucket, prefix = self._parse_s3_path(path)
        
        try:
            objects = self.minio.list_objects(
                bucket,
                prefix=prefix,
                recursive=True
            )
            
            for obj in objects:
                # Skip directories
                if obj.object_name.endswith('/'):
                    continue
                    
                # Parse file metadata
                file_meta = FileMetadata(
                    path=f"s3://{bucket}/{obj.object_name}",
                    size_bytes=obj.size,
                    created_at=obj.last_modified,
                    format=self._detect_format(obj.object_name),
                    partition_values=self._extract_partition_values(obj.object_name)
                )
                
                files.append(file_meta)
                
        except S3Error as e:
            logger.error(f"Failed to list files in {path}: {e}")
            
        # Cache results
        self._file_cache[path] = files
        
        return files
        
    async def _select_files_for_compaction(
        self,
        files: List[FileMetadata],
        strategy: CompactionStrategy
    ) -> List[FileMetadata]:
        """Select files for compaction based on strategy"""
        selected = []
        
        if strategy == CompactionStrategy.SIZE_BASED:
            # Select small files
            selected = [
                f for f in files
                if f.size_bytes < self.config.min_file_size_mb * 1024 * 1024
            ]
            
        elif strategy == CompactionStrategy.FILE_COUNT_BASED:
            # Select if too many files
            if len(files) >= self.config.min_files_to_compact:
                selected = files
                
        elif strategy == CompactionStrategy.TIME_BASED:
            # Select old files
            threshold = datetime.utcnow() - timedelta(
                hours=self.config.time_threshold_hours
            )
            selected = [f for f in files if f.created_at < threshold]
            
        elif strategy == CompactionStrategy.HYBRID:
            # Combine multiple strategies
            small_files = [
                f for f in files
                if f.size_bytes < self.config.min_file_size_mb * 1024 * 1024
            ]
            
            old_files = [
                f for f in files
                if f.created_at < datetime.utcnow() - timedelta(
                    hours=self.config.time_threshold_hours
                )
            ]
            
            # Union of small and old files
            selected = list(set(small_files + old_files))
            
        return selected
        
    def _partition_files(
        self,
        files: List[FileMetadata]
    ) -> Dict[Tuple, List[FileMetadata]]:
        """Group files by partition"""
        partitions = defaultdict(list)
        
        for file in files:
            partition_key = tuple(sorted(file.partition_values.items()))
            partitions[partition_key].append(file)
            
        return dict(partitions)
        
    def _generate_output_path(
        self,
        base_path: str,
        partition: Tuple
    ) -> str:
        """Generate output path for compacted file"""
        timestamp = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
        
        # Add partition values to path
        path_parts = [base_path.rstrip('/')]
        
        for key, value in partition:
            path_parts.append(f"{key}={value}")
            
        # Add filename
        path_parts.append(f"compacted_{timestamp}.{self.config.target_format.value}")
        
        return '/'.join(path_parts)
        
    async def _download_file(self, s3_path: str, local_dir: str) -> str:
        """Download file from S3 to local directory"""
        bucket, key = self._parse_s3_path(s3_path)
        local_path = os.path.join(local_dir, os.path.basename(key))
        
        self.minio.fget_object(bucket, key, local_path)
        
        return local_path
        
    async def _upload_file(
        self,
        local_path: str,
        bucket: str,
        key: str,
        metadata: Optional[Dict[str, str]] = None
    ):
        """Upload file to S3"""
        self.minio.fput_object(
            bucket,
            key,
            local_path,
            metadata=metadata
        )
        
    def _parse_s3_path(self, path: str) -> Tuple[str, str]:
        """Parse S3 path into bucket and key"""
        if path.startswith("s3://"):
            path = path[5:]
            
        parts = path.split('/', 1)
        bucket = parts[0]
        key = parts[1] if len(parts) > 1 else ""
        
        return bucket, key
        
    def _detect_format(self, filename: str) -> FileFormat:
        """Detect file format from filename"""
        ext = filename.lower().split('.')[-1]
        
        format_map = {
            'parquet': FileFormat.PARQUET,
            'avro': FileFormat.AVRO,
            'orc': FileFormat.ORC,
            'json': FileFormat.JSON,
            'csv': FileFormat.CSV
        }
        
        return format_map.get(ext, FileFormat.PARQUET)
        
    def _extract_partition_values(self, path: str) -> Dict[str, str]:
        """Extract partition values from path"""
        partition_values = {}
        
        parts = path.split('/')
        for part in parts:
            if '=' in part:
                key, value = part.split('=', 1)
                partition_values[key] = value
                
        return partition_values
        
    def _has_complex_schema(self, files: List[FileMetadata]) -> bool:
        """Check if files have complex schema suitable for Avro"""
        # Simple heuristic - can be enhanced
        return any(
            f.format in [FileFormat.JSON, FileFormat.CSV]
            for f in files
        )
        
    def _read_file(self, path: str, format: FileFormat) -> pd.DataFrame:
        """Read file into DataFrame"""
        if format == FileFormat.PARQUET:
            return pd.read_parquet(path)
        elif format == FileFormat.CSV:
            return pd.read_csv(path)
        elif format == FileFormat.JSON:
            return pd.read_json(path)
        else:
            raise ValueError(f"Unsupported format: {format}")
            
    async def _get_avro_schema(self, files: List[FileMetadata]) -> dict:
        """Get or infer Avro schema for files"""
        # Try to get schema from registry
        schema_hash = files[0].schema_hash if files else None
        
        if schema_hash and schema_hash in self._schema_registry:
            return self._schema_registry[schema_hash]
            
        # Infer schema from first file
        first_file = files[0] if files else None
        if not first_file:
            raise ValueError("No files to infer schema from")
            
        local_path = await self._download_file(first_file.path, tempfile.gettempdir())
        
        # Read file and infer schema
        df = self._read_file(local_path, first_file.format)
        
        # Convert to Avro schema
        schema = self._dataframe_to_avro_schema(df, "CompactedData")
        
        # Cache schema
        if schema_hash:
            self._schema_registry[schema_hash] = schema
            
        # Clean up
        os.remove(local_path)
        
        return schema
        
    def _dataframe_to_avro_schema(self, df: pd.DataFrame, name: str) -> dict:
        """Convert DataFrame to Avro schema"""
        fields = []
        
        for col, dtype in df.dtypes.items():
            avro_type = "string"  # Default
            
            if dtype == 'int64':
                avro_type = "long"
            elif dtype == 'int32':
                avro_type = "int"
            elif dtype == 'float64':
                avro_type = "double"
            elif dtype == 'float32':
                avro_type = "float"
            elif dtype == 'bool':
                avro_type = "boolean"
                
            fields.append({
                "name": col,
                "type": ["null", avro_type],
                "default": None
            })
            
        return {
            "type": "record",
            "name": name,
            "fields": fields
        }
        
    def _recommend_strategy(
        self,
        files: List[FileMetadata],
        partitions: Dict
    ) -> CompactionStrategy:
        """Recommend compaction strategy based on file characteristics"""
        avg_file_size = sum(f.size_bytes for f in files) / len(files) if files else 0
        
        # Many small files -> size-based
        if avg_file_size < 64 * 1024 * 1024:  # 64MB
            return CompactionStrategy.SIZE_BASED
            
        # Too many files -> file count based
        if len(files) > 1000:
            return CompactionStrategy.FILE_COUNT_BASED
            
        # Mix of issues -> hybrid
        return CompactionStrategy.HYBRID
        
    async def _mark_files_for_deletion(
        self,
        files: List[FileMetadata],
        job_id: str
    ):
        """Mark files for deletion after retention period"""
        deletion_date = datetime.utcnow() + timedelta(days=self.config.retention_days)
        
        for file in files:
            await self.consul.kv_put(
                f"compaction/deletions/{deletion_date.date()}/{file.path}",
                json.dumps({
                    "job_id": job_id,
                    "scheduled_deletion": deletion_date.isoformat(),
                    "original_size": file.size_bytes
                })
            )
            
    async def _auto_compaction_scheduler(self):
        """Automatic compaction scheduler"""
        logger.info("Auto-compaction scheduler started")
        
        while self._running:
            try:
                # Get datasets from Consul
                datasets = await self._get_registered_datasets()
                
                for dataset in datasets:
                    # Analyze dataset
                    analysis = await self.analyze_dataset(
                        dataset["name"],
                        dataset["path"]
                    )
                    
                    # Submit job if needed
                    if analysis["compaction_candidates"]:
                        await self.submit_compaction_job(
                            dataset=dataset["name"],
                            source_path=dataset["path"],
                            strategy=analysis["recommended_strategy"]
                        )
                        
                # Wait for next schedule
                await asyncio.sleep(3600)  # Check hourly
                
            except Exception as e:
                logger.error(f"Auto-compaction scheduler error: {e}")
                await asyncio.sleep(60)
                
    async def _get_registered_datasets(self) -> List[Dict[str, str]]:
        """Get registered datasets from Consul"""
        datasets = []
        
        keys = await self.consul.kv_list("datasets/")
        for key in keys:
            data = await self.consul.kv_get(key)
            if data:
                dataset_info = json.loads(data)
                if dataset_info.get("compaction_enabled", True):
                    datasets.append({
                        "name": dataset_info["name"],
                        "path": dataset_info["path"]
                    })
                    
        return datasets
        
    async def _load_configuration(self):
        """Load configuration from Consul"""
        config_data = await self.consul.kv_get("config/data-compaction")
        
        if config_data:
            config = json.loads(config_data)
            
            # Update configuration
            self.config.min_file_size_mb = config.get(
                "min_file_size_mb",
                self.config.min_file_size_mb
            )
            self.config.max_file_size_mb = config.get(
                "max_file_size_mb",
                self.config.max_file_size_mb
            )
            self.config.compression = config.get(
                "compression",
                self.config.compression
            )
            self.config.enable_auto_compaction = config.get(
                "enable_auto_compaction",
                self.config.enable_auto_compaction
            )
            
    async def _register_service(self):
        """Register service in Consul"""
        await self.consul.register_service(
            name="data-compaction-service",
            service_id="data-compaction-1",
            address=os.getenv("SERVICE_HOST", "localhost"),
            port=int(os.getenv("SERVICE_PORT", "8000")),
            tags=["data-lake", "compaction", "storage-optimization"],
            meta={
                "version": "1.0.0",
                "capabilities": "parquet,avro,auto-compaction"
            }
        )
        
    async def _start_workers(self):
        """Start background workers"""
        self._running = True
        
        # Start configurable number of workers
        num_workers = int(os.getenv("COMPACTION_WORKERS", "3"))
        
        for i in range(num_workers):
            worker = asyncio.create_task(self._worker(i))
            self._workers.append(worker)
            
    async def _store_job(self, job: CompactionJob):
        """Store job in Consul"""
        key = f"compaction/jobs/{job.dataset}/{job.job_id}"
        await self.consul.kv_put(key, json.dumps(self._serialize_job(job)))
        
    async def _load_job(self, job_id: str) -> Optional[CompactionJob]:
        """Load job from Consul"""
        # Search for job
        keys = await self.consul.kv_list(f"compaction/jobs/")
        
        for key in keys:
            if job_id in key:
                data = await self.consul.kv_get(key)
                if data:
                    return self._deserialize_job(json.loads(data))
                    
        return None
        
    def _serialize_job(self, job: CompactionJob) -> dict:
        """Serialize job to dict"""
        return {
            "job_id": job.job_id,
            "dataset": job.dataset,
            "source_path": job.source_path,
            "target_path": job.target_path,
            "status": job.status.value,
            "strategy": job.strategy.value,
            "started_at": job.started_at.isoformat() if job.started_at else None,
            "completed_at": job.completed_at.isoformat() if job.completed_at else None,
            "files_processed": job.files_processed,
            "bytes_processed": job.bytes_processed,
            "bytes_saved": job.bytes_saved,
            "error_message": job.error_message,
            "metadata": job.metadata
        }
        
    def _deserialize_job(self, data: dict) -> CompactionJob:
        """Deserialize job from dict"""
        job = CompactionJob(
            job_id=data["job_id"],
            dataset=data["dataset"],
            source_path=data["source_path"],
            target_path=data["target_path"],
            status=CompactionStatus(data["status"]),
            strategy=CompactionStrategy(data["strategy"]),
            files_processed=data.get("files_processed", 0),
            bytes_processed=data.get("bytes_processed", 0),
            bytes_saved=data.get("bytes_saved", 0),
            error_message=data.get("error_message"),
            metadata=data.get("metadata", {})
        )
        
        if data.get("started_at"):
            job.started_at = datetime.fromisoformat(data["started_at"])
        if data.get("completed_at"):
            job.completed_at = datetime.fromisoformat(data["completed_at"])
            
        return job 