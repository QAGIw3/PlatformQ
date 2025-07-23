"""
Asset discovery engine for catalog.

Provides automated discovery of data assets from various sources.
"""

import re
import uuid
from abc import ABC, abstractmethod
from typing import Any, Dict, List, Optional, Set, Union, Pattern
from datetime import datetime
from enum import Enum
from dataclasses import dataclass, field
import asyncio
from concurrent.futures import ThreadPoolExecutor

from ..caching import CacheManager
from ..events import EventBus, Event
from ...monitoring import StructuredLogger

logger = StructuredLogger.get_logger(__name__)


class AssetType(str, Enum):
    """Types of discoverable assets"""
    TABLE = "table"
    FILE = "file"
    STREAM = "stream"
    API = "api"
    DATABASE = "database"
    SCHEMA = "schema"
    TOPIC = "topic"
    INDEX = "index"
    MODEL = "model"
    DASHBOARD = "dashboard"
    REPORT = "report"
    UNKNOWN = "unknown"


class DiscoveryStatus(str, Enum):
    """Discovery job status"""
    PENDING = "pending"
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"
    CANCELLED = "cancelled"


@dataclass
class DataSource:
    """Data source configuration"""
    id: str = field(default_factory=lambda: str(uuid.uuid4()))
    name: str = ""
    type: str = ""  # s3, database, kafka, etc.
    connection_config: Dict[str, Any] = field(default_factory=dict)
    discovery_config: Dict[str, Any] = field(default_factory=dict)
    credentials: Optional[Dict[str, Any]] = None
    tags: List[str] = field(default_factory=list)
    is_active: bool = True
    
    def get_connection_string(self) -> str:
        """Build connection string"""
        # Implementation depends on source type
        if self.type == "database":
            config = self.connection_config
            return f"{config.get('driver')}://{config.get('host')}:{config.get('port')}/{config.get('database')}"
        elif self.type == "s3":
            return f"s3://{self.connection_config.get('bucket')}"
        else:
            return self.connection_config.get("url", "")


@dataclass
class DiscoveryPattern:
    """Pattern for asset discovery"""
    id: str = field(default_factory=lambda: str(uuid.uuid4()))
    name: str = ""
    pattern_type: str = "regex"  # regex, glob, prefix
    pattern: str = ""
    asset_type: AssetType = AssetType.UNKNOWN
    metadata_extractors: List[Dict[str, Any]] = field(default_factory=list)
    
    def matches(self, path: str) -> bool:
        """Check if path matches pattern"""
        if self.pattern_type == "regex":
            return bool(re.match(self.pattern, path))
        elif self.pattern_type == "glob":
            import fnmatch
            return fnmatch.fnmatch(path, self.pattern)
        elif self.pattern_type == "prefix":
            return path.startswith(self.pattern)
        return False


@dataclass
class DiscoveryResult:
    """Result of discovery operation"""
    asset_id: str = field(default_factory=lambda: str(uuid.uuid4()))
    name: str = ""
    path: str = ""
    asset_type: AssetType = AssetType.UNKNOWN
    source_id: str = ""
    metadata: Dict[str, Any] = field(default_factory=dict)
    discovered_at: datetime = field(default_factory=datetime.utcnow)
    size_bytes: Optional[int] = None
    modified_at: Optional[datetime] = None
    schema: Optional[Dict[str, Any]] = None
    sample_data: Optional[List[Dict[str, Any]]] = None
    
    def to_catalog_entity(self) -> Dict[str, Any]:
        """Convert to catalog entity format"""
        return {
            "name": self.name,
            "type": self.asset_type.value,
            "qualified_name": f"{self.source_id}:{self.path}",
            "description": self.metadata.get("description"),
            "metadata": {
                "source_id": self.source_id,
                "path": self.path,
                "size_bytes": self.size_bytes,
                "modified_at": self.modified_at.isoformat() if self.modified_at else None,
                "discovered_at": self.discovered_at.isoformat(),
                **self.metadata
            },
            "schema": self.schema
        }


@dataclass
class DiscoveryConfig:
    """Discovery configuration"""
    max_depth: int = 10
    sample_size: int = 100
    infer_schema: bool = True
    extract_metadata: bool = True
    follow_symlinks: bool = False
    exclude_patterns: List[str] = field(default_factory=list)
    include_patterns: List[str] = field(default_factory=list)
    max_file_size: int = 1024 * 1024 * 100  # 100MB
    parallel_workers: int = 4
    timeout: int = 3600  # seconds
    

class BaseDiscoveryAdapter(ABC):
    """Base adapter for different data sources"""
    
    @abstractmethod
    async def discover(
        self,
        source: DataSource,
        config: DiscoveryConfig,
        patterns: List[DiscoveryPattern]
    ) -> List[DiscoveryResult]:
        """Discover assets from source"""
        pass
        
    @abstractmethod
    async def get_schema(
        self,
        source: DataSource,
        asset_path: str
    ) -> Optional[Dict[str, Any]]:
        """Get schema for asset"""
        pass
        
    @abstractmethod
    async def get_sample_data(
        self,
        source: DataSource,
        asset_path: str,
        limit: int = 100
    ) -> List[Dict[str, Any]]:
        """Get sample data from asset"""
        pass


class S3DiscoveryAdapter(BaseDiscoveryAdapter):
    """Discovery adapter for S3/MinIO"""
    
    async def discover(
        self,
        source: DataSource,
        config: DiscoveryConfig,
        patterns: List[DiscoveryPattern]
    ) -> List[DiscoveryResult]:
        """Discover S3 objects"""
        results = []
        
        try:
            import boto3
            
            # Create S3 client
            s3_config = source.connection_config
            client = boto3.client(
                's3',
                endpoint_url=s3_config.get('endpoint_url'),
                aws_access_key_id=source.credentials.get('access_key') if source.credentials else None,
                aws_secret_access_key=source.credentials.get('secret_key') if source.credentials else None
            )
            
            bucket = s3_config.get('bucket')
            prefix = s3_config.get('prefix', '')
            
            # List objects
            paginator = client.get_paginator('list_objects_v2')
            pages = paginator.paginate(Bucket=bucket, Prefix=prefix)
            
            for page in pages:
                for obj in page.get('Contents', []):
                    key = obj['Key']
                    
                    # Check patterns
                    asset_type = AssetType.FILE
                    for pattern in patterns:
                        if pattern.matches(key):
                            asset_type = pattern.asset_type
                            break
                            
                    # Check exclude patterns
                    if any(re.match(p, key) for p in config.exclude_patterns):
                        continue
                        
                    # Create result
                    result = DiscoveryResult(
                        name=key.split('/')[-1],
                        path=f"s3://{bucket}/{key}",
                        asset_type=asset_type,
                        source_id=source.id,
                        size_bytes=obj.get('Size'),
                        modified_at=obj.get('LastModified'),
                        metadata={
                            'storage_class': obj.get('StorageClass'),
                            'etag': obj.get('ETag')
                        }
                    )
                    
                    # Infer schema if needed
                    if config.infer_schema and self._is_structured_file(key):
                        schema = await self.get_schema(source, key)
                        if schema:
                            result.schema = schema
                            
                    results.append(result)
                    
        except Exception as e:
            logger.error(f"S3 discovery failed: {e}")
            raise
            
        return results
        
    async def get_schema(
        self,
        source: DataSource,
        asset_path: str
    ) -> Optional[Dict[str, Any]]:
        """Get schema for S3 object"""
        # Implementation would depend on file type
        # For now, return None
        return None
        
    async def get_sample_data(
        self,
        source: DataSource,
        asset_path: str,
        limit: int = 100
    ) -> List[Dict[str, Any]]:
        """Get sample data from S3 object"""
        # Implementation would depend on file type
        return []
        
    def _is_structured_file(self, path: str) -> bool:
        """Check if file is structured data"""
        structured_extensions = ['.csv', '.json', '.parquet', '.avro', '.orc']
        return any(path.lower().endswith(ext) for ext in structured_extensions)


class DatabaseDiscoveryAdapter(BaseDiscoveryAdapter):
    """Discovery adapter for databases"""
    
    async def discover(
        self,
        source: DataSource,
        config: DiscoveryConfig,
        patterns: List[DiscoveryPattern]
    ) -> List[DiscoveryResult]:
        """Discover database objects"""
        results = []
        
        try:
            import sqlalchemy
            
            # Create engine
            conn_string = source.get_connection_string()
            if source.credentials:
                # Add credentials to connection string
                pass
                
            engine = sqlalchemy.create_engine(conn_string)
            
            with engine.connect() as conn:
                # Get metadata
                metadata = sqlalchemy.MetaData()
                metadata.reflect(bind=engine)
                
                # Discover schemas
                inspector = sqlalchemy.inspect(engine)
                schemas = inspector.get_schema_names()
                
                for schema_name in schemas:
                    # Check patterns
                    if not self._matches_patterns(f"{schema_name}", patterns):
                        continue
                        
                    # Add schema
                    results.append(DiscoveryResult(
                        name=schema_name,
                        path=f"{conn_string}/{schema_name}",
                        asset_type=AssetType.SCHEMA,
                        source_id=source.id,
                        metadata={'database': source.connection_config.get('database')}
                    ))
                    
                    # Discover tables
                    tables = inspector.get_table_names(schema=schema_name)
                    for table_name in tables:
                        table_path = f"{schema_name}.{table_name}"
                        
                        if not self._matches_patterns(table_path, patterns):
                            continue
                            
                        # Get table info
                        columns = inspector.get_columns(table_name, schema=schema_name)
                        
                        # Create schema
                        schema_info = {
                            'columns': [
                                {
                                    'name': col['name'],
                                    'type': str(col['type']),
                                    'nullable': col['nullable'],
                                    'default': col.get('default')
                                }
                                for col in columns
                            ]
                        }
                        
                        # Get row count if enabled
                        row_count = None
                        if config.extract_metadata:
                            try:
                                result = conn.execute(
                                    f"SELECT COUNT(*) FROM {schema_name}.{table_name}"
                                ).scalar()
                                row_count = result
                            except:
                                pass
                                
                        results.append(DiscoveryResult(
                            name=table_name,
                            path=f"{conn_string}/{schema_name}/{table_name}",
                            asset_type=AssetType.TABLE,
                            source_id=source.id,
                            schema=schema_info,
                            metadata={
                                'schema': schema_name,
                                'row_count': row_count,
                                'column_count': len(columns)
                            }
                        ))
                        
        except Exception as e:
            logger.error(f"Database discovery failed: {e}")
            raise
            
        return results
        
    async def get_schema(
        self,
        source: DataSource,
        asset_path: str
    ) -> Optional[Dict[str, Any]]:
        """Get schema for database object"""
        # Parse path to get schema and table
        # Implementation would query database
        return None
        
    async def get_sample_data(
        self,
        source: DataSource,
        asset_path: str,
        limit: int = 100
    ) -> List[Dict[str, Any]]:
        """Get sample data from database"""
        # Implementation would query database
        return []
        
    def _matches_patterns(
        self,
        path: str,
        patterns: List[DiscoveryPattern]
    ) -> bool:
        """Check if path matches any pattern"""
        if not patterns:
            return True
        return any(p.matches(path) for p in patterns)


class DiscoveryEngine:
    """
    Asset discovery engine.
    
    Features:
    - Multi-source discovery
    - Pattern matching
    - Schema inference
    - Parallel processing
    - Incremental discovery
    """
    
    def __init__(
        self,
        cache_manager: Optional[CacheManager] = None,
        event_bus: Optional[EventBus] = None
    ):
        self.cache = cache_manager
        self.event_bus = event_bus
        
        # Adapters
        self._adapters: Dict[str, BaseDiscoveryAdapter] = {
            's3': S3DiscoveryAdapter(),
            'minio': S3DiscoveryAdapter(),
            'database': DatabaseDiscoveryAdapter(),
            # Add more adapters as needed
        }
        
        # Storage
        self._sources: Dict[str, DataSource] = {}
        self._patterns: Dict[str, DiscoveryPattern] = {}
        self._jobs: Dict[str, Dict[str, Any]] = {}
        
        # Thread pool for parallel discovery
        self._executor = ThreadPoolExecutor(max_workers=4)
        
    def register_source(self, source: DataSource) -> str:
        """Register data source"""
        self._sources[source.id] = source
        
        # Publish event
        if self.event_bus:
            self.event_bus.publish(Event(
                type="discovery.source.registered",
                source="discovery_engine",
                data={"source_id": source.id, "source_type": source.type}
            ))
            
        logger.info(f"Registered discovery source: {source.name}")
        return source.id
        
    def register_pattern(self, pattern: DiscoveryPattern) -> str:
        """Register discovery pattern"""
        self._patterns[pattern.id] = pattern
        logger.info(f"Registered discovery pattern: {pattern.name}")
        return pattern.id
        
    async def discover(
        self,
        source_ids: Optional[List[str]] = None,
        config: Optional[DiscoveryConfig] = None,
        patterns: Optional[List[str]] = None
    ) -> str:
        """Start discovery job"""
        job_id = str(uuid.uuid4())
        config = config or DiscoveryConfig()
        
        # Get sources
        sources = []
        if source_ids:
            sources = [self._sources[sid] for sid in source_ids if sid in self._sources]
        else:
            sources = [s for s in self._sources.values() if s.is_active]
            
        # Get patterns
        pattern_list = []
        if patterns:
            pattern_list = [self._patterns[pid] for pid in patterns if pid in self._patterns]
        else:
            pattern_list = list(self._patterns.values())
            
        # Create job
        self._jobs[job_id] = {
            'id': job_id,
            'status': DiscoveryStatus.PENDING,
            'sources': [s.id for s in sources],
            'config': config,
            'patterns': [p.id for p in pattern_list],
            'started_at': None,
            'completed_at': None,
            'results': [],
            'errors': []
        }
        
        # Start discovery
        asyncio.create_task(self._run_discovery(job_id, sources, config, pattern_list))
        
        # Publish event
        if self.event_bus:
            self.event_bus.publish(Event(
                type="discovery.job.started",
                source="discovery_engine",
                data={"job_id": job_id}
            ))
            
        logger.info(f"Started discovery job: {job_id}")
        return job_id
        
    async def _run_discovery(
        self,
        job_id: str,
        sources: List[DataSource],
        config: DiscoveryConfig,
        patterns: List[DiscoveryPattern]
    ):
        """Run discovery job"""
        job = self._jobs[job_id]
        job['status'] = DiscoveryStatus.RUNNING
        job['started_at'] = datetime.utcnow()
        
        all_results = []
        
        try:
            # Discover from each source in parallel
            tasks = []
            for source in sources:
                adapter = self._adapters.get(source.type)
                if adapter:
                    task = self._discover_source(source, adapter, config, patterns)
                    tasks.append(task)
                else:
                    logger.warning(f"No adapter for source type: {source.type}")
                    
            # Wait for all discoveries
            results = await asyncio.gather(*tasks, return_exceptions=True)
            
            # Process results
            for i, result in enumerate(results):
                if isinstance(result, Exception):
                    job['errors'].append({
                        'source_id': sources[i].id,
                        'error': str(result)
                    })
                else:
                    all_results.extend(result)
                    
            # Store results
            job['results'] = [r.to_catalog_entity() for r in all_results]
            job['status'] = DiscoveryStatus.COMPLETED
            
            # Cache results
            if self.cache:
                cache_key = f"discovery:job:{job_id}"
                self.cache.set(cache_key, job, ttl=3600)
                
            # Publish event
            if self.event_bus:
                self.event_bus.publish(Event(
                    type="discovery.job.completed",
                    source="discovery_engine",
                    data={
                        "job_id": job_id,
                        "discovered_count": len(all_results)
                    }
                ))
                
            logger.info(f"Discovery job completed: {job_id}, found {len(all_results)} assets")
            
        except Exception as e:
            job['status'] = DiscoveryStatus.FAILED
            job['errors'].append({'error': str(e)})
            logger.error(f"Discovery job failed: {job_id}, error: {e}")
            
        finally:
            job['completed_at'] = datetime.utcnow()
            
    async def _discover_source(
        self,
        source: DataSource,
        adapter: BaseDiscoveryAdapter,
        config: DiscoveryConfig,
        patterns: List[DiscoveryPattern]
    ) -> List[DiscoveryResult]:
        """Discover from single source"""
        logger.info(f"Discovering from source: {source.name}")
        
        try:
            # Get credentials from Vault if needed
            if not source.credentials and source.connection_config.get('use_vault'):
                # TODO: Integrate with Vault
                pass
                
            # Run discovery
            results = await adapter.discover(source, config, patterns)
            
            logger.info(f"Discovered {len(results)} assets from {source.name}")
            return results
            
        except Exception as e:
            logger.error(f"Discovery failed for source {source.name}: {e}")
            raise
            
    def get_job_status(self, job_id: str) -> Optional[Dict[str, Any]]:
        """Get discovery job status"""
        # Check cache first
        if self.cache:
            cache_key = f"discovery:job:{job_id}"
            cached = self.cache.get(cache_key)
            if cached:
                return cached
                
        return self._jobs.get(job_id)
        
    def list_sources(
        self,
        source_type: Optional[str] = None,
        is_active: Optional[bool] = None
    ) -> List[DataSource]:
        """List registered sources"""
        sources = list(self._sources.values())
        
        if source_type:
            sources = [s for s in sources if s.type == source_type]
        if is_active is not None:
            sources = [s for s in sources if s.is_active == is_active]
            
        return sources
        
    def list_patterns(
        self,
        asset_type: Optional[AssetType] = None
    ) -> List[DiscoveryPattern]:
        """List registered patterns"""
        patterns = list(self._patterns.values())
        
        if asset_type:
            patterns = [p for p in patterns if p.asset_type == asset_type]
            
        return patterns
        
    async def test_connection(self, source_id: str) -> bool:
        """Test source connection"""
        source = self._sources.get(source_id)
        if not source:
            return False
            
        adapter = self._adapters.get(source.type)
        if not adapter:
            return False
            
        try:
            # Try minimal discovery
            config = DiscoveryConfig(max_depth=1, sample_size=1)
            results = await adapter.discover(source, config, [])
            return True
        except:
            return False
            
    def register_adapter(self, source_type: str, adapter: BaseDiscoveryAdapter):
        """Register custom discovery adapter"""
        self._adapters[source_type] = adapter
        logger.info(f"Registered discovery adapter for: {source_type}")
        
    async def shutdown(self):
        """Shutdown discovery engine"""
        self._executor.shutdown(wait=True)
        logger.info("Discovery engine shutdown complete") 