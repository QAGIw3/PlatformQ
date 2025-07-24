"""
Integration Hub Service Base Classes

Migrated to use the unified data-intelligence-common library.
"""

from typing import Dict, Any, List, Optional, Union, Callable, Type
from datetime import datetime, timedelta
from dataclasses import dataclass, field
from enum import Enum
import asyncio
import uuid

from data_intelligence_common.base_service import DataIntelligenceBaseService
from data_intelligence_common.core.config.unified import UnifiedServiceConfig, ConnectionConfig
from data_intelligence_common.core.processing import (
    UnifiedProcessor, ProcessingConfig, ProcessingMode,
    DataSource, DataSink, ProcessingStage, ProcessingContext
)
from data_intelligence_common.core.events import Event, EventType
from data_intelligence_common.core.patterns.factory import PluginFactory, Factory
from data_intelligence_common.core.patterns.resilience import (
    RetryConfig, CircuitBreakerConfig, ResiliencePolicy
)
from data_intelligence_common.monitoring import StructuredLogger

logger = StructuredLogger.get_logger(__name__)


class ConnectorType(str, Enum):
    """Types of connectors"""
    DATABASE = "database"
    API = "api"
    FILE = "file"
    MESSAGING = "messaging"
    CLOUD = "cloud"
    CUSTOM = "custom"


class SyncMode(str, Enum):
    """Data synchronization modes"""
    FULL = "full"
    INCREMENTAL = "incremental"
    CDC = "cdc"  # Change Data Capture
    REAL_TIME = "real_time"


@dataclass
class IntegrationHubConfig(UnifiedServiceConfig):
    """Configuration for integration hub service"""
    # Connector settings
    connector_plugin_dir: str = "connectors"
    max_concurrent_connections: int = 100
    connection_pool_size: int = 10
    
    # Sync settings
    default_sync_mode: SyncMode = SyncMode.INCREMENTAL
    default_batch_size: int = 1000
    enable_parallel_sync: bool = True
    
    # Transformation settings
    enable_transformations: bool = True
    transformation_plugin_dir: str = "transformations"
    
    # Monitoring
    enable_connection_monitoring: bool = True
    connection_health_check_interval: timedelta = field(default_factory=lambda: timedelta(minutes=5))
    
    # Security
    enable_credential_rotation: bool = True
    credential_rotation_interval: timedelta = field(default_factory=lambda: timedelta(days=30))
    
    # Resilience
    default_retry_config: RetryConfig = field(default_factory=lambda: RetryConfig(
        max_retries=3,
        initial_delay=1.0,
        max_delay=60.0,
        exponential_base=2.0
    ))
    
    default_circuit_breaker_config: CircuitBreakerConfig = field(default_factory=lambda: CircuitBreakerConfig(
        failure_threshold=5,
        recovery_timeout=60.0,
        expected_exception_types=[Exception]
    ))


@dataclass
class ConnectorConfig:
    """Configuration for a connector instance"""
    connector_id: str
    name: str
    connector_type: ConnectorType
    connection_config: ConnectionConfig
    sync_config: Dict[str, Any] = field(default_factory=dict)
    transformation_config: Optional[Dict[str, Any]] = None
    metadata: Dict[str, Any] = field(default_factory=dict)


class IntegrationHubService(DataIntelligenceBaseService):
    """
    Integration Hub service for managing data connections and integrations.
    
    Provides connector management, data synchronization, and transformation.
    """
    
    def __init__(self, config: IntegrationHubConfig):
        super().__init__(config)
        self.config = config
        
        # Connector management
        self._connector_factory: PluginFactory = None
        self._transformation_factory: PluginFactory = None
        self._connectors: Dict[str, 'BaseConnector'] = {}
        self._sync_pipelines: Dict[str, UnifiedProcessor] = {}
        
        # Connection pools
        self._connection_pools: Dict[str, 'ConnectionPool'] = {}
        
        # Resilience policies
        self._resilience_policies: Dict[str, ResiliencePolicy] = {}
        
    async def _initialize_internal(self):
        """Initialize integration hub components"""
        await super()._initialize_internal()
        
        # Initialize factories
        self._initialize_factories()
        
        # Initialize default resilience policies
        self._initialize_resilience_policies()
        
        # Register health checks
        self.register_health_check(
            "connectors",
            self._check_connectors_health,
            critical=True
        )
        
        # Start background tasks
        if self.config.enable_connection_monitoring:
            self._start_background_task(self._monitor_connections_loop())
            
        if self.config.enable_credential_rotation:
            self._start_background_task(self._credential_rotation_loop())
            
        logger.info("Integration hub service initialized")
        
    def _initialize_factories(self):
        """Initialize plugin factories"""
        # Connector factory
        self._connector_factory = PluginFactory(
            plugin_dir=self.config.connector_plugin_dir,
            base_class="BaseConnector"
        )
        
        # Transformation factory
        if self.config.enable_transformations:
            self._transformation_factory = PluginFactory(
                plugin_dir=self.config.transformation_plugin_dir,
                base_class="BaseTransformation"
            )
            
    def _initialize_resilience_policies(self):
        """Initialize default resilience policies"""
        # Default policy
        self._resilience_policies["default"] = ResiliencePolicy(
            retry_config=self.config.default_retry_config,
            circuit_breaker_config=self.config.default_circuit_breaker_config,
            timeout_config=None,
            bulkhead_config=None
        )
        
    async def register_connector(
        self,
        connector_config: ConnectorConfig
    ) -> Dict[str, Any]:
        """Register a new connector"""
        try:
            # Create connector instance
            connector_class = self._connector_factory.get_plugin(
                connector_config.connector_type.value
            )
            
            connector = connector_class(
                config=connector_config,
                resilience_policy=self._resilience_policies.get(
                    connector_config.connector_id,
                    self._resilience_policies["default"]
                )
            )
            
            # Initialize connector
            await connector.initialize()
            
            # Test connection
            await connector.test_connection()
            
            # Store connector
            self._connectors[connector_config.connector_id] = connector
            
            # Create connection pool if applicable
            if connector.supports_pooling:
                pool = ConnectionPool(
                    connector=connector,
                    size=self.config.connection_pool_size
                )
                await pool.initialize()
                self._connection_pools[connector_config.connector_id] = pool
                
            # Emit registration event
            await self.publish_event(
                event_type="connector.registered",
                data={
                    "connector_id": connector_config.connector_id,
                    "name": connector_config.name,
                    "type": connector_config.connector_type.value
                }
            )
            
            # Record metrics
            self.record_operation("connector_registered", {
                "type": connector_config.connector_type.value
            })
            
            return {
                "connector_id": connector_config.connector_id,
                "status": "registered",
                "connection_test": "passed"
            }
            
        except Exception as e:
            self.record_error("connector_registration_failed", e)
            raise
            
    async def create_sync_pipeline(
        self,
        pipeline_name: str,
        source_connector_id: str,
        target_connector_id: str,
        sync_config: Dict[str, Any]
    ) -> Dict[str, Any]:
        """Create a data synchronization pipeline"""
        try:
            # Get connectors
            source_connector = self._connectors.get(source_connector_id)
            target_connector = self._connectors.get(target_connector_id)
            
            if not source_connector or not target_connector:
                raise ValueError("Source or target connector not found")
                
            # Create pipeline configuration
            pipeline_config = ProcessingConfig(
                name=pipeline_name,
                mode=ProcessingMode.ADAPTIVE,
                batch_size=sync_config.get("batch_size", self.config.default_batch_size),
                enable_quality_checks=sync_config.get("enable_quality_checks", True),
                enable_lineage_tracking=True
            )
            
            # Create source and sink
            source = ConnectorDataSource(
                connector=source_connector,
                query_config=sync_config.get("source_config", {})
            )
            
            sink = ConnectorDataSink(
                connector=target_connector,
                write_config=sync_config.get("target_config", {})
            )
            
            # Build pipeline
            builder = UnifiedProcessor.pipeline(pipeline_config).from_source(source)
            
            # Add transformations if configured
            if sync_config.get("transformations"):
                for transform_config in sync_config["transformations"]:
                    transform = self._create_transformation_stage(transform_config)
                    builder = builder.transform(transform)
                    
            # Add sync-specific stages
            builder = builder.transform(
                self._create_sync_tracking_stage(pipeline_name)
            )
            
            # Set sink
            builder = builder.to_sink(sink)
            
            # Build processor
            processor = builder.build(
                metrics_collector=self.metrics,
                event_bus=self.event_bus,
                cache_manager=self.cache
            )
            
            # Store pipeline
            self._sync_pipelines[pipeline_name] = processor
            
            # Emit creation event
            await self.publish_event(
                event_type="sync_pipeline.created",
                data={
                    "pipeline_name": pipeline_name,
                    "source": source_connector_id,
                    "target": target_connector_id
                }
            )
            
            return {
                "pipeline_name": pipeline_name,
                "status": "created",
                "source": source_connector_id,
                "target": target_connector_id
            }
            
        except Exception as e:
            self.record_error("sync_pipeline_creation_failed", e)
            raise
            
    def _create_transformation_stage(
        self,
        transform_config: Dict[str, Any]
    ) -> ProcessingStage:
        """Create transformation stage"""
        if not self._transformation_factory:
            raise ValueError("Transformations not enabled")
            
        transform_type = transform_config["type"]
        transform_class = self._transformation_factory.get_plugin(transform_type)
        
        return TransformationStage(
            transformation=transform_class(**transform_config.get("config", {}))
        )
        
    def _create_sync_tracking_stage(self, pipeline_name: str) -> ProcessingStage:
        """Create sync tracking stage"""
        service = self
        
        class SyncTrackingStage(ProcessingStage):
            def __init__(self):
                self.records_synced = 0
                self.last_sync_time = datetime.utcnow()
                
            async def process(self, data: Dict[str, Any], context: ProcessingContext) -> Optional[Dict[str, Any]]:
                # Add sync metadata
                data["_sync_pipeline"] = pipeline_name
                data["_sync_timestamp"] = datetime.utcnow().isoformat()
                data["_sync_job_id"] = context.job_id
                
                # Update tracking
                self.records_synced += 1
                
                # Emit progress event periodically
                if self.records_synced % 1000 == 0:
                    await service.publish_event(
                        event_type="sync.progress",
                        data={
                            "pipeline": pipeline_name,
                            "records_synced": self.records_synced,
                            "job_id": context.job_id
                        }
                    )
                    
                return data
                
        return SyncTrackingStage()
        
    async def execute_sync(
        self,
        pipeline_name: str,
        sync_mode: Optional[SyncMode] = None
    ) -> Dict[str, Any]:
        """Execute a sync pipeline"""
        pipeline = self._sync_pipelines.get(pipeline_name)
        if not pipeline:
            raise ValueError(f"Pipeline {pipeline_name} not found")
            
        job_id = str(uuid.uuid4())
        
        try:
            # Determine sync mode
            mode = sync_mode or self.config.default_sync_mode
            
            # Configure pipeline for sync mode
            if mode == SyncMode.INCREMENTAL:
                # Get last sync timestamp
                last_sync = await self.get_state(f"sync:{pipeline_name}:last_sync")
                if last_sync:
                    # Add filter to source
                    pass
                    
            # Execute pipeline
            result = await pipeline.process(
                job_id=job_id,
                context_metadata={
                    "sync_mode": mode.value,
                    "pipeline": pipeline_name
                }
            )
            
            # Update last sync timestamp
            await self.set_state(
                f"sync:{pipeline_name}:last_sync",
                datetime.utcnow().isoformat()
            )
            
            # Emit completion event
            await self.publish_event(
                event_type="sync.completed",
                data={
                    "pipeline": pipeline_name,
                    "job_id": job_id,
                    "records_processed": result.get("records_processed", 0),
                    "duration": result.get("duration", 0)
                }
            )
            
            # Record metrics
            self.record_operation("sync_executed", {
                "pipeline": pipeline_name,
                "mode": mode.value,
                "records": result.get("records_processed", 0)
            })
            
            return result
            
        except Exception as e:
            # Emit failure event
            await self.publish_event(
                event_type="sync.failed",
                data={
                    "pipeline": pipeline_name,
                    "job_id": job_id,
                    "error": str(e)
                }
            )
            
            self.record_error("sync_execution_failed", e)
            raise
            
    async def query_connector(
        self,
        connector_id: str,
        query: str,
        parameters: Optional[Dict[str, Any]] = None
    ) -> List[Dict[str, Any]]:
        """Query data from a connector"""
        connector = self._connectors.get(connector_id)
        if not connector:
            raise ValueError(f"Connector {connector_id} not found")
            
        # Use connection pool if available
        pool = self._connection_pools.get(connector_id)
        if pool:
            async with pool.acquire() as connection:
                return await connection.query(query, parameters)
        else:
            return await connector.query(query, parameters)
            
    async def write_to_connector(
        self,
        connector_id: str,
        data: Union[Dict[str, Any], List[Dict[str, Any]]],
        write_config: Optional[Dict[str, Any]] = None
    ) -> Dict[str, Any]:
        """Write data to a connector"""
        connector = self._connectors.get(connector_id)
        if not connector:
            raise ValueError(f"Connector {connector_id} not found")
            
        # Use connection pool if available
        pool = self._connection_pools.get(connector_id)
        if pool:
            async with pool.acquire() as connection:
                return await connection.write(data, write_config)
        else:
            return await connector.write(data, write_config)
            
    async def _monitor_connections_loop(self):
        """Monitor connector health"""
        while True:
            try:
                await asyncio.sleep(
                    self.config.connection_health_check_interval.total_seconds()
                )
                
                for connector_id, connector in self._connectors.items():
                    try:
                        # Test connection
                        health = await connector.check_health()
                        
                        if not health.get("healthy"):
                            # Emit unhealthy event
                            await self.publish_event(
                                event_type="connector.unhealthy",
                                data={
                                    "connector_id": connector_id,
                                    "reason": health.get("reason")
                                }
                            )
                            
                            # Try to reconnect
                            await connector.reconnect()
                            
                    except Exception as e:
                        logger.error(f"Health check failed for connector {connector_id}: {e}")
                        
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Error in connection monitoring: {e}")
                
    async def _credential_rotation_loop(self):
        """Rotate connector credentials"""
        while True:
            try:
                await asyncio.sleep(
                    self.config.credential_rotation_interval.total_seconds()
                )
                
                for connector_id, connector in self._connectors.items():
                    if connector.supports_credential_rotation:
                        try:
                            # Rotate credentials
                            new_credentials = await self._get_rotated_credentials(
                                connector_id
                            )
                            
                            await connector.update_credentials(new_credentials)
                            
                            # Emit rotation event
                            await self.publish_event(
                                event_type="connector.credentials_rotated",
                                data={
                                    "connector_id": connector_id,
                                    "timestamp": datetime.utcnow().isoformat()
                                }
                            )
                            
                        except Exception as e:
                            logger.error(f"Credential rotation failed for {connector_id}: {e}")
                            
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Error in credential rotation: {e}")
                
    async def _get_rotated_credentials(self, connector_id: str) -> Dict[str, Any]:
        """Get rotated credentials from Vault"""
        # This would integrate with Vault for dynamic credentials
        # For now, return placeholder
        return {
            "username": f"user_{connector_id}",
            "password": f"pass_{uuid.uuid4()}"
        }
        
    async def _check_connectors_health(self) -> Dict[str, Any]:
        """Check overall connector health"""
        total_connectors = len(self._connectors)
        healthy_connectors = 0
        
        for connector in self._connectors.values():
            try:
                health = await connector.check_health()
                if health.get("healthy"):
                    healthy_connectors += 1
            except Exception:
                pass
                
        return {
            "healthy": healthy_connectors == total_connectors,
            "total_connectors": total_connectors,
            "healthy_connectors": healthy_connectors
        }
        
    async def _stop_internal(self):
        """Stop integration hub components"""
        # Stop sync pipelines
        for pipeline in self._sync_pipelines.values():
            await pipeline.stop()
            
        # Close connectors
        for connector in self._connectors.values():
            await connector.close()
            
        # Close connection pools
        for pool in self._connection_pools.values():
            await pool.close()
            
        await super()._stop_internal()
        
        logger.info("Integration hub service stopped")


class BaseConnector(ABC):
    """Base class for connectors"""
    
    def __init__(
        self,
        config: ConnectorConfig,
        resilience_policy: ResiliencePolicy
    ):
        self.config = config
        self.resilience_policy = resilience_policy
        self.supports_pooling = False
        self.supports_credential_rotation = False
        
    @abstractmethod
    async def initialize(self):
        """Initialize connector"""
        pass
        
    @abstractmethod
    async def test_connection(self) -> bool:
        """Test connector connection"""
        pass
        
    @abstractmethod
    async def query(self, query: str, parameters: Optional[Dict[str, Any]] = None) -> List[Dict[str, Any]]:
        """Query data from connector"""
        pass
        
    @abstractmethod
    async def write(self, data: Union[Dict[str, Any], List[Dict[str, Any]]], config: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        """Write data to connector"""
        pass
        
    @abstractmethod
    async def check_health(self) -> Dict[str, Any]:
        """Check connector health"""
        pass
        
    @abstractmethod
    async def close(self):
        """Close connector"""
        pass
        
    async def reconnect(self):
        """Reconnect to data source"""
        await self.close()
        await self.initialize()
        
    async def update_credentials(self, credentials: Dict[str, Any]):
        """Update connector credentials"""
        raise NotImplementedError("Credential rotation not supported")


class ConnectorDataSource(DataSource):
    """Data source adapter for connectors"""
    
    def __init__(self, connector: BaseConnector, query_config: Dict[str, Any]):
        self.connector = connector
        self.query_config = query_config
        
    async def read(self) -> AsyncIterator[Dict[str, Any]]:
        """Read data from connector"""
        query = self.query_config.get("query", "SELECT * FROM data")
        parameters = self.query_config.get("parameters", {})
        
        results = await self.connector.query(query, parameters)
        
        for row in results:
            yield row
            
    async def get_schema(self) -> Dict[str, Any]:
        """Get data schema"""
        # This would query schema from connector
        return {}
        
    async def estimate_size(self) -> int:
        """Estimate data size"""
        # This would estimate based on query
        return -1


class ConnectorDataSink(DataSink):
    """Data sink adapter for connectors"""
    
    def __init__(self, connector: BaseConnector, write_config: Dict[str, Any]):
        self.connector = connector
        self.write_config = write_config
        self._buffer = []
        
    async def write(self, data: Union[Any, List[Any]]) -> None:
        """Buffer data for writing"""
        if isinstance(data, list):
            self._buffer.extend(data)
        else:
            self._buffer.append(data)
            
    async def commit(self) -> None:
        """Write buffered data"""
        if self._buffer:
            await self.connector.write(self._buffer, self.write_config)
            self._buffer.clear()
            
    async def rollback(self) -> None:
        """Clear buffer"""
        self._buffer.clear()


class TransformationStage(ProcessingStage):
    """Processing stage for transformations"""
    
    def __init__(self, transformation):
        self.transformation = transformation
        
    async def process(self, data: Dict[str, Any], context: ProcessingContext) -> Optional[Dict[str, Any]]:
        """Apply transformation"""
        return await self.transformation.transform(data)


class ConnectionPool:
    """Connection pool for connectors"""
    
    def __init__(self, connector: BaseConnector, size: int):
        self.connector = connector
        self.size = size
        self._pool = asyncio.Queue(maxsize=size)
        self._all_connections = []
        
    async def initialize(self):
        """Initialize connection pool"""
        for _ in range(self.size):
            # Create connection copy
            # This would properly clone the connector
            connection = self.connector
            self._all_connections.append(connection)
            await self._pool.put(connection)
            
    @asynccontextmanager
    async def acquire(self):
        """Acquire connection from pool"""
        connection = await self._pool.get()
        try:
            yield connection
        finally:
            await self._pool.put(connection)
            
    async def close(self):
        """Close all connections"""
        for connection in self._all_connections:
            await connection.close()


# Export main components
__all__ = [
    'ConnectorType',
    'SyncMode',
    'IntegrationHubConfig',
    'ConnectorConfig',
    'IntegrationHubService',
    'BaseConnector'
] 