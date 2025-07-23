"""
OpenLineage Client Integration

Provides cross-platform data lineage standard implementation.
"""

from typing import Any, Dict, List, Optional, Union, Tuple
from datetime import datetime
from dataclasses import dataclass, field
from enum import Enum
import uuid
from openlineage.client import OpenLineageClient
from openlineage.client.run import RunEvent, RunState, Run, Job, Dataset, DatasetFacets, JobFacets
from openlineage.client.facet import (
    BaseFacet,
    DataSourceDatasetFacet,
    SchemaDatasetFacet,
    SchemaField,
    DataQualityMetricsInputDatasetFacet,
    DataQualityAssertionsDatasetFacet,
    ColumnLineageDatasetFacet,
    DocumentationJobFacet,
    SourceCodeLocationJobFacet,
    SqlJobFacet,
    ErrorMessageRunFacet,
    NominalTimeRunFacet,
    ParentRunFacet
)

from platformq_shared.vault.vault_client import VaultClient
from platformq_shared.consul.consul_client import ConsulClient
from ..clients.base_client import BaseServiceClient, ClientConfig
from ..monitoring import StructuredLogger

logger = StructuredLogger.get_logger(__name__)


class LineageBackend(str, Enum):
    """Supported lineage backends"""
    HTTP = "http"
    KAFKA = "kafka"
    FILE = "file"
    CONSOLE = "console"


class JobType(str, Enum):
    """Job types"""
    BATCH = "BATCH"
    STREAMING = "STREAMING"
    SERVICE = "SERVICE"
    QUERY = "QUERY"


@dataclass
class OpenLineageConfig(ClientConfig):
    """Configuration for OpenLineage client"""
    # Backend configuration
    backend: LineageBackend = LineageBackend.HTTP
    endpoint: str = "http://localhost:5000"
    
    # Authentication
    api_key: Optional[str] = None
    
    # Kafka backend settings
    kafka_config: Dict[str, Any] = field(default_factory=dict)
    kafka_topic: str = "openlineage.events"
    
    # File backend settings
    file_path: str = "/tmp/openlineage"
    
    # Client settings
    namespace: str = "platformq"
    timeout_seconds: float = 30.0
    
    # Event settings
    emit_async: bool = True
    batch_events: bool = False
    batch_size: int = 100
    
    def __post_init__(self):
        super().__post_init__()
        self.service_name = self.service_name or "openlineage"


@dataclass
class LineageDataset:
    """Dataset for lineage tracking"""
    namespace: str
    name: str
    facets: Optional[DatasetFacets] = None
    
    def to_openlineage(self) -> Dataset:
        """Convert to OpenLineage Dataset"""
        return Dataset(
            namespace=self.namespace,
            name=self.name,
            facets=self.facets or DatasetFacets()
        )


@dataclass
class LineageJob:
    """Job for lineage tracking"""
    namespace: str
    name: str
    facets: Optional[JobFacets] = None
    
    def to_openlineage(self) -> Job:
        """Convert to OpenLineage Job"""
        return Job(
            namespace=self.namespace,
            name=self.name,
            facets=self.facets or JobFacets()
        )


@dataclass
class LineageRun:
    """Run for lineage tracking"""
    run_id: str = field(default_factory=lambda: str(uuid.uuid4()))
    facets: Optional[Dict[str, BaseFacet]] = None
    
    def to_openlineage(self) -> Run:
        """Convert to OpenLineage Run"""
        return Run(
            runId=self.run_id,
            facets=self.facets or {}
        )


class OpenLineageClient(BaseServiceClient):
    """
    OpenLineage client for cross-platform data lineage.
    
    Features:
    - Standard lineage events
    - Multiple backend support
    - Rich facets and metadata
    - Column-level lineage
    - Data quality integration
    - Cross-platform compatibility
    """
    
    def __init__(
        self,
        config: Optional[OpenLineageConfig] = None,
        vault_client: Optional[VaultClient] = None,
        consul_client: Optional[ConsulClient] = None,
        **kwargs
    ):
        if not config:
            config = OpenLineageConfig()
            
        super().__init__(config, vault_client, consul_client, **kwargs)
        self.config: OpenLineageConfig = config
        self._client: Optional[OpenLineageClient] = None
        self._pending_events: List[RunEvent] = []
        
    async def connect(self):
        """Connect to OpenLineage backend"""
        await super().connect()
        
        try:
            # Get API key from Vault if configured
            if self.config.use_vault_credentials and not self.config.api_key:
                creds = await self._get_credentials()
                if creds:
                    self.config.api_key = creds.get("api_key")
            
            # Create client based on backend
            if self.config.backend == LineageBackend.HTTP:
                transport_config = {
                    "type": "http",
                    "url": self.config.endpoint,
                    "endpoint": "api/v1/lineage",
                    "timeout": self.config.timeout_seconds,
                    "verify": self.config.use_ssl,
                    "auth": {
                        "type": "api_key",
                        "api_key": self.config.api_key
                    } if self.config.api_key else None
                }
            
            elif self.config.backend == LineageBackend.KAFKA:
                transport_config = {
                    "type": "kafka",
                    "config": self.config.kafka_config,
                    "topic": self.config.kafka_topic,
                    "flush": not self.config.emit_async
                }
            
            elif self.config.backend == LineageBackend.FILE:
                transport_config = {
                    "type": "file",
                    "log_file_path": self.config.file_path
                }
            
            else:  # CONSOLE
                transport_config = {
                    "type": "console"
                }
            
            # Create client
            self._client = OpenLineageClient.from_transport(transport_config)
            
            logger.info(f"Connected to OpenLineage backend: {self.config.backend.value}")
            
        except Exception as e:
            logger.error(f"Failed to connect to OpenLineage: {e}")
            raise
    
    async def emit_start_event(
        self,
        job: LineageJob,
        run: LineageRun,
        inputs: Optional[List[LineageDataset]] = None,
        outputs: Optional[List[LineageDataset]] = None,
        event_time: Optional[datetime] = None
    ) -> bool:
        """
        Emit job start event.
        
        Args:
            job: Job information
            run: Run information
            inputs: Input datasets
            outputs: Output datasets
            event_time: Event timestamp
            
        Returns:
            Success status
        """
        try:
            event = RunEvent(
                eventType=RunState.START,
                eventTime=event_time or datetime.utcnow(),
                run=run.to_openlineage(),
                job=job.to_openlineage(),
                inputs=[d.to_openlineage() for d in inputs] if inputs else None,
                outputs=[d.to_openlineage() for d in outputs] if outputs else None
            )
            
            if self.config.batch_events:
                self._pending_events.append(event)
                if len(self._pending_events) >= self.config.batch_size:
                    await self._flush_events()
            else:
                self._client.emit(event)
            
            logger.info(f"Emitted START event for job: {job.name}")
            return True
            
        except Exception as e:
            logger.error(f"Failed to emit start event: {e}")
            return False
    
    async def emit_complete_event(
        self,
        job: LineageJob,
        run: LineageRun,
        inputs: Optional[List[LineageDataset]] = None,
        outputs: Optional[List[LineageDataset]] = None,
        event_time: Optional[datetime] = None
    ) -> bool:
        """
        Emit job complete event.
        
        Args:
            job: Job information
            run: Run information
            inputs: Input datasets
            outputs: Output datasets
            event_time: Event timestamp
            
        Returns:
            Success status
        """
        try:
            event = RunEvent(
                eventType=RunState.COMPLETE,
                eventTime=event_time or datetime.utcnow(),
                run=run.to_openlineage(),
                job=job.to_openlineage(),
                inputs=[d.to_openlineage() for d in inputs] if inputs else None,
                outputs=[d.to_openlineage() for d in outputs] if outputs else None
            )
            
            if self.config.batch_events:
                self._pending_events.append(event)
                await self._flush_events()  # Flush on complete
            else:
                self._client.emit(event)
            
            logger.info(f"Emitted COMPLETE event for job: {job.name}")
            return True
            
        except Exception as e:
            logger.error(f"Failed to emit complete event: {e}")
            return False
    
    async def emit_fail_event(
        self,
        job: LineageJob,
        run: LineageRun,
        error_message: str,
        inputs: Optional[List[LineageDataset]] = None,
        outputs: Optional[List[LineageDataset]] = None,
        event_time: Optional[datetime] = None
    ) -> bool:
        """
        Emit job fail event.
        
        Args:
            job: Job information
            run: Run information
            error_message: Error message
            inputs: Input datasets
            outputs: Output datasets
            event_time: Event timestamp
            
        Returns:
            Success status
        """
        try:
            # Add error facet
            if not run.facets:
                run.facets = {}
            run.facets["errorMessage"] = ErrorMessageRunFacet(
                message=error_message,
                programmingLanguage="python",
                stackTrace=None
            )
            
            event = RunEvent(
                eventType=RunState.FAIL,
                eventTime=event_time or datetime.utcnow(),
                run=run.to_openlineage(),
                job=job.to_openlineage(),
                inputs=[d.to_openlineage() for d in inputs] if inputs else None,
                outputs=[d.to_openlineage() for d in outputs] if outputs else None
            )
            
            if self.config.batch_events:
                self._pending_events.append(event)
                await self._flush_events()  # Flush on fail
            else:
                self._client.emit(event)
            
            logger.info(f"Emitted FAIL event for job: {job.name}")
            return True
            
        except Exception as e:
            logger.error(f"Failed to emit fail event: {e}")
            return False
    
    async def create_dataset_with_schema(
        self,
        namespace: str,
        name: str,
        schema_fields: List[Dict[str, Any]],
        data_source: Optional[Dict[str, str]] = None
    ) -> LineageDataset:
        """
        Create dataset with schema information.
        
        Args:
            namespace: Dataset namespace
            name: Dataset name
            schema_fields: List of schema fields
            data_source: Data source information
            
        Returns:
            LineageDataset with facets
        """
        # Create schema facet
        fields = []
        for field in schema_fields:
            fields.append(SchemaField(
                name=field["name"],
                type=field.get("type", "string"),
                description=field.get("description")
            ))
        
        schema_facet = SchemaDatasetFacet(fields=fields)
        
        facets = DatasetFacets(schema=schema_facet)
        
        # Add data source facet if provided
        if data_source:
            facets.dataSource = DataSourceDatasetFacet(
                name=data_source.get("name", "unknown"),
                uri=data_source.get("uri", "")
            )
        
        return LineageDataset(
            namespace=namespace,
            name=name,
            facets=facets
        )
    
    async def create_job_with_metadata(
        self,
        namespace: str,
        name: str,
        job_type: JobType,
        description: Optional[str] = None,
        source_code_location: Optional[str] = None,
        sql: Optional[str] = None
    ) -> LineageJob:
        """
        Create job with metadata facets.
        
        Args:
            namespace: Job namespace
            name: Job name
            job_type: Type of job
            description: Job description
            source_code_location: Source code URL
            sql: SQL query if applicable
            
        Returns:
            LineageJob with facets
        """
        facets = JobFacets()
        
        # Add documentation facet
        if description:
            facets.documentation = DocumentationJobFacet(
                description=description
            )
        
        # Add source code location facet
        if source_code_location:
            facets.sourceCodeLocation = SourceCodeLocationJobFacet(
                type="git",
                url=source_code_location
            )
        
        # Add SQL facet
        if sql:
            facets.sql = SqlJobFacet(query=sql)
        
        return LineageJob(
            namespace=namespace,
            name=name,
            facets=facets
        )
    
    async def add_column_lineage(
        self,
        dataset: LineageDataset,
        column_lineage: Dict[str, List[Dict[str, str]]]
    ) -> LineageDataset:
        """
        Add column-level lineage to dataset.
        
        Args:
            dataset: Dataset to add lineage to
            column_lineage: Column lineage mapping
            
        Returns:
            Updated dataset
        """
        if not dataset.facets:
            dataset.facets = DatasetFacets()
        
        # Create column lineage facet
        fields = {}
        for output_field, input_fields in column_lineage.items():
            fields[output_field] = {
                "inputFields": [
                    {
                        "namespace": field["namespace"],
                        "name": field["dataset"],
                        "field": field["field"]
                    }
                    for field in input_fields
                ]
            }
        
        dataset.facets.columnLineage = ColumnLineageDatasetFacet(
            fields=fields
        )
        
        return dataset
    
    async def add_data_quality_metrics(
        self,
        dataset: LineageDataset,
        metrics: Dict[str, float],
        assertions: Optional[List[Dict[str, Any]]] = None
    ) -> LineageDataset:
        """
        Add data quality metrics to dataset.
        
        Args:
            dataset: Dataset to add metrics to
            metrics: Quality metrics
            assertions: Quality assertions
            
        Returns:
            Updated dataset
        """
        if not dataset.facets:
            dataset.facets = DatasetFacets()
        
        # Add metrics facet
        dataset.facets.dataQualityMetrics = DataQualityMetricsInputDatasetFacet(
            rowCount=metrics.get("row_count"),
            bytes=metrics.get("bytes"),
            columnMetrics={
                name: {"nullCount": value}
                for name, value in metrics.items()
                if name not in ["row_count", "bytes"]
            }
        )
        
        # Add assertions facet
        if assertions:
            dataset.facets.dataQualityAssertions = DataQualityAssertionsDatasetFacet(
                assertions=[
                    {
                        "assertion": a["assertion"],
                        "success": a["success"],
                        "column": a.get("column")
                    }
                    for a in assertions
                ]
            )
        
        return dataset
    
    async def create_run_with_parent(
        self,
        run_id: Optional[str] = None,
        parent_run_id: Optional[str] = None,
        parent_job_name: Optional[str] = None,
        nominal_time: Optional[datetime] = None
    ) -> LineageRun:
        """
        Create run with parent and nominal time.
        
        Args:
            run_id: Run ID (generated if not provided)
            parent_run_id: Parent run ID
            parent_job_name: Parent job name
            nominal_time: Nominal execution time
            
        Returns:
            LineageRun with facets
        """
        facets = {}
        
        # Add parent run facet
        if parent_run_id and parent_job_name:
            facets["parent"] = ParentRunFacet(
                run={
                    "runId": parent_run_id
                },
                job={
                    "namespace": self.config.namespace,
                    "name": parent_job_name
                }
            )
        
        # Add nominal time facet
        if nominal_time:
            facets["nominalTime"] = NominalTimeRunFacet(
                nominalStartTime=nominal_time,
                nominalEndTime=nominal_time
            )
        
        return LineageRun(
            run_id=run_id or str(uuid.uuid4()),
            facets=facets
        )
    
    async def track_spark_job(
        self,
        job_name: str,
        sql_query: str,
        input_tables: List[str],
        output_tables: List[str],
        run_id: Optional[str] = None
    ) -> bool:
        """
        Track a Spark SQL job execution.
        
        Args:
            job_name: Spark job name
            sql_query: SQL query
            input_tables: Input table names
            output_tables: Output table names
            run_id: Optional run ID
            
        Returns:
            Success status
        """
        try:
            # Create job
            job = await self.create_job_with_metadata(
                namespace=self.config.namespace,
                name=job_name,
                job_type=JobType.BATCH,
                description=f"Spark SQL job: {job_name}",
                sql=sql_query
            )
            
            # Create datasets
            inputs = []
            for table in input_tables:
                dataset = LineageDataset(
                    namespace=self.config.namespace,
                    name=table
                )
                inputs.append(dataset)
            
            outputs = []
            for table in output_tables:
                dataset = LineageDataset(
                    namespace=self.config.namespace,
                    name=table
                )
                outputs.append(dataset)
            
            # Create run
            run = LineageRun(run_id=run_id)
            
            # Emit events
            await self.emit_start_event(job, run, inputs, outputs)
            # In real usage, complete/fail would be called after job execution
            
            return True
            
        except Exception as e:
            logger.error(f"Failed to track Spark job: {e}")
            return False
    
    async def _flush_events(self):
        """Flush pending events"""
        if self._pending_events:
            try:
                for event in self._pending_events:
                    self._client.emit(event)
                
                logger.info(f"Flushed {len(self._pending_events)} events")
                self._pending_events.clear()
                
            except Exception as e:
                logger.error(f"Failed to flush events: {e}")
    
    async def close(self):
        """Close OpenLineage client"""
        # Flush any pending events
        await self._flush_events()
        
        await super().close()
    
    async def get_client_specific_config(self) -> Dict[str, Any]:
        """Get OpenLineage specific configuration"""
        return {
            "backend": self.config.backend.value,
            "endpoint": self.config.endpoint,
            "namespace": self.config.namespace,
            "emit_async": self.config.emit_async,
            "batch_events": self.config.batch_events
        } 