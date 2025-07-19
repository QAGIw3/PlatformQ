"""
Unified Observability Module for PlatformQ

Integrates OpenTelemetry tracing with Iceberg metadata for comprehensive
observability and data lineage tracking across all services.
"""

import asyncio
import json
import logging
from typing import Dict, List, Optional, Any, Union, Callable, Type
from datetime import datetime, timedelta
from contextlib import asynccontextmanager
from dataclasses import dataclass, field
from enum import Enum
import uuid

from opentelemetry import trace, metrics, baggage
from opentelemetry.sdk.trace import TracerProvider, Span
from opentelemetry.sdk.trace.export import BatchSpanProcessor
from opentelemetry.sdk.metrics import MeterProvider
from opentelemetry.sdk.metrics.export import PeriodicExportingMetricReader
from opentelemetry.sdk.resources import Resource
from opentelemetry.exporter.otlp.proto.grpc.trace_exporter import OTLPSpanExporter
from opentelemetry.exporter.otlp.proto.grpc.metric_exporter import OTLPMetricExporter
from opentelemetry.instrumentation.asyncio import AsyncioInstrumentor
from opentelemetry.instrumentation.requests import RequestsInstrumentor
from opentelemetry.instrumentation.sqlalchemy import SQLAlchemyInstrumentor
from opentelemetry.trace import Status, StatusCode
from opentelemetry.trace.propagation.tracecontext import TraceContextTextMapPropagator
from opentelemetry.propagate import set_global_textmap
from opentelemetry.semconv.trace import SpanAttributes
from opentelemetry.semconv.resource import ResourceAttributes

from elasticsearch import AsyncElasticsearch
from pyiceberg.catalog import load_catalog
from pyiceberg.table import Table
import pandas as pd

logger = logging.getLogger(__name__)


class SpanType(str, Enum):
    """Types of spans for categorization"""
    DATA_PIPELINE = "data_pipeline"
    API_REQUEST = "api_request"
    DATABASE_QUERY = "database_query"
    CACHE_OPERATION = "cache_operation"
    ML_TRAINING = "ml_training"
    ML_INFERENCE = "ml_inference"
    DATA_TRANSFORMATION = "data_transformation"
    EXTERNAL_API = "external_api"
    MESSAGE_PROCESSING = "message_processing"
    BATCH_JOB = "batch_job"


class LineageLevel(str, Enum):
    """Granularity levels for data lineage"""
    DATASET = "dataset"
    TABLE = "table"
    COLUMN = "column"
    ROW = "row"
    CELL = "cell"


@dataclass
class DataLineageContext:
    """Context for tracking data lineage"""
    trace_id: str
    span_id: str
    dataset_id: str
    operation_type: str
    source_tables: List[str] = field(default_factory=list)
    target_tables: List[str] = field(default_factory=list)
    columns_read: List[str] = field(default_factory=list)
    columns_written: List[str] = field(default_factory=list)
    row_count: Optional[int] = None
    partition_keys: Dict[str, Any] = field(default_factory=dict)
    iceberg_snapshot_id: Optional[int] = None
    timestamp: datetime = field(default_factory=datetime.utcnow)


@dataclass
class ObservabilityConfig:
    """Configuration for unified observability"""
    service_name: str
    otlp_endpoint: str = "http://otel-collector:4317"
    enable_traces: bool = True
    enable_metrics: bool = True
    enable_logs: bool = True
    enable_lineage: bool = True
    elasticsearch_url: Optional[str] = "http://elasticsearch:9200"
    iceberg_catalog_uri: Optional[str] = "thrift://hive-metastore:9083"
    jaeger_ui_url: Optional[str] = "http://jaeger:16686"
    trace_sample_rate: float = 1.0
    metrics_export_interval: int = 60  # seconds
    lineage_retention_days: int = 90


class UnifiedObservability:
    """
    Unified observability system integrating:
    - OpenTelemetry distributed tracing
    - Iceberg metadata tracking
    - Data lineage correlation
    - Performance metrics
    - Custom span enrichment
    """
    
    def __init__(self, config: ObservabilityConfig):
        self.config = config
        self.tracer: Optional[trace.Tracer] = None
        self.meter: Optional[metrics.Meter] = None
        self.es_client: Optional[AsyncElasticsearch] = None
        self.iceberg_catalog = None
        self.propagator = TraceContextTextMapPropagator()
        
        # Lineage cache for batch processing
        self._lineage_cache: List[DataLineageContext] = []
        self._lineage_flush_task = None
        
    async def initialize(self):
        """Initialize observability components"""
        # Set up resource
        resource = Resource.create({
            ResourceAttributes.SERVICE_NAME: self.config.service_name,
            ResourceAttributes.SERVICE_VERSION: "1.0.0",
            ResourceAttributes.DEPLOYMENT_ENVIRONMENT: "production",
            ResourceAttributes.SERVICE_NAMESPACE: "platformq"
        })
        
        # Initialize tracing
        if self.config.enable_traces:
            self._init_tracing(resource)
            
        # Initialize metrics
        if self.config.enable_metrics:
            self._init_metrics(resource)
            
        # Initialize lineage tracking
        if self.config.enable_lineage:
            await self._init_lineage_tracking()
            
        # Instrument libraries
        self._instrument_libraries()
        
        # Set global propagator
        set_global_textmap(self.propagator)
        
        logger.info(f"Unified observability initialized for {self.config.service_name}")
        
    def _init_tracing(self, resource: Resource):
        """Initialize OpenTelemetry tracing"""
        # Create tracer provider
        provider = TracerProvider(
            resource=resource,
            sampler=trace.sampling.TraceIdRatioBased(self.config.trace_sample_rate)
        )
        
        # Add OTLP exporter
        otlp_exporter = OTLPSpanExporter(
            endpoint=self.config.otlp_endpoint,
            insecure=True
        )
        
        # Add span processor with custom enrichment
        span_processor = BatchSpanProcessor(
            otlp_exporter,
            max_queue_size=2048,
            max_export_batch_size=512,
            export_timeout_millis=30000
        )
        
        provider.add_span_processor(span_processor)
        
        # Set as global
        trace.set_tracer_provider(provider)
        
        # Get tracer
        self.tracer = trace.get_tracer(
            self.config.service_name,
            schema_url="https://opentelemetry.io/schemas/1.11.0"
        )
        
    def _init_metrics(self, resource: Resource):
        """Initialize OpenTelemetry metrics"""
        # Create metrics reader
        metric_reader = PeriodicExportingMetricReader(
            exporter=OTLPMetricExporter(
                endpoint=self.config.otlp_endpoint,
                insecure=True
            ),
            export_interval_millis=self.config.metrics_export_interval * 1000
        )
        
        # Create meter provider
        provider = MeterProvider(
            resource=resource,
            metric_readers=[metric_reader]
        )
        
        # Set as global
        metrics.set_meter_provider(provider)
        
        # Get meter
        self.meter = metrics.get_meter(self.config.service_name)
        
    async def _init_lineage_tracking(self):
        """Initialize data lineage tracking"""
        # Initialize Elasticsearch client
        if self.config.elasticsearch_url:
            self.es_client = AsyncElasticsearch([self.config.elasticsearch_url])
            await self._ensure_lineage_index()
            
        # Initialize Iceberg catalog
        if self.config.iceberg_catalog_uri:
            try:
                self.iceberg_catalog = load_catalog(
                    "platformq",
                    **{"uri": self.config.iceberg_catalog_uri}
                )
            except Exception as e:
                logger.warning(f"Failed to connect to Iceberg catalog: {e}")
                
        # Start lineage flush task
        self._lineage_flush_task = asyncio.create_task(self._lineage_flush_loop())
        
    def _instrument_libraries(self):
        """Auto-instrument common libraries"""
        # Asyncio instrumentation
        AsyncioInstrumentor().instrument()
        
        # HTTP requests instrumentation
        RequestsInstrumentor().instrument()
        
        # SQLAlchemy instrumentation (if available)
        try:
            SQLAlchemyInstrumentor().instrument()
        except Exception:
            pass
            
    @asynccontextmanager
    async def trace_operation(
        self,
        operation_name: str,
        span_type: SpanType,
        attributes: Optional[Dict[str, Any]] = None,
        links: Optional[List[trace.Link]] = None
    ):
        """
        Create a traced operation with automatic error handling and enrichment
        
        Usage:
            async with observability.trace_operation(
                "process_data", 
                SpanType.DATA_PIPELINE,
                attributes={"dataset": "sales_data"}
            ) as span:
                # Your operation code
                result = await process_data()
                span.set_attribute("row_count", result.row_count)
        """
        # Extract parent context from baggage if available
        ctx = baggage.get_baggage("trace_context")
        
        with self.tracer.start_as_current_span(
            operation_name,
            context=ctx,
            kind=trace.SpanKind.INTERNAL,
            attributes=attributes,
            links=links
        ) as span:
            # Add standard attributes
            span.set_attribute("span.type", span_type.value)
            span.set_attribute("service.name", self.config.service_name)
            span.set_attribute("operation.start_time", datetime.utcnow().isoformat())
            
            try:
                yield span
                span.set_status(Status(StatusCode.OK))
                
            except Exception as e:
                # Record exception
                span.record_exception(e)
                span.set_status(
                    Status(StatusCode.ERROR, f"{type(e).__name__}: {str(e)}")
                )
                raise
                
            finally:
                # Add duration
                span.set_attribute(
                    "operation.duration_ms",
                    (datetime.utcnow() - datetime.fromisoformat(
                        span.attributes.get("operation.start_time", datetime.utcnow().isoformat())
                    )).total_seconds() * 1000
                )
                
    async def trace_data_pipeline(
        self,
        pipeline_name: str,
        source_tables: List[str],
        target_tables: List[str],
        transformation_type: str = "unknown"
    ):
        """
        Trace a data pipeline operation with lineage tracking
        
        Usage:
            async with observability.trace_data_pipeline(
                "sales_aggregation",
                source_tables=["raw.sales", "dim.products"],
                target_tables=["analytics.sales_summary"]
            ) as (span, lineage_ctx):
                # Pipeline logic
                df = spark.read.table("raw.sales")
                result = df.groupBy("product_id").sum("amount")
                result.write.mode("overwrite").saveAsTable("analytics.sales_summary")
                
                # Update lineage context
                lineage_ctx.row_count = result.count()
        """
        async with self.trace_operation(
            pipeline_name,
            SpanType.DATA_PIPELINE,
            attributes={
                "pipeline.name": pipeline_name,
                "pipeline.source_tables": json.dumps(source_tables),
                "pipeline.target_tables": json.dumps(target_tables),
                "pipeline.transformation_type": transformation_type
            }
        ) as span:
            # Create lineage context
            lineage_ctx = DataLineageContext(
                trace_id=format(span.get_span_context().trace_id, '032x'),
                span_id=format(span.get_span_context().span_id, '016x'),
                dataset_id=f"{pipeline_name}_{datetime.utcnow().strftime('%Y%m%d_%H%M%S')}",
                operation_type=transformation_type,
                source_tables=source_tables,
                target_tables=target_tables
            )
            
            try:
                yield span, lineage_ctx
                
                # Enrich span with lineage info
                span.set_attribute("lineage.row_count", lineage_ctx.row_count or 0)
                span.set_attribute("lineage.columns_read", json.dumps(lineage_ctx.columns_read))
                span.set_attribute("lineage.columns_written", json.dumps(lineage_ctx.columns_written))
                
                # Track lineage
                await self._track_lineage(lineage_ctx)
                
            except Exception as e:
                span.set_attribute("lineage.error", str(e))
                raise
                
    async def track_iceberg_operation(
        self,
        table_name: str,
        operation: str,
        span: Optional[Span] = None
    ) -> Optional[Dict[str, Any]]:
        """Track Iceberg table operations and metadata"""
        if not self.iceberg_catalog:
            return None
            
        try:
            # Load table
            table = self.iceberg_catalog.load_table(table_name)
            
            # Get current snapshot
            snapshot = table.current_snapshot()
            
            metadata = {
                "table_name": table_name,
                "operation": operation,
                "snapshot_id": snapshot.snapshot_id if snapshot else None,
                "schema_version": table.metadata.current_schema_id,
                "partition_spec": str(table.spec()),
                "file_count": len(list(table.scan().plan_files())),
                "record_count": snapshot.summary.get("total-records") if snapshot else 0,
                "file_size_bytes": snapshot.summary.get("total-files-size") if snapshot else 0,
                "timestamp": datetime.utcnow().isoformat()
            }
            
            # Add to span if provided
            if span:
                for key, value in metadata.items():
                    span.set_attribute(f"iceberg.{key}", str(value))
                    
            # Store in lineage tracking
            if self.es_client:
                await self.es_client.index(
                    index="platformq_iceberg_operations",
                    document=metadata
                )
                
            return metadata
            
        except Exception as e:
            logger.error(f"Failed to track Iceberg operation: {e}")
            if span:
                span.set_attribute("iceberg.error", str(e))
            return None
            
    async def _track_lineage(self, lineage_ctx: DataLineageContext):
        """Track data lineage information"""
        # Add to cache for batch processing
        self._lineage_cache.append(lineage_ctx)
        
        # Flush if cache is large
        if len(self._lineage_cache) >= 100:
            await self._flush_lineage_cache()
            
    async def _flush_lineage_cache(self):
        """Flush lineage cache to storage"""
        if not self._lineage_cache or not self.es_client:
            return
            
        try:
            # Prepare documents
            documents = []
            for ctx in self._lineage_cache:
                doc = {
                    "trace_id": ctx.trace_id,
                    "span_id": ctx.span_id,
                    "dataset_id": ctx.dataset_id,
                    "operation_type": ctx.operation_type,
                    "source_tables": ctx.source_tables,
                    "target_tables": ctx.target_tables,
                    "columns_read": ctx.columns_read,
                    "columns_written": ctx.columns_written,
                    "row_count": ctx.row_count,
                    "partition_keys": ctx.partition_keys,
                    "iceberg_snapshot_id": ctx.iceberg_snapshot_id,
                    "timestamp": ctx.timestamp,
                    "service_name": self.config.service_name
                }
                documents.append(doc)
                
            # Bulk index
            await self._bulk_index_lineage(documents)
            
            # Clear cache
            self._lineage_cache.clear()
            
        except Exception as e:
            logger.error(f"Failed to flush lineage cache: {e}")
            
    async def _bulk_index_lineage(self, documents: List[Dict[str, Any]]):
        """Bulk index lineage documents to Elasticsearch"""
        actions = []
        for doc in documents:
            actions.append({"index": {"_index": "platformq_data_lineage"}})
            actions.append(doc)
            
        await self.es_client.bulk(body=actions)
        
    async def _lineage_flush_loop(self):
        """Background task to periodically flush lineage cache"""
        while True:
            try:
                await asyncio.sleep(60)  # Flush every minute
                await self._flush_lineage_cache()
            except Exception as e:
                logger.error(f"Error in lineage flush loop: {e}")
                
    async def _ensure_lineage_index(self):
        """Ensure Elasticsearch indices exist for lineage tracking"""
        indices = [
            {
                "name": "platformq_data_lineage",
                "mappings": {
                    "properties": {
                        "trace_id": {"type": "keyword"},
                        "span_id": {"type": "keyword"},
                        "dataset_id": {"type": "keyword"},
                        "operation_type": {"type": "keyword"},
                        "source_tables": {"type": "keyword"},
                        "target_tables": {"type": "keyword"},
                        "columns_read": {"type": "keyword"},
                        "columns_written": {"type": "keyword"},
                        "row_count": {"type": "long"},
                        "partition_keys": {"type": "object"},
                        "iceberg_snapshot_id": {"type": "long"},
                        "timestamp": {"type": "date"},
                        "service_name": {"type": "keyword"}
                    }
                }
            },
            {
                "name": "platformq_iceberg_operations",
                "mappings": {
                    "properties": {
                        "table_name": {"type": "keyword"},
                        "operation": {"type": "keyword"},
                        "snapshot_id": {"type": "long"},
                        "schema_version": {"type": "integer"},
                        "partition_spec": {"type": "text"},
                        "file_count": {"type": "integer"},
                        "record_count": {"type": "long"},
                        "file_size_bytes": {"type": "long"},
                        "timestamp": {"type": "date"}
                    }
                }
            }
        ]
        
        for index_config in indices:
            exists = await self.es_client.indices.exists(index=index_config["name"])
            if not exists:
                await self.es_client.indices.create(
                    index=index_config["name"],
                    mappings=index_config["mappings"]
                )
                
    def create_counter(self, name: str, description: str, unit: str = "1") -> metrics.Counter:
        """Create a counter metric"""
        return self.meter.create_counter(
            name=f"{self.config.service_name}.{name}",
            description=description,
            unit=unit
        )
        
    def create_histogram(self, name: str, description: str, unit: str = "ms") -> metrics.Histogram:
        """Create a histogram metric"""
        return self.meter.create_histogram(
            name=f"{self.config.service_name}.{name}",
            description=description,
            unit=unit
        )
        
    def create_gauge(self, name: str, description: str, unit: str = "1") -> metrics.ObservableGauge:
        """Create a gauge metric"""
        return self.meter.create_observable_gauge(
            name=f"{self.config.service_name}.{name}",
            description=description,
            unit=unit
        )
        
    async def query_lineage(
        self,
        dataset_id: Optional[str] = None,
        table_name: Optional[str] = None,
        start_time: Optional[datetime] = None,
        end_time: Optional[datetime] = None,
        limit: int = 100
    ) -> List[Dict[str, Any]]:
        """Query data lineage information"""
        if not self.es_client:
            return []
            
        # Build query
        must_clauses = []
        
        if dataset_id:
            must_clauses.append({"term": {"dataset_id": dataset_id}})
            
        if table_name:
            must_clauses.append({
                "bool": {
                    "should": [
                        {"term": {"source_tables": table_name}},
                        {"term": {"target_tables": table_name}}
                    ]
                }
            })
            
        if start_time or end_time:
            range_clause = {"range": {"timestamp": {}}}
            if start_time:
                range_clause["range"]["timestamp"]["gte"] = start_time.isoformat()
            if end_time:
                range_clause["range"]["timestamp"]["lte"] = end_time.isoformat()
            must_clauses.append(range_clause)
            
        query = {
            "bool": {
                "must": must_clauses
            }
        } if must_clauses else {"match_all": {}}
        
        # Execute query
        response = await self.es_client.search(
            index="platformq_data_lineage",
            query=query,
            size=limit,
            sort=[{"timestamp": {"order": "desc"}}]
        )
        
        return [hit["_source"] for hit in response["hits"]["hits"]]
        
    def get_trace_url(self, trace_id: str) -> str:
        """Get Jaeger UI URL for a trace"""
        return f"{self.config.jaeger_ui_url}/trace/{trace_id}"
        
    async def close(self):
        """Clean up resources"""
        # Cancel lineage flush task
        if self._lineage_flush_task:
            self._lineage_flush_task.cancel()
            
        # Flush remaining lineage
        await self._flush_lineage_cache()
        
        # Close Elasticsearch client
        if self.es_client:
            await self.es_client.close()
            
        logger.info("Unified observability closed")


# Singleton instance
_observability_instance: Optional[UnifiedObservability] = None


async def get_observability(config: Optional[ObservabilityConfig] = None) -> UnifiedObservability:
    """Get or create observability instance"""
    global _observability_instance
    
    if _observability_instance is None:
        if config is None:
            raise ValueError("Config required for first initialization")
        _observability_instance = UnifiedObservability(config)
        await _observability_instance.initialize()
        
    return _observability_instance 