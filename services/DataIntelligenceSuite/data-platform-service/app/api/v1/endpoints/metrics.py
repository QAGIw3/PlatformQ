"""
Metrics API endpoints
"""

from prometheus_client import generate_latest, Counter, Histogram, Gauge, CONTENT_TYPE_LATEST
from fastapi import APIRouter, Response

router = APIRouter()

# Define metrics
ingestion_records_total = Counter(
    'ingestion_records_total',
    'Total number of records ingested',
    ['source_type', 'destination_type']
)

ingestion_bytes_total = Counter(
    'ingestion_bytes_total',
    'Total bytes processed',
    ['source_type']
)

ingestion_errors_total = Counter(
    'ingestion_errors_total',
    'Total ingestion errors',
    ['source_type', 'error_type']
)

cdc_lag_seconds = Gauge(
    'cdc_lag_seconds',
    'CDC replication lag in seconds',
    ['source_id', 'table']
)

stream_lag_messages = Gauge(
    'stream_lag_messages',
    'Stream consumer lag in messages',
    ['stream_id', 'topic']
)

batch_processing_duration = Histogram(
    'batch_processing_duration_seconds',
    'Batch job processing duration',
    ['job_type', 'status']
)

active_cdc_sources = Gauge(
    'active_cdc_sources',
    'Number of active CDC sources',
    ['source_type']
)

active_stream_ingestions = Gauge(
    'active_stream_ingestions',
    'Number of active stream ingestions',
    ['source_type']
)

schema_registry_size = Gauge(
    'schema_registry_size',
    'Number of schemas in registry',
    ['schema_type']
)

seatunnel_jobs_active = Gauge(
    'seatunnel_jobs_active',
    'Number of active SeaTunnel jobs',
    ['job_type']
)

seatunnel_jobs_total = Counter(
    'seatunnel_jobs_total',
    'Total SeaTunnel jobs created',
    ['job_type', 'status']
)


@router.get("/")
async def get_metrics():
    """Export Prometheus metrics"""
    return Response(
        content=generate_latest(),
        media_type=CONTENT_TYPE_LATEST
    )


@router.get("/summary")
async def get_metrics_summary():
    """Get a summary of key metrics"""
    # This would aggregate metrics from various sources
    # For now, return a simple summary
    return {
        "ingestion": {
            "total_records": 0,  # Would get from actual metrics
            "total_bytes": 0,
            "error_rate": 0.0
        },
        "cdc": {
            "active_sources": 0,
            "average_lag_seconds": 0.0
        },
        "streams": {
            "active_ingestions": 0,
            "total_messages": 0,
            "average_lag": 0.0
        },
        "batch": {
            "jobs_completed": 0,
            "jobs_failed": 0,
            "average_duration_seconds": 0.0
        },
        "schemas": {
            "total_schemas": 0,
            "total_versions": 0
        }
    } 