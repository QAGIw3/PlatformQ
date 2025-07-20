"""
SeaTunnel Zero-ETL Pipeline DAG

Demonstrates zero-ETL data integration with Apache SeaTunnel:
- Declarative pipeline configuration
- Multiple source/sink connectors
- Change Data Capture (CDC)
- Real-time transformations
- No custom code required
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.models import Variable
import logging
import httpx
import json

logger = logging.getLogger(__name__)

default_args = {
    'owner': 'data-platform',
    'depends_on_past': False,
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5)
}


def create_cdc_pipeline(**context):
    """Create CDC pipeline from MySQL to Iceberg"""
    conf = context['dag_run'].conf
    
    pipeline_config = {
        "name": "mysql_to_iceberg_cdc",
        "description": "Real-time CDC from MySQL to Iceberg data lake",
        "engine": "flink",  # Use Flink for streaming
        "mode": "streaming",
        "parallelism": 4,
        "source": {
            "connector_type": "cdc-mysql",
            "config": {
                "hostname": conf.get('mysql_host', 'mysql'),
                "port": conf.get('mysql_port', 3306),
                "username": conf.get('mysql_user', 'root'),
                "password": conf.get('mysql_password', 'password'),
                "database-name": conf.get('mysql_database', 'ecommerce'),
                "table-name": conf.get('mysql_table', 'orders'),
                "server-id": 5432,
                "scan.startup.mode": "initial",
                "debezium.snapshot.mode": "when_needed"
            }
        },
        "transforms": [
            {
                "transform_type": "sql",
                "config": {
                    "query": """
                    SELECT 
                        id,
                        customer_id,
                        order_date,
                        total_amount,
                        status,
                        CASE 
                            WHEN total_amount > 1000 THEN 'high'
                            WHEN total_amount > 500 THEN 'medium'
                            ELSE 'low'
                        END as order_value_category,
                        __op as cdc_operation,
                        __ts_ms as cdc_timestamp
                    FROM source_table
                    """
                }
            }
        ],
        "sink": {
            "connector_type": "iceberg",
            "config": {
                "catalog_name": "iceberg",
                "namespace": "lakehouse",
                "table": "orders_cdc",
                "catalog_type": "hive",
                "warehouse": "s3a://data-lake/iceberg/warehouse",
                "format-version": "2",
                "write.upsert.enabled": "true",
                "primary-key": "id"
            }
        }
    }
    
    async def _create():
        async with httpx.AsyncClient() as client:
            response = await client.post(
                'http://data-platform-service:8000/api/v1/pipelines/create',
                json={
                    "config": pipeline_config,
                    "tenant_id": conf.get('tenant_id', 'default')
                }
            )
            response.raise_for_status()
            return response.json()
    
    import asyncio
    result = asyncio.run(_create())
    
    context['task_instance'].xcom_push(key='cdc_pipeline', value=result)
    logger.info(f"Created CDC pipeline: {result}")
    return result


def create_batch_sync_pipeline(**context):
    """Create batch sync pipeline from Elasticsearch to Delta Lake"""
    conf = context['dag_run'].conf
    
    pipeline_config = {
        "name": "elasticsearch_to_delta_sync",
        "description": "Batch sync from Elasticsearch to Delta Lake",
        "engine": "spark",
        "mode": "batch",
        "parallelism": 8,
        "source": {
            "connector_type": "elasticsearch",
            "config": {
                "hosts": ["http://elasticsearch:9200"],
                "index": conf.get('es_index', 'product_catalog'),
                "query": json.dumps({
                    "query": {
                        "range": {
                            "updated_at": {
                                "gte": "now-1d/d",
                                "lt": "now/d"
                            }
                        }
                    }
                }),
                "scroll_size": 1000,
                "scroll_time": "1m"
            }
        },
        "transforms": [
            {
                "transform_type": "fieldMapper",
                "config": {
                    "field_mapper": {
                        "product_id": "id",
                        "product_name": "name",
                        "product_description": "description",
                        "product_price": "price",
                        "product_category": "category",
                        "last_updated": "updated_at"
                    }
                }
            },
            {
                "transform_type": "filter",
                "config": {
                    "filters": [
                        "price > 0",
                        "category IS NOT NULL"
                    ]
                }
            }
        ],
        "sink": {
            "connector_type": "delta",
            "config": {
                "path": "s3a://data-lake/delta/products",
                "mode": "overwrite",
                "options": {
                    "mergeSchema": "true",
                    "overwriteSchema": "true"
                },
                "partition_by": ["category", "date"]
            }
        }
    }
    
    async def _create():
        async with httpx.AsyncClient() as client:
            response = await client.post(
                'http://data-platform-service:8000/api/v1/pipelines/create',
                json={
                    "config": pipeline_config,
                    "tenant_id": conf.get('tenant_id', 'default')
                }
            )
            response.raise_for_status()
            return response.json()
    
    import asyncio
    result = asyncio.run(_create())
    
    context['task_instance'].xcom_push(key='batch_pipeline', value=result)
    logger.info(f"Created batch sync pipeline: {result}")
    return result


def create_stream_processing_pipeline(**context):
    """Create stream processing pipeline from Pulsar to Ignite"""
    conf = context['dag_run'].conf
    
    pipeline_config = {
        "name": "pulsar_to_ignite_streaming",
        "description": "Real-time stream processing from Pulsar to Ignite cache",
        "engine": "flink",
        "mode": "streaming",
        "parallelism": 4,
        "source": {
            "connector_type": "pulsar",
            "config": {
                "topic": conf.get('pulsar_topic', 'user-activity-events'),
                "service-url": "pulsar://pulsar:6650",
                "admin-url": "http://pulsar:8080",
                "subscription-name": "seatunnel-processor",
                "subscription-type": "Shared",
                "format": "json"
            }
        },
        "transforms": [
            {
                "transform_type": "sql",
                "config": {
                    "query": """
                    SELECT 
                        user_id,
                        event_type,
                        event_timestamp,
                        COUNT(*) OVER (
                            PARTITION BY user_id 
                            ORDER BY event_timestamp 
                            RANGE BETWEEN INTERVAL '5' MINUTE PRECEDING AND CURRENT ROW
                        ) as events_last_5min,
                        COUNT(*) OVER (
                            PARTITION BY user_id, event_type 
                            ORDER BY event_timestamp 
                            RANGE BETWEEN INTERVAL '1' HOUR PRECEDING AND CURRENT ROW
                        ) as event_type_count_1h
                    FROM source_table
                    """
                }
            }
        ],
        "sink": {
            "connector_type": "ignite",
            "config": {
                "host": "ignite",
                "port": 10800,
                "cache_name": "user_activity_metrics",
                "key_field": "user_id",
                "write_mode": "upsert"
            }
        }
    }
    
    async def _create():
        async with httpx.AsyncClient() as client:
            response = await client.post(
                'http://data-platform-service:8000/api/v1/pipelines/create',
                json={
                    "config": pipeline_config,
                    "tenant_id": conf.get('tenant_id', 'default')
                }
            )
            response.raise_for_status()
            return response.json()
    
    import asyncio
    result = asyncio.run(_create())
    
    context['task_instance'].xcom_push(key='stream_pipeline', value=result)
    logger.info(f"Created stream processing pipeline: {result}")
    return result


def run_pipelines(**context):
    """Run all created pipelines"""
    pipelines = []
    for key in ['cdc_pipeline', 'batch_pipeline', 'stream_pipeline']:
        pipeline_result = context['task_instance'].xcom_pull(
            task_ids=f'create_{key.split("_")[0]}_*',
            key=key
        )
        if pipeline_result:
            pipelines.append(pipeline_result)
    
    async def _run_all():
        results = []
        async with httpx.AsyncClient() as client:
            for pipeline in pipelines:
                if pipeline and 'pipeline_id' in pipeline:
                    response = await client.post(
                        f'http://data-platform-service:8000/api/v1/pipelines/{pipeline["pipeline_id"]}/run'
                    )
                    response.raise_for_status()
                    result = response.json()
                    results.append({
                        'pipeline_id': pipeline['pipeline_id'],
                        'pipeline_name': pipeline.get('name'),
                        'status': result.get('status'),
                        'job_id': result.get('job_id')
                    })
                    logger.info(f"Started pipeline {pipeline['pipeline_id']}: {result}")
        
        return results
    
    import asyncio
    results = asyncio.run(_run_all())
    
    return {
        'pipelines_started': len(results),
        'results': results
    }


def monitor_pipeline_status(**context):
    """Monitor pipeline execution status"""
    run_results = context['task_instance'].xcom_pull(
        task_ids='run_pipelines'
    )
    
    if not run_results or 'results' not in run_results:
        logger.warning("No pipeline results to monitor")
        return
    
    async def _monitor():
        statuses = []
        async with httpx.AsyncClient() as client:
            for pipeline in run_results['results']:
                response = await client.get(
                    f'http://data-platform-service:8000/api/v1/pipelines/{pipeline["pipeline_id"]}/status'
                )
                response.raise_for_status()
                status = response.json()
                statuses.append({
                    'pipeline_id': pipeline['pipeline_id'],
                    'pipeline_name': pipeline['pipeline_name'],
                    'status': status['status'],
                    'progress': status.get('progress', {}),
                    'metrics': status.get('metrics', {})
                })
        
        return statuses
    
    import asyncio
    statuses = asyncio.run(_monitor())
    
    # Log status summary
    for status in statuses:
        logger.info(f"Pipeline {status['pipeline_name']}: {status['status']}")
        if status.get('metrics'):
            logger.info(f"  Metrics: {status['metrics']}")
    
    return {
        'pipeline_statuses': statuses,
        'summary': {
            'total': len(statuses),
            'running': sum(1 for s in statuses if s['status'] == 'running'),
            'succeeded': sum(1 for s in statuses if s['status'] == 'succeeded'),
            'failed': sum(1 for s in statuses if s['status'] == 'failed')
        }
    }


# Create DAG
dag = DAG(
    'seatunnel_zero_etl',
    default_args=default_args,
    description='Zero-ETL data integration with Apache SeaTunnel',
    schedule_interval='@daily',
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=['seatunnel', 'zero-etl', 'cdc', 'streaming', 'data-integration']
)

# Define tasks
create_cdc_task = PythonOperator(
    task_id='create_cdc_pipeline',
    python_callable=create_cdc_pipeline,
    dag=dag
)

create_batch_task = PythonOperator(
    task_id='create_batch_sync_pipeline',
    python_callable=create_batch_sync_pipeline,
    dag=dag
)

create_stream_task = PythonOperator(
    task_id='create_stream_processing_pipeline',
    python_callable=create_stream_processing_pipeline,
    dag=dag
)

run_pipelines_task = PythonOperator(
    task_id='run_pipelines',
    python_callable=run_pipelines,
    dag=dag
)

monitor_task = PythonOperator(
    task_id='monitor_pipeline_status',
    python_callable=monitor_pipeline_status,
    dag=dag
)

# Set dependencies - pipelines can be created in parallel
[create_cdc_task, create_batch_task, create_stream_task] >> run_pipelines_task >> monitor_task 