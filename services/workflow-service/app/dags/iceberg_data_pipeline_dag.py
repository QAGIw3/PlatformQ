"""
Iceberg Data Pipeline DAG

Demonstrates Apache Iceberg features in data pipelines:
- Schema evolution
- Time travel queries
- Table optimization
- Format conversion
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.http import SimpleHttpOperator
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


def ingest_to_iceberg(**context):
    """Ingest data to Iceberg table in bronze zone"""
    conf = context['dag_run'].conf
    dataset_name = conf.get('dataset_name', 'sample_data')
    tenant_id = conf.get('tenant_id', 'default')
    
    async def _ingest():
        async with httpx.AsyncClient(timeout=60.0) as client:
            response = await client.post(
                'http://data-platform-service:8000/api/v1/lake/ingest',
                json={
                    'source_path': conf.get('source_path'),
                    'dataset_name': dataset_name,
                    'zone': 'bronze',
                    'tenant_id': tenant_id,
                    'format': 'iceberg',  # Use Iceberg format
                    'partition_by': ['date', 'category']
                }
            )
            response.raise_for_status()
            return response.json()
    
    import asyncio
    result = asyncio.run(_ingest())
    
    # Store result for next tasks
    context['task_instance'].xcom_push(key='ingestion_result', value=result)
    return result


def evolve_schema(**context):
    """Evolve Iceberg table schema"""
    conf = context['dag_run'].conf
    table_name = conf.get('table_name')
    
    if not table_name:
        logger.info("No table specified for schema evolution")
        return
    
    async def _evolve():
        async with httpx.AsyncClient() as client:
            response = await client.put(
                f'http://data-platform-service:8000/api/v1/lake/iceberg/{table_name}/schema',
                json={
                    'add_columns': conf.get('add_columns', {}),
                    'rename_columns': conf.get('rename_columns', {}),
                    'drop_columns': conf.get('drop_columns', [])
                }
            )
            response.raise_for_status()
            return response.json()
    
    import asyncio
    result = asyncio.run(_evolve())
    logger.info(f"Schema evolution result: {result}")
    return result


def time_travel_analysis(**context):
    """Perform time travel analysis on Iceberg table"""
    conf = context['dag_run'].conf
    table_name = conf.get('table_name')
    
    if not table_name:
        logger.info("No table specified for time travel")
        return
    
    async def _analyze():
        async with httpx.AsyncClient() as client:
            # Get snapshots
            snapshots_response = await client.get(
                f'http://data-platform-service:8000/api/v1/lake/iceberg/{table_name}/snapshots'
            )
            snapshots_response.raise_for_status()
            snapshots = snapshots_response.json()['snapshots']
            
            # Analyze changes between snapshots
            if len(snapshots) >= 2:
                # Query current state
                current_response = await client.post(
                    'http://data-platform-service:8000/api/v1/lake/iceberg/time-travel',
                    json={
                        'table_name': table_name,
                        'query': f'SELECT COUNT(*) as count FROM time_travel_table',
                        'limit': 1
                    }
                )
                current_response.raise_for_status()
                
                # Query previous snapshot
                previous_response = await client.post(
                    'http://data-platform-service:8000/api/v1/lake/iceberg/time-travel',
                    json={
                        'table_name': table_name,
                        'snapshot_id': snapshots[1]['snapshot_id'],
                        'query': f'SELECT COUNT(*) as count FROM time_travel_table',
                        'limit': 1
                    }
                )
                previous_response.raise_for_status()
                
                current_count = current_response.json()['data'][0]['count']
                previous_count = previous_response.json()['data'][0]['count']
                
                return {
                    'current_count': current_count,
                    'previous_count': previous_count,
                    'rows_added': current_count - previous_count,
                    'snapshots_analyzed': 2
                }
            
            return {'message': 'Not enough snapshots for comparison'}
    
    import asyncio
    result = asyncio.run(_analyze())
    logger.info(f"Time travel analysis: {result}")
    return result


def optimize_tables(**context):
    """Optimize Iceberg tables"""
    async def _optimize():
        async with httpx.AsyncClient(timeout=300.0) as client:
            response = await client.post(
                'http://data-platform-service:8000/api/v1/lake/iceberg/optimize',
                json={
                    'compact_small_files': True,
                    'expire_snapshots': True,
                    'rewrite_manifests': True
                }
            )
            response.raise_for_status()
            return response.json()
    
    import asyncio
    result = asyncio.run(_optimize())
    logger.info(f"Optimization result: {result}")
    return result


def convert_delta_to_iceberg(**context):
    """Convert existing Delta tables to Iceberg"""
    conf = context['dag_run'].conf
    delta_tables = conf.get('delta_tables', [])
    
    async def _convert():
        results = []
        async with httpx.AsyncClient(timeout=300.0) as client:
            for table in delta_tables:
                try:
                    response = await client.post(
                        'http://data-platform-service:8000/api/v1/lake/iceberg/convert',
                        json={
                            'delta_path': table['delta_path'],
                            'iceberg_table': table['iceberg_table'],
                            'partition_by': table.get('partition_by', [])
                        }
                    )
                    response.raise_for_status()
                    results.append({
                        'table': table['iceberg_table'],
                        'status': 'success'
                    })
                except Exception as e:
                    results.append({
                        'table': table.get('iceberg_table', 'unknown'),
                        'status': 'failed',
                        'error': str(e)
                    })
        
        return results
    
    import asyncio
    results = asyncio.run(_convert())
    logger.info(f"Conversion results: {results}")
    return results


# Create DAG
dag = DAG(
    'iceberg_data_pipeline',
    default_args=default_args,
    description='Iceberg data pipeline with schema evolution and time travel',
    schedule_interval='@daily',
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=['iceberg', 'data-lake', 'schema-evolution', 'time-travel']
)

# Define tasks
ingest_task = PythonOperator(
    task_id='ingest_to_iceberg',
    python_callable=ingest_to_iceberg,
    dag=dag
)

evolve_schema_task = PythonOperator(
    task_id='evolve_schema',
    python_callable=evolve_schema,
    dag=dag
)

time_travel_task = PythonOperator(
    task_id='time_travel_analysis',
    python_callable=time_travel_analysis,
    dag=dag
)

optimize_task = PythonOperator(
    task_id='optimize_tables',
    python_callable=optimize_tables,
    dag=dag
)

convert_task = PythonOperator(
    task_id='convert_delta_to_iceberg',
    python_callable=convert_delta_to_iceberg,
    dag=dag
)

# Set dependencies
ingest_task >> evolve_schema_task >> time_travel_task >> optimize_task
convert_task  # Independent task for conversion 