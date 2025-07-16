"""
Digital Asset Processing DAG

This DAG demonstrates the migration of the workflow service's asset processing
logic to Apache Airflow. It waits for DigitalAssetCreated events and triggers
appropriate processing based on the asset type.
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.dummy import DummyOperator
from airflow.utils.trigger_rule import TriggerRule
from airflow.operators.http_operator import SimpleHttpOperator

# Import PlatformQ custom operators
import sys
sys.path.append('/opt/airflow/platformq_airflow')
from platformq_airflow.operators import (
    PulsarSensorOperator,
    ProcessorJobOperator,
    WASMFunctionOperator,
    PlatformQServiceOperator,
    PulsarEventOperator
)

# Define the DigitalAssetCreated schema
class DigitalAssetCreated:
    def __init__(self, tenant_id: str, asset_id: str, asset_type: str, 
                 raw_data_uri: str, created_by: str, created_at: int):
        self.tenant_id = tenant_id
        self.asset_id = asset_id
        self.asset_type = asset_type
        self.raw_data_uri = raw_data_uri
        self.created_by = created_by
        self.created_at = created_at

# Define the ProcessingCompleted schema
class ProcessingCompleted:
    def __init__(self, asset_id: str, processor_type: str, metadata: dict, 
                 processed_at: int, status: str):
        self.asset_id = asset_id
        self.processor_type = processor_type
        self.metadata = metadata
        self.processed_at = processed_at
        self.status = status

# Default DAG arguments
default_args = {
    'owner': 'platformq',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Create the DAG
dag = DAG(
    'digital_asset_processing',
    default_args=default_args,
    description='Process digital assets based on their type',
    schedule_interval=None,  # Event-driven, not scheduled
    catchup=False,
    tags=['platformq', 'asset-processing', 'event-driven'],
)

# Task 1: Wait for asset creation event
create_asset_task = SimpleHttpOperator(
    task_id="create_digital_asset",
    http_conn_id="platformq_api",
    endpoint="/api/v1/digital-assets",
    method="POST",
    data={
        "tenant_id": "{{ ti.xcom_pull(task_ids='wait_for_asset_created', key='tenant_id') }}",
        "asset_id": "{{ ti.xcom_pull(task_ids='wait_for_asset_created', key='event')['asset_id'] }}",
        "asset_type": "{{ ti.xcom_pull(task_ids='wait_for_asset_created', key='event')['asset_type'] }}",
        "raw_data_uri": "{{ ti.xcom_pull(task_ids='wait_for_asset_created', key='event')['raw_data_uri'] }}",
        "created_by": "{{ ti.xcom_pull(task_ids='wait_for_asset_created', key='event')['created_by'] }}",
        "created_at": "{{ ti.xcom_pull(task_ids='wait_for_asset_created', key='event')['created_at'] }}"
    },
    headers={"Authorization": "Bearer your_service_account_token"}
)

wait_for_asset_created = PulsarSensorOperator(
    task_id='wait_for_asset_created',
    topic_pattern='persistent://platformq/.*/digital-asset-created-events',
    schema_class=DigitalAssetCreated,
    subscription_name='airflow-asset-processing',
    mode='reschedule',  # Don't block a worker slot
    poke_interval=30,
    dag=dag,
)

# Task 2: Determine processor type
def determine_processor(**context):
    """Determine which processor to use based on asset type"""
    event = context['ti'].xcom_pull(task_ids='wait_for_asset_created', key='event')
    asset_type = event.get('asset_type', '').lower()
    
    # Map asset types to processor tasks
    processor_mapping = {
        'blender': 'process_blender_file',
        'blend': 'process_blender_file',
        'freecad': 'process_freecad_file',
        'fcstd': 'process_freecad_file',
        'gimp': 'process_gimp_file',
        'xcf': 'process_gimp_file',
        'openfoam': 'process_openfoam_case',
        'audacity': 'process_audacity_project',
        'aup3': 'process_audacity_project',
        'openshot': 'process_openshot_project',
        'osp': 'process_openshot_project',
        'flightgear': 'process_flightgear_log',
    }
    
    # Check if we have a specific processor
    for key, task_id in processor_mapping.items():
        if key in asset_type:
            return task_id
    
    # Default to WASM processor
    return 'process_with_wasm'

branch_processor = BranchPythonOperator(
    task_id='determine_processor',
    python_callable=determine_processor,
    dag=dag,
)

# Processor tasks for different file types
process_blender = ProcessorJobOperator(
    task_id='process_blender_file',
    processor_type='blender',
    asset_id="{{ ti.xcom_pull(task_ids='wait_for_asset_created', key='event')['asset_id'] }}",
    asset_uri="{{ ti.xcom_pull(task_ids='wait_for_asset_created', key='event')['raw_data_uri'] }}",
    resources={
        'requests': {'cpu': '500m', 'memory': '1Gi'},
        'limits': {'cpu': '2', 'memory': '4Gi'}
    },
    dag=dag,
)

process_freecad = ProcessorJobOperator(
    task_id='process_freecad_file',
    processor_type='freecad',
    asset_id="{{ ti.xcom_pull(task_ids='wait_for_asset_created', key='event')['asset_id'] }}",
    asset_uri="{{ ti.xcom_pull(task_ids='wait_for_asset_created', key='event')['raw_data_uri'] }}",
    dag=dag,
)

process_gimp = ProcessorJobOperator(
    task_id='process_gimp_file',
    processor_type='gimp',
    asset_id="{{ ti.xcom_pull(task_ids='wait_for_asset_created', key='event')['asset_id'] }}",
    asset_uri="{{ ti.xcom_pull(task_ids='wait_for_asset_created', key='event')['raw_data_uri'] }}",
    dag=dag,
)

process_openfoam = ProcessorJobOperator(
    task_id='process_openfoam_case',
    processor_type='openfoam',
    asset_id="{{ ti.xcom_pull(task_ids='wait_for_asset_created', key='event')['asset_id'] }}",
    asset_uri="{{ ti.xcom_pull(task_ids='wait_for_asset_created', key='event')['raw_data_uri'] }}",
    resources={
        'requests': {'cpu': '1', 'memory': '2Gi'},
        'limits': {'cpu': '4', 'memory': '8Gi'}
    },
    dag=dag,
)

process_audacity = ProcessorJobOperator(
    task_id='process_audacity_project',
    processor_type='audacity',
    asset_id="{{ ti.xcom_pull(task_ids='wait_for_asset_created', key='event')['asset_id'] }}",
    asset_uri="{{ ti.xcom_pull(task_ids='wait_for_asset_created', key='event')['raw_data_uri'] }}",
    dag=dag,
)

process_openshot = ProcessorJobOperator(
    task_id='process_openshot_project',
    processor_type='openshot',
    asset_id="{{ ti.xcom_pull(task_ids='wait_for_asset_created', key='event')['asset_id'] }}",
    asset_uri="{{ ti.xcom_pull(task_ids='wait_for_asset_created', key='event')['raw_data_uri'] }}",
    dag=dag,
)

process_flightgear = ProcessorJobOperator(
    task_id='process_flightgear_log',
    processor_type='flightgear',
    asset_id="{{ ti.xcom_pull(task_ids='wait_for_asset_created', key='event')['asset_id'] }}",
    asset_uri="{{ ti.xcom_pull(task_ids='wait_for_asset_created', key='event')['raw_data_uri'] }}",
    dag=dag,
)

process_wasm = WASMFunctionOperator(
    task_id='process_with_wasm',
    wasm_module_id='default_processor',
    asset_id="{{ ti.xcom_pull(task_ids='wait_for_asset_created', key='event')['asset_id'] }}",
    asset_uri="{{ ti.xcom_pull(task_ids='wait_for_asset_created', key='event')['raw_data_uri'] }}",
    tenant_id="{{ ti.xcom_pull(task_ids='wait_for_asset_created', key='tenant_id') }}",
    wait_for_completion=True,
    dag=dag,
)

# Task to aggregate processing results
def aggregate_results(**context):
    """Aggregate results from whichever processor ran"""
    # Get the event data
    event = context['ti'].xcom_pull(task_ids='wait_for_asset_created', key='event')
    
    # Try to get results from different processors
    for task_id in ['process_blender_file', 'process_freecad_file', 'process_gimp_file',
                    'process_openfoam_case', 'process_audacity_project', 'process_openshot_project',
                    'process_flightgear_log', 'process_with_wasm']:
        result = context['ti'].xcom_pull(task_ids=task_id)
        if result:
            return {
                'asset_id': event['asset_id'],
                'processor_result': result,
                'processed_by': task_id
            }
    
    return {
        'asset_id': event['asset_id'],
        'error': 'No processing result found'
    }

aggregate = PythonOperator(
    task_id='aggregate_results',
    python_callable=aggregate_results,
    trigger_rule=TriggerRule.ONE_SUCCESS,  # Run if any processor succeeds
    dag=dag,
)

# Update asset metadata in the digital asset service
update_asset = PlatformQServiceOperator(
    task_id='update_asset_metadata',
    service_name='digital-asset-service',
    endpoint="/api/v1/digital-assets/{{ ti.xcom_pull(task_ids='wait_for_asset_created', key='event')['asset_id'] }}",
    method='PATCH',
    payload="{{ ti.xcom_pull(task_ids='aggregate_results') }}",
    dag=dag,
)

# Publish processing completed event
publish_completed = PulsarEventOperator(
    task_id='publish_processing_completed',
    topic_base='asset-processing-completed-events',
    schema_class=ProcessingCompleted,
    event_data={
        'asset_id': "{{ ti.xcom_pull(task_ids='wait_for_asset_created', key='event')['asset_id'] }}",
        'processor_type': "{{ ti.xcom_pull(task_ids='aggregate_results')['processed_by'] }}",
        'metadata': "{{ ti.xcom_pull(task_ids='aggregate_results')['processor_result'] }}",
        'processed_at': "{{ ts_nodash }}",
        'status': 'completed'
    },
    tenant_id="{{ ti.xcom_pull(task_ids='wait_for_asset_created', key='tenant_id') }}",
    dag=dag,
)

# Error handling task
handle_error = DummyOperator(
    task_id='handle_processing_error',
    trigger_rule=TriggerRule.ONE_FAILED,
    dag=dag,
)

# Define task dependencies
create_asset_task >> wait_for_asset_created
wait_for_asset_created >> branch_processor
branch_processor >> [process_blender, process_freecad, process_gimp, process_openfoam,
                    process_audacity, process_openshot, process_flightgear, process_wasm]

# All processors flow to aggregate
[process_blender, process_freecad, process_gimp, process_openfoam,
 process_audacity, process_openshot, process_flightgear, process_wasm] >> aggregate

aggregate >> update_asset >> publish_completed

# Error handling
[process_blender, process_freecad, process_gimp, process_openfoam,
 process_audacity, process_openshot, process_flightgear, process_wasm] >> handle_error 