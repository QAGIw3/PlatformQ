"""
Distributed Rendering DAG using Apache Spark

This DAG demonstrates parallel rendering of digital assets using
Spark's distributed computing capabilities.
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.http.sensors.http import HttpSensor
from airflow.models import Variable

# Import custom operators
from platformq_operators.pulsar_operator import PulsarEventOperator, PulsarSensorOperator
from platformq_operators.service_operator import PlatformQServiceOperator
from platformq_operators.spark_platformq_operator import (
    SparkProcessorOperator,
    SparkPlatformQOperator
)

default_args = {
    'owner': 'platformq',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'distributed_rendering_spark',
    default_args=default_args,
    description='Distributed rendering pipeline using Spark',
    schedule_interval=None,  # Triggered by events
    catchup=False,
    tags=['spark', 'rendering', 'distributed'],
)


def extract_asset_info(**context):
    """Extract asset information from event"""
    event = context['ti'].xcom_pull(task_ids='wait_for_render_request', key='event_data')
    
    asset_info = {
        'asset_id': event['asset_id'],
        'asset_uri': event['asset_uri'],
        'asset_type': event['asset_type'],
        'tenant_id': event['tenant_id'],
        'processing_config': event.get('processing_config', {})
    }
    
    # Set default processing configuration
    if 'resolution' not in asset_info['processing_config']:
        asset_info['processing_config']['resolution'] = {'x': 1920, 'y': 1080}
    
    if 'output_format' not in asset_info['processing_config']:
        asset_info['processing_config']['output_format'] = 'PNG'
    
    return asset_info


def prepare_spark_config(**context):
    """Prepare Spark configuration based on job requirements"""
    asset_info = context['ti'].xcom_pull(task_ids='extract_asset_info')
    
    # Calculate resource requirements
    frames = asset_info['processing_config'].get('end_frame', 1) - \
             asset_info['processing_config'].get('start_frame', 1) + 1
    
    # Dynamic resource allocation
    executors = min(max(frames // 10, 2), 20)  # 1 executor per 10 frames, max 20
    
    spark_config = {
        'conf': {
            'spark.executor.instances': str(executors),
            'spark.executor.memory': '4g',
            'spark.executor.cores': '2',
            'spark.dynamicAllocation.enabled': 'true',
            'spark.dynamicAllocation.minExecutors': '2',
            'spark.dynamicAllocation.maxExecutors': str(executors * 2)
        }
    }
    
    return spark_config


# Wait for render request event
wait_for_render_request = PulsarSensorOperator(
    task_id='wait_for_render_request',
    topic='render-requests',
    subscription_name='airflow-spark-render',
    event_filter=lambda event: event.get('processing_type') == 'distributed',
    poke_interval=30,
    timeout=3600,
    dag=dag,
)

# Extract asset information
extract_asset = PythonOperator(
    task_id='extract_asset_info',
    python_callable=extract_asset_info,
    provide_context=True,
    dag=dag,
)

# Prepare Spark configuration
prepare_config = PythonOperator(
    task_id='prepare_spark_config',
    python_callable=prepare_spark_config,
    provide_context=True,
    dag=dag,
)

# Distributed Blender rendering
render_blender = SparkProcessorOperator(
    task_id='render_blender_distributed',
    processor_type='blender',
    asset_id="{{ ti.xcom_pull(task_ids='extract_asset_info')['asset_id'] }}",
    asset_uri="{{ ti.xcom_pull(task_ids='extract_asset_info')['asset_uri'] }}",
    tenant_id="{{ ti.xcom_pull(task_ids='extract_asset_info')['tenant_id'] }}",
    processing_config="{{ ti.xcom_pull(task_ids='extract_asset_info')['processing_config'] }}",
    spark_config="{{ ti.xcom_pull(task_ids='prepare_spark_config') }}",
    trigger_rule='all_success',
    dag=dag,
)

# Post-process results
def post_process_results(**context):
    """Post-process rendering results"""
    result = context['ti'].xcom_pull(task_ids='render_blender_distributed', key='processing_result')
    asset_info = context['ti'].xcom_pull(task_ids='extract_asset_info')
    
    # Create composite video from frames if animation
    if result.get('total_frames_rendered', 0) > 1:
        # Submit video composition job
        compose_config = {
            'frame_uris': result['frame_uris'],
            'output_format': 'mp4',
            'fps': asset_info['processing_config'].get('fps', 24)
        }
        
        # This would trigger another Spark job for video composition
        return {
            'status': 'composition_required',
            'compose_config': compose_config
        }
    
    return {
        'status': 'completed',
        'result': result
    }


post_process = PythonOperator(
    task_id='post_process_results',
    python_callable=post_process_results,
    provide_context=True,
    dag=dag,
)

# Update asset metadata
update_metadata = PlatformQServiceOperator(
    task_id='update_asset_metadata',
    service='digital-asset-service',
    endpoint='/api/v1/assets/{{ ti.xcom_pull(task_ids="extract_asset_info")["asset_id"] }}/metadata',
    method='PATCH',
    data={
        'processing_status': 'completed',
        'render_output': '{{ ti.xcom_pull(task_ids="render_blender_distributed", key="processing_result") }}',
        'processed_at': '{{ ds }}'
    },
    dag=dag,
)

# Publish completion event
publish_completion = PulsarEventOperator(
    task_id='publish_render_complete',
    topic='render-completed',
    event_type='asset.render.completed',
    event_data={
        'asset_id': '{{ ti.xcom_pull(task_ids="extract_asset_info")["asset_id"] }}',
        'tenant_id': '{{ ti.xcom_pull(task_ids="extract_asset_info")["tenant_id"] }}',
        'result': '{{ ti.xcom_pull(task_ids="render_blender_distributed", key="processing_result") }}',
        'timestamp': '{{ ts }}'
    },
    dag=dag,
)

# Optional: Video composition for animations
compose_video = SparkPlatformQOperator(
    task_id='compose_video',
    tenant_id="{{ ti.xcom_pull(task_ids='extract_asset_info')['tenant_id'] }}",
    application='/opt/spark-jobs/video_composer.py',
    application_args=[
        '{{ ti.xcom_pull(task_ids="extract_asset_info")["asset_id"] }}',
        '{{ ti.xcom_pull(task_ids="post_process_results")["compose_config"] | tojson }}'
    ],
    conf={
        'spark.executor.instances': '4',
        'spark.executor.memory': '2g'
    },
    trigger_rule='none_failed_or_skipped',
    dag=dag,
)

# Define task dependencies
wait_for_render_request >> extract_asset >> prepare_config >> render_blender
render_blender >> post_process >> [update_metadata, publish_completion]
post_process >> compose_video

# Parallel quality analysis using Spark
analyze_quality = SparkPlatformQOperator(
    task_id='analyze_render_quality',
    tenant_id="{{ ti.xcom_pull(task_ids='extract_asset_info')['tenant_id'] }}",
    application='/opt/spark-jobs/quality_analyzer.py',
    application_args=[
        '{{ ti.xcom_pull(task_ids="render_blender_distributed", key="processing_result")["frame_uris"] | tojson }}'
    ],
    conf={
        'spark.executor.instances': '2',
        'spark.executor.memory': '2g'
    },
    trigger_rule='none_failed_or_skipped',
    dag=dag,
)

render_blender >> analyze_quality 