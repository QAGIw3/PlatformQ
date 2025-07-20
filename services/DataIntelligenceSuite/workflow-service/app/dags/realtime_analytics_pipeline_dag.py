"""
Real-time Analytics Pipeline DAG

Orchestrates the setup and management of real-time analytics pipelines.
Integrates data-platform-service streaming capabilities with analytics-service.
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.sensors.external_task import ExternalTaskSensor
import logging
import httpx
import json
import asyncio

logger = logging.getLogger(__name__)

default_args = {
    'owner': 'data-platform',
    'depends_on_past': False,
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=2)
}


def setup_event_subscription(**context):
    """Set up event subscription for real-time data"""
    conf = context['dag_run'].conf
    event_types = conf.get('event_types', [])
    tenant_id = conf.get('tenant_id')
    
    async def _subscribe():
        async with httpx.AsyncClient() as client:
            response = await client.post(
                'http://data-platform-service:8000/api/v1/integrations/events/subscribe',
                json={
                    'event_types': event_types,
                    'consumer_group': f"analytics_{conf.get('pipeline_name', 'default')}",
                    'processing_config': {
                        'batch_size': conf.get('batch_size', 100),
                        'batch_timeout_ms': conf.get('batch_timeout_ms', 1000)
                    }
                },
                headers={'X-Tenant-ID': tenant_id}
            )
            response.raise_for_status()
            return response.json()
    
    result = asyncio.run(_subscribe())
    context['task_instance'].xcom_push(key='subscription_id', value=result['subscription_id'])
    logger.info(f"Created event subscription: {result['subscription_id']}")
    return result


def create_streaming_pipeline(**context):
    """Create streaming data pipeline"""
    conf = context['dag_run'].conf
    subscription_id = context['task_instance'].xcom_pull(task_ids='setup_event_subscription', key='subscription_id')
    
    pipeline_config = {
        'name': conf.get('pipeline_name', 'realtime_analytics'),
        'source': {
            'type': 'pulsar',
            'subscription_id': subscription_id,
            'topics': conf.get('topics', [])
        },
        'transformations': conf.get('transformations', [
            {'type': 'parse_json'},
            {'type': 'filter', 'condition': 'event_type != null'},
            {'type': 'enrich', 'lookup_table': 'user_profiles'}
        ]),
        'sink': {
            'type': conf.get('sink_type', 'elasticsearch'),
            'index': conf.get('sink_index', 'realtime_events'),
            'batch_size': conf.get('sink_batch_size', 1000)
        }
    }
    
    async def _create_pipeline():
        async with httpx.AsyncClient() as client:
            response = await client.post(
                'http://data-platform-service:8000/api/v1/integrations/events/streaming-pipeline',
                json=pipeline_config,
                headers={'X-Tenant-ID': conf.get('tenant_id')}
            )
            response.raise_for_status()
            return response.json()
    
    result = asyncio.run(_create_pipeline())
    context['task_instance'].xcom_push(key='pipeline_id', value=result['pipeline_id'])
    logger.info(f"Created streaming pipeline: {result['pipeline_id']}")
    return result


def setup_analytics_dashboard(**context):
    """Configure analytics dashboard for real-time data"""
    conf = context['dag_run'].conf
    pipeline_id = context['task_instance'].xcom_pull(task_ids='create_streaming_pipeline', key='pipeline_id')
    
    dashboard_config = {
        'name': f"Real-time Dashboard - {conf.get('pipeline_name', 'default')}",
        'description': conf.get('dashboard_description', 'Real-time analytics dashboard'),
        'datasources': [{
            'type': 'elasticsearch',
            'index': conf.get('sink_index', 'realtime_events'),
            'refresh_interval': '5s'
        }],
        'visualizations': conf.get('visualizations', [
            {
                'type': 'line_chart',
                'title': 'Events per Minute',
                'metric': 'count',
                'group_by': 'event_type',
                'interval': '1m'
            },
            {
                'type': 'pie_chart',
                'title': 'Event Distribution',
                'metric': 'count',
                'group_by': 'event_category'
            },
            {
                'type': 'data_table',
                'title': 'Recent Events',
                'columns': ['timestamp', 'event_type', 'user_id', 'status'],
                'limit': 100
            }
        ])
    }
    
    async def _create_dashboard():
        async with httpx.AsyncClient() as client:
            response = await client.post(
                'http://data-platform-service:8000/api/v1/integrations/analytics/dashboards',
                json=dashboard_config,
                headers={'X-Tenant-ID': conf.get('tenant_id')}
            )
            response.raise_for_status()
            return response.json()
    
    result = asyncio.run(_create_dashboard())
    logger.info(f"Created analytics dashboard: {result['dashboard_id']}")
    return result


def enable_anomaly_detection(**context):
    """Enable anomaly detection on streaming data"""
    conf = context['dag_run'].conf
    
    if not conf.get('enable_anomaly_detection', True):
        logger.info("Anomaly detection not requested")
        return
    
    anomaly_config = {
        'dataset_id': conf.get('sink_index', 'realtime_events'),
        'metrics': conf.get('anomaly_metrics', ['event_count', 'processing_time']),
        'sensitivity': conf.get('anomaly_sensitivity', 'medium'),
        'alert_channels': conf.get('alert_channels', ['email', 'slack'])
    }
    
    async def _enable_anomaly():
        async with httpx.AsyncClient() as client:
            response = await client.post(
                'http://data-platform-service:8000/api/v1/integrations/analytics/anomaly-detection',
                json=anomaly_config,
                headers={'X-Tenant-ID': conf.get('tenant_id')}
            )
            response.raise_for_status()
            return response.json()
    
    result = asyncio.run(_enable_anomaly())
    logger.info(f"Enabled anomaly detection: {result}")
    return result


def validate_pipeline_health(**context):
    """Validate the pipeline is working correctly"""
    pipeline_id = context['task_instance'].xcom_pull(task_ids='create_streaming_pipeline', key='pipeline_id')
    
    async def _check_health():
        async with httpx.AsyncClient() as client:
            # Check pipeline status
            response = await client.get(
                f'http://data-platform-service:8000/api/v1/pipelines/{pipeline_id}/status',
                headers={'X-Tenant-ID': context['dag_run'].conf.get('tenant_id')}
            )
            response.raise_for_status()
            status = response.json()
            
            if status['state'] != 'RUNNING':
                raise Exception(f"Pipeline not running: {status['state']}")
            
            # Check data flow
            metrics = status.get('metrics', {})
            if metrics.get('events_processed', 0) == 0:
                logger.warning("No events processed yet")
            else:
                logger.info(f"Pipeline processed {metrics['events_processed']} events")
            
            return status
    
    result = asyncio.run(_check_health())
    return result


def create_monitoring_alerts(**context):
    """Set up monitoring alerts for the pipeline"""
    conf = context['dag_run'].conf
    pipeline_id = context['task_instance'].xcom_pull(task_ids='create_streaming_pipeline', key='pipeline_id')
    
    alert_rules = [
        {
            'name': 'Pipeline Failure',
            'condition': f'pipeline.{pipeline_id}.status != "RUNNING"',
            'severity': 'critical',
            'action': 'restart_pipeline'
        },
        {
            'name': 'High Latency',
            'condition': f'pipeline.{pipeline_id}.latency_ms > 5000',
            'severity': 'warning',
            'action': 'scale_up'
        },
        {
            'name': 'Low Throughput',
            'condition': f'pipeline.{pipeline_id}.events_per_second < 10',
            'severity': 'info',
            'action': 'investigate'
        }
    ]
    
    # In production, would integrate with monitoring service
    logger.info(f"Would create {len(alert_rules)} monitoring alerts")
    return {'alerts_created': len(alert_rules)}


with DAG(
    dag_id='realtime_analytics_pipeline',
    default_args=default_args,
    description='Set up and manage real-time analytics pipelines',
    schedule_interval=None,
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=['analytics', 'streaming', 'real-time']
) as dag:
    
    # Task 1: Set up event subscription
    subscribe_task = PythonOperator(
        task_id='setup_event_subscription',
        python_callable=setup_event_subscription,
        provide_context=True
    )
    
    # Task 2: Create streaming pipeline
    pipeline_task = PythonOperator(
        task_id='create_streaming_pipeline',
        python_callable=create_streaming_pipeline,
        provide_context=True
    )
    
    # Task 3: Set up analytics dashboard
    dashboard_task = PythonOperator(
        task_id='setup_analytics_dashboard',
        python_callable=setup_analytics_dashboard,
        provide_context=True
    )
    
    # Task 4: Enable anomaly detection
    anomaly_task = PythonOperator(
        task_id='enable_anomaly_detection',
        python_callable=enable_anomaly_detection,
        provide_context=True
    )
    
    # Task 5: Validate pipeline health
    validate_task = PythonOperator(
        task_id='validate_pipeline_health',
        python_callable=validate_pipeline_health,
        provide_context=True,
        retries=5,
        retry_delay=timedelta(minutes=1)
    )
    
    # Task 6: Create monitoring alerts
    alerts_task = PythonOperator(
        task_id='create_monitoring_alerts',
        python_callable=create_monitoring_alerts,
        provide_context=True
    )
    
    # Define dependencies
    subscribe_task >> pipeline_task >> [dashboard_task, anomaly_task, validate_task]
    validate_task >> alerts_task 