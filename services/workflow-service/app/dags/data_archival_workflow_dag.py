"""
Data Archival Workflow DAG

Orchestrates data archival, backup, and lifecycle management workflows.
Integrates with data-platform-service and storage-service.
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.dummy import DummyOperator
from airflow.utils.task_group import TaskGroup
import logging
import httpx
import json
import asyncio
from typing import List, Dict, Any

logger = logging.getLogger(__name__)

default_args = {
    'owner': 'data-platform',
    'depends_on_past': False,
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5)
}


def identify_datasets_for_archival(**context):
    """Identify datasets that need to be archived based on policies"""
    conf = context['dag_run'].conf
    tenant_id = conf.get('tenant_id')
    
    # Archive policies
    policies = conf.get('archive_policies', {
        'age_days': 90,  # Archive data older than 90 days
        'access_count_threshold': 5,  # Archive if accessed less than 5 times
        'size_threshold_gb': 100,  # Archive datasets larger than 100GB
        'exclude_tags': ['critical', 'active', 'ml-training']
    })
    
    async def _identify():
        async with httpx.AsyncClient() as client:
            # Query data catalog for datasets matching criteria
            response = await client.get(
                'http://data-platform-service:8000/api/v1/catalog/datasets',
                params={
                    'last_accessed_before': (datetime.utcnow() - timedelta(days=policies['age_days'])).isoformat(),
                    'max_access_count': policies['access_count_threshold'],
                    'min_size_gb': policies['size_threshold_gb']
                },
                headers={'X-Tenant-ID': tenant_id}
            )
            response.raise_for_status()
            datasets = response.json()['datasets']
            
            # Filter out excluded tags
            eligible_datasets = []
            for dataset in datasets:
                tags = dataset.get('tags', [])
                if not any(tag in policies['exclude_tags'] for tag in tags):
                    eligible_datasets.append(dataset)
            
            logger.info(f"Found {len(eligible_datasets)} datasets eligible for archival")
            return eligible_datasets
    
    datasets = asyncio.run(_identify())
    context['task_instance'].xcom_push(key='datasets_to_archive', value=datasets)
    return datasets


def create_backup_pipeline(**context):
    """Create backup pipeline for datasets before archival"""
    datasets = context['task_instance'].xcom_pull(task_ids='identify_datasets_for_archival', key='datasets_to_archive')
    conf = context['dag_run'].conf
    
    if not datasets:
        logger.info("No datasets to backup")
        return []
    
    backup_configs = []
    
    async def _create_backups():
        async with httpx.AsyncClient(timeout=60.0) as client:
            for dataset in datasets:
                try:
                    response = await client.post(
                        'http://data-platform-service:8000/api/v1/integrations/storage/backup',
                        json={
                            'dataset_id': dataset['id'],
                            'backup_type': conf.get('backup_type', 'incremental'),
                            'compression': conf.get('compression', 'snappy'),
                            'encryption': conf.get('encryption', {'enabled': True, 'algorithm': 'AES-256'}),
                            'retention_days': conf.get('backup_retention_days', 365)
                        },
                        headers={'X-Tenant-ID': conf.get('tenant_id')}
                    )
                    response.raise_for_status()
                    backup_config = response.json()
                    backup_configs.append(backup_config)
                    logger.info(f"Created backup pipeline for dataset {dataset['id']}")
                except Exception as e:
                    logger.error(f"Failed to create backup for dataset {dataset['id']}: {e}")
                    # Continue with other datasets
    
    asyncio.run(_create_backups())
    context['task_instance'].xcom_push(key='backup_configs', value=backup_configs)
    return backup_configs


def archive_datasets(**context):
    """Archive datasets to cold storage"""
    datasets = context['task_instance'].xcom_pull(task_ids='identify_datasets_for_archival', key='datasets_to_archive')
    backup_configs = context['task_instance'].xcom_pull(task_ids='create_backup_pipeline', key='backup_configs')
    conf = context['dag_run'].conf
    
    archived_datasets = []
    
    async def _archive():
        async with httpx.AsyncClient(timeout=300.0) as client:  # Long timeout for large datasets
            for dataset in datasets:
                # Find corresponding backup
                backup = next((b for b in backup_configs if b['dataset_id'] == dataset['id']), None)
                if not backup or backup.get('status') != 'completed':
                    logger.warning(f"Skipping archival for dataset {dataset['id']} - backup not completed")
                    continue
                
                try:
                    response = await client.post(
                        'http://data-platform-service:8000/api/v1/integrations/storage/archive',
                        json={
                            'dataset_id': dataset['id'],
                            'archive_tier': conf.get('archive_tier', 'glacier'),
                            'delete_original': conf.get('delete_original', False),
                            'metadata': {
                                'archived_by': conf.get('triggered_by', 'system'),
                                'archive_reason': 'policy',
                                'original_location': dataset.get('location'),
                                'backup_id': backup['backup_id']
                            }
                        },
                        headers={'X-Tenant-ID': conf.get('tenant_id')}
                    )
                    response.raise_for_status()
                    archive_result = response.json()
                    archived_datasets.append(archive_result)
                    logger.info(f"Archived dataset {dataset['id']} to {archive_result['archive_location']}")
                except Exception as e:
                    logger.error(f"Failed to archive dataset {dataset['id']}: {e}")
    
    asyncio.run(_archive())
    context['task_instance'].xcom_push(key='archived_datasets', value=archived_datasets)
    return archived_datasets


def setup_disaster_recovery(**context):
    """Set up disaster recovery for critical datasets"""
    conf = context['dag_run'].conf
    
    if not conf.get('enable_disaster_recovery', True):
        logger.info("Disaster recovery not enabled")
        return
    
    dr_config = {
        'recovery_point_objective': conf.get('rpo_hours', 24),
        'recovery_time_objective': conf.get('rto_hours', 4),
        'replication_regions': conf.get('dr_regions', ['us-west-2', 'eu-west-1']),
        'test_frequency_days': conf.get('dr_test_frequency', 90)
    }
    
    async def _setup_dr():
        async with httpx.AsyncClient() as client:
            response = await client.post(
                'http://data-platform-service:8000/api/v1/integrations/storage/disaster-recovery',
                json=dr_config,
                headers={'X-Tenant-ID': conf.get('tenant_id')}
            )
            response.raise_for_status()
            return response.json()
    
    result = asyncio.run(_setup_dr())
    logger.info(f"Disaster recovery configured: {result}")
    return result


def migrate_to_cold_storage(**context):
    """Migrate old data to cold storage tiers"""
    conf = context['dag_run'].conf
    
    migration_config = {
        'source_tier': conf.get('source_tier', 'standard'),
        'target_tier': conf.get('target_tier', 'glacier'),
        'age_threshold_days': conf.get('cold_storage_age_days', 180),
        'size_threshold_gb': conf.get('cold_storage_size_gb', 1000)
    }
    
    async def _migrate():
        async with httpx.AsyncClient(timeout=600.0) as client:
            response = await client.post(
                'http://data-platform-service:8000/api/v1/integrations/storage/cold-migration',
                json=migration_config,
                headers={'X-Tenant-ID': conf.get('tenant_id')}
            )
            response.raise_for_status()
            return response.json()
    
    result = asyncio.run(_migrate())
    logger.info(f"Cold storage migration completed: {result}")
    return result


def generate_archival_report(**context):
    """Generate comprehensive archival report"""
    archived_datasets = context['task_instance'].xcom_pull(task_ids='archive_datasets', key='archived_datasets')
    conf = context['dag_run'].conf
    
    report_config = {
        'report_type': 'archival_summary',
        'include_sections': [
            'executive_summary',
            'archived_datasets',
            'storage_savings',
            'compliance_status',
            'recovery_procedures'
        ],
        'format': conf.get('report_format', 'pdf'),
        'recipients': conf.get('report_recipients', [])
    }
    
    async def _generate_report():
        async with httpx.AsyncClient() as client:
            response = await client.post(
                'http://data-platform-service:8000/api/v1/integrations/reports/generate',
                json={
                    **report_config,
                    'data': {
                        'archived_datasets': archived_datasets,
                        'archival_date': datetime.utcnow().isoformat(),
                        'total_archived_gb': sum(d.get('size_gb', 0) for d in archived_datasets),
                        'estimated_savings': sum(d.get('monthly_savings_usd', 0) for d in archived_datasets)
                    }
                },
                headers={'X-Tenant-ID': conf.get('tenant_id')}
            )
            response.raise_for_status()
            return response.json()
    
    report = asyncio.run(_generate_report())
    logger.info(f"Generated archival report: {report['report_url']}")
    return report


def cleanup_and_notify(**context):
    """Clean up temporary resources and send notifications"""
    archived_datasets = context['task_instance'].xcom_pull(task_ids='archive_datasets', key='archived_datasets')
    report = context['task_instance'].xcom_pull(task_ids='generate_archival_report', key='return_value')
    conf = context['dag_run'].conf
    
    # Prepare notification
    notification = {
        'type': 'archival_completed',
        'summary': f"Archived {len(archived_datasets)} datasets",
        'details': {
            'total_size_gb': sum(d.get('size_gb', 0) for d in archived_datasets),
            'storage_savings_monthly_usd': sum(d.get('monthly_savings_usd', 0) for d in archived_datasets),
            'report_url': report.get('report_url')
        },
        'recipients': conf.get('notification_recipients', [])
    }
    
    # In production, would send actual notifications
    logger.info(f"Archival workflow completed: {notification}")
    return notification


with DAG(
    dag_id='data_archival_workflow',
    default_args=default_args,
    description='Orchestrate data archival and lifecycle management',
    schedule_interval='@monthly',  # Run monthly by default
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=['data-lifecycle', 'archival', 'backup']
) as dag:
    
    # Task 1: Identify datasets for archival
    identify_task = PythonOperator(
        task_id='identify_datasets_for_archival',
        python_callable=identify_datasets_for_archival,
        provide_context=True
    )
    
    # Task 2: Create backup pipelines
    backup_task = PythonOperator(
        task_id='create_backup_pipeline',
        python_callable=create_backup_pipeline,
        provide_context=True
    )
    
    # Task 3: Archive datasets
    archive_task = PythonOperator(
        task_id='archive_datasets',
        python_callable=archive_datasets,
        provide_context=True
    )
    
    # Task Group for optional operations
    with TaskGroup(group_id='optional_operations') as optional_ops:
        # Disaster recovery setup
        dr_task = PythonOperator(
            task_id='setup_disaster_recovery',
            python_callable=setup_disaster_recovery,
            provide_context=True
        )
        
        # Cold storage migration
        cold_storage_task = PythonOperator(
            task_id='migrate_to_cold_storage',
            python_callable=migrate_to_cold_storage,
            provide_context=True
        )
    
    # Task 4: Generate report
    report_task = PythonOperator(
        task_id='generate_archival_report',
        python_callable=generate_archival_report,
        provide_context=True
    )
    
    # Task 5: Cleanup and notify
    cleanup_task = PythonOperator(
        task_id='cleanup_and_notify',
        python_callable=cleanup_and_notify,
        provide_context=True
    )
    
    # Define dependencies
    identify_task >> backup_task >> archive_task
    archive_task >> optional_ops
    [archive_task, optional_ops] >> report_task >> cleanup_task 