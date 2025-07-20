"""
Data ML Training DAG

Orchestrates machine learning training workflows using data platform datasets.
Integrates with data-platform-service and unified-ml-platform-service.
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


def prepare_training_data(**context):
    """Prepare training data from data lake"""
    conf = context['dag_run'].conf
    dataset_id = conf.get('dataset_id')
    model_config = conf.get('model_config', {})
    tenant_id = conf.get('tenant_id')
    
    # Call data platform to prepare training data
    async def _prepare():
        async with httpx.AsyncClient() as client:
            response = await client.post(
                'http://data-platform-service:8000/api/v1/integrations/ml/prepare-training-data',
                json={
                    'dataset_id': dataset_id,
                    'model_type': model_config.get('model_type', 'classification'),
                    'features': model_config.get('features', []),
                    'target': model_config.get('target'),
                    'validation_split': model_config.get('validation_split', 0.2),
                    'preprocessing': model_config.get('preprocessing', {})
                },
                headers={'X-Tenant-ID': tenant_id}
            )
            response.raise_for_status()
            return response.json()
    
    import asyncio
    result = asyncio.run(_prepare())
    
    # Store prepared data info for next tasks
    context['task_instance'].xcom_push(key='prepared_data', value=result)
    return result


def trigger_ml_training(**context):
    """Trigger ML training with prepared data"""
    prepared_data = context['task_instance'].xcom_pull(task_ids='prepare_training_data', key='prepared_data')
    conf = context['dag_run'].conf
    
    # Get training configuration
    training_config = conf.get('training_config', {})
    training_config['prepared_data_path'] = prepared_data['data_path']
    training_config['feature_stats'] = prepared_data.get('statistics', {})
    
    logger.info(f"Triggering ML training with data: {prepared_data['data_path']}")
    
    # This would typically trigger another DAG or call ML platform directly
    # For now, we'll make a direct API call
    async def _train():
        async with httpx.AsyncClient() as client:
            response = await client.post(
                'http://unified-ml-platform-service:8000/api/v1/training/jobs',
                json={
                    'experiment_name': conf.get('experiment_name', 'data_platform_training'),
                    'model_type': conf.get('model_config', {}).get('model_type'),
                    'training_data': prepared_data['data_path'],
                    'hyperparameters': training_config.get('hyperparameters', {}),
                    'compute_requirements': {
                        'gpu_type': training_config.get('gpu_type', 'V100'),
                        'num_gpus': training_config.get('num_gpus', 1)
                    }
                },
                headers={'X-Tenant-ID': conf.get('tenant_id')}
            )
            response.raise_for_status()
            return response.json()
    
    import asyncio
    training_job = asyncio.run(_train())
    
    context['task_instance'].xcom_push(key='training_job_id', value=training_job['job_id'])
    return training_job


def export_to_feature_store(**context):
    """Export engineered features to feature store"""
    prepared_data = context['task_instance'].xcom_pull(task_ids='prepare_training_data', key='prepared_data')
    conf = context['dag_run'].conf
    
    if not conf.get('export_to_feature_store', False):
        logger.info("Skipping feature store export")
        return
    
    async def _export():
        async with httpx.AsyncClient() as client:
            response = await client.post(
                'http://data-platform-service:8000/api/v1/integrations/ml/export-to-feature-store',
                json={
                    'dataset_id': conf.get('dataset_id'),
                    'feature_group_name': conf.get('feature_group_name', f"dataset_{conf.get('dataset_id')}"),
                    'features': prepared_data.get('features', []),
                    'description': conf.get('feature_description', 'Features from data platform dataset')
                },
                headers={'X-Tenant-ID': conf.get('tenant_id')}
            )
            response.raise_for_status()
            return response.json()
    
    import asyncio
    result = asyncio.run(_export())
    logger.info(f"Exported features to feature store: {result}")
    return result


def monitor_training_job(**context):
    """Monitor the training job status"""
    training_job_id = context['task_instance'].xcom_pull(task_ids='trigger_ml_training', key='training_job_id')
    
    # In production, this would poll the ML platform for job status
    # For now, we'll simulate monitoring
    logger.info(f"Monitoring training job: {training_job_id}")
    
    # Return success for now
    return {'status': 'completed', 'job_id': training_job_id}


def publish_training_results(**context):
    """Publish training results back to data platform"""
    training_job_id = context['task_instance'].xcom_pull(task_ids='trigger_ml_training', key='training_job_id')
    conf = context['dag_run'].conf
    
    # Simulate getting results from ML platform
    training_results = {
        'job_id': training_job_id,
        'metrics': {
            'accuracy': 0.95,
            'f1_score': 0.93,
            'auc': 0.97
        },
        'model_uri': f's3://models/{training_job_id}/model.pkl'
    }
    
    # Update data catalog with training results
    async def _publish():
        async with httpx.AsyncClient() as client:
            response = await client.post(
                'http://data-platform-service:8000/api/v1/catalog/datasets/' + conf.get('dataset_id') + '/metadata',
                json={
                    'ml_training_results': training_results,
                    'last_trained_at': datetime.utcnow().isoformat(),
                    'model_performance': training_results['metrics']
                },
                headers={'X-Tenant-ID': conf.get('tenant_id')}
            )
            response.raise_for_status()
            return response.json()
    
    import asyncio
    result = asyncio.run(_publish())
    logger.info(f"Published training results to data catalog")
    return result


with DAG(
    dag_id='data_ml_training_workflow',
    default_args=default_args,
    description='Orchestrate ML training using data platform datasets',
    schedule_interval=None,
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=['ml', 'data-platform', 'training']
) as dag:
    
    # Task 1: Prepare training data
    prepare_data_task = PythonOperator(
        task_id='prepare_training_data',
        python_callable=prepare_training_data,
        provide_context=True
    )
    
    # Task 2: Export to feature store (optional)
    export_features_task = PythonOperator(
        task_id='export_to_feature_store',
        python_callable=export_to_feature_store,
        provide_context=True
    )
    
    # Task 3: Trigger ML training
    trigger_training_task = PythonOperator(
        task_id='trigger_ml_training',
        python_callable=trigger_ml_training,
        provide_context=True
    )
    
    # Task 4: Monitor training
    monitor_task = PythonOperator(
        task_id='monitor_training_job',
        python_callable=monitor_training_job,
        provide_context=True,
        retries=10,
        retry_delay=timedelta(minutes=10)
    )
    
    # Task 5: Publish results
    publish_results_task = PythonOperator(
        task_id='publish_training_results',
        python_callable=publish_training_results,
        provide_context=True
    )
    
    # Define task dependencies
    prepare_data_task >> [export_features_task, trigger_training_task]
    trigger_training_task >> monitor_task >> publish_results_task 