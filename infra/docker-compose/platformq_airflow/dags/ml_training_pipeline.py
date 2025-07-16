"""
ML Training Pipeline DAG using Apache Spark MLlib

This DAG demonstrates automated machine learning model training
and deployment using Spark's distributed ML capabilities.
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.dummy import DummyOperator
from airflow.providers.http.sensors.http import HttpSensor
from airflow.models import Variable

# Import custom operators
from platformq_operators.pulsar_operator import PulsarEventOperator
from platformq_operators.service_operator import PlatformQServiceOperator
from platformq_operators.spark_platformq_operator import SparkMLOperator

default_args = {
    'owner': 'platformq-ml',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'ml_training_pipeline',
    default_args=default_args,
    description='Automated ML model training pipeline',
    schedule_interval='@weekly',  # Train models weekly
    catchup=False,
    tags=['ml', 'spark', 'training'],
)


def check_data_quality(**context):
    """Check if we have enough data for training"""
    # This would typically query the data lake
    # For demo, we'll simulate the check
    
    data_stats = {
        'total_samples': 10000,
        'unique_classes': 8,
        'data_quality_score': 0.92
    }
    
    # Store stats for later use
    context['ti'].xcom_push(key='data_stats', value=data_stats)
    
    # Decide if we should proceed with training
    if data_stats['total_samples'] >= 5000 and data_stats['data_quality_score'] >= 0.8:
        return 'prepare_training_data'
    else:
        return 'skip_training'


def prepare_training_data(**context):
    """Prepare training data from the data lake"""
    tenant_id = Variable.get("default_tenant_id", "tenant_001")
    
    # Define training data configuration
    training_config = {
        'source_table': f's3://datalake/assets/{tenant_id}/features/',
        'date_range': {
            'start': (datetime.now() - timedelta(days=90)).strftime('%Y-%m-%d'),
            'end': datetime.now().strftime('%Y-%m-%d')
        },
        'filters': {
            'has_label': True,
            'quality_score': {'gte': 0.7}
        },
        'sample_size': 50000
    }
    
    context['ti'].xcom_push(key='training_config', value=training_config)
    context['ti'].xcom_push(key='tenant_id', value=tenant_id)
    
    return training_config


def evaluate_model_performance(**context):
    """Evaluate if the new model is better than the current one"""
    # Get model metrics from XCom
    model_info = context['ti'].xcom_pull(task_ids='train_asset_classifier', key='model_info')
    
    # Get current model metrics (from model registry)
    current_model_f1 = Variable.get("current_asset_classifier_f1", 0.0)
    
    new_model_f1 = model_info['result']['metrics']['f1']
    
    # Check if new model is better
    improvement = new_model_f1 - float(current_model_f1)
    
    evaluation_result = {
        'current_f1': float(current_model_f1),
        'new_f1': new_model_f1,
        'improvement': improvement,
        'should_deploy': improvement > 0.01  # Deploy if >1% improvement
    }
    
    context['ti'].xcom_push(key='evaluation_result', value=evaluation_result)
    
    if evaluation_result['should_deploy']:
        return 'deploy_model'
    else:
        return 'archive_model'


def deploy_model(**context):
    """Deploy the new model to production"""
    model_info = context['ti'].xcom_pull(task_ids='train_asset_classifier', key='model_info')
    evaluation = context['ti'].xcom_pull(task_ids='evaluate_model', key='evaluation_result')
    
    deployment_config = {
        'model_name': model_info['model_name'],
        'model_version': model_info['result'].get('version', 'latest'),
        'deployment_stage': 'production',
        'metrics': evaluation,
        'deployed_at': datetime.utcnow().isoformat()
    }
    
    # Update variable with new model metrics
    Variable.set("current_asset_classifier_f1", str(evaluation['new_f1']))
    
    return deployment_config


# Start of DAG
start_task = DummyOperator(
    task_id='start',
    dag=dag,
)

# Check data quality
check_data = BranchPythonOperator(
    task_id='check_data_quality',
    python_callable=check_data_quality,
    provide_context=True,
    dag=dag,
)

# Prepare training data
prepare_data = PythonOperator(
    task_id='prepare_training_data',
    python_callable=prepare_training_data,
    provide_context=True,
    dag=dag,
)

# Train asset classifier model
train_classifier = SparkMLOperator(
    task_id='train_asset_classifier',
    model_type='asset_classifier',
    tenant_id="{{ ti.xcom_pull(task_ids='prepare_training_data', key='tenant_id') }}",
    training_data="{{ ti.xcom_pull(task_ids='prepare_training_data', key='training_config')['source_table'] }}",
    parameters={
        'validation_split': 0.2,
        'hyperparameter_tuning': True,
        'max_iterations': 100
    },
    dag=dag,
)

# Train anomaly detection model
train_anomaly_detector = SparkMLOperator(
    task_id='train_anomaly_detector',
    model_type='anomaly_detector',
    tenant_id="{{ ti.xcom_pull(task_ids='prepare_training_data', key='tenant_id') }}",
    training_data="{{ ti.xcom_pull(task_ids='prepare_training_data', key='training_config')['source_table'] }}",
    parameters={
        'contamination': 0.05,
        'algorithm': 'isolation_forest'
    },
    dag=dag,
)

# Train recommendation model
train_recommender = SparkMLOperator(
    task_id='train_recommendation_engine',
    model_type='recommendation',
    tenant_id="{{ ti.xcom_pull(task_ids='prepare_training_data', key='tenant_id') }}",
    training_data="s3://datalake/interactions/{{ ti.xcom_pull(task_ids='prepare_training_data', key='tenant_id') }}/",
    parameters={
        'algorithm': 'als',
        'rank': 10,
        'max_iter': 10,
        'reg_param': 0.01
    },
    dag=dag,
)

# Evaluate model performance
evaluate_model = BranchPythonOperator(
    task_id='evaluate_model',
    python_callable=evaluate_model_performance,
    provide_context=True,
    trigger_rule='none_failed_or_skipped',
    dag=dag,
)

# Deploy model
deploy = PythonOperator(
    task_id='deploy_model',
    python_callable=deploy_model,
    provide_context=True,
    dag=dag,
)

# Archive model (if not better)
archive = DummyOperator(
    task_id='archive_model',
    dag=dag,
)

# Skip training (insufficient data)
skip = DummyOperator(
    task_id='skip_training',
    dag=dag,
)

# Model serving update
update_serving = PlatformQServiceOperator(
    task_id='update_model_serving',
    service='functions-service',
    endpoint='/api/v1/functions/ml-models/reload',
    method='POST',
    data={
        'models': ['asset_classifier', 'anomaly_detector', 'recommendation_engine']
    },
    trigger_rule='none_failed_or_skipped',
    dag=dag,
)

# Publish training completion event
publish_completion = PulsarEventOperator(
    task_id='publish_training_complete',
    topic='ml-training-completed',
    event_type='ml.training.completed',
    event_data={
        'models_trained': ['asset_classifier', 'anomaly_detector', 'recommendation_engine'],
        'training_date': '{{ ds }}',
        'evaluation_result': '{{ ti.xcom_pull(task_ids="evaluate_model", key="evaluation_result") }}'
    },
    trigger_rule='none_failed_or_skipped',
    dag=dag,
)

# Feature engineering job (runs in parallel)
feature_engineering = SparkMLOperator(
    task_id='feature_engineering',
    model_type='feature_extraction',
    tenant_id="{{ ti.xcom_pull(task_ids='prepare_training_data', key='tenant_id') }}",
    training_data="s3://datalake/raw/{{ ti.xcom_pull(task_ids='prepare_training_data', key='tenant_id') }}/",
    parameters={
        'feature_types': ['text', 'image', 'metadata'],
        'output_path': 's3://datalake/features/'
    },
    dag=dag,
)

# End task
end_task = DummyOperator(
    task_id='end',
    trigger_rule='none_failed_or_skipped',
    dag=dag,
)

# Define task dependencies
start_task >> check_data
check_data >> [prepare_data, skip]
prepare_data >> [train_classifier, train_anomaly_detector, train_recommender, feature_engineering]

# Model evaluation flow
train_classifier >> evaluate_model
evaluate_model >> [deploy, archive]
deploy >> update_serving
archive >> end_task

# Other models go directly to serving update
[train_anomaly_detector, train_recommender] >> update_serving

# Feature engineering is independent
feature_engineering >> end_task

# Final notifications
update_serving >> publish_completion >> end_task
skip >> end_task 