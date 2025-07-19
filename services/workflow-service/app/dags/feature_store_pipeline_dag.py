"""
Feature Store Pipeline DAG

Demonstrates the unified feature store capabilities:
- Feature computation from data lake
- Online/offline feature materialization
- Feature versioning
- Real-time serving with Ignite
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


def create_feature_group(**context):
    """Create a feature group definition"""
    conf = context['dag_run'].conf
    
    feature_group_config = {
        "name": conf.get('feature_group_name', 'user_activity_features'),
        "description": "User activity features for ML models",
        "entity_keys": ["user_id"],
        "features": [
            {
                "name": "total_transactions",
                "type": "integer",
                "description": "Total number of transactions",
                "default_value": 0
            },
            {
                "name": "avg_transaction_amount",
                "type": "float",
                "description": "Average transaction amount",
                "default_value": 0.0
            },
            {
                "name": "days_since_last_activity",
                "type": "integer",
                "description": "Days since last activity",
                "default_value": -1
            },
            {
                "name": "user_risk_score",
                "type": "float",
                "description": "Computed risk score",
                "default_value": 0.5
            }
        ],
        "source_query": """
        SELECT 
            user_id,
            COUNT(*) as total_transactions,
            AVG(amount) as avg_transaction_amount,
            DATEDIFF(CURRENT_DATE, MAX(transaction_date)) as days_since_last_activity,
            CASE 
                WHEN COUNT(*) > 100 THEN 0.8
                WHEN COUNT(*) > 50 THEN 0.5
                ELSE 0.2
            END as user_risk_score
        FROM transactions
        WHERE transaction_date >= '{start_date}'
          AND transaction_date <= '{end_date}'
        GROUP BY user_id
        """,
        "transform_type": "sql"
    }
    
    async def _create():
        async with httpx.AsyncClient() as client:
            response = await client.post(
                'http://data-platform-service:8000/api/v1/features/groups',
                json=feature_group_config
            )
            response.raise_for_status()
            return response.json()
    
    import asyncio
    result = asyncio.run(_create())
    
    context['task_instance'].xcom_push(key='feature_group', value=result)
    logger.info(f"Created feature group: {result}")
    return result


def compute_features(**context):
    """Compute features for the feature group"""
    conf = context['dag_run'].conf
    feature_group_result = context['task_instance'].xcom_pull(
        task_ids='create_feature_group',
        key='feature_group'
    )
    
    feature_group_name = feature_group_result['name']
    
    async def _compute():
        async with httpx.AsyncClient(timeout=300.0) as client:
            response = await client.post(
                'http://data-platform-service:8000/api/v1/features/compute',
                json={
                    'feature_group': feature_group_name,
                    'start_date': conf.get('start_date', (datetime.now() - timedelta(days=30)).isoformat()),
                    'end_date': conf.get('end_date', datetime.now().isoformat()),
                    'incremental': conf.get('incremental', True)
                }
            )
            response.raise_for_status()
            return response.json()
    
    import asyncio
    result = asyncio.run(_compute())
    
    context['task_instance'].xcom_push(key='compute_result', value=result)
    logger.info(f"Computed features: {result}")
    return result


def materialize_features(**context):
    """Materialize features to online and offline stores"""
    conf = context['dag_run'].conf
    feature_group_result = context['task_instance'].xcom_pull(
        task_ids='create_feature_group',
        key='feature_group'
    )
    
    feature_group_name = feature_group_result['name']
    
    async def _materialize():
        async with httpx.AsyncClient(timeout=300.0) as client:
            response = await client.post(
                'http://data-platform-service:8000/api/v1/features/materialize',
                json={
                    'feature_group': feature_group_name,
                    'write_to_online': True,  # Write to Ignite
                    'write_to_offline': True,  # Write to Iceberg
                    'partition_by': ['date']
                }
            )
            response.raise_for_status()
            return response.json()
    
    import asyncio
    result = asyncio.run(_materialize())
    
    context['task_instance'].xcom_push(key='materialize_result', value=result)
    logger.info(f"Materialized features: {result}")
    return result


def test_online_serving(**context):
    """Test online feature serving"""
    conf = context['dag_run'].conf
    feature_group_result = context['task_instance'].xcom_pull(
        task_ids='create_feature_group',
        key='feature_group'
    )
    
    # Test entities
    test_entities = conf.get('test_entities', [
        {"user_id": "user_001"},
        {"user_id": "user_002"},
        {"user_id": "user_003"}
    ])
    
    feature_group_name = feature_group_result['name']
    features = [f"{feature_group_name}.{f['name']}" for f in feature_group_result['features']]
    
    async def _test_serving():
        results = []
        async with httpx.AsyncClient() as client:
            for entity in test_entities:
                response = await client.post(
                    'http://data-platform-service:8000/api/v1/features/serve',
                    json={
                        'entities': entity,
                        'features': features,
                        'include_metadata': True
                    }
                )
                response.raise_for_status()
                result = response.json()
                results.append({
                    'entity': entity,
                    'features': result['features'],
                    'latency_ms': result['latency_ms']
                })
                logger.info(f"Served features for {entity}: latency={result['latency_ms']}ms")
        
        return results
    
    import asyncio
    results = asyncio.run(_test_serving())
    
    # Calculate average latency
    avg_latency = sum(r['latency_ms'] for r in results) / len(results)
    logger.info(f"Average serving latency: {avg_latency}ms")
    
    return {
        'test_results': results,
        'avg_latency_ms': avg_latency,
        'entities_tested': len(results)
    }


def monitor_feature_quality(**context):
    """Monitor feature quality and statistics"""
    feature_group_result = context['task_instance'].xcom_pull(
        task_ids='create_feature_group',
        key='feature_group'
    )
    
    feature_group_name = feature_group_result['name']
    
    async def _monitor():
        async with httpx.AsyncClient() as client:
            # Get feature statistics
            stats_response = await client.get(
                f'http://data-platform-service:8000/api/v1/features/groups/{feature_group_name}/statistics'
            )
            stats_response.raise_for_status()
            
            # Get cache statistics
            cache_response = await client.get(
                'http://data-platform-service:8000/api/v1/features/cache/statistics'
            )
            cache_response.raise_for_status()
            
            return {
                'feature_statistics': stats_response.json(),
                'cache_statistics': cache_response.json()
            }
    
    import asyncio
    result = asyncio.run(_monitor())
    
    logger.info(f"Feature quality monitoring: {result}")
    return result


# Create DAG
dag = DAG(
    'feature_store_pipeline',
    default_args=default_args,
    description='Unified feature store pipeline with online/offline serving',
    schedule_interval='@daily',
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=['feature-store', 'ml', 'ignite', 'online-serving']
)

# Define tasks
create_group_task = PythonOperator(
    task_id='create_feature_group',
    python_callable=create_feature_group,
    dag=dag
)

compute_task = PythonOperator(
    task_id='compute_features',
    python_callable=compute_features,
    dag=dag
)

materialize_task = PythonOperator(
    task_id='materialize_features',
    python_callable=materialize_features,
    dag=dag
)

test_serving_task = PythonOperator(
    task_id='test_online_serving',
    python_callable=test_online_serving,
    dag=dag
)

monitor_task = PythonOperator(
    task_id='monitor_feature_quality',
    python_callable=monitor_feature_quality,
    dag=dag
)

# Set dependencies
create_group_task >> compute_task >> materialize_task >> [test_serving_task, monitor_task] 