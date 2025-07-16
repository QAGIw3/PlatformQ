"""
Trust Score Calculation DAG

Periodically calculates trust scores for all active users and requests
issuance of TrustScoreCredentials.
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.http.operators.http import SimpleHttpOperator
import logging
import httpx
import asyncio
from typing import List, Dict, Any

logger = logging.getLogger(__name__)

# Service URLs
AUTH_SERVICE_URL = "http://auth-service:8000"
GRAPH_INTELLIGENCE_URL = "http://graph-intelligence-service:8000"

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
    'trust_score_calculation',
    default_args=default_args,
    description='Calculate and issue trust score VCs for all users',
    schedule_interval=timedelta(hours=6),  # Run every 6 hours
    catchup=False,
    tags=['trust', 'verifiable-credentials'],
)


def get_active_users(**context):
    """Get list of active users from auth service"""
    try:
        # In production, this would paginate through all users
        response = httpx.get(
            f"{AUTH_SERVICE_URL}/api/v1/users",
            params={"active": True, "limit": 1000}
        )
        response.raise_for_status()
        
        users = response.json().get("users", [])
        logger.info(f"Found {len(users)} active users")
        
        # Store user IDs for next task
        context['task_instance'].xcom_push(key='user_ids', value=[u['id'] for u in users])
        
        return len(users)
        
    except Exception as e:
        logger.error(f"Failed to get active users: {e}")
        raise


def calculate_trust_scores(**context):
    """Calculate trust scores for all users"""
    user_ids = context['task_instance'].xcom_pull(key='user_ids')
    
    if not user_ids:
        logger.warning("No users to process")
        return
    
    successful_calculations = 0
    failed_calculations = 0
    
    for user_id in user_ids:
        try:
            # Call graph intelligence service to calculate verifiable trust score
            response = httpx.post(
                f"{GRAPH_INTELLIGENCE_URL}/api/v1/trust-score/{user_id}/calculate-verifiable",
                timeout=30.0
            )
            
            if response.status_code == 200:
                result = response.json()
                logger.info(f"Successfully calculated trust score for user {user_id}: {result['trust_score']}")
                successful_calculations += 1
            else:
                logger.error(f"Failed to calculate trust score for user {user_id}: {response.status_code}")
                failed_calculations += 1
                
        except Exception as e:
            logger.error(f"Error calculating trust score for user {user_id}: {e}")
            failed_calculations += 1
    
    logger.info(f"Trust score calculation complete: {successful_calculations} successful, {failed_calculations} failed")
    
    # Store results for monitoring
    context['task_instance'].xcom_push(key='calculation_results', value={
        'successful': successful_calculations,
        'failed': failed_calculations,
        'total': len(user_ids)
    })


def check_results(**context):
    """Check calculation results and alert if too many failures"""
    results = context['task_instance'].xcom_pull(key='calculation_results')
    
    if not results:
        return
    
    failure_rate = results['failed'] / results['total'] if results['total'] > 0 else 0
    
    if failure_rate > 0.1:  # More than 10% failure rate
        raise Exception(f"High failure rate in trust score calculation: {failure_rate:.2%}")
    
    logger.info(f"Trust score calculation completed with {failure_rate:.2%} failure rate")


# Task definitions
get_users_task = PythonOperator(
    task_id='get_active_users',
    python_callable=get_active_users,
    dag=dag,
)

calculate_scores_task = PythonOperator(
    task_id='calculate_trust_scores',
    python_callable=calculate_trust_scores,
    dag=dag,
)

check_results_task = PythonOperator(
    task_id='check_results',
    python_callable=check_results,
    dag=dag,
)

# Task dependencies
get_users_task >> calculate_scores_task >> check_results_task 