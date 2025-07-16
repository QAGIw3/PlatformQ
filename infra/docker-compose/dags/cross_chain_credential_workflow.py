"""
Cross-Chain Credential Workflow DAG

This DAG orchestrates cross-chain credential operations including:
1. Issuing credentials on source chain
2. Verifying credentials via trust network
3. Bridging credentials to target chains
4. Updating reputation scores
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.http.operators.http import SimpleHttpOperator
from airflow.providers.http.sensors.http import HttpSensor
from airflow.models import Variable
import json
import logging

logger = logging.getLogger(__name__)

# Default arguments
default_args = {
    'owner': 'platformq',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
}

# Service URLs (from Airflow Variables)
VC_SERVICE_URL = Variable.get("vc_service_url", "http://verifiable-credential-service:8000")
TRUST_SERVICE_URL = Variable.get("trust_service_url", "http://verifiable-credential-service:8000")


def issue_credential(**context):
    """Issue a credential on the source blockchain"""
    import requests
    
    # Get parameters from DAG run conf
    conf = context['dag_run'].conf
    
    credential_data = {
        "subject": conf.get("subject", {}),
        "type": conf.get("credential_type", "GenericCredential"),
        "blockchain": conf.get("source_chain", "ethereum"),
        "store_on_ipfs": True,
        "encrypt_storage": True,
        "issuer_did": conf.get("issuer_did")
    }
    
    response = requests.post(
        f"{VC_SERVICE_URL}/api/v1/issue",
        json=credential_data,
        headers={"Authorization": f"Bearer {Variable.get('api_token', '')}"}
    )
    response.raise_for_status()
    
    credential = response.json()
    logger.info(f"Issued credential: {credential['id']}")
    
    # Push credential data to XCom for next tasks
    context['ti'].xcom_push(key='credential', value=credential)
    context['ti'].xcom_push(key='credential_id', value=credential['id'])
    return credential['id']


def verify_via_trust_network(**context):
    """Verify credential through the trust network"""
    import requests
    
    credential = context['ti'].xcom_pull(key='credential')
    
    verification_request = {
        "credential_id": credential['id'],
        "credential_data": credential,
        "required_verifiers": 3,
        "consensus_threshold": 0.66
    }
    
    response = requests.post(
        f"{TRUST_SERVICE_URL}/api/v1/verify/network",
        json=verification_request,
        headers={"Authorization": f"Bearer {Variable.get('api_token', '')}"}
    )
    response.raise_for_status()
    
    result = response.json()
    logger.info(f"Verification result: {result}")
    
    if not result.get("consensus_reached") or not result.get("is_valid"):
        raise ValueError("Credential verification failed")
    
    context['ti'].xcom_push(key='verification_result', value=result)
    return result


def bridge_credential(**context):
    """Bridge credential to target chain"""
    import requests
    
    credential = context['ti'].xcom_pull(key='credential')
    conf = context['dag_run'].conf
    target_chain = conf.get("target_chain", "polygon")
    
    bridge_request = {
        "credential_id": credential['id'],
        "credential_data": credential,
        "source_chain": conf.get("source_chain", "ethereum"),
        "target_chain": target_chain
    }
    
    response = requests.post(
        f"{VC_SERVICE_URL}/api/v1/bridge/transfer",
        json=bridge_request,
        headers={"Authorization": f"Bearer {Variable.get('api_token', '')}"}
    )
    response.raise_for_status()
    
    bridge_result = response.json()
    logger.info(f"Bridge request initiated: {bridge_result['request_id']}")
    
    context['ti'].xcom_push(key='bridge_request_id', value=bridge_result['request_id'])
    return bridge_result['request_id']


def check_bridge_status(**context):
    """Check bridge transfer status"""
    import requests
    import time
    
    request_id = context['ti'].xcom_pull(key='bridge_request_id')
    max_attempts = 60  # 5 minutes with 5-second intervals
    
    for attempt in range(max_attempts):
        response = requests.get(
            f"{VC_SERVICE_URL}/api/v1/bridge/status/{request_id}",
            headers={"Authorization": f"Bearer {Variable.get('api_token', '')}"}
        )
        response.raise_for_status()
        
        status = response.json()
        logger.info(f"Bridge status: {status['status']}")
        
        if status['status'] == 'CONFIRMED':
            context['ti'].xcom_push(key='bridge_status', value=status)
            return status
        elif status['status'] == 'FAILED':
            raise ValueError(f"Bridge transfer failed: {status}")
        
        time.sleep(5)
    
    raise TimeoutError("Bridge transfer timed out")


def update_trust_scores(**context):
    """Update trust scores based on successful operations"""
    import requests
    
    verification_result = context['ti'].xcom_pull(key='verification_result')
    bridge_status = context['ti'].xcom_pull(key='bridge_status')
    conf = context['dag_run'].conf
    
    # Update trust relationship for successful bridge
    trust_update = {
        "to_entity": conf.get("issuer_did", ""),
        "trust_value": 0.1,  # Increment trust
        "credential_type": conf.get("credential_type", "GenericCredential"),
        "evidence": [
            context['ti'].xcom_pull(key='credential_id'),
            bridge_status.get('request_id')
        ]
    }
    
    response = requests.post(
        f"{TRUST_SERVICE_URL}/api/v1/trust/relationships",
        json=trust_update,
        headers={"Authorization": f"Bearer {Variable.get('api_token', '')}"}
    )
    response.raise_for_status()
    
    logger.info("Trust scores updated successfully")


def notify_completion(**context):
    """Send notifications about workflow completion"""
    credential_id = context['ti'].xcom_pull(key='credential_id')
    bridge_status = context['ti'].xcom_pull(key='bridge_status')
    conf = context['dag_run'].conf
    
    message = f"""
    Cross-Chain Credential Workflow Completed:
    - Credential ID: {credential_id}
    - Source Chain: {conf.get('source_chain', 'ethereum')}
    - Target Chain: {conf.get('target_chain', 'polygon')}
    - Bridge Status: {bridge_status.get('status')}
    - Completed At: {bridge_status.get('completed_at')}
    """
    
    logger.info(message)
    
    # In production, send to notification service
    # notification_service.send(message)


# Create the DAG
dag = DAG(
    'cross_chain_credential_workflow',
    default_args=default_args,
    description='Orchestrates cross-chain credential operations',
    schedule_interval=None,  # Triggered manually or via API
    catchup=False,
    tags=['credentials', 'blockchain', 'cross-chain'],
)

# Define tasks
issue_task = PythonOperator(
    task_id='issue_credential',
    python_callable=issue_credential,
    dag=dag,
)

verify_task = PythonOperator(
    task_id='verify_credential',
    python_callable=verify_via_trust_network,
    dag=dag,
)

bridge_task = PythonOperator(
    task_id='bridge_credential',
    python_callable=bridge_credential,
    dag=dag,
)

check_bridge_task = PythonOperator(
    task_id='check_bridge_status',
    python_callable=check_bridge_status,
    dag=dag,
)

update_trust_task = PythonOperator(
    task_id='update_trust_scores',
    python_callable=update_trust_scores,
    dag=dag,
)

notify_task = PythonOperator(
    task_id='notify_completion',
    python_callable=notify_completion,
    trigger_rule='all_done',  # Run even if previous tasks fail
    dag=dag,
)

# Set task dependencies
issue_task >> verify_task >> bridge_task >> check_bridge_task >> update_trust_task >> notify_task 