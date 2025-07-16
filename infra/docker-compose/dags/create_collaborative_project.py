"""
Collaborative Project Creation DAG

This DAG orchestrates the creation of a collaborative project across
multiple integrated systems (Nextcloud, OpenProject, Zulip) in response
to a project creation request.
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.trigger_rule import TriggerRule
from airflow.models import Variable

# Import PlatformQ custom operators
import sys
sys.path.append('/opt/airflow/platformq_airflow')
from platformq_airflow.operators import (
    PlatformQServiceOperator,
    PulsarEventOperator
)

# Define event schemas
class ProjectCreatedEvent:
    def __init__(self, project_id: str, project_name: str, 
                 creator_id: str, event_timestamp: int):
        self.project_id = project_id
        self.project_name = project_name
        self.creator_id = creator_id
        self.event_timestamp = event_timestamp

class ProjectProvisioningCompleted:
    def __init__(self, project_id: str, project_name: str, 
                 nextcloud_folder: str, openproject_id: str,
                 zulip_stream: str, status: str):
        self.project_id = project_id
        self.project_name = project_name
        self.nextcloud_folder = nextcloud_folder
        self.openproject_id = openproject_id
        self.zulip_stream = zulip_stream
        self.status = status

# Default DAG arguments
default_args = {
    'owner': 'platformq',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=2),
}

# Create the DAG
dag = DAG(
    'create_collaborative_project',
    default_args=default_args,
    description='Create a collaborative project across multiple systems',
    schedule_interval=None,  # Triggered via API
    catchup=False,
    tags=['platformq', 'project-management', 'multi-system'],
)

# Task 1: Validate project input
def validate_project_input(**context):
    """Validate the project creation request"""
    # Get DAG run configuration
    dag_run = context['dag_run']
    conf = dag_run.conf or {}
    
    # Validate required fields
    project_name = conf.get('project_name')
    creator_id = conf.get('creator_id')
    tenant_id = conf.get('tenant_id', Variable.get('default_tenant_id', '00000000-0000-0000-0000-000000000000'))
    
    if not project_name:
        raise ValueError("project_name is required")
    
    if not creator_id:
        raise ValueError("creator_id is required")
    
    # Generate project ID if not provided
    import uuid
    project_id = conf.get('project_id', str(uuid.uuid4()))
    
    # Store validated data in XCom
    validated_data = {
        'project_id': project_id,
        'project_name': project_name,
        'creator_id': creator_id,
        'tenant_id': tenant_id,
        'description': conf.get('description', ''),
        'team_members': conf.get('team_members', [])
    }
    
    return validated_data

validate_input = PythonOperator(
    task_id='validate_project_input',
    python_callable=validate_project_input,
    dag=dag,
)

# Task 2: Create Nextcloud folder structure
create_nextcloud_folder = PlatformQServiceOperator(
    task_id='create_nextcloud_folder',
    service_name='provisioning-service',
    endpoint='/api/v1/provision/nextcloud/folder',
    method='POST',
    payload={
        'folder_path': "Projects/{{ ti.xcom_pull(task_ids='validate_project_input')['project_name'] }}",
        'permissions': {
            'share': True,
            'edit': True
        }
    },
    dag=dag,
)

# Task 3: Create OpenProject project
create_openproject = PlatformQServiceOperator(
    task_id='create_openproject_project',
    service_name='provisioning-service',
    endpoint='/api/v1/provision/openproject/project',
    method='POST',
    payload={
        'name': "{{ ti.xcom_pull(task_ids='validate_project_input')['project_name'] }}",
        'identifier': "{{ ti.xcom_pull(task_ids='validate_project_input')['project_name'].lower().replace(' ', '-') }}",
        'description': "{{ ti.xcom_pull(task_ids='validate_project_input')['description'] }}",
        'is_public': False
    },
    dag=dag,
)

# Task 4: Create Zulip stream
create_zulip_stream = PlatformQServiceOperator(
    task_id='create_zulip_stream',
    service_name='provisioning-service',
    endpoint='/api/v1/provision/zulip/stream',
    method='POST',
    payload={
        'stream_name': "project-{{ ti.xcom_pull(task_ids='validate_project_input')['project_name'].lower().replace(' ', '-') }}",
        'description': "{{ ti.xcom_pull(task_ids='validate_project_input')['description'] }}",
        'invite_users': "{{ ti.xcom_pull(task_ids='validate_project_input')['team_members'] }}"
    },
    dag=dag,
)

# Task 5: Save project metadata
def save_project_metadata(**context):
    """Aggregate results and save project metadata"""
    project_data = context['ti'].xcom_pull(task_ids='validate_project_input')
    
    # Get results from each system
    nextcloud_result = context['ti'].xcom_pull(task_ids='create_nextcloud_folder', key='response')
    openproject_result = context['ti'].xcom_pull(task_ids='create_openproject_project', key='response')
    zulip_result = context['ti'].xcom_pull(task_ids='create_zulip_stream', key='response')
    
    # Prepare metadata for storage
    project_metadata = {
        'project_id': project_data['project_id'],
        'project_name': project_data['project_name'],
        'nextcloud_folder_path': nextcloud_result.get('folder_path'),
        'openproject_id': openproject_result.get('id'),
        'zulip_stream_name': zulip_result.get('stream_name'),
        'created_by_user_id': project_data['creator_id'],
        'status': 'active'
    }
    
    return project_metadata

save_metadata = PythonOperator(
    task_id='save_project_metadata',
    python_callable=save_project_metadata,
    dag=dag,
)

# Task 6: Store in projects service
store_project = PlatformQServiceOperator(
    task_id='store_project_record',
    service_name='projects-service',
    endpoint='/api/v1/projects',
    method='POST',
    payload="{{ ti.xcom_pull(task_ids='save_project_metadata') }}",
    dag=dag,
)

# Task 7: Publish success event
publish_success = PulsarEventOperator(
    task_id='publish_project_created',
    topic_base='project-events',
    schema_class=ProjectCreatedEvent,
    event_data={
        'project_id': "{{ ti.xcom_pull(task_ids='validate_project_input')['project_id'] }}",
        'project_name': "{{ ti.xcom_pull(task_ids='validate_project_input')['project_name'] }}",
        'creator_id': "{{ ti.xcom_pull(task_ids='validate_project_input')['creator_id'] }}",
        'event_timestamp': "{{ ts }}"
    },
    tenant_id="{{ ti.xcom_pull(task_ids='validate_project_input')['tenant_id'] }}",
    dag=dag,
)

# Task 8: Handle partial failures
def handle_partial_failure(**context):
    """Clean up any partially created resources"""
    project_data = context['ti'].xcom_pull(task_ids='validate_project_input')
    
    # Check which tasks succeeded
    tasks_status = {}
    for task_id in ['create_nextcloud_folder', 'create_openproject_project', 'create_zulip_stream']:
        try:
            result = context['ti'].xcom_pull(task_ids=task_id, key='status_code')
            tasks_status[task_id] = 'success' if result == 200 else 'failed'
        except:
            tasks_status[task_id] = 'failed'
    
    # Log the failure state
    failure_message = f"Project creation partially failed for {project_data['project_name']}: {tasks_status}"
    context['ti'].log.error(failure_message)
    
    # In production, we would trigger cleanup tasks here
    return {
        'project_id': project_data['project_id'],
        'status': 'partial_failure',
        'details': tasks_status
    }

handle_failure = PythonOperator(
    task_id='handle_partial_failure',
    python_callable=handle_partial_failure,
    trigger_rule=TriggerRule.ONE_FAILED,
    dag=dag,
)

# Task 9: Send notifications
def send_notifications(**context):
    """Send notifications about project creation"""
    project_data = context['ti'].xcom_pull(task_ids='save_project_metadata')
    
    notification_payload = {
        'type': 'project_created',
        'project_name': project_data['project_name'],
        'creator_id': project_data['created_by_user_id'],
        'resources': {
            'nextcloud': project_data['nextcloud_folder_path'],
            'openproject': f"projects/{project_data['openproject_id']}",
            'zulip': project_data['zulip_stream_name']
        }
    }
    
    return notification_payload

notify = PythonOperator(
    task_id='send_notifications',
    python_callable=send_notifications,
    dag=dag,
)

# Publish notification event
publish_notification = PulsarEventOperator(
    task_id='publish_notification',
    topic_base='notification-events',
    schema_class=dict,  # Generic notification schema
    event_data="{{ ti.xcom_pull(task_ids='send_notifications') }}",
    tenant_id="{{ ti.xcom_pull(task_ids='validate_project_input')['tenant_id'] }}",
    dag=dag,
)

# Define task dependencies
validate_input >> [create_nextcloud_folder, create_openproject, create_zulip_stream]

# All creation tasks must complete before saving metadata
[create_nextcloud_folder, create_openproject, create_zulip_stream] >> save_metadata

# Save and store project
save_metadata >> store_project >> publish_success

# Notifications after successful storage
store_project >> notify >> publish_notification

# Error handling for any creation task failure
[create_nextcloud_folder, create_openproject, create_zulip_stream] >> handle_failure 