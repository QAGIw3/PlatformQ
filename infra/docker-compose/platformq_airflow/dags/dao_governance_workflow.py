from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.operators.python import PythonOperator
from platformq_airflow.operators.dao_operator import DAOProposalOperator, DAOVotingOperator, DAOExecuteProposalOperator
import json
import logging

logger = logging.getLogger(__name__)

def _log_output(**context):
    ti = context["ti"]
    task_id = context["task_instance_key_str"]
    output = ti.xcom_pull(task_ids=task_id.split(".")[-1].split("__")[0]) # Extract task_id before __ for xcom pull
    logger.info(f"Output of {task_id}: {json.dumps(output, indent=2)}")

with DAG(
    dag_id="dao_governance_workflow",
    start_date=days_ago(1),
    schedule_interval=None,
    catchup=False,
    tags=["dao", "governance", "blockchain"],
    params={
        "dao_id": {"type": "string", "title": "DAO ID", "description": "Identifier of the DAO"},
        "tenant_id": {"type": "string", "title": "Tenant ID", "description": "Identifier of the Tenant"},
        "initial_proposal_title": {"type": "string", "title": "Initial Proposal Title", "default": "Fund Community Project"},
        "initial_proposal_description": {"type": "string", "title": "Initial Proposal Description", "default": "Proposal to allocate funds for a new community initiative."},
        "voter_id": {"type": "string", "title": "Voter ID", "description": "Identifier of the voter", "default": "user-123"},
        "vote_choice": {"type": "string", "title": "Vote Choice", "enum": ["for", "against", "abstain"], "default": "for"},
        "execution_target_address": {"type": "string", "title": "Execution Target Address", "default": "0x0000000000000000000000000000000000000000"},
        "execution_value": {"type": "integer", "title": "Execution Value (Wei)", "default": 0},
        "execution_calldata": {"type": "string", "title": "Execution Calldata", "default": "0x"},
    }
) as dag:
    # Define common parameters for DAO operators
    DAO_SERVICE_URL = "http://proposals-service:8000" # Assuming proposals-service handles DAO interactions
    TENANT_ID = "{{ dag_run.conf.tenant_id or params.tenant_id }}"
    DAO_ID = "{{ dag_run.conf.dao_id or params.dao_id }}"

    # Task 1: Create a new DAO proposal
    create_proposal = DAOProposalOperator(
        task_id="create_initial_proposal",
        dao_service_url=DAO_SERVICE_URL,
        tenant_id=TENANT_ID,
        dao_id=DAO_ID,
        proposal_data={
            "title": "{{ dag_run.conf.initial_proposal_title or params.initial_proposal_title }}",
            "description": "{{ dag_run.conf.initial_proposal_description or params.initial_proposal_description }}",
            "proposer_id": "{{ dag_run.conf.voter_id or params.voter_id }}", # Proposer is also a voter for simplicity
            "type": "funding",
            "amount": "1000000000000000000", # 1 ETH in Wei for example
            "options": ["for", "against", "abstain"],
            "snapshot_space": "platformq.eth", # Placeholder Snapshot space
            "gnosis_safe_address": "{{ dag_run.conf.gnosis_safe_address or params.gnosis_safe_address }}", # Optional
        }
    )

    log_create_proposal_output = PythonOperator(
        task_id="log_create_proposal_output",
        python_callable=_log_output,
        op_kwargs={
            "task_instance_key_str": "{{ ti.task_instance_key_str }}"
        },
        provide_context=True,
    )

    # Task 2: Cast a vote on the proposal
    # The proposal_id should be pulled from the output of the create_proposal task
    cast_vote = DAOVotingOperator(
        task_id="cast_vote_on_proposal",
        dao_service_url=DAO_SERVICE_URL,
        tenant_id=TENANT_ID,
        dao_id=DAO_ID,
        proposal_id="{{ ti.xcom_pull(task_ids='create_initial_proposal', key='return_value')['proposal_id'] }}",
        voter_id="{{ dag_run.conf.voter_id or params.voter_id }}",
        vote="{{ dag_run.conf.vote_choice or params.vote_choice }}",
    )

    log_cast_vote_output = PythonOperator(
        task_id="log_cast_vote_output",
        python_callable=_log_output,
        op_kwargs={
            "task_instance_key_str": "{{ ti.task_instance_key_str }}"
        },
        provide_context=True,
    )

    # Task 3: Execute the proposal (simulate on-chain transaction)
    # This task would typically run after voting concludes and passes
    execute_proposal = DAOExecuteProposalOperator(
        task_id="execute_dao_proposal",
        dao_service_url=DAO_SERVICE_URL,
        tenant_id=TENANT_ID,
        dao_id=DAO_ID,
        proposal_id="{{ ti.xcom_pull(task_ids='create_initial_proposal', key='return_value')['proposal_id'] }}",
        execution_data={
            "target_address": "{{ dag_run.conf.execution_target_address or params.execution_target_address }}",
            "value": "{{ dag_run.conf.execution_value or params.execution_value }}",
            "calldata": "{{ dag_run.conf.execution_calldata or params.execution_calldata }}",
            "description": "Execute funding for community project as per proposal",
        },
    )

    log_execute_proposal_output = PythonOperator(
        task_id="log_execute_proposal_output",
        python_callable=_log_output,
        op_kwargs={
            "task_instance_key_str": "{{ ti.task_instance_key_str }}"
        },
        provide_context=True,
    )

    # Define task dependencies
    create_proposal >> log_create_proposal_output >> cast_vote >> log_cast_vote_output >> execute_proposal >> log_execute_proposal_output 