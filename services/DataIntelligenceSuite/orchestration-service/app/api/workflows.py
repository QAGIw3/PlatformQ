"""
Workflow/DAG management API endpoints
"""

from typing import Dict, Any, List, Optional
from datetime import datetime

from fastapi import APIRouter, HTTPException, Query, Body
from pydantic import BaseModel

from platformq_shared.logging import get_logger
from ..core import AirflowBridge, DagState

logger = get_logger(__name__)

router = APIRouter(prefix="/api/v1/workflows", tags=["workflows"])

# Dependency injection
airflow_bridge: Optional[AirflowBridge] = None

def set_dependencies(bridge: AirflowBridge):
    """Set API dependencies"""
    global airflow_bridge
    airflow_bridge = bridge


# Request/Response models
class TriggerWorkflowRequest(BaseModel):
    context: Optional[Dict[str, Any]] = {}
    conf: Optional[Dict[str, Any]] = {}
    run_id: Optional[str] = None


class UpdateWorkflowRequest(BaseModel):
    is_paused: Optional[bool] = None
    description: Optional[str] = None
    schedule_interval: Optional[str] = None


class WorkflowResponse(BaseModel):
    dag_id: str
    description: Optional[str]
    is_paused: bool
    is_active: bool
    schedule_interval: Optional[str]
    tags: List[str]
    next_dagrun: Optional[str]
    last_run_state: Optional[str]


class WorkflowRunResponse(BaseModel):
    run_id: str
    dag_id: str
    state: str
    execution_date: str
    start_date: Optional[str]
    end_date: Optional[str]
    external_trigger: bool


# API Endpoints
@router.get("", response_model=List[WorkflowResponse])
async def list_workflows(
    limit: int = Query(100, ge=1, le=500),
    offset: int = Query(0, ge=0),
    tags: Optional[List[str]] = Query(None),
    only_active: bool = Query(True),
    search: Optional[str] = Query(None)
):
    """List all workflows (DAGs)"""
    if not airflow_bridge:
        raise HTTPException(status_code=503, detail="Airflow bridge not initialized")
        
    try:
        dags = await airflow_bridge.list_dags(
            limit=limit,
            offset=offset,
            tags=tags,
            only_active=only_active
        )
        
        # Apply search filter if provided
        if search:
            dags = [
                dag for dag in dags
                if search.lower() in dag['dag_id'].lower() or
                   search.lower() in dag.get('description', '').lower()
            ]
            
        return [
            WorkflowResponse(
                dag_id=dag['dag_id'],
                description=dag.get('description'),
                is_paused=dag['is_paused'],
                is_active=dag['is_active'],
                schedule_interval=dag.get('schedule_interval'),
                tags=dag.get('tags', []),
                next_dagrun=dag.get('next_dagrun'),
                last_run_state=dag.get('last_dagrun_state')
            )
            for dag in dags
        ]
        
    except Exception as e:
        logger.error(f"Failed to list workflows: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/{workflow_id}", response_model=WorkflowResponse)
async def get_workflow(workflow_id: str):
    """Get workflow details"""
    if not airflow_bridge:
        raise HTTPException(status_code=503, detail="Airflow bridge not initialized")
        
    try:
        dag = await airflow_bridge.get_dag(workflow_id)
        if not dag:
            raise HTTPException(status_code=404, detail=f"Workflow {workflow_id} not found")
            
        return WorkflowResponse(
            dag_id=dag['dag_id'],
            description=dag.get('description'),
            is_paused=dag['is_paused'],
            is_active=dag['is_active'],
            schedule_interval=dag.get('schedule_interval'),
            tags=dag.get('tags', []),
            next_dagrun=dag.get('next_dagrun'),
            last_run_state=dag.get('last_dagrun_state')
        )
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Failed to get workflow: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/{workflow_id}/trigger", response_model=WorkflowRunResponse)
async def trigger_workflow(
    workflow_id: str,
    request: TriggerWorkflowRequest = Body(...)
):
    """Trigger workflow execution"""
    if not airflow_bridge:
        raise HTTPException(status_code=503, detail="Airflow bridge not initialized")
        
    try:
        # Merge context and conf
        execution_conf = {
            **request.context,
            **request.conf
        }
        
        run = await airflow_bridge.trigger_dag(
            dag_id=workflow_id,
            conf=execution_conf,
            run_id=request.run_id
        )
        
        if not run:
            raise HTTPException(status_code=400, detail="Failed to trigger workflow")
            
        return WorkflowRunResponse(
            run_id=run['dag_run_id'],
            dag_id=run['dag_id'],
            state=run['state'],
            execution_date=run['execution_date'],
            start_date=run.get('start_date'),
            end_date=run.get('end_date'),
            external_trigger=run.get('external_trigger', True)
        )
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Failed to trigger workflow: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/{workflow_id}/runs", response_model=List[WorkflowRunResponse])
async def get_workflow_runs(
    workflow_id: str,
    limit: int = Query(25, ge=1, le=100),
    offset: int = Query(0, ge=0),
    state: Optional[str] = Query(None),
    start_date_gte: Optional[datetime] = Query(None),
    start_date_lte: Optional[datetime] = Query(None)
):
    """Get workflow execution history"""
    if not airflow_bridge:
        raise HTTPException(status_code=503, detail="Airflow bridge not initialized")
        
    try:
        runs = await airflow_bridge.get_dag_runs(
            dag_id=workflow_id,
            limit=limit,
            offset=offset,
            state=state,
            start_date_gte=start_date_gte,
            start_date_lte=start_date_lte
        )
        
        return [
            WorkflowRunResponse(
                run_id=run['dag_run_id'],
                dag_id=run['dag_id'],
                state=run['state'],
                execution_date=run['execution_date'],
                start_date=run.get('start_date'),
                end_date=run.get('end_date'),
                external_trigger=run.get('external_trigger', False)
            )
            for run in runs
        ]
        
    except Exception as e:
        logger.error(f"Failed to get workflow runs: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/{workflow_id}/runs/{run_id}", response_model=WorkflowRunResponse)
async def get_workflow_run(workflow_id: str, run_id: str):
    """Get specific workflow run details"""
    if not airflow_bridge:
        raise HTTPException(status_code=503, detail="Airflow bridge not initialized")
        
    try:
        run = await airflow_bridge.get_dag_run(workflow_id, run_id)
        if not run:
            raise HTTPException(status_code=404, detail=f"Run {run_id} not found")
            
        return WorkflowRunResponse(
            run_id=run['dag_run_id'],
            dag_id=run['dag_id'],
            state=run['state'],
            execution_date=run['execution_date'],
            start_date=run.get('start_date'),
            end_date=run.get('end_date'),
            external_trigger=run.get('external_trigger', False)
        )
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Failed to get workflow run: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.patch("/{workflow_id}")
async def update_workflow(
    workflow_id: str,
    request: UpdateWorkflowRequest = Body(...)
):
    """Update workflow state"""
    if not airflow_bridge:
        raise HTTPException(status_code=503, detail="Airflow bridge not initialized")
        
    try:
        # Update DAG state
        if request.is_paused is not None:
            await airflow_bridge.set_dag_state(
                workflow_id,
                DagState.PAUSED if request.is_paused else DagState.ENABLED
            )
            
        # Update other properties if supported
        # Note: Airflow API may have limitations on what can be updated
        
        return {"message": f"Workflow {workflow_id} updated successfully"}
        
    except Exception as e:
        logger.error(f"Failed to update workflow: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.delete("/{workflow_id}/runs/{run_id}")
async def cancel_workflow_run(workflow_id: str, run_id: str):
    """Cancel a running workflow"""
    if not airflow_bridge:
        raise HTTPException(status_code=503, detail="Airflow bridge not initialized")
        
    try:
        success = await airflow_bridge.cancel_dag_run(workflow_id, run_id)
        if not success:
            raise HTTPException(status_code=400, detail="Failed to cancel workflow run")
            
        return {"message": f"Workflow run {run_id} cancelled successfully"}
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Failed to cancel workflow run: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/{workflow_id}/tasks")
async def get_workflow_tasks(workflow_id: str):
    """Get workflow task details"""
    if not airflow_bridge:
        raise HTTPException(status_code=503, detail="Airflow bridge not initialized")
        
    try:
        tasks = await airflow_bridge.get_dag_tasks(workflow_id)
        return tasks
        
    except Exception as e:
        logger.error(f"Failed to get workflow tasks: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/{workflow_id}/runs/{run_id}/tasks")
async def get_workflow_run_tasks(workflow_id: str, run_id: str):
    """Get task instances for a workflow run"""
    if not airflow_bridge:
        raise HTTPException(status_code=503, detail="Airflow bridge not initialized")
        
    try:
        task_instances = await airflow_bridge.get_task_instances(workflow_id, run_id)
        return task_instances
        
    except Exception as e:
        logger.error(f"Failed to get task instances: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/{workflow_id}/clear")
async def clear_workflow_tasks(
    workflow_id: str,
    start_date: datetime = Query(...),
    end_date: datetime = Query(...),
    only_failed: bool = Query(False)
):
    """Clear task instances for re-run"""
    if not airflow_bridge:
        raise HTTPException(status_code=503, detail="Airflow bridge not initialized")
        
    try:
        # Clear task instances
        await airflow_bridge.clear_task_instances(
            workflow_id,
            start_date=start_date,
            end_date=end_date,
            only_failed=only_failed
        )
        
        return {"message": f"Tasks cleared for workflow {workflow_id}"}
        
    except Exception as e:
        logger.error(f"Failed to clear tasks: {e}")
        raise HTTPException(status_code=500, detail=str(e)) 