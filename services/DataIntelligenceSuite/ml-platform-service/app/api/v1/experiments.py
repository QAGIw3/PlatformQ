"""
Experiments API endpoints
"""
from typing import List, Dict, Any, Optional
from fastapi import APIRouter, Depends, HTTPException, Query
from dependency_injector.wiring import inject, Provide

from ...core.container import Container
from ...infrastructure.mlflow import MLflowClient
from ..dependencies import get_current_user

router = APIRouter(prefix="/experiments", tags=["experiments"])


@router.post("/")
@inject
async def create_experiment(
    name: str,
    description: Optional[str] = None,
    tags: Optional[Dict[str, str]] = None,
    current_user: dict = Depends(get_current_user),
    mlflow_client: MLflowClient = Depends(Provide[Container.mlflow_client])
) -> dict:
    """Create a new experiment"""
    try:
        # TODO: Implement experiment creation
        return {
            "experiment_id": "exp_123",
            "name": name,
            "description": description,
            "tags": tags or {}
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/")
@inject
async def list_experiments(
    limit: int = Query(100, le=1000),
    mlflow_client: MLflowClient = Depends(Provide[Container.mlflow_client])
) -> List[Dict[str, Any]]:
    """List experiments"""
    try:
        # TODO: Implement experiment listing
        return [
            {
                "experiment_id": "exp_123",
                "name": "Test Experiment",
                "creation_time": "2024-01-01T00:00:00Z",
                "last_update_time": "2024-01-01T00:00:00Z"
            }
        ]
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/{experiment_id}/runs")
@inject
async def get_experiment_runs(
    experiment_id: str,
    limit: int = Query(100, le=1000),
    mlflow_client: MLflowClient = Depends(Provide[Container.mlflow_client])
) -> List[Dict[str, Any]]:
    """Get runs for an experiment"""
    try:
        runs_df = await mlflow_client.search_runs(
            experiment_ids=[experiment_id],
            max_results=limit
        )
        
        # Convert DataFrame to list of dicts
        runs = runs_df.to_dict(orient="records") if not runs_df.empty else []
        return runs
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/{experiment_id}/runs")
@inject
async def create_run(
    experiment_id: str,
    run_name: str,
    tags: Optional[Dict[str, str]] = None,
    current_user: dict = Depends(get_current_user),
    mlflow_client: MLflowClient = Depends(Provide[Container.mlflow_client])
) -> dict:
    """Create a new run in an experiment"""
    try:
        # Set experiment context
        mlflow_client.experiment_id = experiment_id
        
        # Create run
        run_id = await mlflow_client.create_run(
            run_name=run_name,
            tags=tags
        )
        
        return {
            "run_id": run_id,
            "experiment_id": experiment_id,
            "run_name": run_name
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e)) 