from fastapi import APIRouter, Depends, HTTPException
from typing import List

from ...schemas.function import Function, FunctionCreate, FunctionDeploy
from ...knative_client import KnativeClient

router = APIRouter()

def get_knative_client() -> KnativeClient:
    return KnativeClient()

@router.post("/functions", response_model=Function, status_code=201)
def deploy_function(
    function: FunctionDeploy,
    knative_client: KnativeClient = Depends(get_knative_client),
):
    """
    Deploy a new function.
    """
    return knative_client.deploy_function(function, function.code, function.requirements)

@router.get("/functions", response_model=List[Function])
def list_functions(
    knative_client: KnativeClient = Depends(get_knative_client),
):
    """
    List all deployed functions.
    """
    return knative_client.list_functions()

@router.get("/functions/{name}", response_model=Function)
def get_function(
    name: str,
    knative_client: KnativeClient = Depends(get_knative_client),
):
    """
    Get a specific function by name.
    """
    function = knative_client.get_function(name)
    if not function:
        raise HTTPException(status_code=404, detail="Function not found")
    return function

@router.delete("/functions/{name}", status_code=204)
def delete_function(
    name: str,
    knative_client: KnativeClient = Depends(get_knative_client),
):
    """
    Delete a function.
    """
    knative_client.delete_function(name)
    return

@router.post("/functions/{name}/invoke")
def invoke_function(
    name: str,
    knative_client: KnativeClient = Depends(get_knative_client),
):
    """
    Invoke a function.
    """
    return knative_client.invoke_function(name) 