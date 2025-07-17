from pydantic import BaseModel, Field
from typing import List, Dict, Any

class SimulationStep(BaseModel):
    name: str
    processor: str # e.g., "openfoam-processor", "flightgear-processor"
    parameters: Dict[str, Any]
    dependencies: List[str] = Field(default_factory=list)

class FederatedSimulation(BaseModel):
    name: str
    description: str
    steps: List[SimulationStep]

def generate_federated_dag(sim_def: FederatedSimulation) -> str:
    """
    Generates a dynamic Airflow DAG from a federated simulation definition.
    """
    dag_name = sim_def.name
    dag_description = sim_def.description

    template = f"""
from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime

with DAG(
    dag_id='{dag_name}',
    start_date=datetime(2023, 1, 1),
    description='{dag_description}',
    catchup=False,
) as dag:
"""
    
    tasks = {}
    for step in sim_def.steps:
        # This is a simplified example. In a real implementation, we would use
        # the KubernetesPodOperator to launch each processor in its own pod.
        command = f"echo 'Running processor {step.processor} with params {step.parameters}'"
        
        template += f"""
    {step.name} = BashOperator(
        task_id='{step.name}',
        bash_command='{command}',
    )
"""
        tasks[step.name] = step

    # Set dependencies
    for step_name, step in tasks.items():
        if step.dependencies:
            deps_str = ", ".join(step.dependencies)
            template += f"\n    [{deps_str}] >> {step_name}"
            
    return template 