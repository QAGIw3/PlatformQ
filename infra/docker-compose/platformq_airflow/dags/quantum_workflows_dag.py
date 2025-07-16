"""
An Airflow DAG to demonstrate orchestrating complex, conditional hybrid quantum-classical workflows.
"""
from __future__ import annotations

import pendulum
import json
from airflow.models.dag import DAG
from airflow.providers.http.operators.http import SimpleHttpOperator
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.models.baseoperator import chain


# Define the body for the TSP hybrid solver request
tsp_request_body = {
    "problem_type": "tsp",
    "solver_type": "hybrid",
    "problem_data": {
        "distance_matrix": [
            [0, 29, 20, 21, 10],
            [29, 0, 15, 17, 18],
            [20, 15, 0, 28, 22],
            [21, 17, 28, 0, 12],
            [10, 18, 22, 12, 0]
        ]
    },
    "synchronous": True 
}

# Define the body for the Benders solver request for Facility Location
flp_request_body = {
    "problem_type": "facility_location",
    "solver_type": "benders",
    "problem_data": {
        "opening_costs": [200, 250, 300, 350],
        "transportation_costs": [
            [50, 60, 70, 80, 90], # Costs from facility 0 
            [80, 70, 60, 50, 40], # Costs from facility 1
            [40, 55, 65, 75, 85], # Costs from facility 2
            [90, 85, 75, 65, 55]  # Costs from facility 3
        ]
    },
    "synchronous": True
}

def check_tsp_solution_and_branch(**kwargs):
    """
    Pulls the TSP result from XComs, checks it, and decides which path to take.
    """
    ti = kwargs['ti']
    tsp_result_str = ti.xcom_pull(task_ids='solve_hybrid_tsp', key='return_value')
    
    try:
        tsp_result = json.loads(tsp_result_str)
        final_result = tsp_result.get('result', {}).get('final_result', {})
        distance = final_result.get('optimal_value', float('inf'))
        print(f"Pulled TSP result. Final tour distance: {distance}")
        
        # Conditional logic: branch based on solution quality
        if distance < 90:
            print("TSP solution is good. Proceeding to Benders solver.")
            return "trigger_benders_solver"
        else:
            print("TSP solution does not meet quality threshold. Skipping Benders.")
            return "skip_benders_solver"
    except (json.JSONDecodeError, AttributeError) as e:
        print(f"Error processing TSP result: {e}. Skipping Benders solver.")
        return "skip_benders_solver"

with DAG(
    dag_id="conditional_quantum_workflow",
    start_date=pendulum.datetime(2023, 1, 1, tz="UTC"),
    catchup=False,
    schedule="@daily",
    tags=["quantum", "hybrid-solver", "benders", "conditional"],
    doc_md="""
    ### Conditional Quantum Workflow DAG
    Demonstrates a more complex workflow:
    1. Solves a TSP problem using a hybrid algorithm.
    2. A Python function checks the solution quality and branches the workflow.
    3. If the solution is good, it triggers a Benders solver for a Facility Location problem.
    """
) as dag:
    # Task 1: Solve the TSP
    solve_hybrid_tsp = SimpleHttpOperator(
        task_id="solve_hybrid_tsp",
        http_conn_id="quantum_optimization_service_conn",
        endpoint="/api/v1/solve",
        method="POST",
        headers={"Content-Type": "application/json"},
        data=json.dumps(tsp_request_body),
        response_check=lambda response: response.status_code == 200,
        log_response=True,
    )

    # Task 2: Branch based on the TSP result
    branching = BranchPythonOperator(
        task_id="check_tsp_solution_and_branch",
        python_callable=check_tsp_solution_and_branch,
    )

    # Task 3: (Conditional) Solve the Facility Location Problem using Benders
    trigger_benders_solver = SimpleHttpOperator(
        task_id="trigger_benders_solver",
        http_conn_id="quantum_optimization_service_conn",
        endpoint="/api/v1/solve",
        method="POST",
        headers={"Content-Type": "application/json"},
        data=json.dumps(flp_request_body),
        response_check=lambda response: response.status_code == 200,
        log_response=True,
    )

    # Task 4: (Conditional) A dummy task for the 'skip' branch
    skip_benders_solver = PythonOperator(
        task_id='skip_benders_solver',
        python_callable=lambda: print("Skipping Benders solver as TSP solution quality was not met."),
        trigger_rule='all_done' # Ensures it runs regardless of upstream failures in the branch
    )

    # Define the workflow dependencies
    solve_hybrid_tsp >> branching
    branching >> trigger_benders_solver
    branching >> skip_benders_solver 