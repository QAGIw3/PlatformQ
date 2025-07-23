"""
Quantum Optimization API endpoints.
"""

from typing import Dict, Any, List, Optional
import time
from fastapi import APIRouter, Depends, HTTPException, BackgroundTasks
from pydantic import BaseModel, Field

from app.api.deps import get_db_session
from app.core.config import settings
from app.engines.quantum import (
    QuantumOptimizer,
    ProblemType,
    SolverType,
    BackendType
)
from data_intelligence_common.core.events import EventBus
from data_intelligence_common.core.caching import CacheManager
from data_intelligence_common.integrations import IgniteClient

from platformq_shared.logging_config import get_logger

logger = get_logger(__name__)

router = APIRouter()

# Global instances (would be dependency injected in production)
_quantum_optimizer = None


def get_quantum_optimizer() -> QuantumOptimizer:
    """Get or create quantum optimizer instance."""
    global _quantum_optimizer
    if _quantum_optimizer is None:
        event_bus = EventBus()
        cache_manager = CacheManager()
        ignite_client = IgniteClient() if settings.IGNITE_URL else None
        
        _quantum_optimizer = QuantumOptimizer(
            event_bus=event_bus,
            cache_manager=cache_manager,
            ignite_client=ignite_client
        )
    
    return _quantum_optimizer


# Request/Response models
class ObjectiveFunction(BaseModel):
    """Objective function specification."""
    linear: Optional[List[float]] = Field(default=None, description="Linear coefficients")
    quadratic: Optional[List[List[float]]] = Field(default=None, description="Quadratic coefficients matrix")
    constant: Optional[float] = Field(default=0, description="Constant term")


class Constraint(BaseModel):
    """Constraint specification."""
    type: str = Field(..., description="Constraint type: equality, inequality")
    terms: List[float] = Field(..., description="Constraint coefficients")
    rhs: float = Field(..., description="Right-hand side value")
    name: Optional[str] = Field(default=None, description="Constraint name")


class Variable(BaseModel):
    """Variable specification."""
    name: str = Field(..., description="Variable name")
    type: str = Field(default="binary", description="Variable type: binary, integer, continuous")
    bounds: Optional[List[float]] = Field(default=None, description="Variable bounds [min, max]")


class ProblemCreateRequest(BaseModel):
    """Request to create optimization problem."""
    name: str = Field(..., description="Problem name")
    problem_type: ProblemType = Field(..., description="Type of optimization problem")
    objective_function: ObjectiveFunction = Field(..., description="Objective function to optimize")
    constraints: List[Constraint] = Field(default=[], description="Problem constraints")
    variables: Dict[str, Variable] = Field(..., description="Problem variables")
    metadata: Optional[Dict[str, Any]] = Field(default=None, description="Additional problem metadata")


class SolveRequest(BaseModel):
    """Request to solve optimization problem."""
    problem_id: str = Field(..., description="ID of problem to solve")
    solver_type: Optional[SolverType] = Field(default=None, description="Solver to use (auto-selected if None)")
    backend_type: BackendType = Field(default=BackendType.SIMULATOR, description="Quantum backend type")
    solver_params: Optional[Dict[str, Any]] = Field(default=None, description="Solver-specific parameters")


class SolverRecommendationRequest(BaseModel):
    """Request for solver recommendation."""
    problem_type: ProblemType = Field(..., description="Type of optimization problem")
    problem_size: int = Field(..., description="Number of variables")
    optimize_for: str = Field(default="quality", description="Optimization goal: quality, speed, cost")


class ProblemResponse(BaseModel):
    """Problem creation response."""
    problem_id: str
    name: str
    problem_type: str
    num_variables: int
    num_constraints: int
    created_at: str
    status: str = "created"


class SolutionResponse(BaseModel):
    """Solution response."""
    job_id: str
    problem_id: str
    status: str
    solution: Optional[Dict[str, Any]] = None
    objective_value: Optional[float] = None
    quality_metrics: Optional[Dict[str, Any]] = None
    solver_info: Optional[Dict[str, Any]] = None
    timing: Optional[Dict[str, Any]] = None


class SolverRecommendationResponse(BaseModel):
    """Solver recommendation response."""
    solver_type: str
    backend_type: str
    estimated_time_seconds: float
    estimated_cost: float
    confidence: float


# API Endpoints
@router.post("/problems", response_model=ProblemResponse)
async def create_problem(
    request: ProblemCreateRequest,
    quantum_optimizer: QuantumOptimizer = Depends(get_quantum_optimizer)
):
    """
    Create a new quantum optimization problem.
    
    This endpoint allows you to define optimization problems that can be solved
    using quantum algorithms. Supported problem types include:
    - QUBO (Quadratic Unconstrained Binary Optimization)
    - Max-Cut, TSP, Portfolio Optimization
    - Knapsack, Vertex Cover, Scheduling
    - And more...
    """
    try:
        # Convert request to problem definition
        variables_dict = {
            name: {
                "type": var.type,
                "bounds": var.bounds
            }
            for name, var in request.variables.items()
        }
        
        problem_def = await quantum_optimizer.create_optimization_problem(
            name=request.name,
            problem_type=request.problem_type,
            objective_function=request.objective_function.dict(),
            constraints=[c.dict() for c in request.constraints],
            variables=variables_dict,
            metadata=request.metadata
        )
        
        return ProblemResponse(
            problem_id=problem_def["problem_id"],
            name=problem_def["name"],
            problem_type=problem_def["problem_type"],
            num_variables=len(request.variables),
            num_constraints=len(request.constraints),
            created_at=problem_def["created_at"]
        )
        
    except Exception as e:
        logger.error(f"Error creating problem: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/solve", response_model=SolutionResponse)
async def solve_problem(
    request: SolveRequest,
    background_tasks: BackgroundTasks,
    quantum_optimizer: QuantumOptimizer = Depends(get_quantum_optimizer)
):
    """
    Solve a quantum optimization problem.
    
    This endpoint initiates the solving process for a previously created problem.
    The solving can be done synchronously or asynchronously depending on problem size.
    """
    try:
        # For large problems, solve in background
        if request.solver_params and request.solver_params.get("async", False):
            job_id = f"qjob-{request.problem_id}-{int(time.time())}"
            
            background_tasks.add_task(
                quantum_optimizer.solve_problem,
                problem_id=request.problem_id,
                solver_type=request.solver_type,
                backend_type=request.backend_type,
                solver_params=request.solver_params,
                job_id=job_id
            )
            
            return SolutionResponse(
                job_id=job_id,
                problem_id=request.problem_id,
                status="submitted",
                solver_info={
                    "solver_type": request.solver_type.value if request.solver_type else "auto",
                    "backend_type": request.backend_type.value
                }
            )
        else:
            # Solve synchronously
            solution = await quantum_optimizer.solve_problem(
                problem_id=request.problem_id,
                solver_type=request.solver_type,
                backend_type=request.backend_type,
                solver_params=request.solver_params
            )
            
            return SolutionResponse(
                job_id=solution.get("job_id", "sync"),
                problem_id=request.problem_id,
                status="completed",
                solution=solution.get("variables"),
                objective_value=solution.get("objective_value"),
                quality_metrics=solution.get("quality_metrics"),
                solver_info=solution.get("solver_info"),
                timing=solution.get("timing")
            )
            
    except ValueError as e:
        raise HTTPException(status_code=404, detail=str(e))
    except Exception as e:
        logger.error(f"Error solving problem: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/problems/{problem_id}", response_model=Dict[str, Any])
async def get_problem(
    problem_id: str,
    quantum_optimizer: QuantumOptimizer = Depends(get_quantum_optimizer)
):
    """Get problem details by ID."""
    try:
        # Retrieve from cache
        problem = await quantum_optimizer.cache_manager.get(f"quantum_problem:{problem_id}")
        
        if not problem:
            raise HTTPException(status_code=404, detail="Problem not found")
        
        return problem
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error retrieving problem: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/jobs/{job_id}", response_model=SolutionResponse)
async def get_job_status(
    job_id: str,
    quantum_optimizer: QuantumOptimizer = Depends(get_quantum_optimizer)
):
    """Get job status and results."""
    try:
        # Check if solution is ready
        solution = await quantum_optimizer.cache_manager.get(f"quantum_solution:{job_id}")
        
        if solution:
            return SolutionResponse(
                job_id=job_id,
                problem_id=solution["problem_id"],
                status="completed",
                solution=solution.get("variables"),
                objective_value=solution.get("objective_value"),
                quality_metrics=solution.get("quality_metrics"),
                solver_info=solution.get("solver_info")
            )
        else:
            # Job still running or not found
            return SolutionResponse(
                job_id=job_id,
                problem_id="unknown",
                status="running"
            )
            
    except Exception as e:
        logger.error(f"Error getting job status: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/solver-recommendation", response_model=SolverRecommendationResponse)
async def get_solver_recommendation(
    request: SolverRecommendationRequest,
    quantum_optimizer: QuantumOptimizer = Depends(get_quantum_optimizer)
):
    """
    Get solver recommendation based on problem characteristics.
    
    This endpoint helps select the best quantum algorithm and backend
    for your specific optimization problem.
    """
    try:
        recommendation = await quantum_optimizer.get_solver_recommendation(
            problem_type=request.problem_type,
            problem_size=request.problem_size,
            optimize_for=request.optimize_for
        )
        
        return SolverRecommendationResponse(**recommendation)
        
    except Exception as e:
        logger.error(f"Error getting recommendation: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/problem-types", response_model=List[str])
async def get_supported_problem_types():
    """Get list of supported optimization problem types."""
    return [pt.value for pt in ProblemType]


@router.get("/solver-types", response_model=List[str])
async def get_available_solvers():
    """Get list of available quantum solvers."""
    return [st.value for st in SolverType]


@router.get("/backend-types", response_model=List[str])
async def get_available_backends():
    """Get list of available quantum backends."""
    return [bt.value for bt in BackendType]


# Example problems endpoint
@router.get("/examples/{problem_type}", response_model=Dict[str, Any])
async def get_example_problem(problem_type: ProblemType):
    """Get example problem definition for a given problem type."""
    examples = {
        ProblemType.MAX_CUT: {
            "name": "Max-Cut Example",
            "problem_type": "max_cut",
            "objective_function": {
                "linear": [],
                "quadratic": []
            },
            "variables": {
                f"v{i}": {"type": "binary"} for i in range(5)
            },
            "metadata": {
                "edges": [[0, 1], [1, 2], [2, 3], [3, 4], [4, 0], [0, 2], [1, 3]]
            }
        },
        ProblemType.PORTFOLIO: {
            "name": "Portfolio Optimization Example",
            "problem_type": "portfolio",
            "objective_function": {
                "linear": [],
                "quadratic": []
            },
            "variables": {
                f"asset_{i}": {"type": "binary"} for i in range(4)
            },
            "metadata": {
                "expected_returns": [0.05, 0.07, 0.03, 0.09],
                "covariance_matrix": [
                    [0.01, 0.002, 0.001, 0.003],
                    [0.002, 0.015, 0.002, 0.001],
                    [0.001, 0.002, 0.008, 0.002],
                    [0.003, 0.001, 0.002, 0.02]
                ],
                "risk_aversion": 0.5
            }
        },
        ProblemType.TSP: {
            "name": "TSP Example (4 cities)",
            "problem_type": "tsp",
            "objective_function": {
                "linear": [],
                "quadratic": []
            },
            "variables": {
                f"x_{i}_{j}": {"type": "binary"} 
                for i in range(4) for j in range(4)
            },
            "metadata": {
                "distance_matrix": [
                    [0, 10, 15, 20],
                    [10, 0, 35, 25],
                    [15, 35, 0, 30],
                    [20, 25, 30, 0]
                ]
            }
        }
    }
    
    example = examples.get(problem_type)
    if not example:
        raise HTTPException(status_code=404, detail="No example available for this problem type")
    
    return example 