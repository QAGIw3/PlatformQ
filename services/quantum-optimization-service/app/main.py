"""
Quantum Optimization Service

Provides quantum and hybrid optimization solvers for complex optimization problems.
"""

import logging
from contextlib import asynccontextmanager
from typing import Dict, Any, Optional, List
from datetime import datetime
import asyncio

from fastapi import FastAPI, Depends, HTTPException, BackgroundTasks
from pydantic import BaseModel, Field
from sqlalchemy.orm import Session

from platformq_shared import (
    create_base_app,
    ErrorCode,
    AppException,
    EventProcessor,
    get_pulsar_client
)
from platformq_shared.event_publisher import EventPublisher
from platformq_shared.database import get_db

from .models import (
    OptimizationProblem,
    OptimizationJob,
    QuantumCircuit,
    OptimizationTemplate,
    SolverBenchmark,
    QuantumResourceAllocation,
    ProblemType,
    SolverType,
    JobStatus,
    BackendType
)
from .repository import (
    OptimizationProblemRepository,
    OptimizationJobRepository,
    QuantumCircuitRepository,
    OptimizationTemplateRepository,
    SolverBenchmarkRepository,
    QuantumResourceAllocationRepository
)
from .event_processors import QuantumOptimizationEventProcessor

# Setup logging
logger = logging.getLogger(__name__)

# Global instances
event_processor = None

@asynccontextmanager
async def lifespan(app: FastAPI):
    """Application lifespan manager"""
    global event_processor
    
    # Startup
    logger.info("Initializing Quantum Optimization Service...")
    
    # Initialize event processor
    pulsar_client = get_pulsar_client()
    event_processor = QuantumOptimizationEventProcessor(
        pulsar_client=pulsar_client,
        service_name="quantum-optimization-service"
    )
    
    # Start event processor
    await event_processor.start()
    
    logger.info("Quantum Optimization Service initialized successfully")
    
    yield
    
    # Shutdown
    logger.info("Shutting down Quantum Optimization Service...")
    
    if event_processor:
        await event_processor.stop()
    
    logger.info("Quantum Optimization Service shutdown complete")

# Create FastAPI app
app = create_base_app(
    title="PlatformQ Quantum Optimization Service",
    description="Quantum and hybrid optimization solvers for complex problems",
    version="1.0.0",
    lifespan=lifespan,
    event_processors=[event_processor] if event_processor else []
)

# Pydantic models for API requests/responses
class ProblemCreateRequest(BaseModel):
    name: str
    description: Optional[str] = None
    problem_type: ProblemType
    objective_function: Dict[str, Any]
    constraints: List[Dict[str, Any]] = Field(default_factory=list)
    variables: Dict[str, Any]
    solver_preferences: Optional[Dict[str, Any]] = None
    accuracy_requirement: float = Field(default=0.95, ge=0, le=1)
    time_limit_seconds: Optional[int] = None


class JobSubmitRequest(BaseModel):
    problem_id: str
    solver_type: Optional[SolverType] = None
    backend_type: BackendType = BackendType.SIMULATOR
    backend_config: Optional[Dict[str, Any]] = None
    solver_params: Optional[Dict[str, Any]] = None
    max_iterations: Optional[int] = None
    convergence_threshold: Optional[float] = None
    optimize_for: str = "quality"  # quality, speed, cost


class TemplateCreateRequest(BaseModel):
    name: str
    description: Optional[str] = None
    category: str
    problem_type: ProblemType
    template_definition: Dict[str, Any]
    parameters: Dict[str, Any]
    example_data: Optional[Dict[str, Any]] = None
    recommended_solver: Optional[SolverType] = None
    is_public: bool = False


class CircuitCreateRequest(BaseModel):
    name: str
    description: Optional[str] = None
    circuit_type: str
    qasm_code: Optional[str] = None
    circuit_json: Optional[Dict[str, Any]] = None
    is_template: bool = False


# API Endpoints

@app.post("/api/v1/problems", response_model=Dict[str, Any])
async def create_problem(
    request: ProblemCreateRequest,
    db: Session = Depends(get_db),
    tenant_id: str = Depends(lambda: "default-tenant"),  # TODO: Get from auth
    user_id: str = Depends(lambda: "user")  # TODO: Get from auth
):
    """Create a new optimization problem"""
    try:
        # Publish problem created event
        event_publisher = EventPublisher()
        problem_id = f"problem-{datetime.utcnow().timestamp()}"
        
        await event_publisher.publish_event(
            {
                "problem_id": problem_id,
                "tenant_id": tenant_id,
                "name": request.name,
                "description": request.description,
                "problem_type": request.problem_type.value,
                "objective_function": request.objective_function,
                "constraints": request.constraints,
                "variables": request.variables,
                "problem_data": {},
                "solver_preferences": request.solver_preferences,
                "accuracy_requirement": request.accuracy_requirement,
                "time_limit_seconds": request.time_limit_seconds,
                "created_by": user_id
            },
            "persistent://public/default/optimization-events"
        )
        
        return {
            "problem_id": problem_id,
            "status": "created",
            "message": "Optimization problem created successfully"
        }
        
    except Exception as e:
        logger.error(f"Error creating problem: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/v1/problems", response_model=List[Dict[str, Any]])
async def list_problems(
    problem_type: Optional[ProblemType] = None,
    is_template: Optional[bool] = None,
    db: Session = Depends(get_db),
    tenant_id: str = Depends(lambda: "default-tenant")
):
    """List optimization problems"""
    try:
        problem_repo = OptimizationProblemRepository(db)
        
        problems = problem_repo.search_problems(
            tenant_id=tenant_id,
            problem_type=problem_type,
            is_template=is_template
        )
        
        return [
            {
                "problem_id": p.problem_id,
                "name": p.name,
                "description": p.description,
                "problem_type": p.problem_type.value,
                "num_variables": p.num_variables,
                "num_constraints": p.num_constraints,
                "is_template": p.is_template,
                "created_at": p.created_at.isoformat()
            }
            for p in problems
        ]
        
    except Exception as e:
        logger.error(f"Error listing problems: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/v1/problems/{problem_id}", response_model=Dict[str, Any])
async def get_problem(
    problem_id: str,
    db: Session = Depends(get_db)
):
    """Get problem details"""
    try:
        problem_repo = OptimizationProblemRepository(db)
        
        problem = problem_repo.get_by_problem_id(problem_id)
        if not problem:
            raise HTTPException(status_code=404, detail="Problem not found")
        
        return {
            "problem_id": problem.problem_id,
            "name": problem.name,
            "description": problem.description,
            "problem_type": problem.problem_type.value,
            "objective_function": problem.objective_function,
            "constraints": problem.constraints,
            "variables": problem.variables,
            "characteristics": {
                "num_variables": problem.num_variables,
                "num_constraints": problem.num_constraints,
                "is_convex": problem.is_convex,
                "is_linear": problem.is_linear,
                "sparsity": problem.sparsity
            },
            "solver_preferences": problem.solver_preferences,
            "created_at": problem.created_at.isoformat()
        }
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error getting problem: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/api/v1/jobs/submit", response_model=Dict[str, Any])
async def submit_job(
    request: JobSubmitRequest,
    background_tasks: BackgroundTasks,
    tenant_id: str = Depends(lambda: "default-tenant"),
    user_id: str = Depends(lambda: "user")
):
    """Submit an optimization job"""
    try:
        # Publish job submitted event
        event_publisher = EventPublisher()
        job_id = f"job-{datetime.utcnow().timestamp()}"
        
        await event_publisher.publish_event(
            {
                "job_id": job_id,
                "tenant_id": tenant_id,
                "user_id": user_id,
                "problem_id": request.problem_id,
                "solver_type": request.solver_type.value if request.solver_type else None,
                "backend_type": request.backend_type.value,
                "backend_config": request.backend_config,
                "solver_params": request.solver_params,
                "max_iterations": request.max_iterations,
                "convergence_threshold": request.convergence_threshold,
                "optimize_for": request.optimize_for
            },
            "persistent://public/default/job-events"
        )
        
        return {
            "job_id": job_id,
            "status": "submitted",
            "message": "Optimization job submitted successfully"
        }
        
    except Exception as e:
        logger.error(f"Error submitting job: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/v1/jobs/{job_id}", response_model=Dict[str, Any])
async def get_job_status(
    job_id: str,
    db: Session = Depends(get_db)
):
    """Get job status and results"""
    try:
        job_repo = OptimizationJobRepository(db)
        
        job = job_repo.get_by_job_id(job_id)
        if not job:
            raise HTTPException(status_code=404, detail="Job not found")
        
        response = {
            "job_id": job.job_id,
            "problem_id": job.problem_id,
            "status": job.status.value,
            "progress": job.progress,
            "solver_type": job.solver_type.value,
            "backend_type": job.backend_type.value,
            "timing": {
                "submitted_at": job.submitted_at.isoformat(),
                "started_at": job.started_at.isoformat() if job.started_at else None,
                "completed_at": job.completed_at.isoformat() if job.completed_at else None,
                "encoding_time_ms": job.encoding_time_ms,
                "solving_time_ms": job.solving_time_ms,
                "decoding_time_ms": job.decoding_time_ms,
                "total_time_ms": job.total_time_ms
            }
        }
        
        if job.status == JobStatus.COMPLETED:
            response["solution"] = {
                "variables": job.solution,
                "objective_value": job.objective_value,
                "solution_quality": job.solution_quality
            }
            response["resource_usage"] = {
                "quantum_volume": job.quantum_volume_used,
                "quantum_credits": job.quantum_credits_used,
                "estimated_cost_usd": job.estimated_cost_usd
            }
        elif job.status == JobStatus.FAILED:
            response["error"] = {
                "message": job.error_message,
                "details": job.error_details
            }
        
        return response
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error getting job status: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/v1/jobs/{job_id}/iterations", response_model=List[Dict[str, Any]])
async def get_job_iterations(
    job_id: str,
    db: Session = Depends(get_db)
):
    """Get optimization iterations for a job"""
    try:
        job_repo = OptimizationJobRepository(db)
        
        job = job_repo.get_by_job_id(job_id)
        if not job:
            raise HTTPException(status_code=404, detail="Job not found")
        
        iterations = job.iterations
        
        return [
            {
                "iteration": i.iteration_number,
                "timestamp": i.timestamp.isoformat(),
                "objective_value": i.objective_value,
                "gradient_norm": i.gradient_norm,
                "step_size": i.step_size,
                "improvement": i.improvement
            }
            for i in sorted(iterations, key=lambda x: x.iteration_number)
        ]
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error getting iterations: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/api/v1/templates", response_model=Dict[str, Any])
async def create_template(
    request: TemplateCreateRequest,
    db: Session = Depends(get_db),
    tenant_id: str = Depends(lambda: "default-tenant"),
    user_id: str = Depends(lambda: "user")
):
    """Create an optimization problem template"""
    try:
        template_repo = OptimizationTemplateRepository(db)
        
        template = template_repo.create({
            "template_id": f"template-{datetime.utcnow().timestamp()}",
            "tenant_id": tenant_id,
            "name": request.name,
            "description": request.description,
            "category": request.category,
            "problem_type": request.problem_type,
            "template_definition": request.template_definition,
            "parameters": request.parameters,
            "example_data": request.example_data,
            "recommended_solver": request.recommended_solver,
            "is_public": request.is_public,
            "created_by": user_id
        })
        
        return {
            "template_id": template.template_id,
            "name": template.name,
            "status": "created"
        }
        
    except Exception as e:
        logger.error(f"Error creating template: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/v1/templates", response_model=List[Dict[str, Any]])
async def list_templates(
    category: Optional[str] = None,
    problem_type: Optional[ProblemType] = None,
    public_only: bool = False,
    db: Session = Depends(get_db)
):
    """List available templates"""
    try:
        template_repo = OptimizationTemplateRepository(db)
        
        if public_only:
            templates = template_repo.get_public_templates(category, problem_type)
        else:
            templates = template_repo.query().all()  # TODO: Filter by tenant
        
        return [
            {
                "template_id": t.template_id,
                "name": t.name,
                "description": t.description,
                "category": t.category,
                "problem_type": t.problem_type.value,
                "parameters": list(t.parameters.keys()),
                "recommended_solver": t.recommended_solver.value if t.recommended_solver else None,
                "usage_count": t.usage_count,
                "average_solving_time": t.average_solving_time,
                "success_rate": t.success_rate
            }
            for t in templates
        ]
        
    except Exception as e:
        logger.error(f"Error listing templates: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/api/v1/problems/from-template", response_model=Dict[str, Any])
async def create_problem_from_template(
    template_id: str,
    name: str,
    variable_values: Dict[str, Any],
    db: Session = Depends(get_db),
    tenant_id: str = Depends(lambda: "default-tenant"),
    user_id: str = Depends(lambda: "user")
):
    """Create a problem from a template"""
    try:
        problem_repo = OptimizationProblemRepository(db)
        template_repo = OptimizationTemplateRepository(db)
        
        # Create problem from template
        problem = problem_repo.create_from_template(
            template_id=template_id,
            name=name,
            tenant_id=tenant_id,
            user_id=user_id,
            variable_values=variable_values
        )
        
        # Increment template usage
        template_repo.increment_usage(template_id)
        
        return {
            "problem_id": problem.problem_id,
            "name": problem.name,
            "status": "created",
            "from_template": template_id
        }
        
    except Exception as e:
        logger.error(f"Error creating problem from template: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/api/v1/circuits", response_model=Dict[str, Any])
async def create_circuit(
    request: CircuitCreateRequest,
    db: Session = Depends(get_db),
    tenant_id: str = Depends(lambda: "default-tenant"),
    user_id: str = Depends(lambda: "user")
):
    """Create a quantum circuit"""
    try:
        circuit_repo = QuantumCircuitRepository(db)
        
        # Parse circuit to get properties
        # This is a stub - in reality would analyze the circuit
        num_qubits = 5  # Mock value
        depth = 10  # Mock value
        gate_count = 20  # Mock value
        
        circuit = circuit_repo.create({
            "circuit_id": f"circuit-{datetime.utcnow().timestamp()}",
            "tenant_id": tenant_id,
            "name": request.name,
            "description": request.description,
            "circuit_type": request.circuit_type,
            "qasm_code": request.qasm_code,
            "circuit_json": request.circuit_json,
            "num_qubits": num_qubits,
            "depth": depth,
            "gate_count": gate_count,
            "is_template": request.is_template,
            "created_by": user_id
        })
        
        return {
            "circuit_id": circuit.circuit_id,
            "name": circuit.name,
            "num_qubits": circuit.num_qubits,
            "depth": circuit.depth,
            "status": "created"
        }
        
    except Exception as e:
        logger.error(f"Error creating circuit: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/v1/benchmarks", response_model=List[Dict[str, Any]])
async def get_benchmarks(
    problem_type: Optional[ProblemType] = None,
    solver_type: Optional[SolverType] = None,
    min_problem_size: Optional[int] = None,
    max_problem_size: Optional[int] = None,
    db: Session = Depends(get_db)
):
    """Get solver benchmark results"""
    try:
        benchmark_repo = SolverBenchmarkRepository(db)
        
        benchmarks = benchmark_repo.get_benchmarks(
            problem_type=problem_type,
            solver_type=solver_type,
            min_problem_size=min_problem_size,
            max_problem_size=max_problem_size
        )
        
        return [
            {
                "benchmark_id": b.benchmark_id,
                "problem_type": b.problem_type.value,
                "problem_size": b.problem_size,
                "solver_type": b.solver_type.value,
                "backend_type": b.backend_type.value,
                "solving_time_ms": b.solving_time_ms,
                "solution_quality": b.solution_quality,
                "quantum_volume": b.quantum_volume,
                "estimated_cost_usd": b.estimated_cost_usd,
                "benchmark_date": b.benchmark_date.isoformat()
            }
            for b in benchmarks
        ]
        
    except Exception as e:
        logger.error(f"Error getting benchmarks: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/v1/solver-recommendation", response_model=Dict[str, Any])
async def get_solver_recommendation(
    problem_type: ProblemType,
    problem_size: int,
    optimize_for: str = "quality",
    db: Session = Depends(get_db)
):
    """Get solver recommendation based on benchmarks"""
    try:
        benchmark_repo = SolverBenchmarkRepository(db)
        
        recommendation = benchmark_repo.get_best_solver(
            problem_type=problem_type,
            problem_size=problem_size,
            optimize_for=optimize_for
        )
        
        if not recommendation:
            return {
                "available": False,
                "message": "No benchmark data available for this problem type and size"
            }
        
        return {
            "available": True,
            "recommendation": recommendation
        }
        
    except Exception as e:
        logger.error(f"Error getting solver recommendation: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/v1/resource-allocation", response_model=Dict[str, Any])
async def get_resource_allocation(
    db: Session = Depends(get_db),
    tenant_id: str = Depends(lambda: "default-tenant"),
    user_id: str = Depends(lambda: "user")
):
    """Get quantum resource allocation for user"""
    try:
        allocation_repo = QuantumResourceAllocationRepository(db)
        
        allocation = allocation_repo.get_user_allocation(tenant_id, user_id)
        
        if not allocation:
            return {
                "allocated": False,
                "message": "No quantum resource allocation found"
            }
        
        return {
            "allocated": True,
            "allocation": {
                "monthly_quantum_credits": allocation.monthly_quantum_credits,
                "credits_used_this_month": allocation.credits_used_this_month,
                "credits_remaining": allocation.monthly_quantum_credits - allocation.credits_used_this_month,
                "daily_job_limit": allocation.daily_job_limit,
                "jobs_today": allocation.jobs_today,
                "max_qubits": allocation.max_qubits,
                "max_circuit_depth": allocation.max_circuit_depth,
                "priority_level": allocation.priority_level,
                "is_active": allocation.is_active
            }
        }
        
    except Exception as e:
        logger.error(f"Error getting resource allocation: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/v1/statistics", response_model=Dict[str, Any])
async def get_service_statistics(
    days: int = 30,
    db: Session = Depends(get_db),
    tenant_id: str = Depends(lambda: "default-tenant")
):
    """Get service statistics"""
    try:
        job_repo = OptimizationJobRepository(db)
        problem_repo = OptimizationProblemRepository(db)
        
        # Get job statistics
        job_stats = job_repo.get_job_statistics(tenant_id, days)
        
        # Get problem counts
        total_problems = problem_repo.query().filter_by(tenant_id=tenant_id).count()
        
        # Get active jobs
        active_jobs = job_repo.get_active_jobs(tenant_id)
        
        return {
            "period_days": days,
            "problems": {
                "total": total_problems
            },
            "jobs": job_stats,
            "active_jobs": len(active_jobs),
            "generated_at": datetime.utcnow().isoformat()
        }
        
    except Exception as e:
        logger.error(f"Error getting statistics: {e}")
        raise HTTPException(status_code=500, detail=str(e))


# Health check handled by base service 