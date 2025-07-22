"""
Quantum Optimization Event Processors

Handles events for quantum optimization job processing.
"""

import logging
from typing import Optional, Dict, Any, List
from datetime import datetime
import uuid
import json
import asyncio

from platformq_shared.event_framework import EventProcessor, event_handler
from platformq_shared.events import (
    OptimizationProblemCreated,
    OptimizationJobSubmitted,
    OptimizationJobStarted,
    OptimizationJobCompleted,
    OptimizationJobFailed,
    QuantumResourceRequested,
    QuantumResourceAllocated,
    QuantumCircuitCreated,
    SolverBenchmarkCompleted
)
from platformq_shared.database import get_db

from .repository import (
    OptimizationProblemRepository,
    OptimizationJobRepository,
    QuantumCircuitRepository,
    OptimizationTemplateRepository,
    SolverBenchmarkRepository,
    QuantumResourceAllocationRepository
)
from .models import (
    OptimizationProblem,
    OptimizationJob,
    JobStatus,
    SolverType,
    BackendType
)
from .engines.problem_encoder import ProblemEncoder
from .engines.solution_decoder import SolutionDecoder
from .solvers.quantum_solver import QuantumSolver
from .solvers.hybrid_solver import HybridSolver
from .algorithms import QAOA, VQE, QuantumAnnealing

logger = logging.getLogger(__name__)


class QuantumOptimizationEventProcessor(EventProcessor):
    """Event processor for quantum optimization operations"""
    
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.problem_repo = None
        self.job_repo = None
        self.circuit_repo = None
        self.template_repo = None
        self.benchmark_repo = None
        self.allocation_repo = None
        
        # Initialize engines and solvers
        self.problem_encoder = None
        self.solution_decoder = None
        self.solvers = {}
    
    def initialize_resources(self):
        """Initialize repositories and solvers"""
        if not self.problem_repo:
            db = next(get_db())
            self.problem_repo = OptimizationProblemRepository(db)
            self.job_repo = OptimizationJobRepository(db)
            self.circuit_repo = QuantumCircuitRepository(db)
            self.template_repo = OptimizationTemplateRepository(db)
            self.benchmark_repo = SolverBenchmarkRepository(db)
            self.allocation_repo = QuantumResourceAllocationRepository(db)
            
            # Initialize engines
            self.problem_encoder = ProblemEncoder()
            self.solution_decoder = SolutionDecoder()
            
            # Initialize solvers
            self.solvers = {
                SolverType.QAOA: QAOA(),
                SolverType.VQE: VQE(),
                SolverType.QUANTUM_ANNEALING: QuantumAnnealing(),
                SolverType.HYBRID_CLASSICAL_QUANTUM: HybridSolver()
            }
    
    @event_handler("persistent://public/default/optimization-events")
    async def handle_problem_created(self, event: OptimizationProblemCreated):
        """Handle new optimization problem creation"""
        self.initialize_resources()
        
        try:
            # Create problem record
            problem = self.problem_repo.create({
                "problem_id": event.problem_id,
                "tenant_id": event.tenant_id,
                "name": event.name,
                "description": event.description,
                "problem_type": event.problem_type,
                "objective_function": event.objective_function,
                "constraints": event.constraints,
                "variables": event.variables,
                "problem_data": event.problem_data,
                "num_variables": len(event.variables),
                "num_constraints": len(event.constraints),
                "solver_preferences": event.solver_preferences,
                "accuracy_requirement": event.accuracy_requirement,
                "time_limit_seconds": event.time_limit_seconds,
                "created_by": event.created_by
            })
            
            # Analyze problem characteristics
            characteristics = await self._analyze_problem(problem)
            
            self.problem_repo.update(problem.id, {
                "is_convex": characteristics.get("is_convex"),
                "is_linear": characteristics.get("is_linear"),
                "sparsity": characteristics.get("sparsity")
            })
            
            logger.info(f"Created optimization problem {problem.problem_id}")
            
        except Exception as e:
            logger.error(f"Error handling problem created: {e}")
    
    @event_handler("persistent://public/default/job-events")
    async def handle_job_submitted(self, event: OptimizationJobSubmitted):
        """Handle optimization job submission"""
        self.initialize_resources()
        
        try:
            # Get problem
            problem = self.problem_repo.get_by_problem_id(event.problem_id)
            if not problem:
                logger.error(f"Problem {event.problem_id} not found")
                return
            
            # Check resource availability
            if event.backend_type != BackendType.SIMULATOR:
                resource_check = self.allocation_repo.check_resource_availability(
                    tenant_id=event.tenant_id,
                    user_id=event.user_id,
                    required_credits=self._estimate_credits(problem, event.solver_type),
                    required_qubits=self._estimate_qubits(problem)
                )
                
                if not resource_check["available"]:
                    # Publish job failed event
                    await self.publish_event(
                        OptimizationJobFailed(
                            job_id=event.job_id,
                            error_message=resource_check["reason"],
                            tenant_id=event.tenant_id
                        ),
                        "persistent://public/default/job-events"
                    )
                    return
            
            # Create job record
            job = self.job_repo.create({
                "job_id": event.job_id,
                "tenant_id": event.tenant_id,
                "problem_id": problem.problem_id,
                "solver_type": event.solver_type,
                "backend_type": event.backend_type,
                "backend_config": event.backend_config,
                "solver_params": event.solver_params,
                "max_iterations": event.max_iterations,
                "convergence_threshold": event.convergence_threshold,
                "created_by": event.user_id,
                "status": JobStatus.PENDING
            })
            
            # Get solver recommendation if not specified
            if not event.solver_type:
                recommendation = self.benchmark_repo.get_best_solver(
                    problem_type=problem.problem_type,
                    problem_size=problem.num_variables,
                    optimize_for=event.optimize_for or "quality"
                )
                
                if recommendation:
                    job.solver_type = SolverType(recommendation["solver_type"])
                    job.backend_type = BackendType(recommendation["backend_type"])
                    self.job_repo.db.commit()
            
            # Start job processing
            asyncio.create_task(self._process_job(job, problem))
            
            # Publish job started event
            await self.publish_event(
                OptimizationJobStarted(
                    job_id=job.job_id,
                    problem_id=problem.problem_id,
                    solver_type=job.solver_type.value,
                    backend_type=job.backend_type.value,
                    tenant_id=job.tenant_id
                ),
                "persistent://public/default/job-events"
            )
            
            logger.info(f"Started optimization job {job.job_id}")
            
        except Exception as e:
            logger.error(f"Error handling job submission: {e}")
    
    async def _process_job(self, job: OptimizationJob, problem: OptimizationProblem):
        """Process an optimization job"""
        try:
            # Update status to encoding
            self.job_repo.update_job_status(job.job_id, JobStatus.ENCODING)
            
            # Encode problem for quantum solver
            encoding_start = datetime.utcnow()
            encoded_problem = await self.problem_encoder.encode(
                problem_type=problem.problem_type,
                objective=problem.objective_function,
                constraints=problem.constraints,
                variables=problem.variables,
                solver_type=job.solver_type
            )
            encoding_time = int((datetime.utcnow() - encoding_start).total_seconds() * 1000)
            
            # Update job with encoding info
            self.job_repo.update(job.id, {
                "num_qubits": encoded_problem.get("num_qubits"),
                "circuit_depth": encoded_problem.get("circuit_depth"),
                "encoding_time_ms": encoding_time
            })
            
            # Update status to running
            self.job_repo.update_job_status(job.job_id, JobStatus.RUNNING)
            
            # Get solver
            solver = self.solvers.get(job.solver_type)
            if not solver:
                raise ValueError(f"Solver {job.solver_type} not available")
            
            # Configure solver
            solver.configure(
                backend_type=job.backend_type,
                backend_config=job.backend_config,
                solver_params=job.solver_params
            )
            
            # Solve problem
            solving_start = datetime.utcnow()
            
            # Callback for iteration updates
            async def iteration_callback(iteration: int, solution: Dict, metrics: Dict):
                self.job_repo.record_iteration(
                    job_id=job.job_id,
                    iteration_number=iteration,
                    solution=solution,
                    objective_value=metrics.get("objective_value"),
                    metrics=metrics
                )
                
                # Update progress
                progress = (iteration / job.max_iterations) * 100 if job.max_iterations else 0
                self.job_repo.update(job.id, {
                    "progress": progress,
                    "current_iteration": iteration
                })
            
            # Run solver
            result = await solver.solve(
                encoded_problem=encoded_problem,
                max_iterations=job.max_iterations,
                convergence_threshold=job.convergence_threshold,
                callback=iteration_callback
            )
            
            solving_time = int((datetime.utcnow() - solving_start).total_seconds() * 1000)
            
            # Update status to decoding
            self.job_repo.update_job_status(job.job_id, JobStatus.DECODING)
            
            # Decode solution
            decoding_start = datetime.utcnow()
            decoded_solution = await self.solution_decoder.decode(
                encoded_solution=result["solution"],
                problem=problem,
                encoding_info=encoded_problem
            )
            decoding_time = int((datetime.utcnow() - decoding_start).total_seconds() * 1000)
            
            # Update job with results
            self.job_repo.update_job_status(
                job_id=job.job_id,
                status=JobStatus.COMPLETED,
                solution=decoded_solution
            )
            
            self.job_repo.update(job.id, {
                "solving_time_ms": solving_time,
                "decoding_time_ms": decoding_time,
                "quantum_volume_used": result.get("quantum_volume"),
                "num_shots": result.get("num_shots")
            })
            
            # Consume quantum credits
            if job.backend_type != BackendType.SIMULATOR:
                credits_used = self._calculate_credits_used(job, result)
                self.allocation_repo.consume_resources(
                    tenant_id=job.tenant_id,
                    user_id=job.created_by,
                    credits_used=credits_used
                )
                
                self.job_repo.update(job.id, {
                    "quantum_credits_used": credits_used,
                    "estimated_cost_usd": credits_used * 0.1  # $0.10 per credit
                })
            
            # Publish completion event
            await self.publish_event(
                OptimizationJobCompleted(
                    job_id=job.job_id,
                    problem_id=problem.problem_id,
                    solution=decoded_solution,
                    objective_value=decoded_solution["objective_value"],
                    solving_time_ms=solving_time,
                    tenant_id=job.tenant_id
                ),
                "persistent://public/default/job-events"
            )
            
            # Record benchmark if applicable
            if job.status == JobStatus.COMPLETED:
                await self._record_benchmark(job, problem)
            
            # Update template statistics if from template
            if problem.description and "template:" in problem.description:
                template_id = problem.description.split("template:")[-1].strip()
                self.template_repo.update_performance_stats(
                    template_id=template_id,
                    solving_time_ms=solving_time,
                    success=True
                )
            
            logger.info(f"Completed optimization job {job.job_id}")
            
        except Exception as e:
            logger.error(f"Error processing job {job.job_id}: {e}")
            
            # Update job status to failed
            self.job_repo.update_job_status(
                job_id=job.job_id,
                status=JobStatus.FAILED,
                error_message=str(e)
            )
            
            # Publish failure event
            await self.publish_event(
                OptimizationJobFailed(
                    job_id=job.job_id,
                    error_message=str(e),
                    tenant_id=job.tenant_id
                ),
                "persistent://public/default/job-events"
            )
    
    async def _analyze_problem(self, problem: OptimizationProblem) -> Dict[str, Any]:
        """Analyze problem characteristics"""
        try:
            # Check if linear
            is_linear = all(
                term.get("degree", 1) == 1
                for term in problem.objective_function.get("terms", [])
            )
            
            # Check convexity (simplified - only for quadratic problems)
            is_convex = False
            if problem.problem_type.value in ["quadratic_programming", "portfolio_optimization"]:
                # Check if Q matrix is positive semidefinite
                # This is a simplified check
                is_convex = True  # Assume convex for now
            
            # Calculate sparsity
            total_coefficients = len(problem.objective_function.get("terms", []))
            for constraint in problem.constraints:
                total_coefficients += len(constraint.get("terms", []))
            
            max_coefficients = problem.num_variables * (problem.num_variables + 1) / 2
            sparsity = 1.0 - (total_coefficients / max_coefficients) if max_coefficients > 0 else 1.0
            
            return {
                "is_linear": is_linear,
                "is_convex": is_convex,
                "sparsity": sparsity
            }
            
        except Exception as e:
            logger.error(f"Error analyzing problem: {e}")
            return {}
    
    def _estimate_qubits(self, problem: OptimizationProblem) -> int:
        """Estimate number of qubits needed"""
        # Simple estimation based on problem size
        if problem.problem_type == "qubo":
            return problem.num_variables
        elif problem.problem_type in ["ising", "max_cut"]:
            return problem.num_variables
        else:
            # For other problems, may need more qubits for encoding
            return min(problem.num_variables * 2, 50)  # Cap at 50 qubits
    
    def _estimate_credits(self, problem: OptimizationProblem, solver_type: SolverType) -> float:
        """Estimate quantum credits needed"""
        base_credits = problem.num_variables * 0.1
        
        # Solver multiplier
        solver_multipliers = {
            SolverType.QAOA: 1.5,
            SolverType.VQE: 2.0,
            SolverType.QUANTUM_ANNEALING: 1.0,
            SolverType.HYBRID_CLASSICAL_QUANTUM: 1.2
        }
        
        multiplier = solver_multipliers.get(solver_type, 1.0)
        
        return base_credits * multiplier
    
    def _calculate_credits_used(self, job: OptimizationJob, result: Dict[str, Any]) -> float:
        """Calculate actual credits used"""
        # Base cost per qubit-second
        qubit_seconds = (job.num_qubits * job.solving_time_ms) / 1000.0
        base_cost = qubit_seconds * 0.01  # $0.01 per qubit-second
        
        # Add shot cost
        shot_cost = (result.get("num_shots", 0) / 1000) * 0.001  # $0.001 per 1000 shots
        
        return base_cost + shot_cost
    
    async def _record_benchmark(self, job: OptimizationJob, problem: OptimizationProblem):
        """Record benchmark result"""
        try:
            benchmark = self.benchmark_repo.create({
                "benchmark_id": f"benchmark-{datetime.utcnow().timestamp()}",
                "problem_type": problem.problem_type,
                "problem_size": problem.num_variables,
                "problem_characteristics": {
                    "num_constraints": problem.num_constraints,
                    "is_convex": problem.is_convex,
                    "is_linear": problem.is_linear,
                    "sparsity": problem.sparsity
                },
                "solver_type": job.solver_type,
                "backend_type": job.backend_type,
                "solver_params": job.solver_params,
                "solving_time_ms": job.solving_time_ms,
                "solution_quality": job.solution_quality,
                "iterations_needed": job.current_iteration,
                "memory_mb": job.memory_mb_used,
                "quantum_volume": job.quantum_volume_used,
                "quantum_depth": job.circuit_depth,
                "estimated_cost_usd": job.estimated_cost_usd
            })
            
            logger.info(f"Recorded benchmark {benchmark.benchmark_id}")
            
        except Exception as e:
            logger.error(f"Error recording benchmark: {e}") 