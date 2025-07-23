"""
Quantum Optimizer Module

Provides quantum and hybrid optimization capabilities for complex optimization problems.
Integrates with the platform's event-driven architecture and common libraries.
"""

import logging
from typing import Dict, Any, Optional, List, Tuple
from datetime import datetime
import numpy as np
from enum import Enum

from platformq_shared.logging_config import get_logger
from data_intelligence_common.core.events import EventBus
from data_intelligence_common.core.caching import CacheManager
from data_intelligence_common.integrations import IgniteClient
from data_intelligence_common.models.processing_models import ProcessingJob, JobStatus

from .problem_encoder import QuantumProblemEncoder
from .solution_decoder import QuantumSolutionDecoder
from .algorithms import (
    QAOAAlgorithm,
    VQEAlgorithm,
    QuantumAnnealingAlgorithm,
    HybridClassicalQuantumSolver
)

logger = get_logger(__name__)


class ProblemType(str, Enum):
    """Supported optimization problem types"""
    QUBO = "qubo"
    ISING = "ising"
    MAX_CUT = "max_cut"
    TSP = "tsp"
    PORTFOLIO = "portfolio"
    KNAPSACK = "knapsack"
    VERTEX_COVER = "vertex_cover"
    SCHEDULING = "scheduling"
    RESOURCE_ALLOCATION = "resource_allocation"
    DESIGN_OPTIMIZATION = "design_optimization"
    GENERIC = "generic"


class SolverType(str, Enum):
    """Available quantum solver types"""
    QAOA = "qaoa"
    VQE = "vqe"
    QUANTUM_ANNEALING = "quantum_annealing"
    HYBRID = "hybrid_classical_quantum"


class BackendType(str, Enum):
    """Quantum backend types"""
    SIMULATOR = "simulator"
    QUANTUM_HARDWARE = "quantum_hardware"
    CLOUD_QUANTUM = "cloud_quantum"


class QuantumOptimizer:
    """
    Main quantum optimization engine that coordinates problem encoding,
    solving, and solution decoding using various quantum algorithms.
    """
    
    def __init__(
        self,
        event_bus: EventBus,
        cache_manager: CacheManager,
        ignite_client: Optional[IgniteClient] = None
    ):
        self.event_bus = event_bus
        self.cache_manager = cache_manager
        self.ignite_client = ignite_client
        
        # Initialize components
        self.encoder = QuantumProblemEncoder()
        self.decoder = QuantumSolutionDecoder()
        
        # Initialize solvers
        self.solvers = {
            SolverType.QAOA: QAOAAlgorithm(),
            SolverType.VQE: VQEAlgorithm(),
            SolverType.QUANTUM_ANNEALING: QuantumAnnealingAlgorithm(),
            SolverType.HYBRID: HybridClassicalQuantumSolver()
        }
        
        # Cache for problem encodings and solutions
        self._encoding_cache = {}
        self._solution_cache = {}
        
        logger.info("Initialized QuantumOptimizer")
    
    async def create_optimization_problem(
        self,
        name: str,
        problem_type: ProblemType,
        objective_function: Dict[str, Any],
        constraints: List[Dict[str, Any]],
        variables: Dict[str, Any],
        metadata: Optional[Dict[str, Any]] = None
    ) -> Dict[str, Any]:
        """
        Create a new optimization problem definition.
        
        Args:
            name: Problem name
            problem_type: Type of optimization problem
            objective_function: Objective function specification
            constraints: List of constraints
            variables: Variable definitions
            metadata: Additional problem metadata
            
        Returns:
            Problem definition with ID
        """
        problem_id = f"qopt-{datetime.utcnow().timestamp()}"
        
        problem_def = {
            "problem_id": problem_id,
            "name": name,
            "problem_type": problem_type.value,
            "objective_function": objective_function,
            "constraints": constraints,
            "variables": variables,
            "metadata": metadata or {},
            "created_at": datetime.utcnow().isoformat()
        }
        
        # Cache problem definition
        await self.cache_manager.set(
            f"quantum_problem:{problem_id}",
            problem_def,
            ttl=3600  # 1 hour
        )
        
        # Publish event
        await self.event_bus.publish(
            "quantum.problem.created",
            problem_def
        )
        
        logger.info(f"Created quantum optimization problem {problem_id}")
        return problem_def
    
    async def solve_problem(
        self,
        problem_id: str,
        solver_type: Optional[SolverType] = None,
        backend_type: BackendType = BackendType.SIMULATOR,
        solver_params: Optional[Dict[str, Any]] = None,
        job_id: Optional[str] = None
    ) -> Dict[str, Any]:
        """
        Solve an optimization problem using quantum algorithms.
        
        Args:
            problem_id: ID of the problem to solve
            solver_type: Solver to use (auto-selected if None)
            backend_type: Quantum backend type
            solver_params: Solver-specific parameters
            job_id: Optional job ID for tracking
            
        Returns:
            Solution dictionary
        """
        # Retrieve problem definition
        problem_def = await self.cache_manager.get(f"quantum_problem:{problem_id}")
        if not problem_def:
            raise ValueError(f"Problem {problem_id} not found")
        
        job_id = job_id or f"qjob-{datetime.utcnow().timestamp()}"
        
        # Publish job started event
        await self.event_bus.publish(
            "quantum.job.started",
            {
                "job_id": job_id,
                "problem_id": problem_id,
                "solver_type": solver_type.value if solver_type else "auto",
                "backend_type": backend_type.value
            }
        )
        
        try:
            # Auto-select solver if not specified
            if not solver_type:
                solver_type = self._select_best_solver(problem_def)
            
            # Encode problem
            encoded_problem = await self._encode_problem(problem_def)
            
            # Get solver
            solver = self.solvers.get(solver_type)
            if not solver:
                raise ValueError(f"Solver {solver_type} not available")
            
            # Configure solver
            solver.configure(
                backend_type=backend_type,
                params=solver_params or {}
            )
            
            # Solve problem
            logger.info(f"Solving problem {problem_id} with {solver_type.value}")
            raw_solution = await solver.solve(
                encoded_problem=encoded_problem,
                callback=lambda iter, sol, metrics: self._handle_iteration(
                    job_id, iter, sol, metrics
                )
            )
            
            # Decode solution
            decoded_solution = await self._decode_solution(
                raw_solution,
                problem_def,
                encoded_problem
            )
            
            # Cache solution
            await self.cache_manager.set(
                f"quantum_solution:{job_id}",
                decoded_solution,
                ttl=7200  # 2 hours
            )
            
            # Publish completion event
            await self.event_bus.publish(
                "quantum.job.completed",
                {
                    "job_id": job_id,
                    "problem_id": problem_id,
                    "solution": decoded_solution,
                    "solver_type": solver_type.value,
                    "backend_type": backend_type.value
                }
            )
            
            return decoded_solution
            
        except Exception as e:
            logger.error(f"Error solving problem {problem_id}: {e}")
            
            # Publish failure event
            await self.event_bus.publish(
                "quantum.job.failed",
                {
                    "job_id": job_id,
                    "problem_id": problem_id,
                    "error": str(e)
                }
            )
            
            raise
    
    async def get_solver_recommendation(
        self,
        problem_type: ProblemType,
        problem_size: int,
        optimize_for: str = "quality"
    ) -> Dict[str, Any]:
        """
        Get solver recommendation based on problem characteristics.
        
        Args:
            problem_type: Type of optimization problem
            problem_size: Number of variables
            optimize_for: Optimization goal (quality, speed, cost)
            
        Returns:
            Solver recommendation
        """
        # Check cache for benchmarks
        cache_key = f"solver_benchmark:{problem_type.value}:{problem_size}:{optimize_for}"
        cached_recommendation = await self.cache_manager.get(cache_key)
        
        if cached_recommendation:
            return cached_recommendation
        
        # Default recommendations based on problem type and size
        recommendations = {
            ProblemType.QUBO: {
                "small": SolverType.QAOA,
                "medium": SolverType.HYBRID,
                "large": SolverType.QUANTUM_ANNEALING
            },
            ProblemType.PORTFOLIO: {
                "small": SolverType.VQE,
                "medium": SolverType.VQE,
                "large": SolverType.HYBRID
            },
            ProblemType.MAX_CUT: {
                "small": SolverType.QAOA,
                "medium": SolverType.QAOA,
                "large": SolverType.QUANTUM_ANNEALING
            }
        }
        
        # Determine problem size category
        if problem_size <= 10:
            size_category = "small"
        elif problem_size <= 50:
            size_category = "medium"
        else:
            size_category = "large"
        
        # Get recommendation
        solver_type = recommendations.get(problem_type, {}).get(
            size_category,
            SolverType.HYBRID  # Default to hybrid
        )
        
        recommendation = {
            "solver_type": solver_type.value,
            "backend_type": BackendType.SIMULATOR.value if problem_size <= 20 else BackendType.CLOUD_QUANTUM.value,
            "estimated_time_seconds": self._estimate_solving_time(problem_type, problem_size, solver_type),
            "estimated_cost": self._estimate_cost(problem_type, problem_size, solver_type),
            "confidence": 0.8
        }
        
        # Cache recommendation
        await self.cache_manager.set(cache_key, recommendation, ttl=3600)
        
        return recommendation
    
    async def _encode_problem(
        self,
        problem_def: Dict[str, Any]
    ) -> Dict[str, Any]:
        """Encode problem for quantum processing"""
        problem_type = ProblemType(problem_def["problem_type"])
        
        # Check encoding cache
        cache_key = f"encoding:{problem_def['problem_id']}"
        cached_encoding = self._encoding_cache.get(cache_key)
        
        if cached_encoding:
            return cached_encoding
        
        # Encode problem
        encoded = await self.encoder.encode(
            problem_type=problem_type,
            objective_function=problem_def["objective_function"],
            constraints=problem_def["constraints"],
            variables=problem_def["variables"],
            metadata=problem_def.get("metadata", {})
        )
        
        # Cache encoding
        self._encoding_cache[cache_key] = encoded
        
        return encoded
    
    async def _decode_solution(
        self,
        raw_solution: Dict[str, Any],
        problem_def: Dict[str, Any],
        encoded_problem: Dict[str, Any]
    ) -> Dict[str, Any]:
        """Decode quantum solution to problem space"""
        decoded = await self.decoder.decode(
            raw_solution=raw_solution,
            problem_def=problem_def,
            encoding_info=encoded_problem
        )
        
        # Add metadata
        decoded["problem_id"] = problem_def["problem_id"]
        decoded["solver_info"] = raw_solution.get("solver_info", {})
        decoded["quality_metrics"] = self._calculate_solution_quality(
            decoded,
            problem_def
        )
        
        return decoded
    
    def _select_best_solver(
        self,
        problem_def: Dict[str, Any]
    ) -> SolverType:
        """Auto-select best solver based on problem characteristics"""
        problem_type = ProblemType(problem_def["problem_type"])
        num_variables = len(problem_def["variables"])
        
        # Simple heuristics for solver selection
        if problem_type in [ProblemType.QUBO, ProblemType.MAX_CUT]:
            return SolverType.QAOA
        elif problem_type == ProblemType.PORTFOLIO:
            return SolverType.VQE
        elif num_variables > 100:
            return SolverType.QUANTUM_ANNEALING
        else:
            return SolverType.HYBRID
    
    def _calculate_solution_quality(
        self,
        solution: Dict[str, Any],
        problem_def: Dict[str, Any]
    ) -> Dict[str, float]:
        """Calculate solution quality metrics"""
        return {
            "objective_value": solution.get("objective_value", 0),
            "constraint_violations": self._count_constraint_violations(
                solution,
                problem_def["constraints"]
            ),
            "solution_validity": 1.0 if solution.get("is_valid", True) else 0.0,
            "convergence_score": solution.get("convergence_score", 0.8)
        }
    
    def _count_constraint_violations(
        self,
        solution: Dict[str, Any],
        constraints: List[Dict[str, Any]]
    ) -> int:
        """Count number of violated constraints"""
        violations = 0
        solution_vars = solution.get("variables", {})
        
        for constraint in constraints:
            # Simple constraint checking - would be more complex in practice
            if not self._check_constraint(solution_vars, constraint):
                violations += 1
        
        return violations
    
    def _check_constraint(
        self,
        variables: Dict[str, Any],
        constraint: Dict[str, Any]
    ) -> bool:
        """Check if a constraint is satisfied"""
        # Simplified constraint checking
        return True  # Placeholder
    
    def _estimate_solving_time(
        self,
        problem_type: ProblemType,
        problem_size: int,
        solver_type: SolverType
    ) -> float:
        """Estimate solving time in seconds"""
        base_times = {
            SolverType.QAOA: 10,
            SolverType.VQE: 20,
            SolverType.QUANTUM_ANNEALING: 5,
            SolverType.HYBRID: 15
        }
        
        base_time = base_times.get(solver_type, 10)
        size_factor = np.log(problem_size + 1)
        
        return base_time * size_factor
    
    def _estimate_cost(
        self,
        problem_type: ProblemType,
        problem_size: int,
        solver_type: SolverType
    ) -> float:
        """Estimate cost in credits"""
        base_costs = {
            SolverType.QAOA: 0.1,
            SolverType.VQE: 0.15,
            SolverType.QUANTUM_ANNEALING: 0.08,
            SolverType.HYBRID: 0.12
        }
        
        base_cost = base_costs.get(solver_type, 0.1)
        size_factor = problem_size * 0.01
        
        return base_cost + size_factor
    
    async def _handle_iteration(
        self,
        job_id: str,
        iteration: int,
        solution: Dict[str, Any],
        metrics: Dict[str, Any]
    ):
        """Handle solver iteration callback"""
        # Publish iteration event
        await self.event_bus.publish(
            "quantum.job.iteration",
            {
                "job_id": job_id,
                "iteration": iteration,
                "metrics": metrics,
                "timestamp": datetime.utcnow().isoformat()
            }
        ) 