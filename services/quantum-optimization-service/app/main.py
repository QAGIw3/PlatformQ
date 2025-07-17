"""
Quantum Optimization Engine Service

Leverages quantum computing simulators for complex optimization problems.
"""

import asyncio
import json
import time
from typing import Dict, List, Optional, Any
from datetime import datetime, timedelta
import numpy as np
import uuid

from fastapi import FastAPI, HTTPException, BackgroundTasks, Request, Depends
from fastapi.responses import JSONResponse
from pydantic import BaseModel, Field
import pulsar
from fastapi.websockets import WebSocket

# Platform shared libraries
from platformq_shared.base_service import BaseService
from platformq_shared.pulsar_client import PulsarClient
from platformq_shared.ignite_utils import IgniteCache
from platformq_shared.logging_config import get_logger
from platformq_shared.monitoring import track_processing_time
from platformq_shared.vc_auth import require_vc, VCRequirement, researcher_required, compute_allowance_required

# Local modules
from .engines.problem_encoder import ProblemEncoder, ProblemType
from .engines.solution_decoder import SolutionDecoder
from .algorithms import QAOA, VQE, QuantumAnnealing, AmplitudeEstimation
from .algorithms.qaoa import create_qaoa_solver
from .config import QuantumConfig
from .solvers.base import Solver
from .solvers.quantum_solver import QuantumSolver
from .solvers.hybrid_solver import HybridSolver
from .solvers.benders_solver import BendersSolver
from .solvers.neuromorphic_solver import NeuromorphicSolver
from .config import (
    PUBSUB_TIMEOUT, PULSAR_SERVICE_URL, JOB_STATUS_TOPIC, RESULT_TOPIC
)

logger = get_logger(__name__)


class QuantumOptimizationService(BaseService):
    """
    Quantum Optimization Engine Service
    """
    
    def __init__(self):
        super().__init__("quantum-optimization-service", "Quantum Optimization Engine")
        self.config = QuantumConfig()
        self.encoder = None
        self.decoder = None
        self.job_cache = None
        self.result_cache = None
        self.active_jobs = {}
        
    async def startup(self):
        """Initialize service components"""
        await super().startup()
        
        # Initialize components
        self.encoder = ProblemEncoder()
        self.decoder = SolutionDecoder()
        
        # Initialize caches
        self.job_cache = IgniteCache("quantum_jobs")
        self.result_cache = IgniteCache("quantum_results")
        
        # Start consuming optimization requests
        await self._start_request_consumer()
        
        logger.info("Quantum Optimization Service initialized.")
        
    async def _start_request_consumer(self):
        """Start consuming optimization requests from Pulsar"""
        topics = [
            "persistent://public/default/optimization-requests",
            "persistent://public/default/anomaly-triggered-optimization",
            "persistent://public/default/causal-optimization-requests"
        ]
        
        for topic in topics:
            asyncio.create_task(self._consume_requests(topic))
            
    async def _consume_requests(self, topic: str):
        """Consume and process optimization requests"""
        consumer = self.pulsar_client.subscribe(
            topic,
            subscription_name=f"quantum-opt-{topic.split('/')[-1]}",
            consumer_type=pulsar.ConsumerType.Shared
        )
        
        while True:
            try:
                msg = consumer.receive(timeout_millis=1000)
                request = json.loads(msg.data())
                
                # Process optimization request
                job_id = await self._process_optimization_request(request)
                
                consumer.acknowledge(msg)
                
                # Track job
                self.active_jobs[job_id] = {
                    'status': 'processing',
                    'started_at': datetime.utcnow(),
                    'request': request
                }
                
            except Exception as e:
                if "timeout" not in str(e).lower():
                    logger.error(f"Error processing request: {e}")


# Initialize service and app
service = QuantumOptimizationService()
app = service.app


# API Models
class OptimizationRequest(BaseModel):
    problem_type: str
    problem_data: Dict[str, Any]
    constraints: Optional[Dict[str, Any]] = None
    algorithm: Optional[str] = "auto"
    max_time: Optional[int] = 300  # seconds
    quality_target: Optional[float] = 0.95


class OptimizationResponse(BaseModel):
    job_id: str
    status: str
    created_at: str
    estimated_completion: Optional[str] = None


class OptimizationResult(BaseModel):
    job_id: str
    status: str
    solution: Dict[str, Any]
    objective_value: float
    quality_score: float
    execution_time: float
    algorithm_used: str
    quantum_metrics: Dict[str, Any]


class BenchmarkRequest(BaseModel):
    problem_type: str
    problem_size: int
    algorithms: List[str] = ["qaoa", "vqe", "classical"]


class SolveRequest(BaseModel):
    problem_type: str
    problem_data: Dict[str, Any]
    solver_type: str = "quantum"  # 'quantum', 'hybrid', or 'benders'
    algorithm_name: Optional[str] = None
    algorithm_params: Optional[Dict[str, Any]] = None
    workflow: Optional[List[Dict[str, Any]]] = None
    engine: str = "qiskit"
    backend: str = "aer_simulator"
    job_id: str = Field(default_factory=lambda: str(uuid.uuid4()))
    user_id: Optional[str] = None
    synchronous: bool = False


# Multi-physics optimization models
class MultiPhysicsOptimizationRequest(BaseModel):
    simulation_id: str
    domains: List[str]
    coupling_strengths: List[List[float]]
    convergence_history: Dict[str, List[float]]
    optimization_config: Dict[str, Any] = {}


class ResourceAllocationRequest(BaseModel):
    resources: List[float]
    demands: List[List[float]]
    constraints: Dict[str, Any] = {}
    circuit_depth: int = 4

# Pre-defined hybrid workflows
DEFAULT_TSP_HYBRID_WORKFLOW = [
    {
        "solver_type": "classical",
        "algorithm": "greedy_tsp",
        "params": {},
        "output_mapping": {
            "solution_vector": "initial_tour"
        }
    },
    {
        "solver_type": "quantum",
        "algorithm": "quantum_annealing",
        "params": {
            "problem_type": "tsp",
            "use_quantum_fluctuations": True,
            "schedule": {"schedule_type": "adaptive"},
            "use_reverse_annealing": True,
            "initial_state_from": "@initial_tour"
        }
    }
]

DEFAULT_MAXCUT_HYBRID_WORKFLOW = [
    {
        "solver_type": "classical",
        "algorithm": "greedy_maxcut",
        "params": {},
        "output_mapping": {
            "solution_vector": "initial_partition",
            "optimal_value": "initial_cut_size"
        }
    },
    {
        "solver_type": "quantum",
        "algorithm": "qaoa",
        "params": {
            "problem_type": "maxcut",
            "reps": 4, # A reasonable depth for refinement
            "optimizer": "COBYLA"
        }
    }
]

DEFAULT_KNAPSACK_HYBRID_WORKFLOW = [
    {
        "solver_type": "classical",
        "algorithm": "greedy_knapsack",
        "params": {},
        "output_mapping": {
            "solution_vector": "initial_selection"
        }
    },
    {
        "solver_type": "quantum",
        "algorithm": "qaoa", # QAOA can be adapted for Knapsack
        "params": {
            "problem_type": "knapsack",
            "reps": 4,
            "optimizer": "SPSA" # SPSA is often good for noisy landscapes
        }
    }
]

# A simple factory function to get the right solver
def solver_factory(request_data: Dict[str, Any]) -> Solver:
    """
    Factory function to instantiate and return the correct solver
    based on the request data.
    """
    solver_type = request_data.get("solver_type", "quantum")
    engine = request_data.get("engine", "qiskit")
    backend = request_data.get("backend", "aer_simulator")

    if solver_type == "quantum":
        return QuantumSolver(engine=engine, backend_name=backend)
    
    elif solver_type == "hybrid":
        workflow = request_data.get("workflow")
        if not workflow:
            # Use a default workflow if none is provided
            problem_type = request_data["problem_type"]
            if problem_type == "tsp":
                workflow = DEFAULT_TSP_HYBRID_WORKFLOW
            elif problem_type == "maxcut":
                workflow = DEFAULT_MAXCUT_HYBRID_WORKFLOW
            elif problem_type == "knapsack":
                workflow = DEFAULT_KNAPSACK_HYBRID_WORKFLOW
            else:
                raise ValueError("Hybrid solver requires a workflow, and no default is available for this problem type.")
        return HybridSolver(workflow)

    elif solver_type == "benders":
        if request_data["problem_type"] != "facility_location":
            raise ValueError("Benders solver is currently only implemented for 'facility_location' problem type.")
        return BendersSolver()
    
    elif solver_type == "neuromorphic":
        return NeuromorphicSolver()

    else:
        raise ValueError(f"Invalid solver_type: '{solver_type}'. Must be 'quantum', 'hybrid', 'benders', or 'neuromorphic'.")


# REST API Endpoints
@app.post("/api/v1/optimize", response_model=OptimizationResponse)
@require_vc(
    researcher_required(level=2),  # Require level 2 researcher credential
    compute_allowance_required(hours=1)  # Require compute allowance
)
async def submit_optimization(
    request: OptimizationRequest,
    background_tasks: BackgroundTasks,
    req: Request
):
    """Submit an optimization problem
    
    Requires:
    - ResearcherCredential with level >= 2
    - ComputeAllowanceCredential with hours > 1
    """
    job_id = str(uuid.uuid4())
    
    # Extract user info from verified credentials
    user_did = None
    if hasattr(req.state, 'verified_credentials') and req.state.verified_credentials:
        user_did = req.state.verified_credentials[0].subject.get('id', 'unknown')
    
    # Store job information
    job_info = {
        'job_id': job_id,
        'status': 'queued',
        'problem_type': request.problem_type,
        'algorithm': request.algorithm,
        'created_at': datetime.utcnow().isoformat(),
        'request': request.dict(),
        'submitted_by': user_did
    }
    
    await service.job_cache.put(f"job:{job_id}", job_info)
    
    # Process in background
    background_tasks.add_task(
        service._process_optimization,
        job_id,
        request
    )
    
    # Estimate completion time
    estimated_time = service._estimate_completion_time(request)
    
    return OptimizationResponse(
        job_id=job_id,
        status="queued",
        created_at=job_info['created_at'],
        estimated_completion=(
            datetime.utcnow() + 
            timedelta(seconds=estimated_time)
        ).isoformat()
    )


@app.post("/api/v1/solve", status_code=202)
async def solve_problem(request: SolveRequest, background_tasks: BackgroundTasks):
    """Submit a problem for solving (quantum or hybrid)"""
    logger.info(f"Received solve request for job_id: {request.job_id}")

    if request.synchronous:
        result = await run_solver_task(request.dict())
        return {"status": "completed", "job_id": request.job_id, "result": result}
    else:
        background_tasks.add_task(run_solver_task, request.dict())
        return {"status": "processing", "job_id": request.job_id}

async def run_solver_task(request_data: dict):
    """
    The main task for running a solver, whether quantum, classical, or hybrid.
    Can be run in the background or awaited synchronously.
    """
    job_id = request_data['job_id']
    try:
        await update_job_status(job_id, "RUNNING")
        
        # Use the factory to get the appropriate solver
        solver = solver_factory(request_data)

        # The 'solve' method signature is now standardized
        result = solver.solve(
            problem_data=request_data['problem_data'],
            # Pass other relevant params from the request as kwargs
            algorithm_name=request_data.get('algorithm_name'),
            algorithm_params=request_data.get('algorithm_params', {}),
            quantum_engine=request_data.get('engine'),
            quantum_backend=request_data.get('backend')
        )

        logger.info(f"Solver task for job_id: {job_id} completed successfully.")
        await update_job_status(job_id, "COMPLETED", result)

    except Exception as e:
        logger.error(f"Error processing job {job_id}: {e}", exc_info=True)
        await update_job_status(job_id, "FAILED", {"error": str(e)})

async def update_job_status(job_id: str, status: str, result: Optional[Dict] = None):
    """Update job status in cache and publish to Pulsar"""
    try:
        job_info = await service.job_cache.get(f"job:{job_id}")
        if not job_info:
            raise HTTPException(status_code=404, detail="Job not found")

        job_info['status'] = status
        job_info['updated_at'] = datetime.utcnow().isoformat()
        if result:
            job_info['result'] = result
            job_info['completed_at'] = datetime.utcnow().isoformat()

        await service.job_cache.put(f"job:{job_id}", job_info)

        # Publish status update
        await service.pulsar_producer.send_async(
            JOB_STATUS_TOPIC,
            json.dumps({"job_id": job_id, "status": status}).encode('utf-8')
        )

        if result:
            await service.pulsar_producer.send_async(
                RESULT_TOPIC,
                json.dumps({"job_id": job_id, "result": result}).encode('utf-8')
            )

    except Exception as e:
        logger.error(f"Error updating job status for {job_id}: {e}")


@app.get("/api/v1/jobs/{job_id}", response_model=Dict)
async def get_job_status(job_id: str):
    """Get status of optimization job"""
    job_info = await service.job_cache.get(f"job:{job_id}")
    
    if not job_info:
        raise HTTPException(status_code=404, detail="Job not found")
        
    return job_info


@app.get("/api/v1/jobs/{job_id}/result", response_model=OptimizationResult)
async def get_optimization_result(job_id: str):
    """Get result of completed optimization job"""
    result = await service.result_cache.get(f"result:{job_id}")
    
    if not result:
        # Check if job exists
        job_info = await service.job_cache.get(f"job:{job_id}")
        if not job_info:
            raise HTTPException(status_code=404, detail="Job not found")
        elif job_info['status'] != 'completed':
            raise HTTPException(
                status_code=202, 
                detail=f"Job is {job_info['status']}"
            )
        else:
            raise HTTPException(status_code=404, detail="Result not found")
            
    return OptimizationResult(**result)


@app.post("/api/v1/problems/encode")
async def encode_problem(problem_data: Dict):
    """Encode a problem to quantum format"""
    try:
        problem_type = ProblemType(problem_data.get('type', 'generic'))
        encoded = service.encoder.encode(problem_type, problem_data)
        
        return {
            'encoding': encoded['encoding_type'],
            'num_qubits': encoded['num_qubits'],
            'circuit_depth': encoded.get('circuit_depth', 'variable'),
            'hamiltonian_terms': len(encoded.get('hamiltonian', [])),
            'metadata': encoded.get('metadata', {})
        }
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))


@app.get("/api/v1/algorithms")
async def list_algorithms():
    """List available quantum algorithms"""
    return {
        'algorithms': [
            {
                'name': 'qaoa',
                'full_name': 'Quantum Approximate Optimization Algorithm',
                'best_for': ['combinatorial', 'graph', 'scheduling'],
                'parameters': {
                    'p': 'Number of QAOA layers (1-10)',
                    'optimizer': 'Classical optimizer (COBYLA, SPSA, etc.)'
                }
            },
            {
                'name': 'vqe',
                'full_name': 'Variational Quantum Eigensolver',
                'best_for': ['chemistry', 'materials', 'optimization'],
                'parameters': {
                    'ansatz': 'Circuit ansatz type',
                    'depth': 'Circuit depth'
                }
            },
            {
                'name': 'amplitude_estimation',
                'full_name': 'Quantum Amplitude Estimation',
                'best_for': ['finance', 'monte_carlo', 'risk_analysis'],
                'parameters': {
                    'num_eval_qubits': 'Precision qubits',
                    'iterations': 'Number of iterations'
                }
            },
            {
                'name': 'annealing',
                'full_name': 'Quantum Annealing (simulated)',
                'best_for': ['optimization', 'sampling', 'machine_learning'],
                'parameters': {
                    'schedule': 'Annealing schedule',
                    'num_reads': 'Number of samples'
                }
            }
        ]
    }


@app.post("/api/v1/benchmark")
async def benchmark_algorithms(request: BenchmarkRequest):
    """Compare quantum vs classical algorithms"""
    results = {}
    
    # Generate test problem
    test_problem = service._generate_test_problem(
        request.problem_type,
        request.problem_size
    )
    
    # Run each algorithm
    for algorithm in request.algorithms:
        start_time = time.time()
        
        if algorithm == "classical":
            result = await service._run_classical_optimizer(test_problem)
        else:
            result = await service.optimizer.optimize(
                test_problem,
                algorithm=algorithm
            )
            
        results[algorithm] = {
            'execution_time': time.time() - start_time,
            'objective_value': result.get('objective_value'),
            'solution_quality': result.get('quality_score', 1.0),
            'iterations': result.get('iterations', 0)
        }
        
    # Calculate speedups
    if 'classical' in results:
        classical_time = results['classical']['execution_time']
        for algo in results:
            if algo != 'classical':
                results[algo]['speedup'] = classical_time / results[algo]['execution_time']
                
    return {
        'problem_type': request.problem_type,
        'problem_size': request.problem_size,
        'results': results
    }


@app.get("/api/v1/metrics")
async def get_metrics():
    """Get service metrics"""
    return {
        'total_jobs': len(service.active_jobs),
        'active_jobs': sum(1 for j in service.active_jobs.values() if j['status'] == 'processing'),
        'completed_jobs': sum(1 for j in service.active_jobs.values() if j['status'] == 'completed'),
        'average_execution_time': service._calculate_avg_execution_time(),
        'backend': service.config.backend,
        'gpu_enabled': service.config.gpu_acceleration,
        'circuit_cache_size': service.optimizer.get_cache_size() if service.optimizer else 0
    }


# gRPC API Implementation
@service.grpc_server.add_insecure_port('[::]:50053')
async def serve_grpc():
    """Serve gRPC API"""
    # Import proto definitions
    from .proto import quantum_optimization_pb2_grpc
    from .grpc_handlers import QuantumOptimizationServicer
    
    quantum_optimization_pb2_grpc.add_QuantumOptimizationServiceServicer_to_server(
        QuantumOptimizationServicer(service),
        service.grpc_server
    )
    
    await service.grpc_server.start()
    logger.info("gRPC server started on port 50053")


# Health check
@app.post("/api/v1/multi-physics/optimize")
async def optimize_multi_physics_coupling(
    request: MultiPhysicsOptimizationRequest,
    background_tasks: BackgroundTasks
):
    """Optimize coupling parameters for multi-physics simulations"""
    try:
        # Create QAOA solver
        qaoa_config = {
            "p": request.optimization_config.get("circuit_depth", 3),
            "optimizer": request.optimization_config.get("optimizer", "COBYLA"),
            "shots": request.optimization_config.get("shots", 1024),
            "backend": request.optimization_config.get("backend", "aer_simulator")
        }
        
        solver = create_qaoa_solver(qaoa_config)
        
        # Run optimization
        result = solver.solve_coupling_optimization(
            domains=request.domains,
            coupling_strengths=np.array(request.coupling_strengths),
            convergence_data=request.convergence_history
        )
        
        # Store result in Ignite
        result_data = {
            "simulation_id": request.simulation_id,
            "optimal_coupling": result.optimal_bitstring,
            "objective_value": result.optimal_value,
            "convergence_history": result.convergence_history,
            "execution_time": result.execution_time
        }
        
        await service.result_cache.put(f"coupling_opt_{request.simulation_id}", result_data)
        
        # Publish optimization complete event
        await service.pulsar_client.send(
            RESULT_TOPIC,
            json.dumps({
                "event_type": "COUPLING_OPTIMIZATION_COMPLETE",
                "simulation_id": request.simulation_id,
                "result": result_data
            }).encode()
        )
        
        return JSONResponse({
            "status": "success",
            "job_id": f"coupling_opt_{request.simulation_id}",
            "result": result_data
        })
        
    except Exception as e:
        logger.error(f"Error in multi-physics optimization: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/api/v1/resource-allocation/optimize")
async def optimize_resource_allocation(
    request: ResourceAllocationRequest,
    background_tasks: BackgroundTasks
):
    """Optimize resource allocation for simulation domains"""
    try:
        # Create QAOA solver
        qaoa_config = {
            "p": request.circuit_depth,
            "optimizer": "COBYLA",
            "shots": 2048
        }
        
        solver = create_qaoa_solver(qaoa_config)
        
        # Solve resource allocation
        result = solver.solve_resource_allocation(
            resources=request.resources,
            demands=request.demands,
            constraints=request.constraints
        )
        
        # Decode allocation
        allocation = solver._decode_resource_allocation(
            result.optimal_bitstring, 
            request.demands
        )
        
        return JSONResponse({
            "status": "success",
            "allocation": allocation,
            "optimal_value": result.optimal_value,
            "execution_time": result.execution_time,
            "quantum_metrics": {
                "circuit_depth": result.circuit_depth,
                "num_parameters": result.num_parameters,
                "final_convergence": result.convergence_history[-1] if result.convergence_history else None
            }
        })
        
    except Exception as e:
        logger.error(f"Error in resource allocation optimization: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/health")
async def health_check():
    """Service health check"""
    optimizer_ready = service.optimizer is not None
    
    # Test quantum backend
    backend_healthy = False
    if optimizer_ready:
        try:
            test_result = await service.optimizer.test_backend()
            backend_healthy = test_result.get('success', False)
        except:
            backend_healthy = False
            
    return {
        'status': 'healthy' if optimizer_ready and backend_healthy else 'degraded',
        'service': 'quantum-optimization-service',
        'optimizer_initialized': optimizer_ready,
        'backend_healthy': backend_healthy,
        'active_jobs': len(service.active_jobs),
        'timestamp': datetime.utcnow().isoformat()
    }


# Enhanced async optimization processing
async def _process_optimization_request(self, request: Dict[str, Any]) -> str:
    """Process optimization request with quantum algorithms"""
    job_id = str(uuid.uuid4())
    problem_type = request.get("problem_type")
    
    try:
        # Select appropriate quantum algorithm
        if problem_type == "multi_physics_coupling":
            result = await self._optimize_multi_physics_coupling(request)
        elif problem_type == "resource_allocation":
            result = await self._optimize_resource_allocation(request)
        elif problem_type == "molecular_simulation":
            result = await self._run_vqe_optimization(request)
        elif problem_type == "combinatorial":
            result = await self._run_qaoa_optimization(request)
        elif problem_type == "continuous":
            result = await self._run_quantum_annealing(request)
        else:
            # Default to hybrid optimization
            result = await self._run_hybrid_optimization(request)
        
        # Store result
        await self.result_cache.put(job_id, result)
        
        # Publish completion event
        await self._publish_optimization_complete(job_id, result)
        
        return job_id
        
    except Exception as e:
        logger.error(f"Error processing optimization request: {e}")
        await self._publish_optimization_failed(job_id, str(e))
        raise


# Add methods to QuantumOptimizationService class
async def _optimize_multi_physics_coupling(self, request: Dict[str, Any]) -> Dict[str, Any]:
    """Optimize multi-physics coupling using QAOA"""
    solver = create_qaoa_solver(request.get("qaoa_config", {}))
    
    result = solver.solve_coupling_optimization(
        domains=request.get("domains"),
        coupling_strengths=np.array(request.get("coupling_matrix")),
        convergence_data=request.get("convergence_history")
    )
    
    return {
        "algorithm": "QAOA",
        "optimal_coupling": result.optimal_bitstring,
        "objective_value": result.optimal_value,
        "quantum_metrics": {
            "circuit_depth": result.circuit_depth,
            "execution_time": result.execution_time
        }
    }


async def _run_qaoa_optimization(self, request: Dict[str, Any]) -> Dict[str, Any]:
    """Run QAOA for combinatorial optimization"""
    problem_data = request.get("problem_data", {})
    
    # Create QAOA solver
    solver = create_qaoa_solver(request.get("algorithm_params", {}))
    
    # Solve based on problem type
    if problem_data.get("type") == "maxcut":
        import networkx as nx
        graph = nx.from_numpy_array(np.array(problem_data.get("adjacency_matrix")))
        result = solver.solve_maxcut(graph)
    else:
        # Generic QUBO problem
        qubo_matrix = np.array(problem_data.get("qubo_matrix"))
        result = solver.solve_qubo(qubo_matrix)
    
    return {
        "algorithm": "QAOA",
        "optimal_solution": result.optimal_bitstring,
        "optimal_value": result.optimal_value,
        "convergence_history": result.convergence_history,
        "execution_time": result.execution_time
    }


async def _run_hybrid_optimization(self, request: Dict[str, Any]) -> Dict[str, Any]:
    """Run hybrid quantum-classical optimization"""
    # Use classical optimizer for exploration
    classical_result = await self._classical_exploration(request)
    
    # Refine with quantum optimization
    quantum_config = request.get("quantum_config", {})
    quantum_config["initial_point"] = classical_result.get("solution")
    
    quantum_result = await self._quantum_refinement(request, quantum_config)
    
    return {
        "algorithm": "Hybrid-Quantum-Classical",
        "classical_result": classical_result,
        "quantum_result": quantum_result,
        "final_solution": quantum_result.get("optimal_solution"),
        "improvement": classical_result.get("value") - quantum_result.get("optimal_value")
    }


# WebSocket endpoint for real-time optimization monitoring
@app.websocket("/ws/optimization/{job_id}")
async def optimization_monitoring(websocket: WebSocket, job_id: str):
    """WebSocket for monitoring optimization progress"""
    await websocket.accept()
    
    try:
        # Check if job exists
        if job_id not in service.active_jobs:
            await websocket.send_json({
                "error": "Job not found"
            })
            await websocket.close()
            return
        
        # Send updates while job is running
        while job_id in service.active_jobs:
            job_status = service.active_jobs[job_id]
            
            # Get current convergence data if available
            convergence_data = await service.job_cache.get(f"{job_id}_convergence")
            
            update = {
                "job_id": job_id,
                "status": job_status["status"],
                "iteration": convergence_data.get("iteration", 0) if convergence_data else 0,
                "current_value": convergence_data.get("value") if convergence_data else None,
                "convergence_history": convergence_data.get("history", []) if convergence_data else [],
                "timestamp": datetime.utcnow().isoformat()
            }
            
            await websocket.send_json(update)
            await asyncio.sleep(1)  # Send updates every second
        
        # Send final result
        result = await service.result_cache.get(job_id)
        if result:
            await websocket.send_json({
                "job_id": job_id,
                "status": "completed",
                "result": result
            })
        
    except Exception as e:
        logger.error(f"WebSocket error: {e}")
    finally:
        await websocket.close()


# Permission discovery endpoint
@app.get("/api/v1/permissions/required")
async def get_required_permissions(endpoint: str = "/api/v1/optimize"):
    """Get required credentials for an endpoint"""
    
    permissions_map = {
        "/api/v1/optimize": {
            "endpoint": "/api/v1/optimize",
            "required_credentials": [
                {
                    "type": "ResearcherCredential",
                    "claims": {"level": {"$gte": 2}},
                    "description": "Research credential with level 2 or higher"
                },
                {
                    "type": "ComputeAllowanceCredential",
                    "claims": {"compute_hours": {"$gt": 1}},
                    "description": "Compute allowance with more than 1 hour"
                }
            ],
            "optional_credentials": [
                {
                    "type": "TrustScoreCredential",
                    "claims": {"trustScore": {"$gte": 0.7}},
                    "description": "Higher trust score may provide priority processing"
                }
            ],
            "credential_manifest_url": "https://platformq.com/credentials/manifests/quantum-optimization.json"
        }
    }
    
    if endpoint not in permissions_map:
        raise HTTPException(status_code=404, detail=f"No permission info for endpoint: {endpoint}")
    
    return permissions_map[endpoint]


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8001) 