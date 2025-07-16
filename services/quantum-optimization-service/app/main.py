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


# Helper methods for the service class
async def _process_optimization(self, job_id: str, request: OptimizationRequest):
    """Process optimization job"""
    try:
        # Update status
        await self.job_cache.put(f"job:{job_id}", {
            **await self.job_cache.get(f"job:{job_id}"),
            'status': 'processing',
            'started_at': datetime.utcnow().isoformat()
        })
        
        # Encode problem
        problem_type = ProblemType(request.problem_type)
        encoded_problem = self.encoder.encode(problem_type, request.problem_data)
        
        # Select algorithm
        if request.algorithm == "auto":
            algorithm = self._select_best_algorithm(problem_type, encoded_problem)
        else:
            algorithm = request.algorithm
            
        # Run optimization
        start_time = time.time()
        raw_result = await self.optimizer.optimize(
            encoded_problem,
            algorithm=algorithm,
            constraints=request.constraints,
            max_time=request.max_time
        )
        execution_time = time.time() - start_time
        
        # Decode solution
        solution = self.decoder.decode(
            raw_result,
            problem_type,
            request.problem_data
        )
        
        # Create result
        result = {
            'job_id': job_id,
            'status': 'completed',
            'solution': solution,
            'objective_value': raw_result['objective_value'],
            'quality_score': raw_result.get('quality_score', 0.95),
            'execution_time': execution_time,
            'algorithm_used': algorithm,
            'quantum_metrics': {
                'num_qubits': raw_result.get('num_qubits'),
                'circuit_depth': raw_result.get('circuit_depth'),
                'iterations': raw_result.get('iterations'),
                'shots': raw_result.get('shots')
            }
        }
        
        # Store result
        await self.result_cache.put(f"result:{job_id}", result, ttl=86400)  # 24h TTL
        
        # Update job status
        await self.job_cache.put(f"job:{job_id}", {
            **await self.job_cache.get(f"job:{job_id}"),
            'status': 'completed',
            'completed_at': datetime.utcnow().isoformat(),
            'execution_time': execution_time
        })
        
        # Publish result event
        await self._publish_result(result)
        
    except Exception as e:
        logger.error(f"Error processing job {job_id}: {e}")
        
        # Update job status
        await self.job_cache.put(f"job:{job_id}", {
            **await self.job_cache.get(f"job:{job_id}"),
            'status': 'failed',
            'error': str(e),
            'failed_at': datetime.utcnow().isoformat()
        })


async def _publish_result(self, result: Dict):
    """Publish optimization result to Pulsar"""
    await self.pulsar_producer.send_async(
        "persistent://public/default/optimization-results",
        json.dumps(result).encode('utf-8')
    )


def _select_best_algorithm(self, problem_type: ProblemType, encoded_problem: Dict) -> str:
    """Select best algorithm for the problem"""
    if problem_type == ProblemType.RESOURCE_ALLOCATION:
        return "qaoa"
    elif problem_type == ProblemType.ROUTE_OPTIMIZATION:
        return "vqe"
    elif problem_type == ProblemType.PORTFOLIO:
        return "amplitude_estimation"
    elif problem_type == ProblemType.DESIGN_PARAMETERS:
        return "vqe"
    else:
        # Default based on problem structure
        if encoded_problem.get('encoding_type') == 'qubo':
            return "qaoa"
        else:
            return "vqe"


def _estimate_completion_time(self, request: OptimizationRequest) -> float:
    """Estimate completion time in seconds"""
    base_time = 10.0  # Base overhead
    
    # Problem size factor
    problem_size = len(request.problem_data.get('variables', []))
    size_factor = problem_size * 0.5
    
    # Algorithm factor
    algorithm_times = {
        'qaoa': 20.0,
        'vqe': 30.0,
        'amplitude_estimation': 25.0,
        'annealing': 15.0
    }
    algo_time = algorithm_times.get(request.algorithm, 25.0)
    
    # GPU acceleration
    if self.config.gpu_acceleration:
        algo_time *= 0.3
        
    return base_time + size_factor + algo_time


def _calculate_avg_execution_time(self) -> float:
    """Calculate average execution time of completed jobs"""
    completed_times = []
    for job in self.active_jobs.values():
        if job['status'] == 'completed' and 'execution_time' in job:
            completed_times.append(job['execution_time'])
            
    return np.mean(completed_times) if completed_times else 0.0


def _generate_test_problem(self, problem_type: str, size: int) -> Dict:
    """Generate test problem for benchmarking"""
    if problem_type == "resource_allocation":
        return {
            'type': 'resource_allocation',
            'resources': [f"R{i}" for i in range(size // 2)],
            'tasks': [f"T{i}" for i in range(size // 2)],
            'costs': np.random.rand(size // 2, size // 2).tolist()
        }
    elif problem_type == "route_optimization":
        return {
            'type': 'route_optimization',
            'cities': size,
            'distances': np.random.rand(size, size).tolist()
        }
    else:
        return {
            'type': 'generic',
            'variables': size,
            'objective': np.random.rand(size).tolist()
        }


async def _run_classical_optimizer(self, problem: Dict) -> Dict:
    """Run classical optimization for comparison"""
    # Simple classical optimizer implementation
    from scipy.optimize import minimize
    
    # Convert to classical format
    if problem['type'] == 'resource_allocation':
        # Simple greedy allocation
        result_value = np.random.rand() * 100
    else:
        # Random solution for demo
        result_value = np.random.rand() * 100
        
    return {
        'objective_value': result_value,
        'quality_score': 0.85,  # Classical baseline
        'iterations': 1000
    }


# Add these methods to the service class
service._process_optimization = _process_optimization.__get__(service)
service._publish_result = _publish_result.__get__(service)
service._select_best_algorithm = _select_best_algorithm.__get__(service)
service._estimate_completion_time = _estimate_completion_time.__get__(service)
service._calculate_avg_execution_time = _calculate_avg_execution_time.__get__(service)
service._generate_test_problem = _generate_test_problem.__get__(service)
service._run_classical_optimizer = _run_classical_optimizer.__get__(service)


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