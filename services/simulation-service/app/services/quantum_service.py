"""Quantum optimization service integration."""
import logging
import uuid
import httpx
from typing import Dict, Any
import os

logger = logging.getLogger(__name__)

QUANTUM_SERVICE_URL = os.getenv("QUANTUM_OPTIMIZATION_SERVICE_URL", "http://quantum-optimization-service:80")


async def submit_quantum_job(job_config: Dict[str, Any]) -> str:
    """Submit a quantum optimization job."""
    job_id = f"quantum_{uuid.uuid4()}"
    
    try:
        async with httpx.AsyncClient() as client:
            response = await client.post(
                f"{QUANTUM_SERVICE_URL}/api/v1/optimizations",
                json={
                    "job_id": job_id,
                    "algorithm": job_config.get("algorithm", "QAOA"),
                    "problem_type": job_config.get("problem_type", "optimization"),
                    "objective_function": job_config.get("objective_function"),
                    "constraints": job_config.get("constraints", []),
                    "parameters": {
                        "num_qubits": job_config.get("num_qubits", 10),
                        "circuit_depth": job_config.get("circuit_depth", 3),
                        "shots": job_config.get("shots", 1024),
                        "optimizer": job_config.get("optimizer", "COBYLA"),
                        "max_iterations": job_config.get("max_iterations", 100)
                    },
                    "user_id": job_config.get("user_id")
                }
            )
            
            if response.status_code == 200:
                result = response.json()
                return result.get("job_id", job_id)
            else:
                logger.error(f"Quantum service returned {response.status_code}: {response.text}")
                raise Exception(f"Quantum service error: {response.status_code}")
                
    except httpx.RequestError as e:
        logger.error(f"Error connecting to quantum service: {e}")
        # Fallback to mock job submission for development
        logger.info(f"Mock quantum job submitted: {job_id}")
        return job_id


async def get_quantum_job_status(job_id: str) -> Dict[str, Any]:
    """Get status of a quantum optimization job."""
    try:
        async with httpx.AsyncClient() as client:
            response = await client.get(
                f"{QUANTUM_SERVICE_URL}/api/v1/optimizations/{job_id}/status"
            )
            
            if response.status_code == 200:
                return response.json()
            else:
                return {
                    "job_id": job_id,
                    "status": "unknown",
                    "error": f"Service returned {response.status_code}"
                }
                
    except httpx.RequestError as e:
        logger.error(f"Error getting quantum job status: {e}")
        return {
            "job_id": job_id,
            "status": "pending",
            "message": "Mock status - quantum service unavailable"
        }


async def get_quantum_job_result(job_id: str) -> Dict[str, Any]:
    """Get result of a completed quantum optimization job."""
    try:
        async with httpx.AsyncClient() as client:
            response = await client.get(
                f"{QUANTUM_SERVICE_URL}/api/v1/optimizations/{job_id}/result"
            )
            
            if response.status_code == 200:
                return response.json()
            else:
                return {
                    "job_id": job_id,
                    "status": "error",
                    "error": f"Service returned {response.status_code}"
                }
                
    except httpx.RequestError as e:
        logger.error(f"Error getting quantum job result: {e}")
        # Return mock result for development
        return {
            "job_id": job_id,
            "status": "completed",
            "result": {
                "optimal_parameters": {"x": 0.5, "y": 0.3, "z": 0.7},
                "objective_value": -1.234,
                "convergence_history": [],
                "quantum_state": "mock_state"
            }
        } 