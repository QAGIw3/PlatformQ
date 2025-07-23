"""
Neuromorphic Solver Client

A client for the neuromorphic-service that conforms to the Solver interface.
"""
from typing import Dict, Any
import httpx
from platformq_shared.logging_config import get_logger

from .base import Solver

logger = get_logger(__name__)

NEUROMORPHIC_SERVICE_URL = "http://neuromorphic-service:8000" # Internal service URL in Docker Compose

class NeuromorphicSolver(Solver):
    """
    A client implementation of the Solver class that sends problems
    to the neuromorphic-service.
    """

    def __init__(self, service_url: str = NEUROMORPHIC_SERVICE_URL, timeout: int = 120):
        self.service_url = service_url
        self.timeout = timeout
        logger.info(f"Initialized NeuromorphicSolver, targeting service at {self.service_url}")

    def solve(self, problem_data: Dict[str, Any], **kwargs) -> Dict[str, Any]:
        """
        Solves the given problem by sending it to the neuromorphic-service.

        Args:
            problem_data: The high-level description of the problem (e.g., QUBO).
            **kwargs: Can contain 'solver_params' for the neuromorphic service.

        Returns:
            A dictionary containing the solution from the neuromorphic service.
        """
        endpoint = f"{self.service_url}/api/v1/solve"
        
        # The neuromorphic service expects a specific request body
        request_body = {
            "problem_type": problem_data.get("problem_type", "qubo"),
            "problem_data": problem_data,
            "solver_params": kwargs.get("solver_params", {})
        }
        
        logger.info(f"Sending optimization problem to Neuromorphic service at {endpoint}")

        try:
            with httpx.Client(timeout=self.timeout) as client:
                response = client.post(endpoint, json=request_body)
                response.raise_for_status() # Raise an exception for bad status codes
                
            result = response.json()
            logger.info("Received successful response from Neuromorphic service.")
            return result

        except httpx.HTTPStatusError as e:
            logger.error(f"HTTP error occurred when calling Neuromorphic service: {e.response.text}")
            return {"status": "FAILED", "error": f"HTTP Error: {e.response.status_code} - {e.response.text}"}
        except httpx.RequestError as e:
            logger.error(f"Request error occurred when calling Neuromorphic service: {e}")
            return {"status": "FAILED", "error": f"Request Error: {str(e)}"}
        except Exception as e:
            logger.error(f"An unexpected error occurred: {e}", exc_info=True)
            return {"status": "FAILED", "error": f"An unexpected error occurred: {str(e)}"} 