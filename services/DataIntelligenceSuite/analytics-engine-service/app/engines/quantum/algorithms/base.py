"""
Base class for quantum optimization algorithms.
"""

from abc import ABC, abstractmethod
from typing import Dict, Any, Optional, Callable, List
from enum import Enum
import asyncio

from platformq_shared.logging_config import get_logger

logger = get_logger(__name__)


class BackendType(str, Enum):
    """Quantum backend types"""
    SIMULATOR = "simulator"
    QUANTUM_HARDWARE = "quantum_hardware"
    CLOUD_QUANTUM = "cloud_quantum"


class QuantumAlgorithmBase(ABC):
    """
    Abstract base class for quantum optimization algorithms.
    """
    
    def __init__(self):
        self.backend_type = BackendType.SIMULATOR
        self.backend = None
        self.params = {}
        self._is_configured = False
        
    def configure(
        self,
        backend_type: BackendType,
        params: Optional[Dict[str, Any]] = None
    ):
        """
        Configure the algorithm with backend and parameters.
        
        Args:
            backend_type: Type of quantum backend to use
            params: Algorithm-specific parameters
        """
        self.backend_type = backend_type
        self.params = params or {}
        self.backend = self._initialize_backend()
        self._is_configured = True
        
        logger.info(f"Configured {self.__class__.__name__} with backend {backend_type.value}")
    
    @abstractmethod
    def _initialize_backend(self):
        """Initialize the quantum backend."""
        pass
    
    @abstractmethod
    async def solve(
        self,
        encoded_problem: Dict[str, Any],
        callback: Optional[Callable] = None
    ) -> Dict[str, Any]:
        """
        Solve the encoded optimization problem.
        
        Args:
            encoded_problem: Problem encoded in quantum format
            callback: Optional callback for iteration updates
            
        Returns:
            Solution dictionary with results
        """
        pass
    
    def _validate_configuration(self):
        """Ensure algorithm is properly configured."""
        if not self._is_configured:
            raise RuntimeError(f"{self.__class__.__name__} not configured. Call configure() first.")
    
    async def _report_iteration(
        self,
        iteration: int,
        solution: Dict[str, Any],
        metrics: Dict[str, Any],
        callback: Optional[Callable] = None
    ):
        """Report iteration progress via callback."""
        if callback:
            try:
                if asyncio.iscoroutinefunction(callback):
                    await callback(iteration, solution, metrics)
                else:
                    callback(iteration, solution, metrics)
            except Exception as e:
                logger.error(f"Error in iteration callback: {e}")
    
    def _get_param(self, key: str, default: Any = None) -> Any:
        """Get parameter value with default."""
        return self.params.get(key, default)
    
    def _extract_solution_vector(self, quantum_result: Any) -> List[int]:
        """
        Extract binary solution vector from quantum result.
        
        Args:
            quantum_result: Raw result from quantum computation
            
        Returns:
            Binary solution vector
        """
        # This is a placeholder - actual implementation depends on backend
        # Most quantum results come as measurement counts
        if hasattr(quantum_result, 'get_counts'):
            counts = quantum_result.get_counts()
            # Get the most frequent measurement
            best_bitstring = max(counts, key=counts.get)
            # Convert bitstring to list of integers
            return [int(bit) for bit in best_bitstring]
        
        # Fallback for other result formats
        return []
    
    def _calculate_energy(
        self,
        solution_vector: List[int],
        problem_matrix: Any
    ) -> float:
        """
        Calculate energy/objective value for a solution.
        
        Args:
            solution_vector: Binary solution vector
            problem_matrix: Problem matrix (QUBO or Hamiltonian)
            
        Returns:
            Energy value
        """
        import numpy as np
        
        if isinstance(problem_matrix, list):
            problem_matrix = np.array(problem_matrix)
        
        solution = np.array(solution_vector)
        
        # For QUBO: energy = x^T * Q * x
        energy = solution.T @ problem_matrix @ solution
        
        return float(energy) 