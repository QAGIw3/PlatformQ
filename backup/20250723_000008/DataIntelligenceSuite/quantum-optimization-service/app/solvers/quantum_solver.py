"""
Quantum Solver Engine

New solver implementation based on the abstract Solver class.
"""
import numpy as np
from typing import Dict, Any

from qiskit import Aer
from qiskit.providers.aer import AerSimulator
from qiskit.utils import QuantumInstance
from qiskit.algorithms import QAOA as QiskitQAOA
from qiskit_optimization import QuadraticProgram
from qiskit_optimization.algorithms import MinimumEigenOptimizer

from platformq_shared.logging_config import get_logger

from .base import Solver
from ..engines.problem_encoder import ProblemEncoder, ProblemType

logger = get_logger(__name__)


class QuantumSolver(Solver):
    """
    A concrete implementation of the Solver class for running quantum algorithms.
    """

    def __init__(self, engine: str = "qiskit", backend_name: str = "aer_simulator", num_shots: int = 8192):
        self.engine = engine
        self.backend_name = backend_name
        self.num_shots = num_shots
        self.backend = self._initialize_backend()
        self.encoder = ProblemEncoder()
        logger.info(f"Initialized QuantumSolver with engine: {self.engine}, backend: {self.backend_name}")

    def _initialize_backend(self):
        """Initialize the quantum backend."""
        if self.engine == "qiskit":
            # For now, we only support qiskit's aer_simulator
            if self.backend_name == "aer_simulator":
                return Aer.get_backend('aer_simulator')
            else:
                # In the future, we could support other qiskit backends
                raise ValueError(f"Unsupported Qiskit backend: {self.backend_name}")
        else:
            raise ValueError(f"Unsupported quantum engine: {self.engine}")

    def solve(self, problem_data: Dict[str, Any], **kwargs) -> Dict[str, Any]:
        """
        Solves the given problem using the specified quantum algorithm and backend.

        Args:
            problem_data: The high-level description of the problem.
            **kwargs: Must contain 'algorithm_name' and 'algorithm_params'.

        Returns:
            A dictionary containing the solution.
        """
        algorithm_name = kwargs.get("algorithm_name")
        if not algorithm_name:
            raise ValueError("'algorithm_name' must be provided to the QuantumSolver.")

        # 1. Encode the problem
        # The problem_type needs to be part of the problem_data
        problem_type_str = problem_data.get("problem_type")
        if not problem_type_str:
            raise ValueError("'problem_type' must be specified in the problem_data.")
        problem_type = ProblemType(problem_type_str)

        encoded_problem = self.encoder.encode(problem_type, problem_data)

        # 2. Select and run the algorithm
        if self.engine == "qiskit":
            return self._run_qiskit_optimization(
                algorithm_name=algorithm_name,
                encoded_problem=encoded_problem,
                algorithm_params=kwargs.get("algorithm_params", {})
            )
        else:
            # Placeholder for other engines like Cirq, Pennylane
            raise NotImplementedError(f"Engine '{self.engine}' is not yet supported.")


    def _run_qiskit_optimization(self, algorithm_name: str, encoded_problem: Dict[str, Any], algorithm_params: Dict[str, Any]) -> Dict[str, Any]:
        """Run optimization using Qiskit backend."""
        # For now, we focus on QAOA with MinimumEigenOptimizer as a starting point.
        if algorithm_name.lower() != 'qaoa':
            raise NotImplementedError(f"Algorithm '{algorithm_name}' is not yet supported in this refactored solver.")

        try:
            qp = self._qubo_to_quadratic_program(encoded_problem)

            quantum_instance = QuantumInstance(
                self.backend,
                shots=self.num_shots
            )

            qaoa = QiskitQAOA(quantum_instance=quantum_instance, **algorithm_params)
            optimizer = MinimumEigenOptimizer(qaoa)
            result = optimizer.solve(qp)

            solution_dict = {f"x_{i}": result.x[i] for i in range(len(result.x))}

            return {
                'status': 'SUCCESS',
                'objective_value': result.fval,
                'solution': solution_dict,
                'solution_vector': result.x.tolist(),
            }

        except Exception as e:
            logger.error(f"Qiskit optimization failed: {e}", exc_info=True)
            return {'status': 'FAILED', 'error': str(e)}


    def _qubo_to_quadratic_program(self, problem: Dict) -> QuadraticProgram:
        """Convert QUBO problem to Qiskit QuadraticProgram"""
        qp = QuadraticProgram()
        num_vars = problem.get('metadata', {}).get('num_variables')
        if not num_vars:
            raise ValueError("Number of variables not found in encoded problem metadata.")

        for i in range(num_vars):
            qp.binary_var(f'x_{i}')

        qubo_matrix = problem.get('qubo_matrix')
        if qubo_matrix is None:
             raise ValueError("'qubo_matrix' not found in encoded problem.")

        # The qubo_matrix from ProblemEncoder is a list of lists. Convert to numpy array.
        qubo_matrix_np = np.array(qubo_matrix)
        qp.minimize(quadratic=qubo_matrix_np)

        return qp 