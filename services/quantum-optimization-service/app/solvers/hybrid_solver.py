"""
The Hybrid Solver Engine for orchestrating quantum-classical workflows.
"""
from typing import Dict, Any, List, Callable
import time

from platformq_shared.logging_config import get_logger
from services.quantum_optimization_service.app.solvers.classical_solvers import ClassicalSolvers
from services.quantum_optimization_service.app.engines.quantum_optimizer import QuantumOptimizer

logger = get_logger(__name__)

class HybridSolver:
    """
    Orchestrates a sequence of classical and quantum solvers to solve
    a complex optimization problem.
    """

    def __init__(self, workflow: List[Dict[str, Any]]):
        """
        Initializes the HybridSolver with a defined workflow.
        
        Args:
            workflow: A list of dictionaries, where each dictionary
                      represents a step in the optimization workflow.
                      Example:
                      [
                          {
                              "solver_type": "classical",
                              "algorithm": "greedy_tsp",
                              "params": {"start_node": 0},
                              "outputs": ["initial_tour", "initial_distance"]
                          },
                          {
                              "solver_type": "quantum",
                              "algorithm": "quantum_annealing",
                              "params": {
                                  "use_reverse_annealing": True,
                                  "initial_state_from": "initial_tour"
                              }
                          }
                      ]
        """
        self.workflow = workflow
        self.quantum_optimizer = None
        self.classical_solvers = ClassicalSolvers()
        self.context = {}  # Stores results from each step
        logger.info(f"Initialized HybridSolver with a {len(self.workflow)}-step workflow.")

    def solve(self,
            problem_data: Dict[str, Any],
            quantum_engine: str = "qiskit",
            quantum_backend: str = "aer_simulator") -> Dict[str, Any]:
        """
        Executes the entire hybrid workflow.
        
        Args:
            problem_data: The initial problem data.
            quantum_engine: The quantum computing engine to use ('qiskit', 'cirq', 'pennylane').
            quantum_backend: The specific backend for the quantum engine.
        
        Returns:
            A dictionary containing the final result and context from all steps.
        """
        logger.info("Starting hybrid workflow execution.")
        self.context['initial_problem'] = problem_data
        
        # Initialize quantum optimizer only when needed
        if any(step['solver_type'] == 'quantum' for step in self.workflow):
            self.quantum_optimizer = QuantumOptimizer(engine=quantum_engine, backend_name=quantum_backend)
        
        start_time = time.time()

        for i, step in enumerate(self.workflow):
            logger.info(f"Executing workflow step {i+1}/{len(self.workflow)}: {step['solver_type']}.{step['algorithm']}")
            step_start_time = time.time()

            try:
                # Prepare inputs for the current step by resolving references from the context
                step_inputs = self._prepare_step_inputs(step, problem_data)
                
                if step['solver_type'] == 'classical':
                    result = self._run_classical_step(step['algorithm'], step_inputs)
                elif step['solver_type'] == 'quantum':
                    result = self._run_quantum_step(step['algorithm'], step_inputs)
                else:
                    raise ValueError(f"Unknown solver_type: {step['solver_type']}")
                
                # Store the result of the step in the context
                self.context[f"step_{i+1}_result"] = result
                self._map_outputs_to_context(step, result)

                step_duration = time.time() - step_start_time
                logger.info(f"Step {i+1} finished in {step_duration:.2f} seconds.")

            except Exception as e:
                logger.error(f"Error during workflow step {i+1}: {e}", exc_info=True)
                return {
                    "status": "FAILED",
                    "error_message": str(e),
                    "failed_step": i + 1,
                    "context": self.context
                }

        total_duration = time.time() - start_time
        logger.info(f"Hybrid workflow completed successfully in {total_duration:.2f} seconds.")

        return {
            "status": "SUCCESS",
            "final_result": self.context.get(f"step_{len(self.workflow)}_result"),
            "full_context": self.context,
            "total_duration": total_duration
        }

    def _prepare_step_inputs(self, step: Dict[str, Any], initial_problem_data: Dict[str, Any]) -> Dict[str, Any]:
        """
        Prepares the input dictionary for a solver by combining initial problem
        data, step-specific parameters, and outputs from previous steps.
        """
        # Start with the initial problem data as a base
        inputs = initial_problem_data.copy()

        # Get step-specific parameters
        params = step.get('params', {})
        
        # Resolve references from context
        for key, value in params.items():
            if isinstance(value, str) and value.startswith('@'):
                # This is a reference to a value in the context
                context_key = value[1:]
                if context_key not in self.context:
                    raise ValueError(f"Reference '{value}' not found in workflow context. Available keys: {list(self.context.keys())}")
                inputs[key] = self.context[context_key]
            else:
                inputs[key] = value
        
        return inputs

    def _run_classical_step(self, algorithm: str, inputs: Dict[str, Any]) -> Dict[str, Any]:
        """Runs a classical solver step."""
        solver_method = getattr(self.classical_solvers, f"solve_{algorithm}", None)
        if not solver_method or not callable(solver_method):
            raise ValueError(f"Classical algorithm '{algorithm}' not found in ClassicalSolvers.")
        
        return solver_method(inputs)

    def _run_quantum_step(self, algorithm: str, inputs: Dict[str, Any]) -> Dict[str, Any]:
        """Runs a quantum solver step."""
        if not self.quantum_optimizer:
            raise RuntimeError("Quantum optimizer was not initialized.")
        
        # The quantum optimizer's `solve` method expects problem_type, problem_data, and algorithm_params
        problem_type = inputs.pop('problem_type') # e.g., 'tsp', 'maxcut'
        algorithm_params = inputs # The rest of the inputs are algorithm parameters
        
        return self.quantum_optimizer.solve(
            problem_type=problem_type,
            problem_data=self.context['initial_problem'], # Pass the original problem data
            algorithm_name=algorithm,
            algorithm_params=algorithm_params
        )

    def _map_outputs_to_context(self, step: Dict[str, Any], result: Dict[str, Any]):
        """Maps keys from the result of a step to new keys in the context."""
        output_mapping = step.get('output_mapping')
        if not output_mapping:
            return

        for result_key, context_key in output_mapping.items():
            if result_key in result:
                self.context[context_key] = result[result_key]
            else:
                logger.warning(f"Output key '{result_key}' not found in result of step {step['algorithm']}. Available keys: {list(result.keys())}") 