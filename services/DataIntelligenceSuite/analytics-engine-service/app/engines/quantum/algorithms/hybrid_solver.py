"""
Hybrid Classical-Quantum solver implementation.
"""

import numpy as np
from typing import Dict, Any, Optional, Callable, List, Tuple
import asyncio
from scipy.optimize import minimize

from platformq_shared.logging_config import get_logger
from .base import QuantumAlgorithmBase, BackendType
from .qaoa import QAOAAlgorithm
from .vqe import VQEAlgorithm

logger = get_logger(__name__)


class HybridClassicalQuantumSolver(QuantumAlgorithmBase):
    """
    Hybrid solver that combines classical and quantum optimization techniques.
    
    This solver is effective for:
    - Large problems that exceed quantum hardware capabilities
    - Problems with mixed discrete/continuous variables
    - Hierarchical optimization problems
    - Problems requiring warm-start strategies
    """
    
    def __init__(self):
        super().__init__()
        self.quantum_solver = None
        self.decomposition_strategy = None
        
    def _initialize_backend(self):
        """Initialize hybrid solver components."""
        # Initialize quantum component
        solver_type = self._get_param('quantum_solver', 'qaoa')
        
        if solver_type == 'qaoa':
            self.quantum_solver = QAOAAlgorithm()
        elif solver_type == 'vqe':
            self.quantum_solver = VQEAlgorithm()
        else:
            self.quantum_solver = QAOAAlgorithm()
        
        # Configure quantum solver
        self.quantum_solver.configure(
            backend_type=self.backend_type,
            params=self._get_param('quantum_params', {})
        )
        
        return self.quantum_solver.backend
    
    async def solve(
        self,
        encoded_problem: Dict[str, Any],
        callback: Optional[Callable] = None
    ) -> Dict[str, Any]:
        """
        Solve optimization problem using hybrid approach.
        
        Args:
            encoded_problem: Encoded optimization problem
            callback: Optional callback for iteration updates
            
        Returns:
            Solution dictionary
        """
        self._validate_configuration()
        
        logger.info(f"Starting hybrid optimization with {encoded_problem.get('num_qubits')} variables")
        
        try:
            # Determine solving strategy
            strategy = self._determine_strategy(encoded_problem)
            
            if strategy == 'decomposition':
                result = await self._solve_with_decomposition(
                    encoded_problem,
                    callback
                )
            elif strategy == 'warm_start':
                result = await self._solve_with_warm_start(
                    encoded_problem,
                    callback
                )
            elif strategy == 'iterative_refinement':
                result = await self._solve_with_iterative_refinement(
                    encoded_problem,
                    callback
                )
            else:
                # Default: direct quantum solve
                result = await self._solve_direct_quantum(
                    encoded_problem,
                    callback
                )
            
            return result
            
        except Exception as e:
            logger.error(f"Hybrid solve failed: {e}")
            raise
    
    def _determine_strategy(self, encoded_problem: Dict[str, Any]) -> str:
        """Determine best hybrid strategy based on problem characteristics."""
        num_vars = encoded_problem.get('num_qubits', 0)
        problem_type = encoded_problem.get('problem_type', '')
        
        # Strategy selection heuristics
        if num_vars > 50:
            return 'decomposition'
        elif problem_type in ['portfolio', 'design_optimization']:
            return 'warm_start'
        elif num_vars > 20:
            return 'iterative_refinement'
        else:
            return 'direct'
    
    async def _solve_with_decomposition(
        self,
        encoded_problem: Dict[str, Any],
        callback: Optional[Callable] = None
    ) -> Dict[str, Any]:
        """Solve using problem decomposition."""
        logger.info("Using decomposition strategy")
        
        # Decompose problem into subproblems
        subproblems = self._decompose_problem(encoded_problem)
        
        # Solve subproblems
        subsolutions = []
        total_iterations = 0
        
        for idx, subproblem in enumerate(subproblems):
            logger.info(f"Solving subproblem {idx + 1}/{len(subproblems)}")
            
            # Solve subproblem with quantum solver
            if subproblem['num_qubits'] <= 20:
                sub_result = await self.quantum_solver.solve(
                    subproblem,
                    callback=lambda i, s, m: self._subproblem_callback(
                        idx, i, s, m, callback
                    )
                )
            else:
                # Use classical solver for larger subproblems
                sub_result = await self._solve_classical(subproblem)
            
            subsolutions.append(sub_result)
            total_iterations += sub_result.get('num_iterations', 0)
        
        # Combine subsolutions
        final_solution = self._combine_subsolutions(
            subsolutions,
            subproblems,
            encoded_problem
        )
        
        # Refine solution
        refined_solution = await self._refine_solution(
            final_solution,
            encoded_problem,
            callback
        )
        
        refined_solution['solver_info']['strategy'] = 'decomposition'
        refined_solution['solver_info']['num_subproblems'] = len(subproblems)
        
        return refined_solution
    
    async def _solve_with_warm_start(
        self,
        encoded_problem: Dict[str, Any],
        callback: Optional[Callable] = None
    ) -> Dict[str, Any]:
        """Solve using warm-start strategy."""
        logger.info("Using warm-start strategy")
        
        # Get initial solution from classical solver
        classical_result = await self._solve_classical(
            encoded_problem,
            max_time=10  # Quick classical solve
        )
        
        initial_solution = classical_result['solution_vector']
        
        # Use classical solution to initialize quantum solver
        if encoded_problem.get('encoding_type') == 'vqe':
            # For VQE, use classical solution to initialize parameters
            quantum_params = self._solution_to_vqe_params(
                initial_solution,
                encoded_problem
            )
            
            quantum_problem = encoded_problem.copy()
            quantum_problem['initial_parameters'] = quantum_params
        else:
            # For QAOA, use classical solution to bias initial state
            quantum_problem = self._add_solution_bias(
                encoded_problem,
                initial_solution
            )
        
        # Solve with quantum solver
        quantum_result = await self.quantum_solver.solve(
            quantum_problem,
            callback
        )
        
        # Compare solutions
        if quantum_result['objective_value'] < classical_result['objective_value']:
            final_result = quantum_result
        else:
            final_result = classical_result
        
        final_result['solver_info']['strategy'] = 'warm_start'
        final_result['solver_info']['classical_objective'] = classical_result['objective_value']
        final_result['solver_info']['quantum_objective'] = quantum_result['objective_value']
        
        return final_result
    
    async def _solve_with_iterative_refinement(
        self,
        encoded_problem: Dict[str, Any],
        callback: Optional[Callable] = None
    ) -> Dict[str, Any]:
        """Solve using iterative refinement between classical and quantum."""
        logger.info("Using iterative refinement strategy")
        
        max_iterations = self._get_param('max_refinement_iterations', 5)
        improvement_threshold = self._get_param('improvement_threshold', 0.01)
        
        # Initial solution
        current_solution = None
        current_objective = float('inf')
        iteration_count = 0
        
        for iteration in range(max_iterations):
            # Quantum step
            if current_solution is None:
                quantum_problem = encoded_problem
            else:
                # Use current solution to guide quantum search
                quantum_problem = self._create_guided_problem(
                    encoded_problem,
                    current_solution
                )
            
            quantum_result = await self.quantum_solver.solve(
                quantum_problem,
                callback=lambda i, s, m: self._refinement_callback(
                    iteration, 'quantum', i, s, m, callback
                )
            )
            
            # Classical refinement step
            classical_result = await self._local_search(
                quantum_result['solution_vector'],
                encoded_problem
            )
            
            # Update solution if improved
            new_objective = classical_result['objective_value']
            improvement = (current_objective - new_objective) / abs(current_objective)
            
            if improvement > improvement_threshold:
                current_solution = classical_result['solution_vector']
                current_objective = new_objective
                iteration_count += 1
            else:
                break
            
            # Report progress
            if callback:
                await self._report_iteration(
                    iteration,
                    {"solution_vector": current_solution},
                    {
                        "objective_value": current_objective,
                        "improvement": improvement,
                        "phase": "refinement_complete"
                    },
                    callback
                )
        
        return {
            'status': 'SUCCESS',
            'solution_vector': current_solution,
            'objective_value': current_objective,
            'num_iterations': iteration_count,
            'solver_info': {
                'algorithm': 'hybrid_iterative',
                'strategy': 'iterative_refinement',
                'refinement_iterations': iteration_count
            }
        }
    
    async def _solve_direct_quantum(
        self,
        encoded_problem: Dict[str, Any],
        callback: Optional[Callable] = None
    ) -> Dict[str, Any]:
        """Direct quantum solve for small problems."""
        result = await self.quantum_solver.solve(encoded_problem, callback)
        result['solver_info']['strategy'] = 'direct_quantum'
        return result
    
    def _decompose_problem(
        self,
        encoded_problem: Dict[str, Any]
    ) -> List[Dict[str, Any]]:
        """Decompose large problem into smaller subproblems."""
        if encoded_problem['encoding_type'] != 'qubo':
            # For now, only support QUBO decomposition
            return [encoded_problem]
        
        Q = np.array(encoded_problem['qubo_matrix'])
        n = Q.shape[0]
        
        # Simple block decomposition
        block_size = min(20, n // 2)
        subproblems = []
        
        for i in range(0, n, block_size):
            end = min(i + block_size, n)
            
            # Extract submatrix
            sub_Q = Q[i:end, i:end]
            
            # Create subproblem
            subproblem = {
                'encoding_type': 'qubo',
                'qubo_matrix': sub_Q.tolist(),
                'num_qubits': sub_Q.shape[0],
                'original_indices': list(range(i, end)),
                'coupling_terms': self._extract_coupling_terms(Q, i, end)
            }
            
            subproblems.append(subproblem)
        
        return subproblems
    
    def _extract_coupling_terms(
        self,
        Q: np.ndarray,
        start: int,
        end: int
    ) -> Dict[str, float]:
        """Extract coupling terms between blocks."""
        coupling = {}
        
        for i in range(start, end):
            for j in range(Q.shape[1]):
                if j < start or j >= end:
                    if abs(Q[i, j]) > 1e-10:
                        coupling[f"{i},{j}"] = Q[i, j]
        
        return coupling
    
    def _combine_subsolutions(
        self,
        subsolutions: List[Dict[str, Any]],
        subproblems: List[Dict[str, Any]],
        original_problem: Dict[str, Any]
    ) -> Dict[str, Any]:
        """Combine subsolutions into complete solution."""
        n = original_problem['num_qubits']
        combined_solution = [0] * n
        
        for subsol, subprob in zip(subsolutions, subproblems):
            indices = subprob['original_indices']
            sub_vector = subsol['solution_vector']
            
            for local_idx, global_idx in enumerate(indices):
                if local_idx < len(sub_vector):
                    combined_solution[global_idx] = sub_vector[local_idx]
        
        # Calculate objective value
        Q = np.array(original_problem['qubo_matrix'])
        sol_array = np.array(combined_solution)
        objective = sol_array.T @ Q @ sol_array
        
        return {
            'solution_vector': combined_solution,
            'objective_value': float(objective)
        }
    
    async def _refine_solution(
        self,
        solution: Dict[str, Any],
        encoded_problem: Dict[str, Any],
        callback: Optional[Callable] = None
    ) -> Dict[str, Any]:
        """Refine solution using local search."""
        refined = await self._local_search(
            solution['solution_vector'],
            encoded_problem
        )
        
        return {
            'status': 'SUCCESS',
            'solution_vector': refined['solution_vector'],
            'objective_value': refined['objective_value'],
            'solver_info': {
                'algorithm': 'hybrid_classical_quantum',
                'initial_objective': solution['objective_value'],
                'refined_objective': refined['objective_value']
            }
        }
    
    async def _solve_classical(
        self,
        encoded_problem: Dict[str, Any],
        max_time: Optional[float] = None
    ) -> Dict[str, Any]:
        """Solve using classical optimization."""
        if encoded_problem['encoding_type'] == 'qubo':
            Q = np.array(encoded_problem['qubo_matrix'])
            n = Q.shape[0]
            
            # Define objective function
            def objective(x):
                return x.T @ Q @ x
            
            # Random initial point
            x0 = np.random.randint(0, 2, n)
            
            # Use simulated annealing
            result = await self._simulated_annealing(
                objective,
                x0,
                n,
                max_time
            )
            
            return {
                'solution_vector': result['solution'].tolist(),
                'objective_value': result['objective'],
                'num_iterations': result['iterations']
            }
        else:
            # Fallback to random solution
            n = encoded_problem.get('num_qubits', 10)
            solution = np.random.randint(0, 2, n).tolist()
            
            return {
                'solution_vector': solution,
                'objective_value': float('inf'),
                'num_iterations': 0
            }
    
    async def _simulated_annealing(
        self,
        objective_func: Callable,
        x0: np.ndarray,
        n: int,
        max_time: Optional[float] = None
    ) -> Dict[str, Any]:
        """Simple simulated annealing implementation."""
        current = x0.copy()
        best = current.copy()
        current_obj = objective_func(current)
        best_obj = current_obj
        
        T = 1.0
        cooling_rate = 0.995
        iterations = 0
        max_iterations = 1000 if max_time is None else int(max_time * 100)
        
        for _ in range(max_iterations):
            # Generate neighbor
            neighbor = current.copy()
            flip_idx = np.random.randint(n)
            neighbor[flip_idx] = 1 - neighbor[flip_idx]
            
            # Calculate objective
            neighbor_obj = objective_func(neighbor)
            
            # Accept or reject
            delta = neighbor_obj - current_obj
            if delta < 0 or np.random.random() < np.exp(-delta / T):
                current = neighbor
                current_obj = neighbor_obj
                
                if current_obj < best_obj:
                    best = current.copy()
                    best_obj = current_obj
            
            # Cool down
            T *= cooling_rate
            iterations += 1
            
            # Small delay
            await asyncio.sleep(0.001)
        
        return {
            'solution': best,
            'objective': best_obj,
            'iterations': iterations
        }
    
    async def _local_search(
        self,
        initial_solution: List[int],
        encoded_problem: Dict[str, Any]
    ) -> Dict[str, Any]:
        """Perform local search around given solution."""
        if encoded_problem['encoding_type'] != 'qubo':
            return {
                'solution_vector': initial_solution,
                'objective_value': float('inf')
            }
        
        Q = np.array(encoded_problem['qubo_matrix'])
        current = np.array(initial_solution)
        current_obj = current.T @ Q @ current
        
        improved = True
        iterations = 0
        
        while improved and iterations < 100:
            improved = False
            
            # Try flipping each bit
            for i in range(len(current)):
                neighbor = current.copy()
                neighbor[i] = 1 - neighbor[i]
                neighbor_obj = neighbor.T @ Q @ neighbor
                
                if neighbor_obj < current_obj:
                    current = neighbor
                    current_obj = neighbor_obj
                    improved = True
                    break
            
            iterations += 1
            await asyncio.sleep(0.001)
        
        return {
            'solution_vector': current.tolist(),
            'objective_value': float(current_obj)
        }
    
    def _solution_to_vqe_params(
        self,
        solution: List[int],
        encoded_problem: Dict[str, Any]
    ) -> List[float]:
        """Convert binary solution to VQE initial parameters."""
        # Simple mapping: use solution to bias rotation angles
        params = []
        for bit in solution:
            # Map 0 -> -pi/4, 1 -> pi/4
            angle = (bit - 0.5) * np.pi / 2
            params.extend([angle, angle, 0])  # rx, ry, rz
        
        return params
    
    def _add_solution_bias(
        self,
        encoded_problem: Dict[str, Any],
        solution: List[int]
    ) -> Dict[str, Any]:
        """Add bias terms based on known solution."""
        problem = encoded_problem.copy()
        
        if problem['encoding_type'] == 'qubo':
            Q = np.array(problem['qubo_matrix'])
            
            # Add small bias to diagonal to favor known solution
            bias_strength = 0.1 * np.max(np.abs(Q))
            for i, bit in enumerate(solution):
                if bit == 1:
                    Q[i, i] -= bias_strength
                else:
                    Q[i, i] += bias_strength
            
            problem['qubo_matrix'] = Q.tolist()
        
        return problem
    
    def _create_guided_problem(
        self,
        encoded_problem: Dict[str, Any],
        current_solution: List[int]
    ) -> Dict[str, Any]:
        """Create problem guided by current solution."""
        # Add penalty terms to explore around current solution
        problem = encoded_problem.copy()
        
        if problem['encoding_type'] == 'qubo':
            Q = np.array(problem['qubo_matrix'])
            
            # Add exploration bonus for flipping bits
            exploration_strength = 0.05 * np.max(np.abs(Q))
            for i in range(len(current_solution)):
                Q[i, i] += exploration_strength * (2 * current_solution[i] - 1)
            
            problem['qubo_matrix'] = Q.tolist()
        
        return problem
    
    async def _subproblem_callback(
        self,
        subproblem_idx: int,
        iteration: int,
        solution: Dict[str, Any],
        metrics: Dict[str, Any],
        main_callback: Optional[Callable]
    ):
        """Callback wrapper for subproblem solving."""
        if main_callback:
            metrics['subproblem_idx'] = subproblem_idx
            await main_callback(iteration, solution, metrics)
    
    async def _refinement_callback(
        self,
        refinement_iter: int,
        phase: str,
        iteration: int,
        solution: Dict[str, Any],
        metrics: Dict[str, Any],
        main_callback: Optional[Callable]
    ):
        """Callback wrapper for iterative refinement."""
        if main_callback:
            metrics['refinement_iteration'] = refinement_iter
            metrics['phase'] = phase
            await main_callback(iteration, solution, metrics) 