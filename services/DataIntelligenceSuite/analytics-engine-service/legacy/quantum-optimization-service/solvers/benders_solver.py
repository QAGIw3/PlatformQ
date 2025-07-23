"""
Benders Decomposition Solver Engine for Facility Location Problem.
"""
from typing import Dict, Any, List, Tuple
import time
import numpy as np

from platformq_shared.logging_config import get_logger
from services.quantum_optimization_service.app.solvers.classical_solvers import ClassicalSolvers
from services.quantum_optimization_service.app.engines.quantum_optimizer import QuantumOptimizer
from .base import Solver
from .quantum_solver import QuantumSolver

logger = get_logger(__name__)

class BendersSolver(Solver):
    """
    Implements an iterative Benders decomposition loop for the
    Uncapacitated Facility Location Problem (UFLP).
    """

    def __init__(self, max_iter=20, tolerance=1e-4):
        self.max_iter = max_iter
        self.tolerance = tolerance
        self.classical_solvers = ClassicalSolvers()
        self.quantum_optimizer = None
        logger.info("Initialized BendersSolver.")

    def solve(self,
            problem_data: Dict[str, Any],
            **kwargs) -> Dict[str, Any]:
        """
        Executes the Benders Decomposition algorithm.

        Args:
            problem_data: The facility location problem data.
            **kwargs: May contain 'quantum_engine' and 'quantum_backend'.

        Returns:
            A dictionary containing the optimal facility locations and total cost.
        """
        self.quantum_optimizer = QuantumSolver(
            engine=kwargs.get("quantum_engine", "qiskit"),
            backend_name=kwargs.get("quantum_backend", "aer_simulator")
        )

        master_problem_result = self._solve_master_problem_classical(problem_data)
        
        for i in range(self.max_iter):
            logger.info(f"--- Benders Iteration {i+1}/{self.max_iter} ---")
            
            # 1. Solve the Master Problem
            master_solution = self._solve_master_problem(opening_costs, num_customers, self.benders_cuts)
            if not master_solution['success']:
                logger.error("Master problem failed to solve.")
                break
            
            open_facilities_indices = np.where(master_solution['open_facilities'] == 1)[0]
            lower_bound = master_solution['optimal_value']
            
            # 2. Solve the Quantum Sub-problem (Customer Assignment)
            sub_problem_solution, assignment = self._solve_quantum_sub_problem(
                open_facilities_indices, transportation_costs
            )
            
            current_total_cost = np.sum(opening_costs[open_facilities_indices]) + sub_problem_solution['best_energy']
            
            if current_total_cost < upper_bound:
                upper_bound = current_total_cost
                best_solution_so_far = {
                    'open_facilities': master_solution['open_facilities'],
                    'assignment': assignment,
                    'total_cost': upper_bound
                }
            
            logger.info(f"Iteration {i+1}: Lower Bound = {lower_bound:.2f}, Upper Bound = {upper_bound:.2f}")

            # 3. Check for convergence and generate cut
            if upper_bound - lower_bound <= self.tolerance:
                logger.info("Benders loop converged.")
                break
            
            new_cut = self._generate_benders_cut(sub_problem_solution, open_facilities_indices, num_facilities)
            self.benders_cuts.append(new_cut)
            
        total_duration = time.time() - start_time
        return {
            "status": "CONVERGED" if upper_bound - lower_bound <= self.tolerance else "MAX_ITERATIONS_REACHED",
            "solution": best_solution_so_far,
            "num_iterations": i + 1,
            "total_duration": total_duration
        }

    def _solve_master_problem(self, opening_costs: np.ndarray, num_customers: int, cuts: List) -> Dict:
        """
        Solves the MILP master problem.
        Objective: min(sum(f_i * y_i) + theta)
        """
        num_facilities = len(opening_costs)
        # Objective function: c * x, where x = [y_1, ..., y_n, theta]
        c = np.append(opening_costs, 1)

        # Constraints
        A_ub = []
        b_ub = []

        # Add Benders cuts
        for cut in cuts:
            # cut['coeffs'] has length num_facilities + 1 (for theta)
            A_ub.append(cut['coeffs'])
            b_ub.append(cut['rhs'])

        bounds = [(0, 1)] * num_facilities + [(0, None)] # y_i are binary, theta is non-negative

        # Scipy's linprog is for continuous variables, but we use it for the relaxed master problem.
        # For a true MILP, one would use a more advanced solver.
        result = self.classical_solver.solve_linear_program({
            'c': c,
            'A_ub': A_ub if A_ub else None,
            'b_ub': b_ub if b_ub else None,
            'bounds': bounds
        })
        
        if result['success']:
            # Round the y_i variables to get a feasible integer solution
            open_facilities = np.round(result['solution_vector'][:-1]).astype(int)
            return {
                "success": True,
                "open_facilities": open_facilities,
                "optimal_value": result['optimal_value']
            }
        return {"success": False}

    def _solve_quantum_sub_problem(self, open_facilities_idx: np.ndarray, transportation_costs: np.ndarray) -> Tuple[Dict, Dict]:
        """
        Solves the customer assignment sub-problem for a fixed set of open facilities
        by formulating it as a QUBO and solving with a quantum annealer.
        """
        num_open_facilities = len(open_facilities_idx)
        num_customers = transportation_costs.shape[1]
        
        if num_open_facilities == 0:
            # Penalize opening no facilities heavily
            return {'best_energy': 1e9, 'duals': [1e9] * num_customers}, {}

        # Prepare the problem for the encoder
        subproblem_data = {
            'open_facilities_indices': open_facilities_idx,
            'transportation_costs': transportation_costs
        }

        # Solve the QUBO using the quantum optimizer
        # The 'quantum_annealing' algorithm is selected as it directly handles QUBOs
        # via its new 'solve' method.
        logger.info("Calling Quantum Optimizer for facility location sub-problem (QUBO).")
        solution_result = self.quantum_optimizer.solve(
            problem_type="facility_location_subproblem",
            problem_data=subproblem_data,
            algorithm_name="quantum_annealing",
            algorithm_params={'num_reads': 100} # Use a reasonable number of reads
        )

        # Decode the solution
        assignment_vector = solution_result['best_solution']
        assignment = {}
        total_min_cost = 0
        duals = [0.0] * num_customers

        if not assignment_vector:
            logger.warning("Quantum sub-problem returned no solution.")
            # Handle case with no solution by returning a high cost
            return {'best_energy': 1e9, 'duals': [1e9] * num_customers}, {}

        for j in range(num_customers):
            assigned = False
            for k in range(num_open_facilities):
                var_idx = j * num_open_facilities + k
                if var_idx < len(assignment_vector) and assignment_vector[var_idx] == 1:
                    global_facility_idx = open_facilities_idx[k]
                    assignment[j] = global_facility_idx
                    cost = transportation_costs[global_facility_idx, j]
                    total_min_cost += cost
                    duals[j] = cost # The dual is the cost of the assignment
                    assigned = True
                    break
            if not assigned:
                # If constraint wasn't met, assign to cheapest option and add penalty cost
                # This helps guide the master problem away from poor choices
                cheapest_cost = np.min(transportation_costs[open_facilities_idx, j])
                total_min_cost += cheapest_cost
                duals[j] = cheapest_cost
                logger.warning(f"Customer {j} was not assigned by QUBO. Assigning to cheapest option.")
        
        logger.info(f"Quantum sub-problem solved. Min assignment cost: {total_min_cost}")
        
        return {'best_energy': total_min_cost, 'duals': duals}, assignment

    def _generate_benders_cut(self, sub_problem_solution: Dict, open_facilities_idx: np.ndarray, num_facilities: int) -> Dict:
        """
        Generates an optimality cut from the sub-problem solution.
        The cut is of the form: theta >= sum(alpha_j) - sum(alpha_j * y_i)
        """
        duals = sub_problem_solution['duals'] # These are the costs from the subproblem
        
        # Cut form: theta - sum_j(dual_j * (1 - sum_{i in I_k} y_i)) >= cost_k
        # Simplified cut: theta >= cost_k + sum_{i not in I_k} penalty_i * y_i
        
        # We generate a simple optimality cut:
        # theta >= sum_j(dual_j) + sum_{i in I_k}(y_i - 1) * M
        # where M is a large number.
        # A more standard cut is theta >= cost_k + sum_j(min_cost_j_if_i_closed * (1-y_i))
        
        # coeffs for [y_1, ..., y_n, theta]
        coeffs = np.zeros(num_facilities + 1)
        coeffs[-1] = -1 # for -theta

        total_dual_cost = np.sum(duals)
        
        for i in open_facilities_idx:
            # This is a simplified cut logic placeholder. A real implementation
            # would require more complex dual variable calculations.
            coeffs[i] = total_dual_cost / len(open_facilities_idx)

        rhs = -total_dual_cost
        
        # Constraint form: sum(c_i * y_i) - theta <= -total_dual_cost
        # Or: theta >= total_dual_cost + sum(c_i * y_i)
        
        return {
            'coeffs': coeffs.tolist(),
            'rhs': rhs
        } 