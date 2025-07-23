"""
Quantum Problem Encoder

Converts various optimization problems into quantum-compatible formats.
"""

import numpy as np
from typing import Dict, List, Any, Optional, Tuple
import networkx as nx
from scipy.sparse import csr_matrix

from platformq_shared.logging_config import get_logger
from data_intelligence_common.utils.validators import validate_matrix_dimensions

logger = get_logger(__name__)


class QuantumProblemEncoder:
    """
    Encodes optimization problems into quantum-compatible formats (QUBO, Ising, etc.)
    """
    
    def __init__(self):
        self.encoding_strategies = {
            "qubo": self._encode_as_qubo,
            "ising": self._encode_as_ising,
            "max_cut": self._encode_max_cut,
            "tsp": self._encode_tsp,
            "portfolio": self._encode_portfolio,
            "knapsack": self._encode_knapsack,
            "vertex_cover": self._encode_vertex_cover,
            "scheduling": self._encode_scheduling,
            "resource_allocation": self._encode_resource_allocation,
            "design_optimization": self._encode_design_optimization,
            "generic": self._encode_generic
        }
    
    async def encode(
        self,
        problem_type: str,
        objective_function: Dict[str, Any],
        constraints: List[Dict[str, Any]],
        variables: Dict[str, Any],
        metadata: Optional[Dict[str, Any]] = None
    ) -> Dict[str, Any]:
        """
        Encode problem into quantum format.
        
        Args:
            problem_type: Type of optimization problem
            objective_function: Objective function specification
            constraints: List of constraints
            variables: Variable definitions
            metadata: Additional problem metadata
            
        Returns:
            Encoded problem dictionary
        """
        logger.info(f"Encoding '{problem_type}' problem with {len(variables)} variables")
        
        # Get encoding strategy
        encode_func = self.encoding_strategies.get(problem_type)
        if not encode_func:
            raise ValueError(f"Unsupported problem type: {problem_type}")
        
        # Prepare problem data
        problem_data = {
            "objective_function": objective_function,
            "constraints": constraints,
            "variables": variables,
            "metadata": metadata or {}
        }
        
        # Encode problem
        encoded = encode_func(problem_data)
        
        # Add common metadata
        encoded["problem_type"] = problem_type
        encoded["num_variables"] = len(variables)
        encoded["num_constraints"] = len(constraints)
        encoded["encoding_metadata"] = {
            "encoder_version": "2.0",
            "encoding_time": "utc_timestamp"
        }
        
        return encoded
    
    def _encode_as_qubo(self, problem_data: Dict[str, Any]) -> Dict[str, Any]:
        """
        Encode problem as Quadratic Unconstrained Binary Optimization (QUBO).
        """
        variables = problem_data["variables"]
        objective = problem_data["objective_function"]
        constraints = problem_data["constraints"]
        
        num_vars = len(variables)
        
        # Initialize QUBO matrix
        Q = np.zeros((num_vars, num_vars))
        
        # Add objective function terms
        if "linear" in objective:
            for i, coeff in enumerate(objective["linear"]):
                Q[i, i] += coeff
        
        if "quadratic" in objective:
            quad_matrix = np.array(objective["quadratic"])
            Q += quad_matrix
        
        # Add constraint penalties
        penalty_weight = self._calculate_penalty_weight(objective, constraints)
        
        for constraint in constraints:
            Q += self._encode_constraint_as_penalty(
                constraint,
                num_vars,
                penalty_weight
            )
        
        # Make symmetric
        Q = (Q + Q.T) / 2
        
        return {
            "encoding_type": "qubo",
            "qubo_matrix": Q.tolist(),
            "num_qubits": num_vars,
            "variable_mapping": {
                var_name: idx
                for idx, var_name in enumerate(variables.keys())
            },
            "penalty_weight": penalty_weight
        }
    
    def _encode_as_ising(self, problem_data: Dict[str, Any]) -> Dict[str, Any]:
        """
        Encode problem as Ising model.
        """
        # First encode as QUBO
        qubo_encoding = self._encode_as_qubo(problem_data)
        Q = np.array(qubo_encoding["qubo_matrix"])
        
        # Convert QUBO to Ising
        h, J, offset = self._qubo_to_ising(Q)
        
        return {
            "encoding_type": "ising",
            "linear_terms": h.tolist(),
            "quadratic_terms": J.tolist(),
            "offset": float(offset),
            "num_qubits": len(h),
            "variable_mapping": qubo_encoding["variable_mapping"]
        }
    
    def _encode_max_cut(self, problem_data: Dict[str, Any]) -> Dict[str, Any]:
        """
        Encode Max-Cut problem.
        """
        metadata = problem_data["metadata"]
        
        # Extract graph
        if "adjacency_matrix" in metadata:
            adj_matrix = np.array(metadata["adjacency_matrix"])
            n = adj_matrix.shape[0]
        elif "edges" in metadata:
            edges = metadata["edges"]
            n = metadata.get("num_vertices", max(max(e) for e in edges) + 1)
            adj_matrix = np.zeros((n, n))
            for i, j in edges:
                adj_matrix[i, j] = 1
                adj_matrix[j, i] = 1
        else:
            raise ValueError("Max-Cut requires adjacency_matrix or edges in metadata")
        
        # Create QUBO for Max-Cut
        Q = np.zeros((n, n))
        
        for i in range(n):
            for j in range(i + 1, n):
                if adj_matrix[i, j] > 0:
                    # Reward if vertices are in different partitions
                    Q[i, i] += 0.5 * adj_matrix[i, j]
                    Q[j, j] += 0.5 * adj_matrix[i, j]
                    Q[i, j] -= adj_matrix[i, j]
                    Q[j, i] -= adj_matrix[i, j]
        
        return {
            "encoding_type": "qubo",
            "qubo_matrix": Q.tolist(),
            "num_qubits": n,
            "graph_info": {
                "num_vertices": n,
                "num_edges": int(np.sum(adj_matrix) / 2)
            }
        }
    
    def _encode_tsp(self, problem_data: Dict[str, Any]) -> Dict[str, Any]:
        """
        Encode Traveling Salesman Problem.
        """
        metadata = problem_data["metadata"]
        distances = np.array(metadata.get("distance_matrix", []))
        n_cities = distances.shape[0]
        
        # Binary variables x_ij: city i visited at time j
        n_vars = n_cities * n_cities
        Q = np.zeros((n_vars, n_vars))
        
        # Penalty for constraint violations
        penalty = np.max(distances) * n_cities * 10
        
        # Objective: minimize total distance
        for i in range(n_cities):
            for j in range(n_cities):
                if i != j:
                    for t in range(n_cities - 1):
                        idx1 = i * n_cities + t
                        idx2 = j * n_cities + (t + 1)
                        Q[idx1, idx2] += distances[i, j]
        
        # Constraint: each city visited exactly once
        for i in range(n_cities):
            # City must be visited
            for t1 in range(n_cities):
                idx1 = i * n_cities + t1
                Q[idx1, idx1] -= penalty
                
                # City cannot be visited twice
                for t2 in range(t1 + 1, n_cities):
                    idx2 = i * n_cities + t2
                    Q[idx1, idx2] += 2 * penalty
                    Q[idx2, idx1] = Q[idx1, idx2]
        
        # Constraint: each time slot has exactly one city
        for t in range(n_cities):
            for i1 in range(n_cities):
                idx1 = i1 * n_cities + t
                Q[idx1, idx1] -= penalty
                
                for i2 in range(i1 + 1, n_cities):
                    idx2 = i2 * n_cities + t
                    Q[idx1, idx2] += 2 * penalty
                    Q[idx2, idx1] = Q[idx1, idx2]
        
        return {
            "encoding_type": "qubo",
            "qubo_matrix": Q.tolist(),
            "num_qubits": n_vars,
            "variable_mapping": {
                f"x_{i}_{t}": i * n_cities + t
                for i in range(n_cities)
                for t in range(n_cities)
            },
            "problem_metadata": {
                "num_cities": n_cities,
                "constraint_penalty": penalty
            }
        }
    
    def _encode_portfolio(self, problem_data: Dict[str, Any]) -> Dict[str, Any]:
        """
        Encode portfolio optimization problem.
        """
        metadata = problem_data["metadata"]
        returns = np.array(metadata.get("expected_returns", []))
        covariance = np.array(metadata.get("covariance_matrix", []))
        risk_aversion = metadata.get("risk_aversion", 0.5)
        
        n_assets = len(returns)
        
        # For discrete portfolio (binary selection)
        Q = np.zeros((n_assets, n_assets))
        
        # Objective: maximize returns - risk_aversion * variance
        for i in range(n_assets):
            Q[i, i] -= returns[i]  # Negative for maximization
            
            for j in range(n_assets):
                Q[i, j] += risk_aversion * covariance[i, j]
        
        # Budget constraint (if provided)
        if "budget_constraint" in metadata:
            budget = metadata["budget_constraint"]
            costs = metadata.get("asset_costs", np.ones(n_assets))
            penalty = np.max(np.abs(returns)) * 10
            
            # Add penalty for budget violation
            for i in range(n_assets):
                for j in range(n_assets):
                    Q[i, j] += penalty * costs[i] * costs[j] / (budget ** 2)
        
        return {
            "encoding_type": "qubo",
            "qubo_matrix": Q.tolist(),
            "num_qubits": n_assets,
            "variable_mapping": {
                f"asset_{i}": i for i in range(n_assets)
            },
            "portfolio_params": {
                "risk_aversion": risk_aversion,
                "num_assets": n_assets
            }
        }
    
    def _encode_knapsack(self, problem_data: Dict[str, Any]) -> Dict[str, Any]:
        """
        Encode knapsack problem.
        """
        metadata = problem_data["metadata"]
        values = np.array(metadata.get("values", []))
        weights = np.array(metadata.get("weights", []))
        capacity = metadata.get("capacity", 0)
        
        n_items = len(values)
        
        # Penalty for constraint violation
        penalty = np.sum(values) * 2
        
        # QUBO matrix
        Q = np.zeros((n_items, n_items))
        
        # Objective: maximize value (minimize negative value)
        for i in range(n_items):
            Q[i, i] -= values[i]
        
        # Weight constraint with slack variables
        n_slack = int(np.ceil(np.log2(capacity + 1)))
        n_total = n_items + n_slack
        
        Q_full = np.zeros((n_total, n_total))
        Q_full[:n_items, :n_items] = Q
        
        # Add penalty terms for weight constraint
        for i in range(n_items):
            for j in range(n_items):
                Q_full[i, j] += penalty * weights[i] * weights[j] / (capacity ** 2)
        
        # Slack variable terms
        for k in range(n_slack):
            slack_weight = 2 ** k
            Q_full[n_items + k, n_items + k] -= penalty * slack_weight ** 2 / (capacity ** 2)
            
            for i in range(n_items):
                Q_full[i, n_items + k] += 2 * penalty * weights[i] * slack_weight / (capacity ** 2)
                Q_full[n_items + k, i] = Q_full[i, n_items + k]
        
        return {
            "encoding_type": "qubo",
            "qubo_matrix": Q_full.tolist(),
            "num_qubits": n_total,
            "variable_mapping": {
                **{f"item_{i}": i for i in range(n_items)},
                **{f"slack_{k}": n_items + k for k in range(n_slack)}
            },
            "problem_metadata": {
                "num_items": n_items,
                "capacity": capacity,
                "penalty": penalty
            }
        }
    
    def _encode_vertex_cover(self, problem_data: Dict[str, Any]) -> Dict[str, Any]:
        """
        Encode vertex cover problem.
        """
        metadata = problem_data["metadata"]
        
        if "adjacency_matrix" in metadata:
            adj_matrix = np.array(metadata["adjacency_matrix"])
            n_vertices = adj_matrix.shape[0]
            edges = [(i, j) for i in range(n_vertices) for j in range(i+1, n_vertices) if adj_matrix[i, j] > 0]
        else:
            edges = metadata.get("edges", [])
            n_vertices = metadata.get("num_vertices", max(max(e) for e in edges) + 1)
        
        penalty = n_vertices * 2
        
        # QUBO matrix
        Q = np.zeros((n_vertices, n_vertices))
        
        # Objective: minimize number of vertices
        for i in range(n_vertices):
            Q[i, i] = 1
        
        # Constraint: each edge must be covered
        for u, v in edges:
            Q[u, u] -= penalty
            Q[v, v] -= penalty
            Q[u, v] += penalty
            Q[v, u] += penalty
        
        return {
            "encoding_type": "qubo",
            "qubo_matrix": Q.tolist(),
            "num_qubits": n_vertices,
            "graph_info": {
                "num_vertices": n_vertices,
                "num_edges": len(edges)
            }
        }
    
    def _encode_scheduling(self, problem_data: Dict[str, Any]) -> Dict[str, Any]:
        """
        Encode job scheduling problem.
        """
        metadata = problem_data["metadata"]
        jobs = metadata.get("jobs", [])
        machines = metadata.get("machines", [])
        processing_times = np.array(metadata.get("processing_times", []))
        
        n_jobs = len(jobs)
        n_machines = len(machines)
        n_time_slots = metadata.get("time_horizon", n_jobs)
        
        # Variables: x_ijt = 1 if job i on machine j at time t
        n_vars = n_jobs * n_machines * n_time_slots
        Q = np.zeros((n_vars, n_vars))
        
        penalty = np.max(processing_times) * n_jobs * 10
        
        # Constraint: each job scheduled exactly once
        for i in range(n_jobs):
            for j1 in range(n_machines):
                for t1 in range(n_time_slots):
                    idx1 = i * n_machines * n_time_slots + j1 * n_time_slots + t1
                    Q[idx1, idx1] -= penalty
                    
                    for j2 in range(n_machines):
                        for t2 in range(n_time_slots):
                            if j1 != j2 or t1 != t2:
                                idx2 = i * n_machines * n_time_slots + j2 * n_time_slots + t2
                                Q[idx1, idx2] += penalty
        
        # Machine capacity constraints
        for j in range(n_machines):
            for t in range(n_time_slots):
                for i1 in range(n_jobs):
                    for i2 in range(i1 + 1, n_jobs):
                        idx1 = i1 * n_machines * n_time_slots + j * n_time_slots + t
                        idx2 = i2 * n_machines * n_time_slots + j * n_time_slots + t
                        Q[idx1, idx2] += penalty
                        Q[idx2, idx1] += penalty
        
        return {
            "encoding_type": "qubo",
            "qubo_matrix": Q.tolist(),
            "num_qubits": n_vars,
            "variable_mapping": {
                f"x_{i}_{j}_{t}": i * n_machines * n_time_slots + j * n_time_slots + t
                for i in range(n_jobs)
                for j in range(n_machines)
                for t in range(n_time_slots)
            },
            "problem_metadata": {
                "num_jobs": n_jobs,
                "num_machines": n_machines,
                "num_time_slots": n_time_slots
            }
        }
    
    def _encode_resource_allocation(self, problem_data: Dict[str, Any]) -> Dict[str, Any]:
        """
        Encode resource allocation problem.
        """
        metadata = problem_data["metadata"]
        resources = metadata.get("resources", [])
        tasks = metadata.get("tasks", [])
        costs = np.array(metadata.get("cost_matrix", []))
        
        n_resources = len(resources)
        n_tasks = len(tasks)
        n_vars = n_resources * n_tasks
        
        Q = np.zeros((n_vars, n_vars))
        
        # Objective: minimize cost
        for i in range(n_resources):
            for j in range(n_tasks):
                var_idx = i * n_tasks + j
                if i < costs.shape[0] and j < costs.shape[1]:
                    Q[var_idx, var_idx] = costs[i, j]
        
        # Constraint: each task assigned to exactly one resource
        penalty = np.max(costs) * 10
        
        for j in range(n_tasks):
            for i1 in range(n_resources):
                idx1 = i1 * n_tasks + j
                Q[idx1, idx1] -= penalty
                
                for i2 in range(i1 + 1, n_resources):
                    idx2 = i2 * n_tasks + j
                    Q[idx1, idx2] += 2 * penalty
                    Q[idx2, idx1] += 2 * penalty
        
        # Resource capacity constraints (if provided)
        if "resource_capacity" in metadata:
            capacities = metadata["resource_capacity"]
            task_demands = metadata.get("task_demands", [1] * n_tasks)
            
            for i in range(n_resources):
                for j1 in range(n_tasks):
                    for j2 in range(j1 + 1, n_tasks):
                        idx1 = i * n_tasks + j1
                        idx2 = i * n_tasks + j2
                        
                        if task_demands[j1] + task_demands[j2] > capacities[i]:
                            Q[idx1, idx2] += penalty / 2
                            Q[idx2, idx1] += penalty / 2
        
        return {
            "encoding_type": "qubo",
            "qubo_matrix": Q.tolist(),
            "num_qubits": n_vars,
            "variable_mapping": {
                f"x_{i}_{j}": i * n_tasks + j
                for i in range(n_resources)
                for j in range(n_tasks)
            },
            "problem_metadata": {
                "num_resources": n_resources,
                "num_tasks": n_tasks
            }
        }
    
    def _encode_design_optimization(self, problem_data: Dict[str, Any]) -> Dict[str, Any]:
        """
        Encode design parameter optimization for VQE.
        """
        variables = problem_data["variables"]
        objective = problem_data["objective_function"]
        constraints = problem_data["constraints"]
        metadata = problem_data["metadata"]
        
        n_params = len(variables)
        n_qubits = max(4, int(np.ceil(np.log2(n_params))))
        
        # Create Hamiltonian terms
        hamiltonian_terms = []
        
        # Linear terms
        if "linear" in objective:
            for i, coeff in enumerate(objective["linear"]):
                if abs(coeff) > 1e-10:
                    pauli_string = 'I' * i + 'Z' + 'I' * (n_qubits - i - 1)
                    hamiltonian_terms.append({
                        "pauli": pauli_string,
                        "coefficient": float(coeff)
                    })
        
        # Quadratic terms
        if "quadratic" in objective:
            quad_matrix = np.array(objective["quadratic"])
            for i in range(n_params):
                for j in range(i, n_params):
                    if abs(quad_matrix[i, j]) > 1e-10:
                        if i == j:
                            pauli_string = 'I' * i + 'Z' + 'I' * (n_qubits - i - 1)
                        else:
                            pauli_string = ['I'] * n_qubits
                            pauli_string[i] = 'Z'
                            pauli_string[j] = 'Z'
                            pauli_string = ''.join(pauli_string)
                        
                        hamiltonian_terms.append({
                            "pauli": pauli_string,
                            "coefficient": float(quad_matrix[i, j])
                        })
        
        return {
            "encoding_type": "vqe",
            "num_qubits": n_qubits,
            "hamiltonian_terms": hamiltonian_terms,
            "ansatz_type": "hardware_efficient",
            "vqe_depth": metadata.get("circuit_depth", 3),
            "constraints": self._encode_vqe_constraints(constraints, n_qubits),
            "bounds": metadata.get("bounds", [])
        }
    
    def _encode_generic(self, problem_data: Dict[str, Any]) -> Dict[str, Any]:
        """
        Generic encoding for custom problems.
        """
        metadata = problem_data.get("metadata", {})
        
        if "qubo_matrix" in metadata:
            # Already in QUBO format
            Q = np.array(metadata["qubo_matrix"])
            n_vars = Q.shape[0]
            
            return {
                "encoding_type": "qubo",
                "qubo_matrix": Q.tolist(),
                "num_qubits": n_vars
            }
        
        elif "hamiltonian" in metadata:
            # Hamiltonian format
            return {
                "encoding_type": "hamiltonian",
                "hamiltonian_terms": metadata["hamiltonian"],
                "num_qubits": metadata.get("num_qubits", 4)
            }
        
        else:
            # Default: create from objective and constraints
            return self._encode_as_qubo(problem_data)
    
    def _calculate_penalty_weight(
        self,
        objective: Dict[str, Any],
        constraints: List[Dict[str, Any]]
    ) -> float:
        """Calculate appropriate penalty weight for constraints."""
        # Get maximum coefficient from objective
        max_obj_coeff = 0
        
        if "linear" in objective:
            max_obj_coeff = max(max_obj_coeff, max(abs(c) for c in objective["linear"]))
        
        if "quadratic" in objective:
            quad = np.array(objective["quadratic"])
            max_obj_coeff = max(max_obj_coeff, np.max(np.abs(quad)))
        
        # Penalty should be larger than objective coefficients
        return max(max_obj_coeff * 10, 100)
    
    def _encode_constraint_as_penalty(
        self,
        constraint: Dict[str, Any],
        num_vars: int,
        penalty_weight: float
    ) -> np.ndarray:
        """Convert constraint to penalty term in QUBO."""
        Q = np.zeros((num_vars, num_vars))
        
        constraint_type = constraint.get("type", "equality")
        terms = constraint.get("terms", [])
        rhs = constraint.get("rhs", 0)
        
        if constraint_type == "equality":
            # (sum - rhs)^2 penalty
            for i, coeff_i in enumerate(terms):
                Q[i, i] += penalty_weight * (coeff_i ** 2)
                
                for j, coeff_j in enumerate(terms):
                    if i != j:
                        Q[i, j] += penalty_weight * coeff_i * coeff_j
                
                # Linear term from -2*rhs*sum
                Q[i, i] -= 2 * penalty_weight * coeff_i * rhs
        
        elif constraint_type == "inequality":
            # For inequalities, we'd need slack variables
            # Simplified version here
            logger.warning("Inequality constraints require slack variables - using soft penalty")
            
            for i, coeff in enumerate(terms):
                Q[i, i] += penalty_weight * coeff if sum(terms) > rhs else 0
        
        return Q
    
    def _qubo_to_ising(self, Q: np.ndarray) -> Tuple[np.ndarray, np.ndarray, float]:
        """Convert QUBO matrix to Ising model (h, J, offset)."""
        n = Q.shape[0]
        h = np.zeros(n)
        J = np.zeros((n, n))
        offset = 0
        
        for i in range(n):
            h[i] = Q[i, i] / 2
            offset += Q[i, i] / 4
            
            for j in range(i + 1, n):
                J[i, j] = Q[i, j] / 4
                h[i] += Q[i, j] / 4
                h[j] += Q[i, j] / 4
                offset += Q[i, j] / 4
        
        return h, J, offset
    
    def _encode_vqe_constraints(
        self,
        constraints: List[Dict[str, Any]],
        num_qubits: int
    ) -> List[Dict[str, Any]]:
        """Encode constraints for VQE."""
        encoded_constraints = []
        
        for constraint in constraints:
            if constraint["type"] == "equality":
                encoded_constraints.append({
                    "type": "penalty",
                    "terms": constraint.get("terms", []),
                    "penalty_weight": 1000.0
                })
            elif constraint["type"] == "inequality":
                encoded_constraints.append({
                    "type": "inequality",
                    "terms": constraint.get("terms", []),
                    "bound": constraint.get("rhs", 0),
                    "penalty_weight": 1000.0
                })
        
        return encoded_constraints 