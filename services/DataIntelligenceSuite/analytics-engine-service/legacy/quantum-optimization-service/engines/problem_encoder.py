"""
Problem Encoder for Quantum Optimization

Converts various optimization problems into quantum-compatible formats.
"""

import numpy as np
from typing import Dict, List, Tuple, Optional, Any
from enum import Enum
import networkx as nx
from scipy.sparse import csr_matrix

from platformq_shared.logging_config import get_logger

logger = get_logger(__name__)


class ProblemEncoder:
    """
    Encodes optimization problems into quantum-compatible formats
    """
    
    def encode(self, problem_type: str, problem_data: Dict) -> Dict:
        """
        Encode problem into quantum format
        
        Args:
            problem_type: Type of optimization problem
            problem_data: Problem specification
            
        Returns:
            Encoded problem dictionary
        """
        logger.info(f"Encoding '{problem_type}' problem")
        
        if problem_type == "resource_allocation":
            return self._encode_resource_allocation(problem_data)
        elif problem_type == "route_optimization":
            return self._encode_route_optimization(problem_data)
        elif problem_type == "portfolio":
            return self._encode_portfolio(problem_data)
        elif problem_type == "design_parameters":
            return self._encode_design_parameters(problem_data)
        elif problem_type == "max_cut":
            return self._encode_max_cut(problem_data)
        elif problem_type == "knapsack":
            return self._encode_knapsack(problem_data)
        elif problem_type == "vertex_cover":
            return self._encode_vertex_cover(problem_data)
        elif problem_type == "scheduling":
            return self._encode_scheduling(problem_data)
        elif problem_type == "generic":
            return self._encode_generic(problem_data)
        elif problem_type == "qubo":
            return problem_data # Pass through for algorithms that handle QUBOs directly
        elif problem_type == "facility_location_subproblem":
            return self._encode_facility_location_subproblem(problem_data)
        else:
            raise ValueError(f"Unsupported problem type: {problem_type}")
        
    def _encode_resource_allocation(self, problem_data: Dict) -> Dict:
        """
        Encode resource allocation problem as QUBO
        
        Problem: Assign resources to tasks to minimize cost
        Variables: x_ij = 1 if resource i is assigned to task j
        """
        resources = problem_data.get('resources', [])
        tasks = problem_data.get('tasks', [])
        costs = np.array(problem_data.get('costs', []))
        
        num_resources = len(resources)
        num_tasks = len(tasks)
        num_vars = num_resources * num_tasks
        
        # Create QUBO matrix
        Q = np.zeros((num_vars, num_vars))
        
        # Objective: minimize cost
        for i in range(num_resources):
            for j in range(num_tasks):
                var_idx = i * num_tasks + j
                Q[var_idx, var_idx] = costs[i, j] if i < costs.shape[0] and j < costs.shape[1] else 0
                
        # Constraint: each task must be assigned to exactly one resource
        penalty = np.max(costs) * 10  # Large penalty
        
        for j in range(num_tasks):
            # Add penalty for not assigning task j
            for i1 in range(num_resources):
                idx1 = i1 * num_tasks + j
                Q[idx1, idx1] -= penalty
                
                # Add penalty for assigning task j to multiple resources
                for i2 in range(i1 + 1, num_resources):
                    idx2 = i2 * num_tasks + j
                    Q[idx1, idx2] += 2 * penalty
                    Q[idx2, idx1] += 2 * penalty
                    
        # Constraint: resource capacity (if provided)
        if 'resource_capacity' in problem_data:
            capacities = problem_data['resource_capacity']
            task_demands = problem_data.get('task_demands', [1] * num_tasks)
            
            for i in range(num_resources):
                # Soft constraint for capacity
                for j1 in range(num_tasks):
                    for j2 in range(j1 + 1, num_tasks):
                        idx1 = i * num_tasks + j1
                        idx2 = i * num_tasks + j2
                        
                        # Penalty if total demand exceeds capacity
                        if task_demands[j1] + task_demands[j2] > capacities[i]:
                            Q[idx1, idx2] += penalty / 2
                            Q[idx2, idx1] += penalty / 2
                            
        return {
            'encoding_type': 'qubo',
            'qubo_matrix': Q.tolist(),
            'num_variables': num_vars,
            'num_qubits': int(np.ceil(np.log2(num_vars))),
            'variable_mapping': {
                f'x_{i}_{j}': i * num_tasks + j
                for i in range(num_resources)
                for j in range(num_tasks)
            },
            'metadata': {
                'num_resources': num_resources,
                'num_tasks': num_tasks,
                'problem_type': 'resource_allocation'
            }
        }
        
    def _encode_route_optimization(self, problem_data: Dict) -> Dict:
        """
        Encode TSP/VRP as QUBO
        
        Problem: Find shortest route visiting all cities
        Variables: x_ij = 1 if city i is visited at time j
        """
        num_cities = problem_data.get('cities', 0)
        distances = np.array(problem_data.get('distances', []))
        
        if isinstance(num_cities, int) and distances.size == 0:
            # Generate random distances for testing
            distances = np.random.rand(num_cities, num_cities)
            distances = (distances + distances.T) / 2  # Symmetric
            np.fill_diagonal(distances, 0)
            
        num_vars = num_cities * num_cities
        
        # QUBO formulation
        Q = np.zeros((num_vars, num_vars))
        
        # Penalty parameters
        A = np.max(distances) * num_cities  # Constraint penalty
        
        # Objective: minimize total distance
        for i in range(num_cities):
            for j in range(num_cities):
                if i != j:
                    for t in range(num_cities - 1):
                        idx1 = i * num_cities + t
                        idx2 = j * num_cities + (t + 1)
                        Q[idx1, idx2] += distances[i, j]
                        
        # Constraint 1: Each city visited exactly once
        for i in range(num_cities):
            # City i must be visited
            for t1 in range(num_cities):
                idx1 = i * num_cities + t1
                Q[idx1, idx1] -= A
                
                # City i cannot be visited twice
                for t2 in range(t1 + 1, num_cities):
                    idx2 = i * num_cities + t2
                    Q[idx1, idx2] += 2 * A
                    Q[idx2, idx1] = Q[idx1, idx2]
                    
        # Constraint 2: Each time slot has exactly one city
        for t in range(num_cities):
            # Time t must have a city
            for i1 in range(num_cities):
                idx1 = i1 * num_cities + t
                Q[idx1, idx1] -= A
                
                # Time t cannot have multiple cities
                for i2 in range(i1 + 1, num_cities):
                    idx2 = i2 * num_cities + t
                    Q[idx1, idx2] += 2 * A
                    Q[idx2, idx1] = Q[idx1, idx2]
                    
        return {
            'encoding_type': 'qubo',
            'qubo_matrix': Q.tolist(),
            'num_variables': num_vars,
            'num_qubits': num_vars,  # One qubit per variable for TSP
            'variable_mapping': {
                f'x_{i}_{t}': i * num_cities + t
                for i in range(num_cities)
                for t in range(num_cities)
            },
            'metadata': {
                'num_cities': num_cities,
                'problem_type': 'tsp',
                'constraint_penalty': A
            }
        }
        
    def _encode_portfolio(self, problem_data: Dict) -> Dict:
        """
        Encode portfolio optimization using amplitude estimation
        
        Problem: Optimize risk/return tradeoff
        """
        assets = problem_data.get('assets', [])
        returns = np.array(problem_data.get('expected_returns', []))
        covariance = np.array(problem_data.get('covariance_matrix', []))
        risk_aversion = problem_data.get('risk_aversion', 0.5)
        
        num_assets = len(assets)
        
        # For amplitude estimation, we need to encode the problem differently
        # Create a quantum circuit that encodes the portfolio distribution
        
        return {
            'encoding_type': 'amplitude_estimation',
            'num_assets': num_assets,
            'num_qubits': int(np.ceil(np.log2(num_assets))) + 3,  # Extra qubits for precision
            'returns': returns.tolist(),
            'covariance': covariance.tolist(),
            'risk_aversion': risk_aversion,
            'algorithm_params': {
                'num_eval_qubits': 5,
                'num_iterations': 10
            },
            'metadata': {
                'assets': assets,
                'problem_type': 'portfolio_optimization'
            }
        }
        
    def _encode_design_parameters(self, problem_data: Dict) -> Dict:
        """
        Encode design parameter optimization
        
        Problem: Optimize continuous parameters subject to constraints
        Uses VQE with parameterized quantum circuits
        """
        parameters = problem_data.get('parameters', [])
        objective_function = problem_data.get('objective', {})
        constraints = problem_data.get('constraints', [])
        bounds = problem_data.get('bounds', [])
        
        num_params = len(parameters)
        num_qubits = max(4, int(np.ceil(np.log2(num_params))))
        
        # Create Hamiltonian for the objective function
        hamiltonian_terms = []
        
        # Linear terms
        if 'linear' in objective_function:
            for i, coeff in enumerate(objective_function['linear']):
                if abs(coeff) > 1e-10:
                    pauli_string = 'I' * i + 'Z' + 'I' * (num_qubits - i - 1)
                    hamiltonian_terms.append({
                        'pauli': pauli_string,
                        'coefficient': float(coeff)
                    })
                    
        # Quadratic terms
        if 'quadratic' in objective_function:
            quad_matrix = np.array(objective_function['quadratic'])
            for i in range(num_params):
                for j in range(i, num_params):
                    if abs(quad_matrix[i, j]) > 1e-10:
                        if i == j:
                            pauli_string = 'I' * i + 'Z' + 'I' * (num_qubits - i - 1)
                        else:
                            pauli_string = ['I'] * num_qubits
                            pauli_string[i] = 'Z'
                            pauli_string[j] = 'Z'
                            pauli_string = ''.join(pauli_string)
                            
                        hamiltonian_terms.append({
                            'pauli': pauli_string,
                            'coefficient': float(quad_matrix[i, j])
                        })
                        
        return {
            'encoding_type': 'vqe',
            'num_qubits': num_qubits,
            'hamiltonian_terms': hamiltonian_terms,
            'ansatz_type': 'hardware_efficient',
            'vqe_depth': 3,
            'constraints': self._encode_constraints(constraints, num_qubits),
            'bounds': bounds,
            'metadata': {
                'parameters': parameters,
                'num_parameters': num_params,
                'problem_type': 'design_optimization'
            }
        }
        
    def _encode_max_cut(self, problem_data: Dict) -> Dict:
        """
        Encode Max-Cut problem as QUBO
        
        Problem: Partition graph vertices to maximize cut edges
        """
        if 'graph' in problem_data:
            G = problem_data['graph']
        else:
            # Create graph from edge list
            edges = problem_data.get('edges', [])
            G = nx.Graph()
            G.add_edges_from(edges)
            
        num_vertices = G.number_of_nodes()
        
        # QUBO matrix
        Q = np.zeros((num_vertices, num_vertices))
        
        # For each edge (i,j), add terms to maximize cut
        for i, j in G.edges():
            # Reward if vertices are in different partitions
            Q[i, i] += 0.5
            Q[j, j] += 0.5
            Q[i, j] -= 1.0
            Q[j, i] -= 1.0
            
        return {
            'encoding_type': 'qubo',
            'qubo_matrix': Q.tolist(),
            'num_variables': num_vertices,
            'num_qubits': num_vertices,
            'metadata': {
                'num_vertices': num_vertices,
                'num_edges': G.number_of_edges(),
                'problem_type': 'max_cut'
            }
        }
        
    def _encode_knapsack(self, problem_data: Dict) -> Dict:
        """
        Encode knapsack problem as QUBO
        
        Problem: Select items to maximize value within weight constraint
        """
        values = np.array(problem_data.get('values', []))
        weights = np.array(problem_data.get('weights', []))
        capacity = problem_data.get('capacity', 0)
        
        num_items = len(values)
        
        # Penalty for constraint violation
        penalty = np.sum(values) * 2
        
        # QUBO matrix
        Q = np.zeros((num_items, num_items))
        
        # Objective: maximize value (minimize negative value)
        for i in range(num_items):
            Q[i, i] -= values[i]
            
        # Constraint: total weight <= capacity
        # Use slack variables to convert inequality to equality
        num_slack_vars = int(np.ceil(np.log2(capacity + 1)))
        total_vars = num_items + num_slack_vars
        
        Q_full = np.zeros((total_vars, total_vars))
        Q_full[:num_items, :num_items] = Q
        
        # Add penalty terms for weight constraint
        for i in range(num_items):
            for j in range(num_items):
                Q_full[i, j] += penalty * weights[i] * weights[j] / (capacity ** 2)
                
        # Slack variable terms
        for k in range(num_slack_vars):
            slack_weight = 2 ** k
            Q_full[num_items + k, num_items + k] -= penalty * slack_weight ** 2 / (capacity ** 2)
            
            for i in range(num_items):
                Q_full[i, num_items + k] += 2 * penalty * weights[i] * slack_weight / (capacity ** 2)
                Q_full[num_items + k, i] = Q_full[i, num_items + k]
                
        return {
            'encoding_type': 'qubo',
            'qubo_matrix': Q_full.tolist(),
            'num_variables': total_vars,
            'num_qubits': total_vars,
            'variable_mapping': {
                **{f'item_{i}': i for i in range(num_items)},
                **{f'slack_{k}': num_items + k for k in range(num_slack_vars)}
            },
            'metadata': {
                'num_items': num_items,
                'capacity': capacity,
                'problem_type': 'knapsack',
                'penalty': penalty
            }
        }
        
    def _encode_vertex_cover(self, problem_data: Dict) -> Dict:
        """
        Encode vertex cover problem
        
        Problem: Find minimum set of vertices covering all edges
        """
        if 'graph' in problem_data:
            G = problem_data['graph']
        else:
            edges = problem_data.get('edges', [])
            G = nx.Graph()
            G.add_edges_from(edges)
            
        num_vertices = G.number_of_nodes()
        penalty = num_vertices * 2
        
        # QUBO matrix
        Q = np.zeros((num_vertices, num_vertices))
        
        # Objective: minimize number of vertices
        for i in range(num_vertices):
            Q[i, i] = 1
            
        # Constraint: each edge must be covered
        for u, v in G.edges():
            # Penalty if edge not covered
            Q[u, u] -= penalty
            Q[v, v] -= penalty
            Q[u, v] += penalty
            Q[v, u] += penalty
            
        return {
            'encoding_type': 'qubo',
            'qubo_matrix': Q.tolist(),
            'num_variables': num_vertices,
            'num_qubits': num_vertices,
            'metadata': {
                'num_vertices': num_vertices,
                'num_edges': G.number_of_edges(),
                'problem_type': 'vertex_cover',
                'penalty': penalty
            }
        }
        
    def _encode_scheduling(self, problem_data: Dict) -> Dict:
        """
        Encode scheduling problem as QUBO
        
        Problem: Schedule jobs on machines to minimize makespan
        """
        jobs = problem_data.get('jobs', [])
        machines = problem_data.get('machines', [])
        processing_times = np.array(problem_data.get('processing_times', []))
        
        num_jobs = len(jobs)
        num_machines = len(machines)
        num_time_slots = problem_data.get('time_horizon', num_jobs)
        
        # Variables: x_ijt = 1 if job i on machine j at time t
        num_vars = num_jobs * num_machines * num_time_slots
        
        # This is a complex encoding - simplified version
        Q = np.zeros((num_vars, num_vars))
        
        penalty = np.max(processing_times) * num_jobs * 10
        
        # Each job must be scheduled exactly once
        for i in range(num_jobs):
            for j1 in range(num_machines):
                for t1 in range(num_time_slots):
                    idx1 = i * num_machines * num_time_slots + j1 * num_time_slots + t1
                    Q[idx1, idx1] -= penalty
                    
                    # No overlap
                    for j2 in range(num_machines):
                        for t2 in range(num_time_slots):
                            if j1 != j2 or t1 != t2:
                                idx2 = i * num_machines * num_time_slots + j2 * num_time_slots + t2
                                Q[idx1, idx2] += penalty
                                
        # Machine capacity constraints
        for j in range(num_machines):
            for t in range(num_time_slots):
                # At most one job per machine per time
                for i1 in range(num_jobs):
                    for i2 in range(i1 + 1, num_jobs):
                        idx1 = i1 * num_machines * num_time_slots + j * num_time_slots + t
                        idx2 = i2 * num_machines * num_time_slots + j * num_time_slots + t
                        Q[idx1, idx2] += penalty
                        Q[idx2, idx1] += penalty
                        
        return {
            'encoding_type': 'qubo',
            'qubo_matrix': Q.tolist(),
            'num_variables': num_vars,
            'num_qubits': int(np.ceil(np.log2(num_vars))),
            'metadata': {
                'num_jobs': num_jobs,
                'num_machines': num_machines,
                'num_time_slots': num_time_slots,
                'problem_type': 'scheduling'
            }
        }
        
    def _encode_generic(self, problem_data: Dict) -> Dict:
        """
        Generic encoding for custom problems
        """
        if 'qubo_matrix' in problem_data:
            # Already in QUBO format
            Q = np.array(problem_data['qubo_matrix'])
            num_vars = Q.shape[0]
            
            return {
                'encoding_type': 'qubo',
                'qubo_matrix': Q.tolist(),
                'num_variables': num_vars,
                'num_qubits': int(np.ceil(np.log2(num_vars))),
                'metadata': problem_data.get('metadata', {})
            }
            
        elif 'hamiltonian' in problem_data:
            # Hamiltonian format
            return {
                'encoding_type': 'hamiltonian',
                'hamiltonian_terms': problem_data['hamiltonian'],
                'num_qubits': problem_data.get('num_qubits', 4),
                'metadata': problem_data.get('metadata', {})
            }
            
        else:
            # Default: create random QUBO for testing
            num_vars = problem_data.get('num_variables', 4)
            Q = np.random.randn(num_vars, num_vars)
            Q = (Q + Q.T) / 2  # Symmetric
            
            return {
                'encoding_type': 'qubo',
                'qubo_matrix': Q.tolist(),
                'num_variables': num_vars,
                'num_qubits': int(np.ceil(np.log2(num_vars))),
                'metadata': {'problem_type': 'generic'}
            }
            
    def _encode_constraints(self, constraints: List[Dict], num_qubits: int) -> List[Dict]:
        """
        Encode constraints as penalty terms
        """
        encoded_constraints = []
        
        for constraint in constraints:
            if constraint['type'] == 'equality':
                # A = b becomes (A - b)^2 penalty
                encoded_constraints.append({
                    'type': 'penalty',
                    'terms': constraint.get('terms', []),
                    'penalty_weight': 1000.0
                })
            elif constraint['type'] == 'inequality':
                # A <= b requires slack variables
                encoded_constraints.append({
                    'type': 'inequality',
                    'terms': constraint.get('terms', []),
                    'bound': constraint.get('bound', 0),
                    'penalty_weight': 1000.0
                })
                
        return encoded_constraints 

    def _encode_facility_location_subproblem(self, problem_data: Dict) -> Dict:
        """
        Encodes the UFLP customer assignment sub-problem as a QUBO matrix.

        Args:
            problem_data: Dict containing 'open_facilities_indices' and 'transportation_costs'.

        Returns:
            A dictionary containing the QUBO matrix.
        """
        open_facilities_indices = problem_data['open_facilities_indices']
        transportation_costs = np.array(problem_data['transportation_costs'])
        
        num_open_facilities = len(open_facilities_indices)
        num_customers = transportation_costs.shape[1]
        
        # QUBO variables x_jk, where j is customer index, k is open facility index
        num_vars = num_customers * num_open_facilities
        qubo_matrix = np.zeros((num_vars, num_vars))

        # Penalty for violating the 'one facility per customer' constraint
        # Should be larger than the max possible transportation cost
        penalty = np.max(transportation_costs) * num_customers + 1

        for j in range(num_customers):
            # Constraint: sum_k(x_jk) = 1  =>  (1 - sum_k(x_jk))^2
            # This expands to: 1 - 2*sum_k(x_jk) + (sum_k(x_jk))^2
            # (sum_k(x_jk))^2 = sum_k(x_jk^2) + 2*sum_{k<l}(x_jk*x_jl)
            # Since x_jk is binary, x_jk^2 = x_jk
            
            for k in range(num_open_facilities):
                facility_idx = open_facilities_indices[k]
                var_idx_k = j * num_open_facilities + k
                
                # Diagonal terms from objective function and penalty
                cost = transportation_costs[facility_idx, j]
                qubo_matrix[var_idx_k, var_idx_k] += cost - penalty
                
                # Off-diagonal terms from penalty
                for l in range(k + 1, num_open_facilities):
                    var_idx_l = j * num_open_facilities + l
                    qubo_matrix[var_idx_k, var_idx_l] += 2 * penalty
        
        return {"qubo_matrix": qubo_matrix}

    def _qubo_to_ising(self, qubo_matrix: np.ndarray) -> Tuple[PauliSumOp, float]:
        """Converts a QUBO matrix to an Ising Hamiltonian operator."""
        pass # Placeholder for actual conversion logic 