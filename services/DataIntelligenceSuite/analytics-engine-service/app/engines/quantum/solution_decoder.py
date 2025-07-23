"""
Quantum Solution Decoder

Converts quantum algorithm outputs back to problem-specific solutions.
"""

import numpy as np
from typing import Dict, Any, List, Optional, Tuple
from datetime import datetime

from platformq_shared.logging_config import get_logger

logger = get_logger(__name__)


class QuantumSolutionDecoder:
    """
    Decodes quantum algorithm solutions back to the original problem space.
    """
    
    def __init__(self):
        self.decoding_strategies = {
            "qubo": self._decode_qubo_solution,
            "ising": self._decode_ising_solution,
            "max_cut": self._decode_max_cut_solution,
            "tsp": self._decode_tsp_solution,
            "portfolio": self._decode_portfolio_solution,
            "knapsack": self._decode_knapsack_solution,
            "vertex_cover": self._decode_vertex_cover_solution,
            "scheduling": self._decode_scheduling_solution,
            "resource_allocation": self._decode_resource_allocation_solution,
            "design_optimization": self._decode_design_optimization_solution,
            "generic": self._decode_generic_solution
        }
    
    async def decode(
        self,
        raw_solution: Dict[str, Any],
        problem_def: Dict[str, Any],
        encoding_info: Dict[str, Any]
    ) -> Dict[str, Any]:
        """
        Decode quantum solution to problem space.
        
        Args:
            raw_solution: Raw solution from quantum algorithm
            problem_def: Original problem definition
            encoding_info: Information from problem encoding
            
        Returns:
            Decoded solution in problem space
        """
        problem_type = problem_def.get("problem_type", "generic")
        logger.info(f"Decoding solution for '{problem_type}' problem")
        
        # Get decoding strategy
        decode_func = self.decoding_strategies.get(problem_type)
        if not decode_func:
            logger.warning(f"No specific decoder for {problem_type}, using generic")
            decode_func = self._decode_generic_solution
        
        # Decode solution
        decoded = decode_func(raw_solution, problem_def, encoding_info)
        
        # Add common metadata
        decoded["problem_type"] = problem_type
        decoded["decoded_at"] = datetime.utcnow().isoformat()
        decoded["is_valid"] = self._validate_solution(decoded, problem_def)
        
        # Calculate objective value
        if "objective_value" not in decoded:
            decoded["objective_value"] = self._calculate_objective_value(
                decoded,
                problem_def
            )
        
        return decoded
    
    def _decode_qubo_solution(
        self,
        raw_solution: Dict[str, Any],
        problem_def: Dict[str, Any],
        encoding_info: Dict[str, Any]
    ) -> Dict[str, Any]:
        """Decode QUBO solution."""
        solution_vector = raw_solution.get("solution_vector", [])
        variable_mapping = encoding_info.get("variable_mapping", {})
        
        # Map solution back to variable names
        variables = {}
        reverse_mapping = {v: k for k, v in variable_mapping.items()}
        
        for idx, value in enumerate(solution_vector):
            var_name = reverse_mapping.get(idx, f"x_{idx}")
            variables[var_name] = int(value)
        
        return {
            "variables": variables,
            "solution_vector": solution_vector,
            "energy": raw_solution.get("energy", 0)
        }
    
    def _decode_ising_solution(
        self,
        raw_solution: Dict[str, Any],
        problem_def: Dict[str, Any],
        encoding_info: Dict[str, Any]
    ) -> Dict[str, Any]:
        """Decode Ising solution."""
        # Ising solutions are in {-1, +1}, convert to {0, 1}
        ising_solution = raw_solution.get("solution_vector", [])
        binary_solution = [(s + 1) // 2 for s in ising_solution]
        
        # Use QUBO decoder for the rest
        raw_solution["solution_vector"] = binary_solution
        return self._decode_qubo_solution(raw_solution, problem_def, encoding_info)
    
    def _decode_max_cut_solution(
        self,
        raw_solution: Dict[str, Any],
        problem_def: Dict[str, Any],
        encoding_info: Dict[str, Any]
    ) -> Dict[str, Any]:
        """Decode Max-Cut solution."""
        solution_vector = raw_solution.get("solution_vector", [])
        graph_info = encoding_info.get("graph_info", {})
        
        # Partition vertices
        partition_1 = []
        partition_2 = []
        
        for vertex, value in enumerate(solution_vector):
            if value == 0:
                partition_1.append(vertex)
            else:
                partition_2.append(vertex)
        
        # Calculate cut value
        cut_value = self._calculate_cut_value(
            partition_1,
            partition_2,
            problem_def.get("metadata", {})
        )
        
        return {
            "partition_1": partition_1,
            "partition_2": partition_2,
            "cut_value": cut_value,
            "num_vertices": graph_info.get("num_vertices", len(solution_vector)),
            "objective_value": cut_value
        }
    
    def _decode_tsp_solution(
        self,
        raw_solution: Dict[str, Any],
        problem_def: Dict[str, Any],
        encoding_info: Dict[str, Any]
    ) -> Dict[str, Any]:
        """Decode TSP solution."""
        solution_vector = raw_solution.get("solution_vector", [])
        variable_mapping = encoding_info.get("variable_mapping", {})
        problem_metadata = encoding_info.get("problem_metadata", {})
        n_cities = problem_metadata.get("num_cities", 0)
        
        # Extract tour from binary variables
        tour = []
        for t in range(n_cities):
            for i in range(n_cities):
                var_name = f"x_{i}_{t}"
                var_idx = variable_mapping.get(var_name, -1)
                if var_idx >= 0 and var_idx < len(solution_vector):
                    if solution_vector[var_idx] == 1:
                        tour.append(i)
                        break
        
        # Calculate tour distance
        distance_matrix = problem_def.get("metadata", {}).get("distance_matrix", [])
        total_distance = self._calculate_tour_distance(tour, distance_matrix)
        
        # Check if tour is valid
        is_valid_tour = len(tour) == n_cities and len(set(tour)) == n_cities
        
        return {
            "tour": tour,
            "total_distance": total_distance,
            "is_valid_tour": is_valid_tour,
            "objective_value": total_distance if is_valid_tour else float('inf')
        }
    
    def _decode_portfolio_solution(
        self,
        raw_solution: Dict[str, Any],
        problem_def: Dict[str, Any],
        encoding_info: Dict[str, Any]
    ) -> Dict[str, Any]:
        """Decode portfolio optimization solution."""
        solution_vector = raw_solution.get("solution_vector", [])
        variable_mapping = encoding_info.get("variable_mapping", {})
        metadata = problem_def.get("metadata", {})
        
        # Extract selected assets
        selected_assets = []
        for asset_idx in range(len(solution_vector)):
            if solution_vector[asset_idx] == 1:
                selected_assets.append(asset_idx)
        
        # Calculate portfolio metrics
        returns = np.array(metadata.get("expected_returns", []))
        covariance = np.array(metadata.get("covariance_matrix", []))
        
        if selected_assets:
            expected_return = np.sum(returns[selected_assets])
            
            # Portfolio variance
            selected_cov = covariance[np.ix_(selected_assets, selected_assets)]
            portfolio_variance = np.sum(selected_cov)
            portfolio_risk = np.sqrt(portfolio_variance)
            
            # Sharpe ratio (assuming risk-free rate = 0)
            sharpe_ratio = expected_return / portfolio_risk if portfolio_risk > 0 else 0
        else:
            expected_return = 0
            portfolio_risk = 0
            sharpe_ratio = 0
        
        return {
            "selected_assets": selected_assets,
            "expected_return": float(expected_return),
            "portfolio_risk": float(portfolio_risk),
            "sharpe_ratio": float(sharpe_ratio),
            "num_assets_selected": len(selected_assets),
            "objective_value": float(expected_return - 
                metadata.get("risk_aversion", 0.5) * portfolio_variance)
        }
    
    def _decode_knapsack_solution(
        self,
        raw_solution: Dict[str, Any],
        problem_def: Dict[str, Any],
        encoding_info: Dict[str, Any]
    ) -> Dict[str, Any]:
        """Decode knapsack solution."""
        solution_vector = raw_solution.get("solution_vector", [])
        variable_mapping = encoding_info.get("variable_mapping", {})
        metadata = problem_def.get("metadata", {})
        problem_metadata = encoding_info.get("problem_metadata", {})
        
        n_items = problem_metadata.get("num_items", 0)
        values = metadata.get("values", [])
        weights = metadata.get("weights", [])
        capacity = metadata.get("capacity", 0)
        
        # Extract selected items (ignoring slack variables)
        selected_items = []
        total_value = 0
        total_weight = 0
        
        for i in range(min(n_items, len(solution_vector))):
            if solution_vector[i] == 1:
                selected_items.append(i)
                if i < len(values):
                    total_value += values[i]
                if i < len(weights):
                    total_weight += weights[i]
        
        # Check feasibility
        is_feasible = total_weight <= capacity
        
        return {
            "selected_items": selected_items,
            "total_value": total_value,
            "total_weight": total_weight,
            "capacity": capacity,
            "is_feasible": is_feasible,
            "objective_value": total_value if is_feasible else 0
        }
    
    def _decode_vertex_cover_solution(
        self,
        raw_solution: Dict[str, Any],
        problem_def: Dict[str, Any],
        encoding_info: Dict[str, Any]
    ) -> Dict[str, Any]:
        """Decode vertex cover solution."""
        solution_vector = raw_solution.get("solution_vector", [])
        graph_info = encoding_info.get("graph_info", {})
        
        # Extract vertices in cover
        vertex_cover = []
        for vertex, value in enumerate(solution_vector):
            if value == 1:
                vertex_cover.append(vertex)
        
        # Verify cover validity
        edges = problem_def.get("metadata", {}).get("edges", [])
        covered_edges = 0
        
        for u, v in edges:
            if u in vertex_cover or v in vertex_cover:
                covered_edges += 1
        
        is_valid_cover = covered_edges == len(edges)
        
        return {
            "vertex_cover": vertex_cover,
            "cover_size": len(vertex_cover),
            "covered_edges": covered_edges,
            "total_edges": len(edges),
            "is_valid_cover": is_valid_cover,
            "objective_value": len(vertex_cover) if is_valid_cover else float('inf')
        }
    
    def _decode_scheduling_solution(
        self,
        raw_solution: Dict[str, Any],
        problem_def: Dict[str, Any],
        encoding_info: Dict[str, Any]
    ) -> Dict[str, Any]:
        """Decode scheduling solution."""
        solution_vector = raw_solution.get("solution_vector", [])
        variable_mapping = encoding_info.get("variable_mapping", {})
        problem_metadata = encoding_info.get("problem_metadata", {})
        
        n_jobs = problem_metadata.get("num_jobs", 0)
        n_machines = problem_metadata.get("num_machines", 0)
        n_time_slots = problem_metadata.get("num_time_slots", 0)
        
        # Extract schedule
        schedule = {}
        job_assignments = {}
        
        for i in range(n_jobs):
            for j in range(n_machines):
                for t in range(n_time_slots):
                    var_name = f"x_{i}_{j}_{t}"
                    var_idx = variable_mapping.get(var_name, -1)
                    
                    if var_idx >= 0 and var_idx < len(solution_vector):
                        if solution_vector[var_idx] == 1:
                            if j not in schedule:
                                schedule[j] = {}
                            schedule[j][t] = i
                            job_assignments[i] = (j, t)
        
        # Calculate makespan
        makespan = 0
        processing_times = problem_def.get("metadata", {}).get("processing_times", [])
        
        for job, (machine, start_time) in job_assignments.items():
            if job < len(processing_times) and machine < len(processing_times[job]):
                end_time = start_time + processing_times[job][machine]
                makespan = max(makespan, end_time)
        
        # Check validity
        is_valid = len(job_assignments) == n_jobs
        
        return {
            "schedule": schedule,
            "job_assignments": job_assignments,
            "makespan": makespan,
            "is_valid_schedule": is_valid,
            "objective_value": makespan if is_valid else float('inf')
        }
    
    def _decode_resource_allocation_solution(
        self,
        raw_solution: Dict[str, Any],
        problem_def: Dict[str, Any],
        encoding_info: Dict[str, Any]
    ) -> Dict[str, Any]:
        """Decode resource allocation solution."""
        solution_vector = raw_solution.get("solution_vector", [])
        variable_mapping = encoding_info.get("variable_mapping", {})
        problem_metadata = encoding_info.get("problem_metadata", {})
        metadata = problem_def.get("metadata", {})
        
        n_resources = problem_metadata.get("num_resources", 0)
        n_tasks = problem_metadata.get("num_tasks", 0)
        cost_matrix = metadata.get("cost_matrix", [])
        
        # Extract allocations
        allocations = {}
        total_cost = 0
        
        for i in range(n_resources):
            for j in range(n_tasks):
                var_name = f"x_{i}_{j}"
                var_idx = variable_mapping.get(var_name, -1)
                
                if var_idx >= 0 and var_idx < len(solution_vector):
                    if solution_vector[var_idx] == 1:
                        allocations[j] = i
                        if i < len(cost_matrix) and j < len(cost_matrix[i]):
                            total_cost += cost_matrix[i][j]
        
        # Check if all tasks are allocated
        is_complete = len(allocations) == n_tasks
        
        # Check resource capacity constraints
        resource_usage = {i: 0 for i in range(n_resources)}
        task_demands = metadata.get("task_demands", [1] * n_tasks)
        
        for task, resource in allocations.items():
            if task < len(task_demands):
                resource_usage[resource] += task_demands[task]
        
        capacities = metadata.get("resource_capacity", [float('inf')] * n_resources)
        is_feasible = all(
            resource_usage[i] <= capacities[i]
            for i in range(n_resources)
            if i < len(capacities)
        )
        
        return {
            "allocations": allocations,
            "total_cost": total_cost,
            "resource_usage": resource_usage,
            "is_complete": is_complete,
            "is_feasible": is_feasible,
            "objective_value": total_cost if (is_complete and is_feasible) else float('inf')
        }
    
    def _decode_design_optimization_solution(
        self,
        raw_solution: Dict[str, Any],
        problem_def: Dict[str, Any],
        encoding_info: Dict[str, Any]
    ) -> Dict[str, Any]:
        """Decode design optimization solution."""
        # For VQE solutions, we get continuous parameters
        parameters = raw_solution.get("parameters", [])
        energy = raw_solution.get("energy", 0)
        
        # Map parameters to design variables
        variables = problem_def.get("variables", {})
        design_values = {}
        
        for idx, (var_name, var_info) in enumerate(variables.items()):
            if idx < len(parameters):
                # Scale parameter to variable bounds
                bounds = var_info.get("bounds", [-1, 1])
                scaled_value = self._scale_parameter(
                    parameters[idx],
                    bounds[0],
                    bounds[1]
                )
                design_values[var_name] = scaled_value
        
        return {
            "design_parameters": design_values,
            "raw_parameters": parameters,
            "energy": energy,
            "objective_value": -energy  # VQE minimizes energy
        }
    
    def _decode_generic_solution(
        self,
        raw_solution: Dict[str, Any],
        problem_def: Dict[str, Any],
        encoding_info: Dict[str, Any]
    ) -> Dict[str, Any]:
        """Generic solution decoder."""
        solution_vector = raw_solution.get("solution_vector", [])
        variables = problem_def.get("variables", {})
        
        # Map solution to variable names
        decoded_vars = {}
        for idx, (var_name, var_info) in enumerate(variables.items()):
            if idx < len(solution_vector):
                decoded_vars[var_name] = solution_vector[idx]
        
        return {
            "variables": decoded_vars,
            "solution_vector": solution_vector,
            "energy": raw_solution.get("energy", 0)
        }
    
    def _validate_solution(
        self,
        decoded_solution: Dict[str, Any],
        problem_def: Dict[str, Any]
    ) -> bool:
        """Validate solution against constraints."""
        constraints = problem_def.get("constraints", [])
        variables = decoded_solution.get("variables", {})
        
        for constraint in constraints:
            if not self._check_constraint(variables, constraint):
                return False
        
        # Problem-specific validation
        if "is_valid" in decoded_solution:
            return decoded_solution["is_valid"]
        
        return True
    
    def _check_constraint(
        self,
        variables: Dict[str, Any],
        constraint: Dict[str, Any]
    ) -> bool:
        """Check if constraint is satisfied."""
        # Simplified constraint checking
        # In practice, would evaluate constraint expression
        return True
    
    def _calculate_objective_value(
        self,
        decoded_solution: Dict[str, Any],
        problem_def: Dict[str, Any]
    ) -> float:
        """Calculate objective function value."""
        if "objective_value" in decoded_solution:
            return decoded_solution["objective_value"]
        
        objective = problem_def.get("objective_function", {})
        variables = decoded_solution.get("variables", {})
        
        value = 0
        
        # Linear terms
        if "linear" in objective:
            for idx, coeff in enumerate(objective["linear"]):
                var_name = list(variables.keys())[idx] if idx < len(variables) else f"x_{idx}"
                value += coeff * variables.get(var_name, 0)
        
        # Quadratic terms
        if "quadratic" in objective:
            quad_matrix = np.array(objective["quadratic"])
            var_values = list(variables.values())
            
            for i in range(len(var_values)):
                for j in range(len(var_values)):
                    if i < quad_matrix.shape[0] and j < quad_matrix.shape[1]:
                        value += quad_matrix[i, j] * var_values[i] * var_values[j]
        
        return float(value)
    
    def _calculate_cut_value(
        self,
        partition_1: List[int],
        partition_2: List[int],
        metadata: Dict[str, Any]
    ) -> int:
        """Calculate cut value for graph partitioning."""
        if "adjacency_matrix" in metadata:
            adj_matrix = np.array(metadata["adjacency_matrix"])
            cut_value = 0
            
            for u in partition_1:
                for v in partition_2:
                    if u < adj_matrix.shape[0] and v < adj_matrix.shape[1]:
                        cut_value += adj_matrix[u, v]
            
            return cut_value
        
        elif "edges" in metadata:
            edges = metadata["edges"]
            cut_value = 0
            
            for u, v in edges:
                if (u in partition_1 and v in partition_2) or \
                   (u in partition_2 and v in partition_1):
                    cut_value += 1
            
            return cut_value
        
        return 0
    
    def _calculate_tour_distance(
        self,
        tour: List[int],
        distance_matrix: List[List[float]]
    ) -> float:
        """Calculate total distance for TSP tour."""
        if not tour or not distance_matrix:
            return float('inf')
        
        total_distance = 0
        n = len(tour)
        
        for i in range(n):
            from_city = tour[i]
            to_city = tour[(i + 1) % n]  # Loop back to start
            
            if from_city < len(distance_matrix) and to_city < len(distance_matrix[from_city]):
                total_distance += distance_matrix[from_city][to_city]
            else:
                return float('inf')  # Invalid tour
        
        return total_distance
    
    def _scale_parameter(
        self,
        param: float,
        min_bound: float,
        max_bound: float
    ) -> float:
        """Scale parameter from [-1, 1] to [min_bound, max_bound]."""
        # Assuming param is in [-1, 1]
        scaled = (param + 1) / 2  # Convert to [0, 1]
        return min_bound + scaled * (max_bound - min_bound) 