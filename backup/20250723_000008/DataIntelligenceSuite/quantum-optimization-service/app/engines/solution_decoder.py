"""
Solution Decoder for Quantum Optimization

Converts quantum optimization results back to problem-specific solutions.
"""

import numpy as np
from typing import Dict, List, Tuple, Optional, Any
import json

from platformq_shared.logging_config import get_logger
from .problem_encoder import ProblemType

logger = get_logger(__name__)


class SolutionDecoder:
    """
    Decodes quantum optimization results into problem-specific solutions
    """
    
    def __init__(self):
        self.decoders = {
            ProblemType.RESOURCE_ALLOCATION: self._decode_resource_allocation,
            ProblemType.ROUTE_OPTIMIZATION: self._decode_route_optimization,
            ProblemType.PORTFOLIO_OPTIMIZATION: self._decode_portfolio,
            ProblemType.DESIGN_PARAMETERS: self._decode_design_parameters,
            ProblemType.MAX_CUT: self._decode_max_cut,
            ProblemType.KNAPSACK: self._decode_knapsack,
            ProblemType.VERTEX_COVER: self._decode_vertex_cover,
            ProblemType.SCHEDULING: self._decode_scheduling,
            ProblemType.GENERIC: self._decode_generic
        }
        
    def decode(self, 
               quantum_result: Dict,
               problem_type: ProblemType,
               original_problem: Dict) -> Dict:
        """
        Decode quantum result into problem solution
        
        Args:
            quantum_result: Result from quantum optimizer
            problem_type: Type of problem
            original_problem: Original problem specification
            
        Returns:
            Decoded solution dictionary
        """
        if problem_type not in self.decoders:
            logger.warning(f"No specific decoder for {problem_type}, using generic")
            return self._decode_generic(quantum_result, original_problem)
            
        logger.info(f"Decoding {problem_type.value} solution")
        
        return self.decoders[problem_type](quantum_result, original_problem)
        
    def _decode_resource_allocation(self, result: Dict, problem: Dict) -> Dict:
        """Decode resource allocation solution"""
        solution_vector = result.get('solution_vector', [])
        resources = problem.get('resources', [])
        tasks = problem.get('tasks', [])
        costs = np.array(problem.get('costs', []))
        
        num_resources = len(resources)
        num_tasks = len(tasks)
        
        # Extract assignments from solution vector
        assignments = {}
        resource_assignments = {r: [] for r in resources}
        total_cost = 0.0
        
        for i in range(num_resources):
            for j in range(num_tasks):
                var_idx = i * num_tasks + j
                if var_idx < len(solution_vector) and solution_vector[var_idx] > 0.5:
                    assignments[tasks[j]] = resources[i]
                    resource_assignments[resources[i]].append(tasks[j])
                    if i < costs.shape[0] and j < costs.shape[1]:
                        total_cost += costs[i, j]
                        
        # Verify constraints
        unassigned_tasks = [t for t in tasks if t not in assignments]
        multiply_assigned = []
        
        for task in tasks:
            count = sum(1 for r in resources 
                       for t in resource_assignments[r] if t == task)
            if count > 1:
                multiply_assigned.append(task)
                
        return {
            'assignments': assignments,
            'resource_utilization': resource_assignments,
            'total_cost': total_cost,
            'unassigned_tasks': unassigned_tasks,
            'constraint_violations': {
                'unassigned': unassigned_tasks,
                'multiply_assigned': multiply_assigned
            },
            'feasible': len(unassigned_tasks) == 0 and len(multiply_assigned) == 0,
            'solution_quality': self._calculate_allocation_quality(
                assignments, costs, resources, tasks
            )
        }
        
    def _decode_route_optimization(self, result: Dict, problem: Dict) -> Dict:
        """Decode TSP/VRP solution"""
        solution_vector = result.get('solution_vector', [])
        num_cities = problem.get('cities', 0)
        distances = np.array(problem.get('distances', []))
        
        # Extract route from solution vector
        route = []
        visited = set()
        
        for t in range(num_cities):
            for i in range(num_cities):
                var_idx = i * num_cities + t
                if var_idx < len(solution_vector) and solution_vector[var_idx] > 0.5:
                    if i not in visited:
                        route.append(i)
                        visited.add(i)
                        break
                        
        # Calculate total distance
        total_distance = 0.0
        for i in range(len(route) - 1):
            if route[i] < distances.shape[0] and route[i+1] < distances.shape[1]:
                total_distance += distances[route[i], route[i+1]]
                
        # Add return to start for TSP
        if len(route) > 0 and route[0] < distances.shape[0] and route[-1] < distances.shape[1]:
            total_distance += distances[route[-1], route[0]]
            
        # Check feasibility
        missing_cities = [i for i in range(num_cities) if i not in visited]
        
        return {
            'route': route,
            'total_distance': total_distance,
            'cities_visited': len(visited),
            'missing_cities': missing_cities,
            'feasible': len(missing_cities) == 0,
            'route_description': self._format_route(route, problem.get('city_names', [])),
            'optimization_ratio': self._calculate_route_quality(
                route, distances, num_cities
            )
        }
        
    def _decode_portfolio(self, result: Dict, problem: Dict) -> Dict:
        """Decode portfolio optimization solution"""
        # For amplitude estimation, result is different
        assets = problem.get('assets', [])
        returns = np.array(problem.get('expected_returns', []))
        covariance = np.array(problem.get('covariance_matrix', []))
        
        # Extract portfolio weights (simplified)
        if 'amplitudes' in result:
            weights = np.abs(result['amplitudes']) ** 2
        else:
            # Fall back to equal weights
            weights = np.ones(len(assets)) / len(assets)
            
        # Normalize weights
        weights = weights[:len(assets)]
        weights = weights / np.sum(weights)
        
        # Calculate portfolio metrics
        expected_return = np.dot(weights, returns)
        portfolio_variance = np.dot(weights, np.dot(covariance, weights))
        portfolio_risk = np.sqrt(portfolio_variance)
        sharpe_ratio = expected_return / portfolio_risk if portfolio_risk > 0 else 0
        
        # Create allocation dictionary
        allocation = {
            assets[i]: float(weights[i])
            for i in range(len(assets))
        }
        
        # Filter out very small allocations
        allocation = {
            asset: weight
            for asset, weight in allocation.items()
            if weight > 0.01  # 1% threshold
        }
        
        return {
            'allocation': allocation,
            'weights': weights.tolist(),
            'expected_return': float(expected_return),
            'risk': float(portfolio_risk),
            'sharpe_ratio': float(sharpe_ratio),
            'diversification': self._calculate_diversification(weights),
            'largest_position': max(weights),
            'num_positions': sum(1 for w in weights if w > 0.01)
        }
        
    def _decode_design_parameters(self, result: Dict, problem: Dict) -> Dict:
        """Decode design parameter optimization solution"""
        parameters = problem.get('parameters', [])
        bounds = problem.get('bounds', [])
        
        # Extract parameter values from quantum state
        if 'solution' in result:
            solution_dict = result['solution']
            param_values = []
            
            for i, param in enumerate(parameters):
                if f'x_{i}' in solution_dict:
                    value = solution_dict[f'x_{i}']
                else:
                    value = 0.5  # Default
                    
                # Apply bounds if available
                if i < len(bounds):
                    lower, upper = bounds[i]
                    value = lower + value * (upper - lower)
                    
                param_values.append(value)
        else:
            # Use objective value as indicator
            param_values = [0.5] * len(parameters)
            
        # Create parameter dictionary
        optimized_parameters = {
            param: value
            for param, value in zip(parameters, param_values)
        }
        
        # Calculate objective value if possible
        obj_value = result.get('objective_value', 0.0)
        
        return {
            'parameters': optimized_parameters,
            'parameter_values': param_values,
            'objective_value': obj_value,
            'constraints_satisfied': self._check_design_constraints(
                param_values, problem.get('constraints', [])
            ),
            'improvement': self._calculate_improvement(
                obj_value, problem.get('baseline_objective', obj_value * 1.1)
            )
        }
        
    def _decode_max_cut(self, result: Dict, problem: Dict) -> Dict:
        """Decode max-cut solution"""
        solution_vector = result.get('solution_vector', [])
        edges = problem.get('edges', [])
        
        # Partition vertices
        partition_0 = []
        partition_1 = []
        
        for i, value in enumerate(solution_vector):
            if value > 0.5:
                partition_1.append(i)
            else:
                partition_0.append(i)
                
        # Count cut edges
        cut_edges = []
        for u, v in edges:
            if (u in partition_0 and v in partition_1) or \
               (u in partition_1 and v in partition_0):
                cut_edges.append((u, v))
                
        return {
            'partition_0': partition_0,
            'partition_1': partition_1,
            'cut_edges': cut_edges,
            'cut_size': len(cut_edges),
            'total_edges': len(edges),
            'cut_ratio': len(cut_edges) / len(edges) if edges else 0,
            'balance': abs(len(partition_0) - len(partition_1))
        }
        
    def _decode_knapsack(self, result: Dict, problem: Dict) -> Dict:
        """Decode knapsack solution"""
        solution_vector = result.get('solution_vector', [])
        values = problem.get('values', [])
        weights = problem.get('weights', [])
        capacity = problem.get('capacity', 0)
        
        # Extract selected items
        selected_items = []
        total_value = 0
        total_weight = 0
        
        for i in range(len(values)):
            if i < len(solution_vector) and solution_vector[i] > 0.5:
                selected_items.append(i)
                total_value += values[i]
                total_weight += weights[i]
                
        # Check capacity constraint
        feasible = total_weight <= capacity
        
        return {
            'selected_items': selected_items,
            'total_value': total_value,
            'total_weight': total_weight,
            'capacity': capacity,
            'utilization': total_weight / capacity if capacity > 0 else 0,
            'feasible': feasible,
            'items': [
                {
                    'id': i,
                    'selected': i in selected_items,
                    'value': values[i],
                    'weight': weights[i],
                    'value_density': values[i] / weights[i] if weights[i] > 0 else 0
                }
                for i in range(len(values))
            ]
        }
        
    def _decode_vertex_cover(self, result: Dict, problem: Dict) -> Dict:
        """Decode vertex cover solution"""
        solution_vector = result.get('solution_vector', [])
        edges = problem.get('edges', [])
        
        # Extract vertex cover
        vertex_cover = []
        for i, value in enumerate(solution_vector):
            if value > 0.5:
                vertex_cover.append(i)
                
        # Check coverage
        covered_edges = []
        uncovered_edges = []
        
        for u, v in edges:
            if u in vertex_cover or v in vertex_cover:
                covered_edges.append((u, v))
            else:
                uncovered_edges.append((u, v))
                
        return {
            'vertex_cover': vertex_cover,
            'cover_size': len(vertex_cover),
            'covered_edges': covered_edges,
            'uncovered_edges': uncovered_edges,
            'coverage': len(covered_edges) / len(edges) if edges else 1.0,
            'feasible': len(uncovered_edges) == 0,
            'minimal': self._check_minimal_cover(vertex_cover, edges)
        }
        
    def _decode_scheduling(self, result: Dict, problem: Dict) -> Dict:
        """Decode scheduling solution"""
        solution_vector = result.get('solution_vector', [])
        jobs = problem.get('jobs', [])
        machines = problem.get('machines', [])
        processing_times = problem.get('processing_times', [])
        
        num_jobs = len(jobs)
        num_machines = len(machines)
        num_time_slots = problem.get('time_horizon', num_jobs)
        
        # Extract schedule
        schedule = {machine: [] for machine in machines}
        job_assignments = {}
        
        for i in range(num_jobs):
            for j in range(num_machines):
                for t in range(num_time_slots):
                    var_idx = i * num_machines * num_time_slots + j * num_time_slots + t
                    if var_idx < len(solution_vector) and solution_vector[var_idx] > 0.5:
                        schedule[machines[j]].append({
                            'job': jobs[i],
                            'start_time': t,
                            'duration': processing_times[i][j] if i < len(processing_times) else 1
                        })
                        job_assignments[jobs[i]] = {
                            'machine': machines[j],
                            'start_time': t
                        }
                        
        # Calculate makespan
        makespan = 0
        for machine, tasks in schedule.items():
            for task in tasks:
                end_time = task['start_time'] + task['duration']
                makespan = max(makespan, end_time)
                
        return {
            'schedule': schedule,
            'job_assignments': job_assignments,
            'makespan': makespan,
            'machine_utilization': {
                machine: len(tasks) / num_time_slots
                for machine, tasks in schedule.items()
            },
            'unscheduled_jobs': [
                job for job in jobs if job not in job_assignments
            ],
            'conflicts': self._check_schedule_conflicts(schedule)
        }
        
    def _decode_generic(self, result: Dict, problem: Dict) -> Dict:
        """Generic solution decoder"""
        solution = result.get('solution', {})
        solution_vector = result.get('solution_vector', [])
        
        return {
            'solution': solution,
            'solution_vector': solution_vector,
            'objective_value': result.get('objective_value', 0.0),
            'feasible': True,  # Assume feasible for generic
            'metadata': problem.get('metadata', {})
        }
        
    # Helper methods
    def _calculate_allocation_quality(self, assignments: Dict, costs: np.ndarray,
                                    resources: List, tasks: List) -> float:
        """Calculate quality of resource allocation"""
        if not assignments:
            return 0.0
            
        total_cost = 0
        for task, resource in assignments.items():
            if task in tasks and resource in resources:
                i = resources.index(resource)
                j = tasks.index(task)
                if i < costs.shape[0] and j < costs.shape[1]:
                    total_cost += costs[i, j]
                    
        # Compare to average cost
        avg_cost = np.mean(costs) * len(tasks)
        quality = 1.0 - (total_cost / avg_cost) if avg_cost > 0 else 0.5
        
        return max(0.0, min(1.0, quality))
        
    def _format_route(self, route: List[int], city_names: List[str]) -> str:
        """Format route with city names"""
        if city_names:
            route_names = []
            for city_idx in route:
                if city_idx < len(city_names):
                    route_names.append(city_names[city_idx])
                else:
                    route_names.append(f"City_{city_idx}")
            return " → ".join(route_names)
        else:
            return " → ".join(map(str, route))
            
    def _calculate_route_quality(self, route: List[int], distances: np.ndarray,
                               num_cities: int) -> float:
        """Calculate route quality compared to average"""
        if len(route) < 2:
            return 0.0
            
        total_distance = 0
        for i in range(len(route) - 1):
            if route[i] < distances.shape[0] and route[i+1] < distances.shape[1]:
                total_distance += distances[route[i], route[i+1]]
                
        # Add return distance
        if route[0] < distances.shape[0] and route[-1] < distances.shape[1]:
            total_distance += distances[route[-1], route[0]]
            
        # Compare to average distance
        avg_distance = np.mean(distances[distances > 0]) * num_cities
        quality = avg_distance / total_distance if total_distance > 0 else 0
        
        return min(1.0, quality)
        
    def _calculate_diversification(self, weights: np.ndarray) -> float:
        """Calculate portfolio diversification (inverse HHI)"""
        hhi = np.sum(weights ** 2)
        return 1.0 / hhi if hhi > 0 else len(weights)
        
    def _check_design_constraints(self, values: List[float], 
                                constraints: List[Dict]) -> bool:
        """Check if design parameters satisfy constraints"""
        for constraint in constraints:
            # Simple linear constraint check
            constraint_value = sum(
                coeff * values[i]
                for i, coeff in enumerate(constraint.get('coefficients', []))
                if i < len(values)
            )
            
            if constraint['type'] == 'equality':
                if abs(constraint_value - constraint['rhs']) > 1e-6:
                    return False
            elif constraint['type'] == 'inequality':
                if constraint_value > constraint['rhs'] + 1e-6:
                    return False
                    
        return True
        
    def _calculate_improvement(self, current: float, baseline: float) -> float:
        """Calculate percentage improvement"""
        if baseline == 0:
            return 0.0
        return (baseline - current) / abs(baseline) * 100
        
    def _check_minimal_cover(self, cover: List[int], edges: List[Tuple]) -> bool:
        """Check if vertex cover is minimal"""
        # Try removing each vertex
        for vertex in cover:
            test_cover = [v for v in cover if v != vertex]
            # Check if still covers all edges
            all_covered = True
            for u, v in edges:
                if u not in test_cover and v not in test_cover:
                    all_covered = False
                    break
            if all_covered:
                return False  # Not minimal
        return True
        
    def _check_schedule_conflicts(self, schedule: Dict[str, List]) -> List[Dict]:
        """Check for scheduling conflicts"""
        conflicts = []
        
        for machine, tasks in schedule.items():
            # Sort by start time
            sorted_tasks = sorted(tasks, key=lambda x: x['start_time'])
            
            # Check for overlaps
            for i in range(len(sorted_tasks) - 1):
                task1 = sorted_tasks[i]
                task2 = sorted_tasks[i + 1]
                
                if task1['start_time'] + task1['duration'] > task2['start_time']:
                    conflicts.append({
                        'machine': machine,
                        'job1': task1['job'],
                        'job2': task2['job'],
                        'overlap': task1['start_time'] + task1['duration'] - task2['start_time']
                    })
                    
        return conflicts 