"""
Classical optimization algorithms to be used as part of hybrid workflows.
"""
import numpy as np
from typing import Dict, Any, Tuple, List
from scipy.optimize import minimize, linprog
import networkx as nx

from platformq_shared.logging_config import get_logger

logger = get_logger(__name__)

class ClassicalSolvers:
    """
    A collection of classical solvers for optimization problems.
    
    This class provides wrappers for common classical algorithms,
    which can be used for pre-processing, post-processing, or as
    standalone benchmarks within a hybrid quantum-classical workflow.
    """

    @staticmethod
    def solve_scipy_minimize(problem: Dict[str, Any]) -> Dict[str, Any]:
        """
        Solves a general nonlinear problem using scipy.optimize.minimize.
        
        Args:
            problem: A dictionary containing:
                - 'objective_function': A callable function to minimize.
                - 'initial_guess': An initial guess for the variables.
                - 'bounds': (optional) A sequence of (min, max) pairs for each variable.
                - 'constraints': (optional) A list of constraint dictionaries.
                - 'method': (optional) The optimization method to use (e.g., 'COBYLA', 'BFGS').
        
        Returns:
            A dictionary containing the solution.
        """
        logger.info(f"Starting classical optimization with SciPy Minimize (Method: {problem.get('method', 'Default')})")
        
        result = minimize(
            fun=problem['objective_function'],
            x0=problem['initial_guess'],
            method=problem.get('method', 'COBYLA'),
            bounds=problem.get('bounds'),
            constraints=problem.get('constraints', [])
        )
        
        logger.info(f"SciPy Minimize finished. Success: {result.success}, Value: {result.fun:.4f}")

        return {
            'solution_vector': result.x.tolist(),
            'optimal_value': float(result.fun),
            'success': result.success,
            'message': result.message,
            'num_iterations': result.get('nit', 0)
        }

    @staticmethod
    def solve_greedy_tsp(problem: Dict[str, Any]) -> Dict[str, Any]:
        """
        Solves the Traveling Salesperson Problem using a greedy nearest-neighbor heuristic.
        
        Args:
            problem: A dictionary containing:
                - 'distance_matrix': An NxN numpy array of distances.
                - 'start_node': (optional) The node to start the tour from.
        
        Returns:
            A dictionary containing the tour and its total distance.
        """
        distance_matrix = np.array(problem['distance_matrix'])
        num_cities = distance_matrix.shape[0]
        start_node = problem.get('start_node', 0)

        logger.info(f"Starting greedy TSP solver for {num_cities} cities from node {start_node}.")

        tour = [start_node]
        visited = {start_node}
        total_distance = 0.0
        
        current_city = start_node
        while len(visited) < num_cities:
            nearest_neighbor = -1
            min_distance = float('inf')
            
            for next_city in range(num_cities):
                if next_city not in visited:
                    distance = distance_matrix[current_city, next_city]
                    if distance < min_distance:
                        min_distance = distance
                        nearest_neighbor = next_city
            
            if nearest_neighbor != -1:
                tour.append(nearest_neighbor)
                visited.add(nearest_neighbor)
                total_distance += min_distance
                current_city = nearest_neighbor
            else:
                # Should not happen in a fully connected graph
                break

        # Return to start node
        total_distance += distance_matrix[tour[-1], start_node]
        
        logger.info(f"Greedy TSP finished. Tour distance: {total_distance:.4f}")

        return {
            'solution_vector': tour,
            'optimal_value': float(total_distance),
            'start_node': start_node
        }

    @staticmethod
    def solve_greedy_maxcut(problem: Dict[str, Any]) -> Dict[str, Any]:
        """
        Solves the Max-Cut problem using a simple greedy heuristic.
        
        Args:
            problem: A dictionary containing:
                - 'graph': A NetworkX graph object.
        
        Returns:
            A dictionary with the partition and the resulting cut size.
        """
        graph = problem['graph']
        num_nodes = graph.number_of_nodes()
        logger.info(f"Starting greedy Max-Cut solver for a graph with {num_nodes} nodes.")

        partition = {}
        set_a = set()
        set_b = set()
        cut_size = 0

        # Greedily assign each node to the set that maximizes the increase in cut size
        for node in graph.nodes():
            gain_a = 0
            gain_b = 0
            for neighbor in graph.neighbors(node):
                if neighbor in set_a:
                    gain_b += graph[node][neighbor].get('weight', 1)
                elif neighbor in set_b:
                    gain_a += graph[node][neighbor].get('weight', 1)
            
            if gain_a >= gain_b:
                set_a.add(node)
                partition[node] = 0
                cut_size += gain_a - gain_b
            else:
                set_b.add(node)
                partition[node] = 1
                cut_size += gain_b - gain_a

        logger.info(f"Greedy Max-Cut finished. Cut size: {cut_size}")
        
        # Convert partition to a bitstring-like vector
        solution_vector = [partition[node] for node in sorted(graph.nodes())]

        return {
            'solution_vector': solution_vector,
            'optimal_value': int(cut_size),
            'partition': {'set_a': list(set_a), 'set_b': list(set_b)}
        }

    @staticmethod
    def solve_greedy_knapsack(problem: Dict[str, Any]) -> Dict[str, Any]:
        """
        Solves the 0/1 Knapsack problem using a greedy heuristic based on value-to-weight ratio.
        
        Args:
            problem: A dictionary containing:
                - 'weights': A list of item weights.
                - 'values': A list of item values.
                - 'max_weight': The maximum capacity of the knapsack.
        
        Returns:
            A dictionary with the selected items, total value, and total weight.
        """
        weights = problem['weights']
        values = problem['values']
        max_weight = problem['max_weight']
        num_items = len(weights)

        logger.info(f"Starting greedy Knapsack solver for {num_items} items with capacity {max_weight}.")

        # Create items with their ratios
        items = [
            {'index': i, 'weight': weights[i], 'value': values[i], 'ratio': values[i] / weights[i] if weights[i] > 0 else float('inf')}
            for i in range(num_items)
        ]
        
        # Sort items by value-to-weight ratio in descending order
        items.sort(key=lambda x: x['ratio'], reverse=True)
        
        knapsack_items_indices = []
        solution_vector = [0] * num_items
        total_weight = 0
        total_value = 0

        for item in items:
            if total_weight + item['weight'] <= max_weight:
                knapsack_items_indices.append(item['index'])
                solution_vector[item['index']] = 1
                total_weight += item['weight']
                total_value += item['value']

        logger.info(f"Greedy Knapsack finished. Total value: {total_value}, Total weight: {total_weight}")

        return {
            'solution_vector': solution_vector,
            'optimal_value': int(total_value),
            'total_weight': int(total_weight),
            'selected_items': sorted(knapsack_items_indices)
        }

    @staticmethod
    def solve_dijkstra(problem: Dict[str, Any]) -> Dict[str, Any]:
        """
        Finds the shortest path in a graph using Dijkstra's algorithm.
        
        This is useful for problems that can be mapped to shortest path finding.
        
        Args:
            problem: A dictionary containing:
                - 'graph': A NetworkX graph object.
                - 'source': The starting node.
                - 'target': The destination node.
        
        Returns:
            A dictionary with the shortest path and its length.
        """
        graph = problem['graph']
        source = problem['source']
        target = problem['target']

        logger.info(f"Finding shortest path from {source} to {target} using Dijkstra.")

        try:
            length, path = nx.single_source_dijkstra(graph, source, target)
            logger.info(f"Dijkstra finished. Path length: {length:.4f}")
            return {
                'solution_vector': path,
                'optimal_value': float(length)
            }
        except nx.NetworkXNoPath:
            logger.warning(f"No path found between {source} and {target}.")
            return {
                'solution_vector': [],
                'optimal_value': float('inf'),
                'error': f"No path exists between {source} and {target}"
            }

    @staticmethod
    def solve_linear_program(problem: Dict[str, Any]) -> Dict[str, Any]:
        """
        Solves a linear programming problem using scipy.optimize.linprog.
        
        This can be used to solve relaxed versions of integer programming problems.
        
        Args:
            problem: A dictionary matching the parameters for scipy.optimize.linprog:
                - 'c': The coefficients of the linear objective function to be minimized.
                - 'A_ub': (optional) The inequality constraint matrix.
                - 'b_ub': (optional) The inequality constraint vector.
                - 'A_eq': (optional) The equality constraint matrix.
                - 'b_eq': (optional) The equality constraint vector.
                - 'bounds': (optional) A sequence of (min, max) pairs for each variable.
        
        Returns:
            A dictionary containing the solution.
        """
        logger.info("Starting classical Linear Programming solver.")

        result = linprog(
            c=problem['c'],
            A_ub=problem.get('A_ub'),
            b_ub=problem.get('b_ub'),
            A_eq=problem.get('A_eq'),
            b_eq=problem.get('b_eq'),
            bounds=problem.get('bounds')
        )

        logger.info(f"Linear Programming finished. Success: {result.success}, Value: {result.fun:.4f}")

        return {
            'solution_vector': result.x.tolist() if result.success else [],
            'optimal_value': float(result.fun) if result.success else float('inf'),
            'success': result.success,
            'message': result.message,
            'num_iterations': result.get('nit', 0)
        } 