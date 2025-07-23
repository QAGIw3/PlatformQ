"""
Quantum Computing Utilities

Common utilities for quantum computing operations.
"""

from typing import Dict, Any, List, Optional, Tuple, Union
from enum import Enum
import numpy as np
from dataclasses import dataclass
import logging

logger = logging.getLogger(__name__)


class BackendType(str, Enum):
    """Quantum backend types"""
    SIMULATOR = "simulator"
    QPU = "qpu"
    HYBRID = "hybrid"
    ANNEALER = "annealer"


class ProblemType(str, Enum):
    """Quantum optimization problem types"""
    QUBO = "qubo"
    ISING = "ising"
    MAX_CUT = "max_cut"
    TSP = "tsp"
    PORTFOLIO = "portfolio"
    SCHEDULING = "scheduling"
    GRAPH_COLORING = "graph_coloring"
    KNAPSACK = "knapsack"
    VEHICLE_ROUTING = "vehicle_routing"
    FACILITY_LOCATION = "facility_location"
    CUSTOM = "custom"


@dataclass
class QuantumCircuitMetrics:
    """Metrics for quantum circuits"""
    num_qubits: int
    depth: int
    gate_count: int
    two_qubit_gates: int
    measurement_count: int
    estimated_runtime: Optional[float] = None


def qubo_to_ising(Q: Dict[Tuple[int, int], float]) -> Tuple[Dict[Tuple[int, int], float], float]:
    """
    Convert QUBO to Ising model.
    
    Args:
        Q: QUBO matrix as dictionary {(i,j): value}
        
    Returns:
        Tuple of (J dictionary, offset)
    """
    # Extract linear and quadratic terms
    linear = {}
    quadratic = {}
    
    for (i, j), val in Q.items():
        if i == j:
            linear[i] = val
        else:
            # Ensure i < j for consistency
            if i > j:
                i, j = j, i
            if (i, j) in quadratic:
                quadratic[(i, j)] += val
            else:
                quadratic[(i, j)] = val
    
    # Convert to Ising
    J = {}
    h = {}
    offset = 0
    
    # Linear terms
    for i, val in linear.items():
        h[i] = val / 2
        offset += val / 2
    
    # Quadratic terms
    for (i, j), val in quadratic.items():
        J[(i, j)] = val / 4
        
        # Update linear terms
        if i in h:
            h[i] += val / 4
        else:
            h[i] = val / 4
            
        if j in h:
            h[j] += val / 4
        else:
            h[j] = val / 4
            
        offset += val / 4
    
    # Combine into single dictionary
    ising = {}
    for i, val in h.items():
        ising[(i, i)] = val
    for (i, j), val in J.items():
        ising[(i, j)] = val
    
    return ising, offset


def ising_to_qubo(h: Dict[int, float], J: Dict[Tuple[int, int], float]) -> Dict[Tuple[int, int], float]:
    """
    Convert Ising model to QUBO.
    
    Args:
        h: Linear terms dictionary
        J: Quadratic terms dictionary
        
    Returns:
        QUBO matrix as dictionary
    """
    Q = {}
    
    # Linear terms
    for i, val in h.items():
        Q[(i, i)] = -2 * val
    
    # Quadratic terms
    for (i, j), val in J.items():
        # Add to off-diagonal
        Q[(i, j)] = 4 * val
        
        # Update diagonal
        if (i, i) in Q:
            Q[(i, i)] -= 2 * val
        else:
            Q[(i, i)] = -2 * val
            
        if (j, j) in Q:
            Q[(j, j)] -= 2 * val
        else:
            Q[(j, j)] = -2 * val
    
    return Q


def create_max_cut_qubo(graph_edges: List[Tuple[int, int]], weights: Optional[List[float]] = None) -> Dict[Tuple[int, int], float]:
    """
    Create QUBO for Max-Cut problem.
    
    Args:
        graph_edges: List of edges (i, j)
        weights: Optional edge weights
        
    Returns:
        QUBO matrix as dictionary
    """
    if weights is None:
        weights = [1.0] * len(graph_edges)
    
    Q = {}
    
    for (i, j), w in zip(graph_edges, weights):
        # Penalize same partition
        if (i, i) in Q:
            Q[(i, i)] -= w
        else:
            Q[(i, i)] = -w
            
        if (j, j) in Q:
            Q[(j, j)] -= w
        else:
            Q[(j, j)] = -w
            
        # Reward different partition
        if i < j:
            Q[(i, j)] = 2 * w
        else:
            Q[(j, i)] = 2 * w
    
    return Q


def create_tsp_qubo(distances: np.ndarray, penalty: float = 10.0) -> Dict[Tuple[int, int], float]:
    """
    Create QUBO for Traveling Salesman Problem.
    
    Args:
        distances: Distance matrix
        penalty: Constraint violation penalty
        
    Returns:
        QUBO matrix as dictionary
    """
    n = len(distances)
    Q = {}
    
    # Variables: x[i,j] = 1 if city i is visited at position j
    def var_idx(city, pos):
        return city * n + pos
    
    # Objective: minimize total distance
    for pos in range(n):
        next_pos = (pos + 1) % n
        for i in range(n):
            for j in range(n):
                if i != j:
                    idx1 = var_idx(i, pos)
                    idx2 = var_idx(j, next_pos)
                    if idx1 < idx2:
                        Q[(idx1, idx2)] = distances[i][j]
                    else:
                        Q[(idx2, idx1)] = distances[i][j]
    
    # Constraint 1: Each city visited exactly once
    for city in range(n):
        # Linear penalty for not visiting
        for pos in range(n):
            idx = var_idx(city, pos)
            if (idx, idx) in Q:
                Q[(idx, idx)] -= penalty
            else:
                Q[(idx, idx)] = -penalty
        
        # Quadratic penalty for visiting multiple times
        for pos1 in range(n):
            for pos2 in range(pos1 + 1, n):
                idx1 = var_idx(city, pos1)
                idx2 = var_idx(city, pos2)
                if idx1 < idx2:
                    Q[(idx1, idx2)] = 2 * penalty
                else:
                    Q[(idx2, idx1)] = 2 * penalty
    
    # Constraint 2: Each position has exactly one city
    for pos in range(n):
        # Linear penalty
        for city in range(n):
            idx = var_idx(city, pos)
            if (idx, idx) in Q:
                Q[(idx, idx)] -= penalty
            else:
                Q[(idx, idx)] = -penalty
        
        # Quadratic penalty
        for city1 in range(n):
            for city2 in range(city1 + 1, n):
                idx1 = var_idx(city1, pos)
                idx2 = var_idx(city2, pos)
                if idx1 < idx2:
                    Q[(idx1, idx2)] = 2 * penalty
                else:
                    Q[(idx2, idx1)] = 2 * penalty
    
    return Q


def create_portfolio_qubo(
    returns: np.ndarray,
    covariance: np.ndarray,
    risk_weight: float = 0.5,
    budget_constraint: Optional[float] = None
) -> Dict[Tuple[int, int], float]:
    """
    Create QUBO for portfolio optimization.
    
    Args:
        returns: Expected returns for each asset
        covariance: Covariance matrix
        risk_weight: Weight for risk vs return (0-1)
        budget_constraint: Optional budget constraint
        
    Returns:
        QUBO matrix as dictionary
    """
    n = len(returns)
    Q = {}
    
    # Objective: maximize return - risk_weight * variance
    # Linear terms (returns)
    for i in range(n):
        Q[(i, i)] = -(1 - risk_weight) * returns[i]
    
    # Quadratic terms (risk)
    for i in range(n):
        for j in range(n):
            if i <= j:
                val = risk_weight * covariance[i, j]
                if i == j:
                    if (i, i) in Q:
                        Q[(i, i)] += val
                    else:
                        Q[(i, i)] = val
                else:
                    Q[(i, j)] = 2 * val
    
    # Budget constraint if specified
    if budget_constraint is not None:
        penalty = max(abs(returns)) * 10  # Large penalty
        
        # Add auxiliary variables for inequality constraint
        # This is a simplified version - real implementation would be more complex
        logger.warning("Budget constraint in QUBO is simplified - consider using specialized solver")
    
    return Q


def decode_solution(solution: Dict[int, int], problem_type: ProblemType, **kwargs) -> Any:
    """
    Decode binary solution based on problem type.
    
    Args:
        solution: Binary solution {variable: value}
        problem_type: Type of problem
        **kwargs: Problem-specific parameters
        
    Returns:
        Decoded solution in problem-specific format
    """
    if problem_type == ProblemType.MAX_CUT:
        # Return partition sets
        set_0 = [i for i, val in solution.items() if val == 0]
        set_1 = [i for i, val in solution.items() if val == 1]
        return {"partition_0": set_0, "partition_1": set_1}
    
    elif problem_type == ProblemType.TSP:
        n = kwargs.get("num_cities", int(np.sqrt(len(solution))))
        tour = []
        
        # Extract tour from binary variables
        for pos in range(n):
            for city in range(n):
                idx = city * n + pos
                if solution.get(idx, 0) == 1:
                    tour.append(city)
                    break
        
        return {"tour": tour}
    
    elif problem_type == ProblemType.PORTFOLIO:
        selected = [i for i, val in solution.items() if val == 1]
        return {"selected_assets": selected}
    
    else:
        # Return raw solution for custom problems
        return {"solution": solution}


def calculate_circuit_metrics(circuit) -> QuantumCircuitMetrics:
    """
    Calculate metrics for a quantum circuit.
    
    Args:
        circuit: Quantum circuit (format depends on framework)
        
    Returns:
        QuantumCircuitMetrics object
    """
    # This is a placeholder - actual implementation depends on quantum framework
    # (Qiskit, Cirq, PennyLane, etc.)
    
    logger.warning("Circuit metrics calculation not implemented for generic circuit")
    
    return QuantumCircuitMetrics(
        num_qubits=0,
        depth=0,
        gate_count=0,
        two_qubit_gates=0,
        measurement_count=0
    )


def estimate_quantum_advantage(
    problem_size: int,
    problem_type: ProblemType,
    classical_complexity: Optional[str] = None
) -> Dict[str, Any]:
    """
    Estimate potential quantum advantage for a problem.
    
    Args:
        problem_size: Size of the problem
        problem_type: Type of problem
        classical_complexity: Known classical complexity (e.g., "O(n!)")
        
    Returns:
        Dictionary with advantage estimates
    """
    estimates = {
        "problem_size": problem_size,
        "problem_type": problem_type.value,
        "quantum_feasible": True,
        "estimated_qubits": problem_size,
        "estimated_depth": None,
        "quantum_advantage": "unknown"
    }
    
    # Problem-specific estimates
    if problem_type == ProblemType.TSP:
        estimates["estimated_qubits"] = problem_size ** 2
        estimates["quantum_feasible"] = problem_size <= 20
        estimates["quantum_advantage"] = "potential" if problem_size > 10 else "unlikely"
        
    elif problem_type == ProblemType.MAX_CUT:
        estimates["estimated_qubits"] = problem_size
        estimates["quantum_feasible"] = problem_size <= 100
        estimates["quantum_advantage"] = "proven for certain graphs"
        
    elif problem_type == ProblemType.PORTFOLIO:
        estimates["estimated_qubits"] = problem_size
        estimates["quantum_feasible"] = problem_size <= 50
        estimates["quantum_advantage"] = "potential for large portfolios"
    
    return estimates


__all__ = [
    "BackendType",
    "ProblemType",
    "QuantumCircuitMetrics",
    "qubo_to_ising",
    "ising_to_qubo",
    "create_max_cut_qubo",
    "create_tsp_qubo",
    "create_portfolio_qubo",
    "decode_solution",
    "calculate_circuit_metrics",
    "estimate_quantum_advantage"
] 