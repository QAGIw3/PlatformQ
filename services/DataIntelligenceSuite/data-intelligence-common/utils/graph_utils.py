"""
Graph Utilities

Common utilities for graph operations and algorithms.
"""

from typing import Dict, Any, List, Optional, Tuple, Set
from enum import Enum
import networkx as nx
import numpy as np
from dataclasses import dataclass

from ..monitoring import StructuredLogger

logger = StructuredLogger.get_logger(__name__)


class GraphType(str, Enum):
    """Types of graphs"""
    DIRECTED = "directed"
    UNDIRECTED = "undirected"
    WEIGHTED = "weighted"
    MULTIGRAPH = "multigraph"
    BIPARTITE = "bipartite"


@dataclass
class GraphMetrics:
    """Common graph metrics"""
    num_nodes: int
    num_edges: int
    density: float
    average_degree: float
    diameter: Optional[int] = None
    clustering_coefficient: Optional[float] = None
    connected_components: Optional[int] = None
    strongly_connected_components: Optional[int] = None


def calculate_graph_metrics(graph: nx.Graph) -> GraphMetrics:
    """
    Calculate common graph metrics.
    
    Args:
        graph: NetworkX graph
        
    Returns:
        GraphMetrics object with calculated metrics
    """
    metrics = GraphMetrics(
        num_nodes=graph.number_of_nodes(),
        num_edges=graph.number_of_edges(),
        density=nx.density(graph),
        average_degree=sum(dict(graph.degree()).values()) / graph.number_of_nodes() if graph.number_of_nodes() > 0 else 0
    )
    
    # Additional metrics for specific graph types
    if not graph.is_directed():
        metrics.clustering_coefficient = nx.average_clustering(graph)
        metrics.connected_components = nx.number_connected_components(graph)
        if nx.is_connected(graph):
            metrics.diameter = nx.diameter(graph)
    else:
        metrics.strongly_connected_components = nx.number_strongly_connected_components(graph)
    
    return metrics


def detect_communities(graph: nx.Graph, method: str = "louvain") -> Dict[Any, int]:
    """
    Detect communities in a graph.
    
    Args:
        graph: NetworkX graph
        method: Community detection method
        
    Returns:
        Dictionary mapping nodes to community IDs
    """
    import community  # python-louvain
    
    if method == "louvain":
        return community.best_partition(graph)
    elif method == "girvan_newman":
        from networkx.algorithms.community import girvan_newman
        communities = girvan_newman(graph)
        return _communities_to_dict(next(communities))
    else:
        raise ValueError(f"Unknown community detection method: {method}")


def _communities_to_dict(communities: List[Set]) -> Dict[Any, int]:
    """Convert community sets to node->community_id mapping"""
    result = {}
    for i, community in enumerate(communities):
        for node in community:
            result[node] = i
    return result


def calculate_centrality(
    graph: nx.Graph,
    centrality_type: str = "betweenness"
) -> Dict[Any, float]:
    """
    Calculate node centrality scores.
    
    Args:
        graph: NetworkX graph
        centrality_type: Type of centrality measure
        
    Returns:
        Dictionary mapping nodes to centrality scores
    """
    if centrality_type == "degree":
        return nx.degree_centrality(graph)
    elif centrality_type == "betweenness":
        return nx.betweenness_centrality(graph)
    elif centrality_type == "closeness":
        return nx.closeness_centrality(graph)
    elif centrality_type == "eigenvector":
        return nx.eigenvector_centrality(graph, max_iter=1000)
    elif centrality_type == "pagerank":
        return nx.pagerank(graph)
    else:
        raise ValueError(f"Unknown centrality type: {centrality_type}")


def find_shortest_paths(
    graph: nx.Graph,
    source: Any,
    target: Optional[Any] = None,
    weight: Optional[str] = None
) -> Dict[Any, List[Any]]:
    """
    Find shortest paths in a graph.
    
    Args:
        graph: NetworkX graph
        source: Source node
        target: Target node (if None, finds paths to all nodes)
        weight: Edge weight attribute name
        
    Returns:
        Dictionary mapping targets to shortest paths
    """
    if target is not None:
        # Single target
        try:
            if weight:
                path = nx.shortest_path(graph, source, target, weight=weight)
            else:
                path = nx.shortest_path(graph, source, target)
            return {target: path}
        except nx.NetworkXNoPath:
            return {target: []}
    else:
        # All targets
        if weight:
            paths = nx.single_source_dijkstra_path(graph, source, weight=weight)
        else:
            paths = nx.single_source_shortest_path(graph, source)
        return paths


def analyze_influence_propagation(
    graph: nx.Graph,
    seed_nodes: List[Any],
    propagation_prob: float = 0.1,
    max_iterations: int = 100
) -> Dict[Any, float]:
    """
    Analyze influence propagation using Independent Cascade model.
    
    Args:
        graph: NetworkX graph
        seed_nodes: Initial activated nodes
        propagation_prob: Probability of influence propagation
        max_iterations: Maximum iterations
        
    Returns:
        Dictionary mapping nodes to activation probabilities
    """
    activation_counts = {node: 0 for node in graph.nodes()}
    num_simulations = 100
    
    for _ in range(num_simulations):
        # Run one simulation
        active = set(seed_nodes)
        newly_active = set(seed_nodes)
        
        for _ in range(max_iterations):
            next_active = set()
            
            for node in newly_active:
                for neighbor in graph.neighbors(node):
                    if neighbor not in active and np.random.random() < propagation_prob:
                        next_active.add(neighbor)
                        active.add(neighbor)
            
            if not next_active:
                break
                
            newly_active = next_active
        
        # Count activations
        for node in active:
            activation_counts[node] += 1
    
    # Convert to probabilities
    return {node: count / num_simulations for node, count in activation_counts.items()}


def find_cliques(graph: nx.Graph, min_size: int = 3) -> List[Set[Any]]:
    """
    Find cliques in a graph.
    
    Args:
        graph: NetworkX graph
        min_size: Minimum clique size
        
    Returns:
        List of cliques (sets of nodes)
    """
    cliques = []
    for clique in nx.find_cliques(graph):
        if len(clique) >= min_size:
            cliques.append(set(clique))
    return cliques


def calculate_trust_scores(
    graph: nx.Graph,
    initial_trust: Optional[Dict[Any, float]] = None,
    damping: float = 0.85,
    max_iterations: int = 100,
    tolerance: float = 1e-6
) -> Dict[Any, float]:
    """
    Calculate trust scores using PageRank-like algorithm.
    
    Args:
        graph: NetworkX graph with trust edges
        initial_trust: Initial trust scores
        damping: Damping factor
        max_iterations: Maximum iterations
        tolerance: Convergence tolerance
        
    Returns:
        Dictionary mapping nodes to trust scores
    """
    # Initialize trust scores
    num_nodes = graph.number_of_nodes()
    if initial_trust:
        trust = initial_trust.copy()
    else:
        trust = {node: 1.0 / num_nodes for node in graph.nodes()}
    
    # Iterative trust propagation
    for _ in range(max_iterations):
        prev_trust = trust.copy()
        
        for node in graph.nodes():
            # Calculate incoming trust
            incoming_trust = 0.0
            for pred in graph.predecessors(node):
                out_degree = graph.out_degree(pred)
                if out_degree > 0:
                    edge_weight = graph[pred][node].get('weight', 1.0)
                    incoming_trust += prev_trust[pred] * edge_weight / out_degree
            
            # Update trust with damping
            trust[node] = (1 - damping) / num_nodes + damping * incoming_trust
        
        # Check convergence
        if all(abs(trust[n] - prev_trust[n]) < tolerance for n in graph.nodes()):
            break
    
    return trust


def graph_to_adjacency_matrix(graph: nx.Graph, nodelist: Optional[List[Any]] = None) -> np.ndarray:
    """
    Convert graph to adjacency matrix.
    
    Args:
        graph: NetworkX graph
        nodelist: Order of nodes in matrix
        
    Returns:
        NumPy array representing adjacency matrix
    """
    return nx.to_numpy_array(graph, nodelist=nodelist)


def adjacency_matrix_to_graph(
    matrix: np.ndarray,
    nodelist: Optional[List[Any]] = None,
    create_using: Optional[type] = None
) -> nx.Graph:
    """
    Convert adjacency matrix to graph.
    
    Args:
        matrix: Adjacency matrix
        nodelist: Node labels
        create_using: Graph type to create
        
    Returns:
        NetworkX graph
    """
    if nodelist is None:
        nodelist = list(range(matrix.shape[0]))
    
    if create_using is None:
        create_using = nx.DiGraph if not np.allclose(matrix, matrix.T) else nx.Graph
    
    return nx.from_numpy_array(matrix, nodelist=nodelist, create_using=create_using)


__all__ = [
    "GraphType",
    "GraphMetrics",
    "calculate_graph_metrics",
    "detect_communities",
    "calculate_centrality",
    "find_shortest_paths",
    "analyze_influence_propagation",
    "find_cliques",
    "calculate_trust_scores",
    "graph_to_adjacency_matrix",
    "adjacency_matrix_to_graph"
] 