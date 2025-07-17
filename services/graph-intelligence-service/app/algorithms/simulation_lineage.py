"""
Simulation Lineage Tracking Algorithm

Tracks the complete lineage of multi-physics simulations including:
- Parameter evolution
- Convergence history
- Coupling changes
- Optimization decisions
"""

import logging
from typing import Dict, List, Any, Optional, Tuple
from datetime import datetime
from gremlin_python.process.traversal import T
from gremlin_python.process.graph_traversal import __
from gremlin_python.structure.graph import Graph
from gremlin_python.driver.driver_remote_connection import DriverRemoteConnection
import json
import numpy as np

logger = logging.getLogger(__name__)


class SimulationLineageTracker:
    """Track and analyze simulation lineage in JanusGraph"""
    
    def __init__(self, gremlin_url: str):
        self.gremlin_url = gremlin_url
        self.g = None
        self._connect()
        
    def _connect(self):
        """Connect to JanusGraph"""
        self.g = Graph().traversal().withRemote(
            DriverRemoteConnection(self.gremlin_url, 'g')
        )
        
    def create_simulation_node(self, simulation_data: Dict[str, Any]) -> str:
        """Create a new simulation node in the graph"""
        simulation_id = simulation_data.get('simulation_id')
        
        # Create simulation vertex
        vertex = self.g.addV('simulation') \
            .property('simulation_id', simulation_id) \
            .property('type', simulation_data.get('type', 'multi_physics')) \
            .property('created_at', datetime.utcnow().isoformat()) \
            .property('status', 'initialized') \
            .property('domains', json.dumps(simulation_data.get('domains', []))) \
            .property('coupling_type', simulation_data.get('coupling_type', 'one_way')) \
            .next()
            
        # Add domains as separate vertices
        for domain in simulation_data.get('domains', []):
            domain_vertex = self.g.addV('simulation_domain') \
                .property('domain_id', f"{simulation_id}_{domain['domain_id']}") \
                .property('physics_type', domain.get('physics_type')) \
                .property('solver', domain.get('solver')) \
                .property('initial_parameters', json.dumps(domain.get('parameters', {}))) \
                .next()
                
            # Link domain to simulation
            self.g.V(vertex).addE('has_domain').to(V(domain_vertex)).next()
            
        return simulation_id
        
    def track_parameter_change(self, simulation_id: str, parameter_change: Dict[str, Any]):
        """Track parameter changes in simulation"""
        # Find simulation vertex
        sim_vertex = self.g.V().has('simulation', 'simulation_id', simulation_id).next()
        
        # Create parameter change vertex
        change_vertex = self.g.addV('parameter_change') \
            .property('change_id', f"{simulation_id}_param_{datetime.utcnow().timestamp()}") \
            .property('timestamp', datetime.utcnow().isoformat()) \
            .property('parameter_name', parameter_change.get('name')) \
            .property('old_value', str(parameter_change.get('old_value'))) \
            .property('new_value', str(parameter_change.get('new_value'))) \
            .property('domain', parameter_change.get('domain', 'global')) \
            .property('changed_by', parameter_change.get('user_id', 'system')) \
            .next()
            
        # Link to simulation
        self.g.V(sim_vertex).addE('parameter_changed').to(V(change_vertex)) \
            .property('iteration', parameter_change.get('iteration', 0)) \
            .next()
            
        # If this change was triggered by optimization
        if parameter_change.get('optimization_triggered'):
            opt_data = parameter_change.get('optimization_data', {})
            opt_vertex = self.g.addV('optimization_decision') \
                .property('decision_id', f"{simulation_id}_opt_{datetime.utcnow().timestamp()}") \
                .property('algorithm', opt_data.get('algorithm', 'unknown')) \
                .property('objective_value', opt_data.get('objective_value', 0)) \
                .property('quantum_metrics', json.dumps(opt_data.get('quantum_metrics', {}))) \
                .next()
                
            self.g.V(change_vertex).addE('triggered_by').to(V(opt_vertex)).next()
            
    def track_convergence_milestone(self, simulation_id: str, convergence_data: Dict[str, Any]):
        """Track convergence milestones"""
        sim_vertex = self.g.V().has('simulation', 'simulation_id', simulation_id).next()
        
        # Create convergence milestone
        milestone_vertex = self.g.addV('convergence_milestone') \
            .property('milestone_id', f"{simulation_id}_conv_{convergence_data.get('iteration')}") \
            .property('iteration', convergence_data.get('iteration')) \
            .property('timestamp', datetime.utcnow().isoformat()) \
            .property('residual', convergence_data.get('residual')) \
            .property('convergence_rate', convergence_data.get('convergence_rate')) \
            .property('is_converged', convergence_data.get('is_converged', False)) \
            .property('domain_residuals', json.dumps(convergence_data.get('domain_residuals', {}))) \
            .next()
            
        # Link to simulation
        self.g.V(sim_vertex).addE('reached_milestone').to(V(milestone_vertex)).next()
        
        # If convergence triggered coupling change
        if convergence_data.get('coupling_adjusted'):
            self._track_coupling_change(sim_vertex, convergence_data)
            
    def _track_coupling_change(self, sim_vertex, convergence_data: Dict[str, Any]):
        """Track coupling parameter changes"""
        coupling_vertex = self.g.addV('coupling_change') \
            .property('change_id', f"coupling_{datetime.utcnow().timestamp()}") \
            .property('timestamp', datetime.utcnow().isoformat()) \
            .property('reason', convergence_data.get('adjustment_reason', 'slow_convergence')) \
            .property('coupling_matrix', json.dumps(convergence_data.get('new_coupling_matrix', []))) \
            .property('enabled_couplings', json.dumps(convergence_data.get('enabled_couplings', []))) \
            .next()
            
        self.g.V(sim_vertex).addE('coupling_adjusted').to(V(coupling_vertex)).next()
        
    def get_simulation_lineage(self, simulation_id: str) -> Dict[str, Any]:
        """Get complete lineage of a simulation"""
        # Get simulation vertex
        sim_data = self.g.V().has('simulation', 'simulation_id', simulation_id) \
            .valueMap(True).next()
            
        # Get all parameter changes
        param_changes = self.g.V().has('simulation', 'simulation_id', simulation_id) \
            .out('parameter_changed').valueMap(True).toList()
            
        # Get convergence history
        convergence_history = self.g.V().has('simulation', 'simulation_id', simulation_id) \
            .out('reached_milestone').order().by('iteration').valueMap(True).toList()
            
        # Get optimization decisions
        opt_decisions = self.g.V().has('simulation', 'simulation_id', simulation_id) \
            .out('parameter_changed').out('triggered_by').valueMap(True).toList()
            
        # Get coupling changes
        coupling_changes = self.g.V().has('simulation', 'simulation_id', simulation_id) \
            .out('coupling_adjusted').valueMap(True).toList()
            
        return {
            'simulation': self._clean_vertex_data(sim_data),
            'parameter_changes': [self._clean_vertex_data(p) for p in param_changes],
            'convergence_history': [self._clean_vertex_data(c) for c in convergence_history],
            'optimization_decisions': [self._clean_vertex_data(o) for o in opt_decisions],
            'coupling_changes': [self._clean_vertex_data(c) for c in coupling_changes]
        }
        
    def find_similar_simulations(self, simulation_id: str, similarity_threshold: float = 0.8) -> List[Dict[str, Any]]:
        """Find simulations with similar parameters and convergence patterns"""
        # Get reference simulation data
        ref_sim = self.g.V().has('simulation', 'simulation_id', simulation_id).next()
        ref_domains = self.g.V(ref_sim).out('has_domain').valueMap().toList()
        
        # Find simulations with same physics domains
        similar_sims = self.g.V().hasLabel('simulation') \
            .where(__.not_(__.has('simulation_id', simulation_id))) \
            .where(__.values('domains').is_(P.containing(ref_domains[0]['physics_type']))) \
            .toList()
            
        results = []
        for sim in similar_sims:
            sim_data = self.g.V(sim).valueMap(True).next()
            
            # Calculate similarity based on:
            # 1. Domain types
            # 2. Initial parameters
            # 3. Convergence patterns
            similarity = self._calculate_simulation_similarity(ref_sim, sim)
            
            if similarity >= similarity_threshold:
                results.append({
                    'simulation_id': sim_data.get('simulation_id')[0],
                    'similarity_score': similarity,
                    'convergence_comparison': self._compare_convergence_patterns(ref_sim, sim)
                })
                
        return sorted(results, key=lambda x: x['similarity_score'], reverse=True)
        
    def _calculate_simulation_similarity(self, sim1_vertex, sim2_vertex) -> float:
        """Calculate similarity between two simulations"""
        # Get parameter histories
        params1 = self.g.V(sim1_vertex).out('parameter_changed').valueMap().toList()
        params2 = self.g.V(sim2_vertex).out('parameter_changed').valueMap().toList()
        
        # Get convergence patterns
        conv1 = self.g.V(sim1_vertex).out('reached_milestone').values('residual').toList()
        conv2 = self.g.V(sim2_vertex).out('reached_milestone').values('residual').toList()
        
        # Calculate parameter similarity
        param_similarity = self._calculate_parameter_similarity(params1, params2)
        
        # Calculate convergence pattern similarity
        conv_similarity = self._calculate_convergence_similarity(conv1, conv2)
        
        # Weighted average
        return 0.6 * param_similarity + 0.4 * conv_similarity
        
    def _calculate_parameter_similarity(self, params1: List[Dict], params2: List[Dict]) -> float:
        """Calculate parameter similarity between simulations"""
        if not params1 or not params2:
            return 0.0
            
        # Extract parameter names and values
        param_set1 = {p['parameter_name'][0] for p in params1}
        param_set2 = {p['parameter_name'][0] for p in params2}
        
        # Jaccard similarity for parameter names
        intersection = param_set1.intersection(param_set2)
        union = param_set1.union(param_set2)
        
        if not union:
            return 0.0
            
        return len(intersection) / len(union)
        
    def _calculate_convergence_similarity(self, conv1: List[float], conv2: List[float]) -> float:
        """Calculate convergence pattern similarity"""
        if not conv1 or not conv2:
            return 0.0
            
        # Normalize lengths
        min_len = min(len(conv1), len(conv2))
        conv1_norm = conv1[:min_len]
        conv2_norm = conv2[:min_len]
        
        # Calculate correlation coefficient
        if len(conv1_norm) < 2:
            return 0.0
            
        correlation = np.corrcoef(conv1_norm, conv2_norm)[0, 1]
        
        # Convert to similarity score (0 to 1)
        return (correlation + 1) / 2
        
    def _compare_convergence_patterns(self, sim1_vertex, sim2_vertex) -> Dict[str, Any]:
        """Compare convergence patterns between simulations"""
        conv1 = self.g.V(sim1_vertex).out('reached_milestone') \
            .order().by('iteration').valueMap().toList()
        conv2 = self.g.V(sim2_vertex).out('reached_milestone') \
            .order().by('iteration').valueMap().toList()
            
        if not conv1 or not conv2:
            return {}
            
        # Extract residuals
        res1 = [c['residual'][0] for c in conv1]
        res2 = [c['residual'][0] for c in conv2]
        
        # Calculate convergence rates
        rate1 = self._calculate_convergence_rate(res1)
        rate2 = self._calculate_convergence_rate(res2)
        
        return {
            'convergence_rate_sim1': rate1,
            'convergence_rate_sim2': rate2,
            'rate_difference': abs(rate1 - rate2),
            'final_residual_sim1': res1[-1] if res1 else None,
            'final_residual_sim2': res2[-1] if res2 else None,
            'iterations_to_converge_sim1': len(res1),
            'iterations_to_converge_sim2': len(res2)
        }
        
    def _calculate_convergence_rate(self, residuals: List[float]) -> float:
        """Calculate convergence rate from residual history"""
        if len(residuals) < 2:
            return 0.0
            
        # Log scale for residuals
        log_residuals = np.log10(np.array(residuals) + 1e-10)
        
        # Linear regression to find slope
        x = np.arange(len(log_residuals))
        slope = np.polyfit(x, log_residuals, 1)[0]
        
        return slope
        
    def get_optimization_impact_analysis(self, simulation_id: str) -> Dict[str, Any]:
        """Analyze the impact of optimization decisions on simulation convergence"""
        # Get all optimization decisions
        opt_decisions = self.g.V().has('simulation', 'simulation_id', simulation_id) \
            .out('parameter_changed').out('triggered_by') \
            .order().by('timestamp').toList()
            
        impacts = []
        
        for opt_vertex in opt_decisions:
            opt_data = self.g.V(opt_vertex).valueMap(True).next()
            
            # Find parameter changes triggered by this optimization
            param_changes = self.g.V(opt_vertex).in_('triggered_by').valueMap().toList()
            
            # Find convergence milestones after this optimization
            opt_timestamp = opt_data.get('timestamp')[0]
            subsequent_milestones = self.g.V().has('simulation', 'simulation_id', simulation_id) \
                .out('reached_milestone') \
                .has('timestamp', P.gte(opt_timestamp)) \
                .order().by('iteration').limit(10).valueMap().toList()
                
            # Calculate impact metrics
            if subsequent_milestones:
                residuals = [m['residual'][0] for m in subsequent_milestones]
                improvement_rate = self._calculate_convergence_rate(residuals)
                
                impacts.append({
                    'optimization_algorithm': opt_data.get('algorithm', ['unknown'])[0],
                    'timestamp': opt_timestamp,
                    'parameter_changes': len(param_changes),
                    'convergence_improvement_rate': improvement_rate,
                    'objective_value': opt_data.get('objective_value', [0])[0],
                    'subsequent_residuals': residuals
                })
                
        return {
            'simulation_id': simulation_id,
            'total_optimizations': len(opt_decisions),
            'optimization_impacts': impacts,
            'most_effective_algorithm': self._find_most_effective_algorithm(impacts)
        }
        
    def _find_most_effective_algorithm(self, impacts: List[Dict[str, Any]]) -> Optional[str]:
        """Find the most effective optimization algorithm based on convergence improvement"""
        if not impacts:
            return None
            
        best_algorithm = None
        best_improvement = float('inf')
        
        for impact in impacts:
            if impact['convergence_improvement_rate'] < best_improvement:
                best_improvement = impact['convergence_improvement_rate']
                best_algorithm = impact['optimization_algorithm']
                
        return best_algorithm
        
    def _clean_vertex_data(self, vertex_data: Dict) -> Dict[str, Any]:
        """Clean vertex data from Gremlin format"""
        cleaned = {}
        for key, value in vertex_data.items():
            if key == T.id or key == T.label:
                cleaned[str(key)] = value
            elif isinstance(value, list) and len(value) == 1:
                cleaned[key] = value[0]
            else:
                cleaned[key] = value
        return cleaned
        
    def create_simulation_graph_visualization(self, simulation_id: str) -> Dict[str, Any]:
        """Create visualization data for simulation lineage graph"""
        nodes = []
        edges = []
        
        # Get simulation node
        sim_vertex = self.g.V().has('simulation', 'simulation_id', simulation_id).next()
        sim_data = self.g.V(sim_vertex).valueMap(True).next()
        
        nodes.append({
            'id': str(sim_vertex.id),
            'label': f"Simulation {simulation_id}",
            'type': 'simulation',
            'data': self._clean_vertex_data(sim_data)
        })
        
        # Get all connected vertices
        # Domains
        domains = self.g.V(sim_vertex).out('has_domain').toList()
        for domain in domains:
            domain_data = self.g.V(domain).valueMap(True).next()
            nodes.append({
                'id': str(domain.id),
                'label': f"Domain: {domain_data.get('physics_type', ['unknown'])[0]}",
                'type': 'domain',
                'data': self._clean_vertex_data(domain_data)
            })
            edges.append({
                'source': str(sim_vertex.id),
                'target': str(domain.id),
                'label': 'has_domain'
            })
            
        # Parameter changes
        param_changes = self.g.V(sim_vertex).out('parameter_changed').toList()
        for i, param in enumerate(param_changes):
            param_data = self.g.V(param).valueMap(True).next()
            nodes.append({
                'id': str(param.id),
                'label': f"Param: {param_data.get('parameter_name', ['unknown'])[0]}",
                'type': 'parameter_change',
                'data': self._clean_vertex_data(param_data)
            })
            edges.append({
                'source': str(sim_vertex.id),
                'target': str(param.id),
                'label': 'parameter_changed'
            })
            
        # Convergence milestones
        milestones = self.g.V(sim_vertex).out('reached_milestone').toList()
        for milestone in milestones:
            milestone_data = self.g.V(milestone).valueMap(True).next()
            nodes.append({
                'id': str(milestone.id),
                'label': f"Iteration {milestone_data.get('iteration', [0])[0]}",
                'type': 'convergence',
                'data': self._clean_vertex_data(milestone_data)
            })
            edges.append({
                'source': str(sim_vertex.id),
                'target': str(milestone.id),
                'label': 'reached_milestone'
            })
            
        return {
            'nodes': nodes,
            'edges': edges,
            'statistics': {
                'total_nodes': len(nodes),
                'total_edges': len(edges),
                'parameter_changes': len(param_changes),
                'convergence_milestones': len(milestones)
            }
        }


# Import P for predicates
from gremlin_python.process.traversal import P
# Import V for vertex references
from gremlin_python.process.graph_traversal import V 