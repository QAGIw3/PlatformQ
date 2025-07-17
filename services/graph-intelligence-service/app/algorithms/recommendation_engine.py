"""
Recommendation Engine for Simulation Parameters

Uses graph-based collaborative filtering and content-based filtering
to recommend optimal simulation parameters based on historical data.
"""

import logging
from typing import Dict, List, Any, Tuple, Optional
from datetime import datetime, timedelta
from gremlin_python.process.traversal import T, P
from gremlin_python.process.graph_traversal import __
from gremlin_python.structure.graph import Graph
from gremlin_python.driver.driver_remote_connection import DriverRemoteConnection
import numpy as np
from sklearn.metrics.pairwise import cosine_similarity
from sklearn.preprocessing import StandardScaler
import json

logger = logging.getLogger(__name__)


class SimulationRecommendationEngine:
    """Recommendation engine for simulation parameters using graph intelligence"""
    
    def __init__(self, gremlin_url: str):
        self.gremlin_url = gremlin_url
        self.g = None
        self._connect()
        
    def _connect(self):
        """Connect to JanusGraph"""
        self.g = Graph().traversal().withRemote(
            DriverRemoteConnection(self.gremlin_url, 'g')
        )
        
    def recommend_parameters(self, 
                           simulation_type: str,
                           physics_domains: List[str],
                           user_constraints: Dict[str, Any],
                           top_k: int = 5) -> List[Dict[str, Any]]:
        """Recommend simulation parameters based on successful historical simulations"""
        
        # Find successful simulations of same type
        successful_sims = self._find_successful_simulations(simulation_type, physics_domains)
        
        if not successful_sims:
            logger.warning(f"No successful simulations found for type {simulation_type}")
            return self._get_default_recommendations(simulation_type, physics_domains)
            
        # Apply collaborative filtering
        collab_recommendations = self._collaborative_filtering(
            successful_sims, user_constraints, top_k
        )
        
        # Apply content-based filtering
        content_recommendations = self._content_based_filtering(
            successful_sims, physics_domains, user_constraints, top_k
        )
        
        # Merge and rank recommendations
        final_recommendations = self._merge_recommendations(
            collab_recommendations, content_recommendations, top_k
        )
        
        return final_recommendations
        
    def _find_successful_simulations(self, 
                                   simulation_type: str,
                                   physics_domains: List[str]) -> List[Dict[str, Any]]:
        """Find successful simulations matching criteria"""
        
        # Query for simulations with good convergence
        simulations = self.g.V().hasLabel('simulation') \
            .has('type', simulation_type) \
            .where(
                __.out('reached_milestone')
                .has('is_converged', True)
                .count().is_(P.gt(0))
            ).toList()
            
        results = []
        for sim in simulations:
            sim_data = self.g.V(sim).valueMap(True).next()
            
            # Check if physics domains match
            sim_domains = json.loads(sim_data.get('domains', '[]')[0])
            domain_match = any(d in physics_domains for d in sim_domains)
            
            if domain_match:
                # Get final parameters
                final_params = self._get_final_parameters(sim)
                
                # Get convergence metrics
                convergence_metrics = self._get_convergence_metrics(sim)
                
                # Get optimization performance if any
                opt_performance = self._get_optimization_performance(sim)
                
                results.append({
                    'simulation_id': sim_data.get('simulation_id', [''])[0],
                    'parameters': final_params,
                    'convergence_metrics': convergence_metrics,
                    'optimization_performance': opt_performance,
                    'success_score': self._calculate_success_score(
                        convergence_metrics, opt_performance
                    )
                })
                
        return sorted(results, key=lambda x: x['success_score'], reverse=True)
        
    def _get_final_parameters(self, sim_vertex) -> Dict[str, Any]:
        """Get final parameter values for a simulation"""
        # Get all parameter changes
        param_changes = self.g.V(sim_vertex).out('parameter_changed') \
            .order().by('timestamp', T.desc).toList()
            
        final_params = {}
        seen_params = set()
        
        # Build final parameter state (most recent value for each parameter)
        for param in param_changes:
            param_data = self.g.V(param).valueMap().next()
            param_name = param_data.get('parameter_name', [''])[0]
            
            if param_name and param_name not in seen_params:
                final_params[param_name] = {
                    'value': param_data.get('new_value', [''])[0],
                    'domain': param_data.get('domain', ['global'])[0]
                }
                seen_params.add(param_name)
                
        return final_params
        
    def _get_convergence_metrics(self, sim_vertex) -> Dict[str, float]:
        """Get convergence metrics for a simulation"""
        milestones = self.g.V(sim_vertex).out('reached_milestone') \
            .order().by('iteration').valueMap().toList()
            
        if not milestones:
            return {}
            
        residuals = [m.get('residual', [float('inf')])[0] for m in milestones]
        
        return {
            'final_residual': residuals[-1],
            'iterations_to_converge': len(residuals),
            'convergence_rate': self._calculate_convergence_rate(residuals),
            'stability': self._calculate_stability(residuals)
        }
        
    def _get_optimization_performance(self, sim_vertex) -> Dict[str, Any]:
        """Get optimization performance metrics"""
        opt_decisions = self.g.V(sim_vertex).out('parameter_changed') \
            .out('triggered_by').valueMap().toList()
            
        if not opt_decisions:
            return {}
            
        objective_values = [o.get('objective_value', [0])[0] for o in opt_decisions]
        
        return {
            'num_optimizations': len(opt_decisions),
            'final_objective': objective_values[-1] if objective_values else None,
            'objective_improvement': (objective_values[0] - objective_values[-1]) 
                                   if len(objective_values) > 1 else 0,
            'algorithms_used': list(set(o.get('algorithm', [''])[0] for o in opt_decisions))
        }
        
    def _calculate_success_score(self, 
                               convergence_metrics: Dict[str, float],
                               opt_performance: Dict[str, Any]) -> float:
        """Calculate overall success score for a simulation"""
        score = 0.0
        
        # Convergence quality (40% weight)
        if convergence_metrics:
            conv_score = 0.0
            
            # Lower residual is better
            final_residual = convergence_metrics.get('final_residual', 1.0)
            conv_score += (1.0 / (1.0 + final_residual)) * 0.4
            
            # Faster convergence is better
            iterations = convergence_metrics.get('iterations_to_converge', 100)
            conv_score += (1.0 / (1.0 + iterations / 100)) * 0.3
            
            # Better convergence rate
            conv_rate = abs(convergence_metrics.get('convergence_rate', 0))
            conv_score += min(conv_rate / 0.1, 1.0) * 0.3
            
            score += conv_score * 0.4
            
        # Optimization performance (30% weight)
        if opt_performance and opt_performance.get('num_optimizations', 0) > 0:
            opt_score = 0.0
            
            # Objective improvement
            improvement = opt_performance.get('objective_improvement', 0)
            opt_score += min(improvement / 100, 1.0) * 0.5
            
            # Number of optimizations (fewer is better)
            num_opts = opt_performance.get('num_optimizations', 1)
            opt_score += (1.0 / (1.0 + num_opts / 10)) * 0.5
            
            score += opt_score * 0.3
            
        # Stability (30% weight)
        stability = convergence_metrics.get('stability', 0.5)
        score += stability * 0.3
        
        return score
        
    def _calculate_convergence_rate(self, residuals: List[float]) -> float:
        """Calculate convergence rate from residual history"""
        if len(residuals) < 2:
            return 0.0
            
        # Use log scale for residuals
        log_residuals = np.log10(np.array(residuals) + 1e-10)
        
        # Linear regression for rate
        x = np.arange(len(log_residuals))
        rate = np.polyfit(x, log_residuals, 1)[0]
        
        return abs(rate)  # Return absolute rate
        
    def _calculate_stability(self, residuals: List[float]) -> float:
        """Calculate stability metric (lower variance in later iterations is better)"""
        if len(residuals) < 10:
            return 0.5
            
        # Look at last 25% of iterations
        last_quarter = residuals[int(0.75 * len(residuals)):]
        
        # Calculate coefficient of variation
        if np.mean(last_quarter) > 0:
            cv = np.std(last_quarter) / np.mean(last_quarter)
            stability = 1.0 / (1.0 + cv)
        else:
            stability = 1.0
            
        return stability
        
    def _collaborative_filtering(self,
                               successful_sims: List[Dict[str, Any]],
                               user_constraints: Dict[str, Any],
                               top_k: int) -> List[Dict[str, Any]]:
        """Apply collaborative filtering to find similar successful simulations"""
        
        # Create feature matrix from simulation parameters
        feature_matrix, param_names = self._create_feature_matrix(successful_sims)
        
        if feature_matrix.shape[0] == 0:
            return []
            
        # Create user preference vector from constraints
        user_vector = self._create_user_vector(user_constraints, param_names)
        
        # Calculate similarities
        scaler = StandardScaler()
        scaled_features = scaler.fit_transform(feature_matrix)
        
        if user_vector is not None:
            scaled_user = scaler.transform(user_vector.reshape(1, -1))
            similarities = cosine_similarity(scaled_user, scaled_features)[0]
        else:
            # If no user preferences, use success scores
            similarities = np.array([s['success_score'] for s in successful_sims])
            
        # Get top-k similar simulations
        top_indices = np.argsort(similarities)[-top_k:][::-1]
        
        recommendations = []
        for idx in top_indices:
            sim = successful_sims[idx]
            recommendations.append({
                'parameters': sim['parameters'],
                'similarity_score': similarities[idx],
                'source_simulation': sim['simulation_id'],
                'convergence_metrics': sim['convergence_metrics']
            })
            
        return recommendations
        
    def _create_feature_matrix(self, 
                             simulations: List[Dict[str, Any]]) -> Tuple[np.ndarray, List[str]]:
        """Create feature matrix from simulation parameters"""
        # Collect all parameter names
        all_params = set()
        for sim in simulations:
            for param_name in sim['parameters'].keys():
                all_params.add(param_name)
                
        param_names = sorted(list(all_params))
        
        # Create matrix
        matrix = []
        for sim in simulations:
            row = []
            for param in param_names:
                if param in sim['parameters']:
                    try:
                        value = float(sim['parameters'][param]['value'])
                    except:
                        value = 0.0
                else:
                    value = 0.0
                row.append(value)
            matrix.append(row)
            
        return np.array(matrix), param_names
        
    def _create_user_vector(self,
                          user_constraints: Dict[str, Any],
                          param_names: List[str]) -> Optional[np.ndarray]:
        """Create user preference vector from constraints"""
        if not user_constraints:
            return None
            
        vector = np.zeros(len(param_names))
        
        for i, param in enumerate(param_names):
            if param in user_constraints:
                try:
                    vector[i] = float(user_constraints[param])
                except:
                    vector[i] = 0.0
                    
        return vector if np.any(vector != 0) else None
        
    def _content_based_filtering(self,
                               successful_sims: List[Dict[str, Any]],
                               physics_domains: List[str],
                               user_constraints: Dict[str, Any],
                               top_k: int) -> List[Dict[str, Any]]:
        """Apply content-based filtering based on physics domains and constraints"""
        
        recommendations = []
        
        for sim in successful_sims[:top_k * 2]:  # Consider more candidates
            # Calculate domain similarity
            sim_domains = self._get_simulation_domains(sim['simulation_id'])
            domain_similarity = self._calculate_domain_similarity(physics_domains, sim_domains)
            
            # Calculate constraint satisfaction
            constraint_score = self._calculate_constraint_satisfaction(
                sim['parameters'], user_constraints
            )
            
            # Combined score
            content_score = 0.6 * domain_similarity + 0.4 * constraint_score
            
            recommendations.append({
                'parameters': sim['parameters'],
                'content_score': content_score,
                'domain_similarity': domain_similarity,
                'constraint_satisfaction': constraint_score,
                'source_simulation': sim['simulation_id']
            })
            
        # Sort by content score and return top-k
        recommendations.sort(key=lambda x: x['content_score'], reverse=True)
        return recommendations[:top_k]
        
    def _get_simulation_domains(self, simulation_id: str) -> List[str]:
        """Get physics domains for a simulation"""
        sim_vertex = self.g.V().has('simulation', 'simulation_id', simulation_id).next()
        domains = self.g.V(sim_vertex).out('has_domain').values('physics_type').toList()
        return domains
        
    def _calculate_domain_similarity(self, 
                                   target_domains: List[str],
                                   sim_domains: List[str]) -> float:
        """Calculate similarity between domain sets"""
        if not target_domains or not sim_domains:
            return 0.0
            
        intersection = set(target_domains).intersection(set(sim_domains))
        union = set(target_domains).union(set(sim_domains))
        
        return len(intersection) / len(union) if union else 0.0
        
    def _calculate_constraint_satisfaction(self,
                                         parameters: Dict[str, Any],
                                         constraints: Dict[str, Any]) -> float:
        """Calculate how well parameters satisfy user constraints"""
        if not constraints:
            return 1.0
            
        satisfied = 0
        total = 0
        
        for param_name, constraint in constraints.items():
            if param_name in parameters:
                total += 1
                param_value = parameters[param_name]['value']
                
                # Check different constraint types
                if isinstance(constraint, dict):
                    if 'min' in constraint and 'max' in constraint:
                        # Range constraint
                        try:
                            value = float(param_value)
                            if constraint['min'] <= value <= constraint['max']:
                                satisfied += 1
                        except:
                            pass
                    elif 'target' in constraint:
                        # Target value with tolerance
                        try:
                            value = float(param_value)
                            target = float(constraint['target'])
                            tolerance = float(constraint.get('tolerance', 0.1))
                            if abs(value - target) / target <= tolerance:
                                satisfied += 1
                        except:
                            pass
                else:
                    # Exact match
                    if str(param_value) == str(constraint):
                        satisfied += 1
                        
        return satisfied / total if total > 0 else 0.5
        
    def _merge_recommendations(self,
                             collab_recs: List[Dict[str, Any]],
                             content_recs: List[Dict[str, Any]],
                             top_k: int) -> List[Dict[str, Any]]:
        """Merge and rank recommendations from different methods"""
        
        # Create unified recommendation list
        all_recs = {}
        
        # Add collaborative filtering recommendations
        for rec in collab_recs:
            key = rec['source_simulation']
            all_recs[key] = {
                'parameters': rec['parameters'],
                'collaborative_score': rec['similarity_score'],
                'content_score': 0.0,
                'source_simulation': key,
                'convergence_metrics': rec.get('convergence_metrics', {})
            }
            
        # Add content-based recommendations
        for rec in content_recs:
            key = rec['source_simulation']
            if key in all_recs:
                all_recs[key]['content_score'] = rec['content_score']
            else:
                all_recs[key] = {
                    'parameters': rec['parameters'],
                    'collaborative_score': 0.0,
                    'content_score': rec['content_score'],
                    'source_simulation': key,
                    'convergence_metrics': {}
                }
                
        # Calculate final scores
        final_recs = []
        for key, rec in all_recs.items():
            # Weighted combination of scores
            final_score = (0.6 * rec['collaborative_score'] + 
                         0.4 * rec['content_score'])
            
            final_recs.append({
                'parameters': rec['parameters'],
                'recommendation_score': final_score,
                'collaborative_score': rec['collaborative_score'],
                'content_score': rec['content_score'],
                'source_simulation': rec['source_simulation'],
                'convergence_metrics': rec['convergence_metrics']
            })
            
        # Sort by final score and return top-k
        final_recs.sort(key=lambda x: x['recommendation_score'], reverse=True)
        return final_recs[:top_k]
        
    def _get_default_recommendations(self,
                                   simulation_type: str,
                                   physics_domains: List[str]) -> List[Dict[str, Any]]:
        """Get default parameter recommendations when no historical data available"""
        
        defaults = {
            'multi_physics': {
                'convergence_threshold': '1e-4',
                'max_iterations': '1000',
                'relaxation_factor': '0.7',
                'coupling_strength': '1.0'
            },
            'thermal_structural': {
                'thermal_conductivity': '50.0',
                'youngs_modulus': '200e9',
                'poisson_ratio': '0.3',
                'thermal_expansion': '12e-6'
            },
            'fluid_structure': {
                'fluid_density': '1000.0',
                'fluid_viscosity': '0.001',
                'structure_density': '7850.0',
                'coupling_iterations': '5'
            }
        }
        
        base_params = defaults.get(simulation_type, defaults['multi_physics'])
        
        return [{
            'parameters': {k: {'value': v, 'domain': 'global'} 
                         for k, v in base_params.items()},
            'recommendation_score': 0.5,
            'source': 'default',
            'note': 'Default parameters - no historical data available'
        }]
        
    def explain_recommendation(self, recommendation: Dict[str, Any]) -> Dict[str, Any]:
        """Provide explanation for why parameters were recommended"""
        
        explanation = {
            'recommendation_score': recommendation['recommendation_score'],
            'reasoning': []
        }
        
        # Explain collaborative filtering contribution
        if recommendation.get('collaborative_score', 0) > 0:
            explanation['reasoning'].append({
                'method': 'collaborative_filtering',
                'score_contribution': recommendation['collaborative_score'] * 0.6,
                'description': f"Similar to successful simulation {recommendation['source_simulation']} "
                             f"with {recommendation['collaborative_score']:.2f} similarity"
            })
            
        # Explain content-based contribution
        if recommendation.get('content_score', 0) > 0:
            explanation['reasoning'].append({
                'method': 'content_based_filtering',
                'score_contribution': recommendation['content_score'] * 0.4,
                'description': "Matches physics domains and satisfies constraints"
            })
            
        # Add convergence metrics if available
        if recommendation.get('convergence_metrics'):
            metrics = recommendation['convergence_metrics']
            explanation['expected_performance'] = {
                'expected_iterations': metrics.get('iterations_to_converge'),
                'expected_final_residual': metrics.get('final_residual'),
                'convergence_rate': metrics.get('convergence_rate')
            }
            
        return explanation 