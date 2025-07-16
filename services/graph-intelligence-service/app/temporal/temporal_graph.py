"""
Temporal Knowledge Graph Implementation

Adds time-aware reasoning and causal inference to JanusGraph.
"""

import asyncio
import json
from typing import Dict, List, Tuple, Optional, Any, Set
from datetime import datetime, timedelta
import numpy as np
import pandas as pd
from collections import defaultdict, deque
import uuid

# Graph libraries
import networkx as nx
from gremlin_python.process.graph_traversal import __
from gremlin_python.process.traversal import T, P, Order
from gremlin_python.structure.graph import Vertex, Edge

# Causal inference libraries
from pgmpy.models import BayesianNetwork
from pgmpy.estimators import PC, GES, HillClimbSearch
from pgmpy.inference import VariableElimination
import tigramite
from tigramite import data_processing as pp
from tigramite.pcmci import PCMCI
from tigramite.independence_tests import ParCorr

# Time series analysis
from statsmodels.tsa.stattools import grangercausalitytests
from scipy import stats

from platformq_shared.logging_config import get_logger

logger = get_logger(__name__)


class TemporalKnowledgeGraph:
    """
    Temporal extension for JanusGraph with causal inference capabilities
    """
    
    def __init__(self, graph_client, config: Dict):
        self.graph = graph_client
        self.config = config
        
        # Temporal indexing
        self.temporal_index = defaultdict(lambda: defaultdict(list))
        self.time_windows = []
        
        # Causal models
        self.causal_models = {}
        self.causal_cache = {}
        
        # Event history
        self.event_history = deque(maxlen=10000)
        self.state_snapshots = {}
        
        logger.info("Initialized TemporalKnowledgeGraph")
        
    async def add_temporal_vertex(self,
                                 label: str,
                                 properties: Dict,
                                 timestamp: datetime,
                                 valid_from: Optional[datetime] = None,
                                 valid_to: Optional[datetime] = None) -> str:
        """
        Add vertex with temporal properties
        """
        # Add temporal metadata
        properties['_timestamp'] = timestamp.isoformat()
        properties['_valid_from'] = valid_from.isoformat() if valid_from else timestamp.isoformat()
        properties['_valid_to'] = valid_to.isoformat() if valid_to else '9999-12-31T23:59:59'
        
        # Create vertex
        vertex = await self.graph.add_vertex(label, properties)
        vertex_id = vertex.id
        
        # Update temporal index
        self.temporal_index[label][timestamp.date()].append(vertex_id)
        
        # Record event
        self.event_history.append({
            'type': 'vertex_added',
            'vertex_id': vertex_id,
            'label': label,
            'timestamp': timestamp,
            'properties': properties
        })
        
        return vertex_id
        
    async def add_temporal_edge(self,
                              label: str,
                              from_vertex: str,
                              to_vertex: str,
                              properties: Dict,
                              timestamp: datetime,
                              valid_from: Optional[datetime] = None,
                              valid_to: Optional[datetime] = None) -> str:
        """
        Add edge with temporal properties
        """
        # Add temporal metadata
        properties['_timestamp'] = timestamp.isoformat()
        properties['_valid_from'] = valid_from.isoformat() if valid_from else timestamp.isoformat()
        properties['_valid_to'] = valid_to.isoformat() if valid_to else '9999-12-31T23:59:59'
        
        # Create edge
        edge = await self.graph.add_edge(label, from_vertex, to_vertex, properties)
        edge_id = edge.id
        
        # Record event
        self.event_history.append({
            'type': 'edge_added',
            'edge_id': edge_id,
            'label': label,
            'from': from_vertex,
            'to': to_vertex,
            'timestamp': timestamp,
            'properties': properties
        })
        
        return edge_id
        
    async def get_graph_at_time(self, timestamp: datetime) -> nx.DiGraph:
        """
        Get graph state at specific timestamp
        """
        # Create NetworkX graph for analysis
        G = nx.DiGraph()
        
        # Query vertices valid at timestamp
        vertices = await self.graph.g.V().has('_valid_from', P.lte(timestamp.isoformat())) \
                                        .has('_valid_to', P.gte(timestamp.isoformat())) \
                                        .toList()
                                        
        for v in vertices:
            G.add_node(v.id, **v.properties)
            
        # Query edges valid at timestamp
        edges = await self.graph.g.E().has('_valid_from', P.lte(timestamp.isoformat())) \
                                     .has('_valid_to', P.gte(timestamp.isoformat())) \
                                     .toList()
                                     
        for e in edges:
            G.add_edge(e.outV.id, e.inV.id, **e.properties)
            
        return G
        
    async def analyze_temporal_patterns(self,
                                      entity_id: str,
                                      start_time: datetime,
                                      end_time: datetime,
                                      pattern_type: str = "all") -> Dict:
        """
        Analyze temporal patterns for an entity
        """
        patterns = {
            'periodic': [],
            'trending': [],
            'anomalous': [],
            'causal': []
        }
        
        # Get entity's temporal data
        temporal_data = await self._get_entity_temporal_data(
            entity_id, start_time, end_time
        )
        
        if pattern_type in ["all", "periodic"]:
            patterns['periodic'] = self._detect_periodic_patterns(temporal_data)
            
        if pattern_type in ["all", "trending"]:
            patterns['trending'] = self._detect_trends(temporal_data)
            
        if pattern_type in ["all", "anomalous"]:
            patterns['anomalous'] = self._detect_temporal_anomalies(temporal_data)
            
        if pattern_type in ["all", "causal"]:
            patterns['causal'] = await self._detect_causal_patterns(
                entity_id, temporal_data
            )
            
        return patterns
        
    async def discover_causal_relationships(self,
                                          entities: List[str],
                                          start_time: datetime,
                                          end_time: datetime,
                                          method: str = "pc") -> Dict:
        """
        Discover causal relationships between entities
        
        Methods:
        - pc: PC algorithm
        - ges: Greedy Equivalence Search
        - granger: Granger causality
        - transfer_entropy: Transfer entropy
        """
        logger.info(f"Discovering causal relationships using {method}")
        
        # Collect time series data for entities
        time_series_data = await self._collect_time_series(
            entities, start_time, end_time
        )
        
        if method == "pc":
            return self._pc_algorithm(time_series_data)
        elif method == "ges":
            return self._ges_algorithm(time_series_data)
        elif method == "granger":
            return self._granger_causality(time_series_data)
        elif method == "transfer_entropy":
            return self._transfer_entropy(time_series_data)
        else:
            raise ValueError(f"Unknown causal discovery method: {method}")
            
    async def what_if_analysis(self,
                             scenario: Dict,
                             time_horizon: int = 24) -> Dict:
        """
        Perform what-if analysis
        
        Args:
            scenario: Dict with 'entity', 'change_type', 'change_value'
            time_horizon: Hours to predict into future
        """
        entity_id = scenario['entity']
        change_type = scenario['change_type']
        change_value = scenario['change_value']
        
        # Get current state
        current_state = await self._get_current_state()
        
        # Apply hypothetical change
        modified_state = self._apply_change(
            current_state, entity_id, change_type, change_value
        )
        
        # Predict cascading effects
        predictions = await self._predict_cascading_effects(
            modified_state, entity_id, time_horizon
        )
        
        # Assess impact
        impact_assessment = self._assess_impact(
            current_state, predictions, time_horizon
        )
        
        return {
            'scenario': scenario,
            'predictions': predictions,
            'impact_assessment': impact_assessment,
            'confidence': self._calculate_prediction_confidence(predictions),
            'critical_paths': self._identify_critical_paths(predictions)
        }
        
    async def predict_component_failure(self,
                                      component_id: str,
                                      time_window: int = 168) -> Dict:
        """
        Predict component failure probability
        
        Args:
            component_id: Component to analyze
            time_window: Hours to look ahead (default 1 week)
        """
        # Get component history
        history = await self._get_component_history(component_id)
        
        # Extract failure indicators
        indicators = self._extract_failure_indicators(history)
        
        # Build failure prediction model
        if component_id not in self.causal_models:
            await self._build_component_model(component_id, history)
            
        model = self.causal_models[component_id]
        
        # Predict failure probability
        failure_prob = self._predict_failure_probability(
            model, indicators, time_window
        )
        
        # Identify contributing factors
        contributing_factors = self._identify_contributing_factors(
            model, indicators
        )
        
        # Recommend preventive actions
        preventive_actions = self._recommend_preventive_actions(
            component_id, contributing_factors, failure_prob
        )
        
        return {
            'component_id': component_id,
            'failure_probability': failure_prob,
            'time_to_failure': self._estimate_time_to_failure(failure_prob, time_window),
            'contributing_factors': contributing_factors,
            'preventive_actions': preventive_actions,
            'confidence': self._calculate_model_confidence(model, history)
        }
        
    async def analyze_cascade_effects(self,
                                    trigger_event: Dict) -> Dict:
        """
        Analyze potential cascade effects from an event
        """
        trigger_entity = trigger_event['entity']
        trigger_type = trigger_event['type']
        
        # Build dependency graph
        dependency_graph = await self._build_dependency_graph(trigger_entity)
        
        # Simulate cascade
        cascade_simulation = self._simulate_cascade(
            dependency_graph, trigger_entity, trigger_type
        )
        
        # Identify affected components
        affected_components = self._identify_affected_components(
            cascade_simulation
        )
        
        # Calculate impact scores
        impact_scores = self._calculate_cascade_impact(
            cascade_simulation, affected_components
        )
        
        # Generate mitigation strategies
        mitigation_strategies = self._generate_mitigation_strategies(
            cascade_simulation, impact_scores
        )
        
        return {
            'trigger_event': trigger_event,
            'cascade_depth': cascade_simulation['max_depth'],
            'affected_components': affected_components,
            'impact_scores': impact_scores,
            'mitigation_strategies': mitigation_strategies,
            'timeline': cascade_simulation['timeline']
        }
        
    # Private helper methods
    async def _get_entity_temporal_data(self,
                                      entity_id: str,
                                      start_time: datetime,
                                      end_time: datetime) -> pd.DataFrame:
        """Get temporal data for entity"""
        # Query all changes to entity within time range
        changes = await self.graph.g.V(entity_id).as_('v') \
            .outE().has('_timestamp', P.between(start_time.isoformat(), end_time.isoformat())) \
            .as_('e').select('v', 'e').toList()
            
        # Convert to DataFrame
        data = []
        for change in changes:
            data.append({
                'timestamp': change['e']['_timestamp'],
                'property': change['e'].label,
                'value': change['e'].get('value', 1),
                'vertex_properties': change['v'].properties
            })
            
        return pd.DataFrame(data)
        
    def _detect_periodic_patterns(self, data: pd.DataFrame) -> List[Dict]:
        """Detect periodic patterns in temporal data"""
        patterns = []
        
        if data.empty:
            return patterns
            
        # Convert to time series
        ts = data.set_index('timestamp')['value']
        
        # FFT for frequency analysis
        if len(ts) > 10:
            fft = np.fft.fft(ts.values)
            freqs = np.fft.fftfreq(len(ts))
            
            # Find dominant frequencies
            dominant_idx = np.argsort(np.abs(fft))[-5:]
            
            for idx in dominant_idx:
                if freqs[idx] > 0:  # Positive frequencies only
                    period = 1 / freqs[idx]
                    patterns.append({
                        'type': 'periodic',
                        'period_hours': period,
                        'strength': float(np.abs(fft[idx])),
                        'confidence': float(np.abs(fft[idx]) / np.sum(np.abs(fft)))
                    })
                    
        return patterns
        
    def _detect_trends(self, data: pd.DataFrame) -> List[Dict]:
        """Detect trends in temporal data"""
        trends = []
        
        if len(data) < 3:
            return trends
            
        # Simple linear trend
        x = np.arange(len(data))
        y = data['value'].values
        
        slope, intercept, r_value, p_value, std_err = stats.linregress(x, y)
        
        if p_value < 0.05:  # Significant trend
            trends.append({
                'type': 'linear',
                'slope': float(slope),
                'direction': 'increasing' if slope > 0 else 'decreasing',
                'r_squared': float(r_value ** 2),
                'p_value': float(p_value)
            })
            
        # Detect change points
        if len(data) > 20:
            # Simple change point detection
            for i in range(10, len(data) - 10):
                before = y[:i]
                after = y[i:]
                
                # T-test for mean change
                t_stat, p_val = stats.ttest_ind(before, after)
                
                if p_val < 0.01:
                    trends.append({
                        'type': 'change_point',
                        'index': i,
                        'timestamp': data.iloc[i]['timestamp'],
                        'before_mean': float(np.mean(before)),
                        'after_mean': float(np.mean(after)),
                        'p_value': float(p_val)
                    })
                    
        return trends
        
    def _detect_temporal_anomalies(self, data: pd.DataFrame) -> List[Dict]:
        """Detect anomalies in temporal patterns"""
        anomalies = []
        
        if len(data) < 10:
            return anomalies
            
        values = data['value'].values
        
        # Z-score based anomaly detection
        z_scores = np.abs(stats.zscore(values))
        threshold = 3
        
        anomaly_indices = np.where(z_scores > threshold)[0]
        
        for idx in anomaly_indices:
            anomalies.append({
                'type': 'statistical',
                'timestamp': data.iloc[idx]['timestamp'],
                'value': float(values[idx]),
                'z_score': float(z_scores[idx]),
                'expected_range': [
                    float(np.mean(values) - threshold * np.std(values)),
                    float(np.mean(values) + threshold * np.std(values))
                ]
            })
            
        return anomalies
        
    async def _detect_causal_patterns(self,
                                    entity_id: str,
                                    data: pd.DataFrame) -> List[Dict]:
        """Detect causal patterns for entity"""
        patterns = []
        
        # Get related entities
        related = await self.graph.g.V(entity_id).both().id().toList()
        
        if not related:
            return patterns
            
        # Collect time series for related entities
        related_data = {}
        for rel_id in related[:10]:  # Limit to 10 for performance
            rel_data = await self._get_entity_temporal_data(
                rel_id,
                data['timestamp'].min(),
                data['timestamp'].max()
            )
            if not rel_data.empty:
                related_data[rel_id] = rel_data
                
        # Granger causality tests
        entity_ts = data.set_index('timestamp')['value']
        
        for rel_id, rel_data in related_data.items():
            rel_ts = rel_data.set_index('timestamp')['value']
            
            # Align time series
            aligned = pd.concat([entity_ts, rel_ts], axis=1).dropna()
            
            if len(aligned) > 20:
                # Test both directions
                try:
                    # Test if related causes entity
                    result1 = grangercausalitytests(
                        aligned.values, maxlag=5, verbose=False
                    )
                    
                    # Find best lag
                    best_pval = 1.0
                    best_lag = 1
                    for lag, test_result in result1.items():
                        pval = test_result[0]['ssr_ftest'][1]
                        if pval < best_pval:
                            best_pval = pval
                            best_lag = lag
                            
                    if best_pval < 0.05:
                        patterns.append({
                            'type': 'granger_causality',
                            'cause': rel_id,
                            'effect': entity_id,
                            'lag': best_lag,
                            'p_value': float(best_pval),
                            'direction': 'incoming'
                        })
                        
                except Exception as e:
                    logger.debug(f"Granger test failed: {e}")
                    
        return patterns
        
    def _pc_algorithm(self, data: Dict[str, pd.DataFrame]) -> Dict:
        """PC algorithm for causal discovery"""
        # Convert to numpy array
        entities = list(data.keys())
        n_entities = len(entities)
        
        # Create data matrix
        max_len = max(len(df) for df in data.values())
        data_matrix = np.zeros((max_len, n_entities))
        
        for i, entity in enumerate(entities):
            df = data[entity]
            data_matrix[:len(df), i] = df['value'].values
            
        # Run PC algorithm using tigramite
        dataframe = pp.DataFrame(data_matrix, var_names=entities)
        pcmci = PCMCI(dataframe, cond_ind_test=ParCorr())
        
        # Run algorithm
        results = pcmci.run_pcmci(tau_max=5, pc_alpha=0.05)
        
        # Extract causal graph
        causal_graph = {
            'nodes': entities,
            'edges': [],
            'strength_matrix': results['val_matrix'].tolist(),
            'p_matrix': results['p_matrix'].tolist()
        }
        
        # Extract significant edges
        for i, cause in enumerate(entities):
            for j, effect in enumerate(entities):
                if i != j:
                    for lag in range(results['val_matrix'].shape[2]):
                        if results['p_matrix'][i, j, lag] < 0.05:
                            causal_graph['edges'].append({
                                'cause': cause,
                                'effect': effect,
                                'lag': lag,
                                'strength': float(results['val_matrix'][i, j, lag]),
                                'p_value': float(results['p_matrix'][i, j, lag])
                            })
                            
        return causal_graph
        
    async def _collect_time_series(self,
                                 entities: List[str],
                                 start_time: datetime,
                                 end_time: datetime) -> Dict[str, pd.DataFrame]:
        """Collect time series data for multiple entities"""
        time_series = {}
        
        for entity in entities:
            data = await self._get_entity_temporal_data(
                entity, start_time, end_time
            )
            if not data.empty:
                time_series[entity] = data
                
        return time_series
        
    async def _get_current_state(self) -> Dict:
        """Get current state of the graph"""
        # Get all vertices with their current properties
        vertices = await self.graph.g.V().valueMap(True).toList()
        
        # Get all edges
        edges = await self.graph.g.E().valueMap(True).toList()
        
        return {
            'vertices': {v[T.id]: v for v in vertices},
            'edges': edges,
            'timestamp': datetime.utcnow()
        }
        
    def _apply_change(self,
                     state: Dict,
                     entity_id: str,
                     change_type: str,
                     change_value: Any) -> Dict:
        """Apply hypothetical change to state"""
        modified_state = state.copy()
        
        if entity_id in modified_state['vertices']:
            vertex = modified_state['vertices'][entity_id]
            
            if change_type == 'remove':
                # Remove entity
                del modified_state['vertices'][entity_id]
                # Remove connected edges
                modified_state['edges'] = [
                    e for e in modified_state['edges']
                    if e['outV'] != entity_id and e['inV'] != entity_id
                ]
            elif change_type == 'update':
                # Update properties
                for key, value in change_value.items():
                    vertex[key] = value
            elif change_type == 'failure':
                # Mark as failed
                vertex['status'] = 'failed'
                vertex['failure_time'] = datetime.utcnow()
                
        return modified_state
        
    async def _predict_cascading_effects(self,
                                       state: Dict,
                                       trigger_entity: str,
                                       time_horizon: int) -> List[Dict]:
        """Predict cascading effects from a change"""
        predictions = []
        affected_entities = set([trigger_entity])
        
        # Build impact propagation model
        impact_queue = deque([(trigger_entity, 0, 1.0)])  # (entity, time, probability)
        
        while impact_queue:
            entity, time, prob = impact_queue.popleft()
            
            if time > time_horizon:
                continue
                
            # Find dependent entities
            dependents = await self.graph.g.V(entity).out().id().toList()
            
            for dep in dependents:
                if dep not in affected_entities or prob > 0.1:
                    # Calculate impact probability
                    impact_prob = prob * self._calculate_dependency_strength(
                        entity, dep, state
                    )
                    
                    if impact_prob > 0.05:
                        predictions.append({
                            'entity': dep,
                            'impact_time': time + self._estimate_propagation_delay(entity, dep),
                            'impact_probability': float(impact_prob),
                            'impact_type': self._determine_impact_type(entity, dep, state),
                            'source': entity
                        })
                        
                        affected_entities.add(dep)
                        impact_queue.append((
                            dep,
                            time + self._estimate_propagation_delay(entity, dep),
                            impact_prob
                        ))
                        
        return predictions
        
    def _calculate_dependency_strength(self,
                                     source: str,
                                     target: str,
                                     state: Dict) -> float:
        """Calculate dependency strength between entities"""
        # Simple heuristic - can be enhanced with learned models
        edge_count = sum(
            1 for e in state['edges']
            if e['outV'] == source and e['inV'] == target
        )
        
        # Consider edge types and weights
        total_weight = 0.0
        for e in state['edges']:
            if e['outV'] == source and e['inV'] == target:
                weight = e.get('weight', 1.0)
                if e.get('type') == 'critical':
                    weight *= 2.0
                total_weight += weight
                
        return min(1.0, total_weight / 10.0)  # Normalize
        
    def _estimate_propagation_delay(self, source: str, target: str) -> int:
        """Estimate propagation delay in hours"""
        # Simple heuristic - can be learned from historical data
        return np.random.randint(1, 6)
        
    def _determine_impact_type(self,
                             source: str,
                             target: str,
                             state: Dict) -> str:
        """Determine type of impact"""
        # Check edge labels for clues
        for e in state['edges']:
            if e['outV'] == source and e['inV'] == target:
                if 'depends_on' in e.get('label', ''):
                    return 'dependency_failure'
                elif 'supplies' in e.get('label', ''):
                    return 'supply_disruption'
                elif 'communicates' in e.get('label', ''):
                    return 'communication_loss'
                    
        return 'general_impact'
        
    def _assess_impact(self,
                      original_state: Dict,
                      predictions: List[Dict],
                      time_horizon: int) -> Dict:
        """Assess overall impact of predictions"""
        total_entities = len(original_state['vertices'])
        affected_entities = len(set(p['entity'] for p in predictions))
        
        # Calculate various impact metrics
        impact_assessment = {
            'affected_ratio': affected_entities / total_entities if total_entities > 0 else 0,
            'total_affected': affected_entities,
            'max_cascade_depth': self._calculate_cascade_depth(predictions),
            'critical_entities_affected': self._count_critical_entities(predictions, original_state),
            'estimated_recovery_time': self._estimate_recovery_time(predictions),
            'impact_severity': self._calculate_impact_severity(predictions, original_state)
        }
        
        return impact_assessment
        
    def _calculate_cascade_depth(self, predictions: List[Dict]) -> int:
        """Calculate maximum cascade depth"""
        if not predictions:
            return 0
            
        # Build cascade tree
        cascade_tree = defaultdict(list)
        for pred in predictions:
            if 'source' in pred:
                cascade_tree[pred['source']].append(pred['entity'])
                
        # BFS to find max depth
        max_depth = 0
        visited = set()
        
        def bfs(node, depth):
            nonlocal max_depth
            if node in visited:
                return
            visited.add(node)
            max_depth = max(max_depth, depth)
            
            for child in cascade_tree.get(node, []):
                bfs(child, depth + 1)
                
        # Start from nodes with no source (roots)
        roots = [p['entity'] for p in predictions if 'source' not in p]
        for root in roots:
            bfs(root, 0)
            
        return max_depth
        
    def _count_critical_entities(self, 
                               predictions: List[Dict],
                               state: Dict) -> int:
        """Count affected critical entities"""
        critical_count = 0
        
        for pred in predictions:
            entity_id = pred['entity']
            if entity_id in state['vertices']:
                vertex = state['vertices'][entity_id]
                if vertex.get('criticality', 'normal') == 'critical':
                    critical_count += 1
                    
        return critical_count
        
    def _estimate_recovery_time(self, predictions: List[Dict]) -> float:
        """Estimate system recovery time"""
        if not predictions:
            return 0.0
            
        # Base recovery on max impact time and severity
        max_impact_time = max(p['impact_time'] for p in predictions)
        avg_probability = np.mean([p['impact_probability'] for p in predictions])
        
        # Heuristic: recovery takes 2x impact time weighted by probability
        return max_impact_time * 2 * avg_probability
        
    def _calculate_impact_severity(self,
                                 predictions: List[Dict],
                                 state: Dict) -> str:
        """Calculate overall impact severity"""
        if not predictions:
            return 'none'
            
        affected_ratio = len(predictions) / len(state['vertices'])
        critical_affected = self._count_critical_entities(predictions, state)
        max_probability = max(p['impact_probability'] for p in predictions)
        
        # Severity scoring
        severity_score = (
            affected_ratio * 0.3 +
            (critical_affected / max(1, len(predictions))) * 0.4 +
            max_probability * 0.3
        )
        
        if severity_score > 0.7:
            return 'critical'
        elif severity_score > 0.4:
            return 'high'
        elif severity_score > 0.2:
            return 'medium'
        else:
            return 'low'
            
    def _calculate_prediction_confidence(self, predictions: List[Dict]) -> float:
        """Calculate confidence in predictions"""
        if not predictions:
            return 1.0
            
        # Confidence decreases with cascade depth and time
        max_depth = self._calculate_cascade_depth(predictions)
        max_time = max(p['impact_time'] for p in predictions)
        
        # Exponential decay
        depth_factor = np.exp(-0.2 * max_depth)
        time_factor = np.exp(-0.05 * max_time)
        
        return float(depth_factor * time_factor)
        
    def _identify_critical_paths(self, predictions: List[Dict]) -> List[List[str]]:
        """Identify critical propagation paths"""
        # Build directed graph of cascades
        G = nx.DiGraph()
        
        for pred in predictions:
            if 'source' in pred:
                G.add_edge(pred['source'], pred['entity'], 
                          weight=pred['impact_probability'])
                          
        # Find paths with high cumulative probability
        critical_paths = []
        
        # Get all simple paths (limited length for performance)
        sources = [n for n in G.nodes() if G.in_degree(n) == 0]
        sinks = [n for n in G.nodes() if G.out_degree(n) == 0]
        
        for source in sources:
            for sink in sinks:
                try:
                    paths = list(nx.all_simple_paths(G, source, sink, cutoff=5))
                    for path in paths:
                        # Calculate path probability
                        prob = 1.0
                        for i in range(len(path) - 1):
                            prob *= G[path[i]][path[i+1]]['weight']
                            
                        if prob > 0.1:  # Threshold for critical
                            critical_paths.append(path)
                except:
                    continue
                    
        # Sort by probability and return top paths
        critical_paths.sort(key=lambda p: self._calculate_path_probability(p, G), 
                          reverse=True)
        
        return critical_paths[:10]
        
    def _calculate_path_probability(self, path: List[str], G: nx.DiGraph) -> float:
        """Calculate probability along a path"""
        prob = 1.0
        for i in range(len(path) - 1):
            if G.has_edge(path[i], path[i+1]):
                prob *= G[path[i]][path[i+1]]['weight']
        return prob
        
    # Additional helper methods for component failure prediction
    async def _get_component_history(self, component_id: str) -> pd.DataFrame:
        """Get historical data for component"""
        # Get all events related to component
        end_time = datetime.utcnow()
        start_time = end_time - timedelta(days=90)  # 90 days history
        
        return await self._get_entity_temporal_data(
            component_id, start_time, end_time
        )
        
    def _extract_failure_indicators(self, history: pd.DataFrame) -> Dict:
        """Extract indicators of potential failure"""
        indicators = {
            'error_rate': 0.0,
            'performance_degradation': 0.0,
            'maintenance_overdue': False,
            'age_factor': 0.0,
            'stress_level': 0.0
        }
        
        if history.empty:
            return indicators
            
        # Calculate error rate trend
        error_events = history[history['property'] == 'error']
        if not error_events.empty:
            recent_errors = error_events[
                error_events['timestamp'] > datetime.utcnow() - timedelta(days=7)
            ]
            indicators['error_rate'] = len(recent_errors) / 7.0  # Errors per day
            
        # Check performance metrics
        perf_events = history[history['property'] == 'performance']
        if not perf_events.empty:
            recent_perf = perf_events.tail(10)['value'].mean()
            baseline_perf = perf_events.head(10)['value'].mean()
            if baseline_perf > 0:
                indicators['performance_degradation'] = 1.0 - (recent_perf / baseline_perf)
                
        return indicators
        
    async def _build_component_model(self, 
                                   component_id: str,
                                   history: pd.DataFrame):
        """Build failure prediction model for component"""
        # Simple Bayesian network for failure prediction
        model = BayesianNetwork([
            ('error_rate', 'failure'),
            ('performance_degradation', 'failure'),
            ('age', 'failure'),
            ('maintenance', 'failure')
        ])
        
        # Learn from historical data (simplified)
        # In production, would use actual failure events
        self.causal_models[component_id] = model
        
    def _predict_failure_probability(self,
                                   model,
                                   indicators: Dict,
                                   time_window: int) -> float:
        """Predict failure probability within time window"""
        # Simplified calculation
        base_prob = 0.01  # 1% base failure rate
        
        # Adjust based on indicators
        if indicators['error_rate'] > 10:
            base_prob *= 5
        elif indicators['error_rate'] > 5:
            base_prob *= 2
            
        if indicators['performance_degradation'] > 0.3:
            base_prob *= 3
        elif indicators['performance_degradation'] > 0.1:
            base_prob *= 1.5
            
        # Time window adjustment
        daily_prob = base_prob
        window_days = time_window / 24
        
        # Probability of at least one failure in window
        failure_prob = 1 - (1 - daily_prob) ** window_days
        
        return min(1.0, failure_prob)
        
    def _identify_contributing_factors(self,
                                     model,
                                     indicators: Dict) -> List[Dict]:
        """Identify factors contributing to failure risk"""
        factors = []
        
        if indicators['error_rate'] > 5:
            factors.append({
                'factor': 'high_error_rate',
                'value': indicators['error_rate'],
                'contribution': 0.4,
                'description': f"Error rate of {indicators['error_rate']:.1f} errors/day"
            })
            
        if indicators['performance_degradation'] > 0.1:
            factors.append({
                'factor': 'performance_degradation',
                'value': indicators['performance_degradation'],
                'contribution': 0.3,
                'description': f"{indicators['performance_degradation']*100:.1f}% performance degradation"
            })
            
        return factors
        
    def _recommend_preventive_actions(self,
                                    component_id: str,
                                    factors: List[Dict],
                                    failure_prob: float) -> List[Dict]:
        """Recommend actions to prevent failure"""
        actions = []
        
        if failure_prob > 0.5:
            actions.append({
                'action': 'immediate_maintenance',
                'priority': 'critical',
                'description': 'Schedule immediate maintenance',
                'expected_risk_reduction': 0.7
            })
            
        for factor in factors:
            if factor['factor'] == 'high_error_rate':
                actions.append({
                    'action': 'diagnose_errors',
                    'priority': 'high',
                    'description': 'Investigate root cause of errors',
                    'expected_risk_reduction': factor['contribution'] * 0.5
                })
            elif factor['factor'] == 'performance_degradation':
                actions.append({
                    'action': 'performance_tuning',
                    'priority': 'medium',
                    'description': 'Optimize component performance',
                    'expected_risk_reduction': factor['contribution'] * 0.3
                })
                
        return actions
        
    def _calculate_model_confidence(self, model, history: pd.DataFrame) -> float:
        """Calculate confidence in model predictions"""
        # Based on amount and quality of historical data
        if len(history) < 30:
            return 0.3
        elif len(history) < 100:
            return 0.6
        else:
            return 0.8
            
    def _estimate_time_to_failure(self, 
                                failure_prob: float,
                                time_window: int) -> Optional[float]:
        """Estimate expected time to failure"""
        if failure_prob < 0.1:
            return None  # Too low to estimate
            
        # Exponential distribution assumption
        daily_rate = -np.log(1 - failure_prob) / (time_window / 24)
        expected_days = 1 / daily_rate if daily_rate > 0 else time_window / 24
        
        return expected_days * 24  # Convert to hours
        
    # Cascade analysis helpers
    async def _build_dependency_graph(self, entity_id: str) -> nx.DiGraph:
        """Build dependency graph centered on entity"""
        G = nx.DiGraph()
        
        # BFS to build dependency graph
        queue = deque([entity_id])
        visited = set()
        depth = 0
        max_depth = 3  # Limit depth for performance
        
        while queue and depth < max_depth:
            next_queue = deque()
            
            for current in queue:
                if current in visited:
                    continue
                    
                visited.add(current)
                G.add_node(current)
                
                # Get dependencies
                deps = await self.graph.g.V(current).out('depends_on').id().toList()
                for dep in deps:
                    G.add_edge(current, dep, type='depends_on')
                    if dep not in visited:
                        next_queue.append(dep)
                        
                # Get reverse dependencies
                rev_deps = await self.graph.g.V(current).in_('depends_on').id().toList()
                for rev_dep in rev_deps:
                    G.add_edge(rev_dep, current, type='depends_on')
                    if rev_dep not in visited:
                        next_queue.append(rev_dep)
                        
            queue = next_queue
            depth += 1
            
        return G
        
    def _simulate_cascade(self,
                        dependency_graph: nx.DiGraph,
                        trigger_entity: str,
                        trigger_type: str) -> Dict:
        """Simulate cascade through dependency graph"""
        simulation = {
            'timeline': [],
            'max_depth': 0,
            'affected_nodes': set()
        }
        
        # Initialize with trigger
        event_queue = [(0, trigger_entity, trigger_type, 1.0)]
        processed = set()
        
        while event_queue:
            time, entity, event_type, probability = event_queue.pop(0)
            
            if entity in processed:
                continue
                
            processed.add(entity)
            simulation['affected_nodes'].add(entity)
            simulation['timeline'].append({
                'time': time,
                'entity': entity,
                'event': event_type,
                'probability': probability
            })
            
            # Propagate to dependencies
            if dependency_graph.has_node(entity):
                for neighbor in dependency_graph.neighbors(entity):
                    if neighbor not in processed:
                        # Calculate propagation
                        edge_data = dependency_graph[entity][neighbor]
                        prop_prob = probability * 0.8  # Default propagation
                        prop_delay = np.random.randint(1, 5)
                        
                        event_queue.append((
                            time + prop_delay,
                            neighbor,
                            'cascade_failure',
                            prop_prob
                        ))
                        
        # Calculate max depth
        if simulation['timeline']:
            simulation['max_depth'] = max(t['time'] for t in simulation['timeline'])
            
        return simulation
        
    def _identify_affected_components(self, simulation: Dict) -> List[Dict]:
        """Identify and classify affected components"""
        affected = []
        
        for event in simulation['timeline']:
            affected.append({
                'entity': event['entity'],
                'impact_time': event['time'],
                'impact_type': event['event'],
                'impact_probability': event['probability']
            })
            
        return affected
        
    def _calculate_cascade_impact(self,
                                simulation: Dict,
                                affected: List[Dict]) -> Dict:
        """Calculate impact scores for cascade"""
        return {
            'total_affected': len(affected),
            'cascade_duration': simulation['max_depth'],
            'average_probability': np.mean([a['impact_probability'] for a in affected]),
            'high_impact_nodes': sum(1 for a in affected if a['impact_probability'] > 0.7)
        }
        
    def _generate_mitigation_strategies(self,
                                      simulation: Dict,
                                      impact_scores: Dict) -> List[Dict]:
        """Generate strategies to mitigate cascade effects"""
        strategies = []
        
        # Identify critical nodes to protect
        critical_nodes = [
            event['entity'] for event in simulation['timeline']
            if event['probability'] > 0.7
        ]
        
        if critical_nodes:
            strategies.append({
                'strategy': 'protect_critical',
                'description': 'Isolate and protect critical nodes',
                'targets': critical_nodes[:5],  # Top 5
                'expected_reduction': 0.5
            })
            
        # Circuit breaker strategy
        if impact_scores['cascade_duration'] > 10:
            strategies.append({
                'strategy': 'circuit_breakers',
                'description': 'Implement circuit breakers to stop cascade',
                'targets': self._identify_cascade_bottlenecks(simulation),
                'expected_reduction': 0.3
            })
            
        return strategies
        
    def _identify_cascade_bottlenecks(self, simulation: Dict) -> List[str]:
        """Identify bottleneck nodes in cascade"""
        # Count how many times each node appears as source
        source_counts = defaultdict(int)
        
        for i in range(1, len(simulation['timeline'])):
            # Infer source from timing
            event = simulation['timeline'][i]
            potential_sources = [
                e['entity'] for e in simulation['timeline'][:i]
                if e['time'] < event['time']
            ]
            if potential_sources:
                # Assume nearest in time is source
                source = min(potential_sources, 
                           key=lambda s: event['time'] - next(
                               e['time'] for e in simulation['timeline'] if e['entity'] == s
                           ))
                source_counts[source] += 1
                
        # Return top bottlenecks
        bottlenecks = sorted(source_counts.items(), key=lambda x: x[1], reverse=True)
        return [b[0] for b in bottlenecks[:3]]
        
    # Additional utility methods
    def _ges_algorithm(self, data: Dict[str, pd.DataFrame]) -> Dict:
        """Greedy Equivalence Search for causal discovery"""
        # Similar structure to PC algorithm
        # Would implement GES algorithm here
        return self._pc_algorithm(data)  # Placeholder
        
    def _granger_causality(self, data: Dict[str, pd.DataFrame]) -> Dict:
        """Granger causality analysis"""
        causal_graph = {
            'nodes': list(data.keys()),
            'edges': []
        }
        
        # Pairwise Granger tests
        for cause in data:
            for effect in data:
                if cause != effect:
                    # Align time series
                    cause_ts = data[cause].set_index('timestamp')['value']
                    effect_ts = data[effect].set_index('timestamp')['value']
                    aligned = pd.concat([effect_ts, cause_ts], axis=1).dropna()
                    
                    if len(aligned) > 20:
                        try:
                            result = grangercausalitytests(
                                aligned.values, maxlag=5, verbose=False
                            )
                            
                            # Find significant lag
                            for lag, test in result.items():
                                pval = test[0]['ssr_ftest'][1]
                                if pval < 0.05:
                                    causal_graph['edges'].append({
                                        'cause': cause,
                                        'effect': effect,
                                        'lag': lag,
                                        'p_value': float(pval),
                                        'method': 'granger'
                                    })
                                    break
                        except:
                            continue
                            
        return causal_graph
        
    def _transfer_entropy(self, data: Dict[str, pd.DataFrame]) -> Dict:
        """Transfer entropy for causal discovery"""
        # Placeholder - would implement transfer entropy calculation
        return self._granger_causality(data) 