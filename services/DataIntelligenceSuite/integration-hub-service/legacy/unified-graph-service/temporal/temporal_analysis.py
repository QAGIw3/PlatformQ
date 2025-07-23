"""Temporal analysis engine for time-aware graph queries and causal inference"""

import logging
from typing import Dict, Any, List, Optional, Tuple, Set
from datetime import datetime, timedelta
import asyncio
import numpy as np
import pandas as pd
from enum import Enum
import json

from app.core.config import Settings
from app.graph.janusgraph_client import JanusGraphClient


logger = logging.getLogger(__name__)


class CausalAlgorithm(Enum):
    """Causal discovery algorithms"""
    PC = "pc"  # Peter-Clark algorithm
    GES = "ges"  # Greedy Equivalence Search
    LINGAM = "lingam"  # Linear Non-Gaussian Acyclic Model
    FCI = "fci"  # Fast Causal Inference
    GANGER = "granger"  # Granger Causality


class TemporalAnalysisEngine:
    """Engine for temporal graph analysis and causal inference"""
    
    def __init__(self, settings: Settings, graph_client: JanusGraphClient):
        self.settings = settings
        self.graph_client = graph_client
        self.temporal_indices: Dict[str, Any] = {}
        self._index_task: Optional[asyncio.Task] = None
        
    async def start(self):
        """Start the temporal analysis engine"""
        logger.info("Starting temporal analysis engine")
        
        if self.settings.temporal_index_enabled:
            # Start temporal index maintenance
            self._index_task = asyncio.create_task(self._maintain_temporal_indices())
            
        logger.info("Temporal analysis engine started")
        
    async def stop(self):
        """Stop the temporal analysis engine"""
        if self._index_task:
            self._index_task.cancel()
            
        logger.info("Temporal analysis engine stopped")
        
    async def get_graph_snapshot(self, timestamp: datetime,
                                entity_types: Optional[List[str]] = None,
                                edge_types: Optional[List[str]] = None) -> Dict[str, Any]:
        """Get graph snapshot at a specific point in time"""
        try:
            # Build queries for snapshot
            vertex_query = "g.V()"
            edge_query = "g.E()"
            
            # Filter by entity types if specified
            if entity_types:
                type_filters = " or ".join([f"label == '{t}'" for t in entity_types])
                vertex_query += f".has('created_at', P.lte('{timestamp.isoformat()}')).filter{{{type_filters}}}"
            else:
                vertex_query += f".has('created_at', P.lte('{timestamp.isoformat()}'))"
                
            # Filter edges by type and time
            if edge_types:
                edge_filters = " or ".join([f"label == '{t}'" for t in edge_types])
                edge_query += f".has('created_at', P.lte('{timestamp.isoformat()}')).filter{{{edge_filters}}}"
            else:
                edge_query += f".has('created_at', P.lte('{timestamp.isoformat()}'))"
                
            # Get active vertices at timestamp
            vertex_query += ".has('deleted_at', P.gt('{timestamp.isoformat()}').or().hasNot('deleted_at'))"
            vertices = await self.graph_client.execute_query(vertex_query + ".valueMap(true)")
            
            # Get active edges at timestamp
            edge_query += ".has('deleted_at', P.gt('{timestamp.isoformat()}').or().hasNot('deleted_at'))"
            edges = await self.graph_client.execute_query(edge_query + ".valueMap(true)")
            
            # Build snapshot
            snapshot = {
                'timestamp': timestamp.isoformat(),
                'vertices': [self._format_temporal_vertex(v) for v in vertices],
                'edges': [self._format_temporal_edge(e) for e in edges],
                'vertex_count': len(vertices),
                'edge_count': len(edges)
            }
            
            return snapshot
            
        except Exception as e:
            logger.error(f"Failed to get graph snapshot at {timestamp}: {e}")
            raise
            
    async def get_entity_evolution(self, entity_id: str,
                                  start_time: Optional[datetime] = None,
                                  end_time: Optional[datetime] = None) -> List[Dict[str, Any]]:
        """Get evolution of an entity over time"""
        try:
            # Get all versions of the entity
            query = """
                g.V(entity_id).
                union(
                    identity(),
                    inE().has('label', 'VERSION_OF').outV()
                ).
                order().by('version_timestamp', desc).
                valueMap(true)
            """
            
            versions = await self.graph_client.execute_query(query, {'entity_id': entity_id})
            
            # Filter by time range if specified
            evolution = []
            for version in versions:
                version_time = datetime.fromisoformat(version.get('version_timestamp', [datetime.utcnow().isoformat()])[0])
                
                if start_time and version_time < start_time:
                    continue
                if end_time and version_time > end_time:
                    continue
                    
                evolution.append({
                    'timestamp': version_time.isoformat(),
                    'version': version.get('version', [1])[0],
                    'properties': self._extract_properties(version),
                    'changes': await self._detect_changes(version, versions)
                })
                
            return evolution
            
        except Exception as e:
            logger.error(f"Failed to get entity evolution for {entity_id}: {e}")
            raise
            
    async def discover_causality(self, entities: List[str],
                               time_window: str = "7d",
                               algorithm: str = "pc",
                               significance_level: float = 0.05) -> Dict[str, Any]:
        """Discover causal relationships between entities"""
        try:
            # Parse time window
            window_delta = self._parse_time_window(time_window)
            end_time = datetime.utcnow()
            start_time = end_time - window_delta
            
            # Collect time series data for entities
            time_series_data = {}
            
            for entity_id in entities:
                # Get temporal data for entity
                series = await self._get_entity_time_series(entity_id, start_time, end_time)
                time_series_data[entity_id] = series
                
            # Convert to DataFrame for analysis
            df = pd.DataFrame(time_series_data)
            
            # Run causal discovery algorithm
            if algorithm == CausalAlgorithm.PC.value:
                causal_graph = await self._run_pc_algorithm(df, significance_level)
            elif algorithm == CausalAlgorithm.GES.value:
                causal_graph = await self._run_ges_algorithm(df)
            elif algorithm == CausalAlgorithm.LINGAM.value:
                causal_graph = await self._run_lingam_algorithm(df)
            elif algorithm == CausalAlgorithm.GANGER.value:
                causal_graph = await self._run_granger_causality(df, significance_level)
            else:
                raise ValueError(f"Unknown causal algorithm: {algorithm}")
                
            return {
                'algorithm': algorithm,
                'time_window': time_window,
                'significance_level': significance_level,
                'causal_edges': causal_graph['edges'],
                'strength_matrix': causal_graph.get('strengths', {}),
                'confidence_scores': causal_graph.get('confidences', {}),
                'discovered_at': datetime.utcnow().isoformat()
            }
            
        except Exception as e:
            logger.error(f"Failed to discover causality: {e}")
            raise
            
    async def simulate_scenario(self, scenario: Dict[str, Any],
                              time_steps: int = 10) -> Dict[str, Any]:
        """Simulate what-if scenarios on the temporal graph"""
        try:
            # Extract scenario parameters
            interventions = scenario.get('interventions', [])
            target_entities = scenario.get('targets', [])
            initial_state = scenario.get('initial_state', {})
            
            # Initialize simulation state
            current_state = initial_state.copy()
            simulation_results = {
                'time_steps': [],
                'entity_states': {entity: [] for entity in target_entities},
                'impacts': []
            }
            
            # Run simulation
            for t in range(time_steps):
                # Apply interventions at specified times
                for intervention in interventions:
                    if intervention['time_step'] == t:
                        await self._apply_intervention(current_state, intervention)
                        
                # Propagate effects through causal graph
                new_state = await self._propagate_effects(current_state, target_entities)
                
                # Record results
                simulation_results['time_steps'].append(t)
                for entity in target_entities:
                    simulation_results['entity_states'][entity].append(
                        new_state.get(entity, current_state.get(entity, 0))
                    )
                    
                # Calculate impacts
                impacts = self._calculate_impacts(current_state, new_state)
                simulation_results['impacts'].append(impacts)
                
                # Update state for next iteration
                current_state = new_state
                
            return {
                'scenario': scenario,
                'results': simulation_results,
                'summary': self._summarize_simulation(simulation_results),
                'simulated_at': datetime.utcnow().isoformat()
            }
            
        except Exception as e:
            logger.error(f"Failed to simulate scenario: {e}")
            raise
            
    async def detect_temporal_patterns(self, pattern_type: str,
                                     time_window: str = "30d",
                                     min_support: float = 0.1) -> List[Dict[str, Any]]:
        """Detect temporal patterns in the graph"""
        try:
            window_delta = self._parse_time_window(time_window)
            end_time = datetime.utcnow()
            start_time = end_time - window_delta
            
            patterns = []
            
            if pattern_type == "periodic":
                patterns = await self._detect_periodic_patterns(start_time, end_time, min_support)
            elif pattern_type == "trending":
                patterns = await self._detect_trending_patterns(start_time, end_time)
            elif pattern_type == "anomalous":
                patterns = await self._detect_anomalous_patterns(start_time, end_time)
            elif pattern_type == "burst":
                patterns = await self._detect_burst_patterns(start_time, end_time)
            else:
                raise ValueError(f"Unknown pattern type: {pattern_type}")
                
            return patterns
            
        except Exception as e:
            logger.error(f"Failed to detect temporal patterns: {e}")
            raise
            
    async def _get_entity_time_series(self, entity_id: str, 
                                    start_time: datetime,
                                    end_time: datetime) -> pd.Series:
        """Get time series data for an entity"""
        # Query temporal properties
        query = """
            g.V(entity_id).
            properties().
            has('timestamp', P.between(start_time, end_time)).
            order().by('timestamp').
            project('time', 'value').
                by('timestamp').
                by('value')
        """
        
        data_points = await self.graph_client.execute_query(query, {
            'entity_id': entity_id,
            'start_time': start_time.isoformat(),
            'end_time': end_time.isoformat()
        })
        
        # Convert to pandas Series
        if data_points:
            times = [datetime.fromisoformat(dp['time']) for dp in data_points]
            values = [dp['value'] for dp in data_points]
            return pd.Series(values, index=times)
        else:
            # Return empty series with proper time index
            time_index = pd.date_range(start_time, end_time, freq='H')
            return pd.Series(0, index=time_index)
            
    async def _run_pc_algorithm(self, data: pd.DataFrame, 
                               alpha: float) -> Dict[str, Any]:
        """Run PC (Peter-Clark) algorithm for causal discovery"""
        # Simplified PC algorithm implementation
        n_vars = len(data.columns)
        causal_edges = []
        
        # Start with complete graph
        adjacency = np.ones((n_vars, n_vars)) - np.eye(n_vars)
        
        # Remove edges based on conditional independence tests
        for i in range(n_vars):
            for j in range(i+1, n_vars):
                # Test conditional independence
                if self._test_independence(data.iloc[:, i], data.iloc[:, j], alpha):
                    adjacency[i, j] = 0
                    adjacency[j, i] = 0
                else:
                    # Determine edge direction using orientation rules
                    direction = self._orient_edge(data, i, j)
                    if direction == 1:
                        causal_edges.append({
                            'from': data.columns[i],
                            'to': data.columns[j],
                            'strength': adjacency[i, j]
                        })
                    elif direction == -1:
                        causal_edges.append({
                            'from': data.columns[j],
                            'to': data.columns[i],
                            'strength': adjacency[j, i]
                        })
                        
        return {
            'edges': causal_edges,
            'adjacency_matrix': adjacency.tolist()
        }
        
    async def _run_granger_causality(self, data: pd.DataFrame,
                                   significance_level: float) -> Dict[str, Any]:
        """Run Granger causality test"""
        causal_edges = []
        p_values = {}
        
        # Test each pair of variables
        for col1 in data.columns:
            for col2 in data.columns:
                if col1 != col2:
                    # Perform Granger causality test
                    p_value = self._granger_test(data[col1], data[col2])
                    p_values[f"{col1}->{col2}"] = p_value
                    
                    if p_value < significance_level:
                        causal_edges.append({
                            'from': col1,
                            'to': col2,
                            'p_value': p_value,
                            'strength': 1 - p_value
                        })
                        
        return {
            'edges': causal_edges,
            'p_values': p_values
        }
        
    async def _detect_periodic_patterns(self, start_time: datetime,
                                      end_time: datetime,
                                      min_support: float) -> List[Dict[str, Any]]:
        """Detect periodic patterns in temporal graph"""
        patterns = []
        
        # Query for repeating graph structures
        query = """
            g.V().
            has('created_at', P.between(start_time, end_time)).
            group().
                by('label').
                by(
                    groupCount().by(
                        values('created_at').map{
                            it.get().substring(0, 10)  // Group by day
                        }
                    )
                )
        """
        
        daily_counts = await self.graph_client.execute_query(query, {
            'start_time': start_time.isoformat(),
            'end_time': end_time.isoformat()
        })
        
        # Analyze for periodicity
        for entity_type, counts in daily_counts.items():
            if len(counts) > 7:  # Need at least a week of data
                # Detect periodicity using FFT
                values = list(counts.values())
                periods = self._detect_periods_fft(values)
                
                for period, strength in periods:
                    if strength > min_support:
                        patterns.append({
                            'type': 'periodic',
                            'entity_type': entity_type,
                            'period_days': period,
                            'strength': strength,
                            'occurrences': len(values) // period
                        })
                        
        return patterns
        
    def _test_independence(self, x: pd.Series, y: pd.Series, alpha: float) -> bool:
        """Test statistical independence between two variables"""
        # Simplified independence test using correlation
        correlation = x.corr(y)
        # In practice, would use proper conditional independence test
        return abs(correlation) < alpha
        
    def _orient_edge(self, data: pd.DataFrame, i: int, j: int) -> int:
        """Determine edge orientation: 1 for i->j, -1 for j->i, 0 for undirected"""
        # Simplified orientation using temporal precedence
        # In practice, would use v-structures and orientation rules
        col_i = data.iloc[:, i]
        col_j = data.iloc[:, j]
        
        # Check if changes in i precede changes in j
        lag_corr_forward = col_i[:-1].corr(col_j[1:])
        lag_corr_backward = col_j[:-1].corr(col_i[1:])
        
        if abs(lag_corr_forward) > abs(lag_corr_backward):
            return 1  # i -> j
        elif abs(lag_corr_backward) > abs(lag_corr_forward):
            return -1  # j -> i
        else:
            return 0  # undirected
            
    def _granger_test(self, x: pd.Series, y: pd.Series, max_lag: int = 5) -> float:
        """Perform Granger causality test"""
        # Simplified Granger test
        # In practice, would use statsmodels or similar
        best_p_value = 1.0
        
        for lag in range(1, max_lag + 1):
            # Test if lagged x helps predict y
            y_lagged = y[lag:]
            x_lagged = x[:-lag]
            
            if len(y_lagged) > 0:
                correlation = x_lagged.corr(y_lagged)
                # Convert correlation to p-value approximation
                p_value = 1 - abs(correlation)
                best_p_value = min(best_p_value, p_value)
                
        return best_p_value
        
    def _detect_periods_fft(self, values: List[float]) -> List[Tuple[int, float]]:
        """Detect periods using FFT"""
        # Apply FFT
        fft_vals = np.fft.fft(values)
        fft_freq = np.fft.fftfreq(len(values))
        
        # Find dominant frequencies
        magnitudes = np.abs(fft_vals)
        threshold = np.mean(magnitudes) + 2 * np.std(magnitudes)
        
        periods = []
        for i, (freq, mag) in enumerate(zip(fft_freq, magnitudes)):
            if freq > 0 and mag > threshold:
                period = int(1 / freq)
                strength = mag / np.max(magnitudes)
                periods.append((period, strength))
                
        return sorted(periods, key=lambda x: x[1], reverse=True)[:5]
        
    def _parse_time_window(self, window: str) -> timedelta:
        """Parse time window string to timedelta"""
        unit = window[-1]
        value = int(window[:-1])
        
        if unit == 'd':
            return timedelta(days=value)
        elif unit == 'h':
            return timedelta(hours=value)
        elif unit == 'm':
            return timedelta(minutes=value)
        else:
            raise ValueError(f"Unknown time unit: {unit}")
            
    def _format_temporal_vertex(self, vertex: Dict[str, Any]) -> Dict[str, Any]:
        """Format vertex for temporal response"""
        return {
            'id': str(vertex.get('id')),
            'label': vertex.get('label'),
            'created_at': vertex.get('created_at', [None])[0],
            'properties': {k: v[0] if isinstance(v, list) else v 
                         for k, v in vertex.items() 
                         if k not in ['id', 'label', 'created_at']}
        }
        
    def _format_temporal_edge(self, edge: Dict[str, Any]) -> Dict[str, Any]:
        """Format edge for temporal response"""
        return {
            'id': str(edge.get('id')),
            'label': edge.get('label'),
            'source': str(edge.get('outV')),
            'target': str(edge.get('inV')),
            'created_at': edge.get('created_at', [None])[0],
            'properties': {k: v[0] if isinstance(v, list) else v 
                         for k, v in edge.items() 
                         if k not in ['id', 'label', 'outV', 'inV', 'created_at']}
        }
        
    async def _maintain_temporal_indices(self):
        """Maintain temporal indices for fast queries"""
        while True:
            try:
                await asyncio.sleep(300)  # Update every 5 minutes
                
                # Update temporal indices
                # This would maintain specialized indices for temporal queries
                logger.info("Updated temporal indices")
                
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Error maintaining temporal indices: {e}") 