"""
Simulation lineage tracking in JanusGraph

Tracks simulation history, parameter evolution, user actions, branches,
and result provenance using the graph database.
"""

import asyncio
from typing import Dict, List, Optional, Any, Tuple
from datetime import datetime, timedelta
import json
import logging
from collections import defaultdict
import uuid

from gremlin_python.process.graph_traversal import __
from gremlin_python.process.traversal import T, P, Order, Scope
from gremlin_python.structure.graph import Vertex, Edge

from .db.janusgraph_client import janusgraph_service

logger = logging.getLogger(__name__)


class SimulationLineageTracker:
    """Tracks simulation lineage and relationships in JanusGraph"""
    
    def __init__(self):
        self.g = janusgraph_service.g
        
    async def initialize_schema(self):
        """Initialize graph schema for simulation tracking"""
        try:
            # Define vertex labels and properties
            vertex_schema = {
                'simulation': ['simulation_id', 'name', 'created_by', 'created_at'],
                'simulation_session': ['session_id', 'simulation_id', 'started_at', 'state'],
                'simulation_branch': ['branch_id', 'branch_name', 'parent_branch', 'created_at'],
                'simulation_parameter': ['parameter_id', 'name', 'data_type', 'value'],
                'simulation_agent': ['agent_id', 'agent_type', 'created_at'],
                'simulation_checkpoint': ['checkpoint_id', 'created_at', 'tick_count'],
                'simulation_operation': ['operation_id', 'operation_type', 'timestamp', 'user_id'],
                'simulation_result': ['result_id', 'metric_type', 'value', 'computed_at'],
                'user': ['user_id', 'name']
            }
            
            # Define edge labels
            edge_schema = {
                'HAS_SESSION': ['simulation', 'simulation_session'],
                'IN_BRANCH': ['simulation_session', 'simulation_branch'],
                'BRANCHED_FROM': ['simulation_branch', 'simulation_branch'],
                'HAS_PARAMETER': ['simulation_session', 'simulation_parameter'],
                'CONTAINS_AGENT': ['simulation_session', 'simulation_agent'],
                'CREATED_CHECKPOINT': ['simulation_session', 'simulation_checkpoint'],
                'PERFORMED_OPERATION': ['user', 'simulation_operation'],
                'OPERATION_ON': ['simulation_operation', 'simulation_session'],
                'PARAMETER_CHANGED': ['simulation_operation', 'simulation_parameter'],
                'AGENT_AFFECTED': ['simulation_operation', 'simulation_agent'],
                'PRODUCED_RESULT': ['simulation_session', 'simulation_result'],
                'DERIVED_FROM': ['simulation_result', 'simulation_parameter'],
                'RESTORED_FROM': ['simulation_session', 'simulation_checkpoint']
            }
            
            logger.info("Simulation lineage schema initialized")
            
        except Exception as e:
            logger.error(f"Error initializing simulation schema: {e}")
    
    async def track_simulation_created(self, simulation_id: str, name: str, 
                                     created_by: str, metadata: Dict[str, Any]):
        """Track new simulation creation"""
        try:
            # Create simulation vertex
            sim_vertex = self.g.addV('simulation') \
                .property('simulation_id', simulation_id) \
                .property('name', name) \
                .property('created_by', created_by) \
                .property('created_at', datetime.utcnow().isoformat()) \
                .property('metadata', json.dumps(metadata)) \
                .next()
            
            # Create user vertex if not exists
            user_vertex = self.g.V().has('user', 'user_id', created_by).fold() \
                .coalesce(
                    __.unfold(),
                    __.addV('user').property('user_id', created_by)
                ).next()
            
            # Link user to simulation
            self.g.V(user_vertex).addE('CREATED').to(V(sim_vertex)) \
                .property('timestamp', datetime.utcnow().isoformat()) \
                .iterate()
            
            logger.info(f"Tracked simulation creation: {simulation_id}")
            
        except Exception as e:
            logger.error(f"Error tracking simulation creation: {e}")
    
    async def track_session_started(self, session_id: str, simulation_id: str,
                                  user_id: str, initial_parameters: Dict[str, Any]):
        """Track new collaboration session"""
        try:
            # Get simulation vertex
            sim_vertex = self.g.V().has('simulation', 'simulation_id', simulation_id).next()
            
            # Create session vertex
            session_vertex = self.g.addV('simulation_session') \
                .property('session_id', session_id) \
                .property('simulation_id', simulation_id) \
                .property('started_at', datetime.utcnow().isoformat()) \
                .property('state', 'active') \
                .next()
            
            # Link to simulation
            self.g.V(sim_vertex).addE('HAS_SESSION').to(V(session_vertex)).iterate()
            
            # Create initial branch
            branch_vertex = self.g.addV('simulation_branch') \
                .property('branch_id', f"{session_id}:main") \
                .property('branch_name', 'main') \
                .property('created_at', datetime.utcnow().isoformat()) \
                .next()
            
            self.g.V(session_vertex).addE('IN_BRANCH').to(V(branch_vertex)).iterate()
            
            # Track initial parameters
            for param_name, param_value in initial_parameters.items():
                await self.track_parameter_set(
                    session_id, param_name, param_value, user_id, 'initial'
                )
            
            logger.info(f"Tracked session start: {session_id}")
            
        except Exception as e:
            logger.error(f"Error tracking session start: {e}")
    
    async def track_operation(self, session_id: str, operation_id: str,
                            operation_type: str, user_id: str,
                            target_type: str, target_id: str,
                            operation_data: Dict[str, Any],
                            parent_operations: List[str] = None):
        """Track a simulation operation"""
        try:
            # Create operation vertex
            op_vertex = self.g.addV('simulation_operation') \
                .property('operation_id', operation_id) \
                .property('operation_type', operation_type) \
                .property('timestamp', datetime.utcnow().isoformat()) \
                .property('user_id', user_id) \
                .property('data', json.dumps(operation_data)) \
                .next()
            
            # Get session vertex
            session_vertex = self.g.V().has('simulation_session', 'session_id', session_id).next()
            
            # Link operation to session
            self.g.V(op_vertex).addE('OPERATION_ON').to(V(session_vertex)).iterate()
            
            # Link user to operation
            user_vertex = self.g.V().has('user', 'user_id', user_id).fold() \
                .coalesce(
                    __.unfold(),
                    __.addV('user').property('user_id', user_id)
                ).next()
            
            self.g.V(user_vertex).addE('PERFORMED_OPERATION').to(V(op_vertex)) \
                .property('timestamp', datetime.utcnow().isoformat()) \
                .iterate()
            
            # Link to target (parameter, agent, etc.)
            if target_type == 'parameter':
                param_vertex = self.g.V().has('simulation_parameter', 'parameter_id', 
                                            f"{session_id}:{target_id}").next()
                self.g.V(op_vertex).addE('PARAMETER_CHANGED').to(V(param_vertex)).iterate()
                
            elif target_type == 'agent':
                agent_vertex = self.g.V().has('simulation_agent', 'agent_id', 
                                            f"{session_id}:{target_id}").next()
                self.g.V(op_vertex).addE('AGENT_AFFECTED').to(V(agent_vertex)).iterate()
            
            # Link to parent operations for causal chain
            if parent_operations:
                for parent_id in parent_operations:
                    parent_vertex = self.g.V().has('simulation_operation', 
                                                 'operation_id', parent_id).next()
                    self.g.V(op_vertex).addE('DEPENDS_ON').to(V(parent_vertex)).iterate()
            
            logger.info(f"Tracked operation: {operation_id} of type {operation_type}")
            
        except Exception as e:
            logger.error(f"Error tracking operation: {e}")
    
    async def track_parameter_set(self, session_id: str, param_name: str,
                                param_value: Any, user_id: str,
                                data_type: str = 'float'):
        """Track parameter change"""
        try:
            param_id = f"{session_id}:{param_name}"
            
            # Check if parameter exists
            existing = self.g.V().has('simulation_parameter', 'parameter_id', param_id).toList()
            
            if existing:
                # Update existing parameter
                param_vertex = existing[0]
                
                # Store previous value in history
                prev_value = self.g.V(param_vertex).values('value').next()
                self.g.V(param_vertex) \
                    .property('previous_value', prev_value) \
                    .property('value', str(param_value)) \
                    .property('updated_at', datetime.utcnow().isoformat()) \
                    .property('updated_by', user_id) \
                    .iterate()
            else:
                # Create new parameter
                param_vertex = self.g.addV('simulation_parameter') \
                    .property('parameter_id', param_id) \
                    .property('name', param_name) \
                    .property('data_type', data_type) \
                    .property('value', str(param_value)) \
                    .property('created_at', datetime.utcnow().isoformat()) \
                    .property('created_by', user_id) \
                    .next()
                
                # Link to session
                session_vertex = self.g.V().has('simulation_session', 
                                              'session_id', session_id).next()
                self.g.V(session_vertex).addE('HAS_PARAMETER').to(V(param_vertex)).iterate()
            
            logger.info(f"Tracked parameter: {param_name} = {param_value}")
            
        except Exception as e:
            logger.error(f"Error tracking parameter: {e}")
    
    async def track_agent_lifecycle(self, session_id: str, agent_id: str,
                                  agent_type: str, lifecycle_event: str,
                                  user_id: str, properties: Dict[str, Any] = None):
        """Track agent creation, modification, or deletion"""
        try:
            full_agent_id = f"{session_id}:{agent_id}"
            
            if lifecycle_event == 'created':
                # Create agent vertex
                agent_vertex = self.g.addV('simulation_agent') \
                    .property('agent_id', full_agent_id) \
                    .property('agent_type', agent_type) \
                    .property('created_at', datetime.utcnow().isoformat()) \
                    .property('created_by', user_id) \
                    .next()
                
                # Add properties
                if properties:
                    for key, value in properties.items():
                        self.g.V(agent_vertex).property(key, str(value)).iterate()
                
                # Link to session
                session_vertex = self.g.V().has('simulation_session', 
                                              'session_id', session_id).next()
                self.g.V(session_vertex).addE('CONTAINS_AGENT').to(V(agent_vertex)).iterate()
                
            elif lifecycle_event == 'modified':
                # Update agent properties
                agent_vertex = self.g.V().has('simulation_agent', 
                                            'agent_id', full_agent_id).next()
                
                if properties:
                    for key, value in properties.items():
                        self.g.V(agent_vertex).property(key, str(value)).iterate()
                
                self.g.V(agent_vertex) \
                    .property('last_modified', datetime.utcnow().isoformat()) \
                    .property('modified_by', user_id) \
                    .iterate()
                
            elif lifecycle_event == 'deleted':
                # Mark as deleted (soft delete)
                agent_vertex = self.g.V().has('simulation_agent', 
                                            'agent_id', full_agent_id).next()
                self.g.V(agent_vertex) \
                    .property('deleted', True) \
                    .property('deleted_at', datetime.utcnow().isoformat()) \
                    .property('deleted_by', user_id) \
                    .iterate()
            
            logger.info(f"Tracked agent {lifecycle_event}: {agent_id}")
            
        except Exception as e:
            logger.error(f"Error tracking agent lifecycle: {e}")
    
    async def track_branch_created(self, session_id: str, branch_id: str,
                                 branch_name: str, parent_branch_id: str,
                                 user_id: str, base_tick: int):
        """Track branch creation"""
        try:
            # Create branch vertex
            branch_vertex = self.g.addV('simulation_branch') \
                .property('branch_id', f"{session_id}:{branch_id}") \
                .property('branch_name', branch_name) \
                .property('parent_branch', parent_branch_id) \
                .property('created_at', datetime.utcnow().isoformat()) \
                .property('created_by', user_id) \
                .property('base_tick', base_tick) \
                .next()
            
            # Link to parent branch
            if parent_branch_id:
                parent_vertex = self.g.V().has('simulation_branch', 
                                             'branch_id', f"{session_id}:{parent_branch_id}").next()
                self.g.V(branch_vertex).addE('BRANCHED_FROM').to(V(parent_vertex)) \
                    .property('tick', base_tick) \
                    .iterate()
            
            # Link to session
            session_vertex = self.g.V().has('simulation_session', 
                                          'session_id', session_id).next()
            self.g.V(session_vertex).addE('HAS_BRANCH').to(V(branch_vertex)).iterate()
            
            logger.info(f"Tracked branch creation: {branch_name}")
            
        except Exception as e:
            logger.error(f"Error tracking branch: {e}")
    
    async def track_checkpoint(self, session_id: str, checkpoint_id: str,
                             user_id: str, tick_count: int,
                             metadata: Dict[str, Any]):
        """Track checkpoint creation"""
        try:
            # Create checkpoint vertex
            checkpoint_vertex = self.g.addV('simulation_checkpoint') \
                .property('checkpoint_id', checkpoint_id) \
                .property('created_at', datetime.utcnow().isoformat()) \
                .property('created_by', user_id) \
                .property('tick_count', tick_count) \
                .property('metadata', json.dumps(metadata)) \
                .next()
            
            # Link to session
            session_vertex = self.g.V().has('simulation_session', 
                                          'session_id', session_id).next()
            self.g.V(session_vertex).addE('CREATED_CHECKPOINT').to(V(checkpoint_vertex)) \
                .property('tick', tick_count) \
                .iterate()
            
            # Capture current parameter values
            params = self.g.V(session_vertex).out('HAS_PARAMETER').toList()
            for param in params:
                self.g.V(checkpoint_vertex).addE('CAPTURED_PARAMETER').to(param) \
                    .property('value', self.g.V(param).values('value').next()) \
                    .iterate()
            
            logger.info(f"Tracked checkpoint: {checkpoint_id}")
            
        except Exception as e:
            logger.error(f"Error tracking checkpoint: {e}")
    
    async def track_result(self, session_id: str, result_id: str,
                         metric_type: str, value: float,
                         contributing_params: List[str]):
        """Track simulation result with provenance"""
        try:
            # Create result vertex
            result_vertex = self.g.addV('simulation_result') \
                .property('result_id', result_id) \
                .property('metric_type', metric_type) \
                .property('value', value) \
                .property('computed_at', datetime.utcnow().isoformat()) \
                .next()
            
            # Link to session
            session_vertex = self.g.V().has('simulation_session', 
                                          'session_id', session_id).next()
            self.g.V(session_vertex).addE('PRODUCED_RESULT').to(V(result_vertex)).iterate()
            
            # Link to contributing parameters for provenance
            for param_name in contributing_params:
                param_vertex = self.g.V().has('simulation_parameter', 
                                            'parameter_id', f"{session_id}:{param_name}").next()
                self.g.V(result_vertex).addE('DERIVED_FROM').to(V(param_vertex)) \
                    .property('influence_type', 'direct') \
                    .iterate()
            
            logger.info(f"Tracked result: {metric_type} = {value}")
            
        except Exception as e:
            logger.error(f"Error tracking result: {e}")
    
    async def get_parameter_history(self, session_id: str, param_name: str) -> List[Dict[str, Any]]:
        """Get parameter evolution history"""
        try:
            param_id = f"{session_id}:{param_name}"
            
            # Get all operations that changed this parameter
            history = self.g.V().has('simulation_parameter', 'parameter_id', param_id) \
                .in_('PARAMETER_CHANGED') \
                .order().by('timestamp', Order.asc) \
                .project('operation_id', 'user_id', 'timestamp', 'data') \
                .by('operation_id') \
                .by('user_id') \
                .by('timestamp') \
                .by('data') \
                .toList()
            
            return history
            
        except Exception as e:
            logger.error(f"Error getting parameter history: {e}")
            return []
    
    async def get_simulation_lineage(self, simulation_id: str) -> Dict[str, Any]:
        """Get complete lineage graph for a simulation"""
        try:
            # Get all sessions
            sessions = self.g.V().has('simulation', 'simulation_id', simulation_id) \
                .out('HAS_SESSION') \
                .project('session_id', 'started_at', 'state') \
                .by('session_id') \
                .by('started_at') \
                .by('state') \
                .toList()
            
            # Get branches for each session
            lineage = {
                'simulation_id': simulation_id,
                'sessions': []
            }
            
            for session in sessions:
                session_data = {
                    'session_id': session['session_id'],
                    'started_at': session['started_at'],
                    'branches': self._get_branch_tree(session['session_id']),
                    'checkpoints': self._get_checkpoints(session['session_id']),
                    'results': self._get_results(session['session_id'])
                }
                lineage['sessions'].append(session_data)
            
            return lineage
            
        except Exception as e:
            logger.error(f"Error getting simulation lineage: {e}")
            return {}
    
    def _get_branch_tree(self, session_id: str) -> List[Dict[str, Any]]:
        """Get branch hierarchy for a session"""
        try:
            branches = self.g.V().has('simulation_session', 'session_id', session_id) \
                .out('HAS_BRANCH') \
                .project('branch_id', 'branch_name', 'parent_branch', 'created_at') \
                .by('branch_id') \
                .by('branch_name') \
                .by('parent_branch') \
                .by('created_at') \
                .toList()
            
            return branches
            
        except Exception as e:
            logger.error(f"Error getting branch tree: {e}")
            return []
    
    def _get_checkpoints(self, session_id: str) -> List[Dict[str, Any]]:
        """Get checkpoints for a session"""
        try:
            checkpoints = self.g.V().has('simulation_session', 'session_id', session_id) \
                .out('CREATED_CHECKPOINT') \
                .order().by('created_at', Order.desc) \
                .limit(10) \
                .project('checkpoint_id', 'created_at', 'tick_count') \
                .by('checkpoint_id') \
                .by('created_at') \
                .by('tick_count') \
                .toList()
            
            return checkpoints
            
        except Exception as e:
            logger.error(f"Error getting checkpoints: {e}")
            return []
    
    def _get_results(self, session_id: str) -> List[Dict[str, Any]]:
        """Get results for a session"""
        try:
            results = self.g.V().has('simulation_session', 'session_id', session_id) \
                .out('PRODUCED_RESULT') \
                .project('result_id', 'metric_type', 'value', 'computed_at') \
                .by('result_id') \
                .by('metric_type') \
                .by('value') \
                .by('computed_at') \
                .toList()
            
            return results
            
        except Exception as e:
            logger.error(f"Error getting results: {e}")
            return []
    
    async def find_parameter_influence(self, session_id: str, param_name: str, 
                                     result_metric: str) -> List[Dict[str, Any]]:
        """Find how a parameter influenced a specific result"""
        try:
            param_id = f"{session_id}:{param_name}"
            
            # Find paths from parameter to result
            paths = self.g.V().has('simulation_parameter', 'parameter_id', param_id) \
                .repeat(__.out('DERIVED_FROM').simplePath()).times(5) \
                .has('simulation_result', 'metric_type', result_metric) \
                .path() \
                .by(__.valueMap('parameter_id', 'metric_type', 'value')) \
                .toList()
            
            return paths
            
        except Exception as e:
            logger.error(f"Error finding parameter influence: {e}")
            return [] 