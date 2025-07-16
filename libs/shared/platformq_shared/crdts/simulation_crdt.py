"""
Conflict-Free Replicated Data Type (CRDT) for Simulation Collaboration

This module implements a CRDT designed for real-time collaborative simulation editing,
supporting concurrent modifications to simulation parameters, agents, and execution state.
"""

from typing import Dict, List, Set, Tuple, Optional, Any, Union
from dataclasses import dataclass, field
from enum import Enum
import numpy as np
import uuid
import time
import json
import zlib
from collections import defaultdict
import threading
from queue import PriorityQueue

from .geometry_crdt import VectorClock, GeometryOperation


class SimulationOperationType(Enum):
    """Types of simulation operations"""
    # Parameter operations
    SET_PARAMETER = "set_parameter"
    UPDATE_ENVIRONMENT = "update_environment"
    
    # Agent operations
    ADD_AGENT = "add_agent"
    REMOVE_AGENT = "remove_agent"
    UPDATE_AGENT = "update_agent"
    BATCH_ADD_AGENTS = "batch_add_agents"
    
    # Control operations
    START_SIMULATION = "start_simulation"
    PAUSE_SIMULATION = "pause_simulation"
    STEP_SIMULATION = "step_simulation"
    RESET_SIMULATION = "reset_simulation"
    
    # Branch operations
    CREATE_BRANCH = "create_branch"
    SWITCH_BRANCH = "switch_branch"
    MERGE_BRANCH = "merge_branch"
    
    # View operations
    SET_VIEW = "set_view"
    SET_FILTER = "set_filter"


@dataclass
class SimulationAgent:
    """Represents an agent in the simulation"""
    id: str
    agent_type: str
    position: np.ndarray
    velocity: np.ndarray
    properties: Dict[str, Any] = field(default_factory=dict)
    behavior_params: Dict[str, Any] = field(default_factory=dict)
    created_by: str = ""
    created_at: float = 0.0
    deleted: bool = False


@dataclass
class SimulationParameter:
    """Represents a simulation parameter with history"""
    name: str
    value: Any
    data_type: str
    last_modified_by: str
    last_modified_at: float
    history: List[Tuple[str, Any, float]] = field(default_factory=list)


@dataclass
class SimulationBranch:
    """Represents a branch in simulation history"""
    id: str
    name: str
    parent_branch_id: Optional[str]
    created_by: str
    created_at: float
    base_tick: int
    operations: List[str] = field(default_factory=list)


@dataclass
class SimulationOperation:
    """Operation in the simulation CRDT"""
    id: str
    replica_id: str
    operation_type: SimulationOperationType
    timestamp: float
    simulation_tick: int
    vector_clock: VectorClock
    target_id: Optional[str] = None
    data: Dict[str, Any] = field(default_factory=dict)
    parent_operations: List[str] = field(default_factory=list)
    branch_id: str = "main"


class SimulationControlQueue:
    """Priority queue for simulation control operations"""
    
    def __init__(self):
        self.queue = PriorityQueue()
        self.processed = set()
        self.lock = threading.Lock()
    
    def add_operation(self, operation: SimulationOperation, priority: int = 0):
        """Add control operation with priority"""
        with self.lock:
            if operation.id not in self.processed:
                # Priority based on: user role, operation type, timestamp
                self.queue.put((priority, operation.timestamp, operation))
    
    def get_next_operation(self) -> Optional[SimulationOperation]:
        """Get next operation to execute"""
        try:
            _, _, operation = self.queue.get_nowait()
            self.processed.add(operation.id)
            return operation
        except:
            return None


class SimulationCRDT:
    """
    CRDT for collaborative simulation editing
    Supports parameters, agents, control flow, and branching
    """
    
    def __init__(self, replica_id: str, simulation_id: str):
        self.replica_id = replica_id
        self.simulation_id = simulation_id
        
        # Core state
        self.parameters: Dict[str, SimulationParameter] = {}
        self.agents: Dict[str, SimulationAgent] = {}
        self.environment: Dict[str, Any] = {}
        
        # Execution state
        self.simulation_state = "stopped"  # stopped, running, paused
        self.current_tick = 0
        self.tick_rate = 60  # Hz
        
        # Branching
        self.branches: Dict[str, SimulationBranch] = {
            "main": SimulationBranch(
                id="main",
                name="main",
                parent_branch_id=None,
                created_by=replica_id,
                created_at=time.time(),
                base_tick=0
            )
        }
        self.current_branch = "main"
        
        # CRDT machinery
        self.vector_clock = VectorClock()
        self.operation_log: List[SimulationOperation] = []
        self.applied_operations: Set[str] = set()
        
        # Control queue for simulation commands
        self.control_queue = SimulationControlQueue()
        
        # View state (per replica)
        self.view_state = {
            "camera_position": [0, 0, 10],
            "camera_target": [0, 0, 0],
            "selected_agents": [],
            "filters": {}
        }
    
    def set_parameter(self, name: str, value: Any, data_type: str = "float") -> str:
        """Set a simulation parameter"""
        self.vector_clock.increment(self.replica_id)
        
        operation = SimulationOperation(
            id=str(uuid.uuid4()),
            replica_id=self.replica_id,
            operation_type=SimulationOperationType.SET_PARAMETER,
            timestamp=time.time(),
            simulation_tick=self.current_tick,
            vector_clock=VectorClock(clock=self.vector_clock.clock.copy()),
            data={
                "name": name,
                "value": value,
                "data_type": data_type
            },
            branch_id=self.current_branch
        )
        
        self._apply_operation(operation)
        return operation.id
    
    def add_agent(self, agent_type: str, position: List[float], 
                  properties: Dict[str, Any] = None, 
                  behavior_params: Dict[str, Any] = None) -> str:
        """Add an agent to the simulation"""
        agent_id = str(uuid.uuid4())
        self.vector_clock.increment(self.replica_id)
        
        operation = SimulationOperation(
            id=str(uuid.uuid4()),
            replica_id=self.replica_id,
            operation_type=SimulationOperationType.ADD_AGENT,
            timestamp=time.time(),
            simulation_tick=self.current_tick,
            vector_clock=VectorClock(clock=self.vector_clock.clock.copy()),
            target_id=agent_id,
            data={
                "agent_type": agent_type,
                "position": position,
                "velocity": [0, 0, 0],
                "properties": properties or {},
                "behavior_params": behavior_params or {}
            },
            branch_id=self.current_branch
        )
        
        self._apply_operation(operation)
        return agent_id
    
    def update_agent(self, agent_id: str, updates: Dict[str, Any]) -> str:
        """Update agent properties"""
        if agent_id not in self.agents:
            raise ValueError(f"Agent {agent_id} not found")
        
        self.vector_clock.increment(self.replica_id)
        
        operation = SimulationOperation(
            id=str(uuid.uuid4()),
            replica_id=self.replica_id,
            operation_type=SimulationOperationType.UPDATE_AGENT,
            timestamp=time.time(),
            simulation_tick=self.current_tick,
            vector_clock=VectorClock(clock=self.vector_clock.clock.copy()),
            target_id=agent_id,
            data=updates,
            branch_id=self.current_branch
        )
        
        self._apply_operation(operation)
        return operation.id
    
    def control_simulation(self, action: str) -> str:
        """Control simulation execution (start, pause, step, reset)"""
        control_ops = {
            "start": SimulationOperationType.START_SIMULATION,
            "pause": SimulationOperationType.PAUSE_SIMULATION,
            "step": SimulationOperationType.STEP_SIMULATION,
            "reset": SimulationOperationType.RESET_SIMULATION
        }
        
        if action not in control_ops:
            raise ValueError(f"Invalid control action: {action}")
        
        self.vector_clock.increment(self.replica_id)
        
        operation = SimulationOperation(
            id=str(uuid.uuid4()),
            replica_id=self.replica_id,
            operation_type=control_ops[action],
            timestamp=time.time(),
            simulation_tick=self.current_tick,
            vector_clock=VectorClock(clock=self.vector_clock.clock.copy()),
            branch_id=self.current_branch
        )
        
        # Add to control queue with priority
        priority = 0 if action == "pause" else 1  # Pause has highest priority
        self.control_queue.add_operation(operation, priority)
        
        self._apply_operation(operation)
        return operation.id
    
    def create_branch(self, branch_name: str) -> str:
        """Create a new branch from current state"""
        branch_id = str(uuid.uuid4())
        self.vector_clock.increment(self.replica_id)
        
        branch = SimulationBranch(
            id=branch_id,
            name=branch_name,
            parent_branch_id=self.current_branch,
            created_by=self.replica_id,
            created_at=time.time(),
            base_tick=self.current_tick
        )
        
        operation = SimulationOperation(
            id=str(uuid.uuid4()),
            replica_id=self.replica_id,
            operation_type=SimulationOperationType.CREATE_BRANCH,
            timestamp=time.time(),
            simulation_tick=self.current_tick,
            vector_clock=VectorClock(clock=self.vector_clock.clock.copy()),
            target_id=branch_id,
            data={"branch_name": branch_name},
            branch_id=self.current_branch
        )
        
        self._apply_operation(operation)
        return branch_id
    
    def _apply_operation(self, operation: SimulationOperation):
        """Apply an operation to the CRDT state"""
        if operation.id in self.applied_operations:
            return
        
        # Check causal dependencies
        for parent_id in operation.parent_operations:
            if parent_id not in self.applied_operations:
                # Queue for later
                return
        
        # Apply based on operation type
        if operation.operation_type == SimulationOperationType.SET_PARAMETER:
            self._apply_set_parameter(operation)
        elif operation.operation_type == SimulationOperationType.ADD_AGENT:
            self._apply_add_agent(operation)
        elif operation.operation_type == SimulationOperationType.UPDATE_AGENT:
            self._apply_update_agent(operation)
        elif operation.operation_type == SimulationOperationType.REMOVE_AGENT:
            self._apply_remove_agent(operation)
        elif operation.operation_type in [
            SimulationOperationType.START_SIMULATION,
            SimulationOperationType.PAUSE_SIMULATION,
            SimulationOperationType.STEP_SIMULATION,
            SimulationOperationType.RESET_SIMULATION
        ]:
            self._apply_control_operation(operation)
        elif operation.operation_type == SimulationOperationType.CREATE_BRANCH:
            self._apply_create_branch(operation)
        elif operation.operation_type == SimulationOperationType.SET_VIEW:
            self._apply_set_view(operation)
        
        # Record operation
        self.operation_log.append(operation)
        self.applied_operations.add(operation.id)
        
        # Update vector clock
        self.vector_clock.merge(operation.vector_clock)
    
    def _apply_set_parameter(self, operation: SimulationOperation):
        """Apply parameter change"""
        data = operation.data
        param_name = data["name"]
        
        if param_name not in self.parameters:
            self.parameters[param_name] = SimulationParameter(
                name=param_name,
                value=data["value"],
                data_type=data["data_type"],
                last_modified_by=operation.replica_id,
                last_modified_at=operation.timestamp
            )
        else:
            param = self.parameters[param_name]
            # Add to history
            param.history.append((param.last_modified_by, param.value, param.last_modified_at))
            # Update value
            param.value = data["value"]
            param.last_modified_by = operation.replica_id
            param.last_modified_at = operation.timestamp
    
    def _apply_add_agent(self, operation: SimulationOperation):
        """Apply agent addition"""
        agent_id = operation.target_id
        data = operation.data
        
        agent = SimulationAgent(
            id=agent_id,
            agent_type=data["agent_type"],
            position=np.array(data["position"]),
            velocity=np.array(data["velocity"]),
            properties=data["properties"],
            behavior_params=data["behavior_params"],
            created_by=operation.replica_id,
            created_at=operation.timestamp
        )
        
        self.agents[agent_id] = agent
    
    def _apply_update_agent(self, operation: SimulationOperation):
        """Apply agent update"""
        agent_id = operation.target_id
        if agent_id not in self.agents:
            return
        
        agent = self.agents[agent_id]
        updates = operation.data
        
        # Update fields
        if "position" in updates:
            agent.position = np.array(updates["position"])
        if "velocity" in updates:
            agent.velocity = np.array(updates["velocity"])
        if "properties" in updates:
            agent.properties.update(updates["properties"])
        if "behavior_params" in updates:
            agent.behavior_params.update(updates["behavior_params"])
    
    def _apply_remove_agent(self, operation: SimulationOperation):
        """Apply agent removal"""
        agent_id = operation.target_id
        if agent_id in self.agents:
            self.agents[agent_id].deleted = True
    
    def _apply_control_operation(self, operation: SimulationOperation):
        """Apply simulation control operation"""
        if operation.operation_type == SimulationOperationType.START_SIMULATION:
            if self.simulation_state == "stopped" or self.simulation_state == "paused":
                self.simulation_state = "running"
        elif operation.operation_type == SimulationOperationType.PAUSE_SIMULATION:
            if self.simulation_state == "running":
                self.simulation_state = "paused"
        elif operation.operation_type == SimulationOperationType.STEP_SIMULATION:
            if self.simulation_state == "paused":
                self.current_tick += 1
        elif operation.operation_type == SimulationOperationType.RESET_SIMULATION:
            self.simulation_state = "stopped"
            self.current_tick = 0
    
    def _apply_create_branch(self, operation: SimulationOperation):
        """Apply branch creation"""
        branch_id = operation.target_id
        branch_name = operation.data["branch_name"]
        
        branch = SimulationBranch(
            id=branch_id,
            name=branch_name,
            parent_branch_id=self.current_branch,
            created_by=operation.replica_id,
            created_at=operation.timestamp,
            base_tick=self.current_tick
        )
        
        self.branches[branch_id] = branch
    
    def _apply_set_view(self, operation: SimulationOperation):
        """Apply view change (local only)"""
        if operation.replica_id == self.replica_id:
            self.view_state.update(operation.data)
    
    def merge(self, other: 'SimulationCRDT'):
        """Merge another CRDT instance"""
        # Get operations we haven't seen
        new_operations = [
            op for op in other.operation_log 
            if op.id not in self.applied_operations
        ]
        
        # Sort by vector clock and timestamp
        new_operations.sort(key=lambda op: (
            sum(op.vector_clock.clock.values()),
            op.timestamp
        ))
        
        # Apply new operations
        for op in new_operations:
            self._apply_operation(op)
    
    def get_state_snapshot(self) -> Dict[str, Any]:
        """Get current state snapshot"""
        return {
            "simulation_id": self.simulation_id,
            "current_tick": self.current_tick,
            "simulation_state": self.simulation_state,
            "tick_rate": self.tick_rate,
            "current_branch": self.current_branch,
            "parameters": {
                name: {
                    "value": param.value,
                    "data_type": param.data_type,
                    "last_modified_by": param.last_modified_by,
                    "last_modified_at": param.last_modified_at
                }
                for name, param in self.parameters.items()
            },
            "agent_count": len([a for a in self.agents.values() if not a.deleted]),
            "active_agents": [
                {
                    "id": agent.id,
                    "type": agent.agent_type,
                    "position": agent.position.tolist(),
                    "velocity": agent.velocity.tolist(),
                    "properties": agent.properties
                }
                for agent in self.agents.values()
                if not agent.deleted
            ][:100],  # Limit for performance
            "branches": [
                {
                    "id": branch.id,
                    "name": branch.name,
                    "parent": branch.parent_branch_id,
                    "created_by": branch.created_by
                }
                for branch in self.branches.values()
            ],
            "vector_clock": self.vector_clock.clock
        }
    
    def serialize(self) -> bytes:
        """Serialize CRDT state"""
        state = {
            "replica_id": self.replica_id,
            "simulation_id": self.simulation_id,
            "parameters": {
                name: {
                    "value": param.value,
                    "data_type": param.data_type,
                    "last_modified_by": param.last_modified_by,
                    "last_modified_at": param.last_modified_at,
                    "history": param.history
                }
                for name, param in self.parameters.items()
            },
            "agents": {
                agent_id: {
                    "id": agent.id,
                    "agent_type": agent.agent_type,
                    "position": agent.position.tolist(),
                    "velocity": agent.velocity.tolist(),
                    "properties": agent.properties,
                    "behavior_params": agent.behavior_params,
                    "created_by": agent.created_by,
                    "created_at": agent.created_at,
                    "deleted": agent.deleted
                }
                for agent_id, agent in self.agents.items()
            },
            "environment": self.environment,
            "simulation_state": self.simulation_state,
            "current_tick": self.current_tick,
            "tick_rate": self.tick_rate,
            "branches": {
                branch_id: {
                    "id": branch.id,
                    "name": branch.name,
                    "parent_branch_id": branch.parent_branch_id,
                    "created_by": branch.created_by,
                    "created_at": branch.created_at,
                    "base_tick": branch.base_tick,
                    "operations": branch.operations
                }
                for branch_id, branch in self.branches.items()
            },
            "current_branch": self.current_branch,
            "vector_clock": self.vector_clock.clock,
            "operation_log": [
                {
                    "id": op.id,
                    "replica_id": op.replica_id,
                    "operation_type": op.operation_type.value,
                    "timestamp": op.timestamp,
                    "simulation_tick": op.simulation_tick,
                    "vector_clock": op.vector_clock.clock,
                    "target_id": op.target_id,
                    "data": op.data,
                    "parent_operations": op.parent_operations,
                    "branch_id": op.branch_id
                }
                for op in self.operation_log
            ]
        }
        
        return zlib.compress(json.dumps(state).encode())
    
    @classmethod
    def deserialize(cls, data: bytes) -> 'SimulationCRDT':
        """Deserialize CRDT from bytes"""
        state = json.loads(zlib.decompress(data))
        
        crdt = cls(state["replica_id"], state["simulation_id"])
        
        # Restore parameters
        for name, param_data in state["parameters"].items():
            crdt.parameters[name] = SimulationParameter(
                name=name,
                value=param_data["value"],
                data_type=param_data["data_type"],
                last_modified_by=param_data["last_modified_by"],
                last_modified_at=param_data["last_modified_at"],
                history=param_data.get("history", [])
            )
        
        # Restore agents
        for agent_id, agent_data in state["agents"].items():
            crdt.agents[agent_id] = SimulationAgent(
                id=agent_data["id"],
                agent_type=agent_data["agent_type"],
                position=np.array(agent_data["position"]),
                velocity=np.array(agent_data["velocity"]),
                properties=agent_data["properties"],
                behavior_params=agent_data["behavior_params"],
                created_by=agent_data["created_by"],
                created_at=agent_data["created_at"],
                deleted=agent_data["deleted"]
            )
        
        # Restore other state
        crdt.environment = state["environment"]
        crdt.simulation_state = state["simulation_state"]
        crdt.current_tick = state["current_tick"]
        crdt.tick_rate = state["tick_rate"]
        
        # Restore branches
        for branch_id, branch_data in state["branches"].items():
            crdt.branches[branch_id] = SimulationBranch(
                id=branch_data["id"],
                name=branch_data["name"],
                parent_branch_id=branch_data["parent_branch_id"],
                created_by=branch_data["created_by"],
                created_at=branch_data["created_at"],
                base_tick=branch_data["base_tick"],
                operations=branch_data["operations"]
            )
        
        crdt.current_branch = state["current_branch"]
        crdt.vector_clock = VectorClock(clock=state["vector_clock"])
        
        # Restore operation log
        for op_data in state["operation_log"]:
            op = SimulationOperation(
                id=op_data["id"],
                replica_id=op_data["replica_id"],
                operation_type=SimulationOperationType(op_data["operation_type"]),
                timestamp=op_data["timestamp"],
                simulation_tick=op_data["simulation_tick"],
                vector_clock=VectorClock(clock=op_data["vector_clock"]),
                target_id=op_data["target_id"],
                data=op_data["data"],
                parent_operations=op_data["parent_operations"],
                branch_id=op_data["branch_id"]
            )
            crdt.operation_log.append(op)
            crdt.applied_operations.add(op.id)
        
        return crdt 