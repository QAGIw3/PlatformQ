"""
Simulation Domain Adapter

Handles agent-based simulations, multi-physics, and other simulation types.
"""

from typing import Dict, Any, List, Optional, Tuple
from datetime import datetime
import numpy as np

from .base import (
    BaseDomainAdapter, DomainOperation, DomainState, OperationType
)
from platformq_shared.crdt import SimulationCRDT


class SimulationOperation(DomainOperation):
    """Extended operation for simulation domain"""
    
    @property
    def operation_subtype(self) -> str:
        """Get simulation-specific operation type"""
        return self.data.get("subtype", "")
    
    def is_agent_operation(self) -> bool:
        return self.operation_subtype in ["add_agent", "remove_agent", "update_agent"]
    
    def is_parameter_operation(self) -> bool:
        return self.operation_subtype in ["set_parameter", "update_parameter"]
    
    def is_control_operation(self) -> bool:
        return self.operation_subtype in ["start", "pause", "step", "reset"]


class SimulationAdapter(BaseDomainAdapter):
    """Domain adapter for simulation collaboration"""
    
    def __init__(self):
        super().__init__("simulation")
        self.max_agents = 1_000_000
        self.max_broadcast_agents = 1000  # Limit for real-time updates
    
    def _initialize_handlers(self):
        """Initialize simulation-specific operation handlers"""
        self._operation_handlers = {
            "add_agent": self._handle_add_agent,
            "remove_agent": self._handle_remove_agent,
            "update_agent": self._handle_update_agent,
            "batch_update_agents": self._handle_batch_update_agents,
            "set_parameter": self._handle_set_parameter,
            "start_simulation": self._handle_start,
            "pause_simulation": self._handle_pause,
            "step_simulation": self._handle_step,
            "reset_simulation": self._handle_reset,
            "create_checkpoint": self._handle_checkpoint,
            "branch_simulation": self._handle_branch
        }
    
    def create_crdt(self) -> SimulationCRDT:
        """Create simulation-specific CRDT"""
        return SimulationCRDT()
    
    def validate_operation(self, operation: DomainOperation) -> Tuple[bool, Optional[str]]:
        """Validate simulation operation"""
        sim_op = SimulationOperation(**operation.__dict__)
        
        # Check operation type
        if sim_op.operation_subtype not in self._operation_handlers:
            return False, f"Unknown operation: {sim_op.operation_subtype}"
        
        # Validate agent operations
        if sim_op.is_agent_operation():
            if sim_op.operation_subtype == "add_agent":
                if "agent_type" not in sim_op.data:
                    return False, "Agent type required"
                if "position" not in sim_op.data:
                    return False, "Agent position required"
            
            elif sim_op.operation_subtype in ["remove_agent", "update_agent"]:
                if "agent_id" not in sim_op.data:
                    return False, "Agent ID required"
        
        # Validate parameter operations
        elif sim_op.is_parameter_operation():
            if "name" not in sim_op.data or "value" not in sim_op.data:
                return False, "Parameter name and value required"
            
            # Type validation
            param_name = sim_op.data["name"]
            param_value = sim_op.data["value"]
            
            if param_name in ["gravity", "time_step", "speed_multiplier"]:
                try:
                    float(param_value)
                except:
                    return False, f"Parameter {param_name} must be numeric"
        
        return True, None
    
    def apply_operation(self, operation: DomainOperation, state: DomainState) -> DomainState:
        """Apply operation to simulation state"""
        sim_op = SimulationOperation(**operation.__dict__)
        handler = self._operation_handlers[sim_op.operation_subtype]
        
        # Create new state
        new_state = DomainState(
            session_id=state.session_id,
            domain_type=state.domain_type,
            version=state.version + 1,
            data=state.data.copy(),
            metadata=state.metadata.copy()
        )
        
        # Apply operation
        new_state = handler(sim_op, new_state)
        
        # Update metadata
        new_state.metadata["last_operation"] = sim_op.operation_id
        new_state.metadata["last_update"] = datetime.utcnow().isoformat()
        
        return new_state
    
    def merge_states(self, state1: DomainState, state2: DomainState) -> DomainState:
        """Merge two simulation states"""
        # Use CRDT merge logic
        crdt = self.create_crdt()
        
        # Convert states to CRDT format
        crdt1 = crdt.from_state(state1.data)
        crdt2 = crdt.from_state(state2.data)
        
        # Merge
        merged_crdt = crdt.merge(crdt1, crdt2)
        
        # Create merged state
        return DomainState(
            session_id=state1.session_id,
            domain_type=state1.domain_type,
            version=max(state1.version, state2.version) + 1,
            data=merged_crdt.to_state(),
            metadata={
                **state1.metadata,
                **state2.metadata,
                "merge_time": datetime.utcnow().isoformat()
            }
        )
    
    def optimize_state(self, state: DomainState) -> DomainState:
        """Optimize simulation state"""
        optimized_data = state.data.copy()
        
        # Compact agent storage
        if "agents" in optimized_data:
            agents = optimized_data["agents"]
            
            # Remove deleted agents
            agents = {k: v for k, v in agents.items() if not v.get("deleted", False)}
            
            # Compress agent positions to lower precision
            for agent in agents.values():
                if "position" in agent:
                    agent["position"] = [round(p, 3) for p in agent["position"]]
            
            optimized_data["agents"] = agents
        
        # Compact operation history
        if "operation_log" in optimized_data:
            # Keep only last 1000 operations
            optimized_data["operation_log"] = optimized_data["operation_log"][-1000:]
        
        return DomainState(
            session_id=state.session_id,
            domain_type=state.domain_type,
            version=state.version,
            data=optimized_data,
            metadata={
                **state.metadata,
                "optimized": True,
                "optimization_time": datetime.utcnow().isoformat()
            }
        )
    
    def get_view_for_user(self, state: DomainState, user_id: str, 
                         viewport: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        """Get user-specific view of simulation"""
        view = {
            "simulation_id": state.session_id,
            "tick": state.data.get("tick", 0),
            "status": state.data.get("status", "paused"),
            "parameters": state.data.get("parameters", {}),
            "metrics": self._calculate_metrics(state)
        }
        
        # Add visible agents based on viewport
        if viewport:
            visible_agents = self._get_visible_agents(state, viewport)
            view["agents"] = visible_agents
            view["agent_count"] = len(visible_agents)
            view["total_agents"] = len(state.data.get("agents", {}))
        else:
            # Return limited subset if no viewport
            all_agents = state.data.get("agents", {})
            agent_list = list(all_agents.items())[:self.max_broadcast_agents]
            view["agents"] = dict(agent_list)
            view["agent_count"] = len(agent_list)
            view["total_agents"] = len(all_agents)
        
        return view
    
    def get_resource_requirements(self, state: DomainState) -> Dict[str, Any]:
        """Calculate resource requirements for simulation"""
        agent_count = len(state.data.get("agents", {}))
        parameters = state.data.get("parameters", {})
        
        # Base requirements
        cpu_cores = max(4, agent_count // 10000)  # 1 core per 10k agents
        memory_gb = max(8, agent_count * 0.001)  # ~1KB per agent
        
        # GPU requirements based on simulation type
        gpu_required = False
        gpu_type = None
        gpu_count = 0
        
        if parameters.get("physics_enabled", False):
            gpu_required = True
            gpu_type = "GPU_V100" if agent_count < 100000 else "GPU_A100"
            gpu_count = max(1, agent_count // 100000)
        
        return {
            "cpu_cores": cpu_cores,
            "memory_gb": memory_gb,
            "gpu_required": gpu_required,
            "gpu_type": gpu_type,
            "gpu_count": gpu_count,
            "storage_gb": 10,  # For checkpoints
            "network_bandwidth_mbps": 100,
            "estimated_cost_per_hour": self._estimate_cost(
                cpu_cores, memory_gb, gpu_count, gpu_type
            )
        }
    
    # Operation handlers
    
    def _handle_add_agent(self, operation: SimulationOperation, state: DomainState) -> DomainState:
        """Handle add agent operation"""
        agents = state.data.setdefault("agents", {})
        
        agent_id = operation.data.get("agent_id", f"agent_{len(agents)}")
        agents[agent_id] = {
            "id": agent_id,
            "type": operation.data["agent_type"],
            "position": operation.data["position"],
            "velocity": operation.data.get("velocity", [0, 0, 0]),
            "properties": operation.data.get("properties", {}),
            "created_by": operation.user_id,
            "created_at": operation.timestamp.isoformat()
        }
        
        return state
    
    def _handle_remove_agent(self, operation: SimulationOperation, state: DomainState) -> DomainState:
        """Handle remove agent operation"""
        agents = state.data.get("agents", {})
        agent_id = operation.data["agent_id"]
        
        if agent_id in agents:
            # Mark as deleted for CRDT
            agents[agent_id]["deleted"] = True
            agents[agent_id]["deleted_by"] = operation.user_id
            agents[agent_id]["deleted_at"] = operation.timestamp.isoformat()
        
        return state
    
    def _handle_update_agent(self, operation: SimulationOperation, state: DomainState) -> DomainState:
        """Handle update agent operation"""
        agents = state.data.get("agents", {})
        agent_id = operation.data["agent_id"]
        
        if agent_id in agents and not agents[agent_id].get("deleted", False):
            updates = operation.data.get("updates", {})
            agents[agent_id].update(updates)
            agents[agent_id]["last_updated_by"] = operation.user_id
            agents[agent_id]["last_updated_at"] = operation.timestamp.isoformat()
        
        return state
    
    def _handle_batch_update_agents(self, operation: SimulationOperation, state: DomainState) -> DomainState:
        """Handle batch agent updates"""
        agents = state.data.get("agents", {})
        updates = operation.data.get("updates", {})
        
        for agent_id, agent_updates in updates.items():
            if agent_id in agents and not agents[agent_id].get("deleted", False):
                agents[agent_id].update(agent_updates)
                agents[agent_id]["last_updated_by"] = operation.user_id
                agents[agent_id]["last_updated_at"] = operation.timestamp.isoformat()
        
        return state
    
    def _handle_set_parameter(self, operation: SimulationOperation, state: DomainState) -> DomainState:
        """Handle parameter update"""
        params = state.data.setdefault("parameters", {})
        params[operation.data["name"]] = {
            "value": operation.data["value"],
            "updated_by": operation.user_id,
            "updated_at": operation.timestamp.isoformat()
        }
        return state
    
    def _handle_start(self, operation: SimulationOperation, state: DomainState) -> DomainState:
        """Handle simulation start"""
        state.data["status"] = "running"
        state.data["started_at"] = operation.timestamp.isoformat()
        state.data["started_by"] = operation.user_id
        return state
    
    def _handle_pause(self, operation: SimulationOperation, state: DomainState) -> DomainState:
        """Handle simulation pause"""
        state.data["status"] = "paused"
        state.data["paused_at"] = operation.timestamp.isoformat()
        state.data["paused_by"] = operation.user_id
        return state
    
    def _handle_step(self, operation: SimulationOperation, state: DomainState) -> DomainState:
        """Handle simulation step"""
        state.data["tick"] = state.data.get("tick", 0) + 1
        state.data["last_step_at"] = operation.timestamp.isoformat()
        state.data["last_step_by"] = operation.user_id
        return state
    
    def _handle_reset(self, operation: SimulationOperation, state: DomainState) -> DomainState:
        """Handle simulation reset"""
        state.data["tick"] = 0
        state.data["status"] = "paused"
        state.data["agents"] = {}
        state.data["reset_at"] = operation.timestamp.isoformat()
        state.data["reset_by"] = operation.user_id
        return state
    
    def _handle_checkpoint(self, operation: SimulationOperation, state: DomainState) -> DomainState:
        """Handle checkpoint creation"""
        checkpoints = state.data.setdefault("checkpoints", [])
        checkpoints.append({
            "id": operation.data.get("checkpoint_id", f"checkpoint_{len(checkpoints)}"),
            "name": operation.data.get("name", ""),
            "tick": state.data.get("tick", 0),
            "created_by": operation.user_id,
            "created_at": operation.timestamp.isoformat()
        })
        return state
    
    def _handle_branch(self, operation: SimulationOperation, state: DomainState) -> DomainState:
        """Handle simulation branching"""
        branches = state.data.setdefault("branches", [])
        branches.append({
            "id": operation.data.get("branch_id", f"branch_{len(branches)}"),
            "name": operation.data.get("name", ""),
            "parent_tick": state.data.get("tick", 0),
            "created_by": operation.user_id,
            "created_at": operation.timestamp.isoformat()
        })
        return state
    
    # Helper methods
    
    def _get_visible_agents(self, state: DomainState, viewport: Dict[str, Any]) -> Dict[str, Any]:
        """Get agents visible in viewport"""
        agents = state.data.get("agents", {})
        visible = {}
        
        # Extract viewport bounds
        min_x = viewport.get("min_x", -float('inf'))
        max_x = viewport.get("max_x", float('inf'))
        min_y = viewport.get("min_y", -float('inf'))
        max_y = viewport.get("max_y", float('inf'))
        min_z = viewport.get("min_z", -float('inf'))
        max_z = viewport.get("max_z", float('inf'))
        
        for agent_id, agent in agents.items():
            if agent.get("deleted", False):
                continue
                
            pos = agent.get("position", [0, 0, 0])
            if (min_x <= pos[0] <= max_x and 
                min_y <= pos[1] <= max_y and 
                min_z <= pos[2] <= max_z):
                visible[agent_id] = agent
                
                # Stop if we've hit the broadcast limit
                if len(visible) >= self.max_broadcast_agents:
                    break
        
        return visible
    
    def _calculate_metrics(self, state: DomainState) -> Dict[str, Any]:
        """Calculate simulation metrics"""
        agents = state.data.get("agents", {})
        active_agents = [a for a in agents.values() if not a.get("deleted", False)]
        
        return {
            "total_agents": len(active_agents),
            "tick": state.data.get("tick", 0),
            "agent_types": self._count_agent_types(active_agents),
            "avg_position": self._calculate_avg_position(active_agents),
            "bounding_box": self._calculate_bounding_box(active_agents)
        }
    
    def _count_agent_types(self, agents: List[Dict[str, Any]]) -> Dict[str, int]:
        """Count agents by type"""
        counts = {}
        for agent in agents:
            agent_type = agent.get("type", "unknown")
            counts[agent_type] = counts.get(agent_type, 0) + 1
        return counts
    
    def _calculate_avg_position(self, agents: List[Dict[str, Any]]) -> List[float]:
        """Calculate average agent position"""
        if not agents:
            return [0.0, 0.0, 0.0]
        
        positions = [agent.get("position", [0, 0, 0]) for agent in agents]
        avg_pos = np.mean(positions, axis=0)
        return avg_pos.tolist()
    
    def _calculate_bounding_box(self, agents: List[Dict[str, Any]]) -> Dict[str, List[float]]:
        """Calculate bounding box of all agents"""
        if not agents:
            return {"min": [0, 0, 0], "max": [0, 0, 0]}
        
        positions = [agent.get("position", [0, 0, 0]) for agent in agents]
        min_pos = np.min(positions, axis=0)
        max_pos = np.max(positions, axis=0)
        
        return {
            "min": min_pos.tolist(),
            "max": max_pos.tolist()
        }
    
    def _estimate_cost(self, cpu_cores: int, memory_gb: float, 
                      gpu_count: int, gpu_type: Optional[str]) -> float:
        """Estimate hourly cost"""
        # Simple cost model
        cpu_cost = cpu_cores * 0.05  # $0.05 per core/hour
        memory_cost = memory_gb * 0.01  # $0.01 per GB/hour
        
        gpu_cost = 0
        if gpu_count > 0 and gpu_type:
            gpu_prices = {
                "GPU_V100": 2.5,
                "GPU_A100": 4.0,
                "GPU_H100": 8.0
            }
            gpu_cost = gpu_count * gpu_prices.get(gpu_type, 3.0)
        
        return cpu_cost + memory_cost + gpu_cost
    
    def get_capabilities(self) -> Dict[str, Any]:
        """Return simulation domain capabilities"""
        return {
            **super().get_capabilities(),
            "max_agents": self.max_agents,
            "max_broadcast_agents": self.max_broadcast_agents,
            "supports_physics": True,
            "supports_ml": True,
            "supports_checkpoints": True,
            "supports_branching": True,
            "requires_gpu": True,
            "update_rate_hz": 60
        } 