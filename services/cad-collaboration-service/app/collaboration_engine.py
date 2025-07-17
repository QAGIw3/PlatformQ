"""
Advanced CAD Collaboration Engine

Provides real-time collaborative editing capabilities with:
- Operational Transform (OT) for conflict resolution
- CRDT-based synchronization
- Awareness system for user presence
- Version control and branching
"""

import asyncio
import json
import time
from typing import Dict, List, Set, Optional, Any, Tuple
from dataclasses import dataclass, field
from datetime import datetime
import logging
import numpy as np
from collections import defaultdict, deque
import uuid

from .mesh_decimator import MeshDecimator, MeshLOD

logger = logging.getLogger(__name__)


@dataclass
class GeometryOperation:
    """Represents a geometry modification operation"""
    operation_id: str
    operation_type: str  # transform, extrude, boolean, modify, etc.
    user_id: str
    session_id: str
    timestamp: float
    target_objects: List[str]
    parameters: Dict[str, Any]
    parent_operations: List[str] = field(default_factory=list)
    vector_clock: Dict[str, int] = field(default_factory=dict)
    
    def conflicts_with(self, other: 'GeometryOperation') -> bool:
        """Check if this operation conflicts with another"""
        # Operations on different objects don't conflict
        if not set(self.target_objects).intersection(other.target_objects):
            return False
            
        # Same user operations don't conflict (assumed sequential)
        if self.user_id == other.user_id:
            return False
            
        # Check operation type compatibility
        compatible_ops = {
            ("transform", "transform"),  # Transforms can be composed
            ("modify", "modify"),  # Modifications on different properties
        }
        
        op_pair = (self.operation_type, other.operation_type)
        if op_pair in compatible_ops or op_pair[::-1] in compatible_ops:
            # Further check if they actually conflict
            return self._check_parameter_conflicts(other)
            
        # Different operation types on same object conflict
        return True
        
    def _check_parameter_conflicts(self, other: 'GeometryOperation') -> bool:
        """Check if operation parameters conflict"""
        if self.operation_type == "transform" and other.operation_type == "transform":
            # Transforms don't conflict - they compose
            return False
            
        if self.operation_type == "modify" and other.operation_type == "modify":
            # Check if modifying same properties
            my_props = set(self.parameters.get("properties", {}).keys())
            other_props = set(other.parameters.get("properties", {}).keys())
            return bool(my_props.intersection(other_props))
            
        return True


@dataclass
class UserPresence:
    """User presence information"""
    user_id: str
    session_id: str
    name: str
    color: str
    cursor_position: Optional[Dict[str, float]] = None
    selected_objects: List[str] = field(default_factory=list)
    viewport: Optional[Dict[str, Any]] = None
    last_seen: float = field(default_factory=time.time)
    status: str = "active"  # active, idle, away


@dataclass
class CollaborationSession:
    """Collaborative editing session"""
    session_id: str
    project_id: str
    created_at: float
    users: Dict[str, UserPresence] = field(default_factory=dict)
    operations: List[GeometryOperation] = field(default_factory=list)
    operation_graph: Dict[str, Set[str]] = field(default_factory=lambda: defaultdict(set))
    vector_clocks: Dict[str, Dict[str, int]] = field(default_factory=lambda: defaultdict(dict))
    checkpoints: List[Dict[str, Any]] = field(default_factory=list)
    

class CollaborationEngine:
    """
    Engine for real-time CAD collaboration
    """
    
    def __init__(self,
                 conflict_resolution: str = "operational_transform",
                 history_limit: int = 1000):
        """
        Initialize collaboration engine
        
        Args:
            conflict_resolution: Method for resolving conflicts (operational_transform, crdt, last_write_wins)
            history_limit: Maximum operations to keep in history
        """
        self.conflict_resolution = conflict_resolution
        self.history_limit = history_limit
        
        # Active sessions
        self.sessions: Dict[str, CollaborationSession] = {}
        
        # Operation transformers
        self.transformers = {
            "operational_transform": self._operational_transform,
            "crdt": self._crdt_merge,
            "last_write_wins": self._last_write_wins
        }
        
        # Mesh decimator for LOD synchronization
        self.mesh_decimator = MeshDecimator()
        
        # WebSocket connections
        self.connections: Dict[str, List[Any]] = defaultdict(list)
        
    async def create_session(self,
                           project_id: str,
                           user_id: str,
                           user_name: str) -> CollaborationSession:
        """Create a new collaboration session"""
        session_id = str(uuid.uuid4())
        
        session = CollaborationSession(
            session_id=session_id,
            project_id=project_id,
            created_at=time.time()
        )
        
        # Add first user
        await self.join_session(session_id, user_id, user_name)
        
        self.sessions[session_id] = session
        
        logger.info(f"Created collaboration session {session_id} for project {project_id}")
        
        return session
        
    async def join_session(self,
                         session_id: str,
                         user_id: str,
                         user_name: str) -> UserPresence:
        """Join an existing session"""
        if session_id not in self.sessions:
            raise ValueError(f"Session {session_id} not found")
            
        session = self.sessions[session_id]
        
        # Create user presence
        presence = UserPresence(
            user_id=user_id,
            session_id=session_id,
            name=user_name,
            color=self._generate_user_color(len(session.users))
        )
        
        session.users[user_id] = presence
        session.vector_clocks[user_id] = {uid: 0 for uid in session.users}
        
        # Broadcast user joined
        await self._broadcast_event(session_id, {
            "type": "user_joined",
            "user": {
                "id": user_id,
                "name": user_name,
                "color": presence.color
            },
            "timestamp": time.time()
        })
        
        logger.info(f"User {user_id} joined session {session_id}")
        
        return presence
        
    async def leave_session(self, session_id: str, user_id: str):
        """Leave a session"""
        if session_id not in self.sessions:
            return
            
        session = self.sessions[session_id]
        
        if user_id in session.users:
            del session.users[user_id]
            
            # Broadcast user left
            await self._broadcast_event(session_id, {
                "type": "user_left",
                "user_id": user_id,
                "timestamp": time.time()
            })
            
        # Clean up empty sessions
        if not session.users:
            del self.sessions[session_id]
            logger.info(f"Deleted empty session {session_id}")
            
    async def apply_operation(self,
                            session_id: str,
                            operation: GeometryOperation) -> Dict[str, Any]:
        """
        Apply a geometry operation with conflict resolution
        
        Args:
            session_id: Session ID
            operation: Operation to apply
            
        Returns:
            Result with transformed operation and conflicts
        """
        if session_id not in self.sessions:
            raise ValueError(f"Session {session_id} not found")
            
        session = self.sessions[session_id]
        
        # Update vector clock
        user_clock = session.vector_clocks.get(operation.user_id, {})
        user_clock[operation.user_id] = user_clock.get(operation.user_id, 0) + 1
        operation.vector_clock = user_clock.copy()
        
        # Find concurrent operations
        concurrent_ops = self._find_concurrent_operations(session, operation)
        
        # Resolve conflicts
        conflicts = []
        transformed_op = operation
        
        if concurrent_ops:
            transformer = self.transformers.get(
                self.conflict_resolution,
                self._operational_transform
            )
            
            transformed_op, conflicts = await transformer(
                operation, concurrent_ops, session
            )
            
        # Apply operation
        session.operations.append(transformed_op)
        
        # Update operation graph
        for parent_id in transformed_op.parent_operations:
            session.operation_graph[parent_id].add(transformed_op.operation_id)
            
        # Limit history
        if len(session.operations) > self.history_limit:
            self._compact_history(session)
            
        # Broadcast operation
        await self._broadcast_operation(session_id, transformed_op, conflicts)
        
        return {
            "operation": transformed_op,
            "conflicts": conflicts,
            "resolved": len(conflicts) > 0
        }
        
    def _find_concurrent_operations(self,
                                  session: CollaborationSession,
                                  operation: GeometryOperation) -> List[GeometryOperation]:
        """Find operations concurrent with the given operation"""
        concurrent = []
        
        for op in reversed(session.operations[-100:]):  # Check recent operations
            # Check if concurrent using vector clocks
            if self._are_concurrent(operation.vector_clock, op.vector_clock):
                if operation.conflicts_with(op):
                    concurrent.append(op)
                    
        return concurrent
        
    def _are_concurrent(self,
                       clock1: Dict[str, int],
                       clock2: Dict[str, int]) -> bool:
        """Check if two vector clocks represent concurrent operations"""
        # Neither happens before the other
        clock1_before = all(
            clock1.get(k, 0) <= clock2.get(k, 0)
            for k in set(clock1) | set(clock2)
        )
        
        clock2_before = all(
            clock2.get(k, 0) <= clock1.get(k, 0)
            for k in set(clock1) | set(clock2)
        )
        
        return not clock1_before and not clock2_before
        
    async def _operational_transform(self,
                                   operation: GeometryOperation,
                                   concurrent_ops: List[GeometryOperation],
                                   session: CollaborationSession) -> Tuple[GeometryOperation, List[Dict]]:
        """
        Apply operational transformation to resolve conflicts
        """
        transformed = operation
        conflicts = []
        
        for concurrent_op in concurrent_ops:
            if operation.operation_type == "transform" and concurrent_op.operation_type == "transform":
                # Compose transforms
                transformed = self._compose_transforms(transformed, concurrent_op)
                
            elif operation.operation_type == "modify" and concurrent_op.operation_type == "modify":
                # Merge modifications
                transformed, conflict = self._merge_modifications(transformed, concurrent_op)
                if conflict:
                    conflicts.append(conflict)
                    
            else:
                # Other conflicts - use intention preservation
                transformed, conflict = self._preserve_intention(transformed, concurrent_op)
                if conflict:
                    conflicts.append(conflict)
                    
        return transformed, conflicts
        
    def _compose_transforms(self,
                          op1: GeometryOperation,
                          op2: GeometryOperation) -> GeometryOperation:
        """Compose two transform operations"""
        # Extract transform matrices
        matrix1 = self._params_to_matrix(op1.parameters)
        matrix2 = self._params_to_matrix(op2.parameters)
        
        # Compose transforms
        combined_matrix = matrix2 @ matrix1
        
        # Create new operation
        composed = GeometryOperation(
            operation_id=op1.operation_id,
            operation_type="transform",
            user_id=op1.user_id,
            session_id=op1.session_id,
            timestamp=op1.timestamp,
            target_objects=op1.target_objects,
            parameters=self._matrix_to_params(combined_matrix),
            parent_operations=op1.parent_operations + [op2.operation_id],
            vector_clock=op1.vector_clock
        )
        
        return composed
        
    def _merge_modifications(self,
                           op1: GeometryOperation,
                           op2: GeometryOperation) -> Tuple[GeometryOperation, Optional[Dict]]:
        """Merge two modification operations"""
        merged_params = op1.parameters.copy()
        conflict = None
        
        # Check for property conflicts
        props1 = op1.parameters.get("properties", {})
        props2 = op2.parameters.get("properties", {})
        
        conflicting_props = set(props1.keys()) & set(props2.keys())
        
        if conflicting_props:
            # Resolve conflicts
            conflict = {
                "type": "property_conflict",
                "properties": list(conflicting_props),
                "resolution": "last_write_wins",
                "values": {
                    prop: {
                        "user1": props1[prop],
                        "user2": props2[prop],
                        "resolved": props2[prop] if op2.timestamp > op1.timestamp else props1[prop]
                    }
                    for prop in conflicting_props
                }
            }
            
            # Apply resolution
            for prop in conflicting_props:
                if op2.timestamp > op1.timestamp:
                    props1[prop] = props2[prop]
                    
        # Merge non-conflicting properties
        for prop, value in props2.items():
            if prop not in props1:
                props1[prop] = value
                
        merged_params["properties"] = props1
        
        return op1, conflict
        
    def _preserve_intention(self,
                          op1: GeometryOperation,
                          op2: GeometryOperation) -> Tuple[GeometryOperation, Dict]:
        """Preserve user intention when operations conflict"""
        # Default conflict resolution
        conflict = {
            "type": "operation_conflict",
            "operations": [op1.operation_type, op2.operation_type],
            "resolution": "deferred",
            "options": [
                {"apply": op1.operation_id, "description": f"Apply {op1.operation_type} by {op1.user_id}"},
                {"apply": op2.operation_id, "description": f"Apply {op2.operation_type} by {op2.user_id}"},
                {"merge": True, "description": "Attempt automatic merge"}
            ]
        }
        
        # Mark operation as pending conflict resolution
        op1.parameters["pending_conflict"] = True
        
        return op1, conflict
        
    async def _crdt_merge(self,
                        operation: GeometryOperation,
                        concurrent_ops: List[GeometryOperation],
                        session: CollaborationSession) -> Tuple[GeometryOperation, List[Dict]]:
        """
        Use CRDT principles for conflict-free merge
        """
        # For CRDT, we ensure operations are commutative
        # This is a simplified implementation
        
        conflicts = []
        
        # Sort operations by timestamp to ensure consistency
        all_ops = concurrent_ops + [operation]
        all_ops.sort(key=lambda op: (op.timestamp, op.operation_id))
        
        # Apply in order
        for op in all_ops:
            if op.operation_id == operation.operation_id:
                return op, conflicts
                
        return operation, conflicts
        
    async def _last_write_wins(self,
                             operation: GeometryOperation,
                             concurrent_ops: List[GeometryOperation],
                             session: CollaborationSession) -> Tuple[GeometryOperation, List[Dict]]:
        """
        Simple last-write-wins conflict resolution
        """
        # Find the most recent operation
        all_ops = concurrent_ops + [operation]
        latest = max(all_ops, key=lambda op: op.timestamp)
        
        conflicts = []
        if latest.operation_id != operation.operation_id:
            conflicts.append({
                "type": "overridden",
                "by_operation": latest.operation_id,
                "reason": "last_write_wins"
            })
            
        return latest, conflicts
        
    async def update_presence(self,
                            session_id: str,
                            user_id: str,
                            presence_data: Dict[str, Any]):
        """Update user presence information"""
        if session_id not in self.sessions:
            return
            
        session = self.sessions[session_id]
        
        if user_id not in session.users:
            return
            
        presence = session.users[user_id]
        
        # Update presence fields
        if "cursor_position" in presence_data:
            presence.cursor_position = presence_data["cursor_position"]
        if "selected_objects" in presence_data:
            presence.selected_objects = presence_data["selected_objects"]
        if "viewport" in presence_data:
            presence.viewport = presence_data["viewport"]
        if "status" in presence_data:
            presence.status = presence_data["status"]
            
        presence.last_seen = time.time()
        
        # Broadcast presence update
        await self._broadcast_event(session_id, {
            "type": "presence_update",
            "user_id": user_id,
            "presence": {
                "cursor_position": presence.cursor_position,
                "selected_objects": presence.selected_objects,
                "viewport": presence.viewport,
                "status": presence.status
            },
            "timestamp": time.time()
        }, exclude_user=user_id)
        
    async def sync_mesh_lod(self,
                          session_id: str,
                          object_id: str,
                          mesh_data: Dict[str, Any],
                          user_viewport: Dict[str, Any]) -> Dict[str, Any]:
        """
        Synchronize mesh with appropriate LOD for each user
        
        Args:
            session_id: Session ID
            object_id: Object ID
            mesh_data: Full resolution mesh data
            user_viewport: Requesting user's viewport info
            
        Returns:
            LOD data for synchronization
        """
        if session_id not in self.sessions:
            raise ValueError(f"Session {session_id} not found")
            
        session = self.sessions[session_id]
        
        # Extract mesh data
        vertices = np.array(mesh_data["vertices"])
        faces = np.array(mesh_data["faces"])
        
        # Generate LODs if not cached
        cache_key = f"{session_id}:{object_id}"
        # In production, use proper caching
        
        lods = await self.mesh_decimator.generate_lods(
            vertices, faces,
            lod_levels=[1.0, 0.5, 0.25, 0.1, 0.05]
        )
        
        # Determine LOD for each connected user
        user_lods = {}
        
        for user_id, presence in session.users.items():
            if presence.viewport:
                # Calculate appropriate LOD
                view_distance = self._calculate_view_distance(
                    object_id, presence.viewport
                )
                
                selected_lod = self.mesh_decimator.select_lod(
                    lods,
                    view_distance,
                    (presence.viewport.get("width", 1920),
                     presence.viewport.get("height", 1080))
                )
                
                user_lods[user_id] = {
                    "level": selected_lod.level,
                    "vertex_count": selected_lod.vertex_count,
                    "face_count": selected_lod.face_count
                }
                
        # Return appropriate LOD data
        return {
            "object_id": object_id,
            "lods": [
                {
                    "level": lod.level,
                    "vertices": lod.vertices.tolist(),
                    "faces": lod.faces.tolist(),
                    "error_metric": lod.error_metric
                }
                for lod in lods
            ],
            "user_assignments": user_lods
        }
        
    def _calculate_view_distance(self,
                               object_id: str,
                               viewport: Dict[str, Any]) -> float:
        """Calculate view distance from viewport data"""
        # Simplified calculation
        camera_pos = np.array(viewport.get("camera_position", [0, 0, 10]))
        object_pos = np.array(viewport.get("object_positions", {}).get(object_id, [0, 0, 0]))
        
        return float(np.linalg.norm(camera_pos - object_pos))
        
    async def create_checkpoint(self,
                              session_id: str,
                              name: str,
                              description: str = "") -> Dict[str, Any]:
        """Create a checkpoint of current state"""
        if session_id not in self.sessions:
            raise ValueError(f"Session {session_id} not found")
            
        session = self.sessions[session_id]
        
        checkpoint = {
            "id": str(uuid.uuid4()),
            "name": name,
            "description": description,
            "timestamp": time.time(),
            "operation_count": len(session.operations),
            "user_count": len(session.users),
            "vector_clocks": dict(session.vector_clocks)
        }
        
        session.checkpoints.append(checkpoint)
        
        # Broadcast checkpoint created
        await self._broadcast_event(session_id, {
            "type": "checkpoint_created",
            "checkpoint": checkpoint
        })
        
        return checkpoint
        
    async def branch_from_checkpoint(self,
                                   session_id: str,
                                   checkpoint_id: str) -> CollaborationSession:
        """Create a new branch from a checkpoint"""
        if session_id not in self.sessions:
            raise ValueError(f"Session {session_id} not found")
            
        session = self.sessions[session_id]
        
        # Find checkpoint
        checkpoint = next(
            (cp for cp in session.checkpoints if cp["id"] == checkpoint_id),
            None
        )
        
        if not checkpoint:
            raise ValueError(f"Checkpoint {checkpoint_id} not found")
            
        # Create new session as branch
        branch_session = CollaborationSession(
            session_id=str(uuid.uuid4()),
            project_id=session.project_id,
            created_at=time.time()
        )
        
        # Copy operations up to checkpoint
        op_count = checkpoint["operation_count"]
        branch_session.operations = session.operations[:op_count].copy()
        
        # Copy vector clocks
        branch_session.vector_clocks = checkpoint["vector_clocks"].copy()
        
        self.sessions[branch_session.session_id] = branch_session
        
        return branch_session
        
    def _compact_history(self, session: CollaborationSession):
        """Compact operation history to save memory"""
        # Keep last checkpoint operations
        if session.checkpoints:
            last_checkpoint = session.checkpoints[-1]
            keep_from = last_checkpoint["operation_count"]
        else:
            keep_from = max(0, len(session.operations) - self.history_limit // 2)
            
        session.operations = session.operations[keep_from:]
        
        # Update operation graph
        kept_ids = {op.operation_id for op in session.operations}
        session.operation_graph = {
            op_id: deps & kept_ids
            for op_id, deps in session.operation_graph.items()
            if op_id in kept_ids
        }
        
    def _generate_user_color(self, user_index: int) -> str:
        """Generate a color for user display"""
        colors = [
            "#FF6B6B", "#4ECDC4", "#45B7D1", "#96CEB4",
            "#FECA57", "#FF9FF3", "#54A0FF", "#48DBFB"
        ]
        return colors[user_index % len(colors)]
        
    def _params_to_matrix(self, params: Dict[str, Any]) -> np.ndarray:
        """Convert transform parameters to 4x4 matrix"""
        matrix = np.eye(4)
        
        # Translation
        if "translation" in params:
            matrix[:3, 3] = params["translation"]
            
        # Rotation (assuming quaternion)
        if "rotation" in params:
            # Convert quaternion to rotation matrix
            # Simplified - use proper quaternion library in production
            pass
            
        # Scale
        if "scale" in params:
            scale = params["scale"]
            if isinstance(scale, (int, float)):
                scale = [scale, scale, scale]
            matrix[0, 0] = scale[0]
            matrix[1, 1] = scale[1]
            matrix[2, 2] = scale[2]
            
        return matrix
        
    def _matrix_to_params(self, matrix: np.ndarray) -> Dict[str, Any]:
        """Convert 4x4 matrix to transform parameters"""
        params = {}
        
        # Extract translation
        params["translation"] = matrix[:3, 3].tolist()
        
        # Extract scale (simplified - assumes no shear)
        scale_x = np.linalg.norm(matrix[:3, 0])
        scale_y = np.linalg.norm(matrix[:3, 1])
        scale_z = np.linalg.norm(matrix[:3, 2])
        params["scale"] = [scale_x, scale_y, scale_z]
        
        # Extract rotation (simplified)
        # In production, properly decompose rotation
        
        return params
        
    async def _broadcast_event(self,
                             session_id: str,
                             event: Dict[str, Any],
                             exclude_user: Optional[str] = None):
        """Broadcast event to all session participants"""
        if session_id not in self.connections:
            return
            
        event["session_id"] = session_id
        
        for ws in self.connections[session_id]:
            try:
                # Check if should exclude
                if exclude_user and hasattr(ws, 'user_id') and ws.user_id == exclude_user:
                    continue
                    
                await ws.send_json(event)
            except Exception as e:
                logger.error(f"Failed to broadcast to websocket: {e}")
                
    async def _broadcast_operation(self,
                                 session_id: str,
                                 operation: GeometryOperation,
                                 conflicts: List[Dict]):
        """Broadcast operation to session participants"""
        await self._broadcast_event(session_id, {
            "type": "operation",
            "operation": {
                "id": operation.operation_id,
                "type": operation.operation_type,
                "user_id": operation.user_id,
                "target_objects": operation.target_objects,
                "parameters": operation.parameters,
                "timestamp": operation.timestamp,
                "vector_clock": operation.vector_clock
            },
            "conflicts": conflicts
        })
        
    def register_websocket(self, session_id: str, websocket: Any):
        """Register a WebSocket connection for a session"""
        self.connections[session_id].append(websocket)
        
    def unregister_websocket(self, session_id: str, websocket: Any):
        """Unregister a WebSocket connection"""
        if session_id in self.connections:
            try:
                self.connections[session_id].remove(websocket)
            except ValueError:
                pass 