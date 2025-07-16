"""
Conflict-Free Replicated Data Type (CRDT) for 3D Geometry

This module implements a CRDT specifically designed for collaborative 3D modeling,
supporting concurrent edits with automatic conflict resolution.
"""

from typing import Dict, List, Set, Tuple, Optional, Any
from dataclasses import dataclass, field
from enum import Enum
import numpy as np
import uuid
import time
import json
import zlib
from abc import ABC, abstractmethod


class OperationType(Enum):
    """Types of geometry operations"""
    ADD_VERTEX = "add_vertex"
    REMOVE_VERTEX = "remove_vertex"
    MOVE_VERTEX = "move_vertex"
    ADD_EDGE = "add_edge"
    REMOVE_EDGE = "remove_edge"
    ADD_FACE = "add_face"
    REMOVE_FACE = "remove_face"
    TRANSFORM = "transform"
    SET_PROPERTY = "set_property"


@dataclass
class VectorClock:
    """Vector clock for causal ordering"""
    clock: Dict[str, int] = field(default_factory=dict)
    
    def increment(self, replica_id: str):
        """Increment the clock for a replica"""
        self.clock[replica_id] = self.clock.get(replica_id, 0) + 1
    
    def merge(self, other: 'VectorClock'):
        """Merge with another vector clock"""
        for replica_id, timestamp in other.clock.items():
            self.clock[replica_id] = max(self.clock.get(replica_id, 0), timestamp)
    
    def happens_before(self, other: 'VectorClock') -> bool:
        """Check if this clock happens before another"""
        for replica_id, timestamp in self.clock.items():
            if timestamp > other.clock.get(replica_id, 0):
                return False
        return any(t < other.clock.get(r, 0) for r, t in other.clock.items())
    
    def concurrent_with(self, other: 'VectorClock') -> bool:
        """Check if two clocks are concurrent"""
        return not (self.happens_before(other) or other.happens_before(self))


@dataclass
class GeometryElement:
    """Base class for geometry elements"""
    id: str
    replica_id: str
    timestamp: float
    deleted: bool = False
    properties: Dict[str, Any] = field(default_factory=dict)


@dataclass
class Vertex3D(GeometryElement):
    """3D vertex with position"""
    position: np.ndarray = field(default_factory=lambda: np.zeros(3))
    
    def transform(self, matrix: np.ndarray):
        """Apply 4x4 transformation matrix"""
        pos_homo = np.append(self.position, 1)
        self.position = (matrix @ pos_homo)[:3]


@dataclass
class Edge3D(GeometryElement):
    """Edge connecting two vertices"""
    vertex_ids: Tuple[str, str] = ("", "")


@dataclass
class Face3D(GeometryElement):
    """Face defined by vertex IDs"""
    vertex_ids: List[str] = field(default_factory=list)
    normal: Optional[np.ndarray] = None


@dataclass
class GeometryOperation:
    """Represents a single geometry operation"""
    id: str
    replica_id: str
    operation_type: OperationType
    timestamp: float
    vector_clock: VectorClock
    target_id: Optional[str] = None
    data: Dict[str, Any] = field(default_factory=dict)
    parent_operations: List[str] = field(default_factory=list)


class Geometry3DCRDT:
    """
    Conflict-free replicated data type for 3D geometry
    Supports vertices, edges, faces, and transformations
    """
    
    def __init__(self, replica_id: str):
        self.replica_id = replica_id
        self.vertices: Dict[str, Vertex3D] = {}
        self.edges: Dict[str, Edge3D] = {}
        self.faces: Dict[str, Face3D] = {}
        self.vector_clock = VectorClock()
        self.operation_log: List[GeometryOperation] = []
        self.applied_operations: Set[str] = set()
        
    def add_vertex(self, position: np.ndarray, vertex_id: Optional[str] = None) -> str:
        """Add a vertex with automatic conflict resolution"""
        vertex_id = vertex_id or str(uuid.uuid4())
        self.vector_clock.increment(self.replica_id)
        
        vertex = Vertex3D(
            id=vertex_id,
            replica_id=self.replica_id,
            timestamp=time.time(),
            position=position.copy()
        )
        
        operation = GeometryOperation(
            id=str(uuid.uuid4()),
            replica_id=self.replica_id,
            operation_type=OperationType.ADD_VERTEX,
            timestamp=vertex.timestamp,
            vector_clock=VectorClock(clock=self.vector_clock.clock.copy()),
            target_id=vertex_id,
            data={"position": position.tolist()}
        )
        
        self._apply_operation(operation)
        return vertex_id
    
    def move_vertex(self, vertex_id: str, new_position: np.ndarray):
        """Move a vertex to a new position"""
        if vertex_id not in self.vertices:
            raise ValueError(f"Vertex {vertex_id} not found")
        
        self.vector_clock.increment(self.replica_id)
        
        operation = GeometryOperation(
            id=str(uuid.uuid4()),
            replica_id=self.replica_id,
            operation_type=OperationType.MOVE_VERTEX,
            timestamp=time.time(),
            vector_clock=VectorClock(clock=self.vector_clock.clock.copy()),
            target_id=vertex_id,
            data={"position": new_position.tolist()}
        )
        
        self._apply_operation(operation)
    
    def add_edge(self, vertex1_id: str, vertex2_id: str, edge_id: Optional[str] = None) -> str:
        """Add an edge between two vertices"""
        edge_id = edge_id or str(uuid.uuid4())
        self.vector_clock.increment(self.replica_id)
        
        edge = Edge3D(
            id=edge_id,
            replica_id=self.replica_id,
            timestamp=time.time(),
            vertex_ids=(vertex1_id, vertex2_id)
        )
        
        operation = GeometryOperation(
            id=str(uuid.uuid4()),
            replica_id=self.replica_id,
            operation_type=OperationType.ADD_EDGE,
            timestamp=edge.timestamp,
            vector_clock=VectorClock(clock=self.vector_clock.clock.copy()),
            target_id=edge_id,
            data={"vertex_ids": [vertex1_id, vertex2_id]}
        )
        
        self._apply_operation(operation)
        return edge_id
    
    def add_face(self, vertex_ids: List[str], face_id: Optional[str] = None) -> str:
        """Add a face defined by vertices"""
        face_id = face_id or str(uuid.uuid4())
        self.vector_clock.increment(self.replica_id)
        
        face = Face3D(
            id=face_id,
            replica_id=self.replica_id,
            timestamp=time.time(),
            vertex_ids=vertex_ids.copy()
        )
        
        # Calculate face normal
        if len(vertex_ids) >= 3:
            positions = [self.vertices[vid].position for vid in vertex_ids[:3]]
            v1 = positions[1] - positions[0]
            v2 = positions[2] - positions[0]
            face.normal = np.cross(v1, v2)
            face.normal = face.normal / np.linalg.norm(face.normal)
        
        operation = GeometryOperation(
            id=str(uuid.uuid4()),
            replica_id=self.replica_id,
            operation_type=OperationType.ADD_FACE,
            timestamp=face.timestamp,
            vector_clock=VectorClock(clock=self.vector_clock.clock.copy()),
            target_id=face_id,
            data={"vertex_ids": vertex_ids}
        )
        
        self._apply_operation(operation)
        return face_id
    
    def transform_object(self, object_ids: List[str], matrix: np.ndarray):
        """Apply transformation to multiple objects"""
        self.vector_clock.increment(self.replica_id)
        
        operation = GeometryOperation(
            id=str(uuid.uuid4()),
            replica_id=self.replica_id,
            operation_type=OperationType.TRANSFORM,
            timestamp=time.time(),
            vector_clock=VectorClock(clock=self.vector_clock.clock.copy()),
            data={
                "object_ids": object_ids,
                "matrix": matrix.flatten().tolist()
            }
        )
        
        self._apply_operation(operation)
    
    def _apply_operation(self, operation: GeometryOperation):
        """Apply an operation to the local state"""
        if operation.id in self.applied_operations:
            return
        
        self.operation_log.append(operation)
        self.applied_operations.add(operation.id)
        
        if operation.operation_type == OperationType.ADD_VERTEX:
            vertex = Vertex3D(
                id=operation.target_id,
                replica_id=operation.replica_id,
                timestamp=operation.timestamp,
                position=np.array(operation.data["position"])
            )
            self.vertices[operation.target_id] = vertex
            
        elif operation.operation_type == OperationType.MOVE_VERTEX:
            if operation.target_id in self.vertices:
                vertex = self.vertices[operation.target_id]
                # Last-write-wins for position
                if operation.timestamp > vertex.timestamp:
                    vertex.position = np.array(operation.data["position"])
                    vertex.timestamp = operation.timestamp
                    
        elif operation.operation_type == OperationType.ADD_EDGE:
            edge = Edge3D(
                id=operation.target_id,
                replica_id=operation.replica_id,
                timestamp=operation.timestamp,
                vertex_ids=tuple(operation.data["vertex_ids"])
            )
            self.edges[operation.target_id] = edge
            
        elif operation.operation_type == OperationType.ADD_FACE:
            face = Face3D(
                id=operation.target_id,
                replica_id=operation.replica_id,
                timestamp=operation.timestamp,
                vertex_ids=operation.data["vertex_ids"]
            )
            self.faces[operation.target_id] = face
            
        elif operation.operation_type == OperationType.TRANSFORM:
            matrix = np.array(operation.data["matrix"]).reshape(4, 4)
            for obj_id in operation.data["object_ids"]:
                if obj_id in self.vertices:
                    self.vertices[obj_id].transform(matrix)
    
    def merge(self, other: 'Geometry3DCRDT'):
        """Merge another CRDT state into this one"""
        # Merge vector clocks
        self.vector_clock.merge(other.vector_clock)
        
        # Apply operations we haven't seen
        for operation in other.operation_log:
            if operation.id not in self.applied_operations:
                self._apply_operation(operation)
        
        # Sort operations for consistent ordering
        self.operation_log.sort(key=lambda op: (op.timestamp, op.id))
    
    def get_operations_since(self, vector_clock: VectorClock) -> List[GeometryOperation]:
        """Get operations that happened after the given vector clock"""
        return [
            op for op in self.operation_log
            if any(
                op.vector_clock.clock.get(replica_id, 0) > vector_clock.clock.get(replica_id, 0)
                for replica_id in op.vector_clock.clock
            )
        ]
    
    def serialize(self) -> bytes:
        """Serialize the CRDT state to bytes"""
        state = {
            "replica_id": self.replica_id,
            "vector_clock": self.vector_clock.clock,
            "operations": [
                {
                    "id": op.id,
                    "replica_id": op.replica_id,
                    "operation_type": op.operation_type.value,
                    "timestamp": op.timestamp,
                    "vector_clock": op.vector_clock.clock,
                    "target_id": op.target_id,
                    "data": op.data,
                    "parent_operations": op.parent_operations
                }
                for op in self.operation_log
            ]
        }
        json_bytes = json.dumps(state).encode('utf-8')
        return zlib.compress(json_bytes)
    
    @classmethod
    def deserialize(cls, data: bytes) -> 'Geometry3DCRDT':
        """Deserialize CRDT state from bytes"""
        json_bytes = zlib.decompress(data)
        state = json.loads(json_bytes.decode('utf-8'))
        
        crdt = cls(state["replica_id"])
        crdt.vector_clock.clock = state["vector_clock"]
        
        for op_data in state["operations"]:
            operation = GeometryOperation(
                id=op_data["id"],
                replica_id=op_data["replica_id"],
                operation_type=OperationType(op_data["operation_type"]),
                timestamp=op_data["timestamp"],
                vector_clock=VectorClock(clock=op_data["vector_clock"]),
                target_id=op_data["target_id"],
                data=op_data["data"],
                parent_operations=op_data["parent_operations"]
            )
            crdt._apply_operation(operation)
        
        return crdt
    
    def get_geometry_stats(self) -> Dict[str, Any]:
        """Get statistics about the current geometry"""
        return {
            "vertex_count": len([v for v in self.vertices.values() if not v.deleted]),
            "edge_count": len([e for e in self.edges.values() if not e.deleted]),
            "face_count": len([f for f in self.faces.values() if not f.deleted]),
            "operation_count": len(self.operation_log),
            "replicas": list(self.vector_clock.clock.keys())
        } 