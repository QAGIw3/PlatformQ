"""
CAD Domain Adapter

Handles collaborative CAD/3D modeling with mesh operations, materials, and optimization.
"""

from typing import Dict, Any, List, Optional, Tuple
from datetime import datetime
import numpy as np
import trimesh

from .base import (
    BaseDomainAdapter, DomainOperation, DomainState, OperationType
)
from platformq_shared.crdt import Geometry3DCRDT


class CADOperation(DomainOperation):
    """Extended operation for CAD domain"""
    
    @property
    def operation_subtype(self) -> str:
        """Get CAD-specific operation type"""
        return self.data.get("subtype", "")
    
    def is_geometry_operation(self) -> bool:
        return self.operation_subtype in ["transform", "extrude", "boolean", "modify"]
    
    def is_mesh_operation(self) -> bool:
        return self.operation_subtype in ["decimate", "subdivide", "remesh", "optimize"]
    
    def is_material_operation(self) -> bool:
        return self.operation_subtype in ["apply_material", "update_material", "remove_material"]


class CADAdapter(BaseDomainAdapter):
    """Domain adapter for CAD collaboration"""
    
    def __init__(self):
        super().__init__("cad")
        self.max_vertices = 10_000_000
        self.max_broadcast_vertices = 100_000  # Limit for real-time updates
        self.lod_levels = [1.0, 0.5, 0.25, 0.1]  # Level of detail ratios
    
    def _initialize_handlers(self):
        """Initialize CAD-specific operation handlers"""
        self._operation_handlers = {
            # Geometry operations
            "transform": self._handle_transform,
            "extrude": self._handle_extrude,
            "boolean": self._handle_boolean,
            "modify": self._handle_modify,
            
            # Mesh operations
            "decimate": self._handle_decimate,
            "subdivide": self._handle_subdivide,
            "remesh": self._handle_remesh,
            "optimize": self._handle_optimize,
            
            # Material operations
            "apply_material": self._handle_apply_material,
            "update_material": self._handle_update_material,
            
            # Collaborative features
            "select_object": self._handle_select,
            "lock_object": self._handle_lock,
            "create_annotation": self._handle_annotation,
            
            # Optimization
            "quantum_optimize": self._handle_quantum_optimize
        }
    
    def create_crdt(self) -> Geometry3DCRDT:
        """Create CAD-specific CRDT"""
        return Geometry3DCRDT()
    
    def validate_operation(self, operation: DomainOperation) -> Tuple[bool, Optional[str]]:
        """Validate CAD operation"""
        cad_op = CADOperation(**operation.__dict__)
        
        # Check operation type
        if cad_op.operation_subtype not in self._operation_handlers:
            return False, f"Unknown operation: {cad_op.operation_subtype}"
        
        # Validate geometry operations
        if cad_op.is_geometry_operation():
            if "object_id" not in cad_op.data:
                return False, "Object ID required for geometry operations"
            
            if cad_op.operation_subtype == "transform":
                if "matrix" not in cad_op.data:
                    return False, "Transformation matrix required"
                
                # Validate matrix format
                matrix = cad_op.data["matrix"]
                if not isinstance(matrix, list) or len(matrix) != 16:
                    return False, "Invalid transformation matrix format"
            
            elif cad_op.operation_subtype == "boolean":
                if "operation" not in cad_op.data or "target_id" not in cad_op.data:
                    return False, "Boolean operation and target required"
        
        # Validate mesh operations
        elif cad_op.is_mesh_operation():
            if "mesh_id" not in cad_op.data:
                return False, "Mesh ID required"
            
            if cad_op.operation_subtype == "decimate":
                if "target_ratio" not in cad_op.data:
                    return False, "Target ratio required for decimation"
                
                ratio = cad_op.data["target_ratio"]
                if not 0 < ratio <= 1:
                    return False, "Target ratio must be between 0 and 1"
        
        return True, None
    
    def apply_operation(self, operation: DomainOperation, state: DomainState) -> DomainState:
        """Apply operation to CAD state"""
        cad_op = CADOperation(**operation.__dict__)
        handler = self._operation_handlers[cad_op.operation_subtype]
        
        # Create new state
        new_state = DomainState(
            session_id=state.session_id,
            domain_type=state.domain_type,
            version=state.version + 1,
            data=state.data.copy(),
            metadata=state.metadata.copy()
        )
        
        # Apply operation
        new_state = handler(cad_op, new_state)
        
        # Update metadata
        new_state.metadata["last_operation"] = cad_op.operation_id
        new_state.metadata["last_update"] = datetime.utcnow().isoformat()
        
        return new_state
    
    def merge_states(self, state1: DomainState, state2: DomainState) -> DomainState:
        """Merge two CAD states"""
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
        """Optimize CAD state"""
        optimized_data = state.data.copy()
        
        # Compact geometry storage
        if "geometries" in optimized_data:
            geometries = optimized_data["geometries"]
            
            # Remove deleted objects
            geometries = {k: v for k, v in geometries.items() 
                         if not v.get("deleted", False)}
            
            # Compress vertex data
            for geom in geometries.values():
                if "vertices" in geom:
                    # Round to reasonable precision
                    vertices = np.array(geom["vertices"])
                    geom["vertices"] = np.round(vertices, 4).tolist()
            
            optimized_data["geometries"] = geometries
        
        # Generate LOD data
        if "meshes" in optimized_data:
            for mesh_id, mesh_data in optimized_data["meshes"].items():
                if "lod_generated" not in mesh_data:
                    self._generate_lod_data(mesh_data)
        
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
        """Get user-specific view of CAD model"""
        view = {
            "session_id": state.session_id,
            "project_name": state.data.get("project_name", "Untitled"),
            "active_users": self._get_active_users(state),
            "selected_objects": self._get_user_selections(state, user_id),
            "locked_objects": state.data.get("locked_objects", {}),
            "materials": state.data.get("materials", {}),
            "annotations": state.data.get("annotations", [])
        }
        
        # Add visible geometry based on viewport and LOD
        if viewport:
            visible_geometry = self._get_visible_geometry(state, viewport)
            view["geometries"] = visible_geometry
            view["mesh_count"] = len(visible_geometry)
        else:
            # Return all geometry with appropriate LOD
            all_geometry = state.data.get("geometries", {})
            view["geometries"] = self._apply_lod(all_geometry, 0.5)
            view["mesh_count"] = len(all_geometry)
        
        # Add viewport-specific user presence
        if "user_presence" in state.data:
            view["user_cursors"] = self._get_visible_cursors(
                state.data["user_presence"], viewport
            )
        
        return view
    
    def get_resource_requirements(self, state: DomainState) -> Dict[str, Any]:
        """Calculate resource requirements for CAD session"""
        total_vertices = 0
        total_faces = 0
        
        # Count geometry complexity
        for geom in state.data.get("geometries", {}).values():
            if not geom.get("deleted", False):
                total_vertices += len(geom.get("vertices", []))
                total_faces += len(geom.get("faces", []))
        
        # Base requirements
        cpu_cores = max(4, total_vertices // 100000)  # 1 core per 100k vertices
        memory_gb = max(16, total_vertices * 0.00001)  # ~10KB per vertex
        
        # GPU requirements for rendering and optimization
        gpu_required = total_vertices > 10000 or state.data.get("quantum_optimization", False)
        gpu_type = "GPU_V100" if total_vertices < 1000000 else "GPU_A100"
        gpu_count = max(1, total_vertices // 1000000) if gpu_required else 0
        
        return {
            "cpu_cores": cpu_cores,
            "memory_gb": memory_gb,
            "gpu_required": gpu_required,
            "gpu_type": gpu_type,
            "gpu_count": gpu_count,
            "storage_gb": 50,  # For model files and textures
            "network_bandwidth_mbps": 1000,  # High bandwidth for geometry streaming
            "estimated_cost_per_hour": self._estimate_cost(
                cpu_cores, memory_gb, gpu_count, gpu_type
            )
        }
    
    # Operation handlers
    
    def _handle_transform(self, operation: CADOperation, state: DomainState) -> DomainState:
        """Handle geometry transformation"""
        geometries = state.data.setdefault("geometries", {})
        object_id = operation.data["object_id"]
        
        if object_id in geometries and not geometries[object_id].get("deleted", False):
            # Apply transformation matrix
            matrix = np.array(operation.data["matrix"]).reshape(4, 4)
            
            # Transform vertices
            vertices = np.array(geometries[object_id]["vertices"])
            vertices_h = np.column_stack([vertices, np.ones(len(vertices))])
            transformed = vertices_h @ matrix.T
            geometries[object_id]["vertices"] = transformed[:, :3].tolist()
            
            # Update metadata
            geometries[object_id]["last_modified_by"] = operation.user_id
            geometries[object_id]["last_modified_at"] = operation.timestamp.isoformat()
            
            # Clear cached LOD data
            if "lod_data" in geometries[object_id]:
                del geometries[object_id]["lod_data"]
        
        return state
    
    def _handle_boolean(self, operation: CADOperation, state: DomainState) -> DomainState:
        """Handle boolean operations"""
        geometries = state.data.get("geometries", {})
        object_id = operation.data["object_id"]
        target_id = operation.data["target_id"]
        bool_op = operation.data["operation"]  # union, difference, intersection
        
        if (object_id in geometries and target_id in geometries and
            not geometries[object_id].get("deleted") and
            not geometries[target_id].get("deleted")):
            
            # Create trimesh objects
            mesh1 = self._geometry_to_trimesh(geometries[object_id])
            mesh2 = self._geometry_to_trimesh(geometries[target_id])
            
            # Perform boolean operation
            if bool_op == "union":
                result = mesh1.union(mesh2)
            elif bool_op == "difference":
                result = mesh1.difference(mesh2)
            elif bool_op == "intersection":
                result = mesh1.intersection(mesh2)
            else:
                return state
            
            # Create new geometry
            new_id = f"bool_{object_id}_{target_id}_{operation.operation_id[:8]}"
            geometries[new_id] = {
                "id": new_id,
                "vertices": result.vertices.tolist(),
                "faces": result.faces.tolist(),
                "normals": result.vertex_normals.tolist(),
                "created_by": operation.user_id,
                "created_at": operation.timestamp.isoformat(),
                "parent_objects": [object_id, target_id],
                "operation": bool_op
            }
            
            # Mark originals as hidden (not deleted, for history)
            geometries[object_id]["hidden"] = True
            geometries[target_id]["hidden"] = True
        
        return state
    
    def _handle_decimate(self, operation: CADOperation, state: DomainState) -> DomainState:
        """Handle mesh decimation"""
        meshes = state.data.setdefault("meshes", {})
        mesh_id = operation.data["mesh_id"]
        target_ratio = operation.data["target_ratio"]
        
        if mesh_id in meshes:
            mesh_data = meshes[mesh_id]
            
            # Perform decimation
            mesh = trimesh.Trimesh(
                vertices=mesh_data["vertices"],
                faces=mesh_data["faces"]
            )
            
            target_faces = int(len(mesh.faces) * target_ratio)
            decimated = mesh.simplify_quadric_decimation(target_faces)
            
            # Update mesh data
            mesh_data["vertices"] = decimated.vertices.tolist()
            mesh_data["faces"] = decimated.faces.tolist()
            mesh_data["vertex_count"] = len(decimated.vertices)
            mesh_data["face_count"] = len(decimated.faces)
            mesh_data["decimation_ratio"] = target_ratio
            mesh_data["last_decimated_by"] = operation.user_id
            mesh_data["last_decimated_at"] = operation.timestamp.isoformat()
            
            # Clear LOD cache
            if "lod_data" in mesh_data:
                del mesh_data["lod_data"]
        
        return state
    
    def _handle_quantum_optimize(self, operation: CADOperation, state: DomainState) -> DomainState:
        """Handle quantum mesh optimization request"""
        optimization_requests = state.data.setdefault("optimization_requests", [])
        
        request = {
            "id": operation.operation_id,
            "mesh_id": operation.data["mesh_id"],
            "optimization_type": operation.data.get("optimization_type", "lod_generation"),
            "target_vertices": operation.data.get("target_vertices"),
            "quality_threshold": operation.data.get("quality_threshold", 0.95),
            "requested_by": operation.user_id,
            "requested_at": operation.timestamp.isoformat(),
            "status": "pending"
        }
        
        optimization_requests.append(request)
        state.data["quantum_optimization"] = True
        
        return state
    
    def _handle_apply_material(self, operation: CADOperation, state: DomainState) -> DomainState:
        """Handle material application"""
        materials = state.data.setdefault("materials", {})
        material_id = operation.data.get("material_id", f"mat_{len(materials)}")
        
        materials[material_id] = {
            "id": material_id,
            "name": operation.data.get("name", "Material"),
            "type": operation.data.get("type", "pbr"),
            "properties": operation.data.get("properties", {}),
            "applied_to": operation.data.get("object_ids", []),
            "created_by": operation.user_id,
            "created_at": operation.timestamp.isoformat()
        }
        
        # Update objects with material reference
        geometries = state.data.get("geometries", {})
        for obj_id in operation.data.get("object_ids", []):
            if obj_id in geometries:
                geometries[obj_id]["material_id"] = material_id
        
        return state
    
    def _handle_select(self, operation: CADOperation, state: DomainState) -> DomainState:
        """Handle object selection"""
        selections = state.data.setdefault("user_selections", {})
        selections[operation.user_id] = {
            "objects": operation.data.get("object_ids", []),
            "timestamp": operation.timestamp.isoformat()
        }
        return state
    
    def _handle_lock(self, operation: CADOperation, state: DomainState) -> DomainState:
        """Handle object locking"""
        locked = state.data.setdefault("locked_objects", {})
        object_id = operation.data["object_id"]
        
        if operation.data.get("lock", True):
            locked[object_id] = {
                "locked_by": operation.user_id,
                "locked_at": operation.timestamp.isoformat()
            }
        else:
            # Unlock only if locked by same user
            if object_id in locked and locked[object_id]["locked_by"] == operation.user_id:
                del locked[object_id]
        
        return state
    
    def _handle_annotation(self, operation: CADOperation, state: DomainState) -> DomainState:
        """Handle annotation creation"""
        annotations = state.data.setdefault("annotations", [])
        
        annotation = {
            "id": operation.data.get("annotation_id", f"ann_{len(annotations)}"),
            "text": operation.data["text"],
            "position": operation.data["position"],
            "object_id": operation.data.get("object_id"),
            "created_by": operation.user_id,
            "created_at": operation.timestamp.isoformat()
        }
        
        annotations.append(annotation)
        return state
    
    # Helper methods
    
    def _geometry_to_trimesh(self, geometry: Dict[str, Any]) -> trimesh.Trimesh:
        """Convert geometry dict to trimesh object"""
        return trimesh.Trimesh(
            vertices=geometry["vertices"],
            faces=geometry["faces"],
            vertex_normals=geometry.get("normals")
        )
    
    def _generate_lod_data(self, mesh_data: Dict[str, Any]):
        """Generate LOD data for mesh"""
        mesh = trimesh.Trimesh(
            vertices=mesh_data["vertices"],
            faces=mesh_data["faces"]
        )
        
        lod_data = {}
        for level in self.lod_levels:
            if level == 1.0:
                lod_data[str(level)] = {
                    "vertices": mesh_data["vertices"],
                    "faces": mesh_data["faces"]
                }
            else:
                target_faces = int(len(mesh.faces) * level)
                decimated = mesh.simplify_quadric_decimation(target_faces)
                lod_data[str(level)] = {
                    "vertices": decimated.vertices.tolist(),
                    "faces": decimated.faces.tolist()
                }
        
        mesh_data["lod_data"] = lod_data
        mesh_data["lod_generated"] = True
    
    def _get_visible_geometry(self, state: DomainState, viewport: Dict[str, Any]) -> Dict[str, Any]:
        """Get geometry visible in viewport with appropriate LOD"""
        geometries = state.data.get("geometries", {})
        visible = {}
        
        camera_pos = np.array(viewport.get("camera_position", [0, 0, 10]))
        
        for geom_id, geom in geometries.items():
            if geom.get("deleted", False) or geom.get("hidden", False):
                continue
            
            # Calculate distance to camera
            center = self._calculate_center(geom["vertices"])
            distance = np.linalg.norm(camera_pos - center)
            
            # Select LOD based on distance
            lod_ratio = self._select_lod_ratio(distance)
            
            # Apply LOD
            if "lod_data" in geom and str(lod_ratio) in geom["lod_data"]:
                lod_geom = geom["lod_data"][str(lod_ratio)]
            else:
                # Decimate on the fly if no LOD data
                lod_geom = self._quick_decimate(geom, lod_ratio)
            
            visible[geom_id] = {
                **geom,
                "vertices": lod_geom["vertices"],
                "faces": lod_geom["faces"],
                "lod_level": lod_ratio
            }
            
            # Check vertex limit
            total_vertices = sum(len(g["vertices"]) for g in visible.values())
            if total_vertices > self.max_broadcast_vertices:
                break
        
        return visible
    
    def _calculate_center(self, vertices: List[List[float]]) -> np.ndarray:
        """Calculate center of vertices"""
        return np.mean(vertices, axis=0)
    
    def _select_lod_ratio(self, distance: float) -> float:
        """Select LOD ratio based on distance"""
        if distance < 10:
            return 1.0
        elif distance < 50:
            return 0.5
        elif distance < 100:
            return 0.25
        else:
            return 0.1
    
    def _quick_decimate(self, geometry: Dict[str, Any], ratio: float) -> Dict[str, Any]:
        """Quick decimation for real-time use"""
        # Simple vertex sampling for speed
        vertices = geometry["vertices"]
        faces = geometry["faces"]
        
        # Sample vertices
        num_vertices = len(vertices)
        target_vertices = int(num_vertices * ratio)
        indices = np.linspace(0, num_vertices - 1, target_vertices, dtype=int)
        
        # Remap faces (simplified - production would use proper decimation)
        return {
            "vertices": [vertices[i] for i in indices],
            "faces": []  # Would need proper face remapping
        }
    
    def _apply_lod(self, geometries: Dict[str, Any], default_ratio: float) -> Dict[str, Any]:
        """Apply LOD to all geometries"""
        result = {}
        total_vertices = 0
        
        for geom_id, geom in geometries.items():
            if geom.get("deleted", False) or geom.get("hidden", False):
                continue
            
            # Use LOD data if available
            if "lod_data" in geom and str(default_ratio) in geom["lod_data"]:
                lod_geom = geom["lod_data"][str(default_ratio)]
            else:
                lod_geom = geom
            
            result[geom_id] = {
                **geom,
                "vertices": lod_geom.get("vertices", geom["vertices"]),
                "faces": lod_geom.get("faces", geom["faces"]),
                "lod_level": default_ratio
            }
            
            total_vertices += len(result[geom_id]["vertices"])
            if total_vertices > self.max_broadcast_vertices:
                break
        
        return result
    
    def _get_active_users(self, state: DomainState) -> List[Dict[str, Any]]:
        """Get list of active users"""
        user_presence = state.data.get("user_presence", {})
        active_users = []
        
        for user_id, presence in user_presence.items():
            if presence.get("status") == "active":
                active_users.append({
                    "user_id": user_id,
                    "name": presence.get("name", "Unknown"),
                    "color": presence.get("color", "#000000"),
                    "last_seen": presence.get("last_seen")
                })
        
        return active_users
    
    def _get_user_selections(self, state: DomainState, user_id: str) -> List[str]:
        """Get objects selected by user"""
        selections = state.data.get("user_selections", {})
        return selections.get(user_id, {}).get("objects", [])
    
    def _get_visible_cursors(self, user_presence: Dict[str, Any],
                           viewport: Optional[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """Get visible user cursors"""
        cursors = []
        
        for user_id, presence in user_presence.items():
            if presence.get("cursor_position") and presence.get("status") == "active":
                cursor = {
                    "user_id": user_id,
                    "position": presence["cursor_position"],
                    "color": presence.get("color", "#000000")
                }
                
                # Check if in viewport
                if viewport:
                    # Simplified viewport check
                    pos = cursor["position"]
                    if (viewport.get("min_x", -float('inf')) <= pos[0] <= viewport.get("max_x", float('inf')) and
                        viewport.get("min_y", -float('inf')) <= pos[1] <= viewport.get("max_y", float('inf'))):
                        cursors.append(cursor)
                else:
                    cursors.append(cursor)
        
        return cursors
    
    def _estimate_cost(self, cpu_cores: int, memory_gb: float,
                      gpu_count: int, gpu_type: Optional[str]) -> float:
        """Estimate hourly cost"""
        # CAD workloads need more memory
        cpu_cost = cpu_cores * 0.08  # $0.08 per core/hour
        memory_cost = memory_gb * 0.015  # $0.015 per GB/hour
        
        gpu_cost = 0
        if gpu_count > 0 and gpu_type:
            gpu_prices = {
                "GPU_V100": 3.0,  # Higher for rendering
                "GPU_A100": 5.0,
                "GPU_H100": 10.0
            }
            gpu_cost = gpu_count * gpu_prices.get(gpu_type, 4.0)
        
        return cpu_cost + memory_cost + gpu_cost
    
    def get_capabilities(self) -> Dict[str, Any]:
        """Return CAD domain capabilities"""
        return {
            **super().get_capabilities(),
            "max_vertices": self.max_vertices,
            "max_broadcast_vertices": self.max_broadcast_vertices,
            "lod_levels": self.lod_levels,
            "supports_boolean_ops": True,
            "supports_materials": True,
            "supports_annotations": True,
            "supports_quantum_optimization": True,
            "requires_gpu": True,
            "update_rate_hz": 30  # Lower than simulation for CAD
        } 